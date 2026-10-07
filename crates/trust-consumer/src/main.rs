//! `trust-consumer`: persistent consumer for Network Rail's TRUST Train
//! Movements feed (via RDM), filtered to exactly the currently
//! user-tracked `(train_uid, date)` set. It reads the `movement-events`
//! Redis Stream that `movement-relay` fans the RDM Kafka topic into (the
//! direct-Kafka backend was removed in Deploy C, PL-15a). Matched events
//! are forwarded to `api`'s `/private/*` ingest endpoints. NOT a
//! cron-style poller (it consumes a push stream
//! continuously rather than fetching on an interval), which is why it is
//! not named `poller-trust`.

mod config;
mod eta;
mod feed;
mod matching;
mod process;
mod queries;
mod stanox_crs;

use std::time::Duration;

use clap::Parser;
use config::{Config, MovementFeedBackend};
use feed::MovementFeed;
use movement_feed::ActiveFeed;
use movement_feed::redis_stream::RedisStreamMovementFeed;

/// Registers `trust_consumer_errors_total{operation="parse_envelope",msg_type}`
/// at 0 for every `msg_type` a dropped envelope can carry, so the
/// `DistantSignalTrustEnvelopeParseDrops` alert's `increase()` sees the first
/// drop too (R-097).
fn register_parse_envelope_counters() {
    for msg_type in trust_schema::schema::ENVELOPE_FAILURE_MSG_TYPES {
        metrics::counter!(
            common::metrics::metric_name("trust_consumer_errors_total"),
            "operation" => "parse_envelope",
            "msg_type" => msg_type
        )
        .increment(0);
    }
}

/// Every `trust_consumer_errors_total` operation that is a failed call to
/// api (not a data rejection, which is `post_rejected`), registered at 0
/// and summed by the chart's `DistantSignalConsumerApiCallsFailing` alert
/// (2026-10-01: ~23.6k failed tracked-trains reloads raised nothing). The
/// chart's template lists the same operations; a test below keeps the two
/// in step.
const API_CALL_OPERATIONS: &[&str] = &[
    "reload_tracked_trains",
    "post_train_events",
    "reload_stanox_crs",
    "startup_reference_load",
];

/// Retry backoff for a failed tracked-trains reload: 1s doubling to 60s,
/// jittered, and never longer than the reload interval itself.
const TRACKED_TRAINS_RETRY: common::backoff::Backoff =
    common::backoff::Backoff::new(Duration::from_secs(1), Duration::from_secs(60));

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::too_many_lines,
    reason = "these durations are seconds to hours, far below u64::MAX milliseconds; long but linear; splitting it would scatter its shared state across helpers"
)]
async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    common::logging::init("trust-consumer");

    let config = Config::parse();
    if config.metrics.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }
    // Register the stream-gap counter at 0 so the DistantSignalStreamGap
    // alert can use a plain `increase()`: a counter that only appears on its
    // first increment has no increase to see.
    metrics::counter!(common::metrics::metric_name(
        "trust_consumer_stream_gap_detected_total"
    ))
    .increment(0);
    register_parse_envelope_counters();
    common::metrics::register_operation_counters(
        "trust_consumer_errors_total",
        API_CALL_OPERATIONS,
    );
    let (connection_state, progress) = health_http::spawn_with_progress(
        config.health_bind_url.clone(),
        "connected",
        "disconnected",
        Duration::from_secs(config.progress_stall_secs),
    );
    let http = common::ingest::consumer_http_client()?;
    let internal_oauth = config.internal_oauth.token_cache();

    let mut feed = match config.movement_feed_backend {
        MovementFeedBackend::RedisStream => ActiveFeed::RedisStream(
            Box::new(
                connect_redis_feed(&config, common::startup::CONNECT_BACKOFF, &progress).await?,
            ),
            connection_state,
            "trust_consumer_ready",
        ),
    };
    let redis_gap_check_interval = Duration::from_secs(config.redis_gap_check_secs);
    let mut last_redis_gap_check = tokio::time::Instant::now() - redis_gap_check_interval;

    let mut reference = process::Reference {
        pending: Vec::new(),
        by_train_uid: std::collections::HashMap::new(),
        trains_id_by_tracked_train_id: std::collections::HashMap::new(),
        destination_crs_by_trains_id: std::collections::HashMap::new(),
    };
    let reload_interval = Duration::from_secs(config.reference_reload_secs);

    // The CSV-derived table `config.stanox_crs` already loaded at parse
    // time becomes the shared cell's initial value -- the startup value
    // and the fail-open fallback stay exactly as they were (Decision 3);
    // only the read path (a per-cycle snapshot instead of a bare
    // reference) and the addition of this reload block are new.
    let stanox_crs = std::sync::RwLock::new(config.stanox_crs.clone());
    let stanox_crs_reload_interval = Duration::from_secs(config.stanox_crs_reload_secs);
    let mut last_stanox_crs_reload = tokio::time::Instant::now() - stanox_crs_reload_interval;

    // Owned here, for the whole life of the process: TRUST spreads one
    // train's Activation, origin departure, later movements and any
    // cancellation across many batches, so this state must survive every
    // `run_once` call, not be rebuilt per cycle. See
    // `process::ProcessorState`'s docs.
    let mut state = process::ProcessorState::new(config.trust_timestamp_correction_enabled);

    // PL-11: nothing is read from the feed until the first reference load
    // has succeeded. Consuming with the empty `Reference` above matched
    // nothing and XACKed everything, so after a reboot (api and the SSO
    // token endpoint usually come up after the consumers) every movement in
    // the first `reference_reload_secs` -- an origin DEPARTURE for a pinned
    // train among them -- was lost from the live path. Retried on a short,
    // doubling backoff rather than the 60s reload interval.
    let refs = load_reference_until_ok(
        async || {
            queries::fetch_active_tracked_trains(
                &http,
                &config.api_tracked_trains_url,
                &internal_oauth,
            )
            .await
        },
        STARTUP_RETRY_MIN,
        STARTUP_RETRY_MAX,
        || progress.beat(),
    )
    .await;
    apply_loaded_reference(refs, &mut reference, &mut state);
    // Due `reload_interval` after a success; after a failure, on
    // TRACKED_TRAINS_RETRY's backoff. It used to be retried on every pass
    // of this loop while it failed: ~23.6k failed GETs in the 2026-10-01
    // Postgres outage.
    let mut reference_reload =
        common::backoff::RetrySchedule::new(reload_interval, TRACKED_TRAINS_RETRY);
    reference_reload.succeeded();
    // Consecutive failed cycles; see `CYCLE_RETRY_BACKOFF`.
    let mut cycle_failures = common::backoff::FailureStreak::new(CYCLE_RETRY_BACKOFF);

    loop {
        if reference_reload.is_due() {
            match queries::fetch_active_tracked_trains(
                &http,
                &config.api_tracked_trains_url,
                &internal_oauth,
            )
            .await
            {
                Ok(refs) => {
                    apply_loaded_reference(refs, &mut reference, &mut state);
                    reference_reload.succeeded();
                }
                Err(err) => {
                    // An already-loaded reference is kept as it is: a stale
                    // snapshot is far better than none (PL-11).
                    let retry_in =
                        reference_reload.failed_honouring(common::ingest::retry_after(&err));
                    tracing::error!(
                        error = ?err,
                        failures = reference_reload.failures(),
                        retry_in_ms = retry_in.as_millis() as u64,
                        "failed to reload active tracked trains; keeping the previous reference"
                    );
                    metrics::counter!(
                        common::metrics::metric_name("trust_consumer_errors_total"),
                        "operation" => "reload_tracked_trains"
                    )
                    .increment(1);
                }
            }
        }

        if last_stanox_crs_reload.elapsed() >= stanox_crs_reload_interval {
            let fetched =
                queries::fetch_stanox_crs(&http, &config.stanox_crs_url, &internal_oauth).await;
            process::apply_stanox_crs_reload(fetched, &stanox_crs);
            last_stanox_crs_reload = tokio::time::Instant::now();
        }

        if last_redis_gap_check.elapsed() >= redis_gap_check_interval {
            match feed.check_gap().await {
                Ok(Some(gap)) => {
                    tracing::error!(
                        last_delivered = %gap.group_last_delivered_id,
                        new_first_entry = %gap.stream_first_entry_id,
                        "movement-events stream gap detected: some events between these IDs were trimmed before trust-consumer ever read them -- any Activation/Movement in that range is silently lost, possibly stranding a pin in resolution_status='pending' forever"
                    );
                    metrics::counter!(common::metrics::metric_name(
                        "trust_consumer_stream_gap_detected_total"
                    ))
                    .increment(1);
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(error = ?err, "failed to check movement-events stream for a gap; will retry next cycle");
                }
            }
            last_redis_gap_check = tokio::time::Instant::now();
        }

        // api's Retry-After when the POST got a 503 (its database is
        // unavailable).
        let mut retry_after = None;
        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &stanox_crs,
            async |events| {
                let response = queries::post_train_events(
                    &http,
                    &config.api_ingest_url,
                    &internal_oauth,
                    events,
                )
                .await
                .inspect_err(|err| retry_after = common::ingest::retry_after(err))?;
                // Forward signals only for events api actually wrote.
                let rejected: std::collections::HashSet<usize> =
                    response.rejected.iter().map(|row| row.index).collect();
                let written: Vec<common::TrainMovementEventMessage> = events
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| !rejected.contains(index))
                    .map(|(_, event)| event.clone())
                    .collect();
                let signals = process::build_forward_signals(
                    &written,
                    &reference.trains_id_by_tracked_train_id,
                );
                if let Err(err) = queries::post_train_forward_signals(
                    &http,
                    &config.forward_signals_url,
                    &internal_oauth,
                    &signals,
                )
                .await
                {
                    tracing::warn!(error = ?err, "failed to post train forward signals");
                }
                Ok(response.rejected)
            },
        )
        .await;

        if let Some(wait) = cycle_wait(&outcome, &mut cycle_failures, retry_after) {
            // Nothing here waits on anything: `run_once` returns as soon as
            // the feed hands over a batch, and every failure path above
            // skips the commit, so a persistently-down `api` or an erroring
            // feed would otherwise spin this loop at full speed. The batch
            // stays pending in the stream (at-least-once), so waiting longer
            // loses nothing; see `CYCLE_RETRY_BACKOFF`.
            tracing::warn!(
                failures = cycle_failures.failures(),
                retry_in_ms = wait.as_millis() as u64,
                "cycle failed; backing off before the redelivery"
            );
            progress.idle(tokio::time::sleep(wait)).await;
        }
        // One loop iteration completed, however it went -- see
        // `health_http::Progress`.
        progress.beat();
    }
}

/// The Redis Streams backend, connected once Redis is reachable. Redis
/// being down at startup (its pod recreated by the same rollout) is waited
/// for with every attempt logged and beating `progress`, so `/livez` stays
/// 200 through a Redis outage well past `PROGRESS_STALL_SECS` (300s by
/// default) instead of the pod being killed or exiting into
/// `CrashLoopBackOff`. After startup each Redis command is bounded (see
/// `common::redis_conn`) and a failure is a `Cycle::Failed`: backed off by
/// `CYCLE_RETRY_BACKOFF`, progress beaten, retried.
async fn connect_redis_feed(
    config: &Config,
    backoff: common::backoff::Backoff,
    progress: &health_http::Progress,
) -> anyhow::Result<RedisStreamMovementFeed> {
    // REDIS_USERNAME and REDIS_PASSWORD, when set, are applied here
    // (common::redis_auth). The result carries the password: pass it on,
    // never log it.
    let redis_url = common::redis_auth::redis_url_with_credentials(
        &config.redis_url,
        config.redis_username.as_deref(),
        config.redis_password.as_ref(),
    )?;
    RedisStreamMovementFeed::connect_until_ready(
        redis_url.expose(),
        "trust-consumer",
        "trust-consumer-1",
        Duration::from_secs(config.redis_autoclaim_min_idle_secs),
        backoff,
        progress,
    )
    .await
}

/// Wait after a failed cycle (a failed POST, a feed error, a failed
/// dead-letter write): 2s doubling to 60s, jittered, and at least `api`'s
/// `Retry-After` on a 503. It used to be a flat 2s, deliberately: the
/// stream holds the backlog, so waiting longer loses nothing, but a flat 2s
/// through the six-hour 2026-10-01 Postgres outage was a POST every 2s.
const CYCLE_RETRY_BACKOFF: common::backoff::Backoff =
    common::backoff::Backoff::new(Duration::from_secs(2), Duration::from_secs(60));

/// How long to wait after a cycle, if at all: a committed cycle resets the
/// streak, a failed one waits its next backoff (honouring `retry_after`).
fn cycle_wait(
    outcome: &Cycle,
    failures: &mut common::backoff::FailureStreak,
    retry_after: Option<Duration>,
) -> Option<Duration> {
    match outcome {
        Cycle::Committed => {
            failures.succeeded();
            None
        }
        Cycle::Failed => Some(failures.failed(retry_after)),
    }
}

/// First and largest retry delay for the startup reference load (PL-11):
/// 1 s, doubling, capped at 30 s -- the same shape full-coverage-consumer
/// uses for its own startup load.
const STARTUP_RETRY_MIN: Duration = Duration::from_secs(1);
const STARTUP_RETRY_MAX: Duration = Duration::from_secs(30);

/// Rebuilds the matchable pins AND rehydrates already-resolved `train_ids`
/// from a freshly fetched reference, so a restart doesn't permanently lose
/// trains whose origin departure has already been and gone.
fn apply_loaded_reference(
    refs: Vec<common::TrackedTrainRef>,
    reference: &mut process::Reference,
    state: &mut process::ProcessorState,
) {
    process::apply_reference_reload(refs, reference, state);
    // Same cadence, unrelated job: age out parked Activations that no live
    // Movement can still claim, so the national Activation stream can't
    // grow this map without bound.
    //
    // The CURRENT rail day, not `Utc::now().date_naive()` (finding #5): the
    // pruning rule is about how old an Activation's own observed rail day
    // is, so both sides of that comparison have to be rail days on the same
    // Europe/London 02:00 convention, or an Activation observed at 01:00
    // local would be compared against tomorrow's date.
    process::prune_expired_activations(
        &mut state.pending_activations,
        common::rail_day::current_rail_day(chrono::Utc::now()),
    );
}

/// Startup gate (PL-11): calls `fetch` until it succeeds, sleeping `min`,
/// then doubling up to `max`, between attempts, and returns the first
/// successful result. `on_retry` runs on every failure (the caller beats
/// the health-progress heartbeat, so a slow `api` is not mistaken for a
/// wedged loop). The caller must not touch the feed before this returns.
#[expect(
    clippy::cast_possible_truncation,
    reason = "these durations are seconds to hours, far below u64::MAX milliseconds"
)]
async fn load_reference_until_ok<F, B>(
    mut fetch: F,
    min: Duration,
    max: Duration,
    on_retry: B,
) -> Vec<common::TrackedTrainRef>
where
    F: AsyncFnMut() -> anyhow::Result<Vec<common::TrackedTrainRef>>,
    B: Fn(),
{
    let mut backoff = min.min(max);
    loop {
        match fetch().await {
            Ok(refs) => {
                tracing::info!(
                    count = refs.len(),
                    "loaded active tracked trains; starting to consume"
                );
                return refs;
            }
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    retry_in_ms = backoff.as_millis() as u64,
                    "failed to load active tracked trains at startup; not consuming until it loads"
                );
                metrics::counter!(
                    common::metrics::metric_name("trust_consumer_errors_total"),
                    "operation" => "startup_reference_load"
                )
                .increment(1);
                on_retry();
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max);
            }
        }
    }
}

/// What one consume -> post -> commit cycle did. Returned rather than acted
/// on inside `run_cycle` so the caller owns the backoff sleep, and a test of
/// the commit rule doesn't have to wait out a real delay.
#[derive(Debug, PartialEq, Eq)]
enum Cycle {
    /// The batch was posted and its offsets confirmed.
    Committed,
    /// Something failed. Offsets were deliberately left unconfirmed, so the
    /// feed has not advanced past whatever went wrong.
    Failed,
}

/// The `loop` body's consume/post/commit step, extracted so the one rule
/// that matters here -- *never* commit a batch whose post failed -- is
/// unit-testable against `FakeMovementFeed` without a broker or an `api`.
/// `post` is taken as a closure for the same reason: it's the only part of
/// this that needs HTTP.
///
/// The commit is the sole way the consumed position advances (see
/// `MovementFeed::commit`), so skipping it on failure genuinely means "leave
/// this batch to be redelivered", and the `dedup_key` path makes that replay
/// safe.
///
/// **This function also owns the in-memory state's transaction boundary**
/// (finding #4 of the 2026-09-25 review): `process::run_once` journals every
/// mutation it makes to `state`, and exactly one of
/// `ProcessorState::confirm_batch` (the whole cycle succeeded) or
/// `ProcessorState::roll_back_batch` (anything at all failed) is called
/// before returning. Without the rollback, a redelivered batch -- which the
/// Redis backend really does replay into this same process after 30 seconds,
/// via `RedisStreamMovementFeed::reclaim_stale` -- would find its own
/// half-applied state from the failed attempt, decide the train was
/// "already resolved", and drop the one-time
/// `resolved_train_uid`/`resolved_train_id` signal that is the only thing
/// that ever flips a subscription to `'resolved'` in the database.
#[expect(
    clippy::expect_used,
    reason = "the invariant is established just above; the expect message names it"
)]
async fn run_cycle<F, P>(
    feed: &mut F,
    reference: &process::Reference,
    state: &mut process::ProcessorState,
    stanox_crs: &std::sync::RwLock<stanox_crs::StanoxCrsTable>,
    post: P,
) -> Cycle
where
    F: MovementFeed + movement_feed::DeadLetterSink,
    P: AsyncFnOnce(
        &[common::TrainMovementEventMessage],
    ) -> anyhow::Result<Vec<common::RejectedTrustBacklogRow>>,
{
    let snapshot = stanox_crs.read().expect("stanox_crs lock poisoned").clone();

    // The real wall-clock "now" for this whole cycle -- see `run_once`'s own
    // doc comment for why a single per-cycle value (rather than reading the
    // clock again per message) is the right granularity here.
    let received_at = chrono::Utc::now();
    let events = match process::run_once(feed, reference, state, &snapshot, received_at).await {
        Ok(events) => events,
        Err(err) => {
            tracing::error!(error = ?err, "error processing movement feed batch");
            metrics::counter!(
                common::metrics::metric_name("trust_consumer_errors_total"),
                "operation" => "process_batch"
            )
            .increment(1);
            state.roll_back_batch();
            return Cycle::Failed;
        }
    };

    let rejected = match post(&events).await {
        Ok(rejected) => rejected,
        Err(err) => return post_failed(feed, state, err).await,
    };

    // DB2-2: `api` wrote every event except these, which it refused for a
    // data error (each rolled back in full, behind its own savepoint). No
    // retry can fix them, so they are dead-lettered -- with the SQLSTATE and
    // message, for an operator to inspect and re-inject -- and the batch is
    // ACKed like any success. If the dead-letter write fails the batch stays
    // un-ACKed, so the rejected events are not lost; the retry re-posts the
    // good ones harmlessly (`dedup_key`).
    if !rejected.is_empty() {
        let records: Vec<movement_feed::DeadLetter> = rejected
            .iter()
            .map(|row| movement_feed::DeadLetter {
                reason: "rejected_by_api",
                source_id: None,
                delivery_count: None,
                payload: events
                    .get(row.index)
                    .and_then(|event| serde_json::to_string(event).ok())
                    .unwrap_or_default(),
                detail: format!(
                    "{} {} (constraint {}): {} [dedup_key {}]",
                    row.sqlstate,
                    row.reason,
                    row.constraint.as_deref().unwrap_or("-"),
                    row.message,
                    row.dedup_key,
                ),
            })
            .collect();
        if let Err(err) = feed.dead_letter(&records).await {
            tracing::error!(
                error = ?err,
                rejected = records.len(),
                "failed to dead-letter train events api rejected; leaving the batch un-ACKed to retry"
            );
            metrics::counter!(
                common::metrics::metric_name("trust_consumer_errors_total"),
                "operation" => "dead_letter"
            )
            .increment(1);
            state.roll_back_batch();
            return Cycle::Failed;
        }
        tracing::warn!(
            rejected = records.len(),
            "api rejected train events for a data error; dead-lettered them and committing the rest"
        );
        metrics::counter!(
            common::metrics::metric_name("trust_consumer_deadlettered_total"),
            "reason" => "rejected_by_api"
        )
        .increment(records.len() as u64);
    }

    if let Err(err) = feed.commit().await {
        tracing::error!(error = ?err, "failed to commit the movement feed batch");
        metrics::counter!(
            common::metrics::metric_name("trust_consumer_errors_total"),
            "operation" => "commit_offsets"
        )
        .increment(1);
        // Rolled back even though the post itself succeeded: an uncommitted
        // batch WILL be redelivered (`reclaim_stale`), and the replay must build
        // the same events from the same pre-batch state. Re-posting them is
        // harmless -- that is exactly what `dedup_key` and `api`'s
        // `ON CONFLICT` clauses are for.
        state.roll_back_batch();
        return Cycle::Failed;
    }

    // Posted and committed: the batch's in-memory mutations are now facts.
    state.confirm_batch();
    Cycle::Committed
}

/// A failed train-events POST: roll the batch's in-memory state back and
/// leave it un-ACKed. Only an explicit data rejection of the whole request
/// (400/413/422) is handed to `reject_batch` (which isolates and
/// dead-letters the poison entry). A transient failure (unreachable,
/// timeout, 5xx -- which is what `api` now answers for any transient DB
/// error, DB2-2) just leaves the batch pending, to be retried for as long as
/// the outage lasts -- never dead-lettered.
async fn post_failed<F>(
    feed: &mut F,
    state: &mut process::ProcessorState,
    err: anyhow::Error,
) -> Cycle
where
    F: MovementFeed,
{
    state.roll_back_batch();
    if common::ingest::classify_failure(&err) == common::ingest::FailureClass::Rejected {
        tracing::error!(error = ?err, "api rejected this batch's train events; isolating the poison entry");
        metrics::counter!(
            common::metrics::metric_name("trust_consumer_errors_total"),
            "operation" => "post_rejected"
        )
        .increment(1);
        if let Err(reject_err) = feed.reject_batch(&err.to_string()).await {
            tracing::error!(error = ?reject_err, "failed to handle the rejected batch; it stays pending");
        }
        return Cycle::Failed;
    }
    tracing::error!(error = ?err, "failed to post train events; not committing this batch's offsets");
    metrics::counter!(
        common::metrics::metric_name("trust_consumer_errors_total"),
        "operation" => "post_train_events"
    )
    .increment(1);
    Cycle::Failed
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use super::*;
    use crate::feed::FakeMovementFeed;

    /// The real, checked-in `reference-data/stanox-crs.csv`, mirroring
    /// `process.rs`'s own test fixture of the same name -- these tests
    /// depend on the real STANOX `"87212"` translating to `"WAT"` to match
    /// `one_pending_pin`'s pin.
    static TEST_STANOX_CRS: LazyLock<std::sync::RwLock<stanox_crs::StanoxCrsTable>> =
        LazyLock::new(|| {
            let path = common::manifest_dir!().join("../../reference-data/stanox-crs.csv");
            std::sync::RwLock::new(
                stanox_crs::StanoxCrsTable::from_file(&path)
                    .expect("reference-data/stanox-crs.csv should parse"),
            )
        });

    // The raw millis below is 1 hour LATER than the UTC instant it
    // represents (2026-08-28T19:32:00Z as a wire value, not 18:32:00Z) --
    // see `process.rs`'s own `ORIGIN_DEPARTURE` doc comment for why: August
    // is BST, and `common::trust_timestamp::parse_trust_epoch_millis`
    // corrects a BST-period wire value an hour earlier.
    const ORIGIN_DEPARTURE: &str = r#"[{"header":{"msg_type":"0003"},"body":{
        "train_id":"221832406","event_type":"DEPARTURE",
        "planned_timestamp":"1787945520000","actual_timestamp":"1787945520000",
        "loc_stanox":"87212","variation_status":"ON TIME"
    }}]"#;

    fn one_pending_pin() -> process::Reference {
        process::Reference {
            pending: vec![matching::PendingPin {
                tracked_train_id: 1,
                pin_origin_crs: "WAT".to_string(),
                pin_scheduled_departure: "2026-08-28T18:32:00Z".parse().unwrap(),
                train_uid: None,
            }],
            by_train_uid: std::collections::HashMap::new(),
            trains_id_by_tracked_train_id: std::collections::HashMap::new(),
            destination_crs_by_trains_id: std::collections::HashMap::new(),
        }
    }

    /// The regression this guards: while `enable.auto.offset.store` was left
    /// at librdkafka's `true` default, a batch whose post failed still had
    /// its offset stored the moment `recv` returned it, so the *next*
    /// cycle's commit swept it up and the failed batch was never
    /// redelivered. Skipping the commit only preserves the batch if
    /// receiving it never advances anything on its own -- which is exactly
    /// what `FakeMovementFeed` now mirrors.
    #[tokio::test]
    async fn a_failed_post_does_not_commit_the_batch() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |_| Err(anyhow::anyhow!("api is down")),
        )
        .await;

        assert_eq!(outcome, Cycle::Failed);
        assert_eq!(
            feed.committed_count, 0,
            "a batch that never reached api must not be committed"
        );
    }

    /// And the same batch, posted successfully, does commit -- otherwise the
    /// test above would pass against a `commit` that never worked at all.
    #[tokio::test]
    async fn a_successful_post_commits_the_batch() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |events| {
                assert_eq!(events.len(), 1, "the pinned train's origin departure");
                Ok(Vec::new())
            },
        )
        .await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(feed.committed_count, 1);
    }

    /// A cycle that saw nothing has no offset to advance, so it must not
    /// manufacture a commit -- committing an empty poll is how an
    /// unconfirmed offset from a *previous* failed cycle would get swept up.
    #[tokio::test]
    async fn an_empty_poll_commits_nothing() {
        let mut feed = FakeMovementFeed::new(vec![vec![]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |_| Ok(Vec::new()),
        )
        .await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(feed.committed_count, 0);
    }

    /// A run of failures must never commit, however long it goes on -- this
    /// is the shape a real `api` outage takes, and it's the case the offset
    /// fix is there for: nothing is confirmed, so a restart resumes from
    /// before the outage.
    #[tokio::test]
    async fn a_sustained_outage_never_commits() {
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
        ]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        for _ in 0..3 {
            let outcome = run_cycle(
                &mut feed,
                &reference,
                &mut state,
                &TEST_STANOX_CRS,
                async |_| Err(anyhow::anyhow!("api is down")),
            )
            .await;
            assert_eq!(outcome, Cycle::Failed);
        }
        assert_eq!(
            feed.committed_count, 0,
            "nothing reached api, so nothing may be confirmed"
        );
    }

    /// **Finding #8's regression test.** An unparseable payload is now
    /// logged and dropped, NOT propagated as a cycle failure -- so the batch
    /// carrying it is acknowledged instead of being replayed forever.
    ///
    /// This deliberately reverses the previous assertion of this test
    /// (`a_batch_that_fails_to_parse_is_a_failed_cycle_and_commits_nothing`).
    /// That contract was actively harmful: under the Redis backend a batch is
    /// up to 100 entries, acked all-or-nothing, so one poison payload left
    /// every good entry beside it unacked, to be reclaimed 30 seconds later
    /// and fail identically, forever. No retry can fix a payload that does
    /// not parse.
    #[tokio::test]
    async fn an_unparseable_payload_is_dropped_and_the_cycle_still_commits() {
        let mut feed = FakeMovementFeed::new(vec![vec!["not json at all".to_string()]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |events| {
                assert!(events.is_empty(), "nothing parseable to post");
                Ok(Vec::new())
            },
        )
        .await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(
            feed.committed_count, 1,
            "the poison payload must be acknowledged, not replayed forever"
        );
        assert_eq!(
            feed.dead_lettered.len(),
            1,
            "...and dead-lettered, not lost"
        );
        assert_eq!(feed.dead_lettered[0].reason, "unparseable_payload");
        assert_eq!(feed.dead_lettered[0].payload, "not json at all");
    }

    /// If the unparseable payload cannot be dead-lettered, the cycle fails
    /// and nothing is `ACKed`, so it is not lost.
    #[tokio::test]
    async fn a_failed_dead_letter_of_an_unparseable_payload_commits_nothing() {
        let mut feed = FakeMovementFeed::new(vec![vec!["not json at all".to_string()]]);
        feed.fail_next_dead_letter = true;
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |_| Ok(Vec::new()),
        )
        .await;

        assert_eq!(outcome, Cycle::Failed);
        assert_eq!(feed.committed_count, 0);
    }

    fn status_error(status: u16) -> anyhow::Error {
        common::ingest::HttpStatusError {
            prefix: "ingestion POST failed",
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            body: String::new(),
            retry_after: None,
        }
        .into()
    }

    /// Consecutive failed cycles back off exponentially (capped at 60s),
    /// honour api's Retry-After, and reset after a committed cycle.
    #[test]
    fn failed_cycles_back_off_exponentially_and_reset_on_success() {
        let mut failures = common::backoff::FailureStreak::new(CYCLE_RETRY_BACKOFF);
        let mut previous_ceiling = Duration::ZERO;
        for attempt in 0..8 {
            let wait = cycle_wait(&Cycle::Failed, &mut failures, None).unwrap();
            let ceiling = CYCLE_RETRY_BACKOFF.ceiling(attempt);
            assert!(
                wait >= ceiling / 2 && wait <= ceiling,
                "{attempt}: {wait:?}"
            );
            assert!(ceiling >= previous_ceiling);
            previous_ceiling = ceiling;
        }
        assert_eq!(previous_ceiling, Duration::from_secs(60));
        let wait = cycle_wait(&Cycle::Failed, &mut failures, Some(Duration::from_secs(30)));
        assert!(wait.unwrap() >= Duration::from_secs(30));
        assert_eq!(cycle_wait(&Cycle::Committed, &mut failures, None), None);
        assert_eq!(failures.failures(), 0);
        let wait = cycle_wait(&Cycle::Failed, &mut failures, Some(Duration::from_secs(30)));
        assert!(
            wait.unwrap() >= Duration::from_secs(30),
            "Retry-After honoured"
        );
    }

    /// PL-2: transient failures (timeouts, 5xx, auth, ...) never reach
    /// `reject_batch` or the dead-letter sink, however long they go on.
    #[tokio::test]
    async fn a_transient_failure_is_never_rejected_or_dead_lettered() {
        let statuses = [500u16, 502, 503, 504, 401, 403, 404, 429];
        let batches = (0..300)
            .map(|_| vec![ORIGIN_DEPARTURE.to_string()])
            .collect();
        let mut feed = FakeMovementFeed::new(batches);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        for status in statuses.into_iter().cycle().take(300) {
            let outcome = run_cycle(
                &mut feed,
                &reference,
                &mut state,
                &TEST_STANOX_CRS,
                async |_| Err(status_error(status)),
            )
            .await;
            assert_eq!(outcome, Cycle::Failed);
        }
        assert_eq!(feed.committed_count, 0);
        assert!(feed.rejected_batches.is_empty());
        assert!(feed.dead_lettered.is_empty());
    }

    /// PL-2: an explicit data rejection hands the batch to `reject_batch`,
    /// without committing it.
    #[tokio::test]
    async fn a_data_rejection_is_handed_to_reject_batch() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |_| Err(status_error(422)),
        )
        .await;

        assert_eq!(outcome, Cycle::Failed);
        assert_eq!(feed.committed_count, 0);
        assert_eq!(feed.rejected_batches.len(), 1);
        assert!(state.resolved.is_empty(), "rolled back like any failure");
    }

    fn rejected_row(index: usize) -> common::RejectedTrustBacklogRow {
        common::RejectedTrustBacklogRow {
            index,
            dedup_key: "some-dedup-key".to_string(),
            sqlstate: "23514".to_string(),
            reason: "check_violation".to_string(),
            constraint: Some("train_current_state_status_check".to_string()),
            message: "new row violates check constraint".to_string(),
        }
    }

    /// DB2-2: `api` now answers 500 for a transient DB error instead of
    /// swallowing it behind a 200 -- and a 500 must leave the batch
    /// un-ACKed, undead-lettered and rolled back, so the retry re-sends the
    /// same events (including a pin's one-time resolution).
    #[tokio::test]
    async fn a_500_from_api_leaves_the_batch_to_be_retried() {
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
        ]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let failed = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |_| Err(status_error(500)),
        )
        .await;
        assert_eq!(failed, Cycle::Failed);
        assert_eq!(feed.committed_count, 0);
        assert!(feed.dead_lettered.is_empty());
        assert!(feed.rejected_batches.is_empty());
        assert!(state.resolved.is_empty());

        let retried = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |events| {
                assert_eq!(
                    events[0].resolved_train_id,
                    Some("221832406".to_string()),
                    "the retry still carries the resolution"
                );
                Ok(Vec::new())
            },
        )
        .await;
        assert_eq!(retried, Cycle::Committed);
        assert_eq!(feed.committed_count, 1);
    }

    /// DB2-2: events `api` rejected per row for a data error are
    /// dead-lettered (with the event as payload and the SQLSTATE in the
    /// detail) and the batch is committed -- no retry could fix them.
    #[tokio::test]
    async fn per_row_rejections_are_dead_lettered_and_the_batch_commits() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |events| {
                assert_eq!(events.len(), 1);
                Ok(vec![rejected_row(0)])
            },
        )
        .await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(feed.committed_count, 1);
        assert!(
            feed.rejected_batches.is_empty(),
            "not a whole-batch rejection"
        );
        assert_eq!(feed.dead_lettered.len(), 1);
        assert_eq!(feed.dead_lettered[0].reason, "rejected_by_api");
        assert!(
            feed.dead_lettered[0]
                .detail
                .starts_with("23514 check_violation")
        );
        let payload: common::TrainMovementEventMessage =
            serde_json::from_str(&feed.dead_lettered[0].payload).expect("payload is the event");
        assert_eq!(payload.tracked_train_id, 1);
    }

    /// DB2-2: if the rejected events cannot be dead-lettered, the batch is
    /// NOT committed (and its state is rolled back), so they are not lost.
    #[tokio::test]
    async fn a_failed_dead_letter_of_rejected_rows_commits_nothing() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        feed.fail_next_dead_letter = true;
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |_| Ok(vec![rejected_row(0)]),
        )
        .await;

        assert_eq!(outcome, Cycle::Failed);
        assert_eq!(feed.committed_count, 0);
        assert!(state.resolved.is_empty(), "rolled back like any failure");
    }

    /// PL-11: the startup gate keeps retrying a failing reference load --
    /// quickly, not on the 60 s reload interval -- and only returns (letting
    /// the caller start consuming) once a load has succeeded.
    #[tokio::test]
    async fn the_startup_gate_returns_only_after_a_successful_reference_load() {
        let attempts = std::cell::Cell::new(0u32);
        let retries = std::cell::Cell::new(0u32);
        let started = std::time::Instant::now();

        let refs = load_reference_until_ok(
            async || {
                attempts.set(attempts.get() + 1);
                if attempts.get() <= 4 {
                    anyhow::bail!("api is still starting");
                }
                Ok(vec![common::TrackedTrainRef {
                    id: 7,
                    service_date: "2026-08-28".parse().unwrap(),
                    pin_origin_crs: Some("WAT".to_string()),
                    pin_scheduled_departure: Some("2026-08-28T18:32:00Z".parse().unwrap()),
                    resolution_status: "pending".to_string(),
                    train_uid: None,
                    train_id: None,
                    trains_id: None,
                    destination_crs: None,
                }])
            },
            Duration::from_millis(5),
            Duration::from_millis(20),
            || retries.set(retries.get() + 1),
        )
        .await;

        assert_eq!(attempts.get(), 5);
        assert_eq!(retries.get(), 4, "every failure beats the heartbeat");
        assert_eq!(refs.len(), 1);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "retried on the short backoff (5+10+20+20 ms), not the reload interval"
        );
    }

    /// The other half of finding #8, and the one that made it a data-loss
    /// bug rather than just a stuck-batch bug: the GOOD entries sharing a
    /// batch with a bad one must still be processed and posted. Before the
    /// fix, the `?` on the first bad payload abandoned the whole batch --
    /// including a pin's resolving origin departure.
    #[tokio::test]
    async fn one_bad_payload_does_not_trap_the_good_entries_sharing_its_batch() {
        let mut feed = FakeMovementFeed::new(vec![vec![
            "{ not json".to_string(),
            ORIGIN_DEPARTURE.to_string(),
            r#"{"header":{"msg_type":"0003"},"body":{"missing":"everything"}}"#.to_string(),
        ]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |events| {
                assert_eq!(
                    events.len(),
                    1,
                    "the pinned train's origin departure must survive its batch-mates"
                );
                assert_eq!(events[0].tracked_train_id, 1);
                assert_eq!(events[0].resolved_train_id, Some("221832406".to_string()));
                Ok(Vec::new())
            },
        )
        .await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(feed.committed_count, 1);
    }

    /// **Finding #4's end-to-end regression test.** A batch whose POST fails
    /// is redelivered (Redis `reclaim_stale` replays an unacked batch into
    /// this same running process after 30 seconds). The redelivered attempt MUST still carry the one-time
    /// `resolved_train_uid`/`resolved_train_id` signal, because that is the
    /// only thing that ever flips the subscription to `'resolved'` in the
    /// database.
    ///
    /// Before the fix, `process_message` had already written
    /// `state.resolved` during the FAILED attempt, so the replay took the
    /// "already resolved" branch, `freshly_resolved` came back `false`, and
    /// the subscription stayed `'pending'` forever while the train was
    /// visibly running.
    #[tokio::test]
    async fn a_redelivered_batch_still_reports_the_resolution_after_a_failed_post() {
        // The same payload twice: what a replay looks like from this
        // module's point of view.
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
        ]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let failed = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |_| Err(anyhow::anyhow!("api is down")),
        )
        .await;
        assert_eq!(failed, Cycle::Failed);
        assert!(
            state.resolved.is_empty(),
            "a batch that never reached api must leave no resolution behind in memory either"
        );

        let retried = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |events| {
                assert_eq!(events.len(), 1);
                assert_eq!(
                    events[0].resolved_train_id,
                    Some("221832406".to_string()),
                    "the redelivered batch must still announce the resolution"
                );
                assert_eq!(events[0].tracked_train_id, 1);
                Ok(Vec::new())
            },
        )
        .await;
        assert_eq!(retried, Cycle::Committed);
        assert_eq!(
            state.resolved.get("221832406"),
            Some(&vec![1]),
            "and now it is a durable fact"
        );
    }

    /// The confirmed case must NOT be rolled back, or a train would
    /// re-announce its resolution on every later movement.
    #[tokio::test]
    async fn a_confirmed_batch_keeps_its_state_and_does_not_re_resolve() {
        let later_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![later_arrival.to_string()],
        ]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        assert_eq!(
            run_cycle(
                &mut feed,
                &reference,
                &mut state,
                &TEST_STANOX_CRS,
                async |_| Ok(Vec::new())
            )
            .await,
            Cycle::Committed
        );

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            async |events| {
                assert_eq!(events.len(), 1);
                assert_eq!(
                    events[0].resolved_train_id, None,
                    "a confirmed resolution is not re-announced by the next movement"
                );
                Ok(Vec::new())
            },
        )
        .await;
        assert_eq!(outcome, Cycle::Committed);
    }
}

/// Redis unreachable while trust-consumer starts (the Redis pod recreated
/// by the same rollout): same guarantee as movement-relay's
/// `livez_stays_ok_while_redis_is_unreachable`.
#[cfg(test)]
mod redis_outage_tests {
    use clap::Parser;

    use super::*;

    /// R-097: every `parse_envelope` series exists at 0 from startup.
    #[test]
    fn parse_envelope_counters_are_registered_at_zero() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, register_parse_envelope_counters);
        let rendered = handle.render();
        for msg_type in trust_schema::schema::ENVELOPE_FAILURE_MSG_TYPES {
            let series = format!(
                r#"distant_signal_trust_consumer_errors_total{{operation="parse_envelope",msg_type="{msg_type}"}} 0"#
            );
            assert!(
                rendered.contains(&series),
                "{series} missing from {rendered}"
            );
        }
    }

    /// The chart's `DistantSignalConsumerApiCallsFailing` sums exactly
    /// [`API_CALL_OPERATIONS`] for this consumer.
    #[test]
    fn the_chart_alerts_on_every_api_call_operation() {
        let template = std::fs::read_to_string(
            common::manifest_dir!()
                .join("../../charts/distant-signal/templates/prometheusrule.yaml"),
        )
        .unwrap();
        let entry = format!(
            r#"(list "trust_consumer" "trust-consumer" "{}")"#,
            API_CALL_OPERATIONS.join("|")
        );
        assert!(
            template.contains(&entry),
            "the chart template has no {entry}"
        );
    }

    /// A local port with nothing listening on it (bound, then released).
    fn closed_local_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn config(redis_url: &str) -> Config {
        let stanox_crs_file = common::manifest_dir!().join("../../reference-data/stanox-crs.csv");
        Config::try_parse_from([
            "trust-consumer",
            "--internal-oauth-token-url",
            "http://auth.example.com/token",
            "--internal-oauth-client-id",
            "client-id",
            "--internal-oauth-username",
            "svc-user",
            "--internal-oauth-password",
            "svc-pass",
            "--stanox-crs-file",
            stanox_crs_file.to_str().unwrap(),
            "--redis-url",
            redis_url,
        ])
        .expect("minimal required args should parse")
    }

    /// `GET path` against the health listener, as `(status, body)`.
    /// Retries briefly, since the listener binds in a spawned task.
    async fn get(port: u16, path: &str) -> (u16, String) {
        let http = reqwest::Client::new();
        let url = format!("http://127.0.0.1:{port}{path}");
        for _ in 0..100 {
            if let Ok(response) = http.get(&url).send().await {
                let status = response.status().as_u16();
                return (status, response.text().await.unwrap());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("health listener never came up");
    }

    /// While Redis is unreachable, `/livez` stays 200 -- well past the
    /// stall window -- and only readiness (`/healthz`) is 503. With
    /// redis-rs's default connection manager the connect sat in unlogged
    /// internal retries for minutes, beating nothing.
    #[tokio::test]
    async fn livez_stays_ok_while_redis_is_unreachable() {
        let health_port = closed_local_port();
        let config = config(&format!("redis://127.0.0.1:{}", closed_local_port()));
        assert_eq!(
            config.movement_feed_backend,
            MovementFeedBackend::RedisStream
        );
        let stall_after = Duration::from_secs(1);
        let (_ready, progress) = health_http::spawn_with_progress(
            format!("127.0.0.1:{health_port}"),
            "connected",
            "disconnected",
            stall_after,
        );

        let connecting = tokio::spawn(async move {
            connect_redis_feed(
                &config,
                common::backoff::Backoff::new(
                    Duration::from_millis(50),
                    Duration::from_millis(200),
                ),
                &progress,
            )
            .await
            .map(|_| ())
        });

        // 4x the stall window.
        for _ in 0..16 {
            assert_eq!(
                get(health_port, "/livez").await,
                (200, "alive".to_string()),
                "liveness must not depend on Redis"
            );
            assert_eq!(get(health_port, "/healthz").await.0, 503, "not ready");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(
            !connecting.is_finished(),
            "an unreachable Redis is waited for, not exited on"
        );

        // Control: the same window with nothing beating IS a stall, so the
        // loop above really was kept alive by the connect retries.
        connecting.abort();
        tokio::time::sleep(stall_after * 2).await;
        assert_eq!(
            get(health_port, "/livez").await,
            (503, "stalled".to_string())
        );
    }
}
