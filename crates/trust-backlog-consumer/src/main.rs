//! `trust-backlog-consumer`: a third, independent Redis Streams consumer
//! group on the `movement-events` stream, retaining a short,
//! catalogued-line-scoped, key-journey-point-only backlog of TRUST
//! events for late-tracking pins. See
//! docs/superpowers/specs/2026-09-05-trust-event-backlog-design.md and
//! docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md.
//!
//! Loop shape mirrors `full-coverage-consumer/src/main.rs`'s own
//! multi-cadence-in-one-loop shape (`stanox_crs` reload / consume-and-filter
//! / batch POST, each on its own timer or per-iteration, all checked once
//! per loop) -- this crate needs no population/stats-write cadence of its
//! own, so it is simpler than that crate's loop, not a copy of it.

mod config;
mod crs_index;
mod process;
mod queries;
mod reasons;
mod sink;
mod stanox_crs;

use std::collections::HashSet;
use std::sync::RwLock;
use std::time::Duration;

use clap::Parser;
use config::{Config, IngestSink};
use movement_feed::ActiveFeed;
use movement_feed::MovementFeed;
use movement_feed::redis_stream::RedisStreamMovementFeed;
use movement_feed::{DeadLetter, DeadLetterSink};
use sink::{BacklogSink, DbSink, HttpSink, Operations};

/// Every `trust_backlog_consumer_errors_total` operation that is a failed
/// write or read through the sink (not a data rejection, which is
/// `post_rejected`), registered at 0 and summed by the chart's
/// `DistantSignalConsumerApiCallsFailing` alert (2026-10-01: ~2,200 failed
/// backlog POSTs raised nothing). `post_*` are the HTTP sink's, `db_*` the
/// DB sink's (ingest architecture plan 3b.2; `sink::Operations`). The
/// chart's template lists the same operations; a test below keeps the two
/// in step.
const API_CALL_OPERATIONS: &[&str] = &[
    "post_batch",
    "post_train_reasons",
    "reload_stanox_crs",
    "db_write",
    "db_write_reasons",
];

/// `pg_stat_activity.application_name` under `INGEST_SINK=db`.
const APPLICATION_NAME: &str = "distant-signal-trust-backlog-consumer";
/// Spec §6.6: pool 3, role limit 4. One batch is written at a time.
const DEFAULT_MAX_CONNECTIONS: u32 = 3;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    common::logging::init("trust-backlog-consumer");
    let config = Config::parse();
    common::metrics::ingest_sink_info(&common::metrics::value_enum_name(&config.ingest_sink));
    config.validate()?;
    if config.metrics.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }
    // Register the stream-gap counter at 0 so the DistantSignalStreamGap
    // alert can use a plain `increase()`: a counter that only appears on its
    // first increment has no increase to see.
    metrics::counter!(common::metrics::metric_name(
        "trust_backlog_consumer_stream_gap_detected_total"
    ))
    .increment(0);
    // Same for every parse_envelope series, for
    // DistantSignalTrustEnvelopeParseDrops (R-097).
    for msg_type in trust_schema::schema::ENVELOPE_FAILURE_MSG_TYPES {
        metrics::counter!(
            common::metrics::metric_name("trust_backlog_consumer_errors_total"),
            "operation" => "parse_envelope",
            "msg_type" => msg_type
        )
        .increment(0);
    }
    common::metrics::register_operation_counters(
        "trust_backlog_consumer_errors_total",
        API_CALL_OPERATIONS,
    );
    let (connection_state, progress) = health_http::spawn_with_progress(
        config.health_bind_url.clone(),
        "connected",
        "disconnected",
        Duration::from_secs(config.progress_stall_secs),
    );

    // INGEST_SINK=db: Postgres and the schema gate come before the first
    // read from the feed, so nothing is consumed that cannot be written.
    let pool = match config.ingest_sink {
        IngestSink::Http => None,
        IngestSink::Db => Some(connect_database(&config, &progress).await?),
    };

    // Built once: purely static-catalogue-derived, needs no reload at
    // runtime (config.lines doesn't change without a restart).
    let crs_index = crs_index::build_crs_index(&config.lines);

    // Wrapped in `ActiveFeed::RedisStream`, not used bare -- this is what
    // actually threads `connection_state` through to flip the /healthz
    // readiness flag and the `trust_backlog_consumer_ready` gauge on every
    // `next_batch` call, exactly `full-coverage-consumer/src/main.rs`'s own
    // established pattern for a Redis-Streams backend
    // (`crates/movement-feed/src/active_feed.rs`'s own `ActiveFeed::RedisStream`
    // variant already does this generically -- see that module's doc
    // comment).
    let mut feed: ActiveFeed = ActiveFeed::RedisStream(
        // Redis down at startup is waited for (each attempt logged, beating
        // progress so /livez stays 200); afterwards every Redis command is
        // bounded and a failure is retried by this loop. See
        // `common::redis_conn`.
        Box::new(
            RedisStreamMovementFeed::connect_until_ready(
                common::redis_auth::redis_url_with_credentials(
                    &config.redis_url,
                    config.redis_username.as_deref(),
                    config.redis_password.as_ref(),
                )?
                .expose(),
                "trust-event-backlog",
                "trust-event-backlog-1",
                Duration::from_secs(config.redis_autoclaim_min_idle_secs),
                common::startup::CONNECT_BACKOFF,
                &progress,
            )
            .await?,
        ),
        connection_state,
        "trust_backlog_consumer_ready",
    );

    match pool {
        None => {
            let ingest_url = config.api_ingest_url.clone();
            let reasons_url = queries::train_reasons_url(&ingest_url);
            if reasons_url.is_none() {
                tracing::warn!(
                    api_ingest_url = %ingest_url,
                    "API_INGEST_URL does not end in /trust-event-backlog; TRUST reason codes will not be sent"
                );
            }
            let sink = HttpSink {
                client: common::ingest::consumer_http_client()?,
                ingest_url,
                reasons_url,
                stanox_crs_url: config.stanox_crs_url.clone(),
                tokens: config.internal_oauth.token_cache(),
            };
            consume(&config, &crs_index, &mut feed, &progress, &sink).await
        }
        Some(pool) => {
            tracing::info!("INGEST_SINK=db: writing the TRUST backlog to Postgres directly");
            sink::register_db_write_metrics();
            consume(&config, &crs_index, &mut feed, &progress, &DbSink { pool }).await
        }
    }
}

/// `INGEST_SINK=db`: waits for Postgres (INF-5), connects the pool (with
/// the `db_pool_*` metrics) and passes the schema gate as the
/// `trust_backlog` role (spec §12.2). The api's CORPUS fallback setting
/// (`CORPUS_FALLBACK_ENABLED`, which the chart passes on) is read so
/// `list_stanox_crs` returns what the api's `GET /private/stanox-crs` does.
async fn connect_database(
    config: &Config,
    progress: &health_http::Progress,
) -> anyhow::Result<sqlx::PgPool> {
    let url = config
        .database_url
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("INGEST_SINK=db needs DATABASE_URL"))?;
    ds_store::corpus::init_fallback_from_env()?;
    common::startup::retry_until_ready(
        "Postgres",
        common::startup::CONNECT_BACKOFF,
        Some(progress),
        || async {
            use sqlx::Connection;
            sqlx::PgConnection::connect(url.expose())
                .await?
                .close()
                .await
        },
    )
    .await;
    ds_store::pool::register_metrics();
    let pool = ds_store::pool::PoolSettings::from_env(APPLICATION_NAME, DEFAULT_MAX_CONNECTIONS)?
        .connect(url.expose())
        .await?;
    ds_store::schema::wait_for_schema(
        &pool,
        ds_store::schema::DbRole::TrustBacklog,
        Some(progress),
    )
    .await?;
    Ok(pool)
}

/// The consume loop, over either sink. Never returns.
#[expect(
    clippy::expect_used,
    clippy::too_many_lines,
    reason = "a poisoned lock means another thread already panicked; long but linear; splitting it would scatter its shared state across helpers"
)]
async fn consume<S: BacklogSink>(
    config: &Config,
    crs_index: &HashSet<String>,
    feed: &mut ActiveFeed,
    progress: &health_http::Progress,
    sink: &S,
) -> anyhow::Result<()> {
    let operations = S::OPERATIONS;
    let stanox = RwLock::new(config.stanox_crs.clone());
    let mut process_state = process::ProcessorState {
        trust_timestamp_correction_enabled: config.trust_timestamp_correction_enabled,
        ..process::ProcessorState::default()
    };

    let stanox_crs_reload_interval = Duration::from_secs(config.stanox_crs_reload_secs);
    let mut last_stanox_crs_reload = tokio::time::Instant::now() - stanox_crs_reload_interval;
    // Which rail day the parked-Activation maps were last aged out for
    // (finding #5). Pruning is a retain over two maps that can hold the whole
    // day's national Activation stream, so it runs when the rail day actually
    // rolls over rather than on every cycle -- within one rail day those maps
    // only grow by that day's own activations, which is exactly what they are
    // for.
    let mut last_pruned_rail_day: Option<chrono::NaiveDate> = None;
    let redis_gap_check_interval = Duration::from_secs(config.redis_gap_check_secs);
    let mut last_redis_gap_check = tokio::time::Instant::now() - redis_gap_check_interval;

    // Consecutive failed deliveries and feed reads; see `delivery_wait`.
    let mut delivery_failures =
        common::backoff::FailureStreak::new(delivery_backoff(config.ingest_sink));
    let mut feed_failures = common::backoff::FailureStreak::new(FEED_RETRY_BACKOFF);

    loop {
        // 1. stanox_crs reload.
        if last_stanox_crs_reload.elapsed() >= stanox_crs_reload_interval {
            match sink.stanox_crs().await {
                Ok(records) if !records.is_empty() => {
                    *stanox.write().expect("stanox lock poisoned") =
                        stanox_crs::StanoxCrsTable::from_records(records);
                }
                Ok(_) => {
                    tracing::warn!(
                        "live stanox_crs table is empty; keeping the currently loaded table"
                    );
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to reload stanox_crs table; keeping previous snapshot");
                    metrics::counter!(
                        common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                        "operation" => "reload_stanox_crs"
                    )
                    .increment(1);
                }
            }
            last_stanox_crs_reload = tokio::time::Instant::now();
        }

        // 2. redis-stream gap check.
        if last_redis_gap_check.elapsed() >= redis_gap_check_interval {
            match feed.check_gap().await {
                Ok(Some(gap)) => {
                    tracing::error!(
                        last_delivered = %gap.group_last_delivered_id,
                        new_first_entry = %gap.stream_first_entry_id,
                        "movement-events stream gap detected: some events between these IDs were \
                         trimmed before trust-backlog-consumer ever read them"
                    );
                    metrics::counter!(common::metrics::metric_name(
                        "trust_backlog_consumer_stream_gap_detected_total"
                    ))
                    .increment(1);
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(error = ?err, "failed to check movement-events stream for a gap");
                }
            }
            last_redis_gap_check = tokio::time::Instant::now();
        }

        // 3. consume + filter + write.
        let cycle_start = std::time::Instant::now();
        match feed.next_batch().await {
            Ok(batch) => {
                feed_failures.succeeded();
                let now = chrono::Utc::now();
                let today = current_rail_day(now);
                // Age out parked Activations once per rail day (finding #5):
                // before this, `pending_service_dates`/`pending_train_uids`
                // were never pruned at all, so they grew without bound AND
                // could misfile a recycled `train_id`'s movements under a
                // long-dead train's service_date/train_uid. See
                // `process::prune_stale_activations`.
                if last_pruned_rail_day != Some(today) {
                    let before = process_state.pending_service_dates.len();
                    process::prune_stale_activations(&mut process_state, today);
                    let dropped = before - process_state.pending_service_dates.len();
                    if dropped > 0 {
                        tracing::info!(
                            dropped,
                            retained = process_state.pending_service_dates.len(),
                            "pruned parked Activations that no live train can still be matched to"
                        );
                    }
                    last_pruned_rail_day = Some(today);
                }
                let snapshot = stanox.read().expect("stanox lock poisoned").clone();
                let mut events = Vec::new();
                let mut reasons = Vec::new();
                let mut unparseable = Vec::new();
                for entry in &batch {
                    let raw = &entry.payload;
                    // `now`/`today` above stay the wall clock, for pruning;
                    // each message is dated by when it arrived (`arrival`).
                    let (received_at, arrival_rail_day) = arrival(entry, now);
                    match trust_schema::schema::parse_batch_detailed(raw) {
                        Ok(parsed) => {
                            // PL-8: count every envelope the parser dropped.
                            for failure in &parsed.failures {
                                metrics::counter!(
                                    common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                                    "operation" => "parse_envelope",
                                    "msg_type" => failure.msg_type.clone()
                                )
                                .increment(1);
                            }
                            for message in parsed.messages {
                                if let Some(reason) = reasons::reason_message(
                                    &message,
                                    &process_state,
                                    arrival_rail_day,
                                    received_at,
                                ) {
                                    reasons.push(reason);
                                }
                                if let Some(event) = process::process_message(
                                    &message,
                                    &mut process_state,
                                    &snapshot,
                                    crs_index,
                                    arrival_rail_day,
                                    received_at,
                                ) {
                                    events.push(event);
                                }
                            }
                        }
                        Err(err) => {
                            tracing::error!(error = ?err, raw = %raw, "failed to parse TRUST batch; dead-lettering this payload");
                            metrics::counter!(
                                common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                                "operation" => "parse_batch"
                            )
                            .increment(1);
                            unparseable.push(unparseable_payload(raw, &err));
                        }
                    }
                }

                // Reasons first, best-effort: a failure is logged and
                // counted, never allowed to hold up the backlog batch (a
                // reason is enrichment). The common failure, `api` or
                // Postgres being down, also fails the backlog write below,
                // which leaves the batch un-ACKed, so the reasons are
                // re-sent on redelivery. The upsert is idempotent.
                if let Err(err) = sink.write_reasons(&reasons).await {
                    tracing::warn!(error = ?err, count = reasons.len(), "failed to write train reasons; continuing with the backlog batch");
                    metrics::counter!(
                        common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                        "operation" => operations.write_reasons
                    )
                    .increment(1);
                }

                // api's Retry-After when the POST got a 503 (its database is
                // unavailable), for `delivery_wait`. Always `None` for the
                // DB sink.
                let mut retry_after = None;
                let delivery =
                    deliver_batch(feed, &events, &unparseable, &operations, async |events| {
                        let written = sink.write_backlog(events).await;
                        if let Err(err) = &written {
                            retry_after = common::ingest::retry_after(err);
                        }
                        written
                    })
                    .await;
                if let Some(wait) = delivery_wait(&delivery, &mut delivery_failures, retry_after) {
                    tracing::warn!(
                        failures = delivery_failures.failures(),
                        retry_in_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                        "trust-event-backlog batch not delivered; backing off before the redelivery"
                    );
                    // Beats while it waits: a long backoff is not a stall.
                    progress.idle(tokio::time::sleep(wait)).await;
                }
            }
            Err(err) => {
                tracing::error!(error = ?err, "error receiving from movement feed");
                metrics::counter!(
                    common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                    "operation" => "movement_feed_receive"
                )
                .increment(1);
                progress
                    .idle(tokio::time::sleep(feed_failures.failed(None)))
                    .await;
            }
        }
        metrics::histogram!(common::metrics::metric_name(
            "trust_backlog_consumer_cycle_duration_seconds"
        ))
        .record(cycle_start.elapsed().as_secs_f64());
        // One loop iteration completed, however it went -- see
        // `health_http::Progress`.
        progress.beat();
    }
}

/// A payload that does not parse as TRUST can never succeed on retry: it is
/// dead-lettered (not silently dropped) so it can be recovered once
/// whatever produced it, or the parser, is fixed.
fn unparseable_payload(raw: &str, err: &impl std::fmt::Debug) -> DeadLetter {
    DeadLetter {
        reason: "unparseable_payload",
        source_id: None,
        delivery_count: None,
        payload: raw.to_string(),
        detail: format!("{err:?}"),
    }
}

/// Wait after a batch that was not delivered (POST failed, `api` rejected
/// it, or a dead-letter write failed): 2s doubling to 60s, jittered, and at
/// least `api`'s `Retry-After` on a 503. It used to be a flat 2s, so through
/// the 2026-10-01 Postgres outage this consumer re-POSTed its pending batch
/// every 2s for six hours (~2,200 failures).
const DELIVERY_RETRY_BACKOFF: common::backoff::Backoff =
    common::backoff::Backoff::new(Duration::from_secs(2), Duration::from_secs(60));

/// The same wait for the DB sink: 1s doubling to 60s, jittered (spec R1,
/// plan 3b.1). A transient DB failure pauses reading; the batch stays
/// pending in `movement-events`, which holds about 28 hours.
const DB_DELIVERY_RETRY_BACKOFF: common::backoff::Backoff =
    common::backoff::Backoff::new(Duration::from_secs(1), Duration::from_secs(60));

/// The delivery backoff for `sink`.
const fn delivery_backoff(sink: IngestSink) -> common::backoff::Backoff {
    match sink {
        IngestSink::Http => DELIVERY_RETRY_BACKOFF,
        IngestSink::Db => DB_DELIVERY_RETRY_BACKOFF,
    }
}

/// Wait after a failed read from the movement feed (Redis): 1s doubling to
/// 30s, jittered.
const FEED_RETRY_BACKOFF: common::backoff::Backoff =
    common::backoff::Backoff::new(Duration::from_secs(1), Duration::from_secs(30));

/// How long to wait after `delivery` before the next batch, if at all: a
/// committed batch resets the streak; a batch left pending (nothing
/// `XACKed`, so it is redelivered: at-least-once) waits the streak's next
/// backoff, honouring `retry_after`. A failed XACK does not wait: the batch
/// landed, and its redelivery is a harmless duplicate.
fn delivery_wait(
    delivery: &Delivery,
    failures: &mut common::backoff::FailureStreak,
    retry_after: Option<Duration>,
) -> Option<Duration> {
    match delivery {
        Delivery::Committed => {
            failures.succeeded();
            None
        }
        Delivery::CommitFailed => None,
        Delivery::PostFailed | Delivery::Rejected | Delivery::DeadLetterFailed => {
            Some(failures.failed(retry_after))
        }
    }
}

/// What [`deliver_batch`] did with one batch.
#[derive(Debug, PartialEq, Eq)]
enum Delivery {
    /// Written (any rows the sink rejected were dead-lettered) and `XACKed`.
    Committed,
    /// The write failed transiently (HTTP: unreachable, timeout, 5xx, ...;
    /// DB: anything but a per-row data error, the transaction rolled back):
    /// nothing `XACKed` and nothing dead-lettered, so the batch is
    /// redelivered later -- however long the outage lasts.
    PostFailed,
    /// `api` refused the whole batch's data (400/413/422): handed to
    /// `MovementFeed::reject_batch`, which narrows it down to the poison
    /// entry and dead-letters only that.
    Rejected,
    /// `api` rejected rows but they could not be dead-lettered: nothing
    /// `XACKed`, so the rejected rows are not lost. The retry re-posts the
    /// batch; the good rows then conflict harmlessly on `dedup_key`.
    DeadLetterFailed,
    /// Posted, but the XACK itself failed; the batch will be redelivered
    /// and re-posted harmlessly.
    CommitFailed,
}

/// Post -> dead-letter rejected rows -> XACK, extracted from the main loop
/// so it can be tested against `FakeMovementFeed` without Redis or `api`.
///
/// **Rejected rows no longer hold a batch hostage.** When `api` answers 2xx
/// with a non-empty `rejected` list, those rows failed a constraint or were
/// invalid input and will fail identically on every retry, while every other
/// row has already landed. So the batch is `ACKed` like any success, and the
/// rejected rows go to the dead-letter stream (see
/// `movement_feed::DeadLetterSink`) with the SQLSTATE and message, where an
/// operator can inspect them and re-inject them once the cause is fixed.
/// Only a failed POST (transient: `api` answers 500 for those) or a failed
/// dead-letter write leaves the batch un-ACKed. A POST that `api` refused
/// outright (400/413/422, see `common::ingest::classify_failure`) is handed
/// to `MovementFeed::reject_batch` instead.
///
/// `unparseable` (payloads in this batch that did not parse at all) are
/// dead-lettered first; if that fails, nothing is posted or `ACKed`.
///
/// The same for the DB sink (ingest architecture plan 3b.1): `post` is
/// `sink::BacklogSink::write_backlog`, which returns only after its
/// transaction committed, with the same `rejected` rows the api's route
/// reports. `operations` names the counters and the dead-letter reason.
async fn deliver_batch<F, P>(
    feed: &mut F,
    events: &[common::TrustBacklogEventMessage],
    unparseable: &[DeadLetter],
    operations: &Operations,
    post: P,
) -> Delivery
where
    F: MovementFeed + DeadLetterSink,
    P: AsyncFnOnce(
        &[common::TrustBacklogEventMessage],
    ) -> anyhow::Result<common::TrustBacklogIngestResponse>,
{
    if let Err(err) = feed.dead_letter(unparseable).await {
        tracing::error!(error = ?err, "failed to dead-letter unparseable payloads; leaving the batch un-ACKed to retry");
        metrics::counter!(
            common::metrics::metric_name("trust_backlog_consumer_errors_total"),
            "operation" => "dead_letter"
        )
        .increment(1);
        return Delivery::DeadLetterFailed;
    }

    let response = match post(events).await {
        Ok(response) => response,
        Err(err)
            if common::ingest::classify_failure(&err) == common::ingest::FailureClass::Rejected =>
        {
            tracing::error!(error = ?err, "api rejected the trust-event-backlog batch's data; isolating the poison entry");
            metrics::counter!(
                common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                "operation" => "post_rejected"
            )
            .increment(1);
            if let Err(reject_err) = feed.reject_batch(&err.to_string()).await {
                tracing::error!(error = ?reject_err, "failed to handle the rejected batch; it stays pending");
                return Delivery::DeadLetterFailed;
            }
            return Delivery::Rejected;
        }
        Err(err) => {
            tracing::error!(error = ?err, "failed to write the trust-event-backlog batch; will retry next cycle");
            metrics::counter!(
                common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                "operation" => operations.write
            )
            .increment(1);
            // Deliberately does NOT commit on a failed post -- same
            // "only ack after a successful downstream write" posture as
            // trust-consumer's own main loop, since this consumer's whole
            // reason to exist is not losing events a late-tracking pin
            // might need.
            return Delivery::PostFailed;
        }
    };

    if !response.rejected.is_empty() {
        let records: Vec<DeadLetter> = response
            .rejected
            .iter()
            .map(|rejected| DeadLetter {
                reason: operations.rejected,
                source_id: None,
                delivery_count: None,
                payload: events
                    .get(rejected.index)
                    .and_then(|event| serde_json::to_string(event).ok())
                    .unwrap_or_default(),
                detail: format!(
                    "{} {} (constraint {}): {} [dedup_key {}]",
                    rejected.sqlstate,
                    rejected.reason,
                    rejected.constraint.as_deref().unwrap_or("-"),
                    rejected.message,
                    rejected.dedup_key,
                ),
            })
            .collect();
        if let Err(err) = feed.dead_letter(&records).await {
            tracing::error!(
                error = ?err,
                rejected = records.len(),
                "failed to dead-letter rows api rejected; leaving the batch un-ACKed to retry"
            );
            metrics::counter!(
                common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                "operation" => "dead_letter"
            )
            .increment(1);
            return Delivery::DeadLetterFailed;
        }
        metrics::counter!(
            common::metrics::metric_name("trust_backlog_consumer_deadlettered_total"),
            "reason" => operations.rejected
        )
        .increment(records.len() as u64);
    }
    metrics::counter!(common::metrics::metric_name(
        "trust_backlog_consumer_events_stored_total"
    ))
    .increment(events.len().saturating_sub(response.rejected.len()) as u64);

    if let Err(err) = feed.commit().await {
        tracing::error!(error = ?err, "failed to commit Redis Streams offsets");
        metrics::counter!(
            common::metrics::metric_name("trust_backlog_consumer_errors_total"),
            "operation" => "commit_offsets"
        )
        .increment(1);
        return Delivery::CommitFailed;
    }
    Delivery::Committed
}

/// The Europe/London rail day `at` falls on -- the calendar date the
/// process.rs/migration doc comments already promise ("falls back to the
/// current Europe/London rail day"), NOT a bare UTC calendar date. Using
/// `chrono::Utc::now().date_naive()` directly would be plain UTC and
/// ignore both the Europe/London timezone offset AND this codebase's own
/// established 02:00 rail-day cutoff convention.
///
/// Now a one-line delegation to `common::rail_day::current_rail_day`. This
/// used to be a crate-local duplication of that 02:00 cutoff, with its own
/// doc comment naming "add it to `common::rail_day` instead" as the right
/// follow-up; the 2026-09-25 review's findings #2 and #5 gave
/// `trust-consumer` the same need (dating an Activation, and ageing parked
/// Activations out against the current rail day), so the shared version now
/// exists and a third copy would be indefensible. Kept as a named wrapper
/// rather than inlined at the call site purely so this module's own
/// `rail_day_tests` keep testing the behaviour this crate depends on.
fn current_rail_day(at: chrono::DateTime<chrono::Utc>) -> chrono::NaiveDate {
    common::rail_day::current_rail_day(at)
}

/// The instant and rail day a feed entry's messages are dated by
/// (`process::process_message`'s `received_at`/`today`): when the entry
/// ARRIVED -- the relay's `XADD`, see `movement_feed::FeedEntry::received_at`
/// -- not when this possibly-lagging consumer reads it. Otherwise an
/// Activation with no `tp_origin_timestamp` that arrived at 23:59 London
/// would be filed under the next day's `service_date` when processed after
/// midnight, and a message with no date of its own that arrived at 01:59
/// would get the next rail day as its `event_date` when processed after
/// 02:00. `now` stands in only when the arrival time is unknown.
fn arrival(
    entry: &movement_feed::FeedEntry,
    now: chrono::DateTime<chrono::Utc>,
) -> (chrono::DateTime<chrono::Utc>, chrono::NaiveDate) {
    let received_at = entry.received_at_or(now);
    (received_at, current_rail_day(received_at))
}

#[cfg(test)]
mod rail_day_tests {
    use super::*;

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
            r#"(list "trust_backlog_consumer" "trust-backlog-consumer" "{}")"#,
            API_CALL_OPERATIONS.join("|")
        );
        assert!(
            template.contains(&entry),
            "the chart template has no {entry}"
        );
    }

    #[test]
    fn well_after_the_0200_cutoff_is_that_calendar_days_rail_day() {
        let at: chrono::DateTime<chrono::Utc> = "2026-09-05T13:00:00Z".parse().unwrap();
        assert_eq!(
            current_rail_day(at),
            "2026-09-05".parse::<chrono::NaiveDate>().unwrap()
        );
    }

    #[test]
    fn just_before_the_0200_cutoff_is_still_the_previous_calendar_days_rail_day() {
        // 00:30 UTC = 01:30 BST (September is daylight saving), clearly
        // before the 02:00 Europe/London cutoff.
        let at: chrono::DateTime<chrono::Utc> = "2026-09-05T00:30:00Z".parse().unwrap();
        assert_eq!(
            current_rail_day(at),
            "2026-09-04".parse::<chrono::NaiveDate>().unwrap()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_with_a_pass_and_a_departure_keeps_only_the_departure() {
        let activation_and_movements = r#"[
            {"header":{"msg_type":"0003"},"body":{
                "train_id":"221832406","event_type":"PASS",
                "planned_timestamp":"1787941920000","actual_timestamp":"1787941920000",
                "loc_stanox":"87212","variation_status":"ON TIME"
            }},
            {"header":{"msg_type":"0003"},"body":{
                "train_id":"221832406","event_type":"DEPARTURE",
                "planned_timestamp":"1787942000000","actual_timestamp":"1787942000000",
                "loc_stanox":"87212","variation_status":"ON TIME"
            }}
        ]"#;
        let messages = trust_schema::schema::parse_batch(activation_and_movements).unwrap();

        let stanox = stanox_crs::StanoxCrsTable::from_records(vec![common::StanoxCrsRecord {
            stanox: "87212".to_string(),
            crs: "WAT".to_string(),
            tiploc: "WATRLMN".to_string(),
            station_name: "LONDON WATERLOO".to_string(),
            source_sequence: 1,
            change_time_minutes: None,
        }]);
        let crs_index: HashSet<String> = ["WAT".to_string()].into_iter().collect();
        let mut state = process::ProcessorState::default();
        let today: chrono::NaiveDate = "2026-09-05".parse().unwrap();
        // Safely after the raw timestamp fixtures below (2026-08-28) -- this
        // test isn't about the plausibility guard, see
        // `process.rs`'s own `test_received_at` for that coverage.
        let received_at: chrono::DateTime<chrono::Utc> = "2099-01-01T00:00:00Z".parse().unwrap();

        let events: Vec<_> = messages
            .iter()
            .filter_map(|m| {
                process::process_message(m, &mut state, &stanox, &crs_index, today, received_at)
            })
            .collect();

        assert_eq!(
            events.len(),
            1,
            "the PASS event must be dropped, only the DEPARTURE kept"
        );
        assert_eq!(events[0].event_type, Some("DEPARTURE".to_string()));
    }

    fn utc(s: &str) -> chrono::DateTime<chrono::Utc> {
        s.parse().unwrap()
    }

    fn entry_arrived_at(at: &str) -> movement_feed::FeedEntry {
        movement_feed::FeedEntry {
            payload: String::new(),
            received_at: Some(utc(at)),
        }
    }

    /// A message that arrived at 01:59 London but is processed at 02:05 (a
    /// lagging consumer) is dated by the earlier rail day it arrived in.
    #[test]
    fn an_entry_that_arrived_before_0200_is_dated_by_the_earlier_rail_day() {
        // 00:59Z / 01:05Z on 2026-10-01 are 01:59 / 02:05 BST.
        let (received_at, rail_day) = arrival(
            &entry_arrived_at("2026-10-01T00:59:00Z"),
            utc("2026-10-01T01:05:00Z"),
        );
        assert_eq!(received_at, utc("2026-10-01T00:59:00Z"));
        assert_eq!(rail_day, "2026-09-30".parse::<chrono::NaiveDate>().unwrap());
    }

    #[test]
    fn an_entry_with_no_known_arrival_time_is_dated_by_now() {
        let now = utc("2026-10-01T01:05:00Z");
        let (received_at, rail_day) = arrival(&movement_feed::FeedEntry::new("p"), now);
        assert_eq!(received_at, now);
        assert_eq!(rail_day, "2026-10-01".parse::<chrono::NaiveDate>().unwrap());
    }

    /// End to end through `process_message`: an Activation with no
    /// `tp_origin_timestamp` that arrived at 23:59 London, processed at
    /// 00:05, keeps the `service_date` of the day it arrived on (processing
    /// time would put it in the post-midnight window and file it a day
    /// later).
    #[test]
    fn a_lagging_activation_keeps_the_service_date_of_its_arrival() {
        let activation =
            trust_schema::schema::TrustMessage::Activation(trust_schema::schema::Activation {
                train_id: "221832406".to_string(),
                train_uid: "C21373".to_string(),
                toc_id: Some("SW".to_string()),
                train_service_code: None,
                schedule_wtt_id: None,
                schedule_start_date: Some("2026-08-01".to_string()),
                schedule_end_date: Some("2026-12-01".to_string()),
                tp_origin_timestamp: None,
            });
        let stanox = stanox_crs::StanoxCrsTable::from_records(Vec::new());
        let crs_index = HashSet::new();
        // 22:59Z on 2026-09-30 is 23:59 BST; 23:05Z is 00:05 BST on 10-01.
        let (received_at, rail_day) = arrival(
            &entry_arrived_at("2026-09-30T22:59:00Z"),
            utc("2026-09-30T23:05:00Z"),
        );
        let event = process::process_message(
            &activation,
            &mut process::ProcessorState::default(),
            &stanox,
            &crs_index,
            rail_day,
            received_at,
        )
        .unwrap();
        assert_eq!(
            event.service_date,
            "2026-09-30".parse::<chrono::NaiveDate>().unwrap()
        );
    }
}

#[cfg(test)]
mod deliver_batch_tests {
    use movement_feed::FakeMovementFeed;

    use super::*;

    fn event(msg_type: &str, dedup_key: &str) -> common::TrustBacklogEventMessage {
        common::TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            train_uid: None,
            train_id: "221832406".to_string(),
            service_date: "2026-09-05".parse().unwrap(),
            msg_type: msg_type.to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            dedup_key: dedup_key.to_string(),
            gbtt_timestamp: None,
        }
    }

    /// A feed that has handed out one batch, so `commit` has something to
    /// confirm (see `FakeMovementFeed::committed_count`).
    async fn feed_with_one_batch() -> FakeMovementFeed {
        let mut feed = FakeMovementFeed::new(vec![vec!["entry".to_string()]]);
        feed.next_batch().await.unwrap();
        feed
    }

    fn rejection(index: usize, dedup_key: &str) -> common::RejectedTrustBacklogRow {
        common::RejectedTrustBacklogRow {
            index,
            dedup_key: dedup_key.to_string(),
            sqlstate: "23514".to_string(),
            reason: "check_violation".to_string(),
            constraint: Some("trust_event_backlog_msg_type_check".to_string()),
            message: "new row violates check constraint".to_string(),
        }
    }

    /// The production incident's fix, consumer half: a batch where `api`
    /// rejected a row is `ACKed` like any success, and the rejected row goes
    /// to the dead-letter sink with enough to recover it.
    #[tokio::test]
    async fn rejected_rows_are_dead_lettered_and_the_batch_is_acked() {
        let mut feed = feed_with_one_batch().await;
        let events = vec![event("0003", "good"), event("0009", "bad")];

        let outcome = deliver_batch(
            &mut feed,
            &events,
            &[],
            &sink::HTTP_OPERATIONS,
            async |_| {
                Ok(common::TrustBacklogIngestResponse {
                    upserted: 1,
                    rejected: vec![rejection(1, "bad")],
                })
            },
        )
        .await;

        assert_eq!(outcome, Delivery::Committed);
        assert_eq!(feed.committed_count, 1, "the batch must be ACKed");
        assert_eq!(feed.dead_lettered.len(), 1);
        let letter = &feed.dead_lettered[0];
        assert_eq!(letter.reason, "rejected_by_api");
        let payload: common::TrustBacklogEventMessage =
            serde_json::from_str(&letter.payload).expect("payload is the rejected row as JSON");
        assert_eq!(payload.dedup_key, "bad");
        assert_eq!(payload.msg_type, "0009");
        assert!(letter.detail.contains("23514"), "{}", letter.detail);
        assert!(
            letter.detail.contains("trust_event_backlog_msg_type_check"),
            "{}",
            letter.detail
        );
    }

    #[tokio::test]
    async fn a_clean_batch_is_acked_with_nothing_dead_lettered() {
        let mut feed = feed_with_one_batch().await;
        let events = vec![event("0003", "good")];

        let outcome = deliver_batch(
            &mut feed,
            &events,
            &[],
            &sink::HTTP_OPERATIONS,
            async |_| {
                Ok(common::TrustBacklogIngestResponse {
                    upserted: 1,
                    rejected: vec![],
                })
            },
        )
        .await;

        assert_eq!(outcome, Delivery::Committed);
        assert_eq!(feed.committed_count, 1);
        assert!(feed.dead_lettered.is_empty());
    }

    /// A transient failure (api answers 500) must still leave the batch
    /// un-ACKed so it is retried.
    #[tokio::test]
    async fn a_failed_post_is_not_acked_or_dead_lettered() {
        let mut feed = feed_with_one_batch().await;
        let events = vec![event("0003", "good")];

        let outcome = deliver_batch(
            &mut feed,
            &events,
            &[],
            &sink::HTTP_OPERATIONS,
            async |_| Err(anyhow::anyhow!("ingestion POST failed: 500")),
        )
        .await;

        assert_eq!(outcome, Delivery::PostFailed);
        assert_eq!(feed.committed_count, 0);
        assert!(feed.dead_lettered.is_empty());
    }

    /// If the rejected rows cannot be stored, `ACKing` would lose them: the
    /// batch stays pending and is retried instead.
    /// Consecutive undelivered batches wait longer and longer (2s, 4s, 8s,
    /// ... jittered, capped at 60s) instead of a flat 2s, at least api's
    /// Retry-After on a 503, and a delivered batch resets that.
    #[test]
    fn undelivered_batches_back_off_exponentially_and_reset_on_success() {
        let mut failures = common::backoff::FailureStreak::new(DELIVERY_RETRY_BACKOFF);
        let mut previous_ceiling = Duration::ZERO;
        for attempt in 0..8 {
            let wait = delivery_wait(&Delivery::PostFailed, &mut failures, None)
                .expect("a failed post waits");
            let ceiling = DELIVERY_RETRY_BACKOFF.ceiling(attempt);
            assert!(
                wait >= ceiling / 2 && wait <= ceiling,
                "{attempt}: {wait:?}"
            );
            assert!(ceiling >= previous_ceiling);
            previous_ceiling = ceiling;
        }
        assert_eq!(previous_ceiling, Duration::from_secs(60), "capped");
        let wait = delivery_wait(
            &Delivery::PostFailed,
            &mut failures,
            Some(Duration::from_secs(90)),
        )
        .unwrap();
        assert!(
            wait >= Duration::from_secs(90),
            "Retry-After honoured: {wait:?}"
        );
        assert!(delivery_wait(&Delivery::Rejected, &mut failures, None).is_some());
        assert!(delivery_wait(&Delivery::DeadLetterFailed, &mut failures, None).is_some());
        assert_eq!(
            delivery_wait(&Delivery::CommitFailed, &mut failures, None),
            None
        );
        assert_eq!(
            delivery_wait(&Delivery::Committed, &mut failures, None),
            None
        );
        assert_eq!(
            failures.failures(),
            0,
            "a delivered batch resets the streak"
        );
        let wait = delivery_wait(&Delivery::PostFailed, &mut failures, None).unwrap();
        assert!(wait <= Duration::from_secs(2), "{wait:?}");
    }

    /// A 503 + Retry-After from api reaches `delivery_wait` through the
    /// real POST, and the batch stays pending (at-least-once).
    #[tokio::test]
    async fn a_503_leaves_the_batch_pending_and_carries_retry_after() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // The OAuth token endpoint answers normally; the ingest POST gets
        // api's database-unavailable 503.
        let _server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let (status, extra, body) = if buf[..n].starts_with(b"POST /token/") {
                    (
                        "200 OK",
                        "",
                        r#"{"access_token":"fake-jwt","expires_in":300}"#,
                    )
                } else {
                    (
                        "503 Service Unavailable",
                        "retry-after: 30\r\n",
                        r#"{"error":"service_unavailable","retryable":true}"#,
                    )
                };
                let reply = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{extra}content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
            }
        });
        let client = reqwest::Client::new();
        let tokens =
            common::oauth_client::OAuthTokenCache::new(common::oauth_client::OAuthCredentials {
                token_url: format!("http://{addr}/token/"),
                client_id: "c".to_string(),
                scope: "groups".to_string(),
                username: "u".to_string(),
                password: "p".to_string(),
            });
        let url = format!("http://{addr}/private/trust-event-backlog");
        let mut feed = feed_with_one_batch().await;
        let events = vec![event("0003", "good")];
        let mut retry_after = None;
        let delivery = deliver_batch(
            &mut feed,
            &events,
            &[],
            &sink::HTTP_OPERATIONS,
            async |events| {
                let posted =
                    queries::post_trust_event_backlog(&client, &url, &tokens, events).await;
                if let Err(err) = &posted {
                    retry_after = common::ingest::retry_after(err);
                }
                posted
            },
        )
        .await;
        assert_eq!(delivery, Delivery::PostFailed);
        assert_eq!(feed.committed_count, 0, "nothing acked");
        assert!(feed.dead_lettered.is_empty(), "nothing dead-lettered");
        assert!(feed.rejected_batches.is_empty(), "a 503 is not a rejection");
        assert_eq!(retry_after, Some(Duration::from_secs(30)));
        let mut failures = common::backoff::FailureStreak::new(DELIVERY_RETRY_BACKOFF);
        let wait = delivery_wait(&delivery, &mut failures, retry_after).unwrap();
        assert!(wait >= Duration::from_secs(30), "{wait:?}");
    }

    #[tokio::test]
    async fn a_failed_dead_letter_write_leaves_the_batch_un_acked() {
        let mut feed = feed_with_one_batch().await;
        feed.fail_next_dead_letter = true;
        let events = vec![event("0003", "good"), event("0009", "bad")];

        let outcome = deliver_batch(
            &mut feed,
            &events,
            &[],
            &sink::HTTP_OPERATIONS,
            async |_| {
                Ok(common::TrustBacklogIngestResponse {
                    upserted: 1,
                    rejected: vec![rejection(1, "bad")],
                })
            },
        )
        .await;

        assert_eq!(outcome, Delivery::DeadLetterFailed);
        assert_eq!(feed.committed_count, 0);
    }

    /// PL-2: however many times the POST fails transiently, nothing is
    /// dead-lettered, rejected or `ACKed` -- the batch just stays pending.
    #[tokio::test]
    async fn a_transient_failure_never_dead_letters_however_often_it_repeats() {
        let mut feed = feed_with_one_batch().await;
        let events = vec![event("0003", "good")];
        for status in [500u16, 502, 503, 504, 401, 404, 429]
            .into_iter()
            .cycle()
            .take(300)
        {
            let outcome = deliver_batch(
                &mut feed,
                &events,
                &[],
                &sink::HTTP_OPERATIONS,
                async |_| {
                    Err(common::ingest::HttpStatusError {
                        prefix: "ingestion POST failed",
                        status: reqwest::StatusCode::from_u16(status).unwrap(),
                        body: String::new(),
                        retry_after: None,
                    }
                    .into())
                },
            )
            .await;
            assert_eq!(outcome, Delivery::PostFailed, "{status}");
        }
        assert_eq!(feed.committed_count, 0);
        assert!(feed.dead_lettered.is_empty());
        assert!(feed.rejected_batches.is_empty());
    }

    /// PL-2: an explicit data rejection of the batch goes to
    /// `reject_batch` (which isolates and dead-letters the poison entry).
    #[tokio::test]
    async fn a_422_hands_the_batch_to_reject_batch() {
        let mut feed = feed_with_one_batch().await;
        let events = vec![event("0003", "good")];
        let outcome = deliver_batch(
            &mut feed,
            &events,
            &[],
            &sink::HTTP_OPERATIONS,
            async |_| {
                Err(common::ingest::HttpStatusError {
                    prefix: "ingestion POST failed",
                    status: reqwest::StatusCode::UNPROCESSABLE_ENTITY,
                    body: "bad row".to_string(),
                    retry_after: None,
                }
                .into())
            },
        )
        .await;
        assert_eq!(outcome, Delivery::Rejected);
        assert_eq!(feed.committed_count, 0);
        assert_eq!(feed.rejected_batches.len(), 1);
        assert!(
            feed.rejected_batches[0].contains("422"),
            "{:?}",
            feed.rejected_batches
        );
    }

    /// Malformed input is dead-lettered, and the rest of the batch still
    /// posts and ACKs.
    #[tokio::test]
    async fn an_unparseable_payload_is_dead_lettered_and_the_batch_acked() {
        let mut feed = feed_with_one_batch().await;
        let raw = "{ not json";
        let err = trust_schema::schema::parse_batch(raw).unwrap_err();
        let events = vec![event("0003", "good")];
        let outcome = deliver_batch(
            &mut feed,
            &events,
            &[unparseable_payload(raw, &err)],
            &sink::HTTP_OPERATIONS,
            async |_| {
                Ok(common::TrustBacklogIngestResponse {
                    upserted: 1,
                    rejected: vec![],
                })
            },
        )
        .await;
        assert_eq!(outcome, Delivery::Committed);
        assert_eq!(feed.committed_count, 1);
        assert_eq!(feed.dead_lettered.len(), 1);
        assert_eq!(feed.dead_lettered[0].reason, "unparseable_payload");
        assert_eq!(feed.dead_lettered[0].payload, raw);
    }

    /// PL-1: an `api` that accepts the connection and never answers times
    /// out as a transient failure: the entry stays pending (no ACK, no
    /// dead-letter), instead of wedging the consumer forever.
    #[tokio::test]
    async fn a_hung_api_times_out_and_the_batch_stays_pending() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Answers the OAuth token request normally, then accepts the
        // ingest POST and never replies -- a half-open `api`.
        let _server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut held = Vec::new();
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                if buf[..n].starts_with(b"POST /token/") {
                    let body = r#"{"access_token":"fake-jwt","expires_in":300}"#;
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                } else {
                    held.push(socket);
                }
            }
        });
        let client = reqwest::Client::builder()
            .connect_timeout(common::ingest::CONSUMER_CONNECT_TIMEOUT)
            .timeout(Duration::from_millis(300))
            .build()
            .unwrap();
        let tokens =
            common::oauth_client::OAuthTokenCache::new(common::oauth_client::OAuthCredentials {
                token_url: format!("http://{addr}/token/"),
                client_id: "c".to_string(),
                scope: "groups".to_string(),
                username: "u".to_string(),
                password: "p".to_string(),
            });
        let mut feed = feed_with_one_batch().await;
        let events = vec![event("0003", "good")];
        let url = format!("http://{addr}/private/trust-event-backlog");

        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            deliver_batch(
                &mut feed,
                &events,
                &[],
                &sink::HTTP_OPERATIONS,
                async |events| {
                    queries::post_trust_event_backlog(&client, &url, &tokens, events).await
                },
            ),
        )
        .await
        .expect("the hung api must time out, not wedge the consumer");

        assert_eq!(outcome, Delivery::PostFailed);
        assert_eq!(feed.committed_count, 0);
        assert!(feed.dead_lettered.is_empty());
        assert!(feed.rejected_batches.is_empty());
    }

    /// An older `api` that only sends `upserted` still parses, as a clean
    /// success.
    #[test]
    fn an_old_api_response_without_rejected_parses() {
        let response: common::TrustBacklogIngestResponse =
            serde_json::from_str(r#"{"upserted": 3}"#).unwrap();
        assert_eq!(response.upserted, 3);
        assert!(response.rejected.is_empty());
    }
}
