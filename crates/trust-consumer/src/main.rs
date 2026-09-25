//! `trust-consumer`: persistent Kafka consumer for Network Rail's TRUST
//! Train Movements feed (via RDM), filtered to exactly the currently
//! user-tracked `(train_uid, date)` set. NOT a cron-style poller -- see
//! docs/superpowers/plans/2026-08-28-train-tracking.md's Global
//! Constraints for why this crate isn't named `poller-trust`.

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
use feed::kafka::KafkaMovementFeed;
use movement_feed::ActiveFeed;
use movement_feed::redis_stream::RedisStreamMovementFeed;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();
    if config.metrics.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }
    let connection_state =
        health_http::spawn(config.health_bind_url.clone(), "connected", "disconnected");
    let http = reqwest::Client::new();
    let internal_oauth = config.internal_oauth.token_cache();

    let mut feed = match config.movement_feed_backend {
        MovementFeedBackend::Kafka => {
            ActiveFeed::Kafka(KafkaMovementFeed::connect(&config, connection_state)?)
        }
        MovementFeedBackend::RedisStream => ActiveFeed::RedisStream(
            Box::new(
                RedisStreamMovementFeed::connect(
                    &config.redis_url,
                    "trust-consumer",
                    "trust-consumer-1",
                    Duration::from_secs(config.redis_autoclaim_min_idle_secs),
                )
                .await?,
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
    let mut last_reference_reload = tokio::time::Instant::now() - reload_interval;

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

    loop {
        if last_reference_reload.elapsed() >= reload_interval {
            match queries::fetch_active_tracked_trains(
                &http,
                &config.api_tracked_trains_url,
                &internal_oauth,
            )
            .await
            {
                Ok(refs) => {
                    // Rebuilds the matchable pins AND rehydrates already-resolved
                    // train_ids, so a restart doesn't permanently lose trains
                    // whose origin departure has already been and gone.
                    process::apply_reference_reload(refs, &mut reference, &mut state);
                    // Same cadence, unrelated job: age out parked Activations
                    // that no live Movement can still claim, so the national
                    // Activation stream can't grow this map without bound.
                    //
                    // The CURRENT rail day, not `Utc::now().date_naive()`
                    // (finding #5): the pruning rule is now about how old an
                    // Activation's own observed rail day is, so both sides of
                    // that comparison have to be rail days on the same
                    // Europe/London 02:00 convention, or an Activation
                    // observed at 01:00 local would be compared against
                    // tomorrow's date.
                    process::prune_expired_activations(
                        &mut state.pending_activations,
                        common::rail_day::current_rail_day(chrono::Utc::now()),
                    );
                    last_reference_reload = tokio::time::Instant::now();
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to reload active tracked trains; retrying next cycle");
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

        // A no-op under the Kafka backend (ActiveFeed::check_gap returns
        // Ok(None) immediately for that variant) -- only meaningful once
        // this deployment has been cut over to Redis Streams (Deploy B).
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

        let outcome = run_cycle(
            &mut feed,
            &reference,
            &mut state,
            &stanox_crs,
            async |events| {
                queries::post_train_events(&http, &config.api_ingest_url, &internal_oauth, events)
                    .await?;
                let signals = process::build_forward_signals(
                    events,
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
                Ok(())
            },
        )
        .await;

        if outcome == Cycle::Failed {
            // Nothing here waits on anything: `run_once` returns as soon as
            // the feed hands over a batch, and every failure path above
            // skips the commit, so a persistently-down `api` or an erroring
            // feed would otherwise spin this loop at full speed -- hammering
            // `api` and the log for the whole outage. A flat, short pause is
            // enough to make that a trickle; it deliberately isn't
            // exponential or configurable, because the loop has no backlog
            // to drain (Kafka holds the backlog) and a fixed small delay
            // costs nothing once the outage clears.
            tokio::time::sleep(ERROR_BACKOFF).await;
        }
    }
}

/// How long to wait before retrying after a failed cycle. See its one use
/// site above for why a flat constant is the right shape here.
const ERROR_BACKOFF: Duration = Duration::from_secs(2);

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
async fn run_cycle<F, P>(
    feed: &mut F,
    reference: &process::Reference,
    state: &mut process::ProcessorState,
    stanox_crs: &std::sync::RwLock<stanox_crs::StanoxCrsTable>,
    post: P,
) -> Cycle
where
    F: MovementFeed,
    P: AsyncFnOnce(&[common::TrainMovementEventMessage]) -> anyhow::Result<()>,
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

    if let Err(err) = post(&events).await {
        tracing::error!(error = ?err, "failed to post train events; not committing this batch's offsets");
        metrics::counter!(
            common::metrics::metric_name("trust_consumer_errors_total"),
            "operation" => "post_train_events"
        )
        .increment(1);
        state.roll_back_batch();
        return Cycle::Failed;
    }

    if let Err(err) = feed.commit().await {
        tracing::error!(error = ?err, "failed to commit Kafka offsets");
        metrics::counter!(
            common::metrics::metric_name("trust_consumer_errors_total"),
            "operation" => "commit_offsets"
        )
        .increment(1);
        // Rolled back even though the post itself succeeded: an uncommitted
        // batch WILL be redelivered (Kafka: the seek-back in
        // `feed::kafka`; Redis: `reclaim_stale`), and the replay must build
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::LazyLock;

    use super::*;
    use crate::feed::FakeMovementFeed;

    /// The real, checked-in `reference-data/stanox-crs.csv`, mirroring
    /// `process.rs`'s own test fixture of the same name -- these tests
    /// depend on the real STANOX `"87212"` translating to `"WAT"` to match
    /// `one_pending_pin`'s pin.
    static TEST_STANOX_CRS: LazyLock<std::sync::RwLock<stanox_crs::StanoxCrsTable>> =
        LazyLock::new(|| {
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../reference-data/stanox-crs.csv");
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
                Ok(())
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
            async |_| Ok(()),
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
                Ok(())
            },
        )
        .await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(
            feed.committed_count, 1,
            "the poison payload must be acknowledged, not replayed forever"
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
                Ok(())
            },
        )
        .await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(feed.committed_count, 1);
    }

    /// **Finding #4's end-to-end regression test.** A batch whose POST fails
    /// is redelivered (Redis `reclaim_stale` replays an unacked batch into
    /// this same running process after 30 seconds; Kafka now seeks back to
    /// it). The redelivered attempt MUST still carry the one-time
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
                Ok(())
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
                async |_| Ok(())
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
                Ok(())
            },
        )
        .await;
        assert_eq!(outcome, Cycle::Committed);
    }
}
