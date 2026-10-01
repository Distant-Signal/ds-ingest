//! `trust-backlog-consumer`: a third, independent Redis Streams consumer
//! group on the `movement-events` stream, retaining a short,
//! catalogued-line-scoped, key-journey-point-only backlog of TRUST
//! events for late-tracking pins. See
//! docs/superpowers/specs/2026-09-05-trust-event-backlog-design.md and
//! docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md.
//!
//! Loop shape mirrors `full-coverage-consumer/src/main.rs`'s own
//! multi-cadence-in-one-loop shape (stanox_crs reload / consume-and-filter
//! / batch POST, each on its own timer or per-iteration, all checked once
//! per loop) -- this crate needs no population/stats-write cadence of its
//! own, so it is simpler than that crate's loop, not a copy of it.

mod config;
mod crs_index;
mod process;
mod queries;
mod reasons;
mod stanox_crs;

use std::sync::RwLock;
use std::time::Duration;

use clap::Parser;
use config::Config;
use movement_feed::ActiveFeed;
use movement_feed::MovementFeed;
use movement_feed::redis_stream::RedisStreamMovementFeed;
use movement_feed::{DeadLetter, DeadLetterSink};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    common::logging::init("trust-backlog-consumer");
    let config = Config::parse();
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
    let (connection_state, progress) = health_http::spawn_with_progress(
        config.health_bind_url.clone(),
        "connected",
        "disconnected",
        Duration::from_secs(config.progress_stall_secs),
    );
    let http = common::ingest::consumer_http_client()?;
    let internal_oauth = config.internal_oauth.token_cache();
    let reasons_url = queries::train_reasons_url(&config.api_ingest_url);
    if reasons_url.is_none() {
        tracing::warn!(
            api_ingest_url = %config.api_ingest_url,
            "API_INGEST_URL does not end in /trust-event-backlog; TRUST reason codes will not be sent"
        );
    }

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
                common::redis_auth::redis_url_with_password(
                    &config.redis_url,
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

    loop {
        // 1. stanox_crs reload.
        if last_stanox_crs_reload.elapsed() >= stanox_crs_reload_interval {
            match queries::fetch_stanox_crs(&http, &config.stanox_crs_url, &internal_oauth).await {
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

        // 3. consume + filter + POST.
        let cycle_start = std::time::Instant::now();
        match feed.next_batch().await {
            Ok(batch) => {
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
                for raw in &batch {
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
                                if let Some(reason) =
                                    reasons::reason_message(&message, &process_state, today, now)
                                {
                                    reasons.push(reason);
                                }
                                if let Some(event) = process::process_message(
                                    &message,
                                    &mut process_state,
                                    &snapshot,
                                    &crs_index,
                                    today,
                                    now,
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
                // reason is enrichment). The common failure, `api` being
                // down, also fails the backlog POST below, which leaves the
                // batch un-ACKed, so the reasons are re-sent on redelivery.
                // The upsert is idempotent.
                if let Some(url) = reasons_url.as_deref()
                    && let Err(err) =
                        queries::post_train_reasons(&http, url, &internal_oauth, &reasons).await
                {
                    tracing::warn!(error = ?err, count = reasons.len(), "failed to post train reasons; continuing with the backlog batch");
                    metrics::counter!(
                        common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                        "operation" => "post_train_reasons"
                    )
                    .increment(1);
                }

                let delivery = deliver_batch(&mut feed, &events, &unparseable, async |events| {
                    queries::post_trust_event_backlog(
                        &http,
                        &config.api_ingest_url,
                        &internal_oauth,
                        events,
                    )
                    .await
                })
                .await;
                if matches!(
                    delivery,
                    Delivery::PostFailed | Delivery::Rejected | Delivery::DeadLetterFailed
                ) {
                    tokio::time::sleep(ERROR_BACKOFF).await;
                }
            }
            Err(err) => {
                tracing::error!(error = ?err, "error receiving from movement feed");
                metrics::counter!(
                    common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                    "operation" => "movement_feed_receive"
                )
                .increment(1);
                tokio::time::sleep(ERROR_BACKOFF).await;
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

const ERROR_BACKOFF: Duration = Duration::from_secs(2);

/// What [`deliver_batch`] did with one batch.
#[derive(Debug, PartialEq, Eq)]
enum Delivery {
    /// Posted (any rows `api` rejected were dead-lettered) and XACKed.
    Committed,
    /// The POST failed transiently (unreachable, timeout, 5xx, ...): nothing
    /// XACKed and nothing dead-lettered, so the batch is redelivered later --
    /// however long the outage lasts.
    PostFailed,
    /// `api` refused the whole batch's data (400/413/422): handed to
    /// `MovementFeed::reject_batch`, which narrows it down to the poison
    /// entry and dead-letters only that.
    Rejected,
    /// `api` rejected rows but they could not be dead-lettered: nothing
    /// XACKed, so the rejected rows are not lost. The retry re-posts the
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
/// row has already landed. So the batch is ACKed like any success, and the
/// rejected rows go to the dead-letter stream (see
/// `movement_feed::DeadLetterSink`) with the SQLSTATE and message, where an
/// operator can inspect them and re-inject them once the cause is fixed.
/// Only a failed POST (transient: `api` answers 500 for those) or a failed
/// dead-letter write leaves the batch un-ACKed. A POST that `api` refused
/// outright (400/413/422, see `common::ingest::classify_failure`) is handed
/// to `MovementFeed::reject_batch` instead.
///
/// `unparseable` (payloads in this batch that did not parse at all) are
/// dead-lettered first; if that fails, nothing is posted or ACKed.
async fn deliver_batch<F, P>(
    feed: &mut F,
    events: &[common::TrustBacklogEventMessage],
    unparseable: &[DeadLetter],
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
            tracing::error!(error = ?err, "failed to post trust-event-backlog batch; will retry next cycle");
            metrics::counter!(
                common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                "operation" => "post_batch"
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
                reason: "rejected_by_api",
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
            "reason" => "rejected_by_api"
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

#[cfg(test)]
mod rail_day_tests {
    use super::*;

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
        let crs_index: std::collections::HashSet<String> =
            ["WAT".to_string()].into_iter().collect();
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
    /// rejected a row is ACKed like any success, and the rejected row goes
    /// to the dead-letter sink with enough to recover it.
    #[tokio::test]
    async fn rejected_rows_are_dead_lettered_and_the_batch_is_acked() {
        let mut feed = feed_with_one_batch().await;
        let events = vec![event("0003", "good"), event("0009", "bad")];

        let outcome = deliver_batch(&mut feed, &events, &[], async |_| {
            Ok(common::TrustBacklogIngestResponse {
                upserted: 1,
                rejected: vec![rejection(1, "bad")],
            })
        })
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

        let outcome = deliver_batch(&mut feed, &events, &[], async |_| {
            Ok(common::TrustBacklogIngestResponse {
                upserted: 1,
                rejected: vec![],
            })
        })
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

        let outcome = deliver_batch(&mut feed, &events, &[], async |_| {
            Err(anyhow::anyhow!("ingestion POST failed: 500"))
        })
        .await;

        assert_eq!(outcome, Delivery::PostFailed);
        assert_eq!(feed.committed_count, 0);
        assert!(feed.dead_lettered.is_empty());
    }

    /// If the rejected rows cannot be stored, ACKing would lose them: the
    /// batch stays pending and is retried instead.
    #[tokio::test]
    async fn a_failed_dead_letter_write_leaves_the_batch_un_acked() {
        let mut feed = feed_with_one_batch().await;
        feed.fail_next_dead_letter = true;
        let events = vec![event("0003", "good"), event("0009", "bad")];

        let outcome = deliver_batch(&mut feed, &events, &[], async |_| {
            Ok(common::TrustBacklogIngestResponse {
                upserted: 1,
                rejected: vec![rejection(1, "bad")],
            })
        })
        .await;

        assert_eq!(outcome, Delivery::DeadLetterFailed);
        assert_eq!(feed.committed_count, 0);
    }

    /// PL-2: however many times the POST fails transiently, nothing is
    /// dead-lettered, rejected or ACKed -- the batch just stays pending.
    #[tokio::test]
    async fn a_transient_failure_never_dead_letters_however_often_it_repeats() {
        let mut feed = feed_with_one_batch().await;
        let events = vec![event("0003", "good")];
        for status in [500u16, 502, 503, 504, 401, 404, 429]
            .into_iter()
            .cycle()
            .take(300)
        {
            let outcome = deliver_batch(&mut feed, &events, &[], async |_| {
                Err(common::ingest::HttpStatusError {
                    prefix: "ingestion POST failed",
                    status: reqwest::StatusCode::from_u16(status).unwrap(),
                    body: String::new(),
                }
                .into())
            })
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
        let outcome = deliver_batch(&mut feed, &events, &[], async |_| {
            Err(common::ingest::HttpStatusError {
                prefix: "ingestion POST failed",
                status: reqwest::StatusCode::UNPROCESSABLE_ENTITY,
                body: "bad row".to_string(),
            }
            .into())
        })
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
            deliver_batch(&mut feed, &events, &[], async |events| {
                queries::post_trust_event_backlog(&client, &url, &tokens, events).await
            }),
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
