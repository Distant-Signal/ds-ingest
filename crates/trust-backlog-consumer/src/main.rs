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
mod stanox_crs;

use std::sync::RwLock;
use std::time::Duration;

use clap::Parser;
use config::Config;
use movement_feed::ActiveFeed;
use movement_feed::MovementFeed;
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
    // comment). `ActiveFeed<K>` is generic over a Kafka backend type `K`
    // this crate never uses (Task 7's own "Redis-Streams-only" decision);
    // `RedisStreamMovementFeed` itself trivially satisfies `K: MovementFeed`,
    // so `ActiveFeed<RedisStreamMovementFeed>` type-checks even though the
    // `Kafka` variant is never constructed.
    let mut feed: ActiveFeed<RedisStreamMovementFeed> = ActiveFeed::RedisStream(
        Box::new(
            RedisStreamMovementFeed::connect(
                &config.redis_url,
                "trust-event-backlog",
                "trust-event-backlog-1",
                Duration::from_secs(config.redis_autoclaim_min_idle_secs),
            )
            .await?,
        ),
        connection_state,
        "trust_backlog_consumer_ready",
    );

    let stanox = RwLock::new(config.stanox_crs.clone());
    let mut process_state = process::ProcessorState::default();

    let stanox_crs_reload_interval = Duration::from_secs(config.stanox_crs_reload_secs);
    let mut last_stanox_crs_reload = tokio::time::Instant::now() - stanox_crs_reload_interval;
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
                let today = current_rail_day(chrono::Utc::now());
                let snapshot = stanox.read().expect("stanox lock poisoned").clone();
                let mut events = Vec::new();
                for raw in &batch {
                    match trust_schema::schema::parse_batch(raw) {
                        Ok(messages) => {
                            for message in messages {
                                if let Some(event) = process::process_message(
                                    &message,
                                    &mut process_state,
                                    &snapshot,
                                    &crs_index,
                                    today,
                                ) {
                                    events.push(event);
                                }
                            }
                        }
                        Err(err) => {
                            tracing::error!(error = ?err, raw = %raw, "failed to parse TRUST batch; dropping this payload");
                            metrics::counter!(
                                common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                                "operation" => "parse_batch"
                            )
                            .increment(1);
                        }
                    }
                }

                if let Err(err) = queries::post_trust_event_backlog(
                    &http,
                    &config.api_ingest_url,
                    &internal_oauth,
                    &events,
                )
                .await
                {
                    tracing::error!(error = ?err, "failed to post trust-event-backlog batch; will retry next cycle");
                    metrics::counter!(
                        common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                        "operation" => "post_batch"
                    )
                    .increment(1);
                    // Deliberately does NOT commit on a failed post -- same
                    // "only ack after a successful downstream write"
                    // posture as trust-consumer's own main loop, since this
                    // consumer's whole reason to exist is not losing events
                    // a late-tracking pin might need.
                    tokio::time::sleep(ERROR_BACKOFF).await;
                    continue;
                }
                metrics::counter!(common::metrics::metric_name(
                    "trust_backlog_consumer_events_stored_total"
                ))
                .increment(events.len() as u64);

                if let Err(err) = feed.commit().await {
                    tracing::error!(error = ?err, "failed to commit Redis Streams offsets");
                    metrics::counter!(
                        common::metrics::metric_name("trust_backlog_consumer_errors_total"),
                        "operation" => "commit_offsets"
                    )
                    .increment(1);
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
    }
}

const ERROR_BACKOFF: Duration = Duration::from_secs(2);

/// The Europe/London rail day `at` falls on -- the calendar date the
/// process.rs/migration doc comments already promise ("falls back to the
/// current Europe/London rail day"), NOT a bare UTC calendar date. Using
/// `chrono::Utc::now().date_naive()` directly would be plain UTC and
/// ignore both the Europe/London timezone offset AND this codebase's own
/// established 02:00 rail-day cutoff convention
/// (`common::rail_day::next_rail_day_boundary`, extracted specifically so
/// more than one crate could share this exact DST-transition-safe logic).
/// This function is the inverse of `next_rail_day_boundary`: a small,
/// crate-local, pure duplication of the same "before/after local 02:00"
/// check, not a call into `common::rail_day` itself, because that module
/// only exposes the *next boundary*, not *which rail day `at` currently
/// falls in* -- adding the latter to `common::rail_day` instead is a
/// reasonable follow-up, but out of scope for this plan to also change a
/// shared crate's public surface.
fn current_rail_day(at: chrono::DateTime<chrono::Utc>) -> chrono::NaiveDate {
    let local = at.with_timezone(&chrono_tz::Europe::London);
    let cutoff = chrono::NaiveTime::from_hms_opt(2, 0, 0).expect("2:00:00 is a valid time");
    if local.time() < cutoff {
        local.date_naive() - chrono::Duration::days(1)
    } else {
        local.date_naive()
    }
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
        }]);
        let crs_index: std::collections::HashSet<String> =
            ["WAT".to_string()].into_iter().collect();
        let mut state = process::ProcessorState::default();
        let today: chrono::NaiveDate = "2026-09-05".parse().unwrap();

        let events: Vec<_> = messages
            .iter()
            .filter_map(|m| process::process_message(m, &mut state, &stanox, &crs_index, today))
            .collect();

        assert_eq!(
            events.len(),
            1,
            "the PASS event must be dropped, only the DEPARTURE kept"
        );
        assert_eq!(events[0].event_type, Some("DEPARTURE".to_string()));
    }
}
