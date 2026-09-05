//! `full-coverage-consumer`: a second, independent Kafka consumer against
//! the same RDM Train Movements feed `trust-consumer` reads, correlating
//! every event against the FULL scheduled population of every
//! shadow-computed line (not a small pinned-train set) -- see
//! docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md and
//! docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md.
//! SHADOW MODE ONLY: writes real per-line/per-station stats, but nothing
//! reads them into a real line's severity/DataQuality while
//! `LineDefinition.full_coverage_enabled` stays false everywhere (see the
//! design doc's binding condition).
//!
//! # Loop shape (Task 13)
//!
//! Mirrors `trust-consumer/src/main.rs`'s own multi-cadence-in-one-loop
//! shape (population reload / stanox-crs reload / consume-and-correlate,
//! each on its own timer, all checked once per iteration) plus one more
//! cadence this crate alone needs: a periodic stats write.
//!
//! **A real, deliberate difference from `trust-consumer`'s shape, stated
//! plainly**: `trust-consumer` only commits Kafka offsets after a
//! successful POST to `api`, because a failed post there means a real
//! tracked-train event is lost forever if the offset advances anyway. This
//! crate's own Kafka commit cadence and its stats-write cadence are
//! genuinely decoupled -- offsets are committed as soon as a batch is
//! successfully parsed and dispatched into in-memory correlation state,
//! *not* gated on the periodic stats POST succeeding. This is safe because
//! `DerivedState` fields are last-write-wins per event, not additive
//! (confirmed by Task 1's own reading of
//! `trust_schema::journey::apply_movement`/`apply_cancellation`) -- a
//! stats POST that fails this cycle is simply retried with fresher data
//! next cycle, and Kafka redelivery of an already-processed batch would
//! just re-derive the same state, not corrupt it.

mod config;
mod correlate;
mod feed;
mod health;
mod population;
mod queries;
mod stanox_tiploc;
mod station_correlate;
mod stats;

use std::collections::HashMap;
use std::time::Duration;

use clap::Parser;
use config::{Config, MovementFeedBackend};
use feed::MovementFeed;
use feed::kafka::KafkaMovementFeed;
use movement_feed::redis_stream::{GapInfo, RedisStreamMovementFeed};
use trust_schema::schema::TrustMessage;

/// Wraps whichever concrete `MovementFeed` this deployment selected. See
/// `trust-consumer/src/main.rs`'s identical type for the full reasoning on
/// why this is a manual enum rather than `Box<dyn MovementFeed>`
/// (`check_gap` is a `RedisStreamMovementFeed`-only inherent method, not
/// reachable through the trait object).
enum ActiveFeed {
    Kafka(KafkaMovementFeed),
    // Boxed: RedisStreamMovementFeed is meaningfully larger than
    // KafkaMovementFeed (clippy::large_enum_variant) -- see
    // trust-consumer/src/main.rs's identical note. The second field is the
    // same bug fix as that crate's identical type: RedisStreamMovementFeed::connect
    // never received connection_state, so /healthz stayed permanently
    // SERVICE_UNAVAILABLE under this backend -- confirmed live on
    // trust-consumer during Deploy B's B3 step (restart-looped on a
    // failing liveness probe despite genuinely healthy Redis Streams
    // reads). Updated at the ActiveFeed::next_batch call site below,
    // mirroring KafkaMovementFeed's own internal update
    // (feed/kafka.rs:76-95).
    RedisStream(Box<RedisStreamMovementFeed>, health::ConnectionState),
}

#[async_trait::async_trait]
impl MovementFeed for ActiveFeed {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        match self {
            ActiveFeed::Kafka(feed) => feed.next_batch().await,
            ActiveFeed::RedisStream(feed, connection_state) => {
                let result = feed.next_batch().await;
                health::set_connected(connection_state, result.is_ok());
                result
            }
        }
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        match self {
            ActiveFeed::Kafka(feed) => feed.commit().await,
            ActiveFeed::RedisStream(feed, _) => feed.commit().await,
        }
    }
}

impl ActiveFeed {
    async fn check_gap(&mut self) -> anyhow::Result<Option<GapInfo>> {
        match self {
            ActiveFeed::Kafka(_) => Ok(None),
            ActiveFeed::RedisStream(feed, _) => feed.check_gap().await,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = Config::parse();
    if config.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }
    let connection_state = health::spawn(config.health_bind_url.clone());
    let http = reqwest::Client::new();
    let internal_oauth =
        common::oauth_client::OAuthTokenCache::new(common::oauth_client::OAuthCredentials {
            token_url: config.internal_oauth_token_url.clone(),
            client_id: config.internal_oauth_client_id.clone(),
            scope: config.internal_oauth_scope.clone(),
            username: config.internal_oauth_username.clone(),
            password: config.internal_oauth_password.clone(),
        });

    let mut feed = match config.movement_feed_backend {
        MovementFeedBackend::Kafka => {
            ActiveFeed::Kafka(KafkaMovementFeed::connect(&config, connection_state)?)
        }
        MovementFeedBackend::RedisStream => ActiveFeed::RedisStream(
            Box::new(
                RedisStreamMovementFeed::connect(
                    &config.redis_url,
                    "full-coverage-consumer",
                    "full-coverage-consumer-1",
                    Duration::from_secs(config.redis_autoclaim_min_idle_secs),
                )
                .await?,
            ),
            connection_state,
        ),
    };
    let redis_gap_check_interval = Duration::from_secs(config.redis_gap_check_secs);
    let mut last_redis_gap_check = tokio::time::Instant::now() - redis_gap_check_interval;

    // Built once, before the loop: purely static-catalogue-derived, so it
    // only needs rebuilding when config.lines changes, which doesn't
    // happen at runtime.
    let tiploc_index = population::build_tiploc_index(&config.lines);
    let shadow_line_ids = config.shadow_line_ids();
    let defaults = common::Defaults::default();

    let mut population = population::Population::default();
    let stanox = std::sync::RwLock::new(stanox_tiploc::StanoxTable::default());
    let mut correlation_state = correlate::CorrelationState::default();
    let mut station_state = station_correlate::StationCorrelationState::default();

    let mut service_date = chrono::Utc::now().date_naive();

    let population_reload_interval = Duration::from_secs(config.population_reload_secs);
    let mut last_population_reload = tokio::time::Instant::now() - population_reload_interval;
    let stanox_crs_reload_interval = Duration::from_secs(config.stanox_crs_reload_secs);
    let mut last_stanox_crs_reload = tokio::time::Instant::now() - stanox_crs_reload_interval;
    let stats_write_interval = Duration::from_secs(config.stats_write_interval_secs);
    let mut last_stats_write = tokio::time::Instant::now() - stats_write_interval;

    loop {
        // 5. rail-day rollover: a fresh correlation_state/station_state
        // per rail day is deliberate -- yesterday's (line_id, uid) keys
        // must not silently accumulate forever in a long-lived process.
        // Population for the new day is picked up by step 1's own reload
        // below, which already fetches "today's and tomorrow's".
        let today = chrono::Utc::now().date_naive();
        if today != service_date {
            tracing::info!(previous = %service_date, new = %today, "rail day rolled over; resetting correlation state");
            service_date = today;
            correlation_state = correlate::CorrelationState::default();
            station_state = station_correlate::StationCorrelationState::default();
        }

        // 1. population reload (Decision 2b: today's AND tomorrow's, to
        // avoid a gap at the rail-day rollover boundary).
        if last_population_reload.elapsed() >= population_reload_interval {
            reload_population(
                &http,
                &config,
                &internal_oauth,
                &shadow_line_ids,
                &mut population,
                service_date,
            )
            .await;
            last_population_reload = tokio::time::Instant::now();
        }

        // 2. stanox_crs reload.
        if last_stanox_crs_reload.elapsed() >= stanox_crs_reload_interval {
            match queries::fetch_stanox_crs(&http, &config.stanox_crs_url, &internal_oauth).await {
                Ok(records) => {
                    let table = stanox_tiploc::StanoxTable::from_records(&records);
                    *stanox.write().expect("stanox lock poisoned") = table;
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to reload stanox/crs table; keeping previous snapshot");
                    metrics::counter!(
                        common::metrics::metric_name("full_coverage_consumer_errors_total"),
                        "operation" => "reload_stanox_crs"
                    )
                    .increment(1);
                }
            }
            last_stanox_crs_reload = tokio::time::Instant::now();
        }

        // 2b. redis-stream gap check -- a no-op under the Kafka backend
        // (ActiveFeed::check_gap returns Ok(None) immediately for that
        // variant). See docs/superpowers/specs/2026-09-04-movement-relay-design.md
        // Decision 2's "definitive gap detection."
        if last_redis_gap_check.elapsed() >= redis_gap_check_interval {
            match feed.check_gap().await {
                Ok(Some(gap)) => {
                    tracing::error!(
                        last_delivered = %gap.group_last_delivered_id,
                        new_first_entry = %gap.stream_first_entry_id,
                        "movement-events stream gap detected: some events between these IDs were trimmed before full-coverage-consumer ever read them -- this can bias this consumer's own shadow-mode SampleStats for the affected window (e.g. inflating the unconfirmed-by-window-close = cancelled bucket); treat any rail day during which a gap was detected as not clean signal"
                    );
                    metrics::counter!(common::metrics::metric_name(
                        "full_coverage_consumer_stream_gap_detected_total"
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

        // 3. consume + correlate.
        let cycle_start = std::time::Instant::now();
        match feed.next_batch().await {
            Ok(batch) => {
                let snapshot = stanox.read().expect("stanox lock poisoned").clone();
                for raw in &batch {
                    match trust_schema::schema::parse_batch(raw) {
                        Ok(messages) => {
                            for message in messages {
                                dispatch_message(
                                    message,
                                    &mut correlation_state,
                                    &mut station_state,
                                    &snapshot,
                                    &tiploc_index,
                                    &population,
                                    service_date,
                                );
                            }
                        }
                        Err(err) => {
                            tracing::error!(error = ?err, raw = %raw, "failed to parse TRUST batch; dropping this payload");
                            metrics::counter!(
                                common::metrics::metric_name("full_coverage_consumer_errors_total"),
                                "operation" => "parse_batch"
                            )
                            .increment(1);
                        }
                    }
                }
                // Commit as soon as the batch is dispatched into
                // in-memory state -- see this module's own doc comment
                // for why this crate's commit cadence is decoupled from
                // its stats-write cadence, unlike trust-consumer's.
                if let Err(err) = feed.commit().await {
                    tracing::error!(error = ?err, "failed to commit Kafka offsets");
                    metrics::counter!(
                        common::metrics::metric_name("full_coverage_consumer_errors_total"),
                        "operation" => "commit_offsets"
                    )
                    .increment(1);
                }
            }
            Err(err) => {
                tracing::error!(error = ?err, "error receiving from movement feed");
                metrics::counter!(
                    common::metrics::metric_name("full_coverage_consumer_errors_total"),
                    "operation" => "movement_feed_receive"
                )
                .increment(1);
                tokio::time::sleep(ERROR_BACKOFF).await;
            }
        }
        metrics::histogram!(common::metrics::metric_name(
            "full_coverage_consumer_cycle_duration_seconds"
        ))
        .record(cycle_start.elapsed().as_secs_f64());

        // 4. stats write.
        if last_stats_write.elapsed() >= stats_write_interval {
            write_stats(
                &http,
                &config,
                &internal_oauth,
                &shadow_line_ids,
                &population,
                &correlation_state,
                &station_state,
                service_date,
                &defaults,
            )
            .await;
            last_stats_write = tokio::time::Instant::now();
        }
    }
}

/// How long to wait before retrying after a feed-level failure -- flat,
/// not exponential, same reasoning as `trust-consumer::main::ERROR_BACKOFF`:
/// Kafka holds the backlog, so there's nothing to drain, only a log/API to
/// avoid hammering.
const ERROR_BACKOFF: Duration = Duration::from_secs(2);

/// Fetches `line_id`'s population for both `service_date` and
/// `service_date + 1` for every shadow-computed line (Decision 2b).
/// Best-effort per (line, date) pair -- one failure must not block every
/// other line's reload.
async fn reload_population(
    client: &reqwest::Client,
    config: &Config,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
    shadow_line_ids: &[String],
    population: &mut population::Population,
    service_date: chrono::NaiveDate,
) {
    let dates = [service_date, service_date + chrono::Duration::days(1)];
    for line_id in shadow_line_ids {
        for &date in &dates {
            match queries::fetch_line_population(
                client,
                &config.schedule_line_population_url,
                internal_oauth,
                line_id,
                date,
            )
            .await
            {
                Ok(Some(value)) => {
                    match serde_json::from_value::<Vec<schedule_query::LinePopulationEntry>>(value)
                    {
                        Ok(entries) => population.insert(line_id, date, entries),
                        Err(err) => {
                            tracing::error!(error = ?err, line_id = %line_id, %date, "failed to deserialize schedule-line-population response");
                            metrics::counter!(
                                common::metrics::metric_name("full_coverage_consumer_errors_total"),
                                "operation" => "reload_line_population_deserialize"
                            )
                            .increment(1);
                        }
                    }
                }
                Ok(None) => {
                    // Nothing published yet for this (line, date) --
                    // Decision 2e's Pending case, upstream of the rail-day
                    // gate. Not an error.
                }
                Err(err) => {
                    tracing::error!(error = ?err, line_id = %line_id, %date, "failed to fetch schedule line population; keeping previous snapshot");
                    metrics::counter!(
                        common::metrics::metric_name("full_coverage_consumer_errors_total"),
                        "operation" => "reload_line_population_fetch"
                    )
                    .increment(1);
                }
            }
        }
    }
}

/// Dispatches one parsed `TrustMessage` into both running correlation
/// records. `ChangeOfOrigin`/`ChangeOfIdentity`/`Unknown` are deliberately
/// ignored -- `correlate.rs`'s own scope (Decision 2d) only covers
/// Activation/Movement/Cancellation, the same three message types
/// `trust-consumer` itself keys real behaviour on.
fn dispatch_message(
    message: TrustMessage,
    correlation_state: &mut correlate::CorrelationState,
    station_state: &mut station_correlate::StationCorrelationState,
    stanox: &stanox_tiploc::StanoxTable,
    tiploc_index: &HashMap<String, Vec<String>>,
    population: &population::Population,
    service_date: chrono::NaiveDate,
) {
    match message {
        TrustMessage::Activation(activation) => {
            correlate::apply_activation(correlation_state, &activation);
            station_correlate::apply_activation(
                station_state,
                &activation.train_uid,
                &activation.toc_id,
            );
        }
        TrustMessage::Movement(movement) => {
            let result = correlate::apply_movement(
                correlation_state,
                &movement,
                stanox,
                tiploc_index,
                population,
                service_date,
            );
            for (line_id, uid) in &result.matched_lines {
                metrics::counter!(
                    common::metrics::metric_name("full_coverage_consumer_events_matched_total"),
                    "line_id" => line_id.clone()
                )
                .increment(1);

                let Some(crs) = result.loc_crs.as_deref() else {
                    continue;
                };
                let Some(derived) = correlation_state
                    .derived
                    .get(&(line_id.clone(), uid.clone()))
                else {
                    continue;
                };
                let matched_station = station_correlate::apply_movement_station(
                    station_state,
                    &result.train_uid,
                    crs,
                    derived,
                );
                if !matched_station {
                    metrics::counter!(common::metrics::metric_name(
                        "full_coverage_consumer_station_buckets_dropped_total"
                    ))
                    .increment(1);
                }
            }
        }
        TrustMessage::Cancellation(cancellation) => {
            correlate::apply_cancellation(correlation_state, &cancellation);
        }
        TrustMessage::ChangeOfOrigin(_)
        | TrustMessage::ChangeOfIdentity(_)
        | TrustMessage::Unknown(_) => {}
    }
}

/// For every shadow-computed line, builds and POSTs its stats row; for
/// every populated `(crs, toc_id)` station bucket, builds and POSTs its
/// sample. Independent, best-effort failures -- Decision 3's "no shared
/// transaction" rule: one line's or one bucket's POST failing must not
/// block any other's.
#[allow(clippy::too_many_arguments)]
async fn write_stats(
    client: &reqwest::Client,
    config: &Config,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
    shadow_line_ids: &[String],
    population: &population::Population,
    correlation_state: &correlate::CorrelationState,
    station_state: &station_correlate::StationCorrelationState,
    service_date: chrono::NaiveDate,
    defaults: &common::Defaults,
) {
    let now = chrono::Utc::now();
    let closed = stats::rail_day_closed(service_date, now);

    let mut line_rows = Vec::new();
    let mut available_count = 0u64;
    let mut pending_count = 0u64;
    for line_id in shadow_line_ids {
        let population_uids = population.uids_for(line_id, service_date);
        let row = stats::build_line_row(
            line_id,
            service_date,
            &population_uids,
            &correlation_state.derived,
            closed,
            defaults,
        );
        if row.availability == "available" {
            available_count += 1;
        } else {
            pending_count += 1;
        }
        line_rows.push(row);
    }
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_lines_available_total"
    ))
    .set(available_count as f64);
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_lines_pending_total"
    ))
    .set(pending_count as f64);

    if let Err(err) = queries::post_full_coverage_stats(
        client,
        &config.full_coverage_stats_url,
        internal_oauth,
        &line_rows,
    )
    .await
    {
        tracing::error!(error = ?err, "failed to post full-coverage line stats; will retry next cycle");
        metrics::counter!(
            common::metrics::metric_name("full_coverage_consumer_errors_total"),
            "operation" => "post_line_stats"
        )
        .increment(1);
    }

    let station_rows = station_correlate::build_station_rows(station_state, now, defaults);
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_stations_available_total"
    ))
    .set(station_rows.len() as f64);
    if let Err(err) = queries::post_station_full_coverage_samples(
        client,
        &config.station_full_coverage_stats_url,
        internal_oauth,
        &station_rows,
    )
    .await
    {
        tracing::error!(error = ?err, "failed to post station full-coverage samples; will retry next cycle");
        metrics::counter!(
            common::metrics::metric_name("full_coverage_consumer_errors_total"),
            "operation" => "post_station_samples"
        )
        .increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::FakeMovementFeed;

    /// Integration-shaped test against `FakeMovementFeed`, mirroring
    /// `trust-consumer/src/main.rs`'s own `#[cfg(test)] mod tests`
    /// structure: exercises the full wiring together (parse -> correlate
    /// -> station_correlate -> stats), not just each module in isolation.
    #[tokio::test]
    async fn an_activation_and_movement_batch_produces_the_expected_line_and_station_stats() {
        const ACTIVATION: &str = r#"{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C11052","toc_id":"SW",
            "train_service_code":"22345000","schedule_wtt_id":"1234",
            "schedule_start_date":"2026-09-04","schedule_end_date":"2026-09-04"
        }}"#;
        const MOVEMENT: &str = r#"{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1787941920000","actual_timestamp":"1787941920000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}"#;

        let mut feed =
            FakeMovementFeed::new(vec![vec![ACTIVATION.to_string(), MOVEMENT.to_string()]]);

        let stanox = stanox_tiploc::StanoxTable::from_records(&[common::StanoxCrsRecord {
            stanox: "87212".to_string(),
            crs: "WAT".to_string(),
            tiploc: "WATRLMN".to_string(),
            station_name: "LONDON WATERLOO".to_string(),
            source_sequence: 1,
        }]);

        let mut tiploc_index = HashMap::new();
        tiploc_index.insert("WATRLMN".to_string(), vec!["waterloo-reading".to_string()]);

        let service_date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let mut population = population::Population::default();
        population.insert(
            "waterloo-reading",
            service_date,
            vec![schedule_query::LinePopulationEntry {
                uid: "C11052".to_string(),
                calling_points: vec![],
            }],
        );

        let mut correlation_state = correlate::CorrelationState::default();
        let mut station_state = station_correlate::StationCorrelationState::default();

        let batch = feed.next_batch().await.unwrap();
        for raw in &batch {
            for message in trust_schema::schema::parse_batch(raw).unwrap() {
                dispatch_message(
                    message,
                    &mut correlation_state,
                    &mut station_state,
                    &stanox,
                    &tiploc_index,
                    &population,
                    service_date,
                );
            }
        }
        feed.commit().await.unwrap();
        assert_eq!(feed.committed_count, 1);

        let population_uids = population.uids_for("waterloo-reading", service_date);
        let row = stats::build_line_row(
            "waterloo-reading",
            service_date,
            &population_uids,
            &correlation_state.derived,
            false,
            &common::Defaults::default(),
        );
        assert_eq!(row.stats.total, 1);
        assert_eq!(row.stats.cancelled, 0);
        assert_eq!(row.availability, "pending");

        let station_rows = station_correlate::build_station_rows(
            &station_state,
            chrono::Utc::now(),
            &common::Defaults::default(),
        );
        assert_eq!(station_rows.len(), 1);
        assert_eq!(station_rows[0].crs, "WAT");
        assert_eq!(station_rows[0].operator, "SW");
    }
}
