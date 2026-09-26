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
use movement_feed::ActiveFeed;
use movement_feed::redis_stream::RedisStreamMovementFeed;
use trust_schema::schema::TrustMessage;

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
                    "full-coverage-consumer",
                    "full-coverage-consumer-1",
                    Duration::from_secs(config.redis_autoclaim_min_idle_secs),
                )
                .await?,
            ),
            connection_state,
            "full_coverage_consumer_ready",
        ),
    };
    let redis_gap_check_interval = Duration::from_secs(config.redis_gap_check_secs);
    let mut last_redis_gap_check = tokio::time::Instant::now() - redis_gap_check_interval;

    // Rebuilt every stanox_crs reload cycle (step 2, below), alongside
    // `stanox` itself: as of the 2026-09-09 tiploc-schedule-matching-gap
    // fix, this index resolves each catalogued station's real TIPLOC(s)
    // from the live, CIF-derived stanox_crs snapshot via its CRS, not
    // from the (mostly absent) `lines/*.toml` `tiploc` field -- so, unlike
    // before, it can no longer be built once, purely from `config.lines`,
    // before the loop. Starts empty until the first reload below
    // succeeds, same as `stanox` itself starting as `StanoxTable::default()`.
    let mut tiploc_index: HashMap<String, Vec<String>> = HashMap::new();
    // As of the 2026-09-11 tiploc-schedule-matching-gap fix (this crate's
    // fifth independent copy of the bug, see `Config::shadow_line_ids`'s
    // own doc), `"*"` wildcard resolution is based on real, CIF-derived
    // TIPLOC coverage via `stanox_crs`, the same data `tiploc_index` above
    // is built from -- so, like `tiploc_index`, this can no longer be
    // resolved once from `config.lines` alone before the loop. Starts
    // empty and is (re)computed every stanox_crs reload cycle below (step
    // 1), which now runs before step 2's population reload uses it, so
    // even the very first loop iteration sees real coverage once that
    // first reload succeeds.
    let mut shadow_line_ids: Vec<String> = Vec::new();
    let defaults = common::Defaults::default();

    let mut population = population::Population::default();
    let stanox = std::sync::RwLock::new(stanox_tiploc::StanoxTable::default());
    let mut correlation_state = correlate::CorrelationState::default();
    let mut station_state = station_correlate::StationCorrelationState::default();

    // NOT `Utc::now().date_naive()`: between 00:00Z and the 02:00-local
    // rail-day boundary, the calendar day has already changed but the rail
    // day in progress is still the PREVIOUS one, and every train running in
    // that window belongs to it. See `current_rail_service_date`.
    let mut service_date = current_rail_service_date(chrono::Utc::now());

    let population_reload_interval = Duration::from_secs(config.population_reload_secs);
    let mut last_population_reload = tokio::time::Instant::now() - population_reload_interval;
    let stanox_crs_reload_interval = Duration::from_secs(config.stanox_crs_reload_secs);
    // How long to wait before the NEXT stanox/crs reload attempt: the full
    // interval after a success, a much shorter backoff after a failure (see
    // `failed_reload_retry_delay`). Starts at zero so the first iteration
    // loads immediately, the same thing the previous
    // `Instant::now() - interval` seeding achieved.
    let mut stanox_crs_wait = Duration::ZERO;
    let mut last_stanox_crs_reload = tokio::time::Instant::now();
    let stats_write_interval = Duration::from_secs(config.stats_write_interval_secs);
    let mut last_stats_write = tokio::time::Instant::now() - stats_write_interval;

    loop {
        // 0. rail-day rollover, and NOT a `Utc::now().date_naive() !=
        // service_date` check.
        //
        // The calendar day changes at 00:00Z, but `service_date`'s rail day
        // does not END until 02:00 Europe/London the following day
        // (01:00Z under BST). Rolling over at 00:00Z therefore wiped
        // `correlation_state` one to two hours BEFORE the day it belonged
        // to had closed, with two consequences: trains still running in
        // that window lost their resolved/pending-activation mapping (so
        // their later movements went unattributed, and the window was then
        // correlated against the WRONG day's schedule population), and --
        // because `service_date` had already been replaced by the time the
        // stats write at the bottom of this loop ran --
        // `stats::rail_day_closed(service_date, now)` was observed as
        // `false` on every single iteration, for the whole life of the
        // process. That made the "available" availability state
        // structurally unreachable: every row this service had ever posted
        // read "pending".
        //
        // So the rollover now happens at the real rail-day boundary, and
        // the day that just closed gets its final, genuinely-closeable
        // stats write BEFORE its state is replaced.
        let now = chrono::Utc::now();
        if let RailDayTransition::CloseAndRoll { closing, next } =
            rail_day_transition(service_date, now)
        {
            // Yesterday's closing write, with `rail_day_closed(closing,
            // now)` now genuinely true -- this is the row that reads
            // "available".
            write_stats(
                &http,
                &config,
                &internal_oauth,
                &shadow_line_ids,
                &population,
                &correlation_state,
                &station_state,
                closing,
                &defaults,
            )
            .await;
            last_stats_write = tokio::time::Instant::now();

            tracing::info!(closed = %closing, new = %next, "rail day closed; wrote its final stats, then reset correlation state");
            service_date = next;
            correlation_state = correlate::CorrelationState::default();
            station_state = station_correlate::StationCorrelationState::default();
            // Nothing else ever removed a past day's population, so every
            // closed day's full per-line calling-point map stayed resident
            // for the life of the process.
            population.retain_from(service_date);
        }

        // 1. stanox_crs reload -- deliberately runs BEFORE step 2's
        // population reload (2026-09-11 fix): population reload needs
        // `shadow_line_ids`, which (as of that same fix) is itself
        // resolved from these same `stanox_crs` records rather than the
        // static `lines/*.toml` catalogue, so it must be refreshed here
        // first. Previously this ran second, which didn't matter because
        // `shadow_line_ids` was a static, pre-loop value; now it does.
        //
        // A FAILED fetch must not wait the full interval before trying
        // again: `stanox`/`tiploc_index`/`shadow_line_ids` all start empty,
        // so a failure on the very first attempt (api not up yet, the OAuth
        // endpoint down during a rolling deploy) left this consumer blind
        // for a full `stanox_crs_reload_secs` (3600s by default) while it
        // went on consuming, matching against empty lookups, and committing
        // those movements as processed -- gone, for this consumer, for good.
        // So the wait after a failure is a short backoff, not the interval.
        if last_stanox_crs_reload.elapsed() >= stanox_crs_wait {
            match queries::fetch_stanox_crs(&http, &config.stanox_crs_url, &internal_oauth).await {
                Ok(records) => {
                    let table = stanox_tiploc::StanoxTable::from_records(&records);
                    *stanox.write().expect("stanox lock poisoned") = table;
                    tiploc_index = population::build_tiploc_index(&config.lines, &records);
                    shadow_line_ids = config.shadow_line_ids(&records);
                    stanox_crs_wait = stanox_crs_reload_interval;
                }
                Err(err) => {
                    stanox_crs_wait = failed_reload_retry_delay(stanox_crs_reload_interval);
                    tracing::error!(error = ?err, retry_in_secs = stanox_crs_wait.as_secs(), "failed to reload stanox/crs table; keeping previous snapshot and retrying sooner than the normal interval");
                    metrics::counter!(
                        common::metrics::metric_name("full_coverage_consumer_errors_total"),
                        "operation" => "reload_stanox_crs"
                    )
                    .increment(1);
                }
            }
            last_stanox_crs_reload = tokio::time::Instant::now();
        }

        // 1b. redis-stream gap check -- a no-op under the Kafka backend
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

        // 2. population reload (Decision 2b: today's AND tomorrow's, to
        // avoid a gap at the rail-day rollover boundary). Runs after step
        // 1 above so it always sees this cycle's freshly-resolved
        // `shadow_line_ids`, not a stale value from before this cycle's
        // stanox_crs reload.
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

/// What the top of the loop must do about the rail day, given the day it is
/// currently correlating into and the current instant. Split out of the loop
/// body precisely so the sequencing that made `"available"` unreachable is
/// directly testable.
#[derive(Debug, PartialEq, Eq)]
enum RailDayTransition {
    /// `service_date`'s rail day is still open: keep correlating into it,
    /// and keep its correlation state, even if the CALENDAR day has already
    /// changed (the 00:00Z-to-02:00-local window).
    Stay,
    /// `service_date`'s rail day has closed. Its final stats must be
    /// written -- `stats::rail_day_closed(closing, now)` is true by
    /// construction here, so that row reads "available" -- and only then may
    /// its state be wiped and `service_date` replaced with `next`.
    CloseAndRoll {
        closing: chrono::NaiveDate,
        next: chrono::NaiveDate,
    },
}

fn rail_day_transition(
    service_date: chrono::NaiveDate,
    now: chrono::DateTime<chrono::Utc>,
) -> RailDayTransition {
    if stats::rail_day_closed(service_date, now) {
        RailDayTransition::CloseAndRoll {
            closing: service_date,
            next: current_rail_service_date(now),
        }
    } else {
        RailDayTransition::Stay
    }
}

/// The rail day in progress at `now` -- the service date every train
/// currently running belongs to. Between 00:00Z and 02:00 Europe/London
/// that is YESTERDAY's calendar date, not today's, because the rail day
/// that started at 02:00 local yesterday has not ended yet.
///
/// Derived from `stats::rail_day_closed` rather than a second, independent
/// Europe/London 02:00 calculation: the day before `now`'s calendar date is
/// the current rail day exactly when that earlier day has NOT yet closed.
fn current_rail_service_date(now: chrono::DateTime<chrono::Utc>) -> chrono::NaiveDate {
    let today = now.date_naive();
    let yesterday = today - chrono::Duration::days(1);
    if stats::rail_day_closed(yesterday, now) {
        today
    } else {
        yesterday
    }
}

/// How long to wait before re-attempting a stanox/crs reload that FAILED --
/// a twentieth of the normal interval (3 minutes at the 3600s default),
/// floored at 15s so a misconfigured tiny interval can't turn into a hot
/// retry loop against the OAuth endpoint, and never longer than the normal
/// interval itself.
fn failed_reload_retry_delay(interval: Duration) -> Duration {
    const FLOOR: Duration = Duration::from_secs(15);
    if interval <= FLOOR {
        return interval;
    }
    let backoff = interval / 20;
    if backoff < FLOOR { FLOOR } else { backoff }
}

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
    // Only today's and tomorrow's populations are ever read (`uids_for` is
    // always asked about the current `service_date`), so every older date
    // this map still holds is pure retained memory -- pruned here on every
    // reload, as well as at each rail-day rollover.
    population.retain_from(service_date);
}

/// Dispatches one parsed `TrustMessage` into both running correlation
/// records. `ChangeOfOrigin`/`ChangeOfIdentity`/`Reinstatement`/`Unknown`
/// are deliberately ignored -- `correlate.rs`'s own scope (Decision 2d) only
/// covers Activation/Movement/Cancellation, the same three message types
/// `trust-consumer` itself keys real behaviour on. `Reinstatement` (`0005`,
/// confirmed by the H4 fix of the 2026-09-26 review) doesn't regress this
/// consumer's own "cancelled" line-level state by being ignored here: this
/// module's `apply_movement`/`apply_cancellation` already reuse
/// `trust_schema::journey` directly, so that fix's own `status_rank` change
/// (a fresh Movement can un-stick a `"cancelled"` per-line status without
/// needing a Reinstatement message specifically) already applies here too.
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
            // `toc_id` is `Option` as of the 2026-09-25 review's finding #7
            // (a required field with no reader made one absent value drop the
            // WHOLE Activation, costing its far more valuable
            // train_id/train_uid binding). `None` simply means this uid
            // learns no operator here -- `station_correlate` already treats
            // "a UID absent from `activations_by_uid`" as a first-class case
            // and skips station correlation for it, exactly as it does for a
            // Movement whose Activation this process never saw.
            if let Some(toc_id) = activation.toc_id.as_deref() {
                station_correlate::apply_activation(station_state, &activation.train_uid, toc_id);
            } else {
                tracing::debug!(
                    train_uid = %activation.train_uid,
                    "Activation carries no toc_id; skipping station correlation for this uid"
                );
            }
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
        | TrustMessage::Reinstatement(_)
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

    const ACTIVATION_C11052: &str = r#"{"header":{"msg_type":"0001"},"body":{
        "train_id":"221832406","train_uid":"C11052","toc_id":"SW",
        "train_service_code":"22345000","schedule_wtt_id":"1234",
        "schedule_start_date":"2026-07-15","schedule_end_date":"2026-07-15"
    }}"#;
    const MOVEMENT_C11052: &str = r#"{"header":{"msg_type":"0003"},"body":{
        "train_id":"221832406","event_type":"DEPARTURE",
        "planned_timestamp":"1787941920000","actual_timestamp":"1787941920000",
        "loc_stanox":"87212","variation_status":"ON TIME"
    }}"#;

    fn waterloo_stanox_table() -> stanox_tiploc::StanoxTable {
        stanox_tiploc::StanoxTable::from_records(&[common::StanoxCrsRecord {
            stanox: "87212".to_string(),
            crs: "WAT".to_string(),
            tiploc: "WATRLMN".to_string(),
            station_name: "LONDON WATERLOO".to_string(),
            source_sequence: 1,
            change_time_minutes: None,
        }])
    }

    fn waterloo_tiploc_index() -> HashMap<String, Vec<String>> {
        let mut index = HashMap::new();
        index.insert("WATRLMN".to_string(), vec!["waterloo-reading".to_string()]);
        index
    }

    fn population_for(dates: &[chrono::NaiveDate]) -> population::Population {
        let mut population = population::Population::default();
        for &date in dates {
            population.insert(
                "waterloo-reading",
                date,
                vec![schedule_query::LinePopulationEntry {
                    uid: "C11052".to_string(),
                    calling_points: vec![],
                }],
            );
        }
        population
    }

    /// Regression test for the "available is structurally unreachable"
    /// finding, at the point the sequencing turns on.
    ///
    /// `service_date` used to roll over the instant `Utc::now()
    /// .date_naive()` changed -- 00:00Z -- but a rail day does not CLOSE
    /// until 02:00 Europe/London the following day (01:00Z under BST). At
    /// 00:30Z the day must therefore still be open: the loop must keep
    /// correlating into it and must not touch its state.
    #[test]
    fn a_calendar_day_change_before_the_0200_local_boundary_does_not_roll_the_rail_day() {
        let service_date: chrono::NaiveDate = "2026-07-15".parse().unwrap();
        let just_after_utc_midnight: chrono::DateTime<chrono::Utc> =
            "2026-07-16T00:30:00Z".parse().unwrap();

        assert_ne!(
            just_after_utc_midnight.date_naive(),
            service_date,
            "the CALENDAR day has changed -- which is exactly what used to trigger the rollover"
        );
        assert_eq!(
            rail_day_transition(service_date, just_after_utc_midnight),
            RailDayTransition::Stay,
            "but the RAIL day has not closed yet, so nothing may roll or be wiped"
        );
    }

    /// The other half of the same finding: when the rollover DOES happen,
    /// the day being closed out is provably closed at that instant, so its
    /// final stats row reads "available" -- the state this product exists to
    /// report, and which no row had ever carried.
    ///
    /// The second half of this test pins the old sequencing's failure
    /// directly: under it, `service_date` had already been replaced by the
    /// new calendar day before `write_stats` ran, and
    /// `rail_day_closed(<the new day>, now)` is false by construction, so
    /// every row read "pending" forever.
    #[test]
    fn the_rail_day_being_rolled_is_already_closed_so_its_final_row_reads_available() {
        let service_date: chrono::NaiveDate = "2026-07-15".parse().unwrap();
        let just_after_close: chrono::DateTime<chrono::Utc> =
            "2026-07-16T01:00:01Z".parse().unwrap();

        let transition = rail_day_transition(service_date, just_after_close);
        let RailDayTransition::CloseAndRoll { closing, next } = transition else {
            panic!("the rail day must roll once it has closed, got {transition:?}");
        };
        assert_eq!(closing, service_date);
        assert_eq!(next, "2026-07-16".parse::<chrono::NaiveDate>().unwrap());

        let closed = stats::rail_day_closed(closing, just_after_close);
        assert!(
            closed,
            "the closing write happens while the day it covers is closed"
        );
        let row = stats::build_line_row(
            "waterloo-reading",
            closing,
            &["C11052"],
            &HashMap::new(),
            closed,
            &common::Defaults::default(),
        );
        assert_eq!(row.availability, "available");

        assert!(
            !stats::rail_day_closed(next, just_after_close),
            "whereas the day just STARTED is never closed at this instant -- writing stats \
             after replacing service_date (the old sequencing) could only ever emit \"pending\""
        );
    }

    /// The rail day in progress, at startup and after a rollover. Covers
    /// both BST (boundary 01:00Z) and GMT (boundary 02:00Z), since the bug
    /// this guards against is precisely a UTC-midnight assumption.
    #[test]
    fn current_rail_service_date_stays_on_yesterday_until_0200_local() {
        let bst_before: chrono::DateTime<chrono::Utc> = "2026-07-16T00:30:00Z".parse().unwrap();
        let bst_after: chrono::DateTime<chrono::Utc> = "2026-07-16T01:30:00Z".parse().unwrap();
        assert_eq!(
            current_rail_service_date(bst_before),
            "2026-07-15".parse::<chrono::NaiveDate>().unwrap()
        );
        assert_eq!(
            current_rail_service_date(bst_after),
            "2026-07-16".parse::<chrono::NaiveDate>().unwrap()
        );

        let gmt_before: chrono::DateTime<chrono::Utc> = "2026-01-15T01:30:00Z".parse().unwrap();
        let gmt_after: chrono::DateTime<chrono::Utc> = "2026-01-15T02:30:00Z".parse().unwrap();
        assert_eq!(
            current_rail_service_date(gmt_before),
            "2026-01-14".parse::<chrono::NaiveDate>().unwrap()
        );
        assert_eq!(
            current_rail_service_date(gmt_after),
            "2026-01-15".parse::<chrono::NaiveDate>().unwrap()
        );

        let midday: chrono::DateTime<chrono::Utc> = "2026-07-16T13:00:00Z".parse().unwrap();
        assert_eq!(
            current_rail_service_date(midday),
            "2026-07-16".parse::<chrono::NaiveDate>().unwrap()
        );
    }

    /// The finding's secondary effect, end to end through the real dispatch
    /// path: a train still running after 00:00Z must keep its
    /// resolved/pending-activation mapping, so its later movements are still
    /// attributed to the right line and the right day's population -- and
    /// only once the rail day actually closes is that state replaced.
    ///
    /// Under the old sequencing this test's second `dispatch_message` (at
    /// 00:30Z) landed on a `correlation_state` that had just been wiped and
    /// a `service_date` that had already moved to the next day, so it
    /// matched nothing at all: the movement went unattributed and the
    /// 00:00-02:00 window was correlated against the wrong day's population.
    #[test]
    fn a_train_running_past_midnight_keeps_its_mapping_until_the_rail_day_closes() {
        let mut service_date: chrono::NaiveDate = "2026-07-15".parse().unwrap();
        let next_day: chrono::NaiveDate = "2026-07-16".parse().unwrap();
        let stanox = waterloo_stanox_table();
        let tiploc_index = waterloo_tiploc_index();
        let mut population =
            population_for(&["2026-07-14".parse().unwrap(), service_date, next_day]);
        let mut correlation_state = correlate::CorrelationState::default();
        let mut station_state = station_correlate::StationCorrelationState::default();

        let dispatch = |messages: &[&str],
                        correlation_state: &mut correlate::CorrelationState,
                        station_state: &mut station_correlate::StationCorrelationState,
                        population: &population::Population,
                        service_date: chrono::NaiveDate| {
            for raw in messages {
                for message in trust_schema::schema::parse_batch(raw).unwrap() {
                    dispatch_message(
                        message,
                        correlation_state,
                        station_state,
                        &stanox,
                        &tiploc_index,
                        population,
                        service_date,
                    );
                }
            }
        };

        // 23:50Z on the 15th: activation + first movement, well inside the
        // rail day.
        dispatch(
            &[ACTIVATION_C11052, MOVEMENT_C11052],
            &mut correlation_state,
            &mut station_state,
            &population,
            service_date,
        );
        let key = ("waterloo-reading".to_string(), "C11052".to_string());
        assert!(correlation_state.derived.contains_key(&key));

        // 00:30Z on the 16th -- calendar day changed, rail day has not.
        let after_midnight: chrono::DateTime<chrono::Utc> = "2026-07-16T00:30:00Z".parse().unwrap();
        assert_eq!(
            rail_day_transition(service_date, after_midnight),
            RailDayTransition::Stay
        );
        dispatch(
            &[MOVEMENT_C11052],
            &mut correlation_state,
            &mut station_state,
            &population,
            service_date,
        );
        assert!(
            correlation_state.derived.contains_key(&key),
            "the still-running train's own line/uid mapping must survive UTC midnight"
        );
        assert_eq!(
            correlation_state
                .resolved
                .get("221832406")
                .map(String::as_str),
            Some("C11052"),
            "and so must its resolved train_id -> uid mapping"
        );

        // 01:00:01Z on the 16th -- the rail day has now closed. Yesterday's
        // final stats are written from the state that is still intact, and
        // only then is everything replaced.
        let after_close: chrono::DateTime<chrono::Utc> = "2026-07-16T01:00:01Z".parse().unwrap();
        let RailDayTransition::CloseAndRoll { closing, next } =
            rail_day_transition(service_date, after_close)
        else {
            panic!("the rail day must roll once it has closed");
        };

        let row = stats::build_line_row(
            "waterloo-reading",
            closing,
            &population.uids_for("waterloo-reading", closing),
            &correlation_state.derived,
            stats::rail_day_closed(closing, after_close),
            &common::Defaults::default(),
        );
        assert_eq!(row.availability, "available");
        assert_eq!(row.stats.total, 1);
        assert_eq!(
            row.stats.cancelled, 0,
            "the train was matched from live movements, so it is not counted as unobserved"
        );

        service_date = next;
        correlation_state = correlate::CorrelationState::default();
        station_state = station_correlate::StationCorrelationState::default();
        population.retain_from(service_date);

        assert_eq!(service_date, next_day);
        assert!(correlation_state.derived.is_empty());
        assert!(
            station_correlate::build_station_rows(
                &station_state,
                after_close,
                &common::Defaults::default()
            )
            .is_empty()
        );
        assert!(
            population
                .uids_for("waterloo-reading", "2026-07-14".parse().unwrap())
                .is_empty(),
            "and the rollover prunes the populations of days that can no longer be asked about"
        );
        assert_eq!(
            population.uids_for("waterloo-reading", next_day),
            vec!["C11052"],
            "while the new day's own population is kept"
        );
    }

    /// Regression test for the hour-long blind window: a FAILED stanox/crs
    /// fetch must not push the next attempt out by the full reload interval.
    /// With the 3600s default, a startup failure used to mean an hour during
    /// which every movement was matched against an empty crosswalk and then
    /// committed as processed.
    #[test]
    fn a_failed_stanox_crs_reload_retries_far_sooner_than_the_full_interval() {
        let default_interval = Duration::from_secs(3600);
        let retry = failed_reload_retry_delay(default_interval);
        assert_eq!(retry, Duration::from_secs(180));
        assert!(
            retry <= default_interval / 10,
            "a failure must cost minutes, not the full hour"
        );

        // Floored, so a small configured interval can't become a hot retry
        // loop against the OAuth endpoint...
        assert_eq!(
            failed_reload_retry_delay(Duration::from_secs(60)),
            Duration::from_secs(15)
        );
        // ...but never longer than the normal interval itself.
        let tiny = Duration::from_secs(5);
        assert_eq!(failed_reload_retry_delay(tiny), tiny);
    }

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
            change_time_minutes: None,
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
