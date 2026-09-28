//! `full-coverage-consumer`: a second, independent consumer of the same RDM
//! Train Movements feed `trust-consumer` reads -- by default its own
//! consumer group on the `movement-events` Redis Stream that
//! `movement-relay` fills (`--movement-feed-backend kafka` reads RDM's
//! Kafka topic directly instead) -- correlating every event against the
//! FULL scheduled population of every shadow-computed line (not a small
//! pinned-train set) -- see
//! docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md and
//! docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md.
//! Writes per-line/per-station stats for every shadow-computed line; the
//! aggregator only reads them into a line's severity/`DataQuality` for a
//! line that is full-coverage-enabled (`LineDefinition.full_coverage_enabled`,
//! or the aggregator's `--full-coverage-enabled-default`) -- see
//! `aggregator::aggregation::merge_full_coverage`.
//!
//! # Startup (2026-09-27)
//!
//! Nothing is consumed until the process can correlate correctly:
//!
//! 1. The stanox/crs crosswalk is loaded, retrying on a short backoff
//!    (the shadow line set is resolved from it).
//! 2. The background population reloader (`population_reload`) is started,
//!    and its first usable load awaited -- see that module's doc for why
//!    events used to be matched against an empty population here.
//! 3. The current rail day is REBUILT by a group-less replay of
//!    `movement-events` from the day's start up to the group's read
//!    position (`replay`), or marked partial when that cannot be complete.
//!
//! Only then does the loop below start reading as the consumer group.
//!
//! # Windowed stats (2026-09-27, off by default)
//!
//! With `FULL_COVERAGE_WINDOWED_STATS=true`
//! (docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md)
//! the consumer also keeps per-train TRUST state by service date
//! (`trains`), reduces each line's population to its relevant trains' due
//! times (`population`), and on every stats write classifies the trains
//! already due into a `recent` window (due in the last
//! `FULL_COVERAGE_RECENT_WINDOW_MINUTES`, ending
//! `FULL_COVERAGE_GRACE_MINUTES` ago) and a `day_to_date` window
//! (`windows`), posted to `/private/full-coverage-window-stats`. The
//! `full_coverage_line_stats` row then carries the day-to-date counts
//! (the whole day once closed) as `stats_version` 2. The replay also starts
//! `replay::LOOKBACK` before the rail day, so the day's first trains'
//! Activations are seen. With the flag off, every row is exactly the legacy
//! one.
//!
//! # Loop shape (Task 13)
//!
//! Mirrors `trust-consumer/src/main.rs`'s multi-cadence-in-one-loop shape
//! (stanox-crs reload / gap check / consume-and-correlate / stats write, each
//! on its own timer, all checked once per iteration). The population reload
//! is NOT in this loop any more: it runs in its own task and swaps whole
//! snapshots in, so a slow `api` can no longer stall consumption.
//!
//! # Why a batch is ACKed as soon as it is in memory -- and what that costs
//!
//! `trust-consumer` only commits after a successful POST to `api`, because
//! a failed post there loses a real tracked-train event. This crate commits
//! (XACKs) a batch as soon as it has been dispatched into in-memory
//! correlation state, decoupled from the periodic stats POST: a stats POST
//! that fails is simply retried next cycle with fresher data, and
//! re-dispatching a redelivered batch is harmless because `DerivedState`
//! fields are last-write-wins per event, not additive.
//!
//! The cost is that an ACKed entry is never redelivered, so the group
//! itself can NOT restore that state after a restart. (This doc used to
//! claim that redelivery "would just re-derive the same state" -- true of a
//! redelivered batch, but ACKed entries are not redelivered, and nothing
//! else restored them: every restart silently wiped the rail day so far,
//! and every train seen before it was then counted as cancelled.) Startup
//! step 3 is what restores it now, from the stream itself.

mod config;
mod correlate;
mod day;
mod feed;
mod population;
mod population_reload;
mod queries;
mod replay;
mod stanox_tiploc;
mod station_correlate;
mod stats;
mod trains;
mod windows;

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use clap::Parser;
use config::{Config, MovementFeedBackend};
use day::{DayState, Lookups};
use feed::MovementFeed;
use feed::kafka::KafkaMovementFeed;
use movement_feed::ActiveFeed;
use movement_feed::DeadLetterSink;
use movement_feed::redis_stream::RedisStreamMovementFeed;
use population_reload::{SharedGeometry, SharedLineIds, SharedPopulation};
use stats::current_rail_service_date;

#[cfg(test)]
use day::dispatch_message;

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
    init_metrics();
    let (connection_state, progress) = health_http::spawn_with_progress(
        config.health_bind_url.clone(),
        "connected",
        "disconnected",
        Duration::from_secs(config.progress_stall_secs),
    );
    let http = common::ingest::consumer_http_client()?;
    let internal_oauth = Arc::new(config.internal_oauth.token_cache());

    let mut feed = match config.movement_feed_backend {
        MovementFeedBackend::Kafka => {
            ActiveFeed::Kafka(KafkaMovementFeed::connect(&config, connection_state)?)
        }
        MovementFeedBackend::RedisStream => ActiveFeed::RedisStream(
            // Redis down at startup is waited for (each attempt logged,
            // beating progress so /livez stays 200); afterwards every Redis
            // command is bounded and a failure is retried by this loop.
            // See `common::redis_conn`.
            Box::new(
                RedisStreamMovementFeed::connect_until_ready(
                    common::redis_auth::redis_url_with_password(
                        &config.redis_url,
                        config.redis_password.as_ref(),
                    )?
                    .expose(),
                    "full-coverage-consumer",
                    "full-coverage-consumer-1",
                    Duration::from_secs(config.redis_autoclaim_min_idle_secs),
                    common::startup::CONNECT_BACKOFF,
                    &progress,
                )
                .await?,
            ),
            connection_state,
            "full_coverage_consumer_ready",
        ),
    };
    let redis_gap_check_interval = Duration::from_secs(config.redis_gap_check_secs);
    let mut last_redis_gap_check = tokio::time::Instant::now() - redis_gap_check_interval;
    let defaults = common::Defaults::default();

    // Startup step 1: the stanox/crs crosswalk. `lookups.tiploc_index` and
    // the shadow line set are both resolved from the live, CIF-derived
    // stanox_crs snapshot (the 2026-09-09/11 tiploc-schedule-matching-gap
    // fixes), so neither can be built from `config.lines` alone -- and the
    // population reload needs the line set.
    let mut lookups = Lookups::default();
    let line_ids: SharedLineIds = Arc::new(ArcSwap::from_pointee(Vec::new()));
    // Stays empty while windowed stats are off: nothing is reduced.
    let geometry: SharedGeometry = Arc::new(ArcSwap::from_pointee(Default::default()));
    let stanox_crs_reload_interval = Duration::from_secs(config.stanox_crs_reload_secs);
    load_stanox_crs_until_ok(
        &http,
        &config,
        &internal_oauth,
        &mut lookups,
        &line_ids,
        &geometry,
        stanox_crs_reload_interval,
        &progress,
    )
    .await;
    // How long to wait before the NEXT stanox/crs reload attempt: the full
    // interval after a success, a much shorter backoff after a failure (see
    // `failed_reload_retry_delay`).
    let mut stanox_crs_wait = stanox_crs_reload_interval;
    let mut last_stanox_crs_reload = tokio::time::Instant::now();

    // Startup step 2: the population, loaded (and from now on reloaded) in
    // the background.
    let population: SharedPopulation =
        Arc::new(ArcSwap::from_pointee(population::Population::default()));
    let population_reload_interval = Duration::from_secs(config.population_reload_secs);
    let mut first_load = population_reload::Reloader {
        client: http.clone(),
        url: config.schedule_line_population_url.clone(),
        tokens: Arc::clone(&internal_oauth),
        line_ids: Arc::clone(&line_ids),
        geometry: Arc::clone(&geometry),
        population: Arc::clone(&population),
        interval: population_reload_interval,
        min_retry: Duration::from_secs(1),
        max_retry: failed_reload_retry_delay(population_reload_interval),
        initial_wait: Duration::from_secs(config.population_initial_wait_secs),
    }
    .spawn();

    // Startup step 3: rebuild the rail day in progress, then consume.
    let mut day = start_consuming(
        &mut feed,
        &mut first_load,
        &population,
        &lookups,
        &progress,
        config.windowed.enabled,
    )
    .await?;

    let stats_write_interval = Duration::from_secs(config.stats_write_interval_secs);
    let mut last_stats_write = tokio::time::Instant::now() - stats_write_interval;

    loop {
        // 0. rail-day rollover, and NOT a `Utc::now().date_naive() !=
        // service_date` check.
        //
        // The calendar day changes at 00:00Z, but `service_date`'s rail day
        // does not END until 02:00 Europe/London the following day
        // (01:00Z under BST). Rolling over at 00:00Z therefore wiped the
        // correlation state one to two hours BEFORE the day it belonged to
        // had closed, and -- because `service_date` had already been
        // replaced by the time the stats write ran -- made the "available"
        // availability state structurally unreachable.
        //
        // So the rollover happens at the real rail-day boundary, and the
        // day that just closed gets its final, genuinely-closeable stats
        // write BEFORE its state is replaced. The day entered here is never
        // partial: this process sees it from its first event.
        let now = chrono::Utc::now();
        if let RailDayTransition::CloseAndRoll { closing, next } =
            rail_day_transition(day.service_date, now)
        {
            // Yesterday's closing write, with `rail_day_closed(closing,
            // now)` now genuinely true -- this is the row that reads
            // "available" (unless the day was partial).
            debug_assert_eq!(day.service_date, closing);
            write_stats(
                &http,
                &config,
                &internal_oauth,
                &line_ids.load_full(),
                &population.load_full(),
                &geometry,
                &day,
                &defaults,
            )
            .await;
            last_stats_write = tokio::time::Instant::now();

            tracing::info!(closed = %closing, new = %next, carried_activations = day.next_activations.len(), "rail day closed; wrote its final stats, then reset correlation state (keeping the next day's activations)");
            day = day.roll(next);
            publish_day_partial_metrics(&day);
        }

        // 1. stanox_crs reload. A FAILED fetch must not wait the full
        // interval before trying again (it used to be 3600s of matching
        // against a stale or empty crosswalk), so the wait after a failure
        // is a short backoff, not the interval.
        if last_stanox_crs_reload.elapsed() >= stanox_crs_wait {
            match reload_stanox_crs(
                &http,
                &config,
                &internal_oauth,
                &mut lookups,
                &line_ids,
                &geometry,
            )
            .await
            {
                Ok(()) => stanox_crs_wait = stanox_crs_reload_interval,
                Err(err) => {
                    stanox_crs_wait = failed_reload_retry_delay(stanox_crs_reload_interval);
                    tracing::error!(error = ?err, retry_in_secs = stanox_crs_wait.as_secs(), "failed to reload stanox/crs table; keeping previous snapshot and retrying sooner than the normal interval");
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
                        unread_entries_lost = ?gap.unread_entries_lost,
                        pending_entries_trimmed = gap.pending_entries_trimmed,
                        "movement-events stream gap detected: events were trimmed before full-coverage-consumer finished with them -- this biases this consumer's shadow-mode SampleStats for the affected window (e.g. inflating the unconfirmed-by-window-close = cancelled bucket); treat any rail day during which a gap was detected as not clean signal"
                    );
                    metrics::counter!(common::metrics::metric_name(
                        "full_coverage_consumer_stream_gap_detected_total"
                    ))
                    .increment(1);
                    // Everything before now is suspect for the rest of the
                    // day: no window before it may influence severity, and
                    // nothing before it is presumed cancelled.
                    day.observed_from = chrono::Utc::now();
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(error = ?err, "failed to check movement-events stream for a gap; will retry next cycle");
                }
            }
            last_redis_gap_check = tokio::time::Instant::now();
        }

        // 2. consume + correlate, against whatever population snapshot is
        // current -- never waiting on a reload. `load_full` (an owned
        // `Arc`), not `load`: the snapshot is held across `next_batch`'s
        // blocking read, and `ArcSwap` guards are meant to be short-lived.
        let cycle_start = std::time::Instant::now();
        consume_once(&mut feed, &mut day, &lookups, &population.load_full()).await;
        metrics::histogram!(common::metrics::metric_name(
            "full_coverage_consumer_cycle_duration_seconds"
        ))
        .record(cycle_start.elapsed().as_secs_f64());

        // 3. stats write.
        if last_stats_write.elapsed() >= stats_write_interval {
            write_stats(
                &http,
                &config,
                &internal_oauth,
                &line_ids.load_full(),
                &population.load_full(),
                &geometry,
                &day,
                &defaults,
            )
            .await;
            last_stats_write = tokio::time::Instant::now();
        }

        // One loop iteration completed, however it went -- see
        // `health_http::Progress`.
        progress.beat();
    }
}

/// Creates every counter/gauge an alert keys on at its "nothing happened"
/// value, so a missing series means "never scraped", not "no gap" (the
/// 2026-09-27 review found `stream_gap_detected_total` had never existed in
/// Prometheus at all, which is indistinguishable from the check never
/// running).
fn init_metrics() {
    for counter in [
        "full_coverage_consumer_stream_gap_detected_total",
        "full_coverage_consumer_startup_replay_entries_total",
        "full_coverage_consumer_window_rows_posted_total",
    ] {
        metrics::counter!(common::metrics::metric_name(counter)).increment(0);
    }
    for gauge in [
        "full_coverage_consumer_population_loaded",
        "full_coverage_consumer_startup_complete",
        "full_coverage_consumer_day_partial",
        "full_coverage_consumer_lines_partial",
        "full_coverage_consumer_window_feed_stale",
        "full_coverage_consumer_pending_trains",
        "full_coverage_consumer_window_presumed_cancelled",
    ] {
        metrics::gauge!(common::metrics::metric_name(gauge)).set(0.0);
    }
    for msg_type in ["0002", "0005", "0006"] {
        metrics::counter!(
            common::metrics::metric_name("full_coverage_consumer_unattributed_total"),
            "msg_type" => msg_type
        )
        .increment(0);
    }
    // DistantSignalFullCoverageWindowPostErrors keys on this series; at 0
    // from startup so `increase()` sees the first failed window POST.
    metrics::counter!(
        common::metrics::metric_name("full_coverage_consumer_errors_total"),
        "operation" => "post_window_stats"
    )
    .increment(0);
}

/// Waits for the first population load, then replays the current rail day
/// into a fresh [`DayState`]. Nothing is read from the group before this
/// returns -- that ordering is the point, and is what the tests pin.
async fn start_consuming<F: replay::ReplaySource>(
    feed: &mut F,
    first_load: &mut tokio::sync::watch::Receiver<Option<population_reload::FirstLoad>>,
    population: &SharedPopulation,
    lookups: &Lookups,
    progress: &health_http::Progress,
    windowed: bool,
) -> anyhow::Result<DayState> {
    let first = population_reload::wait_for_first_load(first_load, progress).await?;
    let mut day = DayState::new(current_rail_service_date(chrono::Utc::now()));
    if windowed {
        day = day.enable_windowed();
    }
    if first.service_date == day.service_date {
        day.partial_lines.extend(first.missing_lines);
    }

    let started = std::time::Instant::now();
    let report = replay::run_startup_replay(
        feed,
        &mut day,
        lookups,
        population,
        progress,
        replay::REPLAY_PAGE_SIZE,
    )
    .await;
    let elapsed = started.elapsed();
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_startup_replay_seconds"
    ))
    .set(elapsed.as_secs_f64());
    tracing::info!(
        service_date = %day.service_date,
        entries = report.entries,
        skipped_pending = report.skipped_pending,
        parse_errors = report.parse_errors,
        elapsed_secs = elapsed.as_secs_f64(),
        partial = ?day.partial_reason.map(day::PartialReason::as_str),
        partial_lines = day.partial_lines.len(),
        "startup replay complete; consuming as the group"
    );
    publish_day_partial_metrics(&day);
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_startup_complete"
    ))
    .set(1.0);
    Ok(day)
}

fn publish_day_partial_metrics(day: &DayState) {
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_day_partial"
    ))
    .set(if day.partial_reason.is_some() {
        1.0
    } else {
        0.0
    });
    if let Some(reason) = day.partial_reason {
        tracing::warn!(service_date = %day.service_date, reason = reason.as_str(), "this rail day's stats are partial");
    }
}

/// One read-dispatch-commit step against the group.
async fn consume_once<F: MovementFeed + DeadLetterSink>(
    feed: &mut F,
    day: &mut DayState,
    lookups: &Lookups,
    population: &population::Population,
) {
    let batch = match feed.next_batch().await {
        Ok(batch) => batch,
        Err(err) => {
            tracing::error!(error = ?err, "error receiving from movement feed");
            metrics::counter!(
                common::metrics::metric_name("full_coverage_consumer_errors_total"),
                "operation" => "movement_feed_receive"
            )
            .increment(1);
            tokio::time::sleep(ERROR_BACKOFF).await;
            return;
        }
    };
    let mut unparseable = Vec::new();
    for raw in &batch {
        if let Err(err) = day.dispatch_payload(raw, lookups, population, chrono::Utc::now()) {
            tracing::error!(error = ?err, raw = %raw, "failed to parse TRUST batch; dead-lettering this payload");
            metrics::counter!(
                common::metrics::metric_name("full_coverage_consumer_errors_total"),
                "operation" => "parse_batch"
            )
            .increment(1);
            unparseable.push(movement_feed::DeadLetter {
                reason: "unparseable_payload",
                source_id: None,
                delivery_count: None,
                payload: raw.clone(),
                detail: format!("{err:?}"),
            });
        }
    }
    // An unparseable payload is kept, not just logged. If it cannot be
    // stored, the batch is left un-ACKed rather than lose it (re-dispatching
    // the rest on redelivery is harmless -- last-write-wins, see this
    // module's doc).
    if let Err(err) = feed.dead_letter(&unparseable).await {
        tracing::error!(error = ?err, "failed to dead-letter unparseable payloads; not committing this batch");
        metrics::counter!(
            common::metrics::metric_name("full_coverage_consumer_errors_total"),
            "operation" => "dead_letter"
        )
        .increment(1);
        tokio::time::sleep(ERROR_BACKOFF).await;
    }
    // Commit as soon as the batch is dispatched into in-memory state -- see
    // this module's doc for why, and for what restores that state after a
    // restart.
    else if let Err(err) = feed.commit().await {
        tracing::error!(error = ?err, "failed to commit movement feed offsets");
        metrics::counter!(
            common::metrics::metric_name("full_coverage_consumer_errors_total"),
            "operation" => "commit_offsets"
        )
        .increment(1);
    }
}

/// Fetches the stanox/crs crosswalk and rebuilds everything derived from
/// it: the STANOX table, the TIPLOC -> line index and the shadow line set
/// (which the population reloader reads).
async fn reload_stanox_crs(
    client: &reqwest::Client,
    config: &Config,
    tokens: &common::oauth_client::OAuthTokenCache,
    lookups: &mut Lookups,
    line_ids: &SharedLineIds,
    geometry: &SharedGeometry,
) -> anyhow::Result<()> {
    let records = match queries::fetch_stanox_crs(client, &config.stanox_crs_url, tokens).await {
        Ok(records) => records,
        Err(err) => {
            metrics::counter!(
                common::metrics::metric_name("full_coverage_consumer_errors_total"),
                "operation" => "reload_stanox_crs"
            )
            .increment(1);
            return Err(err);
        }
    };
    lookups.stanox = stanox_tiploc::StanoxTable::from_records(&records);
    lookups.tiploc_index = population::build_tiploc_index(&config.lines, &records);
    line_ids.store(Arc::new(config.shadow_line_ids(&records)));
    if config.windowed.enabled {
        // A line whose geometry changes here is re-downloaded by the next
        // population reload (its held population's hash no longer matches).
        geometry.store(Arc::new(population::build_line_geometry(
            &config.lines,
            &records,
        )));
    }
    Ok(())
}

/// Startup step 1: the crosswalk every other step depends on, retried from
/// 1 s, doubling, capped at the normal failure backoff.
#[allow(clippy::too_many_arguments)]
async fn load_stanox_crs_until_ok(
    client: &reqwest::Client,
    config: &Config,
    tokens: &common::oauth_client::OAuthTokenCache,
    lookups: &mut Lookups,
    line_ids: &SharedLineIds,
    geometry: &SharedGeometry,
    interval: Duration,
    progress: &health_http::Progress,
) {
    let cap = failed_reload_retry_delay(interval).min(Duration::from_secs(30));
    let mut backoff = Duration::from_secs(1).min(cap);
    loop {
        match reload_stanox_crs(client, config, tokens, lookups, line_ids, geometry).await {
            Ok(()) => return,
            Err(err) => {
                tracing::error!(error = ?err, retry_in_secs = backoff.as_secs(), "failed to load the stanox/crs table at startup; not consuming until it loads");
                progress.beat();
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(cap);
            }
        }
    }
}

/// How long to wait before retrying after a feed-level failure -- flat,
/// not exponential, same reasoning as `trust-consumer::main::ERROR_BACKOFF`:
/// the stream holds the backlog, so there's nothing to drain, only a log/API
/// to avoid hammering.
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

/// How long to wait before re-attempting a reload that FAILED -- a
/// twentieth of the normal interval (3 minutes at the stanox/crs 3600s
/// default, 15s at the population 300s default), floored at 15s so a
/// misconfigured tiny interval can't turn into a hot retry loop against
/// the OAuth endpoint, and never longer than the normal interval itself.
fn failed_reload_retry_delay(interval: Duration) -> Duration {
    const FLOOR: Duration = Duration::from_secs(15);
    if interval <= FLOOR {
        return interval;
    }
    let backoff = interval / 20;
    if backoff < FLOOR { FLOOR } else { backoff }
}

/// For every shadow-computed line, builds and POSTs its stats row; for
/// every populated `(crs, toc_id)` station bucket, builds and POSTs its
/// sample. Independent, best-effort failures -- Decision 3's "no shared
/// transaction" rule: one line's or one bucket's POST failing must not
/// block any other's.
///
/// With `FULL_COVERAGE_WINDOWED_STATS=true` each line's row is the v2
/// day-to-date (closed-day, once closed) row, and every line's `recent` and
/// `day_to_date` windows are posted as one batch.
#[allow(clippy::too_many_arguments)]
async fn write_stats(
    client: &reqwest::Client,
    config: &Config,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
    shadow_line_ids: &[String],
    population: &population::Population,
    geometry: &SharedGeometry,
    day: &DayState,
    defaults: &common::Defaults,
) {
    let now = chrono::Utc::now();
    let service_date = day.service_date;
    let closed = stats::rail_day_closed(service_date, now);

    let mut line_rows = Vec::new();
    let mut available_count = 0u64;
    let mut pending_count = 0u64;
    let mut partial_count = 0u64;
    let windowed = day
        .trains
        .as_ref()
        .filter(|_| config.windowed.enabled)
        .map(|trains| (trains, windowed_write_context(config, day, geometry, now)));
    let mut window_rows = Vec::new();
    for line_id in shadow_line_ids {
        let partial = day.is_line_partial(line_id);
        let row = match &windowed {
            // Windowed stats (off by default): the v2 row is the
            // day-to-date window, and both windows are posted.
            Some((trains, ctx)) => {
                let Some(pop) = population.line_pop(line_id, service_date) else {
                    // Nothing published for this line yet: the same empty
                    // pending row the legacy path writes, and no windows.
                    line_rows.push(stats::build_line_row(
                        line_id,
                        service_date,
                        &[],
                        &day.correlation.derived,
                        closed,
                        partial,
                        defaults,
                    ));
                    if partial {
                        partial_count += 1;
                    }
                    pending_count += 1;
                    continue;
                };
                let thresholds = ctx.thresholds.get(line_id.as_str()).unwrap_or(defaults);
                let inputs = windows::LineInputs {
                    line_id,
                    service_date,
                    pop,
                    trains,
                    geometry: ctx.geometry.get(line_id).map(Arc::as_ref),
                    thresholds,
                    observed_from: day.observed_from,
                    feed_stale: ctx.feed_stale,
                    line_partial: partial,
                };
                let [recent, day_to_date] =
                    windows::window_ranges(service_date, now, &ctx.params, closed)
                        .map(|(kind, from, to)| inputs.window(kind, from, to, now));
                let row = windows::line_row_v2(&day_to_date, closed);
                window_rows.push(recent);
                window_rows.push(day_to_date);
                row
            }
            None => {
                let population_uids = population.uids_for(line_id, service_date);
                stats::build_line_row(
                    line_id,
                    service_date,
                    &population_uids,
                    &day.correlation.derived,
                    closed,
                    partial,
                    defaults,
                )
            }
        };
        if row.availability == "available" {
            available_count += 1;
        } else {
            pending_count += 1;
        }
        if partial {
            partial_count += 1;
        }
        line_rows.push(row);
    }
    if let Some((trains, ctx)) = &windowed {
        metrics::gauge!(common::metrics::metric_name(
            "full_coverage_consumer_parked_messages"
        ))
        .set(trains.parked_count() as f64);
        post_windows(client, config, internal_oauth, &window_rows, ctx.feed_stale).await;
    }
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_lines_available_total"
    ))
    .set(available_count as f64);
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_lines_pending_total"
    ))
    .set(pending_count as f64);
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_lines_partial"
    ))
    .set(partial_count as f64);

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

    let station_rows = station_correlate::build_station_rows(&day.stations, now, defaults);
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

/// What every line of one windowed stats write shares.
struct WindowedWriteContext {
    params: windows::WindowParams,
    feed_stale: bool,
    /// Per-line merged thresholds (`severity_overrides` on `Defaults`).
    thresholds: std::collections::HashMap<String, common::Defaults>,
    geometry: Arc<std::collections::HashMap<String, Arc<population::LineGeometry>>>,
}

fn window_params(config: &Config) -> windows::WindowParams {
    windows::WindowParams {
        recent_minutes: config.windowed.full_coverage_recent_window_minutes,
        grace_minutes: config.windowed.full_coverage_grace_minutes,
        activations_min: config.windowed.full_coverage_activations_min,
        feed_stale_secs: config.windowed.full_coverage_feed_stale_secs,
    }
}

fn windowed_write_context(
    config: &Config,
    day: &DayState,
    geometry: &SharedGeometry,
    now: chrono::DateTime<chrono::Utc>,
) -> WindowedWriteContext {
    let params = window_params(config);
    let feed_stale = windows::feed_stale(
        day.last_event_at,
        day.activations_in_last_hour(now),
        now,
        &params,
    );
    let defaults = common::Defaults::default();
    WindowedWriteContext {
        params,
        feed_stale,
        thresholds: config
            .lines
            .iter()
            .map(|line| {
                (
                    line.id.clone(),
                    common::thresholds_for(&defaults, &line.severity_overrides),
                )
            })
            .collect(),
        geometry: geometry.load_full(),
    }
}

/// Seconds since the Unix epoch of the last "window POST failed" warning:
/// a new consumer against an old api gets a 404 every minute, logged at
/// warn at most once per 10 minutes.
static LAST_WINDOW_POST_WARNING: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

async fn post_windows(
    client: &reqwest::Client,
    config: &Config,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
    rows: &[common::FullCoverageWindowStatsRow],
    feed_stale: bool,
) {
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_window_feed_stale"
    ))
    .set(if feed_stale { 1.0 } else { 0.0 });
    let recent = rows
        .iter()
        .filter(|r| r.window_kind == common::FullCoverageWindowKind::Recent);
    let (pending, presumed) = recent.fold((0u64, 0u64), |(p, c), r| {
        (
            p + u64::from(r.counts.pending),
            c + u64::from(r.counts.cancelled_presumed),
        )
    });
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_pending_trains"
    ))
    .set(pending as f64);
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_window_presumed_cancelled"
    ))
    .set(presumed as f64);
    match queries::post_full_coverage_window_stats(
        client,
        &config.windowed.full_coverage_window_stats_url,
        internal_oauth,
        rows,
    )
    .await
    {
        Ok(()) => {
            metrics::counter!(common::metrics::metric_name(
                "full_coverage_consumer_window_rows_posted_total"
            ))
            .increment(rows.len() as u64);
        }
        Err(err) => {
            metrics::counter!(
                common::metrics::metric_name("full_coverage_consumer_errors_total"),
                "operation" => "post_window_stats"
            )
            .increment(1);
            let now = chrono::Utc::now().timestamp();
            let last = LAST_WINDOW_POST_WARNING.load(std::sync::atomic::Ordering::Relaxed);
            if now - last >= 600 {
                LAST_WINDOW_POST_WARNING.store(now, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(error = ?err, "failed to post full-coverage window stats (an api without /full-coverage-window-stats answers 404); will retry next cycle, next warning in 10 minutes at the earliest");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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
                    operator_atoc: None,
                    train_status: None,
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
            false,
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
            false,
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
                operator_atoc: None,
                train_status: None,
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

    // --- 2026-09-27: restart replay, first-load gating, background reload ---

    fn shared(population: population::Population) -> SharedPopulation {
        Arc::new(ArcSwap::from_pointee(population))
    }

    fn waterloo_lookups() -> Lookups {
        Lookups {
            stanox: waterloo_stanox_table(),
            tiploc_index: waterloo_tiploc_index(),
        }
    }

    fn progress() -> health_http::Progress {
        health_http::Progress::new(Duration::from_secs(600))
    }

    /// Activation + movement for a second train, so a test can tell "seen
    /// before the restart" from "seen after".
    const ACTIVATION_C22222: &str = r#"{"header":{"msg_type":"0001"},"body":{
        "train_id":"331832406","train_uid":"C22222","toc_id":"SW",
        "train_service_code":"22345000","schedule_wtt_id":"1235",
        "schedule_start_date":"2026-07-15","schedule_end_date":"2026-07-15"
    }}"#;
    const MOVEMENT_C22222: &str = r#"{"header":{"msg_type":"0003"},"body":{
        "train_id":"331832406","event_type":"DEPARTURE",
        "planned_timestamp":"1787941920000","actual_timestamp":"1787941980000",
        "loc_stanox":"87212","variation_status":"LATE"
    }}"#;

    /// Population for the CURRENT rail day (the stream tests below write
    /// real `*` ids, which always fall inside it): the two trains the
    /// stream mentions, plus one that never runs.
    fn todays_population() -> (chrono::NaiveDate, population::Population) {
        let today = current_rail_service_date(chrono::Utc::now());
        let mut population = population::Population::default();
        population.insert(
            "waterloo-reading",
            today,
            ["C11052", "C22222", "C99999"]
                .iter()
                .map(|uid| schedule_query::LinePopulationEntry {
                    uid: uid.to_string(),
                    calling_points: vec![],
                    operator_atoc: None,
                    train_status: None,
                })
                .collect(),
        );
        (today, population)
    }

    fn row_for(
        day: &DayState,
        population: &population::Population,
    ) -> common::FullCoverageLineStatsRow {
        stats::build_line_row(
            "waterloo-reading",
            day.service_date,
            &population.uids_for("waterloo-reading", day.service_date),
            &day.correlation.derived,
            false,
            day.is_line_partial("waterloo-reading"),
            &common::Defaults::default(),
        )
    }

    mod stream {
        use super::*;
        use redis::AsyncCommands;

        fn redis_url() -> String {
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into())
        }

        fn unique_stream(name: &str) -> String {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("fcc-test-{name}-{nanos}")
        }

        async fn conn() -> redis::aio::ConnectionManager {
            redis::Client::open(redis_url())
                .unwrap()
                .get_connection_manager()
                .await
                .unwrap()
        }

        async fn xadd(stream: &str, payload: &str) -> String {
            let mut conn = conn().await;
            conn.xadd(stream, "*", &[("payload", payload)])
                .await
                .unwrap()
        }

        async fn cleanup(stream: &str) {
            let mut conn = conn().await;
            let _: redis::RedisResult<i64> = conn
                .del(&[stream.to_string(), format!("{stream}-deadletter")])
                .await;
        }

        async fn connect(stream: &str) -> RedisStreamMovementFeed {
            RedisStreamMovementFeed::connect_to_named_stream(
                &redis_url(),
                stream,
                "full-coverage-consumer",
                "full-coverage-consumer-1",
                Duration::from_secs(3600),
            )
            .await
            .unwrap()
        }

        /// The data-damage bug of the 2026-09-27 review, end to end: a
        /// process consumes (and ACKs) part of the rail day, dies, and a new
        /// one starts. Without the replay the new process's first stats row
        /// counted every train seen before the restart as cancelled; with it,
        /// the day is rebuilt from the stream -- and entries left pending by
        /// the crash are neither lost nor applied twice.
        #[tokio::test]
        #[ignore = "needs REDIS_URL (local valkey)"]
        async fn a_mid_day_restart_rebuilds_the_day_and_counts_nothing_seen_as_cancelled() {
            let stream = unique_stream("restart-rebuild");
            let (_, population) = todays_population();
            let population = shared(population);
            let lookups = waterloo_lookups();

            // Process 1: C11052 consumed and ACKed; C22222's batch delivered
            // but the process dies before ACKing it.
            let mut feed = connect(&stream).await;
            xadd(&stream, ACTIVATION_C11052).await;
            xadd(&stream, MOVEMENT_C11052).await;
            let mut day = DayState::new(current_rail_service_date(chrono::Utc::now()));
            consume_once(&mut feed, &mut day, &lookups, &population.load()).await; // empty startup PEL
            consume_once(&mut feed, &mut day, &lookups, &population.load()).await;
            xadd(&stream, ACTIVATION_C22222).await;
            xadd(&stream, MOVEMENT_C22222).await;
            assert_eq!(
                feed.next_batch().await.unwrap().len(),
                2,
                "delivered, never ACKed"
            );
            drop(feed);

            // What the old code did on restart: a fresh, empty day.
            let fresh = DayState::new(current_rail_service_date(chrono::Utc::now()));
            assert_eq!(
                row_for(&fresh, &population.load()).stats.cancelled,
                3,
                "the bug: every train, seen or not, reads as cancelled"
            );

            // Process 2.
            let mut feed = connect(&stream).await;
            let (_tx, mut rx) = tokio::sync::watch::channel(Some(population_reload::FirstLoad {
                service_date: fresh.service_date,
                missing_lines: vec![],
            }));
            let mut day = start_consuming(
                &mut feed,
                &mut rx,
                &population,
                &lookups,
                &progress(),
                false,
            )
            .await
            .unwrap();
            assert_eq!(
                day.partial_reason, None,
                "the whole day is still in the stream"
            );
            let key = ("waterloo-reading".to_string(), "C11052".to_string());
            assert!(
                day.correlation.derived.contains_key(&key),
                "rebuilt from the replay"
            );
            assert!(
                !day.correlation
                    .derived
                    .contains_key(&("waterloo-reading".to_string(), "C22222".to_string())),
                "pending entries are left to the group's redelivery, not replayed"
            );

            // The group redelivers the unACKed batch; nothing is lost.
            consume_once(&mut feed, &mut day, &lookups, &population.load()).await;
            let row = row_for(&day, &population.load());
            assert_eq!(row.stats.total, 3);
            assert_eq!(
                row.stats.cancelled, 1,
                "only C99999, which never ran, is unobserved -- nothing seen before the restart is"
            );
            assert!(!row.partial);

            // And live consumption continues from where the group was.
            xadd(&stream, MOVEMENT_C11052).await;
            let batch = feed.next_batch().await.unwrap();
            assert_eq!(
                batch.len(),
                1,
                "only the new entry, not a second copy of the day"
            );
            cleanup(&stream).await;
        }

        /// The day's first entries are gone from the stream (trimmed before
        /// this restart): the replay can only be partial, so the day is marked
        /// partial -- surfaced on every row, never "available", and unseen
        /// trains are not counted as cancelled.
        #[tokio::test]
        #[ignore = "needs REDIS_URL (local valkey)"]
        async fn a_trimmed_day_start_marks_the_day_partial() {
            let stream = unique_stream("trimmed-start");
            let (_, population) = todays_population();
            let population = shared(population);
            let lookups = waterloo_lookups();

            let mut feed = connect(&stream).await;
            xadd(&stream, ACTIVATION_C11052).await;
            xadd(&stream, MOVEMENT_C11052).await;
            let mut day = DayState::new(current_rail_service_date(chrono::Utc::now()));
            consume_once(&mut feed, &mut day, &lookups, &population.load()).await;
            consume_once(&mut feed, &mut day, &lookups, &population.load()).await;
            // The relay's MAXLEN trims C11052's activation and movement away.
            let mut raw = conn().await;
            let _: String = redis::cmd("XADD")
                .arg(&stream)
                .arg("MAXLEN")
                .arg(1)
                .arg("*")
                .arg("payload")
                .arg(ACTIVATION_C22222)
                .query_async(&mut raw)
                .await
                .unwrap();
            drop(feed);

            let mut feed = connect(&stream).await;
            let (_tx, mut rx) = tokio::sync::watch::channel(Some(population_reload::FirstLoad {
                service_date: day.service_date,
                missing_lines: vec![],
            }));
            let day = start_consuming(
                &mut feed,
                &mut rx,
                &population,
                &lookups,
                &progress(),
                false,
            )
            .await
            .unwrap();
            assert_eq!(
                day.partial_reason,
                Some(day::PartialReason::DayStartTrimmed)
            );

            let row = stats::build_line_row(
                "waterloo-reading",
                day.service_date,
                &population
                    .load()
                    .uids_for("waterloo-reading", day.service_date),
                &day.correlation.derived,
                true, // even once the day has closed
                day.is_line_partial("waterloo-reading"),
                &common::Defaults::default(),
            );
            assert!(row.partial);
            assert_eq!(row.availability, "pending");
            assert_eq!(row.stats.cancelled, 0, "unseen is unknown, not cancelled");
            cleanup(&stream).await;
        }
    }

    /// Kafka has no group-less replay: a process starting mid-day there
    /// cannot rebuild the day, so the day is partial.
    #[tokio::test]
    async fn a_backend_without_replay_marks_the_starting_day_partial() {
        let mut feed: ActiveFeed<movement_feed::FakeMovementFeed> =
            ActiveFeed::Kafka(movement_feed::FakeMovementFeed::new(vec![]));
        let (_, population) = todays_population();
        let (_tx, mut rx) = tokio::sync::watch::channel(Some(population_reload::FirstLoad {
            service_date: current_rail_service_date(chrono::Utc::now()),
            missing_lines: vec!["other-line".to_string()],
        }));
        let day = start_consuming(
            &mut feed,
            &mut rx,
            &shared(population),
            &waterloo_lookups(),
            &progress(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            day.partial_reason,
            Some(day::PartialReason::ReplayUnsupported)
        );
        assert!(day.partial_lines.contains("other-line"));
        assert!(day.trains.is_some(), "windowed state on when asked for");
        assert!(
            chrono::Utc::now() - day.observed_from < chrono::Duration::minutes(1),
            "under Kafka nothing before the process start was seen"
        );
    }

    /// A feed that records, every time anything reads from it, whether the
    /// population had been loaded by then.
    struct RecordingFeed {
        inner: movement_feed::FakeMovementFeed,
        population: SharedPopulation,
        date: chrono::NaiveDate,
        touches: Vec<bool>,
    }

    impl RecordingFeed {
        fn touch(&mut self) {
            let loaded = self.population.load().has("waterloo-reading", self.date);
            self.touches.push(loaded);
        }
    }

    #[async_trait::async_trait]
    impl MovementFeed for RecordingFeed {
        async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
            self.touch();
            self.inner.next_batch().await
        }
        async fn commit(&mut self) -> anyhow::Result<()> {
            self.inner.commit().await
        }
    }

    #[async_trait::async_trait]
    impl DeadLetterSink for RecordingFeed {
        async fn dead_letter(
            &mut self,
            records: &[movement_feed::DeadLetter],
        ) -> anyhow::Result<()> {
            self.inner.dead_letter(records).await
        }
    }

    #[async_trait::async_trait]
    impl replay::ReplaySource for RecordingFeed {
        async fn positions(
            &mut self,
        ) -> anyhow::Result<Option<movement_feed::redis_stream::StreamPositions>> {
            self.touch();
            Ok(Some(movement_feed::redis_stream::StreamPositions::default()))
        }
        async fn pending_ids(&mut self) -> anyhow::Result<std::collections::HashSet<String>> {
            self.touch();
            Ok(Default::default())
        }
        async fn read_range(
            &mut self,
            _start: &str,
            _end: &str,
            _count: usize,
        ) -> anyhow::Result<movement_feed::redis_stream::RangePage> {
            self.touch();
            Ok(Default::default())
        }
    }

    /// Nothing is read from the stream -- not the replay, not the group --
    /// until the first population load has succeeded, even while `api`
    /// keeps failing; and the failed load is retried on the short backoff,
    /// not after the 300s interval.
    #[tokio::test]
    async fn nothing_is_consumed_before_the_first_population_load() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let tokens = population_reload::tests::mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(6)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"[{"uid": "C11052", "calling_points": []}]"#),
            )
            .with_priority(2)
            .mount(&server)
            .await;

        let population = shared(population::Population::default());
        let mut rx = population_reload::Reloader {
            client: reqwest::Client::new(),
            url: format!("{}/private/schedule-line-population", server.uri()),
            tokens: Arc::new(tokens),
            line_ids: Arc::new(ArcSwap::from_pointee(vec!["waterloo-reading".to_string()])),
            geometry: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            population: Arc::clone(&population),
            interval: Duration::from_secs(300),
            min_retry: Duration::from_millis(20),
            max_retry: failed_reload_retry_delay(Duration::from_secs(300)),
            initial_wait: Duration::from_secs(600),
        }
        .spawn();

        let mut feed = RecordingFeed {
            inner: movement_feed::FakeMovementFeed::new(vec![vec![
                ACTIVATION_C11052.to_string(),
                MOVEMENT_C11052.to_string(),
            ]]),
            population: Arc::clone(&population),
            date: current_rail_service_date(chrono::Utc::now()),
            touches: vec![],
        };
        let started = std::time::Instant::now();
        let lookups = waterloo_lookups();
        let mut day = start_consuming(
            &mut feed,
            &mut rx,
            &population,
            &lookups,
            &progress(),
            false,
        )
        .await
        .unwrap();
        consume_once(&mut feed, &mut day, &lookups, &population.load()).await;

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "three failing cycles cost milliseconds"
        );
        assert!(!feed.touches.is_empty());
        assert!(
            feed.touches.iter().all(|loaded| *loaded),
            "every read happened after the population was in place: {:?}",
            feed.touches
        );
        assert!(
            day.correlation
                .derived
                .contains_key(&("waterloo-reading".to_string(), "C11052".to_string())),
            "and the first batch was matched against it"
        );
    }

    /// A population reload that takes a long time (a cold start, an ETag
    /// miss, a slow `api`) no longer holds up consumption: the loop keeps
    /// consuming against the snapshot it has while the reload runs.
    #[tokio::test]
    async fn a_slow_population_reload_does_not_block_consumption() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let tokens = population_reload::tests::mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"[{"uid": "C11052", "calling_points": []}]"#)
                    .set_delay(Duration::from_secs(5)),
            )
            .mount(&server)
            .await;

        let (_, loaded) = todays_population();
        let population = shared(loaded);
        let before = population.load_full();
        let _rx = population_reload::Reloader {
            client: reqwest::Client::new(),
            url: format!("{}/private/schedule-line-population", server.uri()),
            tokens: Arc::new(tokens),
            line_ids: Arc::new(ArcSwap::from_pointee(vec!["waterloo-reading".to_string()])),
            geometry: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            population: Arc::clone(&population),
            interval: Duration::from_secs(300),
            min_retry: Duration::from_secs(1),
            max_retry: Duration::from_secs(15),
            initial_wait: Duration::from_secs(600),
        }
        .spawn();
        // Let the reload's first (slow) request get in flight.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|r| r.method.as_str() == "GET"),
            "the reload is under way"
        );

        let mut feed = movement_feed::FakeMovementFeed::new(vec![
            vec![ACTIVATION_C11052.to_string()],
            vec![MOVEMENT_C11052.to_string()],
        ]);
        let mut day = DayState::new(current_rail_service_date(chrono::Utc::now()));
        let lookups = waterloo_lookups();
        let started = std::time::Instant::now();
        for _ in 0..2 {
            consume_once(&mut feed, &mut day, &lookups, &population.load()).await;
        }
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "consumed two batches in {:?}, while the reload is still waiting on api",
            started.elapsed()
        );
        assert_eq!(feed.committed_count, 2);
        assert!(
            Arc::ptr_eq(&before, &population.load_full()),
            "the reload had not finished -- consumption did not wait for it"
        );
        assert!(
            day.correlation
                .derived
                .contains_key(&("waterloo-reading".to_string(), "C11052".to_string()))
        );
    }

    // --- 2026-09-27: windowed stats end to end through write_stats ---

    async fn capture_write(windowed: bool) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let tokens = population_reload::tests::mock_token_cache(&server).await;
        for p in [
            "/private/full-coverage-stats",
            "/private/full-coverage-window-stats",
            "/private/station-full-coverage-samples",
        ] {
            Mock::given(method("POST"))
                .and(path(p))
                .respond_with(ResponseTemplate::new(200).set_body_string("{\"upserted\":0}"))
                .mount(&server)
                .await;
        }
        let mut config = config::tests::base_config(vec![], "*");
        config.full_coverage_stats_url = format!("{}/private/full-coverage-stats", server.uri());
        config.station_full_coverage_stats_url =
            format!("{}/private/station-full-coverage-samples", server.uri());
        config.windowed.enabled = windowed;
        config.windowed.full_coverage_window_stats_url =
            format!("{}/private/full-coverage-window-stats", server.uri());

        let today = current_rail_service_date(chrono::Utc::now());
        let mut population = population::Population::default();
        let body = r#"[{"uid": "C11052", "calling_points": [], "train_status": "P", "operator_atoc": "SW"}]"#;
        population.insert_line_pop(
            "waterloo-reading",
            today,
            population::parse_line_population(body, None, today)
                .unwrap()
                .unwrap(),
            None,
        );
        let mut day = DayState::new(today);
        if windowed {
            day = day.enable_windowed();
        }
        let geometry: SharedGeometry = Arc::new(ArcSwap::from_pointee(HashMap::new()));
        write_stats(
            &reqwest::Client::new(),
            &config,
            &tokens,
            &["waterloo-reading".to_string()],
            &population,
            &geometry,
            &day,
            &common::Defaults::default(),
        )
        .await;

        let requests = server.received_requests().await.unwrap();
        let bodies = |p: &str| -> Vec<serde_json::Value> {
            requests
                .iter()
                .filter(|r| r.url.path() == p)
                .map(|r| serde_json::from_slice(&r.body).unwrap())
                .collect()
        };
        (
            bodies("/private/full-coverage-stats"),
            bodies("/private/full-coverage-window-stats"),
        )
    }

    /// Flag off: exactly the legacy body (a golden comparison against the
    /// pre-windowed shape -- no breakdown, no version), and no window POST.
    #[tokio::test]
    async fn with_windowed_stats_off_the_legacy_row_is_unchanged_and_no_windows_are_posted() {
        let (line_posts, window_posts) = capture_write(false).await;
        assert!(window_posts.is_empty());
        assert_eq!(line_posts.len(), 1);
        let today = current_rail_service_date(chrono::Utc::now());
        assert_eq!(
            line_posts[0],
            serde_json::json!([{
                "line_id": "waterloo-reading",
                "service_date": today.to_string(),
                "availability": "pending",
                "stats": {"total": 1, "delayed": 0, "cancelled": 1, "skipped": 0,
                          "avg_delay_minutes": 0.0},
                "partial": false
            }])
        );
    }

    /// Flag on: a v2 row and both windows for the line.
    #[tokio::test]
    async fn with_windowed_stats_on_v2_rows_and_both_windows_are_posted() {
        let (line_posts, window_posts) = capture_write(true).await;
        assert_eq!(line_posts[0][0]["stats_version"], 2);
        assert!(line_posts[0][0]["breakdown"].is_object());
        assert_eq!(window_posts.len(), 1);
        let kinds: Vec<&str> = window_posts[0]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["window_kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, vec!["recent", "day_to_date"]);
        assert_eq!(
            window_posts[0][0]["feed_stale"], true,
            "nothing consumed yet"
        );
        assert_eq!(window_posts[0][0]["relevance"], "full");
    }
}
