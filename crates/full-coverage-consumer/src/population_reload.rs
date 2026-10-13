//! The schedule population reload, off the consume loop.
//!
//! **Why a background task** (2026-09-27 full-coverage lag review): the
//! reload used to run inline in the consume loop, fetching 243 lines x 2
//! dates one after another. Before the `ETag` change that blocked
//! consumption for about 2 minutes every ~7 minutes (the 1-1.5k lag
//! sawtooth), and a cold start or an `ETag` miss still does. It now runs
//! here, building each new snapshot aside and swapping it in atomically
//! ([`SharedPopulation`], an `ArcSwap`), so the loop only ever reads a
//! complete snapshot and never waits on a reload.
//!
//! **Why consumption waits for the first load**: the reload used to retry
//! only after the full `population_reload_secs` (300s). When its source was
//! not up yet at startup (connection refused, seen at 01:24Z on 2026-09-27),
//! up to 5 minutes of events were matched against an empty population and
//! `ACKed` -- lost for this consumer. The first load is now awaited before
//! anything is consumed (see [`wait_for_first_load`]), and a failed cycle
//! is retried on a short, doubling backoff instead of the full interval --
//! the same idea as the stanox/crs reload's `failed_reload_retry_delay`.
//!
//! **Why a cycle stops early, and why the backoff is jittered up to 60s**
//! (2026-10-01 outage): with its source (then the api) failing for six
//! hours, every cycle still made all ~486 `(line, date)` requests back to
//! back, and failed cycles were retried within 15s: about 24 requests a
//! second (~516k failures). Now a cycle in which the first
//! [`ABORT_AFTER_FAILURES`] fetches all fail (nothing succeeded yet: the
//! source is down, not one line) stops there and keeps every previous snapshot, and
//! consecutive failed cycles wait [`RETRY_BACKOFF`] (1s doubling to 60s,
//! with jitter). Each cycle starts one line further along the list, so a
//! few lines that always fail cannot stop the rest from ever loading.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::population::{LineGeometry, Population};

/// Outcome of one [`PopulationSource::fetch`] that reached its source.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LinePopulationFetch {
    /// The population `if_none_match` described is still current. Only
    /// possible when a validator was given.
    NotModified,
    /// The population as JSON text -- `null` when nothing is published yet
    /// for that `(line, date)` -- plus its validator
    /// (`ds_store::reads::population_version_tag`), if any.
    ///
    /// Left as text for the caller to deserialize, so a body that fails
    /// to parse is still counted as a deserialize error, not a fetch error.
    Fetched { body: String, etag: Option<String> },
}

/// A cycle stops fetching once this many fetches in a row have failed with
/// none succeeding before them (see the module docs).
pub(crate) const ABORT_AFTER_FAILURES: usize = 3;

/// The wait after each consecutive failed cycle: 1s, doubling, capped at
/// 60s, jittered (see [`common::backoff`]).
pub(crate) const RETRY_BACKOFF: common::backoff::Backoff =
    common::backoff::Backoff::new(Duration::from_secs(1), Duration::from_secs(60));

/// The current population snapshot, swapped whole by the reloader.
pub(crate) type SharedPopulation = Arc<ArcSwap<Population>>;
/// The shadow line ids the reloader fetches, refreshed by the consume
/// loop's stanox/crs reload.
pub(crate) type SharedLineIds = Arc<ArcSwap<Vec<String>>>;
/// `line_id` -> [`LineGeometry`], refreshed by the stanox/crs reload. Empty
/// while `FULL_COVERAGE_WINDOWED_STATS` is off, so no trains are reduced
/// (the population is then exactly what it was before windowed stats).
pub(crate) type SharedGeometry = Arc<ArcSwap<HashMap<String, Arc<LineGeometry>>>>;

/// What one reload cycle achieved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CycleOutcome {
    /// Every `(line_id, date)` whose fetch failed (a database error or an
    /// undeserializable body). Its previous snapshot, if any, was kept.
    pub failed: Vec<(String, chrono::NaiveDate)>,
    /// How many `(line_id, date)` fetches reached the source and were
    /// understood (a body or `null`, or not modified).
    pub succeeded: usize,
}

impl CycleOutcome {
    /// Lines whose fetch for `date` failed.
    pub(crate) fn failed_on(&self, date: chrono::NaiveDate) -> Vec<String> {
        self.failed
            .iter()
            .filter(|(_, d)| *d == date)
            .map(|(line, _)| line.clone())
            .collect()
    }
}

/// Where a reload cycle gets each `(line, date)` population
/// (`POPULATION_SOURCE`, ingest architecture spec §11.1, plan 4.3). The
/// cycle around it -- the rotation, the abort after
/// [`ABORT_AFTER_FAILURES`], the carried-over snapshots, the metrics -- is
/// the same for every source.
pub(crate) trait PopulationSource: Sync {
    /// What a source learns once per cycle, before any fetch.
    type Cycle: Send + Sync;

    /// Called once at the start of every cycle with every key it will
    /// fetch.
    fn begin_cycle(
        &self,
        line_ids: &[String],
        dates: &[chrono::NaiveDate],
    ) -> impl Future<Output = Self::Cycle> + Send;

    /// One `(line, date)`, conditional on `if_none_match`, the validator of
    /// the population held for it (`None`: fetch it whole).
    fn fetch(
        &self,
        cycle: &Self::Cycle,
        line_id: &str,
        date: chrono::NaiveDate,
        if_none_match: Option<&str>,
    ) -> impl Future<Output = anyhow::Result<LinePopulationFetch>> + Send;

    /// The source's name, for logs (`db`).
    fn kind(&self) -> &'static str;
}

/// The populations from Postgres (spec §11.1): once per cycle,
/// `ds_store::reads::list_population_versions` (one query for every key);
/// then per key, nothing at all when the version held is still current,
/// else `get_schedule_line_population_conditional` with the version held,
/// so a publish between the two queries degrades to "not modified" or a
/// fresh body. The validator is `ds_store::reads::population_version_tag`.
pub(crate) struct DbSource {
    pub pool: sqlx::PgPool,
}

/// [`DbSource`]'s per-cycle state: every published key's version, or why
/// the version query failed (every fetch of the cycle then fails the same
/// way, so the abort after [`ABORT_AFTER_FAILURES`] still applies).
pub(crate) type PopulationVersions =
    Result<HashMap<(String, chrono::NaiveDate), chrono::DateTime<chrono::Utc>>, String>;

impl PopulationSource for DbSource {
    type Cycle = PopulationVersions;

    async fn begin_cycle(
        &self,
        line_ids: &[String],
        dates: &[chrono::NaiveDate],
    ) -> PopulationVersions {
        match ds_store::reads::list_population_versions(&self.pool, line_ids, dates).await {
            Ok(versions) => Ok(versions
                .into_iter()
                .map(|v| ((v.line_id, v.service_date), v.updated_at))
                .collect()),
            Err(err) => {
                tracing::error!(error = ?err, "failed to list the schedule population versions");
                Err(format!("{err:#}"))
            }
        }
    }

    async fn fetch(
        &self,
        versions: &PopulationVersions,
        line_id: &str,
        date: chrono::NaiveDate,
        if_none_match: Option<&str>,
    ) -> anyhow::Result<LinePopulationFetch> {
        let known = match db_fetch_plan(versions, line_id, date, if_none_match)? {
            DbFetchPlan::NothingPublished => {
                // Nothing published: the `null` body.
                return Ok(LinePopulationFetch::Fetched {
                    body: "null".to_string(),
                    etag: None,
                });
            }
            DbFetchPlan::Unchanged => return Ok(LinePopulationFetch::NotModified),
            DbFetchPlan::Fetch { known } => known,
        };
        Ok(
            match ds_store::reads::get_schedule_line_population_conditional(
                &self.pool, line_id, date, false, &known,
            )
            .await?
            {
                // Deleted since the version query.
                None => LinePopulationFetch::Fetched {
                    body: "null".to_string(),
                    etag: None,
                },
                Some(ds_store::reads::ConditionalPopulation::NotModified { .. }) => {
                    LinePopulationFetch::NotModified
                }
                Some(ds_store::reads::ConditionalPopulation::Modified {
                    updated_at,
                    population,
                }) => LinePopulationFetch::Fetched {
                    body: population,
                    etag: Some(ds_store::reads::population_version_tag(updated_at)),
                },
            },
        )
    }

    fn kind(&self) -> &'static str {
        "db"
    }
}

/// What [`DbSource::fetch`] does for one key, decided from the cycle's
/// version list alone.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DbFetchPlan {
    /// No row: nothing published yet. No query.
    NothingPublished,
    /// The version held is the current one. No query.
    Unchanged,
    /// Read the row, conditional on `known` (the version held, if any).
    Fetch {
        known: Vec<chrono::DateTime<chrono::Utc>>,
    },
}

/// See [`DbFetchPlan`]. `Err` when the cycle's version query failed.
pub(crate) fn db_fetch_plan(
    versions: &PopulationVersions,
    line_id: &str,
    date: chrono::NaiveDate,
    if_none_match: Option<&str>,
) -> anyhow::Result<DbFetchPlan> {
    let versions = versions
        .as_ref()
        .map_err(|err| anyhow::anyhow!("listing the population versions failed: {err}"))?;
    let held = if_none_match.and_then(ds_store::reads::parse_population_version_tag);
    Ok(match versions.get(&(line_id.to_string(), date)) {
        None => DbFetchPlan::NothingPublished,
        Some(current) if held == Some(*current) => DbFetchPlan::Unchanged,
        Some(_) => DbFetchPlan::Fetch {
            known: held.into_iter().collect(),
        },
    })
}

/// A stable checksum of the populations held for `line_ids` x `dates`:
/// every key and its sorted uids, FNV-1a. Logged after every cycle, so the
/// populations can be compared over a day (spec §16 phase 4 exit).
pub(crate) fn population_checksum(
    population: &Population,
    line_ids: &[String],
    dates: &[chrono::NaiveDate],
) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    let mut hash = OFFSET;
    let mut feed = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    };
    let mut lines: Vec<&String> = line_ids.iter().collect();
    lines.sort();
    for line_id in lines {
        for date in dates {
            if !population.has(line_id, *date) {
                continue;
            }
            feed(line_id.as_bytes());
            feed(date.to_string().as_bytes());
            let mut uids = population.uids_for(line_id, *date);
            uids.sort_unstable();
            for uid in uids {
                feed(uid.as_bytes());
            }
        }
    }
    hash
}

/// One reload cycle from `source`: `service_date`'s and the next day's
/// population for every line in `line_ids` (Decision 2b -- tomorrow's too,
/// so the rail-day rollover finds it already loaded), each conditional on
/// the validator held in `previous`. Returns a NEW snapshot; `previous` is
/// never mutated, so the consume loop can keep reading it throughout.
///
/// Best-effort per `(line, date)`: a failure keeps that key's previous
/// snapshot and is reported in [`CycleOutcome::failed`]; it never blocks
/// any other line. Every date older than `service_date` is dropped (only
/// today's and tomorrow's are ever read). The reloader itself calls
/// [`reload_cycle_with`], which also rotates the starting line.
#[cfg(test)]
pub(crate) async fn reload_cycle<S: PopulationSource>(
    source: &S,
    line_ids: &[String],
    geometry: &HashMap<String, Arc<LineGeometry>>,
    previous: &Population,
    service_date: chrono::NaiveDate,
) -> (Population, CycleOutcome) {
    reload_cycle_with(source, line_ids, geometry, previous, service_date, 0).await
}

/// One cycle from `source`, starting at `line_ids[start % len]` and
/// wrapping round. Stops fetching once [`ABORT_AFTER_FAILURES`] fetches in
/// a row have failed with none succeeding before them; every key not
/// fetched keeps its previous snapshot and is reported in
/// [`CycleOutcome::failed`].
#[expect(
    clippy::too_many_lines,
    reason = "one loop body per fetch outcome, read top to bottom"
)]
pub(crate) async fn reload_cycle_with<S: PopulationSource>(
    source: &S,
    line_ids: &[String],
    geometry: &HashMap<String, Arc<LineGeometry>>,
    previous: &Population,
    service_date: chrono::NaiveDate,
    start: usize,
) -> (Population, CycleOutcome) {
    let mut next = Population::default();
    let mut outcome = CycleOutcome::default();
    let dates = [service_date, service_date + chrono::Duration::days(1)];
    let cycle = source.begin_cycle(line_ids, &dates).await;
    let start = if line_ids.is_empty() {
        0
    } else {
        start % line_ids.len()
    };
    let mut failures_in_a_row = 0;
    let mut aborted = false;
    for line_id in line_ids[start..].iter().chain(&line_ids[..start]) {
        for &date in &dates {
            if aborted {
                next.carry_over(previous, line_id, date);
                outcome.failed.push((line_id.clone(), date));
                continue;
            }
            if outcome.succeeded == 0 && failures_in_a_row >= ABORT_AFTER_FAILURES {
                tracing::warn!(
                    failures = failures_in_a_row,
                    "schedule population fetches keep failing with none succeeding; the source looks down, so this cycle stops here and keeps every previous snapshot"
                );
                aborted = true;
                next.carry_over(previous, line_id, date);
                outcome.failed.push((line_id.clone(), date));
                continue;
            }
            let failed_before = outcome.failed.len();
            // The validator of what we already hold for this key, if any:
            // an unchanged population then costs no re-read. See
            // `Population::etags`.
            // ...unless the line's geometry changed since that population
            // was reduced: it must then be downloaded and reduced again.
            let geometry_hash = geometry.get(line_id).map_or(0, |g| g.hash);
            let if_none_match = previous.etag_if_current(line_id, date, geometry_hash);
            match source.fetch(&cycle, line_id, date, if_none_match).await {
                Ok(LinePopulationFetch::NotModified) => {
                    metrics::counter!(
                        common::metrics::metric_name(
                            "full_coverage_consumer_population_reloads_total"
                        ),
                        "result" => "not_modified"
                    )
                    .increment(1);
                    next.carry_over(previous, line_id, date);
                    outcome.succeeded += 1;
                }
                Ok(LinePopulationFetch::Fetched { body, etag }) => {
                    metrics::counter!(
                        common::metrics::metric_name(
                            "full_coverage_consumer_population_reloads_total"
                        ),
                        "result" => "fetched"
                    )
                    .increment(1);
                    // Straight from the body text, one entry at a time,
                    // into uids and (with a geometry) reduced trains -- no
                    // `serde_json::Value` tree and no calling points kept
                    // (see `population::parse_line_population`, which also
                    // leaves rail-replacement buses and ships out). The body
                    // is dropped as soon as it is parsed, so a cold start
                    // holds at most one line's wire payload at a time.
                    let geometry = geometry.get(line_id).map(Arc::as_ref);
                    let parsed = crate::population::parse_line_population(&body, geometry, date);
                    drop(body);
                    match parsed {
                        Ok(Some(pop)) => {
                            let day = if date == dates[0] {
                                "today"
                            } else {
                                "tomorrow"
                            };
                            if let Some(shadow) = &pop.membership_shadow {
                                report_membership_shadow(line_id, date, day, shadow);
                            }
                            report_entries_after_next_rail_day(line_id, date, day, &pop);
                            next.insert_line_pop(line_id, date, pop, etag);
                            outcome.succeeded += 1;
                        }
                        Ok(None) => {
                            // Nothing published yet for this (line, date) --
                            // Decision 2e's Pending case, upstream of the
                            // rail-day gate. Not an error.
                            outcome.succeeded += 1;
                        }
                        Err(err) => {
                            tracing::error!(error = ?err, line_id = %line_id, %date, "failed to deserialize a schedule line population; keeping previous snapshot");
                            metrics::counter!(
                                common::metrics::metric_name("full_coverage_consumer_errors_total"),
                                "operation" => "reload_line_population_deserialize"
                            )
                            .increment(1);
                            next.carry_over(previous, line_id, date);
                            outcome.failed.push((line_id.clone(), date));
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(error = ?err, line_id = %line_id, %date, "failed to fetch schedule line population; keeping previous snapshot");
                    metrics::counter!(
                        common::metrics::metric_name("full_coverage_consumer_errors_total"),
                        "operation" => "reload_line_population_fetch"
                    )
                    .increment(1);
                    next.carry_over(previous, line_id, date);
                    outcome.failed.push((line_id.clone(), date));
                }
            }
            if outcome.failed.len() > failed_before {
                failures_in_a_row += 1;
            } else {
                failures_in_a_row = 0;
            }
        }
    }
    (next, outcome)
}

/// `full_coverage_consumer_line_entries_after_next_rail_day{line,day}`: how
/// many of the line's relevant trains for `date` are due after the next
/// rail day has started, and so are counted in no day
/// (`windows::due_after_next_rail_day_start`; design doc "Decisions
/// (2026-10-07)"). About 9 entries a day network-wide on 2026-10-04/05;
/// growth means more overnight services fall in the gap. Set on every fresh
/// download, like the membership gauges, with the same bounded labels (one
/// series per configured line and `today`/`tomorrow`). Always 0 without the
/// line's geometry (no reduced trains).
fn report_entries_after_next_rail_day(
    line_id: &str,
    date: chrono::NaiveDate,
    day: &'static str,
    pop: &crate::population::LinePop,
) {
    metrics::gauge!(
        common::metrics::metric_name("full_coverage_consumer_line_entries_after_next_rail_day"),
        "line" => line_id.to_string(),
        "day" => day
    )
    .set(f64::from(crate::windows::due_after_next_rail_day_start(
        pop, date,
    )));
}

/// `FULL_COVERAGE_LINE_MEMBERSHIP=shadow`: exports one line and day's
/// comparison of the two relevance rules, set on every fresh download of
/// its population, and logs it -- the evidence for switching to `scope`.
///
/// * `full_coverage_consumer_line_membership_trains{line,day,rule}` --
///   relevant trains under `legacy` (§4.1) and under `scope`;
/// * `full_coverage_consumer_line_membership_differences{line,day,kind}` --
///   trains only one rule keeps (`legacy_only`, `scope_only`);
/// * `full_coverage_consumer_line_membership_scoped_entries{line,day}` --
///   entries carrying `scope` (0: the population predates it).
fn report_membership_shadow(
    line_id: &str,
    date: chrono::NaiveDate,
    day: &'static str,
    shadow: &crate::population::MembershipShadow,
) {
    for (rule, count) in [("legacy", shadow.legacy), ("scope", shadow.scope)] {
        metrics::gauge!(
            common::metrics::metric_name("full_coverage_consumer_line_membership_trains"),
            "line" => line_id.to_string(),
            "day" => day,
            "rule" => rule
        )
        .set(f64::from(count));
    }
    for (kind, count) in [
        ("legacy_only", shadow.legacy_only),
        ("scope_only", shadow.scope_only),
    ] {
        metrics::gauge!(
            common::metrics::metric_name("full_coverage_consumer_line_membership_differences"),
            "line" => line_id.to_string(),
            "day" => day,
            "kind" => kind
        )
        .set(f64::from(count));
    }
    metrics::gauge!(
        common::metrics::metric_name("full_coverage_consumer_line_membership_scoped_entries"),
        "line" => line_id.to_string(),
        "day" => day
    )
    .set(f64::from(shadow.scoped_entries));
    tracing::info!(
        line_id,
        %date,
        legacy = shadow.legacy,
        scope = shadow.scope,
        legacy_only = shadow.legacy_only,
        scope_only = shadow.scope_only,
        scoped_entries = shadow.scoped_entries,
        legacy_only_sample = ?shadow.legacy_only_sample,
        scope_only_sample = ?shadow.scope_only_sample,
        "line membership shadow comparison"
    );
}

/// How many of `line_ids` have no population held for `date` (never
/// published, or not yet fetched successfully).
pub(crate) fn lines_without_population(
    population: &Population,
    line_ids: &[String],
    date: chrono::NaiveDate,
) -> usize {
    line_ids
        .iter()
        .filter(|line_id| !population.has(line_id, date))
        .count()
}

/// London local time by which the rail day's population must be present
/// (DQ11/PL-9): `schedule-reference` publishes each day's population the
/// day before (today and tomorrow), and the nightly delivery normally lands
/// in the small hours, so still missing at 06:00 means both the previous
/// and the current delivery failed to provide it.
const POPULATION_DEADLINE_LONDON: chrono::NaiveTime = match chrono::NaiveTime::from_hms_opt(6, 0, 0)
{
    Some(time) => time,
    None => panic!("06:00 is a valid time"),
};

/// `missing_today` once `now` is past 06:00 London on `service_date`, else
/// 0 -- what the `..._population_missing_past_deadline_lines` gauge
/// exports, so the alert needs no time-of-day logic of its own.
pub(crate) fn missing_past_deadline(
    missing_today: usize,
    service_date: chrono::NaiveDate,
    now: chrono::DateTime<chrono::Utc>,
) -> usize {
    let deadline = common::rail_day::london_to_utc(service_date, POPULATION_DEADLINE_LONDON);
    if now >= deadline { missing_today } else { 0 }
}

/// Exports, after every reload cycle, how many lines have no population
/// for the current rail day and the next
/// (`full_coverage_consumer_population_missing_lines{day}`), and the
/// current day's count once past the 06:00 London deadline
/// (`full_coverage_consumer_population_missing_past_deadline_lines`, read by
/// the chart's `DistantSignalLinePopulationMissing` alert).
#[expect(
    clippy::cast_precision_loss,
    reason = "metric gauges take f64, and these counts and timestamps stay far below 2^52"
)]
fn report_missing_populations(
    population: &Population,
    line_ids: &[String],
    service_date: chrono::NaiveDate,
    now: chrono::DateTime<chrono::Utc>,
) {
    let today = lines_without_population(population, line_ids, service_date);
    let tomorrow = lines_without_population(
        population,
        line_ids,
        service_date + chrono::Duration::days(1),
    );
    for (day, missing) in [("today", today), ("tomorrow", tomorrow)] {
        metrics::gauge!(
            common::metrics::metric_name("full_coverage_consumer_population_missing_lines"),
            "day" => day
        )
        .set(missing as f64);
    }
    metrics::gauge!(common::metrics::metric_name(
        "full_coverage_consumer_population_missing_past_deadline_lines"
    ))
    .set(missing_past_deadline(today, service_date, now) as f64);
}

/// Signalled once, by the first reload cycle that makes the population
/// usable -- see [`Reloader::run`] for exactly when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FirstLoad {
    pub service_date: chrono::NaiveDate,
    /// Lines whose population for `service_date` could still not be
    /// fetched when consumption was allowed to begin (only possible after
    /// `initial_wait`). Their rows are marked partial for that day.
    pub missing_lines: Vec<String>,
}

pub(crate) struct Reloader {
    pub line_ids: SharedLineIds,
    pub geometry: SharedGeometry,
    pub population: SharedPopulation,
    /// Wait after a cycle in which every fetch succeeded.
    pub interval: Duration,
    /// Wait after each consecutive cycle with any failure, by how many
    /// failed in a row ([`RETRY_BACKOFF`] in production).
    pub retry: common::backoff::Backoff,
    /// How long the FIRST load may keep failing for some lines (while
    /// others succeed) before consumption is let through anyway, with those
    /// lines marked partial. While EVERY fetch fails (the source down) the first
    /// load is never signalled, however long that takes.
    pub initial_wait: Duration,
}

impl Reloader {
    /// Spawns the reload loop over `source` (a [`DbSource`] in
    /// production). The returned receiver turns `Some` once, when the first
    /// load is usable; wait on it with [`wait_for_first_load`].
    pub(crate) fn spawn<S>(self, source: S) -> tokio::sync::watch::Receiver<Option<FirstLoad>>
    where
        S: PopulationSource + Send + 'static,
    {
        let (tx, rx) = tokio::sync::watch::channel(None);
        tokio::spawn(self.run(source, tx));
        rx
    }

    /// The first load is signalled after the first cycle in which every
    /// line's fetch for the CURRENT rail day succeeded -- or, once
    /// `initial_wait` has passed, after a cycle in which at least one did
    /// (some lines persistently failing must not stall every other line
    /// forever; those are reported in [`FirstLoad::missing_lines`]).
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        reason = "metric gauges take f64, and these counts and timestamps stay far below 2^52"
    )]
    async fn run<S: PopulationSource>(
        self,
        source: S,
        tx: tokio::sync::watch::Sender<Option<FirstLoad>>,
    ) {
        let started = tokio::time::Instant::now();
        let mut failed_cycles: u32 = 0;
        let mut cycles: usize = 0;
        loop {
            let service_date = crate::stats::current_rail_service_date(chrono::Utc::now());
            let line_ids = self.line_ids.load_full();
            let geometry = self.geometry.load_full();
            let previous = self.population.load_full();
            let cycle_start = std::time::Instant::now();
            let (next, outcome) = reload_cycle_with(
                &source,
                &line_ids,
                &geometry,
                &previous,
                service_date,
                cycles,
            )
            .await;
            cycles = cycles.wrapping_add(1);
            drop(previous);
            // Plan 4.3's checksum (spec §16 phase 4 exit), cycle by cycle.
            tracing::info!(
                source = source.kind(),
                checksum = %format!(
                    "{:016x}",
                    population_checksum(
                        &next,
                        &line_ids,
                        &[service_date, service_date + chrono::Duration::days(1)],
                    )
                ),
                uids = next.total_uids(),
                failed = outcome.failed.len(),
                "population reload cycle"
            );
            metrics::gauge!(common::metrics::metric_name(
                "full_coverage_consumer_population_uids"
            ))
            .set(next.total_uids() as f64);
            report_missing_populations(&next, &line_ids, service_date, chrono::Utc::now());
            self.population.store(Arc::new(next));
            metrics::histogram!(common::metrics::metric_name(
                "full_coverage_consumer_population_reload_duration_seconds"
            ))
            .record(cycle_start.elapsed().as_secs_f64());

            if tx.borrow().is_none() {
                let missing = outcome.failed_on(service_date);
                let today_ok = line_ids.len().saturating_sub(missing.len());
                let give_up_waiting = started.elapsed() >= self.initial_wait && today_ok > 0;
                if missing.is_empty() || give_up_waiting {
                    if !missing.is_empty() {
                        tracing::error!(
                            %service_date,
                            missing = missing.len(),
                            lines = ?missing,
                            "starting consumption without these lines' populations; their stats are partial for this rail day"
                        );
                    }
                    metrics::gauge!(common::metrics::metric_name(
                        "full_coverage_consumer_population_loaded"
                    ))
                    .set(1.0);
                    tx.send_replace(Some(FirstLoad {
                        service_date,
                        missing_lines: missing,
                    }));
                }
            }

            let wait = if outcome.failed.is_empty() {
                metrics::gauge!(common::metrics::metric_name(
                    "full_coverage_consumer_population_last_success_timestamp_seconds"
                ))
                .set(chrono::Utc::now().timestamp() as f64);
                failed_cycles = 0;
                self.interval
            } else {
                let wait = self.retry.delay(failed_cycles);
                failed_cycles = failed_cycles.saturating_add(1);
                tracing::warn!(
                    failed = outcome.failed.len(),
                    succeeded = outcome.succeeded,
                    retry_in_ms = wait.as_millis() as u64,
                    "population reload cycle had failures; retrying sooner than the normal interval"
                );
                wait
            };
            tokio::time::sleep(wait).await;
        }
    }
}

/// Blocks until the reloader signals its first usable load, beating
/// `progress` meanwhile (the process is alive and retrying; a restart would
/// not make the source come up any sooner). `Err` only if the reloader task has
/// died, which should be impossible.
#[expect(
    clippy::expect_used,
    reason = "the watch value was just checked to be Some"
)]
pub(crate) async fn wait_for_first_load(
    rx: &mut tokio::sync::watch::Receiver<Option<FirstLoad>>,
    progress: &health_http::Progress,
) -> anyhow::Result<FirstLoad> {
    const LOG_EVERY: Duration = Duration::from_secs(10);
    let started = tokio::time::Instant::now();
    loop {
        match tokio::time::timeout(LOG_EVERY, rx.wait_for(Option::is_some)).await {
            Ok(Ok(first)) => {
                return Ok(first.clone().expect("wait_for guarantees Some"));
            }
            Ok(Err(_)) => anyhow::bail!("population reloader task stopped before its first load"),
            Err(_) => {
                progress.beat();
                tracing::info!(
                    waited_secs = started.elapsed().as_secs(),
                    "waiting for the first schedule population load before consuming"
                );
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A test [`PopulationSource`]: every fetch counts, waits `delay`, then
    /// answers what `respond` says for `(line, date, validator held,
    /// fetches so far)`.
    pub(crate) struct Scripted<F> {
        pub respond: F,
        pub delay: Duration,
        pub fetches: Arc<AtomicUsize>,
    }

    impl<F> Scripted<F>
    where
        F: Fn(&str, chrono::NaiveDate, Option<&str>, usize) -> anyhow::Result<LinePopulationFetch>
            + Send
            + Sync,
    {
        pub(crate) fn new(respond: F) -> Self {
            Self {
                respond,
                delay: Duration::ZERO,
                fetches: Arc::new(AtomicUsize::new(0)),
            }
        }

        pub(crate) fn fetches(&self) -> usize {
            self.fetches.load(Ordering::Relaxed)
        }
    }

    impl<F> PopulationSource for Scripted<F>
    where
        F: Fn(&str, chrono::NaiveDate, Option<&str>, usize) -> anyhow::Result<LinePopulationFetch>
            + Send
            + Sync,
    {
        type Cycle = ();

        async fn begin_cycle(&self, _line_ids: &[String], _dates: &[chrono::NaiveDate]) {}

        async fn fetch(
            &self,
            (): &(),
            line_id: &str,
            date: chrono::NaiveDate,
            if_none_match: Option<&str>,
        ) -> anyhow::Result<LinePopulationFetch> {
            let before = self.fetches.fetch_add(1, Ordering::Relaxed);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            (self.respond)(line_id, date, if_none_match, before)
        }

        fn kind(&self) -> &'static str {
            "test"
        }
    }

    pub(crate) const BODY: &str = r#"[{"uid": "C11052", "calling_points": []}]"#;

    #[expect(
        clippy::unnecessary_wraps,
        reason = "the shape of a `Scripted` answer, beside `down`"
    )]
    pub(crate) fn body(body: &str) -> anyhow::Result<LinePopulationFetch> {
        Ok(LinePopulationFetch::Fetched {
            body: body.to_string(),
            etag: None,
        })
    }

    pub(crate) fn down() -> anyhow::Result<LinePopulationFetch> {
        Err(anyhow::anyhow!("the database is unavailable"))
    }

    /// DQ11/PL-9: lines with no population for a date are counted, and the
    /// current day's count is only reported past 06:00 London on that day.
    #[test]
    fn a_missing_population_is_reported_only_past_the_06_00_london_deadline() {
        let date: chrono::NaiveDate = "2026-09-27".parse().unwrap();
        let mut population = Population::default();
        population.insert_uids("present", date, HashSet::default(), None);
        let line_ids = vec!["present".to_string(), "absent".to_string()];
        assert_eq!(lines_without_population(&population, &line_ids, date), 1);
        assert_eq!(
            lines_without_population(&population, &line_ids, date + chrono::Duration::days(1)),
            2
        );

        // 2026-09-27 is BST: 06:00 London is 05:00 UTC.
        let at = |raw: &str| raw.parse::<chrono::DateTime<chrono::Utc>>().unwrap();
        assert_eq!(
            missing_past_deadline(1, date, at("2026-09-27T04:59:00Z")),
            0
        );
        assert_eq!(
            missing_past_deadline(1, date, at("2026-09-27T05:00:00Z")),
            1
        );
        assert_eq!(
            missing_past_deadline(0, date, at("2026-09-27T12:00:00Z")),
            0
        );
        // In GMT, 06:00 London is 06:00 UTC.
        let winter: chrono::NaiveDate = "2026-12-01".parse().unwrap();
        assert_eq!(
            missing_past_deadline(3, winter, at("2026-12-01T05:30:00Z")),
            0
        );
        assert_eq!(
            missing_past_deadline(3, winter, at("2026-12-01T06:00:00Z")),
            3
        );
    }

    /// A conditional reload: the first cycle reads both dates and remembers
    /// their validators; the next cycle sends them back, is told nothing
    /// changed, and keeps the snapshot it has.
    #[tokio::test]
    async fn reload_cycle_revalidates_and_keeps_the_snapshot_when_unchanged() {
        let not_modified = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&not_modified);
        let source = Scripted::new(move |_, _, held: Option<&str>, _| {
            if held == Some("v7") {
                counted.fetch_add(1, Ordering::Relaxed);
                Ok(LinePopulationFetch::NotModified)
            } else {
                Ok(LinePopulationFetch::Fetched {
                    body: BODY.to_string(),
                    etag: Some("v7".to_string()),
                })
            }
        });
        let lines = vec!["waterloo-reading".to_string()];
        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let tomorrow = today + chrono::Duration::days(1);
        let mut population = Population::default();

        for _ in 0..2 {
            let (next, outcome) =
                reload_cycle(&source, &lines, &HashMap::new(), &population, today).await;
            assert!(outcome.failed.is_empty());
            population = next;
            for date in [today, tomorrow] {
                assert_eq!(
                    population.uids_for("waterloo-reading", date),
                    vec!["C11052"]
                );
                assert_eq!(population.etag_for("waterloo-reading", date), Some("v7"));
            }
        }
        assert_eq!(source.fetches(), 4);
        assert_eq!(not_modified.load(Ordering::Relaxed), 2);
    }

    /// When a line's geometry changes (a stanox/crs reload changed its
    /// TIPLOCs), its held population was reduced against the old one: the
    /// next cycle must read it whole again, not accept "not modified".
    #[tokio::test]
    async fn a_geometry_change_forces_a_refetch() {
        let whole = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&whole);
        let source = Scripted::new(move |_, _, held: Option<&str>, _| {
            if held == Some("v7") {
                Ok(LinePopulationFetch::NotModified)
            } else {
                counted.fetch_add(1, Ordering::Relaxed);
                Ok(LinePopulationFetch::Fetched {
                    body: BODY.to_string(),
                    etag: Some("v7".to_string()),
                })
            }
        });
        let lines = vec!["waterloo-reading".to_string()];
        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let geometry = |tiplocs: &[&str]| {
            let g = LineGeometry::new(
                tiplocs
                    .iter()
                    .map(|t| ((*t).to_string(), (*t).to_string()))
                    .collect(),
                HashSet::default(),
            );
            let mut map = HashMap::new();
            map.insert("waterloo-reading".to_string(), Arc::new(g));
            map
        };
        let before = geometry(&["WATRLMN", "RDNGSTN"]);
        let after = geometry(&["WATRLMN", "RDNGSTN", "WOKING"]);

        let mut population = Population::default();
        // Cycle 1: read. Cycle 2: same geometry, not modified. Cycle 3: the
        // geometry changed, so both dates are read whole again.
        for g in [&before, &before, &after] {
            let (next, outcome) = reload_cycle(&source, &lines, g, &population, today).await;
            assert!(outcome.failed.is_empty());
            population = next;
        }
        assert_eq!(whole.load(Ordering::Relaxed), 4);
        assert_eq!(
            population
                .line_pop("waterloo-reading", today)
                .unwrap()
                .geometry_hash,
            after["waterloo-reading"].hash
        );
    }

    /// Regression test: a rail-replacement bus (`train_status` `5`) or a
    /// ship in the published population is left out, so it can no longer
    /// count as a cancellation; a train, and an entry published without a
    /// status (an older `schedule-reference`), are kept.
    #[tokio::test]
    async fn buses_and_ships_are_left_out_of_the_population() {
        let source = Scripted::new(|_, _, _, _| {
            body(
                r#"[{"uid": "C11052", "calling_points": [], "train_status": "P", "operator_atoc": "SW"},
                    {"uid": "B00001", "calling_points": [], "train_status": "5", "operator_atoc": "SW"},
                    {"uid": "B00002", "calling_points": [], "train_status": "B"},
                    {"uid": "S00001", "calling_points": [], "train_status": "S"},
                    {"uid": "C22222", "calling_points": []}]"#,
            )
        });
        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let (next, outcome) = reload_cycle(
            &source,
            &["waterloo-reading".to_string()],
            &HashMap::new(),
            &Population::default(),
            today,
        )
        .await;
        assert!(outcome.failed.is_empty());
        let mut uids = next.uids_for("waterloo-reading", today);
        uids.sort_unstable();
        assert_eq!(uids, vec!["C11052", "C22222"]);

        let row = crate::stats::build_line_row(
            "waterloo-reading",
            today,
            &uids,
            &HashMap::new(),
            true,
            false,
            &common::Defaults::default(),
        );
        assert_eq!(
            row.stats.total, 2,
            "the buses are not in the closed-day row"
        );
    }

    /// A failed fetch keeps the previous snapshot for that key -- a
    /// transient error must not empty a line's population.
    #[tokio::test]
    async fn a_failed_fetch_keeps_the_previous_snapshot_and_is_reported() {
        let source = Scripted::new(|_, _, _, _| down());
        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let mut previous = Population::default();
        previous.insert(
            "waterloo-reading",
            today,
            vec![schedule_query::LinePopulationEntry {
                uid: "C11052".to_string(),
                calling_points: vec![],
                operator_atoc: None,
                train_status: None,
                ..Default::default()
            }],
        );
        let lines = vec!["waterloo-reading".to_string()];
        let (next, outcome) =
            reload_cycle(&source, &lines, &HashMap::new(), &previous, today).await;
        assert_eq!(outcome.failed.len(), 2);
        assert_eq!(
            outcome.failed_on(today),
            vec!["waterloo-reading".to_string()]
        );
        assert_eq!(next.uids_for("waterloo-reading", today), vec!["C11052"]);
    }

    pub(crate) fn reloader(lines: &[&str]) -> (Reloader, SharedPopulation) {
        let population: SharedPopulation = Arc::new(ArcSwap::from_pointee(Population::default()));
        let reloader = Reloader {
            line_ids: Arc::new(ArcSwap::from_pointee(
                lines.iter().map(ToString::to_string).collect(),
            )),
            geometry: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            population: Arc::clone(&population),
            interval: Duration::from_secs(3600),
            retry: common::backoff::Backoff::new(
                Duration::from_millis(20),
                Duration::from_millis(100),
            ),
            initial_wait: Duration::from_secs(3600),
        };
        (reloader, population)
    }

    fn lines(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("line-{i}")).collect()
    }

    /// 2026-10-01: with the source down, every cycle still made all ~486
    /// requests. Now three failures with nothing succeeding stop the cycle;
    /// every key keeps its previous snapshot and is reported failed.
    #[tokio::test]
    async fn a_cycle_stops_after_three_failures_while_the_source_is_down() {
        let source = Scripted::new(|_, _, _, _| down());
        let today: chrono::NaiveDate = "2026-10-01".parse().unwrap();
        let mut previous = Population::default();
        previous.insert_uids("line-4", today, ["C1".to_string()].into(), None);

        let (next, outcome) =
            reload_cycle(&source, &lines(5), &HashMap::new(), &previous, today).await;
        assert_eq!(source.fetches(), ABORT_AFTER_FAILURES);
        assert_eq!(outcome.succeeded, 0);
        assert_eq!(outcome.failed.len(), 10, "every key is reported failed");
        assert_eq!(
            next.uids_for("line-4", today),
            vec!["C1"],
            "and keeps its snapshot"
        );
    }

    /// One line failing while the source is up must not stop the others:
    /// once anything has succeeded, the cycle fetches every key.
    #[tokio::test]
    async fn failures_after_a_success_do_not_stop_the_cycle() {
        let source = Scripted::new(
            |line: &str, _, _, _| {
                if line == "line-0" { body(BODY) } else { down() }
            },
        );
        let (_, outcome) = reload_cycle(
            &source,
            &lines(5),
            &HashMap::new(),
            &Population::default(),
            "2026-10-01".parse().unwrap(),
        )
        .await;
        assert_eq!(source.fetches(), 10);
        assert_eq!(outcome.succeeded, 2);
        assert_eq!(outcome.failed.len(), 8);
    }

    /// Each cycle starts one line further along, so lines that always fail
    /// at the front of the list cannot starve the rest.
    #[tokio::test]
    async fn a_rotated_cycle_reaches_lines_behind_failing_ones() {
        let source = Scripted::new(
            |line: &str, _, _, _| {
                if line == "line-2" { body(BODY) } else { down() }
            },
        );
        let today: chrono::NaiveDate = "2026-10-01".parse().unwrap();
        let (_, from_0) = reload_cycle_with(
            &source,
            &lines(3),
            &HashMap::new(),
            &Population::default(),
            today,
            0,
        )
        .await;
        assert_eq!(
            from_0.succeeded, 0,
            "line-0 and line-1 fail first, so line-2 is never tried"
        );
        let (next, from_2) = reload_cycle_with(
            &source,
            &lines(3),
            &HashMap::new(),
            &Population::default(),
            today,
            2,
        )
        .await;
        assert_eq!(from_2.succeeded, 2);
        assert_eq!(next.uids_for("line-2", today), vec!["C11052"]);
    }

    /// Consecutive failed cycles back off: with the source down, a 100ms
    /// backoff doubling (jittered down to half at most) allows at most 5
    /// cycles in the first second -- 2 fetches each for one line -- where a
    /// fixed 100ms retry would make about 10.
    #[tokio::test]
    async fn consecutive_failed_cycles_back_off() {
        let source = Scripted::new(|_, _, _, _| down());
        let fetches = Arc::clone(&source.fetches);
        let (mut reloader, _) = reloader(&["waterloo-reading"]);
        reloader.retry =
            common::backoff::Backoff::new(Duration::from_millis(100), Duration::from_secs(60));
        let _rx = reloader.spawn(source);
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let fetches = fetches.load(Ordering::Relaxed);
        assert!(fetches >= 4, "it keeps retrying: {fetches} fetches");
        assert!(fetches <= 12, "but backs off: {fetches} fetches in 1s");
    }

    /// The source failing at startup must not cost a full reload interval:
    /// the reloader retries on its short backoff, and the first load is
    /// only signalled once the fetch really succeeds -- never against the
    /// empty population.
    #[tokio::test]
    async fn a_failed_first_load_retries_quickly_and_is_only_signalled_on_success() {
        // Two failing cycles (today + tomorrow each), then success.
        let source = Scripted::new(|_, _, _, before| if before < 4 { down() } else { body(BODY) });
        let fetches = Arc::clone(&source.fetches);
        let (reloader, population) = reloader(&["waterloo-reading"]);
        let mut rx = reloader.spawn(source);
        let started = std::time::Instant::now();
        let first = wait_for_first_load(
            &mut rx,
            &health_http::Progress::new(Duration::from_secs(60)),
        )
        .await
        .unwrap();

        assert!(first.missing_lines.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "retried on the short backoff, not the {}s interval",
            3600
        );
        assert_eq!(
            fetches.load(Ordering::Relaxed),
            6,
            "two failing cycles, then one that succeeded"
        );
        assert_eq!(
            population
                .load()
                .uids_for("waterloo-reading", first.service_date),
            vec!["C11052"],
            "the population is already in place when the first load is signalled"
        );
    }

    /// While every fetch fails (the source down), the first load is never
    /// signalled, however long -- consumption must not start against an
    /// empty population.
    #[tokio::test]
    async fn the_first_load_is_never_signalled_while_every_fetch_fails() {
        let source = Scripted::new(|_, _, _, _| down());
        let fetches = Arc::clone(&source.fetches);
        let (mut reloader, _) = reloader(&["waterloo-reading"]);
        reloader.initial_wait = Duration::ZERO;
        let mut rx = reloader.spawn(source);
        let waited =
            tokio::time::timeout(Duration::from_millis(500), rx.wait_for(Option::is_some)).await;
        assert!(
            waited.is_err(),
            "no first load while the source refuses everything"
        );
        let fetches = fetches.load(Ordering::Relaxed);
        assert!(
            fetches >= 6,
            "and it kept retrying quickly meanwhile (got {fetches} fetches)"
        );
    }

    /// One line persistently failing while the others load: once
    /// `initial_wait` has passed, consumption is let through, and that line
    /// is reported so its day can be marked partial.
    #[tokio::test]
    async fn a_persistently_failing_line_is_reported_missing_after_the_initial_wait() {
        let source = Scripted::new(|line: &str, _, _, _| {
            if line == "broken-line" {
                down()
            } else {
                body(BODY)
            }
        });
        let (mut reloader, _) = reloader(&["waterloo-reading", "broken-line"]);
        reloader.initial_wait = Duration::from_millis(100);
        let mut rx = reloader.spawn(source);
        let first = tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_first_load(
                &mut rx,
                &health_http::Progress::new(Duration::from_secs(60)),
            ),
        )
        .await
        .expect("let through after the initial wait")
        .unwrap();
        assert_eq!(first.missing_lines, vec!["broken-line".to_string()]);
    }
}

/// [`DbSource`] (plan 4.3). The plan is unit-tested; the cycles
/// run against a throwaway database (`#[sqlx::test]`, migrated from
/// ds-store's migrations), so `DATABASE_URL` must be able to create
/// databases.
#[cfg(test)]
mod db_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const BODY: &str = r#"[{"uid": "C11052", "calling_points": []}]"#;

    fn day() -> chrono::NaiveDate {
        "2050-01-10".parse().unwrap()
    }

    /// Counts the fetches a cycle makes and records which keys came back
    /// with a body.
    struct Counting<S> {
        inner: S,
        fetches: AtomicUsize,
        bodies: std::sync::Mutex<Vec<(String, chrono::NaiveDate)>>,
    }

    impl<S> Counting<S> {
        fn new(inner: S) -> Self {
            Self {
                inner,
                fetches: AtomicUsize::new(0),
                bodies: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn take(&self) -> (usize, Vec<(String, chrono::NaiveDate)>) {
            let mut bodies = std::mem::take(&mut *self.bodies.lock().unwrap());
            bodies.sort();
            (self.fetches.swap(0, Ordering::Relaxed), bodies)
        }
    }

    impl<S: PopulationSource + Send> PopulationSource for Counting<S> {
        type Cycle = S::Cycle;

        async fn begin_cycle(&self, line_ids: &[String], dates: &[chrono::NaiveDate]) -> S::Cycle {
            self.inner.begin_cycle(line_ids, dates).await
        }

        async fn fetch(
            &self,
            cycle: &S::Cycle,
            line_id: &str,
            date: chrono::NaiveDate,
            if_none_match: Option<&str>,
        ) -> anyhow::Result<LinePopulationFetch> {
            self.fetches.fetch_add(1, Ordering::Relaxed);
            let fetched = self.inner.fetch(cycle, line_id, date, if_none_match).await;
            if let Ok(LinePopulationFetch::Fetched { body, .. }) = &fetched
                && body != "null"
            {
                self.bodies
                    .lock()
                    .unwrap()
                    .push((line_id.to_string(), date));
            }
            fetched
        }

        fn kind(&self) -> &'static str {
            self.inner.kind()
        }
    }

    #[test]
    fn the_plan_reads_only_a_changed_or_unknown_version() {
        let at = |micros| chrono::DateTime::from_timestamp_micros(micros).unwrap();
        let versions: PopulationVersions =
            Ok(HashMap::from([(("wcml".to_string(), day()), at(5))]));
        let held = ds_store::reads::population_version_tag(at(5));
        let stale = ds_store::reads::population_version_tag(at(4));
        assert_eq!(
            db_fetch_plan(&versions, "wcml", day(), Some(&held)).unwrap(),
            DbFetchPlan::Unchanged
        );
        assert_eq!(
            db_fetch_plan(&versions, "wcml", day(), Some(&stale)).unwrap(),
            DbFetchPlan::Fetch { known: vec![at(4)] }
        );
        assert_eq!(
            db_fetch_plan(&versions, "wcml", day(), None).unwrap(),
            DbFetchPlan::Fetch { known: vec![] }
        );
        assert_eq!(
            db_fetch_plan(&versions, "anglia", day(), Some(&held)).unwrap(),
            DbFetchPlan::NothingPublished
        );
        assert!(db_fetch_plan(&Err("down".to_string()), "wcml", day(), None).is_err());
    }

    /// Spec §11.1: the first cycle reads every published pair; an unchanged
    /// population then reads nothing, and a republished one only its own
    /// pair.
    #[sqlx::test(migrations = "../ds-store/migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_changed_version_refetches_only_that_pair(pool: sqlx::PgPool) {
        let next_day = day() + chrono::Duration::days(1);
        for (line, date) in [("wcml", day()), ("wcml", next_day), ("anglia", day())] {
            ds_store::schedule::upsert_schedule_line_population(&pool, line, date, BODY)
                .await
                .unwrap();
        }
        let source = Counting::new(DbSource { pool: pool.clone() });
        let lines = vec!["wcml".to_string(), "anglia".to_string()];
        let geometry = HashMap::new();

        let (first, outcome) =
            reload_cycle_with(&source, &lines, &geometry, &Population::default(), day(), 0).await;
        assert!(outcome.failed.is_empty(), "{outcome:?}");
        assert_eq!(
            outcome.succeeded, 4,
            "anglia's next day is unpublished, not failed"
        );
        let (_, bodies) = source.take();
        assert_eq!(
            bodies,
            vec![
                ("anglia".to_string(), day()),
                ("wcml".to_string(), day()),
                ("wcml".to_string(), next_day)
            ]
        );
        assert_eq!(first.uids_for("wcml", day()), vec!["C11052"]);

        let (second, outcome) =
            reload_cycle_with(&source, &lines, &geometry, &first, day(), 0).await;
        assert!(outcome.failed.is_empty());
        assert!(source.take().1.is_empty(), "nothing changed: no body read");
        assert_eq!(
            population_checksum(&second, &lines, &[day(), next_day]),
            population_checksum(&first, &lines, &[day(), next_day])
        );

        ds_store::schedule::upsert_schedule_line_population(
            &pool,
            "wcml",
            next_day,
            r#"[{"uid": "C11053", "calling_points": []}]"#,
        )
        .await
        .unwrap();
        let (third, _) = reload_cycle_with(&source, &lines, &geometry, &second, day(), 0).await;
        assert_eq!(source.take().1, vec![("wcml".to_string(), next_day)]);
        assert_eq!(third.uids_for("wcml", next_day), vec!["C11053"]);
        assert_eq!(
            third.uids_for("wcml", day()),
            vec!["C11052"],
            "carried over"
        );
        assert_ne!(
            population_checksum(&third, &lines, &[day(), next_day]),
            population_checksum(&second, &lines, &[day(), next_day])
        );
    }

    /// The abort after three failures applies to the database too: with
    /// the version query failing, three fetches fail and the cycle stops,
    /// keeping every previous snapshot.
    #[sqlx::test(migrations = "../ds-store/migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_failing_database_stops_the_cycle_after_three_failures(pool: sqlx::PgPool) {
        let lines: Vec<String> = (0..4).map(|i| format!("line-{i}")).collect();
        let mut previous = Population::default();
        previous.insert_uids(
            "line-3",
            day(),
            std::collections::HashSet::from(["C1".to_string()]),
            None,
        );
        pool.close().await;
        let source = Counting::new(DbSource { pool });
        let (next, outcome) =
            reload_cycle_with(&source, &lines, &HashMap::new(), &previous, day(), 0).await;
        assert_eq!(source.take().0, ABORT_AFTER_FAILURES);
        assert_eq!(outcome.succeeded, 0);
        assert_eq!(outcome.failed.len(), 8, "every key reported failed");
        assert_eq!(next.uids_for("line-3", day()), vec!["C1"], "kept");
    }
}
