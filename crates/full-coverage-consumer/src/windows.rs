//! Windowed full-coverage stats: classifying each train due on a line
//! (design section 4.3.2) and counting them over the `recent` and
//! `day_to_date` windows (section 5), plus the v2 day-to-date / closed-day
//! row. Only used with `FULL_COVERAGE_WINDOWED_STATS=true`; the legacy
//! whole-day row (`stats::build_line_row`) is untouched.
//!
//! See docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md,
//! and its "Decisions (2026-09-27)" section for the delay threshold (3
//! minutes late at the train's first calling point on the line).
//!
//! **A train belongs to the rail day its due time falls in** ("Decisions
//! (2026-10-02)" item 3). Populations are published per CIF service date,
//! but rail day D runs from 02:00 London on D to 02:00 on D + 1, so the
//! trains of service date D + 1 that are due after midnight and before
//! 02:00 are rail day D's. Every window of rail day D therefore counts
//! D's population (with the TRUST state of `TrainState::current`) plus
//! D + 1's trains due before `rail_day_start(D + 1)` (with
//! `TrainState::next`, where their Activations already go).

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use common::full_coverage_window::{
    FULL_COVERAGE_CANCELLED_IN_ADVANCE_MINUTES, FULL_COVERAGE_STATS_VERSION,
};
use common::{
    FullCoverageLineStatsRow, FullCoverageWindowCounts, FullCoverageWindowKind,
    FullCoverageWindowStatsRow,
};

use crate::population::{LineGeometry, LinePop, LineTrain, Relevance};
use crate::trains::{NO_TIPLOC, Report, TrainDay, TrainState, to_minutes};

/// The consumer's window parameters (`config::WindowedStatsArgs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WindowParams {
    pub recent_minutes: u32,
    pub grace_minutes: u32,
    pub activations_min: u32,
    pub feed_stale_secs: u64,
}

/// A presumed cancellation needs the train's Activation to have been
/// visible to this process: its origin departure at least this long after
/// `observed_from` (TRUST activates about an hour ahead).
const ACTIVATION_LEAD_MINUTES: u32 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrainClass {
    OnTime,
    Late,
    /// Reached the line, then a cancellation or change of origin cut it
    /// short on the line.
    Skipped {
        late: bool,
    },
    CancelledExplicit,
    CancelledPresumed,
    /// Activated or reporting upstream, not yet seen on the line: not in
    /// `total`.
    Pending,
    /// Due before this process could see every event: not in `total`.
    Unobserved,
}

/// What classifying one train needs beyond the train itself.
#[derive(Debug, Clone)]
pub(crate) struct ClassifyCtx {
    /// Minutes late (at the first report on the line) from which a train
    /// is delayed: `Defaults::full_coverage_delay_threshold_minutes`.
    pub delay_threshold: i64,
    /// The line/date may presume cancellations at all: `Full` relevance and
    /// a healthy feed.
    pub presumed_allowed: bool,
    pub observed_from_min: u32,
    /// The line's TIPLOCs, as `TrainState` interned ids.
    pub line_tiplocs: HashSet<u32>,
}

/// The report that shows the train on the line: the earliest (by planned
/// time) that is at one of the line's TIPLOCs or planned at or after the
/// train's due time -- the line's own first station is not always a TRUST
/// reporting point. Its delay is the train's delay "at its first calling
/// point on the line".
fn first_report_on_line<'a>(
    train: &LineTrain,
    day: &'a TrainDay,
    line_tiplocs: &HashSet<u32>,
) -> Option<&'a Report> {
    day.reports
        .iter()
        .filter(|r| {
            r.planned_min >= train.due_min
                || (r.tiploc != NO_TIPLOC && line_tiplocs.contains(&r.tiploc))
        })
        .min_by_key(|r| r.planned_min)
}

/// Section 4.3.2's table, in order. Returns the class and the delay it was
/// judged on (0 when none).
pub(crate) fn classify_line_train(
    train: &LineTrain,
    day: Option<&TrainDay>,
    ctx: &ClassifyCtx,
) -> (TrainClass, i32) {
    if train.due_min < ctx.observed_from_min {
        return (TrainClass::Unobserved, 0);
    }
    let empty = TrainDay::default();
    let day = day.unwrap_or(&empty);
    let late = |delay: i32| i64::from(delay) >= ctx.delay_threshold;
    let reached = first_report_on_line(train, day, &ctx.line_tiplocs);
    let cancel = day.cancel.as_ref();

    if let Some(report) = reached {
        let delay = i32::from(report.delay);
        // 1: reached, but cut short on the line.
        let stops_early = cancel.is_some_and(|c| c.dep_min.is_some_and(|d| d < train.last_due_min))
            || day.origin_change_dep_min.is_some_and(|o| o > train.due_min);
        if stops_early {
            return (TrainClass::Skipped { late: late(delay) }, delay);
        }
        // 2.
        let class = if late(delay) {
            TrainClass::Late
        } else {
            TrainClass::OnTime
        };
        return (class, delay);
    }
    // 3: cancelled before or on the line (an EN ROUTE cancellation from
    // beyond the line is not this line's).
    if cancel.is_some_and(|c| c.dep_min.is_none_or(|d| d <= train.last_due_min)) {
        return (TrainClass::CancelledExplicit, 0);
    }
    // 4: now starts after the whole line.
    if day
        .origin_change_dep_min
        .is_some_and(|o| o > train.last_due_min)
    {
        return (TrainClass::CancelledExplicit, 0);
    }
    // 5: never activated, never reported.
    if !day.activated
        && day.reports.is_empty()
        && ctx.presumed_allowed
        && train.origin_dep_min
            >= ctx
                .observed_from_min
                .saturating_add(ACTIVATION_LEAD_MINUTES)
    {
        return (TrainClass::CancelledPresumed, 0);
    }
    // 6: already late upstream.
    if let Some(report) = day.last_report() {
        let delay = i32::from(report.delay);
        if late(delay) {
            return (TrainClass::Late, delay);
        }
    }
    // 7.
    (TrainClass::Pending, 0)
}

/// Whether an explicitly cancelled train's 0002 arrived at least
/// `FULL_COVERAGE_CANCELLED_IN_ADVANCE_MINUTES` before it was due on the
/// line. A train "cancelled" by a change of origin past the line has no
/// 0002 and is never in advance. Only annotates the sparse rule's reason.
fn cancelled_in_advance(train: &LineTrain, day: Option<&TrainDay>) -> bool {
    day.and_then(|d| d.cancel.as_ref()).is_some_and(|c| {
        c.received_min
            .saturating_add(FULL_COVERAGE_CANCELLED_IN_ADVANCE_MINUTES)
            <= train.due_min
    })
}

/// Adds one classified train to `counts`; returns its delay when it
/// counts toward the average (every non-cancelled train in `total`).
fn count(counts: &mut FullCoverageWindowCounts, class: TrainClass, delay: i32) -> Option<i32> {
    match class {
        TrainClass::OnTime => {
            counts.total += 1;
            counts.on_time += 1;
            Some(delay)
        }
        TrainClass::Late => {
            counts.total += 1;
            counts.delayed += 1;
            Some(delay)
        }
        TrainClass::Skipped { late } => {
            counts.total += 1;
            counts.skipped += 1;
            if late {
                counts.delayed += 1;
            }
            Some(delay)
        }
        TrainClass::CancelledExplicit => {
            counts.total += 1;
            counts.cancelled_explicit += 1;
            None
        }
        TrainClass::CancelledPresumed => {
            counts.total += 1;
            counts.cancelled_presumed += 1;
            None
        }
        TrainClass::Pending => {
            counts.pending += 1;
            None
        }
        TrainClass::Unobserved => {
            counts.unobserved += 1;
            None
        }
    }
}

/// Everything about one line that every window of one write shares.
pub(crate) struct LineInputs<'a> {
    pub line_id: &'a str,
    pub service_date: chrono::NaiveDate,
    pub pop: &'a LinePop,
    /// The line's population for `service_date + 1`, when held: its trains
    /// due before that date's rail day starts belong to this rail day (see
    /// the module doc). `None` makes every window that reaches local
    /// midnight `partial`, since those trains cannot be counted.
    pub next_pop: Option<&'a LinePop>,
    pub trains: &'a TrainState,
    pub geometry: Option<&'a LineGeometry>,
    pub thresholds: &'a common::Defaults,
    pub observed_from: DateTime<Utc>,
    pub feed_stale: bool,
    /// The line's population was missing at startup, or the whole day is
    /// partial (`DayState::is_line_partial`).
    pub line_partial: bool,
}

impl LineInputs<'_> {
    fn ctx(&self, pop: &LinePop) -> ClassifyCtx {
        let line_tiplocs = self
            .geometry
            .map(|g| {
                g.crs_by_tiploc
                    .keys()
                    .filter_map(|t| self.trains.tiploc_id(t))
                    .collect()
            })
            .unwrap_or_default();
        ClassifyCtx {
            delay_threshold: self.thresholds.full_coverage_delay_threshold_minutes,
            presumed_allowed: pop.relevance == Relevance::Full && !self.feed_stale,
            observed_from_min: to_minutes(self.observed_from),
            line_tiplocs,
        }
    }

    fn presumed_enabled(&self) -> bool {
        self.pop.relevance == Relevance::Full && !self.feed_stale
    }

    /// The first instant of the next rail day: `service_date + 1`'s trains
    /// due before it are this rail day's.
    fn next_rail_day_start(&self) -> DateTime<Utc> {
        crate::stats::rail_day_start(self.service_date + chrono::Duration::days(1))
    }

    /// Local midnight starting `service_date + 1`: the earliest a train of
    /// that date can be due.
    fn next_service_date_start(&self) -> DateTime<Utc> {
        common::rail_day::london_to_utc(
            self.service_date + chrono::Duration::days(1),
            chrono::NaiveTime::MIN,
        )
    }

    /// Counts every train with `due` in `[from, to]` (inclusive, UTC
    /// minutes).
    pub(crate) fn counts(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> FullCoverageWindowCounts {
        self.counts_in(to_minutes(from), to_minutes(to))
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "counts stay far below 2^52, so the f64 ratio is exact"
    )]
    fn counts_in(&self, from: u32, to: u32) -> FullCoverageWindowCounts {
        let mut counts = FullCoverageWindowCounts::default();
        let mut delay_sum = 0i64;
        let mut delay_n = 0i64;
        let mut add = |pop: &LinePop, state: &HashMap<String, TrainDay>, from: u32, to: u32| {
            let ctx = self.ctx(pop);
            let trains = &pop.trains;
            let lo = trains.partition_point(|t| t.due_min < from);
            let hi = trains.partition_point(|t| t.due_min <= to);
            for train in trains.get(lo..hi).unwrap_or_default() {
                let day = state.get(&*train.uid);
                let (class, delay) = classify_line_train(train, day, &ctx);
                if class == TrainClass::CancelledExplicit && cancelled_in_advance(train, day) {
                    counts.cancelled_in_advance += 1;
                }
                if let Some(delay) = count(&mut counts, class, delay) {
                    delay_sum += i64::from(delay);
                    delay_n += 1;
                }
            }
        };
        add(self.pop, &self.trains.current, from, to);
        if let Some(next_pop) = self.next_pop {
            // Only the next date's trains due before its rail day starts.
            let to = to.min(to_minutes(self.next_rail_day_start()).saturating_sub(1));
            if from <= to {
                add(next_pop, &self.trains.next, from, to);
            }
        }
        counts.avg_delay_minutes = if delay_n == 0 {
            0.0
        } else {
            delay_sum as f64 / delay_n as f64
        };
        counts
    }

    /// One window row over the due-time range `from`..`to`: `(from, to]`
    /// for `recent` (so W minutes long), `[from, to]` for `day_to_date`
    /// (from the rail-day start itself). A window that
    /// starts less than an hour after `observed_from` (or on a line whose
    /// day is partial) is `partial` and never influences severity.
    pub(crate) fn window(
        &self,
        kind: FullCoverageWindowKind,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> FullCoverageWindowStatsRow {
        // Without the next date's population, the trains due between local
        // midnight and the end of the rail day cannot be counted.
        let next_pop_missing = self.next_pop.is_none() && to >= self.next_service_date_start();
        let partial = self.line_partial
            || next_pop_missing
            || from
                < self.observed_from
                    + chrono::Duration::minutes(i64::from(ACTIVATION_LEAD_MINUTES));
        FullCoverageWindowStatsRow {
            line_id: self.line_id.to_string(),
            window_kind: kind,
            service_date: self.service_date,
            window_start: from,
            window_end: to,
            computed_at: now,
            counts: match kind {
                FullCoverageWindowKind::Recent => {
                    self.counts_in(to_minutes(from).saturating_add(1), to_minutes(to))
                }
                FullCoverageWindowKind::DayToDate => self.counts(from, to),
            },
            relevance: self.pop.relevance.as_str().to_string(),
            presumed_enabled: self.presumed_enabled(),
            partial,
            feed_stale: self.feed_stale,
            stats_version: FULL_COVERAGE_STATS_VERSION,
        }
    }
}

/// The `recent` and `day_to_date` due ranges at `now`. When the day has
/// closed, `day_to_date` covers the whole rail day (the closed-day row).
pub(crate) fn window_ranges(
    service_date: chrono::NaiveDate,
    now: DateTime<Utc>,
    params: &WindowParams,
    closed: bool,
) -> [(FullCoverageWindowKind, DateTime<Utc>, DateTime<Utc>); 2] {
    let end = now - chrono::Duration::minutes(i64::from(params.grace_minutes));
    // (end - W, end]: see `LineInputs::window`.
    let recent_start = end - chrono::Duration::minutes(i64::from(params.recent_minutes));
    let day_start = crate::stats::rail_day_start(service_date);
    let day_end = if closed {
        crate::stats::rail_day_start(service_date + chrono::Duration::days(1))
            - chrono::Duration::minutes(1)
    } else {
        end
    };
    [
        (FullCoverageWindowKind::Recent, recent_start, end),
        (
            FullCoverageWindowKind::DayToDate,
            day_start,
            day_end.max(day_start),
        ),
    ]
}

/// How many of service date `service_date`'s relevant trains are due on
/// the line at or after `rail_day_start(service_date + 1)`. These are
/// counted in NO rail day: rail day D's windows end there, and rail day
/// D + 1 counts only D + 1's population (plus D + 2's trains before 02:00).
/// Accepted and documented ("Decisions (2026-10-07)" in the design doc):
/// about 9 line-train entries a day, the up and down Night Riviera on the
/// GWR lines and the up Highlander Caledonian Sleeper on the WCML, all in
/// overnight windows that are 90%+ empty. Exported per line and day as
/// `full_coverage_consumer_line_entries_after_next_rail_day` so growth is
/// visible.
pub(crate) fn due_after_next_rail_day_start(pop: &LinePop, service_date: chrono::NaiveDate) -> u32 {
    let next = to_minutes(crate::stats::rail_day_start(
        service_date + chrono::Duration::days(1),
    ));
    let first = pop.trains.partition_point(|t| t.due_min < next);
    u32::try_from(pop.trains.len().saturating_sub(first)).unwrap_or(u32::MAX)
}

/// The v2 `full_coverage_line_stats` row from a `day_to_date` window: the
/// open day's running counts (`pending`), or -- once closed -- the day's
/// audit record (`available` unless partial).
pub(crate) fn line_row_v2(
    day_to_date: &FullCoverageWindowStatsRow,
    closed: bool,
) -> FullCoverageLineStatsRow {
    FullCoverageLineStatsRow {
        line_id: day_to_date.line_id.clone(),
        service_date: day_to_date.service_date,
        availability: if closed && !day_to_date.partial {
            "available"
        } else {
            "pending"
        }
        .to_string(),
        stats: day_to_date.counts.to_sample_stats(),
        partial: day_to_date.partial,
        breakdown: Some(day_to_date.counts.clone()),
        stats_version: Some(FULL_COVERAGE_STATS_VERSION),
    }
}

/// Section 4.3.4's feed-health gate: the newest consumed event is recent
/// enough, and enough Activations arrived in the last hour. Without it a
/// TRUST or relay outage would read as every due train presumed cancelled.
pub(crate) fn feed_stale(
    last_event_at: Option<DateTime<Utc>>,
    activations_last_hour: usize,
    now: DateTime<Utc>,
    params: &WindowParams,
) -> bool {
    let stale_after =
        chrono::Duration::seconds(i64::try_from(params.feed_stale_secs).unwrap_or(i64::MAX / 1000));
    let old = last_event_at.is_none_or(|at| now - at > stale_after);
    old || activations_last_hour < params.activations_min as usize
}

#[cfg(test)]
#[expect(
    clippy::float_cmp,
    reason = "test code: exact expected values are the point"
)]
mod tests {
    use super::*;
    use crate::trains::Canx;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn m(s: &str) -> u32 {
        to_minutes(at(s))
    }

    fn train(uid: &str, due: &str, last: &str, origin: &str) -> LineTrain {
        LineTrain {
            uid: uid.into(),
            due_min: m(due),
            last_due_min: m(last),
            origin_dep_min: m(origin),
        }
    }

    fn ctx() -> ClassifyCtx {
        ClassifyCtx {
            delay_threshold: 3,
            presumed_allowed: true,
            observed_from_min: m("2026-09-26T19:00:00Z"),
            line_tiplocs: [7].into_iter().collect(),
        }
    }

    fn report(planned: &str, delay: i16, tiploc: u32) -> Report {
        Report {
            planned_min: m(planned),
            delay,
            tiploc,
        }
    }

    fn t() -> LineTrain {
        train(
            "C1",
            "2026-09-27T09:00:00Z",
            "2026-09-27T09:40:00Z",
            "2026-09-27T08:30:00Z",
        )
    }

    fn classify(day: &TrainDay) -> TrainClass {
        classify_line_train(&t(), Some(day), &ctx()).0
    }

    #[test]
    fn rule_2_reached_is_late_at_3_minutes_measured_at_the_first_line_report() {
        let mut day = TrainDay {
            activated: true,
            ..TrainDay::default()
        };
        day.reports = vec![
            report("2026-09-27T08:40:00Z", 9, 1), // upstream, not on the line
            report("2026-09-27T09:00:00Z", 3, 7), // first on the line
            report("2026-09-27T09:20:00Z", 0, 7),
        ];
        assert_eq!(
            classify_line_train(&t(), Some(&day), &ctx()),
            (TrainClass::Late, 3)
        );
        day.reports[1].delay = 2;
        assert_eq!(
            classify(&day),
            TrainClass::OnTime,
            "2 minutes is not delayed"
        );
        // An arrival report at the first line station, planned before the
        // due (departure) time, is still on the line.
        day.reports = vec![report("2026-09-27T08:58:00Z", 4, 7)];
        assert_eq!(classify(&day), TrainClass::Late);
        // A report off the line but planned after due also reaches it.
        day.reports = vec![report("2026-09-27T09:10:00Z", 5, 99)];
        assert_eq!(classify(&day), TrainClass::Late);
    }

    #[test]
    fn rule_1_reached_then_cut_short_is_skipped() {
        let day = TrainDay {
            activated: true,
            cancel: Some(Canx {
                canx_type: Some("EN ROUTE".into()),
                dep_min: Some(m("2026-09-27T09:20:00Z")),
                received_min: m("2026-09-27T09:15:00Z"),
            }),
            reports: vec![report("2026-09-27T09:00:00Z", 6, 7)],
            ..TrainDay::default()
        };
        assert_eq!(classify(&day), TrainClass::Skipped { late: true });
        let mid_line_origin = TrainDay {
            activated: true,
            origin_change_dep_min: Some(m("2026-09-27T09:20:00Z")),
            reports: vec![report("2026-09-27T09:20:00Z", 0, 7)],
            ..TrainDay::default()
        };
        assert_eq!(
            classify(&mid_line_origin),
            TrainClass::Skipped { late: false }
        );
    }

    #[test]
    fn rules_3_and_4_explicit_cancellations() {
        let at_origin = TrainDay {
            activated: true,
            cancel: Some(Canx {
                canx_type: Some("AT ORIGIN".into()),
                dep_min: None,
                received_min: m("2026-09-27T08:00:00Z"),
            }),
            ..TrainDay::default()
        };
        assert_eq!(classify(&at_origin), TrainClass::CancelledExplicit);
        // Cancelled EN ROUTE after the line: not this line's cancellation.
        let after_line = TrainDay {
            activated: true,
            cancel: Some(Canx {
                canx_type: Some("EN ROUTE".into()),
                dep_min: Some(m("2026-09-27T10:30:00Z")),
                received_min: m("2026-09-27T08:00:00Z"),
            }),
            ..TrainDay::default()
        };
        assert_eq!(classify(&after_line), TrainClass::Pending);
        let starts_after_line = TrainDay {
            activated: true,
            origin_change_dep_min: Some(m("2026-09-27T10:00:00Z")),
            ..TrainDay::default()
        };
        assert_eq!(classify(&starts_after_line), TrainClass::CancelledExplicit);
    }

    #[test]
    fn rule_5_presumed_only_when_allowed_and_observable() {
        assert_eq!(
            classify_line_train(&t(), None, &ctx()).0,
            TrainClass::CancelledPresumed
        );
        let mut no_presumed = ctx();
        no_presumed.presumed_allowed = false;
        assert_eq!(
            classify_line_train(&t(), None, &no_presumed).0,
            TrainClass::Pending
        );
        // The origin departure is within an hour of observed_from: its
        // Activation may have been missed.
        let mut late_start = ctx();
        late_start.observed_from_min = m("2026-09-27T08:00:00Z");
        assert_eq!(
            classify_line_train(&t(), None, &late_start).0,
            TrainClass::Pending
        );
        // Activated, not yet seen: pending, never presumed.
        let activated = TrainDay {
            activated: true,
            ..TrainDay::default()
        };
        assert_eq!(classify(&activated), TrainClass::Pending);
    }

    #[test]
    fn rules_6_and_7_upstream_lateness_and_pending() {
        let late_upstream = TrainDay {
            activated: true,
            reports: vec![report("2026-09-27T08:45:00Z", 8, 1)],
            ..TrainDay::default()
        };
        assert_eq!(
            classify_line_train(&t(), Some(&late_upstream), &ctx()),
            (TrainClass::Late, 8)
        );
        let on_time_upstream = TrainDay {
            activated: true,
            reports: vec![report("2026-09-27T08:45:00Z", 1, 1)],
            ..TrainDay::default()
        };
        assert_eq!(classify(&on_time_upstream), TrainClass::Pending);
    }

    #[test]
    fn a_train_due_before_observed_from_is_unobserved() {
        let mut c = ctx();
        c.observed_from_min = m("2026-09-27T09:30:00Z");
        assert_eq!(
            classify_line_train(&t(), None, &c).0,
            TrainClass::Unobserved
        );
    }

    fn params() -> WindowParams {
        WindowParams {
            recent_minutes: 60,
            grace_minutes: 10,
            activations_min: 20,
            feed_stale_secs: 300,
        }
    }

    #[test]
    fn feed_stale_on_an_old_event_or_too_few_activations() {
        let now = at("2026-09-27T12:00:00Z");
        let p = params();
        assert!(!feed_stale(Some(at("2026-09-27T11:58:00Z")), 20, now, &p));
        assert!(
            feed_stale(Some(at("2026-09-27T11:54:00Z")), 20, now, &p),
            "6 min old"
        );
        assert!(feed_stale(Some(at("2026-09-27T11:59:00Z")), 19, now, &p));
        assert!(feed_stale(None, 100, now, &p));
    }

    #[test]
    fn window_ranges_end_a_grace_period_ago_and_day_to_date_starts_at_the_rail_day() {
        let date: chrono::NaiveDate = "2026-09-27".parse().unwrap();
        let [recent, day] = window_ranges(date, at("2026-09-27T12:00:00Z"), &params(), false);
        assert_eq!(recent.0, FullCoverageWindowKind::Recent);
        assert_eq!(recent.1, at("2026-09-27T10:50:00Z"), "exclusive");
        assert_eq!(recent.2, at("2026-09-27T11:50:00Z"));
        assert_eq!(day.1, at("2026-09-27T01:00:00Z"), "02:00 BST");
        assert_eq!(day.2, at("2026-09-27T11:50:00Z"));
        let [_, closed] = window_ranges(date, at("2026-09-28T01:00:30Z"), &params(), true);
        assert_eq!(closed.2, at("2026-09-28T00:59:00Z"), "the whole rail day");
    }

    fn line_pop(trains: Vec<LineTrain>, relevance: Relevance) -> LinePop {
        LinePop {
            trains,
            relevance,
            ..LinePop::default()
        }
    }

    fn inputs<'a>(
        pop: &'a LinePop,
        trains: &'a TrainState,
        defaults: &'a common::Defaults,
        feed_stale: bool,
    ) -> LineInputs<'a> {
        LineInputs {
            line_id: "line-a",
            service_date: "2026-09-27".parse().unwrap(),
            pop,
            next_pop: None,
            trains,
            geometry: None,
            thresholds: defaults,
            observed_from: at("2026-09-26T19:00:00Z"),
            feed_stale,
            line_partial: false,
        }
    }

    /// Pending trains are not in `total`; presumed cancellation follows the
    /// line's relevance and the feed's health.
    #[test]
    fn a_window_counts_due_trains_and_leaves_pending_out_of_total() {
        let mut trains = TrainState::new("2026-09-27".parse().unwrap());
        trains.current.insert(
            "ONTIME".into(),
            TrainDay {
                activated: true,
                reports: vec![report("2026-09-27T11:00:00Z", 0, 1)],
                ..TrainDay::default()
            },
        );
        trains.current.insert(
            "LATE".into(),
            TrainDay {
                activated: true,
                reports: vec![report("2026-09-27T11:10:00Z", 7, 1)],
                ..TrainDay::default()
            },
        );
        trains.current.insert(
            "PENDING".into(),
            TrainDay {
                activated: true,
                ..TrainDay::default()
            },
        );
        let due = |uid: &str, time: &str| train(uid, time, time, "2026-09-27T10:00:00Z");
        let pop = line_pop(
            vec![
                due("OLD", "2026-09-27T09:00:00Z"), // before the recent window
                due("ONTIME", "2026-09-27T11:00:00Z"),
                due("LATE", "2026-09-27T11:10:00Z"),
                due("PENDING", "2026-09-27T11:20:00Z"),
                due("SILENT", "2026-09-27T11:30:00Z"),
                due("FUTURE", "2026-09-27T11:55:00Z"), // not yet due
            ],
            Relevance::Full,
        );
        let defaults = common::Defaults::default();
        let now = at("2026-09-27T12:00:00Z");
        let [(kind, from, to), (_, day_from, day_to)] =
            window_ranges("2026-09-27".parse().unwrap(), now, &params(), false);

        let healthy = inputs(&pop, &trains, &defaults, false);
        let w = healthy.window(kind, from, to, now);
        assert_eq!(w.counts.total, 3, "{:?}", w.counts);
        assert_eq!(w.counts.on_time, 1);
        assert_eq!(w.counts.delayed, 1);
        assert_eq!(w.counts.cancelled_presumed, 1);
        assert_eq!(w.counts.pending, 1);
        assert_eq!(w.counts.avg_delay_minutes, 3.5);
        assert!(w.presumed_enabled && !w.partial && !w.feed_stale);
        assert_eq!(w.relevance, "full");

        let d = healthy.window(FullCoverageWindowKind::DayToDate, day_from, day_to, now);
        assert_eq!(d.counts.total, 4, "the 09:00 train too (presumed)");

        let stale = inputs(&pop, &trains, &defaults, true);
        let w = stale.window(kind, from, to, now);
        assert!(w.feed_stale && !w.presumed_enabled);
        assert_eq!(w.counts.cancelled_presumed, 0);
        assert_eq!(w.counts.pending, 2);

        let old_population = line_pop(pop.trains.clone(), Relevance::StopsOnly);
        let w = inputs(&old_population, &trains, &defaults, false).window(kind, from, to, now);
        assert!(!w.presumed_enabled);
        assert_eq!(w.relevance, "stops_only");
        assert_eq!(w.counts.cancelled_presumed, 0);
    }

    /// A 0002 that arrived 3 hours or more before the train was due counts
    /// as cancelled in advance; a later one, or a change of origin past the
    /// line, does not.
    #[test]
    fn cancellations_that_arrived_three_hours_ahead_are_counted_in_advance() {
        let mut trains = TrainState::new("2026-09-27".parse().unwrap());
        let cancelled = |received: &str| TrainDay {
            activated: true,
            cancel: Some(Canx {
                canx_type: Some("AT ORIGIN".into()),
                dep_min: None,
                received_min: m(received),
            }),
            ..TrainDay::default()
        };
        trains
            .current
            .insert("EARLY".into(), cancelled("2026-09-27T08:00:00Z"));
        trains
            .current
            .insert("EDGE".into(), cancelled("2026-09-27T08:10:00Z"));
        trains
            .current
            .insert("LATE".into(), cancelled("2026-09-27T08:12:00Z"));
        trains.current.insert(
            "MOVED".into(),
            TrainDay {
                activated: true,
                origin_change_dep_min: Some(m("2026-09-27T13:00:00Z")),
                ..TrainDay::default()
            },
        );
        let due = |uid: &str, time: &str| train(uid, time, time, "2026-09-27T10:00:00Z");
        let pop = line_pop(
            vec![
                due("EARLY", "2026-09-27T11:00:00Z"),
                due("EDGE", "2026-09-27T11:10:00Z"),
                due("LATE", "2026-09-27T11:11:00Z"),
                due("MOVED", "2026-09-27T11:20:00Z"),
            ],
            Relevance::Full,
        );
        let defaults = common::Defaults::default();
        let now = at("2026-09-27T12:00:00Z");
        let [(kind, from, to), _] =
            window_ranges("2026-09-27".parse().unwrap(), now, &params(), false);
        let w = inputs(&pop, &trains, &defaults, false).window(kind, from, to, now);
        assert_eq!(w.counts.cancelled_explicit, 4, "{:?}", w.counts);
        assert_eq!(w.counts.cancelled_in_advance, 2, "EARLY and EDGE");
    }

    #[test]
    fn a_window_starting_within_an_hour_of_observed_from_is_partial() {
        let trains = TrainState::new("2026-09-27".parse().unwrap());
        let pop = line_pop(vec![], Relevance::Full);
        let defaults = common::Defaults::default();
        let mut i = inputs(&pop, &trains, &defaults, false);
        i.observed_from = at("2026-09-27T10:30:00Z");
        let now = at("2026-09-27T12:00:00Z");
        let [(kind, from, to), _] = window_ranges(i.service_date, now, &params(), false);
        assert!(i.window(kind, from, to, now).partial, "10:50 < 11:30");
        let later = at("2026-09-27T12:45:00Z");
        let [(kind, from, to), _] = window_ranges(i.service_date, later, &params(), false);
        assert!(!i.window(kind, from, to, later).partial);
        i.line_partial = true;
        assert!(i.window(kind, from, to, later).partial);
    }

    /// The closed-day v2 row is the whole-day day-to-date window, and reads
    /// available only once closed and complete.
    #[test]
    fn the_closed_day_row_is_the_whole_day_window() {
        let mut trains = TrainState::new("2026-09-27".parse().unwrap());
        trains.current.insert(
            "LATE".into(),
            TrainDay {
                activated: true,
                reports: vec![report("2026-09-27T23:50:00Z", 5, 1)],
                ..TrainDay::default()
            },
        );
        let pop = line_pop(
            vec![train(
                "LATE",
                "2026-09-27T23:50:00Z",
                "2026-09-27T23:55:00Z",
                "2026-09-27T23:00:00Z",
            )],
            Relevance::Full,
        );
        let defaults = common::Defaults::default();
        let next_pop = LinePop::default();
        let mut i = inputs(&pop, &trains, &defaults, false);
        i.next_pop = Some(&next_pop);
        let close = at("2026-09-28T00:00:30Z");
        let [_, (kind, from, to)] = window_ranges(i.service_date, close, &params(), true);
        let whole_day = i.window(kind, from, to, close);
        assert_eq!(
            whole_day.counts.delayed, 1,
            "due 10 min before close, still counted"
        );
        let row = line_row_v2(&whole_day, true);
        assert_eq!(row.availability, "available");
        assert_eq!(row.stats_version, Some(2));
        assert_eq!(row.stats.delayed, 1);
        assert_eq!(row.breakdown.as_ref(), Some(&whole_day.counts));
        assert_eq!(line_row_v2(&whole_day, false).availability, "pending");
    }

    // --- 2026-10-02: a train belongs to the rail day its due time is in ---

    /// A train of service date `date` booked at `time` London time.
    fn local_train(uid: &str, date: &str, time: &str) -> LineTrain {
        let due = to_minutes(common::rail_day::london_to_utc(
            date.parse().unwrap(),
            time.parse().unwrap(),
        ));
        LineTrain {
            uid: uid.into(),
            due_min: due,
            last_due_min: due + 20,
            origin_dep_min: due.saturating_sub(10),
        }
    }

    fn late_at(train: &LineTrain, delay: i16) -> TrainDay {
        TrainDay {
            activated: true,
            reports: vec![Report {
                planned_min: train.due_min,
                delay,
                tiploc: 1,
            }],
            ..TrainDay::default()
        }
    }

    /// Rail day `date`'s day-to-date (`now`) and closed-day windows for a
    /// line whose `date` population holds `today` and whose `date + 1`
    /// population holds `tomorrow`; each train is 5 minutes late (its
    /// TRUST state in the map the service date routes it to).
    fn rail_day_counts(
        date: &str,
        today: &[LineTrain],
        tomorrow: &[LineTrain],
        now: DateTime<Utc>,
    ) -> (FullCoverageWindowCounts, FullCoverageWindowCounts, bool) {
        let service_date: chrono::NaiveDate = date.parse().unwrap();
        let mut trains = TrainState::new(service_date);
        for t in today {
            trains.current.insert(t.uid.to_string(), late_at(t, 5));
        }
        for t in tomorrow {
            trains.next.insert(t.uid.to_string(), late_at(t, 5));
        }
        let pop = line_pop(today.to_vec(), Relevance::Full);
        let next_pop = line_pop(tomorrow.to_vec(), Relevance::Full);
        let defaults = common::Defaults::default();
        let mut i = inputs(&pop, &trains, &defaults, false);
        i.service_date = service_date;
        i.observed_from = crate::stats::rail_day_start(service_date) - chrono::Duration::hours(6);
        i.next_pop = Some(&next_pop);
        let [_, (kind, from, to)] = window_ranges(service_date, now, &params(), false);
        let so_far = i.window(kind, from, to, now);
        let close = crate::stats::rail_day_start(service_date + chrono::Duration::days(1))
            + chrono::Duration::seconds(30);
        let [_, (kind, from, to)] = window_ranges(service_date, close, &params(), true);
        let closed = i.window(kind, from, to, close);
        (so_far.counts, closed.counts, closed.partial)
    }

    /// The bug the 2026-10-02 shadow evaluation found: a train of service
    /// date D + 1 departing 00:30 London is due inside rail day D, so it
    /// belongs to D's day-to-date and closed-day counts (it used to fall in
    /// no window at all). A train of D + 1 due after 02:00 is not D's.
    #[test]
    fn the_next_dates_trains_before_0200_belong_to_this_rail_day() {
        let today = [local_train("DAY", "2026-09-30", "18:00:00")];
        let tomorrow = [
            local_train("EARLY", "2026-10-01", "00:30:00"),
            local_train("AFTER", "2026-10-01", "02:30:00"),
        ];
        // 00:50 London (23:50Z): the 00:30 train is due and counted.
        let (so_far, closed, partial) =
            rail_day_counts("2026-09-30", &today, &tomorrow, at("2026-09-30T23:50:00Z"));
        assert_eq!((so_far.total, so_far.delayed), (2, 2), "{so_far:?}");
        assert_eq!((closed.total, closed.delayed), (2, 2), "{closed:?}");
        assert!(!partial);
    }

    /// The 00:30 train is in the recent window around it, judged on the
    /// next date's TRUST state (its Activation went to `next`).
    #[test]
    fn the_recent_window_before_0200_counts_the_next_dates_trains() {
        let service_date: chrono::NaiveDate = "2026-09-30".parse().unwrap();
        let early = local_train("EARLY", "2026-10-01", "00:30:00");
        let mut trains = TrainState::new(service_date);
        // The same UID runs on both dates: each date's state is its own.
        trains.current.insert("EARLY".into(), late_at(&early, 0));
        trains.next.insert("EARLY".into(), late_at(&early, 9));
        let pop = line_pop(vec![], Relevance::Full);
        let next_pop = line_pop(vec![early], Relevance::Full);
        let defaults = common::Defaults::default();
        let mut i = inputs(&pop, &trains, &defaults, false);
        i.service_date = service_date;
        i.next_pop = Some(&next_pop);
        let now = at("2026-10-01T00:00:00Z"); // 01:00 London
        let [(kind, from, to), _] = window_ranges(service_date, now, &params(), false);
        let w = i.window(kind, from, to, now);
        assert_eq!((w.counts.total, w.counts.delayed), (1, 1), "{:?}", w.counts);
        assert_eq!(w.counts.avg_delay_minutes, 9.0);
        assert!(!w.partial);
    }

    /// The autumn change (2026-10-25, a 25-hour rail day 10-24) and the
    /// spring change (2027-03-28, a 23-hour rail day 03-27): the next
    /// date's trains before 02:00 local are still counted, and its trains
    /// after 02:00 local are not.
    #[test]
    fn the_rail_day_boundary_follows_dst() {
        // Autumn: 00:30 BST and the first 01:30 (BST) are before 02:00 GMT
        // (02:00Z); 02:15 GMT is after.
        let tomorrow = [
            local_train("A0030", "2026-10-25", "00:30:00"),
            local_train("A0130", "2026-10-25", "01:30:00"),
            local_train("A0215", "2026-10-25", "02:15:00"),
        ];
        let (_, closed, _) =
            rail_day_counts("2026-10-24", &[], &tomorrow, at("2026-10-25T01:50:00Z"));
        assert_eq!(closed.total, 2, "{closed:?}");
        // Spring: 00:30 GMT is before 02:00 BST (01:00Z); a booked 01:30
        // falls in the skipped hour and reads as 02:30 BST, so it is the
        // next rail day's.
        let tomorrow = [
            local_train("S0030", "2027-03-28", "00:30:00"),
            local_train("S0130", "2027-03-28", "01:30:00"),
        ];
        let (_, closed, _) =
            rail_day_counts("2027-03-27", &[], &tomorrow, at("2027-03-28T00:50:00Z"));
        assert_eq!(closed.total, 1, "{closed:?}");
    }

    /// Without the next date's population, a window that reaches local
    /// midnight is partial (its after-midnight trains cannot be counted);
    /// one that ends before midnight is not.
    #[test]
    fn a_window_past_midnight_without_the_next_population_is_partial() {
        let trains = TrainState::new("2026-09-27".parse().unwrap());
        let pop = line_pop(vec![], Relevance::Full);
        let defaults = common::Defaults::default();
        let i = inputs(&pop, &trains, &defaults, false);
        let evening = at("2026-09-27T22:00:00Z"); // 23:00 BST
        let [(kind, from, to), _] = window_ranges(i.service_date, evening, &params(), false);
        assert!(!i.window(kind, from, to, evening).partial);
        let night = at("2026-09-27T23:30:00Z"); // 00:30 BST, window ends 00:20
        let [(kind, from, to), (dkind, dfrom, dto)] =
            window_ranges(i.service_date, night, &params(), false);
        assert!(i.window(kind, from, to, night).partial);
        assert!(i.window(dkind, dfrom, dto, night).partial);
    }

    /// Decisions (2026-10-07): a train of service date D due on the line
    /// after rail day D + 1 has started (the up and down Night Riviera on
    /// the GWR lines, the up Highlander sleeper on the WCML, due 02:54 to
    /// 04:37) is counted in no rail day. D's closed day ends at
    /// `rail_day_start(D + 1)`, and D + 1's windows count only D + 1's
    /// population. Accepted as bounded; `due_after_next_rail_day_start`
    /// counts them for the metric.
    #[test]
    fn a_sleeper_due_after_the_next_rail_day_starts_is_counted_in_no_day() {
        let today = [
            local_train("DAY", "2026-09-30", "18:00:00"),
            local_train("RIVIERA", "2026-10-01", "04:18:00"),
        ];
        let pop = line_pop(today.to_vec(), Relevance::Full);
        let service_date: chrono::NaiveDate = "2026-09-30".parse().unwrap();
        assert_eq!(due_after_next_rail_day_start(&pop, service_date), 1);
        // Rail day 2026-09-30's closed day leaves it out...
        let (_, closed, _) = rail_day_counts("2026-09-30", &today, &[], at("2026-10-01T00:30:00Z"));
        assert_eq!(closed.total, 1, "{closed:?}");
        // ...and rail day 2026-10-01 never sees the 2026-09-30 population:
        // with nothing of its own, its windows at 04:30 count nothing.
        let (so_far, _, _) = rail_day_counts("2026-10-01", &[], &[], at("2026-10-01T03:30:00Z"));
        assert_eq!(so_far.total, 0, "{so_far:?}");
        // A train due before 02:00 on D + 1 (in D's own population) is D's.
        let early = line_pop(
            vec![local_train("EARLY", "2026-10-01", "01:30:00")],
            Relevance::Full,
        );
        assert_eq!(due_after_next_rail_day_start(&early, service_date), 0);
    }
}
