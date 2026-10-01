//! Windowed full-coverage stats: classifying each train due on a line
//! (design section 4.3.2) and counting them over the `recent` and
//! `day_to_date` windows (section 5), plus the v2 day-to-date / closed-day
//! row. Only used with `FULL_COVERAGE_WINDOWED_STATS=true`; the legacy
//! whole-day row (`stats::build_line_row`) is untouched.
//!
//! See docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md,
//! and its "Decisions (2026-09-27)" section for the delay threshold (3
//! minutes late at the train's first calling point on the line).

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use common::full_coverage_window::FULL_COVERAGE_STATS_VERSION;
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
    fn ctx(&self) -> ClassifyCtx {
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
            presumed_allowed: self.presumed_enabled(),
            observed_from_min: to_minutes(self.observed_from),
            line_tiplocs,
        }
    }

    fn presumed_enabled(&self) -> bool {
        self.pop.relevance == Relevance::Full && !self.feed_stale
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
        let ctx = self.ctx();
        let trains = &self.pop.trains;
        let lo = trains.partition_point(|t| t.due_min < from);
        let hi = trains.partition_point(|t| t.due_min <= to);
        let mut counts = FullCoverageWindowCounts::default();
        let mut delay_sum = 0i64;
        let mut delay_n = 0i64;
        for train in trains.get(lo..hi).unwrap_or_default() {
            let (class, delay) =
                classify_line_train(train, self.trains.current.get(&*train.uid), &ctx);
            if let Some(delay) = count(&mut counts, class, delay) {
                delay_sum += i64::from(delay);
                delay_n += 1;
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
        let partial = self.line_partial
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
        let i = inputs(&pop, &trains, &defaults, false);
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
}
