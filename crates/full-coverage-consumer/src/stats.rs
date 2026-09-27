//! Decision 2f/2g's SampleStats synthesis and Decision 2e's
//! Resolved-vs-Pending rail-day gating, per line.

use std::collections::HashMap;

use common::{FullCoverageLineStatsRow, SampleStats, StationDeparture};
use trust_schema::journey::DerivedState;

pub(crate) fn synthesize_departure(uid: &str, derived: &DerivedState) -> StationDeparture {
    StationDeparture {
        service_id: uid.to_string(),
        operator: String::new(),
        destination_crs: String::new(),
        scheduled: String::new(),
        estimated: String::new(),
        is_cancelled: derived.status == "cancelled",
        delay_minutes: derived.delay_minutes.unwrap_or(0),
        cancel_reason: None,
        delay_reason: None,
        headcode: None,
        skipped_stations: vec![], // Decision 2g -- PASS-to-skipped mapping unresolved; left empty, not guessed
        // TRUST carries no platform signal at all -- Darwin/LDBWS is the
        // only source of platform data in this codebase (see
        // `StationDeparture.platform`'s own doc comment), and this is a
        // TRUST-derived synthesis, not a real LDBWS sample.
        platform: None,
        planned_platform: None,
    }
}

/// Builds one line's stats row for `service_date`. Every population UID
/// for this line/date with NO `derived` entry at all (Decision 2d's "zero
/// observed events" case) is treated as cancelled, per that decision's own
/// flagged accuracy-risk caveat -- applied here regardless of
/// `rail_day_closed`'s value for the STATS COMPUTATION, but `availability`
/// below still reads Pending until the window genuinely closes, per
/// Decision 2e's literal reading. A Pending row's stats are therefore a
/// preview, not yet the line's real determination -- consistent with
/// Available meaning "every scheduled service... has been matched," not
/// "matched so far."
///
/// **`partial`** (2026-09-27): this process does not hold every event of
/// `service_date` for this line -- it started mid-day and could not replay
/// the day's start (trimmed from `movement-events`, or a backend with no
/// replay), or the line's population was missing when consumption began.
/// "No event seen" then means nothing, so an unseen UID is left OUT of the
/// row rather than counted as cancelled, and the row never reads
/// "available", even once the day has closed. Before this, every restart
/// (about 200 on rail day 2026-09-26) turned every train already seen that
/// day into a "cancellation".
pub fn build_line_row(
    line_id: &str,
    service_date: chrono::NaiveDate,
    population_uids: &[&str],
    derived: &HashMap<(String, String), DerivedState>,
    rail_day_closed: bool,
    partial: bool,
    defaults: &common::Defaults,
) -> FullCoverageLineStatsRow {
    let departures: Vec<StationDeparture> = population_uids
        .iter()
        .filter_map(
            |uid| match derived.get(&(line_id.to_string(), uid.to_string())) {
                Some(state) => Some(synthesize_departure(uid, state)),
                None if partial => None,
                None => Some(StationDeparture {
                    service_id: uid.to_string(),
                    operator: String::new(),
                    destination_crs: String::new(),
                    scheduled: String::new(),
                    estimated: String::new(),
                    is_cancelled: true,
                    delay_minutes: 0,
                    cancel_reason: None,
                    delay_reason: None,
                    headcode: None,
                    skipped_stations: vec![],
                    platform: None,
                    planned_platform: None,
                }),
            },
        )
        .collect();

    let refs: Vec<&StationDeparture> = departures.iter().collect();
    let stats: SampleStats =
        common::compute_sample_stats(&refs, defaults.delay_threshold_minutes, |d| {
            !d.skipped_stations.is_empty()
        });

    FullCoverageLineStatsRow {
        line_id: line_id.to_string(),
        service_date,
        availability: if rail_day_closed && !partial {
            "available"
        } else {
            "pending"
        }
        .to_string(),
        stats,
        partial,
        breakdown: None,
        stats_version: None,
    }
}

/// The rail day in progress at `now` -- the service date every train
/// currently running belongs to. Between 00:00Z and 02:00 Europe/London
/// that is YESTERDAY's calendar date, not today's, because the rail day
/// that started at 02:00 local yesterday has not ended yet.
///
/// Derived from [`rail_day_closed`] rather than a second, independent
/// Europe/London 02:00 calculation: the day before `now`'s calendar date is
/// the current rail day exactly when that earlier day has NOT yet closed.
pub fn current_rail_service_date(now: chrono::DateTime<chrono::Utc>) -> chrono::NaiveDate {
    let today = now.date_naive();
    let yesterday = today - chrono::Duration::days(1);
    if rail_day_closed(yesterday, now) {
        today
    } else {
        yesterday
    }
}

/// The instant `service_date`'s rail day BEGAN: 02:00 Europe/London on
/// `service_date` -- equivalently, the boundary that closed the day
/// before, computed with the same `next_rail_day_boundary` anchoring as
/// [`rail_day_closed`] so the two can never disagree about DST.
pub fn rail_day_start(service_date: chrono::NaiveDate) -> chrono::DateTime<chrono::Utc> {
    let previous_midday = (service_date - chrono::Duration::days(1))
        .and_hms_opt(12, 0, 0)
        .expect("midday is a valid time")
        .and_utc();
    common::rail_day::next_rail_day_boundary(previous_midday)
}

/// Decision 2e: a line's rail day is "closed" once `now` has passed the
/// rail-day boundary that ENDS `service_date`'s own rail day (02:00
/// Europe/London on the calendar day after `service_date`).
///
/// **Correction to the plan's own sketch**, found while implementing this
/// function: the sketch anchored `common::rail_day::next_rail_day_boundary`
/// on `service_date` at midnight UTC. `next_rail_day_boundary` returns the
/// next 02:00-Europe/London instant *strictly after* the instant it's
/// given -- and midnight UTC on `service_date` is ~00:00-01:00 local
/// (UTC/BST), which is itself BEFORE that same calendar day's 02:00 local
/// cutoff. So the sketch's `next_rail_day_boundary(day_start)` resolves to
/// `service_date`'s own 02:00 local -- the START of `service_date`'s rail
/// day, not its end -- making `rail_day_closed` flip `true` about an hour
/// after the rail day it's meant to be gating BEGINS, not once it ends.
/// Anchoring on local noon instead keeps the reference instant safely
/// inside `service_date`'s rail day (`service_date`'s 02:00 local through
/// the next calendar day's 02:00 local) regardless of DST, so the
/// boundary this returns is genuinely the day's end.
pub fn rail_day_closed(
    service_date: chrono::NaiveDate,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let midday = service_date
        .and_hms_opt(12, 0, 0)
        .expect("midday is a valid time")
        .and_utc();
    common::rail_day::next_rail_day_boundary(midday) <= now
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> common::Defaults {
        common::Defaults::default()
    }

    #[test]
    fn every_uid_matched_and_none_cancelled_gives_total_equal_to_len() {
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let mut derived = HashMap::new();
        derived.insert(
            ("line-a".to_string(), "C1".to_string()),
            DerivedState {
                status: "en_route".to_string(),
                last_reported_location: None,
                last_event_type: None,
                delay_minutes: Some(0),
                next_calling_point: None,
            },
        );
        derived.insert(
            ("line-a".to_string(), "C2".to_string()),
            DerivedState {
                status: "en_route".to_string(),
                last_reported_location: None,
                last_event_type: None,
                delay_minutes: Some(0),
                next_calling_point: None,
            },
        );

        let row = build_line_row(
            "line-a",
            date,
            &["C1", "C2"],
            &derived,
            false,
            false,
            &defaults(),
        );
        assert_eq!(row.stats.total, 2);
        assert_eq!(row.stats.cancelled, 0);
    }

    #[test]
    fn a_uid_with_no_derived_entry_at_all_counts_as_cancelled_not_dropped() {
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let derived: HashMap<(String, String), DerivedState> = HashMap::new();

        let row = build_line_row("line-a", date, &["C1"], &derived, false, false, &defaults());
        assert_eq!(row.stats.total, 1, "the unmatched UID must still count");
        assert_eq!(row.stats.cancelled, 1);
    }

    #[test]
    fn availability_reads_pending_before_and_available_after_rail_day_closed() {
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let derived: HashMap<(String, String), DerivedState> = HashMap::new();

        let pending = build_line_row("line-a", date, &["C1"], &derived, false, false, &defaults());
        assert_eq!(pending.availability, "pending");

        let available = build_line_row("line-a", date, &["C1"], &derived, true, false, &defaults());
        assert_eq!(available.availability, "available");
    }

    /// A mid-day restart that could not replay the day's start: the
    /// trains seen before the restart are simply unknown to this process,
    /// so they are left out -- NOT counted as cancelled -- and the row can
    /// never claim "available".
    #[test]
    fn a_partial_day_leaves_unseen_uids_out_and_never_reads_available() {
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let mut derived = HashMap::new();
        derived.insert(
            ("line-a".to_string(), "C1".to_string()),
            DerivedState {
                status: "en_route".to_string(),
                last_reported_location: None,
                last_event_type: None,
                delay_minutes: Some(0),
                next_calling_point: None,
            },
        );

        let row = build_line_row(
            "line-a",
            date,
            &["C1", "C2", "C3"],
            &derived,
            true,
            true,
            &defaults(),
        );
        assert!(row.partial);
        assert_eq!(row.stats.total, 1, "only the seen UID is counted");
        assert_eq!(
            row.stats.cancelled, 0,
            "unseen is not cancelled on a partial day"
        );
        assert_eq!(
            row.availability, "pending",
            "even though the rail day has closed"
        );
    }

    #[test]
    fn rail_day_start_is_0200_london_on_the_service_date() {
        let bst: chrono::NaiveDate = "2026-07-15".parse().unwrap();
        assert_eq!(
            rail_day_start(bst),
            "2026-07-15T01:00:00Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
        );
        let gmt: chrono::NaiveDate = "2026-01-15".parse().unwrap();
        assert_eq!(
            rail_day_start(gmt),
            "2026-01-15T02:00:00Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
        );
        // The day after the clocks go forward (2026-03-29): 02:00 BST.
        let after_dst: chrono::NaiveDate = "2026-03-30".parse().unwrap();
        assert_eq!(
            rail_day_start(after_dst),
            "2026-03-30T01:00:00Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
        );
        // A day's start is the previous day's close.
        let prev = bst - chrono::Duration::days(1);
        assert!(rail_day_closed(prev, rail_day_start(bst)));
        assert!(!rail_day_closed(
            prev,
            rail_day_start(bst) - chrono::Duration::seconds(1)
        ));
    }

    #[test]
    fn rail_day_closed_matches_the_02_00_europe_london_boundary() {
        // Reuses Task 2's own verified boundary instant: for service_date
        // 2026-07-15, the rail day closes at 2026-07-16T01:00:00Z (02:00
        // BST) -- this only proves this file calls the shared function
        // correctly, not that the boundary logic itself is correct.
        let service_date: chrono::NaiveDate = "2026-07-15".parse().unwrap();
        let just_before: chrono::DateTime<chrono::Utc> = "2026-07-16T00:59:59Z".parse().unwrap();
        let just_after: chrono::DateTime<chrono::Utc> = "2026-07-16T01:00:01Z".parse().unwrap();

        assert!(!rail_day_closed(service_date, just_before));
        assert!(rail_day_closed(service_date, just_after));
    }
}
