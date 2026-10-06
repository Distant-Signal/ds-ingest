//! Decision 2d's matching algorithm: per-`(line_id, uid)` running record,
//! reusing `trust_schema::journey`'s derivation logic exactly as
//! trust-consumer does, keyed differently (per-(line_id, uid) here vs.
//! per-train_id there) -- confirmed compatible with zero generalization
//! by Task 1's own grounding pass.

use std::collections::HashMap;

use trust_schema::journey::DerivedState;
use trust_schema::schema::{Activation, Cancellation, Movement};

use crate::population::Population;
use crate::stanox_tiploc::StanoxTable;

#[derive(Debug, Clone, Default)]
pub(crate) struct CorrelationState {
    /// `train_id` -> `train_uid`, parked by Activation (mirrors
    /// trust-consumer's `ProcessorState.pending_activations`, but this
    /// consumer has no expiry-pruning need yet since it's rebuilt per
    /// rail day -- see Task 13's own cycle-reset note).
    pub pending_activations: HashMap<String, String>,
    /// (`line_id`, uid) -> `DerivedState`, one entry per line a UID has been
    /// matched against.
    pub derived: HashMap<(String, String), DerivedState>,
    /// `train_id` -> `train_uid`, learned once an Activation OR a matched
    /// Movement confirms it (mirrors ProcessorState.resolved).
    pub resolved: HashMap<String, String>,
}

/// The day of the month a TRUST `train_id` was activated for: its last
/// two characters (the origin departure's day of the month, by TRUST's own
/// id convention). `None` for an id not ending in two digits.
pub(crate) fn train_id_day_of_month(train_id: &str) -> Option<u32> {
    let digits = train_id.get(train_id.len().checked_sub(2)?..)?;
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Which of `candidates` an Activation's train runs on: the date of its
/// origin departure. From `tp_origin_timestamp` (a plain `YYYY-MM-DD`)
/// when present, else from the `train_id`'s day-of-month digits, matched
/// against `candidates` -- never from `schedule_start_date`, which is the
/// CIF validity window's start. `None` when neither says.
pub(crate) fn activation_service_date(
    activation: &Activation,
    candidates: &[chrono::NaiveDate],
) -> Option<chrono::NaiveDate> {
    use chrono::Datelike;
    if let Some(date) = activation
        .tp_origin_timestamp
        .as_deref()
        .and_then(|d| chrono::NaiveDate::parse_from_str(d.trim(), "%Y-%m-%d").ok())
    {
        return Some(date);
    }
    let day = train_id_day_of_month(&activation.train_id)?;
    candidates.iter().copied().find(|date| date.day() == day)
}

pub(crate) fn apply_activation(state: &mut CorrelationState, activation: &Activation) {
    state
        .pending_activations
        .insert(activation.train_id.clone(), activation.train_uid.clone());
}

/// Returns every `(line_id, uid)` this Movement was matched against, along
/// with the translated CRS for that STANOX (Decision 2c's STANOX->CRS
/// half) -- so the caller (Task 13) can also feed matches into
/// `station_correlate::apply_movement_station` without `correlate.rs`
/// itself depending on `station_correlate.rs` (keeps the two modules'
/// test suites independent).
pub(crate) fn apply_movement(
    state: &mut CorrelationState,
    movement: &Movement,
    stanox: &StanoxTable,
    tiploc_index: &HashMap<String, Vec<String>>,
    population: &Population,
    service_date: chrono::NaiveDate,
) -> MovementMatch {
    let Some(train_uid) = state
        .resolved
        .get(&movement.train_id)
        .cloned()
        .or_else(|| state.pending_activations.get(&movement.train_id).cloned())
    else {
        // no Activation seen for this train_id yet -- nothing to attribute
        return MovementMatch::default();
    };

    let loc_tiploc = movement
        .loc_stanox
        .as_deref()
        .and_then(|s| stanox.tiploc(s));
    let loc_crs = movement.loc_stanox.as_deref().and_then(|s| stanox.crs(s));
    let candidate_lines = loc_tiploc
        .and_then(|t| tiploc_index.get(t))
        .cloned()
        .unwrap_or_default();

    let mut matched = vec![];
    for line_id in candidate_lines {
        if population.contains(&line_id, service_date, &train_uid) {
            state
                .resolved
                .insert(movement.train_id.clone(), train_uid.clone());
            let key = (line_id.clone(), train_uid.clone());
            let previous = state
                .derived
                .entry(key.clone())
                .or_insert_with(DerivedState::awaiting_activation);
            // `None`: this per-`(line_id, uid)` correlation state has no
            // notion of a train's own schedule/destination at all (see this
            // module's own doc comment -- it's a pure STANOX/tiploc/
            // population match, not schedule-aware). Confirmed-arrival
            // detection (`apply_movement`'s new `destination_crs` param) is
            // scoped to the per-train public journey page
            // (`trust-consumer`/`crates/api`'s shared `trains` table), not
            // line-level correlation -- passing `None` here preserves this
            // module's exact pre-existing behavior (status only ever
            // "en_route"/"cancelled").
            *previous = trust_schema::journey::apply_movement(previous, movement, loc_crs, None);
            // `journey::apply_movement` leaves a LATE report's delay as
            // `None` for its caller to fill in, and this consumer never
            // did, so `stats::synthesize_departure` read every late train
            // as 0 minutes late and every row's `delayed` was 0 (windowed
            // stats design, section 3.1). TRUST's own `timetable_variation`
            // is the value.
            if let Some(delay) = trust_schema::schema::movement_delay_minutes(movement) {
                previous.delay_minutes = Some(delay);
            }
            matched.push(key);
        }
    }
    MovementMatch {
        train_uid,
        matched_lines: matched,
        loc_crs: loc_crs.map(str::to_string),
    }
}

/// Everything `main.rs`'s loop (Task 13) needs to also update
/// station-level state (Task 12) for one Movement -- `train_uid` even when
/// `matched_lines` is empty (a Movement can be un-matched at the line
/// level -- no candidate line's population contains this UID -- while
/// still carrying a real, already-resolved `train_uid` the station-level
/// pass has independent use for, per Decision 2h's own asymmetric rule).
#[derive(Debug, Clone, Default)]
pub(crate) struct MovementMatch {
    pub train_uid: String,
    pub matched_lines: Vec<(String, String)>,
    pub loc_crs: Option<String>,
}

/// Flips every line of the cancelled train's UID to `"cancelled"`.
///
/// The UID comes from a matched Movement (`resolved`) OR, failing that, from
/// the train's Activation (`pending_activations`), and every line whose
/// `service_date` population holds the UID gets an entry, whether or not a
/// Movement ever matched there. It used to need both a prior matched
/// Movement and an existing `derived` entry, so a train cancelled before it
/// moved -- most cancellations (windowed stats design, section 3.4) -- was
/// ignored, and only read "cancelled" through the no-event rule, which a
/// partial day switches off.
pub(crate) fn apply_cancellation(
    state: &mut CorrelationState,
    cancellation: &Cancellation,
    population: &Population,
    service_date: chrono::NaiveDate,
) -> Vec<(String, String)> {
    let Some(train_uid) = state
        .resolved
        .get(&cancellation.train_id)
        .or_else(|| state.pending_activations.get(&cancellation.train_id))
        .cloned()
    else {
        return vec![];
    };
    for line_id in population.lines_containing(service_date, &train_uid) {
        state
            .derived
            .entry((line_id.to_string(), train_uid.clone()))
            .or_insert_with(DerivedState::awaiting_activation);
    }
    let mut cancelled = vec![];
    for (key, derived) in &mut state.derived {
        if key.1 == train_uid {
            *derived = trust_schema::journey::apply_cancellation(derived);
            cancelled.push(key.clone());
        }
    }
    cancelled.sort();
    cancelled
}

#[cfg(test)]
#[expect(
    clippy::similar_names,
    reason = "test code: paired test values share names"
)]
mod tests {
    use super::*;

    fn tiploc_index_sharing_one_tiploc() -> HashMap<String, Vec<String>> {
        let mut index = HashMap::new();
        index.insert(
            "WATRLMN".to_string(),
            vec!["line-a".to_string(), "line-b".to_string()],
        );
        index
    }

    fn stanox_table() -> StanoxTable {
        StanoxTable::from_records(&[common::StanoxCrsRecord {
            stanox: "87212".to_string(),
            crs: "WAT".to_string(),
            tiploc: "WATRLMN".to_string(),
            station_name: "LONDON WATERLOO".to_string(),
            source_sequence: 1,
            change_time_minutes: None,
        }])
    }

    fn population_with_uid_in_line_a(date: chrono::NaiveDate) -> Population {
        let mut population = Population::default();
        population.insert(
            "line-a",
            date,
            vec![schedule_query::LinePopulationEntry {
                uid: "C11052".to_string(),
                calling_points: vec![],
                operator_atoc: None,
                train_status: None,
                ..Default::default()
            }],
        );
        population
    }

    fn activation(train_id: &str, train_uid: &str) -> Activation {
        Activation {
            train_id: train_id.to_string(),
            train_uid: train_uid.to_string(),
            toc_id: Some("SW".to_string()),
            train_service_code: Some("22345000".to_string()),
            schedule_wtt_id: None,
            schedule_start_date: Some("2026-09-04".to_string()),
            schedule_end_date: Some("2026-09-04".to_string()),
            tp_origin_timestamp: None,
        }
    }

    fn cancellation(train_id: &str) -> Cancellation {
        Cancellation {
            train_id: train_id.to_string(),
            canx_timestamp: None,
            canx_reason_code: None,
            canx_type: Some("AT ORIGIN".to_string()),
            dep_timestamp: None,
            loc_stanox: None,
        }
    }

    fn late_movement(train_id: &str, minutes: &str) -> Movement {
        Movement {
            variation_status: Some("LATE".to_string()),
            timetable_variation: Some(minutes.to_string()),
            ..movement(train_id)
        }
    }

    fn movement(train_id: &str) -> Movement {
        Movement {
            train_id: train_id.to_string(),
            event_type: "DEPARTURE".to_string(),
            gbtt_timestamp: None,
            planned_timestamp: None,
            actual_timestamp: None,
            reporting_stanox: None,
            loc_stanox: Some("87212".to_string()),
            toc_id: None,
            variation_status: Some("ON TIME".to_string()),
            timetable_variation: None,
        }
    }

    #[test]
    fn an_activation_then_a_movement_matches_only_the_line_whose_population_has_the_uid() {
        let mut state = CorrelationState::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        apply_activation(&mut state, &activation("T1", "C11052"));

        let result = apply_movement(
            &mut state,
            &movement("T1"),
            &stanox_table(),
            &tiploc_index_sharing_one_tiploc(),
            &population_with_uid_in_line_a(date),
            date,
        );

        assert_eq!(
            result.matched_lines,
            vec![("line-a".to_string(), "C11052".to_string())]
        );
        assert_eq!(result.train_uid, "C11052");
        assert_eq!(result.loc_crs.as_deref(), Some("WAT"));
        assert!(
            state
                .derived
                .contains_key(&("line-a".to_string(), "C11052".to_string()))
        );
        assert!(
            !state
                .derived
                .contains_key(&("line-b".to_string(), "C11052".to_string()))
        );
    }

    /// Regression test: a LATE movement's delay comes from TRUST's
    /// `timetable_variation`. It used to stay `None`, so every full-coverage
    /// row read `delayed = 0`.
    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "a single 12-minute sample averages to exactly 12.0"
    )]
    fn a_late_movement_records_trusts_timetable_variation_as_the_delay() {
        let mut state = CorrelationState::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        apply_activation(&mut state, &activation("T1", "C11052"));
        apply_movement(
            &mut state,
            &late_movement("T1", "12"),
            &stanox_table(),
            &tiploc_index_sharing_one_tiploc(),
            &population_with_uid_in_line_a(date),
            date,
        );
        let key = ("line-a".to_string(), "C11052".to_string());
        assert_eq!(state.derived[&key].delay_minutes, Some(12));

        let row = crate::stats::build_line_row(
            "line-a",
            date,
            &["C11052"],
            &state.derived,
            true,
            false,
            &common::Defaults::default(),
        );
        assert_eq!(row.stats.delayed, 1, "12 minutes late is delayed at 5");
        assert_eq!(row.stats.avg_delay_minutes, 12.0);

        // A later ON TIME report at the line overwrites it (last write
        // wins, as for every other DerivedState field).
        apply_movement(
            &mut state,
            &movement("T1"),
            &stanox_table(),
            &tiploc_index_sharing_one_tiploc(),
            &population_with_uid_in_line_a(date),
            date,
        );
        assert_eq!(state.derived[&key].delay_minutes, Some(0));
    }

    #[test]
    fn a_movement_with_no_prior_activation_matches_nothing_and_mutates_nothing() {
        let mut state = CorrelationState::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();

        let result = apply_movement(
            &mut state,
            &movement("unknown-train"),
            &stanox_table(),
            &tiploc_index_sharing_one_tiploc(),
            &population_with_uid_in_line_a(date),
            date,
        );

        assert!(result.matched_lines.is_empty());
        assert!(state.derived.is_empty());
    }

    #[test]
    fn two_movements_for_the_same_line_and_uid_update_the_same_derived_state_in_place() {
        let mut state = CorrelationState::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        apply_activation(&mut state, &activation("T1", "C11052"));

        apply_movement(
            &mut state,
            &movement("T1"),
            &stanox_table(),
            &tiploc_index_sharing_one_tiploc(),
            &population_with_uid_in_line_a(date),
            date,
        );
        apply_movement(
            &mut state,
            &movement("T1"),
            &stanox_table(),
            &tiploc_index_sharing_one_tiploc(),
            &population_with_uid_in_line_a(date),
            date,
        );

        assert_eq!(state.derived.len(), 1, "one entry, updated, not duplicated");
    }

    #[test]
    fn a_cancellation_after_a_movement_flips_every_matched_line_to_cancelled() {
        let mut state = CorrelationState::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        apply_activation(&mut state, &activation("T1", "C11052"));
        apply_movement(
            &mut state,
            &movement("T1"),
            &stanox_table(),
            &tiploc_index_sharing_one_tiploc(),
            &population_with_uid_in_line_a(date),
            date,
        );

        let cancelled = apply_cancellation(
            &mut state,
            &cancellation("T1"),
            &population_with_uid_in_line_a(date),
            date,
        );

        assert_eq!(
            cancelled,
            vec![("line-a".to_string(), "C11052".to_string())]
        );
        let derived = &state.derived[&("line-a".to_string(), "C11052".to_string())];
        assert_eq!(derived.status, "cancelled");
        assert_eq!(derived.last_reported_location, Some("WAT".to_string()));
    }

    /// Regression test: a train cancelled at origin never moves, so no
    /// Movement ever resolved its `train_id`. Its 0002 used to be ignored;
    /// now its Activation is enough, and every line whose population holds
    /// the UID reads it cancelled -- which matters on a partial day, where
    /// an unseen train is left out rather than presumed cancelled.
    #[test]
    fn a_cancellation_before_any_movement_cancels_the_train_on_its_lines() {
        let mut state = CorrelationState::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let population = population_with_uid_in_line_a(date);
        apply_activation(&mut state, &activation("T1", "C11052"));

        let cancelled = apply_cancellation(&mut state, &cancellation("T1"), &population, date);

        assert_eq!(
            cancelled,
            vec![("line-a".to_string(), "C11052".to_string())]
        );
        let row = crate::stats::build_line_row(
            "line-a",
            date,
            &["C11052"],
            &state.derived,
            true,
            true, // partial: unseen trains are left out
            &common::Defaults::default(),
        );
        assert_eq!(row.stats.total, 1, "the explicit cancellation is counted");
        assert_eq!(row.stats.cancelled, 1);
    }

    /// A 0002 for a train this process never saw activated still does
    /// nothing: there is no UID to attribute it to.
    #[test]
    fn a_cancellation_for_an_unknown_train_id_does_nothing() {
        let mut state = CorrelationState::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let cancelled = apply_cancellation(
            &mut state,
            &cancellation("T9"),
            &population_with_uid_in_line_a(date),
            date,
        );
        assert!(cancelled.is_empty());
        assert!(state.derived.is_empty());
    }

    #[test]
    fn an_activations_service_date_comes_from_tp_origin_then_the_train_id() {
        let d: chrono::NaiveDate = "2026-09-26".parse().unwrap();
        let next = d + chrono::Duration::days(1);
        let mut a = activation("722N71MW27", "C11052");
        a.tp_origin_timestamp = Some("2026-09-26".to_string());
        assert_eq!(activation_service_date(&a, &[d, next]), Some(d));
        a.tp_origin_timestamp = None;
        assert_eq!(activation_service_date(&a, &[d, next]), Some(next));
        assert_eq!(activation_service_date(&a, &[d]), None);
        assert_eq!(train_id_day_of_month("722N71MW27"), Some(27));
        assert_eq!(train_id_day_of_month("X"), None);
        assert_eq!(train_id_day_of_month("722N71MWAB"), None);
    }
}
