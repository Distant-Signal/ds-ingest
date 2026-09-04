//! Decision 2d's matching algorithm: per-`(line_id, uid)` running record,
//! reusing trust_schema::journey's derivation logic exactly as
//! trust-consumer does, keyed differently (per-(line_id, uid) here vs.
//! per-train_id there) -- confirmed compatible with zero generalization
//! by Task 1's own grounding pass.

use std::collections::HashMap;

use trust_schema::journey::DerivedState;
use trust_schema::schema::{Activation, Cancellation, Movement};

use crate::population::Population;
use crate::stanox_tiploc::StanoxTable;

#[derive(Debug, Clone, Default)]
pub struct CorrelationState {
    /// train_id -> train_uid, parked by Activation (mirrors
    /// trust-consumer's ProcessorState.pending_activations, but this
    /// consumer has no expiry-pruning need yet since it's rebuilt per
    /// rail day -- see Task 13's own cycle-reset note).
    pub pending_activations: HashMap<String, String>,
    /// (line_id, uid) -> DerivedState, one entry per line a UID has been
    /// matched against.
    pub derived: HashMap<(String, String), DerivedState>,
    /// train_id -> train_uid, learned once an Activation OR a matched
    /// Movement confirms it (mirrors ProcessorState.resolved).
    pub resolved: HashMap<String, String>,
}

pub fn apply_activation(state: &mut CorrelationState, activation: &Activation) {
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
pub fn apply_movement(
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
        if population
            .uids_for(&line_id, service_date)
            .contains(&train_uid.as_str())
        {
            state
                .resolved
                .insert(movement.train_id.clone(), train_uid.clone());
            let key = (line_id.clone(), train_uid.clone());
            let previous = state
                .derived
                .entry(key.clone())
                .or_insert_with(DerivedState::awaiting_activation);
            *previous = trust_schema::journey::apply_movement(previous, movement, loc_crs);
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
/// still carrying a real, already-resolved train_uid the station-level
/// pass has independent use for, per Decision 2h's own asymmetric rule).
#[derive(Debug, Clone, Default)]
pub struct MovementMatch {
    pub train_uid: String,
    pub matched_lines: Vec<(String, String)>,
    pub loc_crs: Option<String>,
}

pub fn apply_cancellation(
    state: &mut CorrelationState,
    cancellation: &Cancellation,
) -> Vec<(String, String)> {
    let Some(train_uid) = state.resolved.get(&cancellation.train_id).cloned() else {
        return vec![];
    };
    let mut cancelled = vec![];
    for (key, derived) in state.derived.iter_mut() {
        if key.1 == train_uid {
            *derived = trust_schema::journey::apply_cancellation(derived);
            cancelled.push(key.clone());
        }
    }
    cancelled
}

#[cfg(test)]
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
            }],
        );
        population
    }

    fn activation(train_id: &str, train_uid: &str) -> Activation {
        Activation {
            train_id: train_id.to_string(),
            train_uid: train_uid.to_string(),
            toc_id: "SW".to_string(),
            train_service_code: "22345000".to_string(),
            schedule_wtt_id: "".to_string(),
            schedule_start_date: "2026-09-04".to_string(),
            schedule_end_date: "2026-09-04".to_string(),
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
            &Cancellation {
                train_id: "T1".to_string(),
                canx_timestamp: None,
                canx_reason_code: None,
                canx_type: None,
            },
        );

        assert_eq!(
            cancelled,
            vec![("line-a".to_string(), "C11052".to_string())]
        );
        let derived = &state.derived[&("line-a".to_string(), "C11052".to_string())];
        assert_eq!(derived.status, "cancelled");
        assert_eq!(derived.last_reported_location, Some("WAT".to_string()));
    }
}
