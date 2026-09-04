//! Decision 2h: a second, parallel running record keyed (crs, toc_id),
//! fed off the same event stream correlate.rs already processes. The
//! asymmetric-population rule (a population UID only contributes once
//! its toc_id is learned from a real Activation) is this module's one
//! genuinely new correctness property -- see this module's own doc on
//! `StationCorrelationState::activations_by_uid`.
//!
//! **Merge-order note on this module's write path** (`build_station_rows`,
//! `queries::post_station_full_coverage_samples`), now resolved: this was
//! developed against a local placeholder (`StationFullCoverageSampleRow`)
//! while the separate `per-station-full-coverage-stats` branch, which owns
//! `common::StationFullCoverageSample`, hadn't merged yet. Both branches
//! are now merged, so this module uses the real type directly -- same
//! field shape (`crs`, `operator`, `resolved_at`, `stats`) the placeholder
//! always had, no logic change.

use std::collections::HashMap;

use trust_schema::journey::DerivedState;

#[derive(Debug, Clone, Default)]
pub struct StationCorrelationState {
    /// train_uid -> toc_id, learned only from a real Activation
    /// (`trust_schema::schema::Activation::toc_id`). A UID absent here has
    /// NOT been confirmed by TRUST this rail day -- Decision 2h's own
    /// "excluded entirely, not guessed" rule for the station-level output,
    /// asymmetric with the line-level output's treatment of the same case
    /// (Decision 2d still counts it as a line-level cancellation).
    pub activations_by_uid: HashMap<String, String>,
    /// (crs, toc_id) -> uid -> DerivedState
    pub derived: HashMap<(String, String), HashMap<String, DerivedState>>,
}

pub fn apply_activation(state: &mut StationCorrelationState, train_uid: &str, toc_id: &str) {
    state
        .activations_by_uid
        .insert(train_uid.to_string(), toc_id.to_string());
}

/// Called by `main.rs`'s loop (Task 13) once per matched `(line_id, uid)`
/// `correlate::apply_movement` reports, with the movement's translated CRS
/// (Decision 2c's STANOX->CRS half) -- a UID with no learned toc_id yet is
/// silently skipped here (returns `false`), per Decision 2h's own rule;
/// the caller increments `full_coverage_consumer_station_buckets_dropped_total`
/// when this returns `false`, so the drop is observable, not silent in the
/// operational sense even though it's silent in the stats themselves.
pub fn apply_movement_station(
    state: &mut StationCorrelationState,
    train_uid: &str,
    crs: &str,
    derived_line_level: &DerivedState,
) -> bool {
    let Some(toc_id) = state.activations_by_uid.get(train_uid).cloned() else {
        return false;
    };
    state
        .derived
        .entry((crs.to_string(), toc_id))
        .or_default()
        .insert(train_uid.to_string(), derived_line_level.clone());
    true
}

/// Mirrors Task 11's `build_line_row`/`post_full_coverage_stats` shape,
/// but producing one row per `(crs, toc_id)` bucket with at least one
/// `derived` entry -- Decision 2h's own "only pairs that actually
/// resolved this cycle are included" rule, no `Pending`-sentinel row.
pub fn build_station_rows(
    state: &StationCorrelationState,
    resolved_at: chrono::DateTime<chrono::Utc>,
    defaults: &common::Defaults,
) -> Vec<common::StationFullCoverageSample> {
    state
        .derived
        .iter()
        .map(|((crs, operator), by_uid)| {
            let departures: Vec<common::StationDeparture> = by_uid
                .iter()
                .map(|(uid, derived)| crate::stats::synthesize_departure(uid, derived))
                .collect();
            let refs: Vec<&common::StationDeparture> = departures.iter().collect();
            let stats =
                common::compute_sample_stats(&refs, defaults.delay_threshold_minutes, |d| {
                    !d.skipped_stations.is_empty()
                });
            common::StationFullCoverageSample {
                crs: crs.clone(),
                operator: operator.clone(),
                resolved_at,
                stats,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn derived(status: &str) -> DerivedState {
        DerivedState {
            status: status.to_string(),
            last_reported_location: Some("WAT".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: None,
        }
    }

    #[test]
    fn a_movement_with_no_prior_activation_is_dropped_and_mutates_nothing() {
        let mut state = StationCorrelationState::default();
        let matched = apply_movement_station(&mut state, "C11052", "WAT", &derived("en_route"));
        assert!(!matched);
        assert!(state.derived.is_empty());
    }

    #[test]
    fn a_movement_after_an_activation_files_under_the_right_crs_toc_bucket() {
        let mut state = StationCorrelationState::default();
        apply_activation(&mut state, "C11052", "SW");
        let matched = apply_movement_station(&mut state, "C11052", "WAT", &derived("en_route"));
        assert!(matched);
        assert!(
            state
                .derived
                .contains_key(&("WAT".to_string(), "SW".to_string()))
        );
    }

    #[test]
    fn two_uids_for_the_same_crs_toc_bucket_both_contribute_to_one_bucket() {
        let mut state = StationCorrelationState::default();
        apply_activation(&mut state, "C1", "SW");
        apply_activation(&mut state, "C2", "SW");
        apply_movement_station(&mut state, "C1", "WAT", &derived("en_route"));
        apply_movement_station(&mut state, "C2", "WAT", &derived("en_route"));

        assert_eq!(state.derived.len(), 1, "one bucket, not two");
        let bucket = &state.derived[&("WAT".to_string(), "SW".to_string())];
        assert_eq!(bucket.len(), 2, "both uids contribute to it");
    }

    #[test]
    fn build_station_rows_on_an_empty_state_is_an_empty_vec_no_pending_sentinel() {
        let state = StationCorrelationState::default();
        let rows = build_station_rows(&state, chrono::Utc::now(), &common::Defaults::default());
        assert!(rows.is_empty());
    }

    #[test]
    fn build_station_rows_produces_one_row_per_populated_bucket() {
        let mut state = StationCorrelationState::default();
        apply_activation(&mut state, "C1", "SW");
        apply_movement_station(&mut state, "C1", "WAT", &derived("en_route"));

        let rows = build_station_rows(&state, chrono::Utc::now(), &common::Defaults::default());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].crs, "WAT");
        assert_eq!(rows[0].operator, "SW");
        assert_eq!(rows[0].stats.total, 1);
    }
}
