//! Best-effort resolution of a user's pin (origin CRS + scheduled
//! departure time, date -- no train_uid) against the live TRUST feed. See
//! this plan's Task 10 for why this matches on the first origin-station
//! Movement event rather than on Activation alone (this app has no CIF
//! schedule lookup to bridge Activation's train_uid to a departure time).
//! A heuristic, not a guaranteed join -- same posture the design doc takes
//! on Darwin correlation.

use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct PendingPin {
    pub tracked_train_id: i64,
    pub pin_origin_crs: String,
    pub pin_scheduled_departure: DateTime<Utc>,
}

/// How far apart a pin's scheduled departure and an observed origin
/// departure event can be and still be considered the same real-world
/// service. Wide enough to survive a train running late from origin (the
/// single most common case), narrow enough that two different services
/// from the same station rarely both fall inside it.
const MATCH_TOLERANCE: chrono::Duration = chrono::Duration::minutes(20);

/// `loc_crs` is the origin-departure Movement event's location, already
/// translated from STANOX by the caller (see Task 11's translation table).
/// Returns the first pending pin whose origin CRS matches and whose
/// scheduled departure is within `MATCH_TOLERANCE` of `actual_timestamp`.
/// If more than one pending pin matches (two users pinned trains that
/// happen to depart the same station within the tolerance window), the
/// earliest-created pin wins -- `pending` is expected to be pre-sorted by
/// `tracked_at` by the caller; this function itself stays a simple
/// first-match scan rather than re-deriving an ordering it shouldn't own.
pub fn resolve_origin_departure(
    loc_crs: &str,
    actual_timestamp: DateTime<Utc>,
    pending: &[PendingPin],
) -> Option<i64> {
    pending
        .iter()
        .find(|pin| {
            pin.pin_origin_crs.eq_ignore_ascii_case(loc_crs)
                && (pin.pin_scheduled_departure - actual_timestamp).abs() <= MATCH_TOLERANCE
        })
        .map(|pin| pin.tracked_train_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin(id: i64, crs: &str, scheduled: &str) -> PendingPin {
        PendingPin {
            tracked_train_id: id,
            pin_origin_crs: crs.to_string(),
            pin_scheduled_departure: scheduled.parse().unwrap(),
        }
    }

    #[test]
    fn matches_an_on_time_departure() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();
        assert_eq!(resolve_origin_departure("WAT", actual, &pending), Some(1));
    }

    #[test]
    fn matches_a_late_departure_within_tolerance() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T18:45:00Z".parse().unwrap(); // 13m late
        assert_eq!(resolve_origin_departure("WAT", actual, &pending), Some(1));
    }

    #[test]
    fn does_not_match_outside_tolerance() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T19:10:00Z".parse().unwrap(); // 38m late
        assert_eq!(resolve_origin_departure("WAT", actual, &pending), None);
    }

    #[test]
    fn does_not_match_a_different_station() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();
        assert_eq!(resolve_origin_departure("PAD", actual, &pending), None);
    }

    #[test]
    fn the_earliest_created_pending_pin_wins_on_ambiguity() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z"), pin(2, "WAT", "2026-08-28T18:35:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T18:33:00Z".parse().unwrap();
        assert_eq!(resolve_origin_departure("WAT", actual, &pending), Some(1));
    }
}
