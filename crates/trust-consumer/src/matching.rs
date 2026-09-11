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

/// `loc_crs` is the origin-departure Movement event's location, already
/// translated from STANOX by the caller (see Task 11's translation table).
/// Returns the first pending pin whose origin CRS matches and whose
/// scheduled departure is within `common::MATCH_TOLERANCE` of
/// `actual_timestamp`. If more than one pending pin matches (two users
/// pinned trains that happen to depart the same station within the
/// tolerance window), the earliest-created pin wins -- `pending` is
/// expected to be pre-sorted by `tracked_at` by the caller; this function
/// itself stays a simple first-match scan rather than re-deriving an
/// ordering it shouldn't own.
///
/// **Plausibility guard (defense-in-depth against the still-unconfirmed
/// TRUST timestamp corruption -- see
/// `common::trust_timestamp`'s own doc comment for the full background):**
/// before attempting any match at all, `actual_timestamp` is checked
/// against `received_at` (the wall-clock time this process is handling the
/// message that carried it) via `common::trust_timestamp::is_plausible_actual_timestamp`.
/// A message reporting an event that supposedly hasn't happened yet by
/// more than ordinary clock skew is rejected outright -- `None` is
/// returned without consulting `pending` at all, so no pin is bound to
/// what could easily be the wrong train. This guard fires independently of
/// whatever correction `common::trust_timestamp::parse_trust_epoch_millis`
/// already applied to `actual_timestamp` upstream of this call: even a bug
/// in that correction can't smuggle an implausible value into a matching
/// decision. Callers must NOT resolve the pin on a rejected match -- the
/// pin is left exactly as it was, for the next reference-reload cycle (or,
/// once the live TRUST window has closed, `api`'s own backlog-match sweep)
/// to retry once a plausible message eventually arrives.
pub fn resolve_origin_departure(
    loc_crs: &str,
    actual_timestamp: DateTime<Utc>,
    pending: &[PendingPin],
    received_at: DateTime<Utc>,
) -> Option<i64> {
    if !common::trust_timestamp::is_plausible_actual_timestamp(actual_timestamp, received_at) {
        tracing::warn!(
            loc_crs,
            actual_timestamp = %actual_timestamp,
            received_at = %received_at,
            "rejecting an origin-departure match: actual_timestamp is implausibly ahead of \
             receipt (beyond common::trust_timestamp::MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT) -- \
             this looks like the still-unconfirmed TRUST timestamp corruption documented in \
             common::trust_timestamp; leaving every pending pin unresolved so a later, \
             plausible message can be matched instead of binding to a likely-wrong train"
        );
        return None;
    }

    pending
        .iter()
        .find(|pin| {
            pin.pin_origin_crs.eq_ignore_ascii_case(loc_crs)
                && (pin.pin_scheduled_departure - actual_timestamp).abs() <= common::MATCH_TOLERANCE
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

    /// A `received_at` a plausible feed-processing lag (1 minute) after
    /// `actual`, for every test below that isn't specifically exercising
    /// the plausibility guard itself.
    fn received_shortly_after(actual: DateTime<Utc>) -> DateTime<Utc> {
        actual + chrono::Duration::minutes(1)
    }

    #[test]
    fn matches_an_on_time_departure() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();
        assert_eq!(
            resolve_origin_departure("WAT", actual, &pending, received_shortly_after(actual)),
            Some(1)
        );
    }

    #[test]
    fn matches_a_late_departure_within_tolerance() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T18:45:00Z".parse().unwrap(); // 13m late
        assert_eq!(
            resolve_origin_departure("WAT", actual, &pending, received_shortly_after(actual)),
            Some(1)
        );
    }

    #[test]
    fn does_not_match_outside_tolerance() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T19:10:00Z".parse().unwrap(); // 38m late
        assert_eq!(
            resolve_origin_departure("WAT", actual, &pending, received_shortly_after(actual)),
            None
        );
    }

    #[test]
    fn does_not_match_a_different_station() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();
        assert_eq!(
            resolve_origin_departure("PAD", actual, &pending, received_shortly_after(actual)),
            None
        );
    }

    #[test]
    fn the_earliest_created_pending_pin_wins_on_ambiguity() {
        let pending = vec![
            pin(1, "WAT", "2026-08-28T18:32:00Z"),
            pin(2, "WAT", "2026-08-28T18:35:00Z"),
        ];
        let actual: DateTime<Utc> = "2026-08-28T18:33:00Z".parse().unwrap();
        assert_eq!(
            resolve_origin_departure("WAT", actual, &pending, received_shortly_after(actual)),
            Some(1)
        );
    }

    // --- Plausibility guard (defense-in-depth against the TRUST timestamp
    // corruption documented in `common::trust_timestamp`) ---

    #[test]
    fn a_plausible_match_is_allowed_through() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();
        // A couple of minutes of ordinary clock skew, well within
        // `common::trust_timestamp::MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT`.
        let received_at = actual - chrono::Duration::minutes(2);
        assert_eq!(
            resolve_origin_departure("WAT", actual, &pending, received_at),
            Some(1),
            "ordinary clock skew must not trip the guard"
        );
    }

    #[test]
    fn an_implausible_match_is_rejected_and_does_not_resolve_the_pin() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        // actual_timestamp is inside MATCH_TOLERANCE of the pin's scheduled
        // departure -- everything about the CRS+time heuristic alone says
        // "match" -- but it's ~60 minutes AHEAD of when this message was
        // received, exactly the shape of the still-unconfirmed TRUST
        // timestamp corruption.
        let actual: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();
        let received_at: DateTime<Utc> = "2026-08-28T17:32:00Z".parse().unwrap();
        assert_eq!(
            resolve_origin_departure("WAT", actual, &pending, received_at),
            None,
            "an implausible actual_timestamp must not resolve any pin, even one that would \
             otherwise match cleanly"
        );
    }

    /// Reproduces the investigation's own concrete failure mode: a
    /// corrupted `actual_timestamp` ~60 minutes ahead of receipt falls
    /// OUTSIDE the truly-intended pin's tolerance window (so it could never
    /// have matched it anyway) while landing INSIDE some other pin's
    /// window purely by coincidence -- and the guard must reject the match
    /// to that other, wrong pin rather than silently binding to it.
    #[test]
    fn an_implausible_timestamp_does_not_silently_bind_to_a_different_nearby_pin() {
        let pending = vec![
            // The truly-intended pin: the real train that should match.
            pin(1, "WAT", "2026-08-28T18:32:00Z"),
            // A different, earlier-departing train at the same station,
            // whose own equally-corrupted actual_timestamp would otherwise
            // land inside ITS tolerance window.
            pin(2, "WAT", "2026-08-28T17:35:00Z"),
        ];
        // Raw (corrupted) actual_timestamp: ~60 minutes ahead of receipt.
        // It falls outside pin 1's tolerance window (an hour early) but
        // inside pin 2's -- exactly the mis-attribution risk this guard
        // exists to close.
        let actual: DateTime<Utc> = "2026-08-28T17:32:00Z".parse().unwrap();
        let received_at: DateTime<Utc> = "2026-08-28T16:32:00Z".parse().unwrap();
        assert_eq!(
            resolve_origin_departure("WAT", actual, &pending, received_at),
            None,
            "the corrupted timestamp must not be allowed to bind pin 2 just because it \
             happens to fall in its tolerance window"
        );
    }

    /// End-to-end reproduction of the investigation's own concrete failure
    /// mode, exercising BOTH fix layers together: Layer 2's corrected
    /// parsing (`common::trust_timestamp::parse_trust_epoch_millis`) feeds
    /// its result straight into `resolve_origin_departure`, and the RIGHT
    /// pin is matched -- not the wrong, merely-nearby one a raw,
    /// uncorrected reading would have coincidentally satisfied.
    #[test]
    fn a_corrected_timestamp_matches_the_correct_pin_instead_of_a_wrong_nearby_one() {
        // The true event: a real departure at WAT at 2026-08-28T18:32:00Z.
        // The corrupted feed emits this as a mislabelled Europe/London LOCAL
        // reading -- 19:32:00 BST -- read naively as if it were already
        // UTC, i.e. this raw wire value.
        let raw_actual_timestamp = "1787945520000"; // 2026-08-28T19:32:00Z as millis
        // The consumer's own real receipt time, shortly after the TRUE
        // event -- reliable and unaffected by the corruption (per the
        // investigation).
        let received_at: DateTime<Utc> = "2026-08-28T18:33:00Z".parse().unwrap();

        let pending = vec![
            // Pin 1: the truly-intended pin, matching the real 18:32:00Z
            // departure.
            pin(1, "WAT", "2026-08-28T18:32:00Z"),
            // Pin 2: a different train's pin that the RAW, uncorrected
            // 19:32:00Z reading would have coincidentally fallen inside the
            // tolerance window of (19:32 is 3 minutes from 19:35) -- the
            // exact wrong-train mis-attribution the investigation
            // documented.
            pin(2, "WAT", "2026-08-28T19:35:00Z"),
        ];

        let corrected =
            common::trust_timestamp::parse_trust_epoch_millis(raw_actual_timestamp, received_at)
                .expect("a valid epoch-millis string always parses");
        assert_eq!(
            corrected,
            "2026-08-28T18:32:00Z".parse::<DateTime<Utc>>().unwrap(),
            "the BST-period correction must bring the raw value back to the true UTC instant"
        );

        assert_eq!(
            resolve_origin_departure("WAT", corrected, &pending, received_at),
            Some(1),
            "the corrected timestamp must resolve to the CORRECT pin (1), not the wrong pin \
             (2) that only the raw, uncorrected reading would have matched"
        );
    }
}
