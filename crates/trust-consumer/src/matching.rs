//! Best-effort resolution of a user's pin (origin CRS + scheduled
//! departure time, date -- no train_uid) against the live TRUST feed. See
//! this plan's Task 10 for why this matches on the first origin-station
//! Movement event rather than on Activation alone (this app has no CIF
//! schedule lookup to bridge Activation's train_uid to a departure time).
//! A heuristic, not a guaranteed join -- same posture the design doc takes
//! on Darwin correlation.
//!
//! This is the fallback path. The reliable path is
//! `process::Reference::by_train_uid`'s Activation fast path, which knows
//! the train's real identity; everything here exists for the pins that
//! genuinely don't have one yet.

use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone)]
pub struct PendingPin {
    pub tracked_train_id: i64,
    pub pin_origin_crs: String,
    pub pin_scheduled_departure: DateTime<Utc>,
    /// The CIF schedule identity this pin ALREADY knows it is tracking
    /// (`common::TrackedTrainRef::train_uid`), when it has one -- a
    /// `schedule_matched` subscription, or an NR-primary one created via
    /// `POST /Train/by-uid/.../track`. `None` for a plain `pending` pin that
    /// genuinely only knows an origin CRS and a scheduled departure time.
    ///
    /// Deliberately NOT consulted by [`resolve_origin_departure`] below,
    /// which stays a pure CRS+time heuristic. It exists for
    /// `process::process_message`'s own contradiction filter, which drops a
    /// pin from the candidate set entirely when a parked Activation has
    /// already told this process that the claiming Movement's `train_id`
    /// belongs to a DIFFERENT `train_uid` -- see that filter's own comment
    /// for the production mis-attribution it closes.
    pub train_uid: Option<String>,
}

/// How far a TRUST Movement's own SCHEDULED (`planned_timestamp`, i.e. WTT)
/// departure time may sit from a pin's `pin_scheduled_departure` and still
/// be believed to describe the same booked service.
///
/// Deliberately far tighter than `common::MATCH_TOLERANCE` (20 minutes),
/// because it compares two *scheduled* times rather than a scheduled time
/// against an actual one: lateness is exactly what the wide tolerance
/// exists to absorb, and there is no lateness between two timetabled
/// values. The only real slack needed is the small disagreement between the
/// public timetable a Darwin-sourced pin was created from (GBTT) and the
/// working timetable TRUST reports (WTT) -- at an ORIGIN departure, the one
/// event this function ever matches, those are normally the same minute and
/// rarely differ by more than one or two.
///
/// 5 minutes is therefore generous for its purpose while still excluding
/// the busy-terminus neighbours that the 20-minute window swept in: at
/// London Waterloo a 20-minute window around one departure routinely
/// contains a dozen other services.
pub const SCHEDULED_DEPARTURE_TOLERANCE: Duration = Duration::minutes(5);

/// `loc_crs` is the origin-departure Movement event's location, already
/// translated from STANOX by the caller (see Task 11's translation table).
/// Returns the pending pin this departure most plausibly belongs to, or
/// `None` -- and `None` is always the right answer when in doubt, because a
/// claim is IRREVERSIBLE: `process::ProcessorState::resolved` has no unwind
/// path, so a pin bound to the wrong train_id locks the correct train out
/// for the life of the process, while an unclaimed pin is simply retried on
/// the next departure/reference-reload cycle (or, once the live TRUST
/// window has closed, by `api`'s own backlog-match sweep).
///
/// # Matching on the SCHEDULED time, nearest-first (finding #1 of the 2026-09-25 review)
///
/// This function used to take only `actual_timestamp` and return the FIRST
/// pin whose `pin_scheduled_departure` was within `common::MATCH_TOLERANCE`
/// (±20 minutes) of it, in the caller's `tracked_at` order. That is the same
/// bug family three rounds of fixes to `api`'s
/// `data::schedule_matching::find_schedule_match` chased on the CIF side:
/// **a proximity contest decided by iteration order rather than by
/// closeness, and against the loosest available signal.** Two concrete
/// failures, both real at any busy terminus:
///
/// 1. *A wrong train claims a distant pin.* Waterloo can see six or more
///    departures inside one 20-minute window. Whichever of them TRUST
///    reported first claimed the pin, with no identity check of any kind --
///    and once claimed, the correct train's own departure found the pin
///    taken and was permanently locked out.
/// 2. *The wrong pin is claimed among several open ones.* `pending` arrives
///    in `tracked_at` (pin-creation) order, so of two pins in range the
///    older one won even when the other was a minute away and it was
///    thirteen.
///
/// Both are closed by matching on what the message itself says its BOOKED
/// time was -- `planned_timestamp`, the WTT time, which `process.rs` already
/// parses and (before this fix) used only for the delay calculation -- with
/// [`SCHEDULED_DEPARTURE_TOLERANCE`]'s tight window, and by taking the
/// CLOSEST candidate rather than the first one seen. Scheduled-vs-scheduled
/// is the nearest thing this path has to an identity check: two different
/// services out of the same station genuinely do have different booked
/// departure times, however close together they run, whereas their ACTUAL
/// departure times converge as soon as either is delayed.
///
/// A Movement with no `planned_timestamp` at all still falls back to the
/// old, wide actual-time comparison (an unscheduled or ad-hoc working can
/// legitimately lack one, and dropping those outright would lose real
/// resolutions) -- but nearest-first there too, never first-found.
///
/// Ties are broken by `pending`'s own order, i.e. earliest-created wins,
/// which is the caller's documented `tracked_at` sort.
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
/// what could easily be the wrong train. The guard still anchors on
/// `actual_timestamp` even now that the match itself prefers
/// `planned_timestamp`: `actual` is the field the corruption was observed
/// in, and a message whose actual time is impossible is not one to trust
/// the rest of for a one-way decision. This guard fires independently of
/// whatever correction `common::trust_timestamp::parse_trust_epoch_millis`
/// already applied to `actual_timestamp` upstream of this call: even a bug
/// in that correction can't smuggle an implausible value into a matching
/// decision. Callers must NOT resolve the pin on a rejected match -- the
/// pin is left exactly as it was, for the next reference-reload cycle (or
/// `api`'s backlog-match sweep) to retry once a plausible message
/// eventually arrives.
pub fn resolve_origin_departure(
    loc_crs: &str,
    planned_timestamp: Option<DateTime<Utc>>,
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

    match planned_timestamp {
        // The ordinary case: compare booked time against booked time, with
        // the tight tolerance, nearest wins.
        Some(planned) => {
            closest_pin(loc_crs, planned, pending, SCHEDULED_DEPARTURE_TOLERANCE).map(|pin| {
                tracing::debug!(
                    loc_crs,
                    tracked_train_id = pin.tracked_train_id,
                    planned_timestamp = %planned,
                    pin_scheduled_departure = %pin.pin_scheduled_departure,
                    "claiming a pin on the closest scheduled-departure match"
                );
                pin.tracked_train_id
            })
        }
        // No booked time on the message at all -- fall back to the historic
        // wide actual-time window, still nearest-first. Weaker by
        // construction, so it is logged when it actually decides something.
        None => {
            closest_pin(loc_crs, actual_timestamp, pending, common::MATCH_TOLERANCE).map(|pin| {
                tracing::info!(
                    loc_crs,
                    tracked_train_id = pin.tracked_train_id,
                    actual_timestamp = %actual_timestamp,
                    pin_scheduled_departure = %pin.pin_scheduled_departure,
                    "claiming a pin on the weaker actual-time fallback: this Movement carried no \
                     planned_timestamp to compare booked times against"
                );
                pin.tracked_train_id
            })
        }
    }
}

/// The pin at `loc_crs` whose `pin_scheduled_departure` is CLOSEST to
/// `against`, provided it is within `tolerance`. Ties keep the earlier
/// element of `pending` (`min_by_key` is stable), which is the caller's
/// `tracked_at` ordering -- so "two users pinned genuinely
/// indistinguishable departures" still resolves deterministically to the
/// earliest-created pin, as it always did.
fn closest_pin<'a>(
    loc_crs: &str,
    against: DateTime<Utc>,
    pending: &'a [PendingPin],
    tolerance: Duration,
) -> Option<&'a PendingPin> {
    pending
        .iter()
        .filter(|pin| pin.pin_origin_crs.eq_ignore_ascii_case(loc_crs))
        .map(|pin| ((pin.pin_scheduled_departure - against).abs(), pin))
        .filter(|(delta, _)| *delta <= tolerance)
        .min_by_key(|(delta, _)| *delta)
        .map(|(_, pin)| pin)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin(id: i64, crs: &str, scheduled: &str) -> PendingPin {
        PendingPin {
            tracked_train_id: id,
            pin_origin_crs: crs.to_string(),
            pin_scheduled_departure: scheduled.parse().unwrap(),
            // This function never reads `train_uid` (see its own doc
            // comment) -- the contradiction filter that does lives in
            // `process`, and is covered by that module's own tests.
            train_uid: None,
        }
    }

    /// A `received_at` a plausible feed-processing lag (1 minute) after
    /// `actual`, for every test below that isn't specifically exercising
    /// the plausibility guard itself.
    fn received_shortly_after(actual: DateTime<Utc>) -> DateTime<Utc> {
        actual + chrono::Duration::minutes(1)
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        raw.parse().unwrap()
    }

    #[test]
    fn matches_an_on_time_departure() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual = ts("2026-08-28T18:32:00Z");
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(ts("2026-08-28T18:32:00Z")),
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            Some(1)
        );
    }

    /// A LATE departure still matches, because the comparison is
    /// booked-vs-booked: the 13 minutes of lateness live entirely in
    /// `actual_timestamp`, which no longer decides anything here.
    #[test]
    fn matches_a_late_departure_because_the_booked_times_still_agree() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual = ts("2026-08-28T18:45:00Z"); // 13m late
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(ts("2026-08-28T18:32:00Z")),
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            Some(1)
        );
    }

    #[test]
    fn does_not_match_a_different_booked_time() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual = ts("2026-08-28T19:10:00Z");
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(ts("2026-08-28T19:10:00Z")),
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            None
        );
    }

    #[test]
    fn does_not_match_a_different_station() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual = ts("2026-08-28T18:32:00Z");
        assert_eq!(
            resolve_origin_departure(
                "PAD",
                Some(actual),
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            None
        );
    }

    /// A small GBTT-vs-WTT disagreement (the pin came off a Darwin public
    /// departure board, the Movement reports the working timetable) must
    /// still match -- that slack is exactly what
    /// `SCHEDULED_DEPARTURE_TOLERANCE` is sized for.
    #[test]
    fn a_one_minute_public_vs_working_timetable_difference_still_matches() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual = ts("2026-08-28T18:33:30Z");
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(ts("2026-08-28T18:31:00Z")),
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            Some(1)
        );
    }

    // --- Finding #1: the busy-terminus wrong-train claim ---

    /// **Finding #1, failure 1.** A DIFFERENT service out of the same
    /// terminus, booked 12 minutes after the pinned one -- inside the old
    /// ±20-minute `MATCH_TOLERANCE` window, so the old code claimed the pin
    /// for it outright. Once claimed there is no unwind path, so the
    /// correct train was locked out for the life of the process.
    #[test]
    fn a_different_service_within_the_old_twenty_minute_window_does_not_claim_the_pin() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        // The other train's own booked departure, and it is running on time.
        let planned = ts("2026-08-28T18:44:00Z");
        let actual = ts("2026-08-28T18:44:00Z");
        assert!(
            (pending[0].pin_scheduled_departure - actual).abs() <= common::MATCH_TOLERANCE,
            "precondition: the OLD ±20-minute rule would have matched this"
        );
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(planned),
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            None,
            "a train booked 12 minutes away from the pin is not the pinned train"
        );
    }

    /// **Finding #1, failure 2.** Two open pins at the same terminus; the
    /// departing train's booked time is 1 minute from pin 2 and 13 minutes
    /// from pin 1. `pending` arrives in `tracked_at` order, so the old
    /// first-match scan took pin 1 -- the worse match -- and, being
    /// irreversible, left pin 2 unresolvable by its own train (which would
    /// find the only other pin already claimed).
    #[test]
    fn the_closest_pin_wins_not_the_earliest_created_one() {
        let pending = vec![
            pin(1, "WAT", "2026-08-28T18:32:00Z"), // created first, 13m away
            pin(2, "WAT", "2026-08-28T18:46:00Z"), // created second, 1m away
        ];
        let planned = ts("2026-08-28T18:45:00Z");
        let actual = ts("2026-08-28T18:45:00Z");
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(planned),
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            Some(2),
            "the pin whose booked departure is a minute away must win over one 13 minutes away, \
             whatever order the pins were created in"
        );
    }

    /// The same shape, with the closer pin FIRST -- so the test above can't
    /// pass merely by having reversed the old preference.
    #[test]
    fn the_closest_pin_wins_when_it_is_also_the_earliest_created_one() {
        let pending = vec![
            pin(1, "WAT", "2026-08-28T18:46:00Z"), // 1m away
            pin(2, "WAT", "2026-08-28T18:32:00Z"), // 13m away
        ];
        let planned = ts("2026-08-28T18:45:00Z");
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(planned),
                planned,
                &pending,
                received_shortly_after(planned)
            ),
            Some(1)
        );
    }

    /// Two pins genuinely equidistant from the departure (a user pinned the
    /// same minute twice, or two services really do share a booked minute):
    /// deterministic, earliest-created wins, exactly as the pre-fix
    /// behaviour documented.
    #[test]
    fn an_exact_tie_still_resolves_to_the_earliest_created_pin() {
        let pending = vec![
            pin(1, "WAT", "2026-08-28T18:32:00Z"),
            pin(2, "WAT", "2026-08-28T18:32:00Z"),
        ];
        let planned = ts("2026-08-28T18:32:00Z");
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(planned),
                planned,
                &pending,
                received_shortly_after(planned)
            ),
            Some(1)
        );
    }

    /// Pins at other stations must not be considered at all, however close
    /// their booked times are.
    #[test]
    fn a_closer_pin_at_a_different_station_is_ignored() {
        let pending = vec![
            pin(1, "PAD", "2026-08-28T18:45:00Z"), // closest, wrong station
            pin(2, "WAT", "2026-08-28T18:47:00Z"),
        ];
        let planned = ts("2026-08-28T18:45:00Z");
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(planned),
                planned,
                &pending,
                received_shortly_after(planned)
            ),
            Some(2)
        );
    }

    // --- The no-planned_timestamp fallback ---

    #[test]
    fn with_no_planned_timestamp_the_wide_actual_time_window_still_applies() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual = ts("2026-08-28T18:45:00Z"); // 13m late, no booked time
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                None,
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            Some(1)
        );
    }

    #[test]
    fn the_fallback_also_prefers_the_closest_pin() {
        let pending = vec![
            pin(1, "WAT", "2026-08-28T18:32:00Z"), // 13m from actual
            pin(2, "WAT", "2026-08-28T18:46:00Z"), // 1m from actual
        ];
        let actual = ts("2026-08-28T18:45:00Z");
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                None,
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            Some(2),
            "even the weaker fallback must not be decided by tracked_at order"
        );
    }

    #[test]
    fn the_fallback_still_respects_the_wide_tolerance_bound() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual = ts("2026-08-28T19:10:00Z"); // 38m late
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                None,
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            None
        );
    }

    /// A message that DOES carry a booked time is judged on it alone -- it
    /// must not silently fall back to the wide actual-time window when the
    /// booked comparison rejects every pin, or finding #1's whole fix would
    /// be bypassed by any late-running neighbour.
    #[test]
    fn a_booked_time_mismatch_does_not_fall_back_to_the_wide_actual_window() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        // Another service booked 12 minutes later, running 10 minutes early,
        // so its ACTUAL time lands 2 minutes from the pin's booked time.
        let planned = ts("2026-08-28T18:44:00Z");
        let actual = ts("2026-08-28T18:34:00Z");
        assert_eq!(
            resolve_origin_departure(
                "WAT",
                Some(planned),
                actual,
                &pending,
                received_shortly_after(actual)
            ),
            None
        );
    }

    // --- Plausibility guard (defense-in-depth against the TRUST timestamp
    // corruption documented in `common::trust_timestamp`) ---

    #[test]
    fn a_plausible_match_is_allowed_through() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        let actual = ts("2026-08-28T18:32:00Z");
        // A couple of minutes of ordinary clock skew, well within
        // `common::trust_timestamp::MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT`.
        let received_at = actual - chrono::Duration::minutes(2);
        assert_eq!(
            resolve_origin_departure("WAT", Some(actual), actual, &pending, received_at),
            Some(1),
            "ordinary clock skew must not trip the guard"
        );
    }

    #[test]
    fn an_implausible_match_is_rejected_and_does_not_resolve_the_pin() {
        let pending = vec![pin(1, "WAT", "2026-08-28T18:32:00Z")];
        // Everything about the heuristic alone says "match" -- the booked
        // times are identical -- but `actual_timestamp` is ~60 minutes
        // AHEAD of when this message was received, exactly the shape of the
        // still-unconfirmed TRUST timestamp corruption.
        let actual = ts("2026-08-28T18:32:00Z");
        let received_at = ts("2026-08-28T17:32:00Z");
        assert_eq!(
            resolve_origin_departure("WAT", Some(actual), actual, &pending, received_at),
            None,
            "an implausible actual_timestamp must not resolve any pin, even one that would \
             otherwise match cleanly"
        );
    }

    /// Reproduces the investigation's own concrete failure mode: a
    /// corrupted timestamp pair ~60 minutes ahead of receipt falls OUTSIDE
    /// the truly-intended pin's window (so it could never have matched it
    /// anyway) while landing INSIDE some other pin's window purely by
    /// coincidence -- and the guard must reject the match to that other,
    /// wrong pin rather than silently binding to it.
    #[test]
    fn an_implausible_timestamp_does_not_silently_bind_to_a_different_nearby_pin() {
        let pending = vec![
            // The truly-intended pin: the real train that should match.
            pin(1, "WAT", "2026-08-28T18:32:00Z"),
            // A different, earlier-departing train at the same station,
            // whose own equally-corrupted timestamps would otherwise land
            // inside ITS window.
            pin(2, "WAT", "2026-08-28T17:35:00Z"),
        ];
        // Raw (corrupted) timestamps: ~60 minutes ahead of receipt.
        let corrupted = ts("2026-08-28T17:32:00Z");
        let received_at = ts("2026-08-28T16:32:00Z");
        assert_eq!(
            resolve_origin_departure("WAT", Some(corrupted), corrupted, &pending, received_at),
            None,
            "the corrupted timestamp must not be allowed to bind pin 2 just because it \
             happens to fall in its window"
        );
    }

    /// End-to-end reproduction of the investigation's own concrete failure
    /// mode, exercising BOTH fix layers together: Layer 2's corrected
    /// parsing (`common::trust_timestamp::parse_trust_epoch_millis_pair`)
    /// feeds its result straight into `resolve_origin_departure`, and the
    /// RIGHT pin is matched -- not the wrong, merely-nearby one a raw,
    /// uncorrected reading would have coincidentally satisfied.
    #[test]
    fn a_corrected_timestamp_matches_the_correct_pin_instead_of_a_wrong_nearby_one() {
        // The true event: a real departure at WAT at 2026-08-28T18:32:00Z.
        // The corrupted feed emits this as a mislabelled Europe/London LOCAL
        // reading -- 19:32:00 BST -- read naively as if it were already
        // UTC, i.e. this raw wire value.
        let raw = "1787945520000"; // 2026-08-28T19:32:00Z as millis
        // The consumer's own real receipt time, shortly after the TRUE
        // event -- reliable and unaffected by the corruption (per the
        // investigation).
        let received_at = ts("2026-08-28T18:33:00Z");

        let pending = vec![
            // Pin 1: the truly-intended pin, matching the real 18:32:00Z
            // departure.
            pin(1, "WAT", "2026-08-28T18:32:00Z"),
            // Pin 2: a different train's pin that the RAW, uncorrected
            // 19:32:00Z reading would have coincidentally landed near.
            pin(2, "WAT", "2026-08-28T19:35:00Z"),
        ];

        let pair = common::trust_timestamp::parse_trust_epoch_millis_pair(
            Some(raw),
            Some(raw),
            received_at,
            true,
        );
        assert_eq!(
            pair.actual,
            Some(ts("2026-08-28T18:32:00Z")),
            "the BST-period correction must bring the raw value back to the true UTC instant"
        );

        assert_eq!(
            resolve_origin_departure(
                "WAT",
                pair.planned,
                pair.actual.unwrap(),
                &pending,
                received_at
            ),
            Some(1),
            "the corrected timestamps must resolve to the CORRECT pin (1), not the wrong pin \
             (2) that only the raw, uncorrected reading would have matched"
        );
    }
}
