//! Pure position-in-journey derivation from a sequence of TRUST events.
//! Structured the way `crates/aggregator/src/matcher.rs` is pure and
//! independently testable -- no I/O, no database, just "given the
//! previous state and one new event, what's the new state."

use crate::schema::Movement;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DerivedState {
    pub status: String, // "awaiting_activation" | "en_route" | "cancelled" | "completed"
    pub last_reported_location: Option<String>,
    pub last_event_type: Option<String>,
    pub delay_minutes: Option<i32>,
    pub next_calling_point: Option<String>,
}

impl DerivedState {
    pub fn awaiting_activation() -> Self {
        Self { status: "awaiting_activation".to_string(), ..Default::default() }
    }
}

/// `loc_crs` is the movement's location, already translated from STANOX by
/// the caller (a real STANOX->CRS table is out of scope for this plan --
/// see Task 14's note on where that lookup comes from); `None` if
/// untranslatable, in which case `last_reported_location` falls back to the
/// raw STANOX so nothing is silently dropped.
pub fn apply_movement(previous: &DerivedState, movement: &Movement, loc_crs: Option<&str>) -> DerivedState {
    let location = loc_crs.map(str::to_string).or_else(|| movement.loc_stanox.clone());
    let delay_minutes = variation_to_minutes(movement.variation_status.as_deref());

    // "PASS" doesn't complete the journey; only the last scheduled
    // location's ARRIVAL/DEPARTURE would, and this crate has no scheduled
    // calling-point list to know which location is "last" (see this
    // plan's Global Constraints) -- so status stays en_route regardless
    // of event_type until an explicit Cancellation ends it. A future
    // CIF-backed pass is the natural place to add real completion
    // detection.
    DerivedState {
        status: "en_route".to_string(),
        last_reported_location: location,
        last_event_type: Some(movement.event_type.clone()),
        delay_minutes,
        next_calling_point: previous.next_calling_point.clone(), // see module docs -- never populated ahead of time
    }
}

pub fn apply_cancellation(previous: &DerivedState) -> DerivedState {
    DerivedState { status: "cancelled".to_string(), ..previous.clone() }
}

/// TRUST's `variation_status` is a category ("ON TIME", "LATE", "EARLY"),
/// not itself a minute count in the confirmed field list -- delay minutes
/// have to come from actual_timestamp - planned_timestamp instead, which
/// this function deliberately does NOT compute (it needs both timestamps
/// parsed, done by the caller in Task 14 where they're already in scope).
/// This function only normalizes the enum-shaped part: "ON TIME"/"EARLY"
/// clamp to zero (never negative -- a train running early isn't a
/// passenger-facing "delay"), "LATE" is left for the caller to fill in
/// with the real minute count, and anything else is `None`.
fn variation_to_minutes(variation_status: Option<&str>) -> Option<i32> {
    match variation_status {
        Some("ON TIME") | Some("EARLY") => Some(0),
        Some("LATE") => None, // caller overwrites with a real value
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn movement(event_type: &str, variation_status: Option<&str>) -> Movement {
        Movement {
            train_id: "221832406".to_string(),
            event_type: event_type.to_string(),
            gbtt_timestamp: None,
            planned_timestamp: None,
            actual_timestamp: None,
            reporting_stanox: None,
            loc_stanox: Some("87701".to_string()),
            toc_id: None,
            variation_status: variation_status.map(str::to_string),
        }
    }

    #[test]
    fn a_movement_sets_status_to_en_route() {
        let previous = DerivedState::awaiting_activation();
        let state = apply_movement(&previous, &movement("DEPARTURE", Some("ON TIME")), Some("WAT"));
        assert_eq!(state.status, "en_route");
        assert_eq!(state.last_reported_location, Some("WAT".to_string()));
        assert_eq!(state.last_event_type, Some("DEPARTURE".to_string()));
    }

    #[test]
    fn falls_back_to_raw_stanox_when_untranslatable() {
        let previous = DerivedState::awaiting_activation();
        let state = apply_movement(&previous, &movement("PASS", None), None);
        assert_eq!(state.last_reported_location, Some("87701".to_string()));
    }

    #[test]
    fn on_time_and_early_clamp_delay_to_zero() {
        let previous = DerivedState::awaiting_activation();
        assert_eq!(apply_movement(&previous, &movement("ARRIVAL", Some("ON TIME")), Some("WOK")).delay_minutes, Some(0));
        assert_eq!(apply_movement(&previous, &movement("ARRIVAL", Some("EARLY")), Some("WOK")).delay_minutes, Some(0));
    }

    #[test]
    fn late_is_left_for_the_caller_to_fill_in() {
        let previous = DerivedState::awaiting_activation();
        assert_eq!(apply_movement(&previous, &movement("ARRIVAL", Some("LATE")), Some("WOK")).delay_minutes, None);
    }

    #[test]
    fn cancellation_preserves_last_known_location() {
        let previous = DerivedState {
            status: "en_route".to_string(),
            last_reported_location: Some("WOK".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(4),
            next_calling_point: None,
        };
        let state = apply_cancellation(&previous);
        assert_eq!(state.status, "cancelled");
        assert_eq!(state.last_reported_location, Some("WOK".to_string()));
    }
}
