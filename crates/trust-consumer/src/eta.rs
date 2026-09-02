//! Naive TRUST-only ETA: propagate the delay observed at a tracked
//! train's last-reported location uniformly forward. Coarse by design --
//! see docs/superpowers/specs/2026-08-28-train-tracking-design.md's ETA
//! approach section for why this is deliberately simple rather than
//! ML-derived, and `crates/api/src/data/eta_blend.rs` (Task 6) for the
//! Darwin-estimated alternative this yields to when available.

use chrono::{DateTime, Utc};

/// `remaining_scheduled` is the scheduled time of the calling point this
/// ETA is being computed for. Returns `None` if there's nothing to
/// propagate onto -- always true in this plan's v1 (see this file's
/// module docs on the missing CIF-backed calling-point list); wired in
/// now so a future pass only needs to start passing `Some(...)`, not
/// rewrite this function.
// `pub` doesn't exempt anything from `dead_code` in a binary crate, and the
// only caller this function is waiting on is the CIF-backed calling-point
// pass described above -- until that exists there is no `remaining_scheduled`
// to hand it, so `process.rs` documents it as deliberately uncalled rather
// than calling it for a guaranteed `None`. Allowed rather than deleted: the
// rule it encodes is tested, and re-deriving it later is pure waste.
#[allow(dead_code)]
pub fn propagate_eta(
    last_reported_planned: DateTime<Utc>,
    last_reported_actual: DateTime<Utc>,
    remaining_scheduled: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    let remaining_scheduled = remaining_scheduled?;
    let delay = last_reported_actual - last_reported_planned;
    Some(remaining_scheduled + delay)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_remaining_scheduled_time_means_no_eta() {
        let planned: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();
        let actual: DateTime<Utc> = "2026-08-28T18:37:00Z".parse().unwrap();
        assert_eq!(propagate_eta(planned, actual, None), None);
    }

    #[test]
    fn a_five_minute_delay_propagates_forward_uniformly() {
        let planned: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();
        let actual: DateTime<Utc> = "2026-08-28T18:37:00Z".parse().unwrap(); // 5m late
        let next_scheduled: DateTime<Utc> = "2026-08-28T18:50:00Z".parse().unwrap();
        assert_eq!(
            propagate_eta(planned, actual, Some(next_scheduled)),
            Some("2026-08-28T18:55:00Z".parse().unwrap())
        );
    }

    #[test]
    fn running_early_propagates_a_negative_offset() {
        let planned: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();
        let actual: DateTime<Utc> = "2026-08-28T18:30:00Z".parse().unwrap(); // 2m early
        let next_scheduled: DateTime<Utc> = "2026-08-28T18:50:00Z".parse().unwrap();
        assert_eq!(
            propagate_eta(planned, actual, Some(next_scheduled)),
            Some("2026-08-28T18:48:00Z".parse().unwrap())
        );
    }
}
