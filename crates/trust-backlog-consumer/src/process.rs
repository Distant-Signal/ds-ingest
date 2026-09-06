//! Filters and maps raw `TrustMessage`s into `trust_event_backlog` rows,
//! per docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md's
//! own "What counts as a key journey point" section:
//!
//! - Only Activation (`0001`) / Cancellation (`0002`) / Movement (`0003`)
//!   survive at all -- `ChangeOfOrigin`/`ChangeOfIdentity`/`Unknown` are
//!   dropped unconditionally, they carry no journey-point data.
//! - A Movement survives only if its `event_type` is `ARRIVAL` or
//!   `DEPARTURE` (never `PASS`) AND its translated CRS is in this
//!   consumer's own `crs_index` (catalogued-line scoping, Decision 2).
//! - An Activation/Cancellation survives regardless of location (neither
//!   carries one) -- scoping by CRS is meaningless for them; they are
//!   kept because they're load-bearing plumbing (Activation) or
//!   themselves a real journey event (Cancellation), per the plan's own
//!   reasoning.
//!
//! `service_date` for a bare Movement/Cancellation (neither carries a
//! date field) is sourced from a parked Activation's own
//! `schedule_start_date` when one has been observed for this `train_id`
//! in-process, falling back to the current Europe/London rail day
//! otherwise -- an accepted approximation identical in kind to
//! `trust-consumer::process.rs`'s own pre-existing "an Activation this
//! process never saw" gap, not a new one this module invents.

use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;
use trust_schema::schema::TrustMessage;

use crate::stanox_crs::StanoxCrsTable;

/// Cross-batch memory, mirroring `trust-consumer::process::ProcessorState`'s
/// own `pending_activations` map exactly (same purpose: a later
/// Movement/Cancellation needs the `service_date` an earlier Activation
/// carried). Deliberately does NOT carry a `resolved`/`last_derived`
/// equivalent -- this consumer has no notion of "resolving a pin" and no
/// per-train derived-state fold to maintain; every message is mapped
/// independently, not folded against a running journey state.
#[derive(Debug, Default)]
pub struct ProcessorState {
    pub pending_service_dates: HashMap<String, NaiveDate>,
}

pub fn process_message(
    message: &TrustMessage,
    state: &mut ProcessorState,
    stanox_crs: &StanoxCrsTable,
    crs_index: &HashSet<String>,
    today: NaiveDate,
) -> Option<common::TrustBacklogEventMessage> {
    match message {
        TrustMessage::Activation(activation) => {
            let service_date = activation
                .schedule_start_date
                .parse::<NaiveDate>()
                .unwrap_or(today);
            state
                .pending_service_dates
                .insert(activation.train_id.clone(), service_date);

            let dedup =
                trust_schema::dedup::dedup_key(&activation.train_id, "0001", None, None, None);
            Some(common::TrustBacklogEventMessage {
                crs: None,
                train_uid: Some(activation.train_uid.clone()),
                train_id: activation.train_id.clone(),
                service_date,
                msg_type: "0001".to_string(),
                event_type: None,
                planned_timestamp: None,
                actual_timestamp: None,
                variation_status: None,
                delay_minutes: None,
                dedup_key: dedup,
            })
        }

        TrustMessage::Movement(movement) => {
            // Only a real calling point -- never PASS. See this module's
            // own doc comment.
            if movement.event_type != "ARRIVAL" && movement.event_type != "DEPARTURE" {
                return None;
            }

            let loc_crs = movement
                .loc_stanox
                .as_deref()
                .and_then(|stanox| stanox_crs.stanox_to_crs(stanox))?;
            if !crs_index.contains(&loc_crs.to_uppercase()) {
                return None;
            }

            let planned = movement
                .planned_timestamp
                .as_deref()
                .and_then(parse_epoch_millis);
            let actual = movement
                .actual_timestamp
                .as_deref()
                .and_then(parse_epoch_millis);
            let delay_minutes = match (planned, actual, movement.variation_status.as_deref()) {
                (Some(p), Some(a), Some("LATE")) => Some((a - p).num_minutes() as i32),
                _ => None,
            };

            let service_date = state
                .pending_service_dates
                .get(&movement.train_id)
                .copied()
                .unwrap_or(today);

            let dedup = trust_schema::dedup::dedup_key(
                &movement.train_id,
                "0003",
                Some(&movement.event_type),
                movement.loc_stanox.as_deref(),
                movement.planned_timestamp.as_deref(),
            );

            Some(common::TrustBacklogEventMessage {
                crs: Some(loc_crs),
                train_uid: None, // this consumer doesn't correlate Activation->Movement in-process;
                // api's own backlog-match (Task 5) joins them at read time instead.
                train_id: movement.train_id.clone(),
                service_date,
                msg_type: "0003".to_string(),
                event_type: Some(movement.event_type.clone()),
                planned_timestamp: planned,
                actual_timestamp: actual,
                variation_status: movement.variation_status.clone(),
                delay_minutes,
                dedup_key: dedup,
            })
        }

        TrustMessage::Cancellation(cancellation) => {
            let service_date = state
                .pending_service_dates
                .get(&cancellation.train_id)
                .copied()
                .unwrap_or(today);
            let actual = cancellation
                .canx_timestamp
                .as_deref()
                .and_then(parse_epoch_millis);

            let dedup =
                trust_schema::dedup::dedup_key(&cancellation.train_id, "0002", None, None, None);

            Some(common::TrustBacklogEventMessage {
                crs: None,
                train_uid: None,
                train_id: cancellation.train_id.clone(),
                service_date,
                msg_type: "0002".to_string(),
                event_type: None,
                planned_timestamp: None,
                actual_timestamp: actual,
                variation_status: None,
                delay_minutes: None,
                dedup_key: dedup,
            })
        }

        TrustMessage::ChangeOfOrigin(_)
        | TrustMessage::ChangeOfIdentity(_)
        | TrustMessage::Unknown(_) => None,
    }
}

fn parse_epoch_millis(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let millis: i64 = raw.parse().ok()?;
    chrono::DateTime::from_timestamp_millis(millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stanox_table() -> StanoxCrsTable {
        StanoxCrsTable::from_records(vec![common::StanoxCrsRecord {
            stanox: "87212".to_string(),
            crs: "WAT".to_string(),
            tiploc: "WATRLMN".to_string(),
            station_name: "LONDON WATERLOO".to_string(),
            source_sequence: 1,
        }])
    }

    fn crs_index_with(crs: &[&str]) -> HashSet<String> {
        crs.iter().map(|c| c.to_uppercase()).collect()
    }

    fn today() -> NaiveDate {
        "2026-09-05".parse().unwrap()
    }

    fn movement(
        train_id: &str,
        event_type: &str,
        loc_stanox: Option<&str>,
        variation_status: Option<&str>,
    ) -> trust_schema::schema::Movement {
        trust_schema::schema::Movement {
            train_id: train_id.to_string(),
            event_type: event_type.to_string(),
            gbtt_timestamp: None,
            planned_timestamp: Some("1787941920000".to_string()),
            actual_timestamp: Some("1787941920000".to_string()),
            reporting_stanox: None,
            loc_stanox: loc_stanox.map(str::to_string),
            toc_id: None,
            variation_status: variation_status.map(str::to_string),
        }
    }

    fn activation(
        train_id: &str,
        train_uid: &str,
        schedule_start_date: &str,
    ) -> trust_schema::schema::Activation {
        trust_schema::schema::Activation {
            train_id: train_id.to_string(),
            train_uid: train_uid.to_string(),
            toc_id: "SW".to_string(),
            train_service_code: "22345000".to_string(),
            schedule_wtt_id: "WTT1".to_string(),
            schedule_start_date: schedule_start_date.to_string(),
            schedule_end_date: schedule_start_date.to_string(),
        }
    }

    #[test]
    fn a_departure_at_a_catalogued_crs_is_kept() {
        let message = TrustMessage::Movement(movement(
            "221832406",
            "DEPARTURE",
            Some("87212"),
            Some("ON TIME"),
        ));
        let mut state = ProcessorState::default();
        let result = process_message(
            &message,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
        );
        assert!(result.is_some());
        assert_eq!(result.unwrap().crs, Some("WAT".to_string()));
    }

    #[test]
    fn a_pass_event_is_dropped() {
        let message = TrustMessage::Movement(movement(
            "221832406",
            "PASS",
            Some("87212"),
            Some("ON TIME"),
        ));
        let mut state = ProcessorState::default();
        let result = process_message(
            &message,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
        );
        assert!(result.is_none());
    }

    #[test]
    fn a_departure_at_an_uncatalogued_crs_is_dropped() {
        let message = TrustMessage::Movement(movement(
            "221832406",
            "DEPARTURE",
            Some("87212"),
            Some("ON TIME"),
        ));
        let mut state = ProcessorState::default();
        let result = process_message(
            &message,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["EUS"]), // WAT not in scope
            today(),
        );
        assert!(result.is_none());
    }

    #[test]
    fn a_departure_at_an_untranslatable_stanox_is_dropped() {
        let message = TrustMessage::Movement(movement(
            "221832406",
            "DEPARTURE",
            Some("99999"), // not in stanox_table()
            Some("ON TIME"),
        ));
        let mut state = ProcessorState::default();
        let result = process_message(
            &message,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
        );
        assert!(result.is_none());
    }

    #[test]
    fn a_change_of_origin_is_always_dropped() {
        let message = TrustMessage::ChangeOfOrigin(trust_schema::schema::ChangeOfOrigin {
            train_id: "221832406".to_string(),
        });
        let mut state = ProcessorState::default();
        let result = process_message(
            &message,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
        );
        assert!(result.is_none());
    }

    #[test]
    fn a_movement_reuses_the_activations_own_service_date() {
        let activation_msg =
            TrustMessage::Activation(activation("221832406", "C21373", "2026-09-04"));
        let mut state = ProcessorState::default();
        process_message(
            &activation_msg,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
        );

        let movement_msg = TrustMessage::Movement(movement(
            "221832406",
            "DEPARTURE",
            Some("87212"),
            Some("ON TIME"),
        ));
        let result = process_message(
            &movement_msg,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
        )
        .unwrap();
        assert_eq!(
            result.service_date,
            "2026-09-04".parse::<NaiveDate>().unwrap()
        );
    }

    #[test]
    fn a_movement_with_no_parked_activation_falls_back_to_today() {
        let message = TrustMessage::Movement(movement(
            "999999999",
            "DEPARTURE",
            Some("87212"),
            Some("ON TIME"),
        ));
        let mut state = ProcessorState::default();
        let result = process_message(
            &message,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
        )
        .unwrap();
        assert_eq!(result.service_date, today());
    }
}
