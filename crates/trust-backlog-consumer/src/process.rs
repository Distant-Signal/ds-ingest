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
//! date field) is sourced from a parked Activation's own `service_date`
//! when one has been observed for this `train_id` in-process, falling
//! back to the current Europe/London rail day otherwise -- an accepted
//! approximation identical in kind to `trust-consumer::process.rs`'s own
//! pre-existing "an Activation this process never saw" gap, not a new
//! one this module invents.
//!
//! An Activation's own `service_date` is the Europe/London rail day this
//! process was on when it handled the Activation message (`today`,
//! passed in by the caller) -- NOT `schedule_start_date`. That field is
//! the CIF schedule's own multi-month validity-window start (the CIF
//! `BS` record's Date-From field), not the calendar date this specific
//! train instance is running today; using it as `service_date` was a
//! real, confirmed live-production bug (every `trust_event_backlog` row
//! for a permanent CIF schedule ended up filed under the schedule's
//! validity-window start date instead of the day it actually ran,
//! silently breaking `api::data::trust_event_backlog_match`'s
//! `service_date = '<today>'` filter). TRUST delivers an Activation in
//! real time for the specific day's running, so the day it's processed
//! on is the correct service_date.

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
#[derive(Debug)]
pub struct ProcessorState {
    pub pending_service_dates: HashMap<String, NaiveDate>,
    /// `train_id -> train_uid`, populated identically to
    /// `pending_service_dates` (same Activation message, same lifetime --
    /// see this module's own doc comment). Closes the gap named in
    /// docs/superpowers/specs/2026-09-06-shared-train-identity-design.md §3:
    /// this consumer is now the PRIMARY writer for the shared trains/
    /// train_movement_events tables, and a real train_uid on every event
    /// is what lets `api` key a Movement into the right `trains` row at
    /// all. Never removed on read (unlike trust-consumer's own
    /// one-shot-claim `pending_activations`) -- a train's whole
    /// Activation-to-Cancellation lifetime may span many Movements, every
    /// one of which needs the same train_uid, not just the first.
    pub pending_train_uids: HashMap<String, String>,

    /// Finding #2's kill switch, mirroring
    /// `trust-consumer::process::ProcessorState`'s identical field exactly
    /// (same reasoning for living here rather than as a `process_message`
    /// parameter: avoids rippling a signature change through this module's
    /// own many existing test call sites for a value every one of them
    /// wants defaulted to `true`). Defaults to `true` via this struct's own
    /// `Default` impl below, NOT `#[derive(Default)]` (which would default
    /// a bare `bool` to `false`). `main.rs` sets it once from
    /// `config.trust_timestamp_correction_enabled`.
    pub trust_timestamp_correction_enabled: bool,
}

impl Default for ProcessorState {
    fn default() -> Self {
        Self {
            pending_service_dates: HashMap::new(),
            pending_train_uids: HashMap::new(),
            trust_timestamp_correction_enabled: true,
        }
    }
}

/// `received_at` is the wall-clock time this message is being processed
/// at (`main.rs` passes `chrono::Utc::now()`), threaded through to
/// `common::trust_timestamp::parse_trust_epoch_millis_pair` for every
/// `planned_timestamp`/`actual_timestamp`/`canx_timestamp` this function
/// parses -- see that function's own doc comment for the corrected-parsing
/// background, why it decides correction ONCE per message rather than
/// independently per field, and its guard against the correction itself
/// being wrong.
pub fn process_message(
    message: &TrustMessage,
    state: &mut ProcessorState,
    stanox_crs: &StanoxCrsTable,
    crs_index: &HashSet<String>,
    today: NaiveDate,
    received_at: chrono::DateTime<chrono::Utc>,
) -> Option<common::TrustBacklogEventMessage> {
    match message {
        TrustMessage::Activation(activation) => {
            // `schedule_start_date` is the CIF schedule's own multi-month
            // validity-window start (the CIF `BS` record's Date-From
            // field), not the calendar date this specific train instance
            // is running today -- see this module's own doc comment.
            // `today` (the day this Activation is actually being
            // processed) is the correct service_date: TRUST delivers an
            // Activation in real time, for the specific day's running, so
            // the processing day and the running day are the same.
            let service_date = today;
            state
                .pending_service_dates
                .insert(activation.train_id.clone(), service_date);
            state
                .pending_train_uids
                .insert(activation.train_id.clone(), activation.train_uid.clone());

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

            // ONE correction decision for both fields, anchored on
            // `actual_timestamp` -- see
            // `common::trust_timestamp::parse_trust_epoch_millis_pair`'s own
            // doc comment for why independent single-field calls could
            // desync `planned`/`actual` by a full hour (Finding #1).
            let timestamp_pair = common::trust_timestamp::parse_trust_epoch_millis_pair(
                movement.planned_timestamp.as_deref(),
                movement.actual_timestamp.as_deref(),
                received_at,
                state.trust_timestamp_correction_enabled,
            );
            let planned = timestamp_pair.planned;
            let actual = timestamp_pair.actual;
            if let Some(was_corrected) = timestamp_pair.was_corrected {
                metrics::counter!(
                    common::metrics::metric_name(
                        "trust_backlog_consumer_timestamp_correction_total"
                    ),
                    "outcome" => if was_corrected { "corrected" } else { "raw" }
                )
                .increment(1);
            }
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
                train_uid: state.pending_train_uids.get(&movement.train_id).cloned(),
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
            // Same decision function as a Movement's fields, with
            // `planned: None` (a Cancellation has no companion field) --
            // keeps this path under the same guard, kill switch, and
            // correction metric as everything else (Finding #1's own note
            // that this path needed checking too).
            let canx_pair = common::trust_timestamp::parse_trust_epoch_millis_pair(
                None,
                cancellation.canx_timestamp.as_deref(),
                received_at,
                state.trust_timestamp_correction_enabled,
            );
            if let Some(was_corrected) = canx_pair.was_corrected {
                metrics::counter!(
                    common::metrics::metric_name(
                        "trust_backlog_consumer_timestamp_correction_total"
                    ),
                    "outcome" => if was_corrected { "corrected" } else { "raw" }
                )
                .increment(1);
            }
            let actual = canx_pair.actual;

            let dedup =
                trust_schema::dedup::dedup_key(&cancellation.train_id, "0002", None, None, None);

            Some(common::TrustBacklogEventMessage {
                crs: None,
                train_uid: state
                    .pending_train_uids
                    .get(&cancellation.train_id)
                    .cloned(),
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

    /// `process_message`'s `received_at` for every test in this module.
    /// Deliberately set far past any raw timestamp fixture used anywhere in
    /// this file, so `common::trust_timestamp`'s plausibility guard can
    /// never reject a correction here by construction -- none of these
    /// tests are about that guard (see `common::trust_timestamp`'s own test
    /// module, and `api::data::trust_event_backlog_match`'s, for guard
    /// coverage).
    fn test_received_at() -> chrono::DateTime<chrono::Utc> {
        "2099-01-01T00:00:00Z".parse().unwrap()
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
            test_received_at(),
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
            test_received_at(),
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
            test_received_at(),
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
            test_received_at(),
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
            test_received_at(),
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
            test_received_at(),
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
            test_received_at(),
        )
        .unwrap();
        // The Activation was parked while processing `today()`
        // (2026-09-05), so that's the service_date a later Movement for
        // the same train_id must reuse -- NOT `schedule_start_date`
        // ("2026-09-04" here), which is the CIF schedule's own multi-month
        // validity-window start, not the date this specific instance is
        // running. See `an_activations_service_date_is_todays_date_not_the_schedules_validity_window_start`
        // below for the direct regression test against the Activation's
        // own emitted `service_date`.
        assert_eq!(result.service_date, today());
    }

    /// Regression test for a live-production bug: `schedule_start_date` on
    /// a real TRUST Activation is the CIF schedule's own multi-month
    /// validity-window start (the same value as the CIF `BS` record's
    /// Date-From field), NOT "the calendar date this specific train
    /// instance is running today". Confirmed against a real, currently
    /// running SWR Kingston-loop service (`train_uid=L83673`, CIF STP=P,
    /// valid 2026-07-27 through 2026-12-11, Mon-Fri): every
    /// `trust_event_backlog` row recorded on 2026-09-09 for real,
    /// same-day movements was stamped `service_date=2026-07-27` -- the
    /// schedule's validity-window start -- instead of 2026-09-09, the
    /// actual date those movements happened. That silently broke
    /// `api::data::trust_event_backlog_match`'s `service_date = '<today>'`
    /// filter for every tracked pin relying on the backlog fallback match.
    #[test]
    fn an_activations_service_date_is_todays_date_not_the_schedules_validity_window_start() {
        let activation_msg =
            TrustMessage::Activation(activation("221832406", "L83673", "2026-07-27"));
        let mut state = ProcessorState::default();
        let today = "2026-09-09".parse::<NaiveDate>().unwrap();
        let result = process_message(
            &activation_msg,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today,
            test_received_at(),
        )
        .unwrap();
        assert_eq!(result.service_date, today);
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
            test_received_at(),
        )
        .unwrap();
        assert_eq!(result.service_date, today());
    }

    #[test]
    fn a_movement_after_a_parked_activation_carries_the_real_train_uid() {
        let activation_msg =
            TrustMessage::Activation(activation("221832406", "C21373", "2026-09-05"));
        let mut state = ProcessorState::default();
        process_message(
            &activation_msg,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
            test_received_at(),
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
            test_received_at(),
        )
        .unwrap();
        assert_eq!(result.train_uid, Some("C21373".to_string()));
    }

    #[test]
    fn a_movement_with_no_parked_activation_still_carries_no_train_uid() {
        // The accepted, unavoidable gap this task's own doc comment names --
        // an Activation this process never saw leaves nothing to attach.
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
            test_received_at(),
        )
        .unwrap();
        assert_eq!(result.train_uid, None);
    }
}
