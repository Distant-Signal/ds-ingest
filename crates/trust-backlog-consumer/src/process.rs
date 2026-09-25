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
//! `service_date` for a Movement/Cancellation is sourced from a parked
//! Activation's own `service_date` when one has been observed for this
//! `train_id` in-process; failing that (Low finding #3 of the 2026-09-25
//! review's own fix), from the Europe/London rail day the message's OWN
//! timestamp falls in -- a Movement's `actual`/`planned` timestamp, or a
//! Cancellation's `canx_timestamp` -- and only as a last resort, when this
//! message carries no parseable timestamp of its own either, the current
//! processing-time rail day (`today`, passed in by the caller). Before this
//! fix that last resort was the ONLY fallback, which misfiled a message
//! under the wrong rail day whenever processing lagged the message's own
//! real-world time across the 02:00 cutover (a restart, a catch-up
//! backlog) -- see `process_message`'s own comments on the Movement/
//! Cancellation arms for the detail. An accepted approximation remains for
//! Activation only (which carries no usable timestamp of its own at all,
//! `schedule_start_date` deliberately excluded -- see below), identical in
//! kind to `trust-consumer::process.rs`'s own pre-existing "an Activation
//! this process never saw" gap, not a new one this module invents.
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

/// How many rail days a parked Activation's `(service_date, train_uid)` pair
/// is kept for. Mirrors `trust-consumer`'s own
/// `process::MAX_PARKED_ACTIVATION_AGE_DAYS`, and for the same reason: an
/// overnight working activated late on rail day D still emits Movements into
/// rail day D+1, so one day is too tight and three buys nothing.
pub const MAX_PARKED_ACTIVATION_AGE_DAYS: i64 = 2;

/// Ages out parked Activation state. Pure, so the caller supplies `today`
/// (the current Europe/London rail day) rather than this reading the clock,
/// exactly like `trust-consumer`'s `prune_expired_activations`.
///
/// # Why this exists at all (finding #5 of the 2026-09-25 review)
///
/// `pending_service_dates`/`pending_train_uids` were NEVER pruned -- not on
/// a weak signal like `trust-consumer`'s old `schedule_end_date` rule, but
/// not at all. Two consequences, and the second is worse than the unbounded
/// growth:
///
/// 1. Both maps are fed by the whole national Activation stream and this
///    process is designed to run indefinitely, so they grew without bound.
/// 2. TRUST RECYCLES `train_id`s, roughly monthly. A stale entry that
///    outlived its train meant a later, completely unrelated train reusing
///    that `train_id` -- whose own fresh Activation this process happened to
///    miss (a restart, a trimmed stream, a dropped payload) -- had every one
///    of its Movements filed under the OLD train's `service_date` and
///    stamped with the OLD train's `train_uid`. That is silent
///    cross-contamination of the backlog `api` matches late-tracking pins
///    against, not merely wasted memory. `unwrap_or(today)` (the
///    no-parked-Activation fallback) is strictly better than a stale hit:
///    it is honestly approximate, where the stale hit is confidently wrong.
pub fn prune_stale_activations(state: &mut ProcessorState, today: NaiveDate) {
    let oldest_kept = today - chrono::Duration::days(MAX_PARKED_ACTIVATION_AGE_DAYS);
    state
        .pending_service_dates
        .retain(|_, service_date| *service_date >= oldest_kept);
    // Kept in lockstep: both maps are written by the same Activation, keyed
    // by the same `train_id`, so `pending_service_dates` is the one source of
    // truth for how old an entry is (`pending_train_uids` carries no date of
    // its own). A uid whose service_date has been dropped must go with it --
    // otherwise the worse half of the bug above survives the prune.
    state
        .pending_train_uids
        .retain(|train_id, _| state.pending_service_dates.contains_key(train_id));
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

            // `today` (the processing rail day), not `service_date`, as the
            // key's date component -- see `trust_schema::dedup::dedup_key`'s
            // own doc comment for why every live consumer must use the same
            // rule. They are the same value on this path anyway (an
            // Activation's `service_date` IS `today`); passing `today`
            // explicitly keeps that a property of the call, not a
            // coincidence a later change could quietly break.
            let dedup = trust_schema::dedup::dedup_key(
                &activation.train_id,
                "0001",
                None,
                None,
                None,
                today,
            );
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

            // **Low finding #3 of the 2026-09-25 review.** The parked
            // Activation's own `service_date` is preferred first, unchanged
            // -- it is the authoritative, already-established running day
            // for this `train_id`. But the OLD fallback here was `today`,
            // the rail day this BATCH happened to be processed on
            // (`main.rs`'s `chrono::Utc::now()` at `next_batch` time, passed
            // in as `today`/`received_at`) -- wall-clock time, not this
            // Movement's own event time. Under any processing delay or
            // catch-up backlog that spans the 02:00 Europe/London rail-day
            // cutover (a restart, a slow consumer falling behind, a burst of
            // queued messages worked through after 02:00), a Movement whose
            // REAL `actual`/`planned` timestamp falls in the earlier rail day
            // was filed under the LATER one instead -- exactly the "late-night
            // event misfiled under the wrong day" class of bug
            // `common::rail_day::current_rail_day`'s own doc comment already
            // warns a bare wall-clock read causes. This consumer already has
            // a real per-event timestamp in scope for a Movement (`actual`,
            // falling back to `planned`) whenever this `train_id`'s
            // Activation was never parked, so deriving the rail day from
            // that -- not the processing clock -- is a strictly better
            // fallback: it dates the event by when it actually happened,
            // just like the parked-Activation path already does.
            //
            // `today` remains the LAST-resort fallback, for the rare case
            // this Movement itself carries no parseable timestamp either
            // (both `planned_timestamp`/`actual_timestamp` missing or
            // corrupted) -- there is genuinely nothing else to date it by.
            //
            // Deliberately NOT applied to `dedup`'s own `event_date` below:
            // `trust_schema::dedup::dedup_key`'s doc comment makes that date
            // a hard cross-consumer invariant (it MUST be "the rail day the
            // message was processed on," in every caller, so this consumer
            // and `trust-consumer` agree on the same key for the same live
            // message) -- changing it here would desync the two and defeat
            // `ON CONFLICT (trains_id, dedup_key)`'s de-duplication instead
            // of fixing a bug.
            let service_date = state
                .pending_service_dates
                .get(&movement.train_id)
                .copied()
                .unwrap_or_else(|| {
                    actual
                        .or(planned)
                        .map(common::rail_day::current_rail_day)
                        .unwrap_or(today)
                });

            let dedup = trust_schema::dedup::dedup_key(
                &movement.train_id,
                "0003",
                Some(&movement.event_type),
                movement.loc_stanox.as_deref(),
                movement.planned_timestamp.as_deref(),
                today,
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

            // Low finding #3, same fix and same reasoning as the Movement
            // arm above: prefer the parked Activation's own `service_date`,
            // then this Cancellation's own `canx_timestamp` (`actual`,
            // computed just above -- this is why it's computed before this
            // line rather than after, unlike the pre-fix ordering), and only
            // fall back to the processing-time `today` when neither is
            // available. `dedup`'s `event_date` below is deliberately left
            // as `today` for the same cross-consumer-invariant reason
            // documented on the Movement arm.
            let service_date = state
                .pending_service_dates
                .get(&cancellation.train_id)
                .copied()
                .unwrap_or_else(|| {
                    actual
                        .map(common::rail_day::current_rail_day)
                        .unwrap_or(today)
                });

            // A Cancellation's key carries nothing but `(train_id, msg_type)`
            // otherwise, and `api`'s `trust_event_backlog` enforces a GLOBAL
            // unique `dedup_key` across a 90-day retention -- so without this
            // date a recycled `train_id`'s genuinely new cancellation was
            // silently dropped as a duplicate of the previous month's
            // unrelated train. See `trust_schema::dedup::dedup_key`.
            let dedup = trust_schema::dedup::dedup_key(
                &cancellation.train_id,
                "0002",
                None,
                None,
                None,
                today,
            );

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
            change_time_minutes: None,
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
            toc_id: Some("SW".to_string()),
            train_service_code: Some("22345000".to_string()),
            schedule_wtt_id: Some("WTT1".to_string()),
            schedule_start_date: Some(schedule_start_date.to_string()),
            schedule_end_date: Some(schedule_start_date.to_string()),
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

    /// **Low finding #3 of the 2026-09-25 review, this fix's own regression
    /// test.** `movement()`'s fixture timestamp (`1787941920000` millis) is
    /// deliberately 2026-08-28 -- a different DAY from `today()`
    /// (2026-09-05, standing in here for "whatever rail day this batch
    /// happens to be processed on"). Before this fix, a Movement with no
    /// parked Activation fell back to `today` unconditionally, so this
    /// exact fixture would have come back service_date-2026-09-05 -- the
    /// PROCESSING day, not the day the event actually happened. After the
    /// fix, it must come back dated by its own `actual_timestamp` (run
    /// through the same Europe/London correction every other caller
    /// applies) instead: 2026-08-28.
    #[test]
    fn a_movement_with_no_parked_activation_uses_its_own_event_timestamps_rail_day() {
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
        assert_ne!(
            result.service_date,
            today(),
            "the fixture's own event timestamp and `today()` are deliberately different days -- \
             this proves the fix actually consults the event's own timestamp rather than \
             coincidentally matching `today` anyway"
        );
        assert_eq!(
            result.service_date,
            "2026-08-28".parse::<NaiveDate>().unwrap(),
            "the rail day `1787941920000` millis (Europe/London-corrected) actually falls in"
        );
    }

    /// The genuine last-resort case: no parked Activation AND no parseable
    /// timestamp of its own either (both fields missing) -- there is
    /// nothing left to date the event by except the processing-time
    /// fallback, so `today` is still the right answer here.
    #[test]
    fn a_movement_with_no_parked_activation_and_no_parseable_timestamp_falls_back_to_today() {
        let message = TrustMessage::Movement(trust_schema::schema::Movement {
            train_id: "999999999".to_string(),
            event_type: "DEPARTURE".to_string(),
            gbtt_timestamp: None,
            planned_timestamp: None,
            actual_timestamp: None,
            reporting_stanox: None,
            loc_stanox: Some("87212".to_string()),
            toc_id: None,
            variation_status: Some("ON TIME".to_string()),
        });
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

    // --- Parked-activation pruning (finding #5) ---

    /// The growth half of finding #5: entries older than
    /// `MAX_PARKED_ACTIVATION_AGE_DAYS` go, current ones stay, and both maps
    /// move together.
    #[test]
    fn pruning_drops_parked_activations_older_than_the_retention_window() {
        let mut state = ProcessorState::default();
        for (train_id, service_date) in [
            ("today", "2026-09-05"),
            ("yesterday", "2026-09-04"),
            ("two_days_ago", "2026-09-03"),
            ("last_month", "2026-08-05"),
        ] {
            state
                .pending_service_dates
                .insert(train_id.to_string(), service_date.parse().unwrap());
            state
                .pending_train_uids
                .insert(train_id.to_string(), format!("UID-{train_id}"));
        }

        prune_stale_activations(&mut state, today());

        assert!(state.pending_service_dates.contains_key("today"));
        assert!(
            state.pending_service_dates.contains_key("yesterday"),
            "an overnight working's Movements can still arrive a day later"
        );
        assert!(
            state.pending_service_dates.contains_key("two_days_ago"),
            "exactly at the retention boundary, still kept"
        );
        assert!(
            !state.pending_service_dates.contains_key("last_month"),
            "a month-old parked Activation can no longer belong to any live train"
        );
        assert!(
            !state.pending_train_uids.contains_key("last_month"),
            "the train_uid map must be pruned in lockstep, or the misfiling half of the bug \
             survives"
        );
        assert_eq!(state.pending_train_uids.len(), 3);
    }

    /// The correctness half of finding #5, which is the worse half: TRUST
    /// recycles `train_id`s monthly. A stale parked entry that outlives its
    /// train makes every Movement of the NEXT train to reuse that `train_id`
    /// -- when this process missed that train's own Activation -- get filed
    /// under the old train's `service_date` and stamped with the old train's
    /// `train_uid`. After pruning, the same Movement falls back to its own
    /// event timestamp's rail day (Low finding #3's fix) and carries no uid:
    /// honestly approximate instead of confidently wrong.
    #[test]
    fn a_recycled_train_id_is_not_misfiled_under_the_previous_trains_service_date() {
        let mut state = ProcessorState::default();
        let last_month: NaiveDate = "2026-08-05".parse().unwrap();
        state
            .pending_service_dates
            .insert("221832406".to_string(), last_month);
        state
            .pending_train_uids
            .insert("221832406".to_string(), "OLD001".to_string());

        // Before pruning: the stale entry wins, and it is wrong.
        let movement_msg = TrustMessage::Movement(movement(
            "221832406",
            "DEPARTURE",
            Some("87212"),
            Some("ON TIME"),
        ));
        let misfiled = process_message(
            &movement_msg,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
            test_received_at(),
        )
        .unwrap();
        assert_eq!(
            misfiled.service_date, last_month,
            "precondition: this is the misfiling the prune exists to stop"
        );
        assert_eq!(misfiled.train_uid, Some("OLD001".to_string()));

        prune_stale_activations(&mut state, today());

        let result = process_message(
            &movement_msg,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
            test_received_at(),
        )
        .unwrap();
        assert_eq!(
            result.service_date,
            "2026-08-28".parse::<NaiveDate>().unwrap(),
            "with the stale entry gone, the movement is filed under the day it actually \
             happened -- this fixture's own event timestamp's rail day, not the processing \
             day `today()`"
        );
        assert_eq!(
            result.train_uid, None,
            "and carries no uid rather than a completely unrelated train's"
        );
    }

    /// The Cancellation-arm twin of
    /// `a_movement_with_no_parked_activation_uses_its_own_event_timestamps_rail_day`:
    /// a Cancellation with no parked Activation must date itself by its own
    /// `canx_timestamp`, not by the processing-time `today`.
    #[test]
    fn a_cancellation_with_no_parked_activation_uses_its_own_event_timestamps_rail_day() {
        let cancellation = TrustMessage::Cancellation(trust_schema::schema::Cancellation {
            train_id: "999999999".to_string(),
            canx_timestamp: Some("1787941920000".to_string()),
            canx_reason_code: None,
            canx_type: None,
        });
        let mut state = ProcessorState::default();
        let result = process_message(
            &cancellation,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
            test_received_at(),
        )
        .unwrap();
        assert_eq!(
            result.service_date,
            "2026-08-28".parse::<NaiveDate>().unwrap(),
            "must be dated by its own canx_timestamp's rail day, not the processing day `today()`"
        );
    }

    /// And the Cancellation-arm last resort: no parked Activation AND no
    /// parseable `canx_timestamp` either -- `today` remains the only option.
    #[test]
    fn a_cancellation_with_no_parked_activation_and_no_timestamp_falls_back_to_today() {
        let cancellation = TrustMessage::Cancellation(trust_schema::schema::Cancellation {
            train_id: "999999999".to_string(),
            canx_timestamp: None,
            canx_reason_code: None,
            canx_type: None,
        });
        let mut state = ProcessorState::default();
        let result = process_message(
            &cancellation,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
            test_received_at(),
        )
        .unwrap();
        assert_eq!(result.service_date, today());
    }

    // --- Dedup keys (finding #6) ---

    /// A recycled `train_id`'s Cancellation a month later must not hash as a
    /// duplicate of the old month's one -- `api`'s `trust_event_backlog`
    /// enforces `ON CONFLICT (dedup_key) DO NOTHING` globally over a 90-day
    /// retention, so a collision silently discarded the newer event.
    #[test]
    fn a_recycled_train_ids_cancellation_gets_a_different_dedup_key_a_month_later() {
        let cancellation = TrustMessage::Cancellation(trust_schema::schema::Cancellation {
            train_id: "221832406".to_string(),
            canx_timestamp: None,
            canx_reason_code: None,
            canx_type: Some("AT ORIGIN".to_string()),
        });
        let mut state = ProcessorState::default();
        let august = process_message(
            &cancellation,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            "2026-08-05".parse().unwrap(),
            test_received_at(),
        )
        .unwrap();
        let september = process_message(
            &cancellation,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            "2026-09-05".parse().unwrap(),
            test_received_at(),
        )
        .unwrap();
        assert_ne!(august.dedup_key, september.dedup_key);
    }

    /// And the same real event processed twice on the same rail day still
    /// dedupes -- at-least-once redelivery depends on it.
    #[test]
    fn the_same_cancellation_on_the_same_day_keeps_one_dedup_key() {
        let cancellation = TrustMessage::Cancellation(trust_schema::schema::Cancellation {
            train_id: "221832406".to_string(),
            canx_timestamp: Some("1787941920000".to_string()),
            canx_reason_code: None,
            canx_type: None,
        });
        let mut state = ProcessorState::default();
        let first = process_message(
            &cancellation,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
            test_received_at(),
        )
        .unwrap();
        let redelivered = process_message(
            &cancellation,
            &mut state,
            &stanox_table(),
            &crs_index_with(&["WAT"]),
            today(),
            test_received_at(),
        )
        .unwrap();
        assert_eq!(first.dedup_key, redelivered.dedup_key);
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
