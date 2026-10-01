//! TRUST reason codes, forwarded to `api`'s `POST /private/train-reasons`
//! (`common::TrainReasonMessage`, stored in `train_reasons`). This runs
//! beside [`crate::process::process_message`], not inside it: `0006`
//! Change of Origin has no backlog row at all, and a reason must never
//! change what the backlog stores.
//!
//! `service_date` and `train_uid` are derived exactly as the same message's
//! backlog row derives them, so `api` files the reason under the same
//! `trains` row as the cancellation itself: the parked Activation's date,
//! else the Europe/London calendar date of the message's own timestamp,
//! else `today`.

use chrono::{DateTime, NaiveDate, Utc};
use trust_schema::schema::TrustMessage;

use crate::process::{ProcessorState, service_date_for_instant};

/// The reason carried by `message`, or `None` when it is not a `0002` or
/// `0006` or carries no (non-blank) reason code.
pub(crate) fn reason_message(
    message: &TrustMessage,
    state: &ProcessorState,
    today: NaiveDate,
    received_at: DateTime<Utc>,
) -> Option<common::TrainReasonMessage> {
    let (train_id, msg_type, reason_code, canx_type, loc_stanox, raw_timestamp) = match message {
        TrustMessage::Cancellation(c) => (
            &c.train_id,
            "0002",
            c.canx_reason_code.as_deref(),
            c.canx_type.clone(),
            c.loc_stanox.clone(),
            c.canx_timestamp.as_deref(),
        ),
        TrustMessage::ChangeOfOrigin(o) => (
            &o.train_id,
            "0006",
            o.reason_code.as_deref(),
            None,
            o.loc_stanox.clone(),
            o.dep_timestamp.as_deref(),
        ),
        _ => return None,
    };
    let reason_code = reason_code.map(str::trim).filter(|c| !c.is_empty())?;
    let event_at = common::trust_timestamp::parse_trust_epoch_millis_pair(
        None,
        raw_timestamp,
        received_at,
        state.trust_timestamp_correction_enabled,
    )
    .actual;
    let service_date = state
        .pending_service_dates
        .get(train_id)
        .copied()
        .unwrap_or_else(|| event_at.map_or(today, service_date_for_instant));
    Some(common::TrainReasonMessage {
        train_id: train_id.clone(),
        train_uid: state.pending_train_uids.get(train_id).cloned(),
        service_date,
        msg_type: msg_type.to_string(),
        reason_code: reason_code.to_string(),
        canx_type,
        loc_stanox,
        event_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use trust_schema::schema::{Cancellation, ChangeOfOrigin};

    fn today() -> NaiveDate {
        "2026-09-05".parse().unwrap()
    }

    fn received_at() -> DateTime<Utc> {
        "2099-01-01T12:00:00Z".parse().unwrap()
    }

    fn cancellation(code: Option<&str>) -> TrustMessage {
        TrustMessage::Cancellation(Cancellation {
            train_id: "722N71MW27".to_string(),
            canx_timestamp: None,
            canx_reason_code: code.map(str::to_string),
            canx_type: Some("EN ROUTE".to_string()),
            dep_timestamp: None,
            loc_stanox: Some("87702".to_string()),
        })
    }

    #[test]
    fn a_coded_cancellation_uses_the_parked_activations_identity() {
        let mut state = ProcessorState::default();
        let parked: NaiveDate = "2026-09-04".parse().unwrap();
        state
            .pending_service_dates
            .insert("722N71MW27".to_string(), parked);
        state
            .pending_train_uids
            .insert("722N71MW27".to_string(), "C11052".to_string());
        let reason = reason_message(&cancellation(Some(" TG ")), &state, today(), received_at())
            .expect("a coded cancellation yields a reason");
        assert_eq!(reason.msg_type, "0002");
        assert_eq!(reason.reason_code, "TG");
        assert_eq!(reason.train_uid.as_deref(), Some("C11052"));
        assert_eq!(reason.service_date, parked);
        assert_eq!(reason.canx_type.as_deref(), Some("EN ROUTE"));
        assert_eq!(reason.loc_stanox.as_deref(), Some("87702"));
    }

    #[test]
    fn a_cancellation_without_a_code_yields_nothing() {
        let state = ProcessorState::default();
        assert_eq!(
            reason_message(&cancellation(None), &state, today(), received_at()),
            None
        );
        assert_eq!(
            reason_message(&cancellation(Some("  ")), &state, today(), received_at()),
            None
        );
    }

    #[test]
    fn a_change_of_origin_reason_falls_back_to_today_without_a_timestamp() {
        let state = ProcessorState::default();
        let message = TrustMessage::ChangeOfOrigin(ChangeOfOrigin {
            train_id: "879H40MQ27".to_string(),
            dep_timestamp: None,
            loc_stanox: Some("87203".to_string()),
            reason_code: Some("YI".to_string()),
        });
        let reason = reason_message(&message, &state, today(), received_at()).unwrap();
        assert_eq!(reason.msg_type, "0006");
        assert_eq!(reason.reason_code, "YI");
        assert_eq!(reason.canx_type, None);
        assert_eq!(reason.train_uid, None);
        assert_eq!(reason.service_date, today());
    }

    #[test]
    fn other_messages_yield_nothing() {
        let state = ProcessorState::default();
        let message = TrustMessage::Unknown("0008".to_string());
        assert_eq!(
            reason_message(&message, &state, today(), received_at()),
            None
        );
    }
}
