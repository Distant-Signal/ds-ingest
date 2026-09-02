//! TRUST movement-feed message parsing. Field shapes are drawn only from
//! what docs/superpowers/specs/2026-08-28-train-tracking-design.md's
//! research pass independently confirmed (five of eight msg_types, by
//! name and field). `0005`/`0008` are unconfirmed and parse into
//! `TrustMessage::Unknown` rather than being guessed at -- per this
//! codebase's "no invented API details" convention.
//!
//! One thing that research pass got wrong: it claimed TRUST delivers a
//! JSON array of `{header, body}` envelopes per batch. A real RDM Train
//! Movements Kafka consumer run against `local.env` proved otherwise --
//! every record's payload was a single bare `{header, body}` object, which
//! made the old array-only `serde_json::from_str::<Vec<Envelope>>` fail
//! with `invalid type: map, expected a sequence` on every message. `parse_batch`
//! below accepts either shape defensively, since a single live data point
//! disproving "always an array" doesn't prove "never an array" either.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct Envelope {
    header: Header,
    body: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct Header {
    msg_type: String,
}

// `toc_id`/`train_service_code`/`schedule_wtt_id`/`schedule_start_date` are
// part of `0001`'s confirmed shape (this module's header doc) but have no
// consumer yet -- `process.rs`'s Activation handling only needs
// `train_id`/`train_uid` (to park a pin claim) and `schedule_end_date` (to
// expire it). Kept, not deleted, for the same "faithful port of the
// confirmed wire shape" reason as `matcher::Evidence`.
#[derive(Debug, Clone, Deserialize)]
pub struct Activation {
    pub train_id: String,
    pub train_uid: String,
    #[allow(dead_code)]
    pub toc_id: String,
    #[allow(dead_code)]
    pub train_service_code: String,
    #[allow(dead_code)]
    pub schedule_wtt_id: String,
    #[allow(dead_code)]
    pub schedule_start_date: String,
    pub schedule_end_date: String,
}

// `gbtt_timestamp`/`reporting_stanox`/`toc_id` are part of `0003`'s
// confirmed shape but have no consumer yet -- see the Activation comment
// above for why they're kept rather than deleted.
#[derive(Debug, Clone, Deserialize)]
pub struct Movement {
    pub train_id: String,
    pub event_type: String, // ARRIVAL | DEPARTURE | PASS
    #[allow(dead_code)]
    pub gbtt_timestamp: Option<String>,
    pub planned_timestamp: Option<String>,
    pub actual_timestamp: Option<String>,
    #[allow(dead_code)]
    pub reporting_stanox: Option<String>,
    pub loc_stanox: Option<String>,
    #[allow(dead_code)]
    pub toc_id: Option<String>,
    pub variation_status: Option<String>,
}

// `canx_reason_code`/`canx_type` are part of `0002`'s confirmed shape but
// have no consumer yet -- see the Activation comment above for why they're
// kept rather than deleted.
#[derive(Debug, Clone, Deserialize)]
pub struct Cancellation {
    pub train_id: String,
    pub canx_timestamp: Option<String>,
    #[allow(dead_code)]
    pub canx_reason_code: Option<String>,
    #[allow(dead_code)]
    pub canx_type: Option<String>, // "EN ROUTE" | "AT ORIGIN"
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChangeOfOrigin {
    pub train_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChangeOfIdentity {
    pub train_id: String,
}

#[derive(Debug, Clone)]
pub enum TrustMessage {
    Activation(Activation),
    Movement(Movement),
    Cancellation(Cancellation),
    ChangeOfOrigin(ChangeOfOrigin),
    ChangeOfIdentity(ChangeOfIdentity),
    /// Any `msg_type` this pass doesn't confirm the shape of (`0005`,
    /// `0008`, or anything else RDM's schema turns out to send). Carries
    /// the raw `msg_type` string for logging; the raw body is intentionally
    /// dropped here since there's no confirmed shape to hold it in.
    Unknown(String),
}

/// A real Kafka record's payload delivers a single `{header, body}` envelope
/// object -- confirmed by a live production error (see this module's header
/// doc). Defensive handling for a JSON array of envelopes is kept too, since
/// the design doc's research claimed that shape and it hasn't been *proven*
/// to never occur, e.g. on some other topic/product variant. Either way, one
/// malformed envelope inside an otherwise-good payload is logged and
/// skipped, not treated as a reason to drop everything else.
///
/// Dispatches on `serde_json::Value::is_array` rather than an
/// `#[serde(untagged)]` enum: untagged deserialization was tried first, but
/// on a genuinely malformed payload (wrong shape, not just wrong type) it
/// collapses both attempts into a single unhelpful "data did not match any
/// variant of untagged enum" with no field-level detail -- confirmed by
/// hand against this exact struct shape. Parsing to `Value` first and then
/// routing through `serde_json::from_value` keeps serde's normal, specific
/// field-level errors (e.g. "missing field `header`") for a shape that's
/// neither of the two expected ones.
pub fn parse_batch(raw: &str) -> anyhow::Result<Vec<TrustMessage>> {
    let value: serde_json::Value = serde_json::from_str(raw)?;
    let envelopes: Vec<Envelope> = if value.is_array() {
        serde_json::from_value(value)?
    } else {
        vec![serde_json::from_value(value)?]
    };
    Ok(envelopes.into_iter().filter_map(parse_envelope).collect())
}

fn parse_envelope(envelope: Envelope) -> Option<TrustMessage> {
    let parsed = match envelope.header.msg_type.as_str() {
        "0001" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::Activation),
        "0002" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::Cancellation),
        "0003" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::Movement),
        "0006" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::ChangeOfOrigin),
        "0007" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::ChangeOfIdentity),
        other => return Some(TrustMessage::Unknown(other.to_string())),
    };
    if parsed.is_none() {
        tracing::warn!(msg_type = %envelope.header.msg_type, "confirmed msg_type failed to parse against its known shape; dropping");
    }
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_an_activation_message() {
        let raw = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
            "train_service_code":"22345000","schedule_wtt_id":"WTT1",
            "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
        }}]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(matches!(&messages[0], TrustMessage::Activation(a) if a.train_uid == "C21373"));
    }

    #[test]
    fn parses_a_movement_message() {
        let raw = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
            "loc_stanox":"87701","variation_status":"LATE"
        }}]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(matches!(&messages[0], TrustMessage::Movement(m) if m.event_type == "DEPARTURE"));
    }

    /// The exact shape a real RDM Train Movements Kafka record delivers: a
    /// single bare `{header, body}` object, NOT wrapped in an array. This is
    /// the payload shape that triggered the live `invalid type: map,
    /// expected a sequence` error against `parse_batch`'s old
    /// array-only `serde_json::from_str::<Vec<Envelope>>` -- without the
    /// single/array dispatch above, this exact input reproduces that
    /// failure.
    #[test]
    fn parses_a_single_bare_envelope_object_not_wrapped_in_an_array() {
        let raw = r#"{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
            "loc_stanox":"87701","variation_status":"LATE"
        }}"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(matches!(&messages[0], TrustMessage::Movement(m) if m.event_type == "DEPARTURE"));
    }

    /// A payload that is neither a bare envelope object nor an array of
    /// them (e.g. missing `header` entirely) must fail with a specific,
    /// actionable field-level error -- not serde's generic untagged-enum
    /// "data did not match any variant" message, which names no field and
    /// gives no hint what's wrong.
    #[test]
    fn a_malformed_payload_produces_a_specific_field_level_error() {
        let raw = r#"{"not_an_envelope": true}"#;
        let err = parse_batch(raw).unwrap_err();
        assert!(
            err.to_string().contains("header"),
            "expected a field-level error mentioning `header`, got: {err}"
        );
    }

    #[test]
    fn unconfirmed_msg_types_become_unknown_not_a_parse_error() {
        let raw = r#"[{"header":{"msg_type":"0005"},"body":{"anything":"goes"}}]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(matches!(&messages[0], TrustMessage::Unknown(t) if t == "0005"));
    }

    #[test]
    fn a_confirmed_type_with_a_malformed_body_is_dropped_not_fatal() {
        let raw = r#"[
            {"header":{"msg_type":"0001"},"body":{"not_the_right_shape":true}},
            {"header":{"msg_type":"0001"},"body":{
                "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
                "train_service_code":"22345000","schedule_wtt_id":"WTT1",
                "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
            }}
        ]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(
            messages.len(),
            1,
            "the malformed envelope is dropped, the good one survives"
        );
    }

    #[test]
    fn a_batch_of_multiple_message_types_parses_all_of_them() {
        let raw = r#"[
            {"header":{"msg_type":"0002"},"body":{"train_id":"221832406","canx_type":"EN ROUTE"}},
            {"header":{"msg_type":"0006"},"body":{"train_id":"221832406"}},
            {"header":{"msg_type":"0007"},"body":{"train_id":"221832406"}}
        ]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 3);
        assert!(matches!(&messages[0], TrustMessage::Cancellation(_)));
        assert!(matches!(&messages[1], TrustMessage::ChangeOfOrigin(_)));
        assert!(matches!(&messages[2], TrustMessage::ChangeOfIdentity(_)));
    }
}
