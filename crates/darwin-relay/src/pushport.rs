//! Decoding one Kafka record of the Darwin Push Port JSON topic into a
//! [`Batch`].
//!
//! **The record.** Darwin reaches the Rail Data Marketplace through a
//! message bridge, so a record's value is a JSON envelope of the original
//! message (`destination`, `messageID`, `properties`, `timestamp`, ...)
//! whose `bytes` field carries the Push Port document itself: on the JSON
//! topic, the `Pport` element rendered as JSON text. The value, or the
//! document inside `bytes`, may be gzipped (and then base64-encoded inside
//! `bytes`); [`decode_record`] undoes either. An XML document (the XML
//! topic, or the base topic's base64 XML) is recognised and skipped: this
//! relay reads the JSON topic only.
//!
//! **The document.** The JSON rendering follows the Push Port v18 XSD
//! closely: an element's attributes and children are both keys, a
//! repeated element is an array while a single one is an object, an
//! element's text is under the key `""` (or the element is a bare string
//! when it has no attributes), and values are strings (`"true"`, `"12"`).
//! [`many`], [`flag`] and [`text`] absorb those shapes. The root carries
//! `ts` (the message time, with an offset) and `version`, plus `uR` (an
//! update) or `sR` (a snapshot), or a heartbeat (`FailureResp`, `HBOK`).
//!
//! Kept from each update ([`parse_document`]): `schedule`, `TS`,
//! `association` and `deactivated`. Everything else (station messages,
//! formations, loading, alarms, train orders) is ignored.

use std::io::Read;

use base64::Engine;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use common::darwin::{
    Association, Batch, CallKey, Deactivated, Event, Platform, ScheduleCall, ScheduleFields,
    Service, StatusCall,
};
use serde_json::Value;

/// The largest document this relay will gunzip to. A Push Port update is a
/// few KB, a large snapshot fragment a few MB.
const MAX_DECODED_BYTES: u64 = 32 * 1024 * 1024;

/// The schedule location elements, by their XSD names: origin, operational
/// origin, intermediate, operational intermediate, passing point,
/// destination, operational destination.
const SCHEDULE_LOCATIONS: [&str; 7] = ["OR", "OPOR", "IP", "OPIP", "PP", "DT", "OPDT"];

/// What one record held.
#[derive(Debug, PartialEq)]
pub(crate) enum Decoded {
    /// A Push Port update or snapshot, possibly with nothing DS keeps.
    Document(Batch),
    /// A heartbeat or a message with no update (`FailureResp`).
    Heartbeat,
    /// An XML document: not this relay's topic.
    Xml,
}

/// Why a record could not be read. Permanent for those bytes.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum DecodeError {
    #[error("the record is not JSON: {0}")]
    NotJson(String),
    #[error("the envelope's bytes field is neither JSON, XML nor base64")]
    UnknownPayload,
    #[error("gzip: {0}")]
    Gzip(String),
    #[error("the document has no valid ts: {0:?}")]
    BadTimestamp(Option<String>),
}

/// Reads one Kafka record value. See the module docs.
pub(crate) fn decode_record(value: &[u8]) -> Result<Decoded, DecodeError> {
    let value = maybe_gunzip(value)?;
    let trimmed = trim_ascii(&value);
    if trimmed.first() == Some(&b'<') {
        return Ok(Decoded::Xml);
    }
    let root: Value =
        serde_json::from_slice(trimmed).map_err(|err| DecodeError::NotJson(err.to_string()))?;
    let Some(bytes) = root.get("bytes") else {
        // A bare document (as a test or a replay tool sends it).
        return parse_document(&root);
    };
    let Some(payload) = bytes.as_str() else {
        return Err(DecodeError::UnknownPayload);
    };
    decode_payload(payload.trim())
}

fn decode_payload(payload: &str) -> Result<Decoded, DecodeError> {
    if payload.starts_with('{') {
        let doc: Value =
            serde_json::from_str(payload).map_err(|err| DecodeError::NotJson(err.to_string()))?;
        return parse_document(&doc);
    }
    if payload.starts_with('<') {
        return Ok(Decoded::Xml);
    }
    let raw = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|_| DecodeError::UnknownPayload)?;
    let raw = maybe_gunzip(&raw)?;
    let text = std::str::from_utf8(trim_ascii(&raw)).map_err(|_| DecodeError::UnknownPayload)?;
    if text.starts_with('<') {
        return Ok(Decoded::Xml);
    }
    if text.starts_with('{') {
        return decode_payload(text);
    }
    Err(DecodeError::UnknownPayload)
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(start, |at| at + 1);
    &bytes[start..end]
}

/// Gunzips `bytes` when they start with the gzip magic number, up to
/// [`MAX_DECODED_BYTES`].
fn maybe_gunzip(bytes: &[u8]) -> Result<std::borrow::Cow<'_, [u8]>, DecodeError> {
    if !bytes.starts_with(&[0x1f, 0x8b]) {
        return Ok(std::borrow::Cow::Borrowed(bytes));
    }
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .take(MAX_DECODED_BYTES + 1)
        .read_to_end(&mut out)
        .map_err(|err| DecodeError::Gzip(err.to_string()))?;
    if out.len() as u64 > MAX_DECODED_BYTES {
        return Err(DecodeError::Gzip(format!(
            "decodes to more than {MAX_DECODED_BYTES} bytes"
        )));
    }
    Ok(std::borrow::Cow::Owned(out))
}

/// One element or an array of them, as a slice-like iterator; nothing for
/// an absent key or `null`.
pub(crate) fn many(value: Option<&Value>) -> impl Iterator<Item = &Value> {
    let items: &[Value] = match value {
        Some(Value::Array(items)) => items,
        Some(Value::Null) | None => &[],
        Some(one) => std::slice::from_ref(one),
    };
    items.iter()
}

/// A boolean attribute: `true`, or the string `"true"`.
pub(crate) fn flag(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

/// [`flag`] when present, `None` when absent.
fn optional_flag(value: Option<&Value>) -> Option<bool> {
    value.map(|v| flag(Some(v)))
}

/// A string attribute (a number is rendered as text); `None` when absent
/// or empty.
pub(crate) fn attr(value: Option<&Value>) -> Option<String> {
    let s = match value? {
        Value::String(s) => s.trim().to_owned(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => return None,
    };
    (!s.is_empty()).then_some(s)
}

/// An element's text: a bare string, or the `""` key of an object.
pub(crate) fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Object(map) => attr(map.get("")),
        other => attr(Some(other)),
    }
}

fn parse_ts(doc: &Value) -> Result<DateTime<Utc>, DecodeError> {
    let raw = attr(doc.get("ts"));
    raw.as_deref()
        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        .map(|ts| ts.with_timezone(&Utc))
        .ok_or(DecodeError::BadTimestamp(raw))
}

fn parse_date(value: Option<&Value>) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(&attr(value)?, "%Y-%m-%d").ok()
}

/// A Darwin time, `HH:MM` or `HH:MM:SS`.
pub(crate) fn parse_time(value: Option<&Value>) -> Option<NaiveTime> {
    let raw = attr(value)?;
    NaiveTime::parse_from_str(&raw, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(&raw, "%H:%M"))
        .ok()
}

/// A RID starts with its scheduled start date, `YYYYMMDD`.
fn ssd_of_rid(rid: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(rid.get(..8)?, "%Y%m%d").ok()
}

/// Reads a `Pport` document. See the module docs.
pub(crate) fn parse_document(doc: &Value) -> Result<Decoded, DecodeError> {
    let updates: Vec<&Value> = ["uR", "sR"]
        .iter()
        .filter_map(|key| doc.get(*key))
        .collect();
    if updates.is_empty() {
        return Ok(Decoded::Heartbeat);
    }
    let ts = parse_ts(doc)?;
    let mut batch = Batch::default();
    for update in updates {
        for schedule in many(update.get("schedule")) {
            parse_schedule(schedule, ts, &mut batch);
        }
        for status in many(update.get("TS")) {
            parse_status(status, ts, &mut batch);
        }
        for association in many(update.get("association")) {
            if let Some(row) = parse_association(association, ts) {
                batch.associations.push(row);
            }
        }
        for deactivated in many(update.get("deactivated")) {
            if let Some(rid) = attr(deactivated.get("rid")) {
                batch.deactivated.push(Deactivated { rid, ts });
            }
        }
    }
    // One document can name a key twice (a snapshot fragment, or a
    // schedule with a TS in the same update): merge it like a batch.
    let mut merged = Batch::default();
    merged.merge(batch);
    Ok(Decoded::Document(merged))
}

/// The service identity every row of a service carries; `None` (and the
/// element skipped) without a RID, UID or valid SSD.
fn identity(element: &Value) -> Option<(String, String, NaiveDate)> {
    Some((
        attr(element.get("rid"))?,
        attr(element.get("uid"))?,
        parse_date(element.get("ssd"))?,
    ))
}

fn call_key(rid: &str, location: &Value) -> Option<CallKey> {
    Some(CallKey {
        rid: rid.to_owned(),
        tiploc: attr(location.get("tpl"))?.to_ascii_uppercase(),
        wta: attr(location.get("wta")).unwrap_or_default(),
        wtd: attr(location.get("wtd")).unwrap_or_default(),
        wtp: attr(location.get("wtp")).unwrap_or_default(),
    })
}

fn parse_schedule(schedule: &Value, ts: DateTime<Utc>, batch: &mut Batch) {
    let Some((rid, uid, ssd)) = identity(schedule) else {
        return;
    };
    batch.services.push(Service {
        rid: rid.clone(),
        uid,
        ssd,
        schedule: Some(ScheduleFields {
            toc: attr(schedule.get("toc")),
            train_id: attr(schedule.get("trainId")),
            rsid: attr(schedule.get("rsid")),
            // The XSD's default is true.
            is_passenger: Some(optional_flag(schedule.get("isPassengerSvc")).unwrap_or(true)),
            deleted: flag(schedule.get("deleted")),
            cancel_reason: text(schedule.get("cancelReason")),
        }),
        ts,
    });
    for kind in SCHEDULE_LOCATIONS {
        for location in many(schedule.get(kind)) {
            let Some(key) = call_key(&rid, location) else {
                continue;
            };
            batch.schedule_calls.push(ScheduleCall {
                key,
                ssd,
                pta: attr(location.get("pta")),
                ptd: attr(location.get("ptd")),
                planned_platform: attr(location.get("plat")),
                cancelled: flag(location.get("can")),
                ts,
            });
        }
    }
}

fn parse_event(value: Option<&Value>) -> Option<Event> {
    let value = value?;
    Some(Event {
        et: attr(value.get("et")),
        at: attr(value.get("at")),
        delayed: flag(value.get("delayed")),
    })
}

/// A TS location's `plat`: a bare string, or the number under `""` with
/// the flags as attributes. `None` without a number.
fn parse_platform(value: Option<&Value>) -> Option<Platform> {
    let value = value?;
    let number = text(Some(value))?;
    Some(Platform {
        number,
        confirmed: flag(value.get("conf")),
        suppressed: flag(value.get("platsup")),
        cis_suppressed: flag(value.get("cisPlatsup")),
        source: attr(value.get("platsrc"))
            .and_then(|s| s.chars().next())
            .filter(|c| matches!(c, 'P' | 'A' | 'M')),
    })
}

fn parse_status(status: &Value, ts: DateTime<Utc>, batch: &mut Batch) {
    let Some((rid, uid, ssd)) = identity(status) else {
        return;
    };
    batch.services.push(Service {
        rid: rid.clone(),
        uid,
        ssd,
        schedule: None,
        ts,
    });
    for location in many(status.get("Location")) {
        let Some(key) = call_key(&rid, location) else {
            continue;
        };
        batch.status_calls.push(StatusCall {
            key,
            ssd,
            pta: attr(location.get("pta")),
            ptd: attr(location.get("ptd")),
            platform: parse_platform(location.get("plat")),
            arrival: parse_event(location.get("arr")),
            departure: parse_event(location.get("dep")),
            pass: parse_event(location.get("pass")),
            // `suppr` is an element with text `true`/`false`.
            suppressed: text(location.get("suppr")).is_some_and(|s| s == "true"),
            ts,
        });
    }
}

fn parse_association(association: &Value, ts: DateTime<Utc>) -> Option<Association> {
    let category = attr(association.get("category"))?.to_ascii_uppercase();
    if !matches!(category.as_str(), "NP" | "JJ" | "VV" | "LK") {
        return None;
    }
    let main = association.get("main")?;
    let assoc = association.get("assoc")?;
    let main_rid = attr(main.get("rid"))?;
    Some(Association {
        main_ssd: ssd_of_rid(&main_rid),
        main_rid,
        assoc_rid: attr(assoc.get("rid"))?,
        tiploc: attr(association.get("tiploc"))?.to_ascii_uppercase(),
        category,
        cancelled: flag(association.get("isCancelled")),
        deleted: flag(association.get("isDeleted")),
        main_wta: parse_time(main.get("wta")),
        main_wtd: parse_time(main.get("wtd")),
        main_wtp: parse_time(main.get("wtp")),
        assoc_wta: parse_time(assoc.get("wta")),
        assoc_wtd: parse_time(assoc.get("wtd")),
        assoc_wtp: parse_time(assoc.get("wtp")),
        ts,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    /// Fixtures written from the Push Port v18 schema (no captured traffic):
    /// synthetic RIDs and UIDs, in the JSON topic's rendering.
    fn fixture(name: &str) -> String {
        std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures")
                .join(name),
        )
        .expect("fixture")
    }

    fn document(record: &str) -> Batch {
        match decode_record(record.as_bytes()).expect("decodes") {
            Decoded::Document(batch) => batch,
            other => panic!("expected a document, got {other:?}"),
        }
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid")
            .with_timezone(&Utc)
    }

    #[test]
    fn a_train_status_gives_platforms_with_their_flags_and_forecasts() {
        let batch = document(&fixture("ts.json"));
        assert_eq!(batch.services.len(), 1);
        let service = &batch.services[0];
        assert_eq!(service.rid, "209901018799001");
        assert_eq!(service.uid, "Z99001");
        assert_eq!(
            service.ssd,
            NaiveDate::from_ymd_opt(2099, 1, 1).expect("date")
        );
        assert!(service.schedule.is_none());
        assert_eq!(service.ts, ts("2099-01-01T09:01:00.1+00:00"));

        assert_eq!(batch.status_calls.len(), 3);
        let origin = batch
            .status_calls
            .iter()
            .find(|c| c.key.tiploc == "LEEDS")
            .expect("origin");
        assert_eq!(origin.key.wtd, "09:00");
        assert_eq!(origin.key.wta, "");
        let platform = origin.platform.as_ref().expect("platform");
        assert_eq!(platform.number, "12");
        assert!(platform.confirmed);
        assert!(!platform.suppressed);
        assert_eq!(platform.source, Some('A'));
        assert_eq!(
            origin.departure.as_ref().and_then(|e| e.et.as_deref()),
            Some("09:03")
        );

        let hidden = batch
            .status_calls
            .iter()
            .find(|c| c.key.tiploc == "YORK")
            .expect("YORK");
        let platform = hidden.platform.as_ref().expect("platform");
        assert_eq!(platform.number, "5");
        assert!(platform.suppressed && platform.cis_suppressed);
        assert!(!platform.confirmed);
        assert!(hidden.arrival.as_ref().is_some_and(|e| e.delayed));

        // A bare-string platform, and a pass with an actual.
        let pass = batch
            .status_calls
            .iter()
            .find(|c| c.key.tiploc == "COLTONJ")
            .expect("pass");
        assert_eq!(pass.key.wtp, "09:20:30");
        assert_eq!(
            pass.platform.as_ref().map(|p| p.number.as_str()),
            Some("UM")
        );
        assert_eq!(
            pass.pass.as_ref().and_then(|e| e.at.as_deref()),
            Some("09:21")
        );
        assert!(pass.suppressed);
    }

    #[test]
    fn a_schedule_gives_the_service_and_its_planned_platforms_and_cancellations() {
        let batch = document(&fixture("schedule.json"));
        let service = &batch.services[0];
        let fields = service.schedule.as_ref().expect("schedule fields");
        assert_eq!(fields.toc.as_deref(), Some("NT"));
        assert_eq!(fields.train_id.as_deref(), Some("2Z01"));
        assert_eq!(fields.rsid.as_deref(), Some("NT999900"));
        assert_eq!(fields.is_passenger, Some(true));
        assert_eq!(fields.cancel_reason.as_deref(), Some("104"));
        assert!(!fields.deleted);

        assert_eq!(batch.schedule_calls.len(), 4);
        let york = batch
            .schedule_calls
            .iter()
            .find(|c| c.key.tiploc == "YORK")
            .expect("YORK");
        assert_eq!(york.planned_platform.as_deref(), Some("3"));
        assert!(york.cancelled);
        assert_eq!(york.pta.as_deref(), Some("09:30"));
        assert_eq!(york.key.wta, "09:29:30");
        let origin = batch
            .schedule_calls
            .iter()
            .find(|c| c.key.tiploc == "LEEDS")
            .expect("origin");
        assert!(!origin.cancelled);
    }

    #[test]
    fn associations_and_deactivations_are_kept() {
        let batch = document(&fixture("association.json"));
        assert_eq!(
            batch.associations.len(),
            2,
            "the unknown category is dropped"
        );
        let np = batch
            .associations
            .iter()
            .find(|a| a.category == "NP")
            .expect("NP");
        assert_eq!(np.main_rid, "209901018799001");
        assert_eq!(np.assoc_rid, "209901018799002");
        assert_eq!(np.tiploc, "YORK");
        assert_eq!(np.main_wta, NaiveTime::from_hms_opt(9, 30, 0));
        assert_eq!(np.assoc_wtd, NaiveTime::from_hms_opt(9, 42, 30));
        assert_eq!(np.main_ssd, NaiveDate::from_ymd_opt(2099, 1, 1));
        assert!(!np.cancelled);
        let vv = batch
            .associations
            .iter()
            .find(|a| a.category == "VV")
            .expect("VV");
        assert!(vv.cancelled);
        assert_eq!(batch.deactivated.len(), 1);
        assert_eq!(batch.deactivated[0].rid, "209901018799003");
    }

    #[test]
    fn a_heartbeat_is_recognised() {
        assert_eq!(
            decode_record(fixture("heartbeat.json").as_bytes()),
            Ok(Decoded::Heartbeat)
        );
    }

    #[test]
    fn a_gzipped_record_and_a_base64_gzipped_document_decode_the_same() {
        let record = fixture("ts.json");
        let plain = document(&record);

        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(record.as_bytes()).expect("gzip");
        let gzipped = gz.finish().expect("gzip");
        assert_eq!(
            decode_record(&gzipped),
            Ok(Decoded::Document(plain.clone()))
        );

        let root: Value = serde_json::from_str(&record).expect("json");
        let inner = root["bytes"].as_str().expect("bytes");
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(inner.as_bytes()).expect("gzip");
        let encoded = base64::engine::general_purpose::STANDARD.encode(gz.finish().expect("gzip"));
        let wrapped = serde_json::json!({ "bytes": encoded }).to_string();
        assert_eq!(
            decode_record(wrapped.as_bytes()),
            Ok(Decoded::Document(plain))
        );
    }

    #[test]
    fn xml_is_recognised_and_skipped() {
        assert_eq!(decode_record(b"<Pport/>"), Ok(Decoded::Xml));
        let wrapped = serde_json::json!({ "bytes": "<Pport ts=\"x\"/>" }).to_string();
        assert_eq!(decode_record(wrapped.as_bytes()), Ok(Decoded::Xml));
        let b64 = base64::engine::general_purpose::STANDARD.encode("<Pport/>");
        let wrapped = serde_json::json!({ "bytes": b64 }).to_string();
        assert_eq!(decode_record(wrapped.as_bytes()), Ok(Decoded::Xml));
    }

    #[test]
    fn malformed_records_are_errors() {
        assert!(matches!(
            decode_record(b"not json"),
            Err(DecodeError::NotJson(_))
        ));
        let wrapped = serde_json::json!({ "bytes": "!!not base64!!" }).to_string();
        assert_eq!(
            decode_record(wrapped.as_bytes()),
            Err(DecodeError::UnknownPayload)
        );
        let no_ts = serde_json::json!({ "uR": { "TS": [] } }).to_string();
        assert_eq!(
            decode_record(no_ts.as_bytes()),
            Err(DecodeError::BadTimestamp(None))
        );
    }

    #[test]
    fn an_element_without_identity_is_skipped_not_fatal() {
        let doc = serde_json::json!({
            "ts": "2099-01-01T09:00:00Z",
            "uR": {
                "TS": [{ "rid": "209901018799001" }, {
                    "rid": "209901018799002", "uid": "Z99002", "ssd": "2099-01-01",
                    "Location": { "wtd": "09:00", "plat": "1" }
                }]
            }
        });
        let Decoded::Document(batch) = parse_document(&doc).expect("decodes") else {
            panic!("a document");
        };
        assert_eq!(batch.services.len(), 1);
        assert!(
            batch.status_calls.is_empty(),
            "a location with no tpl is skipped"
        );
    }

    #[test]
    fn the_shape_helpers_accept_both_renderings() {
        let one = serde_json::json!({ "a": 1 });
        assert_eq!(many(Some(&one)).count(), 1);
        assert_eq!(many(Some(&serde_json::json!([1, 2]))).count(), 2);
        assert_eq!(many(None).count(), 0);
        assert!(flag(Some(&serde_json::json!("true"))));
        assert!(flag(Some(&serde_json::json!(true))));
        assert!(!flag(Some(&serde_json::json!("false"))));
        assert_eq!(
            text(Some(&serde_json::json!({ "": "4", "conf": "true" }))).as_deref(),
            Some("4")
        );
        assert_eq!(text(Some(&serde_json::json!("4"))).as_deref(), Some("4"));
        assert_eq!(text(Some(&serde_json::json!({ "conf": "true" }))), None);
        assert_eq!(
            parse_time(Some(&serde_json::json!("23:59:30"))),
            NaiveTime::from_hms_opt(23, 59, 30)
        );
        assert_eq!(
            ssd_of_rid("209912318700001"),
            NaiveDate::from_ymd_opt(2099, 12, 31)
        );
        assert_eq!(ssd_of_rid("x"), None);
    }
}
