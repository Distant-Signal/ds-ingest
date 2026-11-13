//! A low-volume probe for Push Port `LateReason` elements (DS-512).
//!
//! DS keeps a service's cancel reason but not its late reason, and nothing
//! DS stores shows whether Darwin's JSON topic carries `LateReason` at all
//! (the relay's fixtures are written from the schema, not captured
//! traffic). This probe answers that before any column is added. It is
//! off by default (`DARWIN_LATE_REASON_SAMPLING`) and changes nothing the
//! relay publishes.
//!
//! When on, each `TS` element is counted
//! (`darwin_relay_status_elements_total`), each `LateReason` is counted by
//! where it sat (`darwin_relay_late_reasons_total{level}`: `service`, a
//! child of `TS`, or `location`, a child of a `Location`), and the first
//! [`MAX_LOGGED`] are logged, at most one per [`LOG_EVERY`]. A log line
//! has the reason code, the tiploc, the `near` flag and the level: no
//! RID, UID, headcode or timestamp, so a sample cannot be tied to a train.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::metrics;
use crate::pushport::{attr, flag, many};

/// The most samples logged in one process lifetime.
const MAX_LOGGED: u64 = 50;
/// The least time between two logged samples.
const LOG_EVERY: Duration = Duration::from_secs(30);

static ENABLED: AtomicBool = AtomicBool::new(false);
static LOGGED: AtomicU64 = AtomicU64::new(0);
static LAST_LOG: std::sync::Mutex<Option<Instant>> = std::sync::Mutex::new(None);

/// Turns the probe on for this process.
pub(crate) fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

/// One `LateReason` element.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Sample {
    pub level: &'static str,
    /// The element's text: Darwin's numeric reason code. A value that is
    /// not 1 to 4 digits is reported as `"other"` so free text never
    /// reaches a log.
    pub code: String,
    pub tiploc: Option<String>,
    pub near: bool,
}

fn sample(level: &'static str, element: &Value) -> Sample {
    let text = crate::pushport::text(Some(element)).unwrap_or_default();
    let numeric = (1..=4).contains(&text.len()) && text.bytes().all(|b| b.is_ascii_digit());
    let tiploc = match element {
        Value::Object(map) => attr(map.get("tiploc")),
        _ => None,
    }
    .map(|t| t.to_ascii_uppercase())
    .filter(|t| t.len() <= 7 && t.bytes().all(|b| b.is_ascii_alphanumeric()));
    Sample {
        level,
        code: if numeric { text } else { "other".to_owned() },
        tiploc,
        near: element.get("near").is_some_and(|v| flag(Some(v))),
    }
}

/// Every `LateReason` in one `TS` element: its own, and each location's.
pub(crate) fn late_reasons(status: &Value) -> Vec<Sample> {
    let mut found: Vec<Sample> = many(status.get("LateReason"))
        .map(|e| sample("service", e))
        .collect();
    for location in many(status.get("Location")) {
        found.extend(many(location.get("LateReason")).map(|e| sample("location", e)));
    }
    found
}

/// Called for each `TS` element the relay reads. A no-op unless enabled.
pub(crate) fn note_status(status: &Value) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    metrics::status_element();
    for found in late_reasons(status) {
        metrics::late_reason(found.level);
        log(&found);
    }
}

fn log(found: &Sample) {
    if LOGGED.load(Ordering::Relaxed) >= MAX_LOGGED {
        return;
    }
    let Ok(mut last) = LAST_LOG.lock() else {
        return;
    };
    let now = Instant::now();
    if last.is_some_and(|at| now.duration_since(at) < LOG_EVERY) {
        return;
    }
    *last = Some(now);
    LOGGED.fetch_add(1, Ordering::Relaxed);
    tracing::info!(
        level = found.level,
        code = %found.code,
        tiploc = found.tiploc.as_deref().unwrap_or(""),
        near = found.near,
        "sampled a Push Port LateReason"
    );
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_service_level_late_reason_gives_its_code_tiploc_and_near_flag() {
        let status = json!({
            "rid": "1", "uid": "A", "ssd": "2099-01-01",
            "LateReason": {"": "894", "tiploc": "leeds", "near": "true"},
            "Location": {"tpl": "LEEDS"}
        });
        assert_eq!(
            late_reasons(&status),
            vec![Sample {
                level: "service",
                code: "894".into(),
                tiploc: Some("LEEDS".into()),
                near: true
            }]
        );
    }

    #[test]
    fn a_bare_string_and_a_location_level_reason_are_both_found() {
        let status = json!({
            "LateReason": "895",
            "Location": [{"tpl": "A"}, {"tpl": "B", "LateReason": {"": "104", "tiploc": "B"}}]
        });
        let found = late_reasons(&status);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].code, "895");
        assert_eq!(found[0].tiploc, None);
        assert!(!found[0].near);
        assert_eq!(
            (found[1].level, found[1].code.as_str()),
            ("location", "104")
        );
    }

    #[test]
    fn free_text_and_odd_tiplocs_never_reach_a_sample() {
        let status =
            json!({"LateReason": {"": "a train driver called Sam", "tiploc": "not a tiploc!"}});
        let found = late_reasons(&status);
        assert_eq!(found[0].code, "other");
        assert_eq!(found[0].tiploc, None);
    }

    #[test]
    fn a_status_without_one_gives_nothing() {
        assert_eq!(late_reasons(&json!({"Location": {"tpl": "A"}})), Vec::new());
    }

    #[test]
    fn the_fixture_status_has_a_service_level_reason() {
        let raw = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ts.json"),
        )
        .expect("fixture");
        let envelope: Value = serde_json::from_str(&raw).expect("json");
        let doc: Value =
            serde_json::from_str(envelope["bytes"].as_str().expect("bytes")).expect("doc");
        let found = late_reasons(&doc["uR"]["TS"]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].code, "104");
        assert_eq!(found[0].tiploc.as_deref(), Some("LEEDS"));
    }
}
