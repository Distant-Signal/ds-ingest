//! darwin-relay's metrics, all `distant_signal_`-prefixed
//! (`common::metrics::metric_name`). The chart's alerts read
//! [`LAST_MESSAGE`] (staleness) and [`CONSUMER_LAG`] (lag).

#![expect(
    clippy::cast_precision_loss,
    reason = "gauges take f64; these counts and Unix times stay far below 2^52"
)]

use chrono::{DateTime, Utc};
use common::metrics::metric_name;

/// `darwin_relay_ready`: 1 while Kafka partitions are assigned.
pub(crate) const READY: &str = "darwin_relay_ready";
/// `darwin_relay_records_total{outcome}`: Kafka records read, by what they
/// held (`document`, `heartbeat`, `xml`, `malformed`).
pub(crate) const RECORDS: &str = "darwin_relay_records_total";
/// `darwin_relay_rows_total{kind}`: rows produced, by kind.
pub(crate) const ROWS: &str = "darwin_relay_rows_total";
/// `darwin_relay_batches_published_total`.
pub(crate) const BATCHES: &str = "darwin_relay_batches_published_total";
/// `darwin_relay_last_message_timestamp_seconds`: the newest Darwin `ts`
/// relayed (or of a heartbeat). Its age is how stale DS's Push Port data is.
pub(crate) const LAST_MESSAGE: &str = "darwin_relay_last_message_timestamp_seconds";
/// `darwin_relay_consumer_lag{partition}`: messages behind the partition's
/// high watermark, from librdkafka's statistics every 30 s.
pub(crate) const CONSUMER_LAG: &str = "darwin_relay_consumer_lag";
/// `darwin_relay_errors_total{operation}`.
pub(crate) const ERRORS: &str = "darwin_relay_errors_total";

pub(crate) fn record(outcome: &'static str) {
    metrics::counter!(metric_name(RECORDS), "outcome" => outcome).increment(1);
}

pub(crate) fn rows(kind: &'static str, n: usize) {
    metrics::counter!(metric_name(ROWS), "kind" => kind).increment(n as u64);
}

pub(crate) fn batch_published() {
    metrics::counter!(metric_name(BATCHES)).increment(1);
}

pub(crate) fn error(operation: &'static str) {
    metrics::counter!(metric_name(ERRORS), "operation" => operation).increment(1);
}

pub(crate) fn last_message(ts: DateTime<Utc>) {
    metrics::gauge!(metric_name(LAST_MESSAGE)).set(ts.timestamp_millis() as f64 / 1000.0);
}

pub(crate) fn set_consumer_lag(partition: i32, lag: i64) {
    metrics::gauge!(metric_name(CONSUMER_LAG), "partition" => partition.to_string())
        .set(lag as f64);
}

/// Every counter at 0, so `increase()` sees the first one.
pub(crate) fn register() {
    for outcome in ["document", "heartbeat", "xml", "malformed"] {
        metrics::counter!(metric_name(RECORDS), "outcome" => outcome).increment(0);
    }
    metrics::counter!(metric_name(BATCHES)).increment(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chart's `PrometheusRule` names these; a rename breaks an alert.
    #[test]
    fn names_match_the_chart_alert_rules() {
        assert_eq!(
            metric_name(LAST_MESSAGE),
            "distant_signal_darwin_relay_last_message_timestamp_seconds"
        );
        assert_eq!(
            metric_name(CONSUMER_LAG),
            "distant_signal_darwin_relay_consumer_lag"
        );
        assert_eq!(metric_name(READY), "distant_signal_darwin_relay_ready");
    }
}
