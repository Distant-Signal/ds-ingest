//! movement-relay's Kafka-side metrics, all `distant_signal_`-prefixed
//! (`ds_runtime::metrics::metric_name`). Together they let an alert tell
//! "connected but not publishing" apart from "quiet feed" and "not
//! connected":
//!
//! - [`ASSIGNED_PARTITIONS`]: how many TRUST partitions this consumer owns
//!   right now (0 before the first assignment, after a revoke and while the
//!   group rebalances);
//! - [`CONSUMER_LAG`]: per partition, how far the group's committed offset
//!   is behind the partition's high watermark (librdkafka's statistics,
//!   every [`STATISTICS_INTERVAL_MS`]), modelled on darwin-relay's
//!   `darwin_relay_consumer_lag`;
//! - [`RECORDS_COMMITTED`]: Kafka records whose every envelope reached
//!   `movement-events` and whose offset was then committed. Registered at 0,
//!   so `increase()` works from the first scrape. The per-type
//!   `movement_relay_events_published_total{msg_type}` (one per `XADD`)
//!   stays as it was.

#![expect(
    clippy::cast_precision_loss,
    reason = "gauges take f64; partition counts and offsets stay far below 2^52"
)]

use ds_runtime::metrics::metric_name;

/// `movement_relay_assigned_partitions`: partitions currently assigned.
pub(crate) const ASSIGNED_PARTITIONS: &str = "movement_relay_assigned_partitions";
/// `movement_relay_consumer_lag{partition}`: messages between the group's
/// committed offset and the partition's high watermark.
pub(crate) const CONSUMER_LAG: &str = "movement_relay_consumer_lag";
/// `movement_relay_records_committed_total`: records published in full and
/// committed.
pub(crate) const RECORDS_COMMITTED: &str = "movement_relay_records_committed_total";

/// librdkafka's `statistics.interval.ms`: how often the lag gauge updates.
pub(crate) const STATISTICS_INTERVAL_MS: &str = "30000";

pub(crate) fn set_assigned_partitions(count: usize) {
    metrics::gauge!(metric_name(ASSIGNED_PARTITIONS)).set(count as f64);
}

pub(crate) fn set_consumer_lag(partition: i32, lag: i64) {
    metrics::gauge!(metric_name(CONSUMER_LAG), "partition" => partition.to_string())
        .set(lag as f64);
}

pub(crate) fn records_committed(count: usize) {
    metrics::counter!(metric_name(RECORDS_COMMITTED)).increment(count as u64);
}

/// The counter at 0 and the assignment gauge at 0 (nothing is assigned
/// until the first rebalance), so both series exist from the first scrape.
pub(crate) fn register() {
    metrics::counter!(metric_name(RECORDS_COMMITTED)).increment(0);
    set_assigned_partitions(0);
}

/// The `(partition, lag)` samples worth reporting from one statistics
/// callback: librdkafka's internal unassigned partition (-1) and a lag it
/// does not know yet (-1: no committed offset, or no high watermark) are
/// skipped, as darwin-relay does.
pub(crate) fn lag_samples(partitions: impl IntoIterator<Item = (i32, i64)>) -> Vec<(i32, i64)> {
    partitions
        .into_iter()
        .filter(|&(partition, lag)| partition >= 0 && lag >= 0)
        .collect()
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    /// The deploy repository's alert rules name these; a rename breaks an
    /// alert.
    #[test]
    fn names_match_the_alert_rules() {
        assert_eq!(
            metric_name(ASSIGNED_PARTITIONS),
            "distant_signal_movement_relay_assigned_partitions"
        );
        assert_eq!(
            metric_name(CONSUMER_LAG),
            "distant_signal_movement_relay_consumer_lag"
        );
        assert_eq!(
            metric_name(RECORDS_COMMITTED),
            "distant_signal_movement_relay_records_committed_total"
        );
    }

    #[test]
    fn unknown_lags_and_the_internal_partition_are_not_reported() {
        assert_eq!(
            lag_samples([(-1, 7), (0, -1), (1, 0), (2, 42)]),
            vec![(1, 0), (2, 42)]
        );
    }

    #[test]
    fn the_series_exist_before_anything_is_published() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            register();
            records_committed(2);
            set_consumer_lag(3, 15);
        });
        let rendered = handle.render();
        assert!(
            rendered.contains("distant_signal_movement_relay_records_committed_total 2"),
            "{rendered}"
        );
        assert!(
            rendered.contains("distant_signal_movement_relay_assigned_partitions 0"),
            "{rendered}"
        );
        assert!(
            rendered.contains("distant_signal_movement_relay_consumer_lag{partition=\"3\"} 15"),
            "{rendered}"
        );
    }
}
