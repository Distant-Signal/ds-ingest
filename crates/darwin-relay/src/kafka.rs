//! The Kafka side: a librdkafka consumer on the Push Port JSON topic, in
//! the same shape as movement-relay's (SASL, the RDM-issued group, manual
//! offset store and commit, readiness from the partition assignment), plus
//! a consumer-lag gauge from librdkafka's statistics.
//!
//! Offsets are committed only after the batch holding their records is in
//! Redis ([`crate::relay`]), and then only once that write is
//! `--kafka-commit-delay-secs` old, so durable in the AOF
//! (`durable_offsets`). A crash or restart re-reads the records of the
//! batch in flight and of the batches still held; their rows are
//! idempotent (Darwin timestamp guards in the writer).

use std::time::Duration;

use async_trait::async_trait;
use durable_offsets::DurableOffsets;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, ClientContext, Offset, Statistics, TopicPartitionList};

use crate::config::Config;
use crate::metrics;

/// One record, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub payload: Vec<u8>,
}

/// What [`crate::relay`] needs of a consumer; a fake in its tests.
#[async_trait]
pub(crate) trait RecordSource: Send {
    /// The next record; `None` if none arrives within `timeout`.
    async fn recv(&mut self, timeout: Option<Duration>) -> anyhow::Result<Option<Record>>;
    /// Marks `offsets` as processed: each is stored (the commit is
    /// `offset + 1`) and committed once the batch's write is durable. A
    /// partition revoked in the meantime is skipped: its new owner re-reads
    /// from the last commit.
    fn commit(&mut self, offsets: &[(String, i32, i64)]) -> anyhow::Result<()>;
}

/// Readiness from the partition assignment (movement-relay's rule), the
/// lag gauge from librdkafka's statistics, and offset-reset detection
/// ([`metrics::OFFSET_RESETS`]): after each assignment, the partitions with
/// no committed offset; from librdkafka's log, a reset after an error.
pub(crate) struct RelayContext {
    pub ready: health_http::ConnectionState,
    /// `auto.offset.reset`, for the reset log lines.
    pub auto_offset_reset: String,
}

/// How long the committed-offset lookup after an assignment may block the
/// poll it runs in (once per rebalance).
const COMMITTED_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether a librdkafka log line is a partition's position being reset
/// after an error (`rd_kafka_offset_reset`, facility `OFFSET`, logged at
/// warning level; a reset for "no committed offset" is only debug-logged,
/// which is why assignments are checked as well).
fn is_offset_reset_log(fac: &str, message: &str) -> bool {
    fac == "OFFSET" && message.contains(": offset reset (")
}

/// The partitions in `committed` (an `OffsetFetch` answer) that have no
/// committed offset, so `auto.offset.reset` decides where they start.
fn partitions_without_committed_offset(committed: &TopicPartitionList) -> Vec<i32> {
    committed
        .elements()
        .iter()
        .filter(|elem| elem.offset() == Offset::Invalid)
        .map(rdkafka::topic_partition_list::TopicPartitionListElem::partition)
        .collect()
}

impl RelayContext {
    /// Counts and logs the newly assigned partitions that have no committed
    /// offset. A failed lookup is logged and counted as an error, not as a
    /// reset.
    fn check_committed_offsets(
        &self,
        consumer: &BaseConsumer<Self>,
        assigned: &TopicPartitionList,
    ) {
        match consumer.committed_offsets(assigned.clone(), COMMITTED_LOOKUP_TIMEOUT) {
            Ok(committed) => {
                let missing = partitions_without_committed_offset(&committed);
                if !missing.is_empty() {
                    tracing::error!(
                        partitions = ?missing,
                        auto_offset_reset = %self.auto_offset_reset,
                        "darwin-relay: assigned Kafka partitions have no committed offset for the \
                         group; they start where auto.offset.reset says (with latest, everything \
                         before now is skipped)"
                    );
                    metrics::offset_resets(metrics::RESET_NO_COMMITTED_OFFSET, missing.len());
                }
            }
            Err(err) => {
                tracing::warn!(error = ?err, "darwin-relay: could not read the committed offsets of the assigned partitions");
                metrics::error("committed_offsets");
            }
        }
    }
}

impl ClientContext for RelayContext {
    fn stats(&self, statistics: Statistics) {
        for topic in statistics.topics.values() {
            for (partition, stats) in &topic.partitions {
                // -1 is librdkafka's internal UA partition; a lag of -1 is
                // "not known yet".
                if *partition >= 0 && stats.consumer_lag >= 0 {
                    metrics::set_consumer_lag(*partition, stats.consumer_lag);
                }
            }
        }
    }

    /// librdkafka's log, to tracing (target `librdkafka`, as rust-rdkafka's
    /// default does through `log`), counting offset resets on the way.
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if is_offset_reset_log(fac, log_message) {
            metrics::offset_resets(metrics::RESET_AFTER_ERROR, 1);
            tracing::error!(
                target: "librdkafka",
                auto_offset_reset = %self.auto_offset_reset,
                "librdkafka: {fac} {log_message}"
            );
            return;
        }
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                tracing::error!(target: "librdkafka", "librdkafka: {fac} {log_message}");
            }
            RDKafkaLogLevel::Warning => {
                tracing::warn!(target: "librdkafka", "librdkafka: {fac} {log_message}");
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                tracing::info!(target: "librdkafka", "librdkafka: {fac} {log_message}");
            }
            RDKafkaLogLevel::Debug => {
                tracing::debug!(target: "librdkafka", "librdkafka: {fac} {log_message}");
            }
        }
    }
}

impl ConsumerContext for RelayContext {
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) if !partitions.elements().is_empty() => {
                health_http::set_connected(&self.ready, metrics::READY, true);
                tracing::info!(
                    partitions = partitions.elements().len(),
                    "darwin-relay: Kafka partitions assigned; ready"
                );
                self.check_committed_offsets(consumer, partitions);
            }
            Rebalance::Revoke(_) => {
                health_http::set_connected(&self.ready, metrics::READY, false);
                tracing::warn!("darwin-relay: Kafka partitions revoked; not ready");
            }
            Rebalance::Error(err) => {
                health_http::set_connected(&self.ready, metrics::READY, false);
                tracing::error!(error = ?err, "darwin-relay: Kafka rebalance error; not ready");
            }
            Rebalance::Assign(_) => {}
        }
    }
}

/// librdkafka's `max.poll.interval.ms`: a batch waits for its Redis write
/// before the next poll, and the producer retries a Redis outage for as
/// long as it lasts. Past this the group reassigns the partitions and the
/// records are re-read from the last commit, which is safe.
const MAX_POLL_INTERVAL_MS: &str = "900000";
/// How often librdkafka reports statistics (the lag gauge).
const STATISTICS_INTERVAL_MS: &str = "30000";

/// The client settings, split out so they are testable without a broker.
pub(crate) fn client_config(config: &Config) -> anyhow::Result<ClientConfig> {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", &config.kafka_consumer_group)
        .set("security.protocol", &config.kafka_security_protocol)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("max.poll.interval.ms", MAX_POLL_INTERVAL_MS)
        .set("statistics.interval.ms", STATISTICS_INTERVAL_MS)
        .set("auto.offset.reset", &config.kafka_auto_offset_reset);
    if config
        .kafka_security_protocol
        .to_ascii_uppercase()
        .starts_with("SASL")
    {
        let (username, password) = config.sasl_credentials().ok_or_else(|| {
            anyhow::anyhow!(
                "KAFKA_SECURITY_PROTOCOL is {} but no SASL user name and password are set \
                 (KAFKA_SASL_USERNAME/KAFKA_SASL_PASSWORD, or KAFKA_SASL_JAAS_CONFIG)",
                config.kafka_security_protocol
            )
        })?;
        client
            .set("sasl.mechanisms", &config.kafka_sasl_mechanism)
            .set("sasl.username", username)
            .set("sasl.password", password.expose());
    }
    Ok(client)
}

pub(crate) struct KafkaSource {
    consumer: StreamConsumer<RelayContext>,
    progress: health_http::Progress,
    /// Offsets of published batches, held until the writes are durable.
    durable: DurableOffsets,
}

/// How long the wait for a batch's first record backs off after a failed
/// store or commit of held offsets.
const FLUSH_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// The longest [`KafkaSource::finish`] waits for held offsets to become
/// durable.
const FINISH_WAIT_MAX: Duration = Duration::from_secs(15);

/// `DurableOffsets::flush`'s view of the consumer: a partition no longer
/// assigned is skipped.
struct StoreOn<'a>(&'a StreamConsumer<RelayContext>);

impl durable_offsets::OffsetSink for StoreOn<'_> {
    type Error = anyhow::Error;

    fn store(&self, (topic, partition, offset): &durable_offsets::Offset) -> anyhow::Result<bool> {
        let assigned = self
            .0
            .assignment()?
            .elements()
            .iter()
            .any(|e| e.topic() == topic && e.partition() == *partition);
        if !assigned {
            return Ok(false);
        }
        self.0.store_offset(topic, *partition, *offset)?;
        Ok(true)
    }

    fn commit(&self) -> anyhow::Result<()> {
        Ok(self
            .0
            .commit_consumer_state(rdkafka::consumer::CommitMode::Async)?)
    }
}

impl KafkaSource {
    pub(crate) fn connect(
        config: &Config,
        ready: health_http::ConnectionState,
        progress: health_http::Progress,
    ) -> anyhow::Result<Self> {
        let consumer: StreamConsumer<RelayContext> =
            client_config(config)?.create_with_context(RelayContext {
                ready,
                auto_offset_reset: config.kafka_auto_offset_reset.clone(),
            })?;
        consumer.subscribe(&[&config.kafka_topic])?;
        Ok(Self {
            consumer,
            progress,
            durable: DurableOffsets::new(Duration::from_secs(config.kafka_commit_delay_secs)),
        })
    }

    /// Stores and commits the held offsets durable at `now`.
    fn flush(&mut self, now: tokio::time::Instant) -> anyhow::Result<()> {
        let flushed = self.durable.flush(&StoreOn(&self.consumer), now)?;
        for (topic, partition, offset) in flushed.revoked {
            tracing::warn!(
                topic,
                partition,
                offset,
                "Kafka partition revoked before its offset was stored; its new owner re-reads it"
            );
        }
        Ok(())
    }

    /// The first record of a batch, however long the topic is quiet,
    /// committing held offsets as they become durable meanwhile.
    async fn recv_idle(
        &mut self,
    ) -> Result<rdkafka::message::OwnedMessage, rdkafka::error::KafkaError> {
        let mut backoff_until = None;
        loop {
            let wake_at = match (self.durable.next_due(), backoff_until) {
                (None, _) => {
                    return self
                        .progress
                        .idle(self.consumer.recv())
                        .await
                        .map(|m| m.detach());
                }
                (Some(due), None) => due,
                (Some(due), Some(until)) => due.max(until),
            };
            let received = tokio::select! {
                biased;
                message = self.progress.idle(self.consumer.recv()) => Some(message.map(|m| m.detach())),
                () = tokio::time::sleep_until(wake_at) => None,
            };
            if let Some(message) = received {
                return message;
            }
            backoff_until = match self.flush(tokio::time::Instant::now()) {
                Ok(()) => None,
                Err(err) => {
                    tracing::error!(error = ?err, "failed to store or commit durable Kafka offsets; retrying");
                    metrics::error("commit");
                    Some(tokio::time::Instant::now() + FLUSH_RETRY_BACKOFF)
                }
            };
        }
    }

    /// Shutdown: waits (bounded) for the held offsets to become durable,
    /// then stores them and commits synchronously, so a clean restart does
    /// not re-read them. Offsets not durable in time stay uncommitted and
    /// are re-read (duplicates the writer absorbs).
    pub(crate) async fn finish(&mut self) {
        if let Some(last_due) = self.durable.last_due() {
            let wait = last_due.saturating_duration_since(tokio::time::Instant::now());
            tokio::time::sleep(wait.min(FINISH_WAIT_MAX)).await;
        }
        if let Err(err) = self.flush(tokio::time::Instant::now()) {
            tracing::error!(error = ?err, "failed to store the durable Kafka offsets at shutdown; their records are re-read");
        }
        match self
            .consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
        {
            Ok(())
            | Err(rdkafka::error::KafkaError::ConsumerCommit(
                rdkafka::types::RDKafkaErrorCode::NoOffset,
            )) => {}
            Err(err) => {
                tracing::error!(error = ?err, "final Kafka offset commit failed; the records since the last commit are re-read");
            }
        }
    }
}

#[async_trait]
impl RecordSource for KafkaSource {
    async fn recv(&mut self, timeout: Option<Duration>) -> anyhow::Result<Option<Record>> {
        let received = match timeout {
            // Waiting for the first record of a batch is idle time, not a
            // stall: a quiet topic must not get the relay restarted.
            None => Some(self.recv_idle().await),
            Some(timeout) => tokio::time::timeout(timeout, self.consumer.recv())
                .await
                .ok()
                .map(|message| message.map(|m| m.detach())),
        };
        let Some(message) = received else {
            return Ok(None);
        };
        let message = message?;
        Ok(Some(Record {
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset: message.offset(),
            payload: message.payload().map(<[u8]>::to_vec).unwrap_or_default(),
        }))
    }

    fn commit(&mut self, offsets: &[(String, i32, i64)]) -> anyhow::Result<()> {
        let now = tokio::time::Instant::now();
        self.durable.written(offsets.to_vec(), now);
        self.flush(now)
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn config(extra: &[&str]) -> Config {
        let mut args = vec![
            "darwin-relay",
            "--kafka-brokers",
            "kafka.example:9093",
            "--kafka-topic",
            "example-topic",
            "--kafka-consumer-group",
            "example-group",
        ];
        args.extend_from_slice(extra);
        Config::try_parse_from(args).expect("parses")
    }

    #[test]
    fn offsets_are_committed_by_hand_and_sasl_is_set_from_the_secret() {
        let client = client_config(&config(&[
            "--kafka-sasl-username",
            "user",
            "--kafka-sasl-password",
            "fake-password",
        ]))
        .expect("config");
        assert_eq!(client.get("enable.auto.commit"), Some("false"));
        assert_eq!(client.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(client.get("auto.offset.reset"), Some("latest"));
        assert_eq!(client.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(client.get("sasl.mechanisms"), Some("PLAIN"));
        assert_eq!(client.get("sasl.username"), Some("user"));
        assert_eq!(client.get("group.id"), Some("example-group"));
        assert_eq!(
            client.get("max.poll.interval.ms"),
            Some(MAX_POLL_INTERVAL_MS)
        );
    }

    /// The warning `rd_kafka_offset_reset` logs for a reset after an error
    /// (librdkafka 2.12's format), and lines that are not one.
    #[test]
    fn an_offset_reset_after_an_error_is_recognised_in_the_log() {
        assert!(is_offset_reset_log(
            "OFFSET",
            "darwin-topic [0]: offset reset (at offset 120 (leader epoch 3), broker 2) to \
             BEGINNING: fetch failed due to requested offset not available on the broker: \
             Broker: Offset out of range"
        ));
        assert!(!is_offset_reset_log(
            "OFFSET",
            "darwin-topic [0]: committed offset 120"
        ));
        assert!(!is_offset_reset_log("FETCH", "offset reset (at 1)"));
    }

    /// An `OffsetFetch` answer: -1001 (`Offset::Invalid`) is "no committed
    /// offset"; a real offset is not.
    #[test]
    fn partitions_without_a_committed_offset_are_found() {
        let mut committed = TopicPartitionList::new();
        committed
            .add_partition_offset("t", 0, Offset::Offset(120))
            .unwrap();
        committed
            .add_partition_offset("t", 1, Offset::Invalid)
            .unwrap();
        committed
            .add_partition_offset("t", 2, Offset::Offset(0))
            .unwrap();
        assert_eq!(partitions_without_committed_offset(&committed), vec![1]);
    }

    #[test]
    fn sasl_without_credentials_is_refused() {
        assert!(client_config(&config(&[])).is_err());
        let plain = client_config(&config(&["--kafka-security-protocol", "SSL"])).expect("ok");
        assert_eq!(plain.get("sasl.username"), None);
    }
}
