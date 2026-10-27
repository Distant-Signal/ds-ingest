//! The Kafka side: a librdkafka consumer on the Push Port JSON topic, in
//! the same shape as movement-relay's (SASL, the RDM-issued group, manual
//! offset store and commit, readiness from the partition assignment), plus
//! a consumer-lag gauge from librdkafka's statistics.
//!
//! Offsets are committed only after the batch holding their records is in
//! Redis ([`crate::relay`]), so a crash or restart re-reads at most the
//! records of the batch in flight; their rows are idempotent (Darwin
//! timestamp guards in the writer).

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, ClientContext, Statistics};

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
    /// Stores `offset` as processed (the commit is `offset + 1`), then
    /// commits. A partition revoked in the meantime is skipped: its new
    /// owner re-reads from the last commit.
    fn commit(&mut self, offsets: &[(String, i32, i64)]) -> anyhow::Result<()>;
}

/// Readiness from the partition assignment (movement-relay's rule), and
/// the lag gauge from librdkafka's statistics.
pub(crate) struct RelayContext {
    pub ready: health_http::ConnectionState,
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
}

impl ConsumerContext for RelayContext {
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) if !partitions.elements().is_empty() => {
                health_http::set_connected(&self.ready, metrics::READY, true);
                tracing::info!(
                    partitions = partitions.elements().len(),
                    "darwin-relay: Kafka partitions assigned; ready"
                );
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
}

impl KafkaSource {
    pub(crate) fn connect(
        config: &Config,
        ready: health_http::ConnectionState,
        progress: health_http::Progress,
    ) -> anyhow::Result<Self> {
        let consumer: StreamConsumer<RelayContext> =
            client_config(config)?.create_with_context(RelayContext { ready })?;
        consumer.subscribe(&[&config.kafka_topic])?;
        Ok(Self { consumer, progress })
    }
}

#[async_trait]
impl RecordSource for KafkaSource {
    async fn recv(&mut self, timeout: Option<Duration>) -> anyhow::Result<Option<Record>> {
        let received = match timeout {
            // Waiting for the first record of a batch is idle time, not a
            // stall: a quiet topic must not get the relay restarted.
            None => Some(self.progress.idle(self.consumer.recv()).await),
            Some(timeout) => tokio::time::timeout(timeout, self.consumer.recv())
                .await
                .ok(),
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
        let assignment = self.consumer.assignment()?;
        let mut stored = 0;
        for (topic, partition, offset) in offsets {
            let assigned = assignment
                .elements()
                .iter()
                .any(|e| e.topic() == topic && e.partition() == *partition);
            if !assigned {
                tracing::warn!(
                    topic,
                    partition,
                    offset,
                    "Kafka partition revoked before its offset was stored; its new owner re-reads it"
                );
                continue;
            }
            self.consumer.store_offset(topic, *partition, *offset)?;
            stored += 1;
        }
        if stored > 0 {
            self.consumer
                .commit_consumer_state(rdkafka::consumer::CommitMode::Async)?;
        }
        Ok(())
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

    #[test]
    fn sasl_without_credentials_is_refused() {
        assert!(client_config(&config(&[])).is_err());
        let plain = client_config(&config(&["--kafka-security-protocol", "SSL"])).expect("ok");
        assert_eq!(plain.get("sasl.username"), None);
    }
}
