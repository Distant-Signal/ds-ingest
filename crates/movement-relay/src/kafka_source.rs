//! Raw Kafka source for movement-relay. Structurally close to
//! `trust-consumer/src/feed/kafka.rs` (same ClientConfig shape, same
//! store-then-commit offset discipline) but deliberately NOT shared via
//! `crates/movement-feed` -- see
//! docs/superpowers/plans/2026-09-04-movement-relay-plan.md Task 6 for why:
//! `movement-feed` is scoped to the two downstream Redis Streams *readers*,
//! while this crate is a raw-Kafka-payload consumer / Redis *producer*, a
//! structurally different role (Decision 3's own tree sketch: "movement-relay's
//! OWN Kafka consume loop does NOT depend on this crate"). Do not "fix" this
//! duplication by merging it into `movement-feed` -- it is a small,
//! deliberate exception to this repo's usual DRY instinct, justified by
//! crate-boundary purity. `trust-consumer`'s own copy is deleted in Deploy C
//! (Task 13); this crate's copy is permanent.
//!
//! Returns RAW record payloads (unclassified) -- classification into
//! confirmed/unknown message types happens in `main.rs` via
//! `trust_schema::schema::confirmed_envelope_bodies`, not here.

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;

use crate::config::Config;
use crate::health::RelayContext;

#[async_trait]
pub trait RawKafkaSource: Send {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>>;
    async fn commit(&mut self) -> anyhow::Result<()>;
}

pub struct KafkaRawSource {
    consumer: StreamConsumer<RelayContext>,
    /// `(topic, partition, offset)` of the message the most recent
    /// successful `next_batch` returned, held until `commit` either stores
    /// it or it's replaced by the next received message -- same
    /// receive/confirm split `trust-consumer/src/feed/kafka.rs`'s
    /// `KafkaMovementFeed::last_received` already established.
    last_received: Option<(String, i32, i64)>,
}

impl KafkaRawSource {
    /// Readiness is entirely owned by `RelayContext`'s rebalance callback
    /// (Task 5), NOT by an `Err` path in this module's own `next_batch` the
    /// way `trust-consumer`'s `KafkaMovementFeed` uses its
    /// `connection_state` flag -- the one structural divergence from that
    /// crate's copy beyond the return-shape difference.
    pub fn connect(config: &Config, ready: health_http::ConnectionState) -> anyhow::Result<Self> {
        let context = RelayContext { ready };
        let consumer: StreamConsumer<RelayContext> = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka.kafka_brokers)
            .set("group.id", &config.kafka_consumer_group)
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanisms", &config.kafka.kafka_sasl_mechanism)
            .set("sasl.username", &config.kafka.kafka_sasl_username)
            .set("sasl.password", &config.kafka.kafka_sasl_password)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .create_with_context(context)?;

        consumer.subscribe(&[&config.kafka.kafka_topic])?;

        Ok(Self {
            consumer,
            last_received: None,
        })
    }
}

#[async_trait]
impl RawKafkaSource for KafkaRawSource {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        let message = self.consumer.recv().await?;
        let payload = message
            .payload()
            .ok_or_else(|| anyhow::anyhow!("empty Kafka message payload"))?;
        let batch = String::from_utf8_lossy(payload).into_owned();
        self.last_received = Some((
            message.topic().to_string(),
            message.partition(),
            message.offset(),
        ));
        Ok(vec![batch])
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        let Some((topic, partition, offset)) = self.last_received.as_ref() else {
            return Ok(());
        };
        self.consumer.store_offset(topic, *partition, *offset)?;
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Async)?;
        self.last_received = None;
        Ok(())
    }
}

#[cfg(test)]
pub struct FakeRawSource {
    batches: std::collections::VecDeque<Vec<String>>,
    received_since_commit: bool,
    pub committed_count: usize,
}

#[cfg(test)]
impl FakeRawSource {
    pub fn new(batches: Vec<Vec<String>>) -> Self {
        Self {
            batches: batches.into(),
            received_since_commit: false,
            committed_count: 0,
        }
    }
}

#[cfg(test)]
#[async_trait]
impl RawKafkaSource for FakeRawSource {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        let batch = self.batches.pop_front().unwrap_or_default();
        if !batch.is_empty() {
            self.received_since_commit = true;
        }
        Ok(batch)
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        if !self.received_since_commit {
            return Ok(());
        }
        self.received_since_commit = false;
        self.committed_count += 1;
        Ok(())
    }
}
