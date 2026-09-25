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

    /// Hands a batch whose downstream write failed back to the source, so
    /// the NEXT `next_batch` call re-delivers exactly that batch instead of
    /// fetching anything new.
    ///
    /// This exists because `recv()`-then-`store_offset()` is a
    /// receive/confirm split with NO rollback of its own: every
    /// `consumer.recv()` advances librdkafka's own fetch position and
    /// overwrites `last_received`, whether or not the previously received
    /// record was ever confirmed. Without this hand-back, a cycle that
    /// returned `Cycle::Failed` because a downstream XADD failed would have
    /// its record silently skipped -- the next cycle's `recv()` moves the
    /// fetch position past it, and the next SUCCESSFUL `commit()` stores
    /// that later offset, implicitly committing past the record that never
    /// made it into `movement-events`. During a Redis outage that is one
    /// permanently dropped record per retry cycle, from the single feed
    /// `trust-consumer`, `full-coverage-consumer` and `trust-event-backlog`
    /// all read.
    ///
    /// Retaining the batch in memory (rather than `seek()`ing the consumer
    /// back to `last_received`) keeps the re-delivery synchronous and
    /// independent of librdkafka's asynchronous seek/fetch-position
    /// machinery: the payload we already have is exactly the payload we
    /// need, and `last_received` still points at it, so a later successful
    /// `commit()` stores the right offset.
    fn retain_for_retry(&mut self, batch: Vec<String>);

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
    /// A batch handed back by `retain_for_retry` because its downstream
    /// write failed. While this is `Some`, `next_batch` re-delivers it and
    /// deliberately does NOT call `consumer.recv()` -- `last_received` must
    /// keep pointing at this record's own offset until it is either
    /// committed or handed back again.
    pending_retry: Option<Vec<String>>,
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
            pending_retry: None,
        })
    }
}

#[async_trait]
impl RawKafkaSource for KafkaRawSource {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        // A previously-received batch whose downstream write failed is
        // re-delivered before anything new is fetched: `recv()` here would
        // advance the fetch position past it and overwrite `last_received`,
        // which is exactly how such a record used to be dropped.
        if let Some(batch) = self.pending_retry.take() {
            return Ok(batch);
        }
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

    fn retain_for_retry(&mut self, batch: Vec<String>) {
        self.pending_retry = Some(batch);
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

/// Test double modelling librdkafka's real position semantics, not a
/// forgiving abstraction over them: every `next_batch` that actually
/// fetches advances an internal fetch position by one record and
/// OVERWRITES `last_received`, and `commit` stores only whatever
/// `last_received` currently holds. That is what makes the
/// dropped-record failure this fake is used to test reproducible at all --
/// a `run_cycle` that fails a downstream write and does not hand its batch
/// back will see the next cycle fetch record N+1 and then commit N+1's
/// offset, silently skipping N.
#[cfg(test)]
pub struct FakeRawSource {
    /// Records not yet fetched, in offset order -- one element per Kafka
    /// record. An empty element models a poll that returned nothing.
    batches: std::collections::VecDeque<Vec<String>>,
    /// The offset the next fetched record will carry.
    next_offset: i64,
    /// Offset of the most recently fetched record, cleared by a successful
    /// `commit` -- the fake's stand-in for `KafkaRawSource::last_received`.
    last_received: Option<i64>,
    pending_retry: Option<Vec<String>>,
    /// When true, the next `commit` call fails (and leaves
    /// `last_received` in place, exactly as the real `store_offset`/
    /// `commit_consumer_state` error path does).
    pub fail_next_commit: bool,
    pub committed_count: usize,
    /// Every offset a successful `commit` stored, in order -- so a test can
    /// assert WHICH record's offset was committed, not merely how many
    /// commits happened.
    pub committed_offsets: Vec<i64>,
}

#[cfg(test)]
impl FakeRawSource {
    pub fn new(batches: Vec<Vec<String>>) -> Self {
        Self {
            batches: batches.into(),
            next_offset: 0,
            last_received: None,
            pending_retry: None,
            fail_next_commit: false,
            committed_count: 0,
            committed_offsets: Vec::new(),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl RawKafkaSource for FakeRawSource {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        if let Some(batch) = self.pending_retry.take() {
            return Ok(batch);
        }
        let batch = self.batches.pop_front().unwrap_or_default();
        if !batch.is_empty() {
            self.last_received = Some(self.next_offset);
            self.next_offset += 1;
        }
        Ok(batch)
    }

    fn retain_for_retry(&mut self, batch: Vec<String>) {
        self.pending_retry = Some(batch);
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        if self.fail_next_commit {
            self.fail_next_commit = false;
            return Err(anyhow::anyhow!("simulated offset commit failure"));
        }
        let Some(offset) = self.last_received else {
            return Ok(());
        };
        self.committed_offsets.push(offset);
        self.committed_count += 1;
        self.last_received = None;
        Ok(())
    }
}
