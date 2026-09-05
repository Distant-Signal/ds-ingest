//! Production `MovementFeed`: wraps `rdkafka`'s `StreamConsumer` against
//! RDM's Kafka Train Movements product. SASL_SSL is assumed (RDM's Kafka
//! products are described as SASL-authenticated in the design doc's
//! research; the exact mechanism is a startup-time GAP, see `config.rs`).

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;

use super::MovementFeed;
use crate::config::Config;
use crate::health::ConnectionState;

pub struct KafkaMovementFeed {
    consumer: StreamConsumer,
    connection_state: ConnectionState,
    /// `(topic, partition, offset)` of the message the most recent
    /// successful `next_batch` returned, held until `commit` either stores
    /// it (the caller confirmed the batch reached `api`) or it is replaced
    /// by the next received message.
    ///
    /// The three primitives are copied out rather than the message itself
    /// being kept: `StreamConsumer::recv` yields a `BorrowedMessage` whose
    /// lifetime is tied to the poll that produced it, so it cannot live on
    /// this struct across calls. That rules out
    /// `Consumer::store_offset_from_message` and is why `commit` uses the
    /// `(topic, partition, offset)` form of `Consumer::store_offset`
    /// instead -- the two do identical work (both hand the message's own
    /// offset to `rd_kafka_offset_store`, which stores `offset + 1` as the
    /// next offset to consume), they just differ in what they take.
    last_received: Option<(String, i32, i64)>,
}

impl KafkaMovementFeed {
    pub fn connect(config: &Config, connection_state: ConnectionState) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka.kafka_brokers)
            .set("group.id", &config.kafka_consumer_group)
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanisms", &config.kafka.kafka_sasl_mechanism)
            .set("sasl.username", &config.kafka.kafka_sasl_username)
            .set("sasl.password", &config.kafka.kafka_sasl_password)
            .set("enable.auto.commit", "false") // explicit commit, see MovementFeed::commit
            // MUST accompany the manual `store_offset` call in `commit`.
            // librdkafka defaults this to `true`, which marks a message's
            // offset as ready-to-commit the instant `recv` hands it to us,
            // before this crate has parsed it let alone posted it. With
            // auto-store left on, `enable.auto.commit=false` buys nothing:
            // the next successful `commit_consumer_state` would sweep up
            // the auto-stored offset of a message whose post had just
            // failed, so the failure would advance past that message
            // rather than leaving it to be redelivered. Turning it off
            // makes "committed" mean exactly "this crate confirmed it",
            // which is what the at-least-once framing in
            // `MovementFeed::commit`'s docs assumes. librdkafka itself
            // requires this setting to be `false` when `store_offset` is
            // used (see rdkafka.h's `rd_kafka_offset_store` remarks).
            .set("enable.auto.offset.store", "false")
            .create()?;

        consumer.subscribe(&[&config.kafka.kafka_topic])?;

        Ok(Self {
            consumer,
            connection_state,
            last_received: None,
        })
    }
}

#[async_trait]
impl MovementFeed for KafkaMovementFeed {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        match self.consumer.recv().await {
            Ok(message) => {
                crate::health::set_connected(&self.connection_state, true);
                let payload = message
                    .payload()
                    .ok_or_else(|| anyhow::anyhow!("empty Kafka message payload"))?;
                let batch = String::from_utf8_lossy(payload).into_owned();
                // Recorded only once the payload is in hand: an empty
                // payload returns `Err` above, and an errored batch must
                // not leave an offset behind for `commit` to store.
                self.last_received = Some((
                    message.topic().to_string(),
                    message.partition(),
                    message.offset(),
                ));
                Ok(vec![batch])
            }
            Err(err) => {
                crate::health::set_connected(&self.connection_state, false);
                Err(err.into())
            }
        }
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        // Nothing received since the last commit -- there is no offset to
        // advance, so this is a no-op rather than a redundant broker
        // round-trip.
        let Some((topic, partition, offset)) = self.last_received.as_ref() else {
            return Ok(());
        };

        // Store first, then commit: with `enable.auto.offset.store=false`
        // this is the only thing that ever marks an offset committable, so
        // it happens here -- on the caller's confirmation of success --
        // and nowhere else. `commit_consumer_state` then writes whatever
        // is stored across the whole assignment.
        self.consumer.store_offset(topic, *partition, *offset)?;
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Async)?;

        // Cleared only once both steps succeeded, so a failed store or
        // commit leaves the offset in hand for the next attempt rather
        // than dropping it on the floor.
        self.last_received = None;
        Ok(())
    }
}
