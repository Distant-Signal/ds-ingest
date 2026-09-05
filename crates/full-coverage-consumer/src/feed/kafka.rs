//! Production `MovementFeed`: wraps `rdkafka`'s `StreamConsumer` against
//! RDM's Kafka Train Movements product. Structurally identical to
//! `crates/trust-consumer/src/feed/kafka.rs` -- a second, independent
//! consumer against the same feed, per Decision 1's "connection vs. group
//! membership" reasoning (own `group.id`, same broker/topic/SASL
//! mechanism at the Helm layer).

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
    /// See `trust-consumer`'s own identical field for the full reasoning:
    /// `(topic, partition, offset)` of the most recently received message,
    /// held until `commit` stores it or it's replaced by the next one.
    last_received: Option<(String, i32, i64)>,
}

impl KafkaMovementFeed {
    pub fn connect(config: &Config, connection_state: ConnectionState) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("group.id", &config.kafka_consumer_group)
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanisms", &config.kafka_sasl_mechanism)
            .set("sasl.username", &config.kafka_sasl_username)
            .set("sasl.password", &config.kafka_sasl_password)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .create()?;

        consumer.subscribe(&[&config.kafka_topic])?;

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
