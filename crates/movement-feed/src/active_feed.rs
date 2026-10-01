//! `ActiveFeed` + `MovementFeedBackend`: the movement feed a consumer
//! reads, plus the readiness reporting every caller needs around it.
//! Previously duplicated near-verbatim across `trust-consumer`'s and
//! `full-coverage-consumer`'s own `main.rs`/`config.rs`. See
//! docs/superpowers/specs/2026-09-05-rust-service-deduplication-design.md
//! §3.5.
//!
//! Deploy C (PL-15a / R-101) removed the consumers' legacy direct-Kafka
//! backend: `movement-relay` is the only Kafka client, and every consumer
//! reads its `movement-events` Redis stream. `MOVEMENT_FEED_BACKEND=kafka`
//! now fails startup (see [`MovementFeedBackend`]'s `FromStr`), because the
//! chart passed trust-consumer the relay's own RDM consumer group: a
//! consumer that silently honoured it would have joined that group and
//! taken half its partitions away from the relay.

use std::fmt;
use std::str::FromStr;

use crate::redis_stream::{GapInfo, RedisStreamMovementFeed};
use crate::{DeadLetter, DeadLetterSink, MovementFeed};

/// Which transport a `MovementFeed` consumer uses. Only `redis-stream`
/// remains; the setting is kept so that an explicit
/// `MOVEMENT_FEED_BACKEND=redis-stream` (production's value) still parses,
/// and an explicit `kafka` is refused with a clear message instead of
/// being silently ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MovementFeedBackend {
    /// The Redis Streams reader (`RedisStreamMovementFeed`, this crate),
    /// reading what `movement-relay` publishes.
    RedisStream,
}

/// The refusal for `MOVEMENT_FEED_BACKEND=kafka`. Public so callers' tests
/// can assert on it.
pub const KAFKA_BACKEND_REMOVED: &str = "the kafka movement feed backend was removed (Deploy C, PL-15a): \
     movement-relay is the only Kafka client and every consumer reads its movement-events \
     Redis stream. A consumer reading Kafka directly would join movement-relay's RDM \
     consumer group and take partitions away from it. Unset MOVEMENT_FEED_BACKEND or set \
     it to redis-stream";

impl FromStr for MovementFeedBackend {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "redis-stream" => Ok(MovementFeedBackend::RedisStream),
            "kafka" => Err(KAFKA_BACKEND_REMOVED.to_string()),
            other => Err(format!(
                "unknown movement feed backend {other:?}; the only backend is redis-stream"
            )),
        }
    }
}

impl fmt::Display for MovementFeedBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MovementFeedBackend::RedisStream => f.write_str("redis-stream"),
        }
    }
}

/// The selected `MovementFeed` backend. The `RedisStream` variant's third
/// field is the Prometheus gauge name to report readiness under (e.g.
/// `"trust_consumer_ready"` / `"full_coverage_consumer_ready"`) --
/// per-caller, so it's supplied at construction time rather than hardcoded
/// inside this shared type's own `next_batch` impl.
pub enum ActiveFeed {
    RedisStream(
        Box<RedisStreamMovementFeed>,
        health_http::ConnectionState,
        &'static str,
    ),
}

#[async_trait::async_trait]
impl MovementFeed for ActiveFeed {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        match self {
            ActiveFeed::RedisStream(feed, connection_state, gauge_name) => {
                let result = feed.next_batch().await;
                health_http::set_connected(connection_state, gauge_name, result.is_ok());
                result
            }
        }
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        match self {
            ActiveFeed::RedisStream(feed, _, _) => feed.commit().await,
        }
    }

    async fn reject_batch(&mut self, detail: &str) -> anyhow::Result<()> {
        match self {
            ActiveFeed::RedisStream(feed, _, _) => feed.reject_batch(detail).await,
        }
    }
}

#[async_trait::async_trait]
impl DeadLetterSink for ActiveFeed {
    async fn dead_letter(&mut self, records: &[DeadLetter]) -> anyhow::Result<()> {
        match self {
            ActiveFeed::RedisStream(feed, _, _) => feed.dead_letter(records).await,
        }
    }
}

impl ActiveFeed {
    /// The Redis Streams reader, for callers that need more than the
    /// `MovementFeed` surface (e.g. a group-less `XRANGE` replay).
    pub fn redis_stream(&mut self) -> &mut RedisStreamMovementFeed {
        match self {
            ActiveFeed::RedisStream(feed, _, _) => feed,
        }
    }

    /// Delegates to `RedisStreamMovementFeed::check_gap`.
    pub async fn check_gap(&mut self) -> anyhow::Result<Option<GapInfo>> {
        self.redis_stream().check_gap().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_stream_round_trips_through_its_name() {
        let backend: MovementFeedBackend = "redis-stream".parse().unwrap();
        assert_eq!(backend, MovementFeedBackend::RedisStream);
        assert_eq!(backend.to_string(), "redis-stream");
    }

    /// R-101: an explicit `kafka` must refuse to start, not fall back to
    /// Redis quietly and not connect to Kafka.
    #[test]
    fn the_removed_kafka_backend_is_refused_with_an_explanation() {
        let err = "kafka".parse::<MovementFeedBackend>().unwrap_err();
        assert_eq!(err, KAFKA_BACKEND_REMOVED);
        assert!(err.contains("removed"));
        assert!(err.contains("redis-stream"));
    }

    #[test]
    fn an_unknown_backend_is_refused() {
        let err = "kafak".parse::<MovementFeedBackend>().unwrap_err();
        assert!(err.contains("\"kafak\""), "{err}");
    }
}
