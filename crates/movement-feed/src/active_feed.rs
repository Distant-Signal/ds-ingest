//! `ActiveFeed<K>` + `MovementFeedBackend`: shared dispatch between
//! whichever concrete `MovementFeed` backend a caller selected. Previously
//! duplicated near-verbatim (differing only in doc-comment wording) across
//! `trust-consumer`'s and `full-coverage-consumer`'s own `main.rs`/
//! `config.rs`. See
//! docs/superpowers/specs/2026-09-05-rust-service-deduplication-design.md
//! §3.5 -- and this plan's own "Fresh-verification corrections" section
//! for why `ActiveFeed` is generic over its Kafka variant: `KafkaMovementFeed`
//! is a distinct, crate-local type per caller (each crate's own
//! `feed::kafka` module, scheduled for deletion in Deploy C, out of scope
//! here), not actually shared the way the design spec assumed. Sharing it
//! would require either merging those two modules (out of scope) or
//! picking one crate's own type arbitrarily (worse than the status quo);
//! a generic parameter avoids both.

use crate::MovementFeed;
use crate::redis_stream::{GapInfo, RedisStreamMovementFeed};

/// Which transport a `MovementFeed` consumer uses. Verbatim move of the
/// two byte-identical enums previously duplicated in
/// `trust-consumer/src/config.rs` and
/// `full-coverage-consumer/src/config.rs`.
#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq, Eq)]
pub enum MovementFeedBackend {
    /// A direct Kafka consumer, via each caller's own crate-local
    /// `feed::kafka::KafkaMovementFeed`.
    Kafka,
    /// The Redis Streams reader (`RedisStreamMovementFeed`, this crate),
    /// reading what `movement-relay` publishes.
    RedisStream,
}

/// Wraps whichever concrete `MovementFeed` backend was selected. Generic
/// over `K` (each caller's own Kafka implementation) -- see this module's
/// own doc for why. The `RedisStream` variant's third field is the
/// Prometheus gauge name to report readiness under (e.g.
/// `"trust_consumer_ready"` / `"full_coverage_consumer_ready"`) --
/// per-caller, so it's supplied at construction time rather than hardcoded
/// inside this now-shared type's own `next_batch` impl.
pub enum ActiveFeed<K: MovementFeed> {
    Kafka(K),
    RedisStream(
        Box<RedisStreamMovementFeed>,
        health_http::ConnectionState,
        &'static str,
    ),
}

#[async_trait::async_trait]
impl<K: MovementFeed> MovementFeed for ActiveFeed<K> {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        match self {
            ActiveFeed::Kafka(feed) => feed.next_batch().await,
            ActiveFeed::RedisStream(feed, connection_state, gauge_name) => {
                let result = feed.next_batch().await;
                health_http::set_connected(connection_state, gauge_name, result.is_ok());
                result
            }
        }
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        match self {
            ActiveFeed::Kafka(feed) => feed.commit().await,
            ActiveFeed::RedisStream(feed, _, _) => feed.commit().await,
        }
    }
}

impl<K: MovementFeed> ActiveFeed<K> {
    /// `Ok(None)` immediately for the `Kafka` variant (no analog);
    /// delegates to `RedisStreamMovementFeed::check_gap` for the
    /// `RedisStream` variant.
    pub async fn check_gap(&mut self) -> anyhow::Result<Option<GapInfo>> {
        match self {
            ActiveFeed::Kafka(_) => Ok(None),
            ActiveFeed::RedisStream(feed, _, _) => feed.check_gap().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FakeMovementFeed;

    #[tokio::test]
    async fn kafka_variant_check_gap_is_always_none() {
        let mut feed: ActiveFeed<FakeMovementFeed> =
            ActiveFeed::Kafka(FakeMovementFeed::new(vec![]));
        assert_eq!(feed.check_gap().await.unwrap(), None);
    }

    #[tokio::test]
    async fn kafka_variant_delegates_next_batch_and_commit() {
        let mut feed: ActiveFeed<FakeMovementFeed> =
            ActiveFeed::Kafka(FakeMovementFeed::new(vec![vec!["one".to_string()]]));
        let batch = feed.next_batch().await.unwrap();
        assert_eq!(batch, vec!["one".to_string()]);
        feed.commit().await.unwrap();
    }
}
