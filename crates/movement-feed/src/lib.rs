//! `MovementFeed`: the shared trait between `crates/trust-consumer` and
//! `crates/full-coverage-consumer`'s consume loops and their transport.
//! Historically each crate hand-duplicated this trait plus its own Kafka
//! implementation (`crates/trust-consumer/src/feed/{mod,kafka}.rs`,
//! `crates/full-coverage-consumer/src/feed/{mod,kafka}.rs`) -- that
//! duplication was justified while each crate's transport was genuinely
//! per-consumer (a different Kafka `group.id` each). It stopped being
//! justified once both became structurally identical Redis Streams
//! readers of the same `movement-events` stream, differing only in which
//! named consumer group they read as -- see
//! docs/superpowers/specs/2026-09-04-movement-relay-design.md Decision 3
//! and docs/superpowers/plans/2026-09-04-movement-relay-plan.md Task 2.
//!
//! `crates/movement-relay`'s own Kafka consume loop does NOT depend on
//! this crate -- it is a producer/publisher into `movement-events`, not a
//! `MovementFeed` implementer. This crate is consumed only by the two
//! downstream Redis Streams readers.

pub mod redis_stream;

use async_trait::async_trait;

#[async_trait]
pub trait MovementFeed: Send {
    /// Returns the next batch of raw JSON message-batch bodies (each
    /// element is one Redis Stream entry's `payload` field -- the
    /// surviving envelope's raw bytes, unchanged from what `movement-relay`
    /// `XADD`ed; per `trust_schema::schema::parse_batch`'s input shape,
    /// that's normally a single bare `{header, body}` envelope object) not
    /// yet acknowledged. An empty `Vec` means "nothing new right now," not
    /// an error.
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>>;

    /// Acknowledges (`XACK`s, for the real implementation) everything
    /// returned by the most recent `next_batch` call. Only called after
    /// every message in that batch has been successfully written through
    /// downstream -- same at-least-once framing this trait has always had
    /// under Kafka: a crash between `next_batch` and `commit` means the
    /// same batch is redelivered next time (via this consumer's own
    /// pending-entries list, replayed on the next startup -- see
    /// `redis_stream::RedisStreamMovementFeed`'s own doc), which the
    /// `dedup_key` path makes safe.
    ///
    /// A `commit` with nothing received since the last one is a no-op that
    /// still returns `Ok(())`.
    async fn commit(&mut self) -> anyhow::Result<()>;
}

/// Test double for `MovementFeed` -- verbatim in spirit from the two
/// pre-existing, now-deleted copies in `trust-consumer`/
/// `full-coverage-consumer`. `committed_count` only moves for a `commit`
/// that had something to confirm, so a test can assert "this failure path
/// did not advance the feed" and mean it.
#[cfg(any(test, feature = "test-util"))]
pub struct FakeMovementFeed {
    batches: std::collections::VecDeque<Vec<String>>,
    received_since_commit: bool,
    pub committed_count: usize,
}

#[cfg(any(test, feature = "test-util"))]
impl FakeMovementFeed {
    pub fn new(batches: Vec<Vec<String>>) -> Self {
        Self {
            batches: batches.into(),
            received_since_commit: false,
            committed_count: 0,
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
#[async_trait]
impl MovementFeed for FakeMovementFeed {
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
