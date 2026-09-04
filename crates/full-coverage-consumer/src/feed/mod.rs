//! `MovementFeed`: the one trait that stands between the Kafka-specific
//! consumer (`kafka.rs`) and everything else in this crate. Copied
//! structurally from `crates/trust-consumer/src/feed/mod.rs` -- Task 13's
//! own note: this is genuinely per-consumer Kafka plumbing, not shared
//! logic Task 1's `trust-schema` extraction was ever meant to cover (that
//! extraction was explicitly parsing/dedup/journey only).

pub mod kafka;

use async_trait::async_trait;

#[async_trait]
pub trait MovementFeed: Send {
    /// Returns the next batch of raw JSON message-batch bodies (each element
    /// is one raw Kafka record's payload -- per `trust_schema::schema::parse_batch`'s
    /// input shape, that's normally a single bare `{header, body}` envelope
    /// object, with a JSON array of envelopes handled defensively too) not
    /// yet committed. An empty `Vec` means "nothing new right now," not an
    /// error.
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>>;

    /// Commits offsets for everything returned by the most recent
    /// `next_batch` call. See this crate's `main.rs` module doc for why,
    /// unlike `trust-consumer`, this crate's own commit cadence does not
    /// need to wait on the periodic stats POST succeeding -- correlation
    /// state derivation here is idempotent on redelivery.
    ///
    /// A `commit` with nothing received since the last one is a no-op that
    /// still returns `Ok(())`.
    async fn commit(&mut self) -> anyhow::Result<()>;
}

/// Test double for `MovementFeed`, deliberately mirroring `KafkaMovementFeed`'s
/// receive/confirm split rather than being a bare counter -- verbatim copy
/// of `trust-consumer`'s own `FakeMovementFeed`.
#[cfg(test)]
pub struct FakeMovementFeed {
    batches: std::collections::VecDeque<Vec<String>>,
    received_since_commit: bool,
    pub committed_count: usize,
}

#[cfg(test)]
impl FakeMovementFeed {
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
