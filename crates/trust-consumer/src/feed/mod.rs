//! `MovementFeed`: the one trait that stands between the Kafka-specific
//! consumer (`kafka.rs`) and everything else in this crate. See Task 9's
//! doc comment in the implementation plan for why this replaces
//! `wiremock`-style testing here.

pub mod kafka;

use async_trait::async_trait;

#[async_trait]
pub trait MovementFeed: Send {
    /// Returns the next batch of raw JSON message-batch bodies (each element
    /// is one raw Kafka record's payload -- per `schema::parse_batch`'s
    /// input shape, that's normally a single bare `{header, body}` envelope
    /// object, confirmed live against a real RDM Train Movements feed, with
    /// a JSON array of envelopes handled defensively too) not yet committed.
    /// An empty `Vec` means "nothing new right now," not an error.
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>>;

    /// Commits offsets for everything returned by the most recent
    /// `next_batch` call. Only called after every message in that batch
    /// has been successfully written through to `api` -- see Task 14's
    /// at-least-once framing: a crash between `next_batch` and `commit`
    /// means the same batch is redelivered next time, which the dedup_key
    /// path (Task 4/13) makes safe.
    ///
    /// This is the *only* thing an implementation may use to advance its
    /// consumed position. Nothing about receiving a batch may mark it
    /// committable on its own -- `kafka.rs` sets
    /// `enable.auto.offset.store=false` precisely so that receiving a
    /// message and confirming it are two separate events. An
    /// implementation that advances on receipt turns the caller's
    /// "don't commit, this post failed" into a silent data loss, since
    /// the *next* commit would sweep the uncommitted message up with it.
    ///
    /// A `commit` with nothing received since the last one is a no-op that
    /// still returns `Ok(())`.
    async fn commit(&mut self) -> anyhow::Result<()>;
}

/// Test double for `MovementFeed`, deliberately mirroring `KafkaMovementFeed`'s
/// receive/confirm split rather than being a bare counter: `committed_count`
/// only moves for a `commit` that had something to confirm, so a test can
/// assert "this failure path did not advance the feed" and mean it.
#[cfg(test)]
pub struct FakeMovementFeed {
    batches: std::collections::VecDeque<Vec<String>>,
    /// The real feed's `last_received`, reduced to the one bit these tests
    /// can observe: is there an unconfirmed message in hand?
    received_since_commit: bool,
    pub committed_count: usize,
}

#[cfg(test)]
impl FakeMovementFeed {
    pub fn new(batches: Vec<Vec<String>>) -> Self {
        Self { batches: batches.into(), received_since_commit: false, committed_count: 0 }
    }
}

#[cfg(test)]
#[async_trait]
impl MovementFeed for FakeMovementFeed {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        let batch = self.batches.pop_front().unwrap_or_default();
        // An empty batch is "nothing new right now" (see the trait doc), so
        // it leaves nothing to confirm -- same as the real feed, which only
        // records an offset when a payload actually arrived.
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
