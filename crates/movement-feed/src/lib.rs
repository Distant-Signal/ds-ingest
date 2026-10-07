//! `MovementFeed`: the shared trait between the `movement-events` consume
//! loops (`crates/trust-consumer`, `crates/full-coverage-consumer`, and
//! `crates/trust-backlog-consumer`) and their transport.
//! Historically each crate hand-duplicated this trait plus its own Kafka
//! implementation (`feed/{mod,kafka}.rs` in trust-consumer and
//! full-coverage-consumer). That stopped being justified once both became
//! structurally identical Redis Streams readers of the same
//! `movement-events` stream, differing only in which named consumer group
//! they read as, and Deploy C (PL-15a) then deleted the Kafka copies -- see
//! docs/superpowers/specs/2026-09-04-movement-relay-design.md Decision 3
//! and docs/superpowers/plans/2026-09-04-movement-relay-plan.md Task 2.
//!
//! `crates/movement-relay`'s own Kafka consume loop does NOT depend on
//! this crate -- it is a producer/publisher into `movement-events`, not a
//! `MovementFeed` implementer. This crate is consumed only by the three
//! downstream Redis Streams readers.

pub mod active_feed;
pub mod redis_stream;

pub use active_feed::{ActiveFeed, MovementFeedBackend};
pub use redis_stream::LONG_PENDING_DELIVERIES;

use async_trait::async_trait;

/// One delivered `movement-events` entry: its payload plus the time it
/// arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedEntry {
    /// The entry's `payload` field -- the surviving envelope's raw bytes,
    /// unchanged from what `movement-relay` `XADD`ed; per
    /// `trust_schema::schema::parse_batch`'s input shape, that's normally a
    /// single bare `{header, body}` envelope object.
    pub payload: String,
    /// When the message arrived: the millisecond part of the entry's
    /// stream id, i.e. the moment `movement-relay` `XADD`ed it (Redis
    /// assigns `<ms>-<seq>` ids from its own clock). This is the closest
    /// record of arrival the entry carries -- the relay writes only
    /// `payload` and `msg_type`, no Kafka timestamp -- and, unlike the
    /// consumer's own clock, it does not move when the consumer lags.
    ///
    /// `None` when the id did not parse (logged, and counted in
    /// `movement_feed_entry_id_unparseable_total`), and for every entry of
    /// [`FakeMovementFeed::new`]; callers fall back to their own "now" via
    /// [`FeedEntry::received_at_or`].
    pub received_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl FeedEntry {
    /// An entry with no known arrival time.
    pub fn new(payload: impl Into<String>) -> Self {
        Self {
            payload: payload.into(),
            received_at: None,
        }
    }

    /// The time this entry arrived, or `now` when that is unknown. Never
    /// later than `now`: an entry cannot arrive after it is processed, so a
    /// stream id slightly ahead of this process's clock (clock skew between
    /// the Redis and consumer pods) is clamped rather than trusted. With no
    /// lag this is within milliseconds of `now`.
    #[must_use]
    pub fn received_at_or(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        self.received_at.map_or(now, |at| at.min(now))
    }
}

/// The instant a stream entry id (`<ms>-<seq>`) was generated at, or
/// `None` if its millisecond part does not parse.
pub fn stream_id_time(id: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let millis: i64 = id.split('-').next()?.parse().ok()?;
    chrono::DateTime::from_timestamp_millis(millis)
}

#[async_trait]
pub trait MovementFeed: Send {
    /// Returns the next batch of entries (see [`FeedEntry`]) not yet
    /// acknowledged. An empty `Vec` means "nothing new right now," not an
    /// error.
    async fn next_batch(&mut self) -> anyhow::Result<Vec<FeedEntry>>;

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

    /// Called INSTEAD of `commit` when the downstream explicitly rejected
    /// the data of the batch the most recent `next_batch` returned (a
    /// 400/413/422 -- see `common::ingest::classify_failure`). NOT called
    /// for a transient failure (unreachable, timeout, 5xx): then the caller
    /// just skips `commit`, and the batch stays pending and is retried
    /// indefinitely.
    ///
    /// The Redis implementation dead-letters a rejected single entry and
    /// narrows a rejected multi-entry batch down one entry at a time -- see
    /// `redis_stream::RedisStreamMovementFeed::reject_batch`. The default
    /// does nothing, leaving the batch uncommitted. `Err` means nothing was dead-lettered
    /// or `ACKed`.
    async fn reject_batch(&mut self, _detail: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Every [`DeadLetter::reason`] in use. `RedisStreamMovementFeed` registers
/// `distant_signal_movement_feed_deadlettered_total{group, reason}` at 0 for
/// each of these when it connects, so the dead-letter alert can use a plain
/// `increase()`. A new reason must be added here.
pub const DEAD_LETTER_REASONS: [&str; 3] =
    ["rejected_by_api", "malformed_entry", "unparseable_payload"];

/// One poison record set aside instead of being retried forever: a stream
/// entry the downstream explicitly rejected (see
/// [`MovementFeed::reject_batch`]), a malformed or unparseable entry, or one
/// row `api` refused from a batch (trust-backlog-consumer). Never an entry
/// that merely failed transiently, however many times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    /// Fixed-vocabulary cause, used as the `reason` metric label: one of
    /// [`DEAD_LETTER_REASONS`].
    pub reason: &'static str,
    /// The `movement-events` entry id, when the record is a whole entry.
    pub source_id: Option<String>,
    /// XPENDING delivery count, when known.
    pub delivery_count: Option<u64>,
    /// The raw entry payload, or the rejected row as JSON -- enough to
    /// re-inject by hand once the cause is fixed.
    pub payload: String,
    /// Free-text explanation (e.g. the SQLSTATE and Postgres message).
    pub detail: String,
}

/// Where a consumer sends [`DeadLetter`]s. Implemented by
/// `RedisStreamMovementFeed` (a small capped Redis stream), by
/// [`ActiveFeed`] (which delegates), and by
/// [`FakeMovementFeed`] (which records them for tests).
#[async_trait]
pub trait DeadLetterSink: Send {
    /// Must return `Err` if the records were not stored, so the caller can
    /// leave the batch un-ACKed and try again rather than lose them.
    async fn dead_letter(&mut self, records: &[DeadLetter]) -> anyhow::Result<()>;
}

/// Test double for `MovementFeed` -- verbatim in spirit from the two
/// pre-existing, now-deleted copies in `trust-consumer`/
/// `full-coverage-consumer`. `committed_count` only moves for a `commit`
/// that had something to confirm, so a test can assert "this failure path
/// did not advance the feed" and mean it.
#[cfg(any(test, feature = "test-util"))]
pub struct FakeMovementFeed {
    batches: std::collections::VecDeque<Vec<FeedEntry>>,
    received_since_commit: bool,
    pub committed_count: usize,
    /// Everything passed to [`DeadLetterSink::dead_letter`], in order.
    pub dead_lettered: Vec<DeadLetter>,
    /// When set, the next `dead_letter` call fails (and clears this).
    pub fail_next_dead_letter: bool,
    /// The `detail` of every [`MovementFeed::reject_batch`] call, in order.
    pub rejected_batches: Vec<String>,
}

#[cfg(any(test, feature = "test-util"))]
impl FakeMovementFeed {
    /// Payload-only batches: no entry has a known arrival time, so the
    /// consumer under test falls back to its own "now".
    pub fn new(batches: Vec<Vec<String>>) -> Self {
        Self::with_entries(
            batches
                .into_iter()
                .map(|batch| batch.into_iter().map(FeedEntry::new).collect())
                .collect(),
        )
    }

    /// Batches of full entries, for tests that set an arrival time.
    pub fn with_entries(batches: Vec<Vec<FeedEntry>>) -> Self {
        Self {
            batches: batches.into(),
            received_since_commit: false,
            committed_count: 0,
            dead_lettered: Vec::new(),
            fail_next_dead_letter: false,
            rejected_batches: Vec::new(),
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
#[async_trait]
impl MovementFeed for FakeMovementFeed {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<FeedEntry>> {
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

    async fn reject_batch(&mut self, detail: &str) -> anyhow::Result<()> {
        self.rejected_batches.push(detail.to_string());
        Ok(())
    }
}

#[cfg(any(test, feature = "test-util"))]
#[async_trait]
impl DeadLetterSink for FakeMovementFeed {
    async fn dead_letter(&mut self, records: &[DeadLetter]) -> anyhow::Result<()> {
        if std::mem::take(&mut self.fail_next_dead_letter) {
            anyhow::bail!("fake dead-letter failure");
        }
        self.dead_lettered.extend_from_slice(records);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> chrono::DateTime<chrono::Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn a_stream_id_parses_to_its_millisecond_time() {
        assert_eq!(
            stream_id_time("1790384340123-7"),
            Some(utc("2026-09-26T00:59:00.123Z"))
        );
        // A bare `<ms>` with no sequence part is still a time.
        assert_eq!(stream_id_time("0"), Some(utc("1970-01-01T00:00:00Z")));
    }

    #[test]
    fn a_malformed_stream_id_has_no_time() {
        for id in [
            "",
            "-1",
            "abc-0",
            "1.5-0",
            " 12-0",
            "99999999999999999999-0",
        ] {
            assert_eq!(stream_id_time(id), None, "{id:?}");
        }
    }

    #[test]
    fn an_unknown_arrival_time_falls_back_to_now() {
        let now = utc("2026-10-01T01:05:00Z");
        assert_eq!(FeedEntry::new("p").received_at_or(now), now);
    }

    #[test]
    fn a_lagging_entry_keeps_its_arrival_time() {
        let now = utc("2026-10-01T01:05:00Z");
        let arrived = utc("2026-10-01T00:59:00Z");
        let entry = FeedEntry {
            payload: "p".to_string(),
            received_at: Some(arrived),
        };
        assert_eq!(entry.received_at_or(now), arrived);
    }

    #[test]
    fn an_arrival_time_ahead_of_now_is_clamped_to_now() {
        let now = utc("2026-10-01T01:05:00Z");
        let entry = FeedEntry {
            payload: "p".to_string(),
            received_at: Some(now + chrono::Duration::seconds(2)),
        };
        assert_eq!(entry.received_at_or(now), now);
    }
}
