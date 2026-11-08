//! Kafka offsets held back until the Redis writes they stand for are on
//! disk, for the two Kafka relays (`movement-relay`, `darwin-relay`).
//!
//! **Why.** The chart's Redis runs AOF with `appendfsync everysec`: an
//! `XADD` is acknowledged once it is in Redis's memory (and, normally, the
//! AOF's page cache), and fsynced by a background job about a second later.
//! A relay that commits a Kafka offset the moment its `XADD` returns can
//! therefore commit records that a hard crash (the node, or Redis while an
//! fsync is stalled and the AOF write is postponed) then loses: Kafka
//! considers them consumed and nothing re-reads them, a silent gap in every
//! downstream consumer.
//!
//! **What.** A relay hands each offset to [`DurableOffsets::written`] once
//! its `XADD`s succeed, and stores and commits only what
//! [`DurableOffsets::take_due`] returns: offsets whose writes are at least
//! `delay` old, several fsync intervals. A crash re-reads at most `delay`
//! of already-written records, duplicates that every consumer of the
//! streams absorbs (dedup keys, idempotency keys, timestamp guards); the
//! ingest rule is to never trade a gap for fewer duplicates.
//!
//! A time bound, not `WAITAOF`: that needs every relay's Redis ACL user to
//! be granted it and blocks the write path for each fsync, where this only
//! delays the commit. `delay` is configurable per relay; 0 restores the
//! old commit-at-once behaviour (for a Redis on `appendfsync always`).

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use tokio::time::Instant;

/// `(topic, partition, offset)`: the last offset PROCESSED, as both relays
/// pass it to librdkafka's `store_offset` (which commits `offset + 1`).
pub type Offset = (String, i32, i64);

/// The default hold, in seconds: two `everysec` fsync intervals are the
/// usual worst case, but Redis postpones an AOF write for up to 2 s while a
/// slow fsync is still running and the production AOF reports such delayed
/// fsyncs every few hours (`aof_delayed_fsync`), so the margin is generous.
/// The cost is only the duplicates a relay crash re-reads.
pub const DEFAULT_DELAY_SECS: u64 = 10;

/// See the module docs.
#[derive(Debug)]
pub struct DurableOffsets {
    delay: Duration,
    /// Offsets in the order they were written, each with the instant it
    /// becomes due. Due instants only grow from front to back, except an
    /// entry [`Self::requeue`]d to the front, which is never earlier.
    pending: VecDeque<(Instant, Vec<Offset>)>,
}

impl DurableOffsets {
    #[must_use]
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            pending: VecDeque::new(),
        }
    }

    /// How long a write is held before its offset is committed.
    #[must_use]
    pub fn delay(&self) -> Duration {
        self.delay
    }

    /// Records `offsets` as written to Redis at `at`.
    pub fn written(&mut self, offsets: Vec<Offset>, at: Instant) {
        if offsets.is_empty() {
            return;
        }
        let due = at + self.delay;
        // `at` comes from the caller's clock and is never meant to go
        // backwards; clamp so the queue's order holds even if it does.
        let due = self.pending.back().map_or(due, |(last, _)| due.max(*last));
        self.pending.push_back((due, offsets));
    }

    /// When the oldest held offset becomes due, if any is held.
    #[must_use]
    pub fn next_due(&self) -> Option<Instant> {
        self.pending.front().map(|(due, _)| *due)
    }

    /// When the newest held offset becomes due, if any is held: once it
    /// has passed, everything held is durable.
    #[must_use]
    pub fn last_due(&self) -> Option<Instant> {
        self.pending.iter().map(|(due, _)| *due).max()
    }

    /// Whether nothing is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// How many offsets are held (one per written batch and partition).
    #[must_use]
    pub fn len(&self) -> usize {
        self.pending.iter().map(|(_, offsets)| offsets.len()).sum()
    }

    /// Removes and returns every offset due at `now`, merged to the highest
    /// per `(topic, partition)` (committing an offset commits everything
    /// before it). Stops at the first entry not yet due, so an offset is
    /// never returned ahead of an earlier write on the same partition.
    pub fn take_due(&mut self, now: Instant) -> Vec<Offset> {
        let mut merged: BTreeMap<(String, i32), i64> = BTreeMap::new();
        while let Some((due, _)) = self.pending.front() {
            if *due > now {
                break;
            }
            let Some((_, offsets)) = self.pending.pop_front() else {
                break;
            };
            for (topic, partition, offset) in offsets {
                merged
                    .entry((topic, partition))
                    .and_modify(|held| *held = (*held).max(offset))
                    .or_insert(offset);
            }
        }
        merged
            .into_iter()
            .map(|((topic, partition), offset)| (topic, partition, offset))
            .collect()
    }

    /// Removes and returns every held offset, due or not, merged as in
    /// [`Self::take_due`]. For a shutdown that has already waited out the
    /// delay.
    pub fn take_all(&mut self) -> Vec<Offset> {
        let Some(last) = self.last_due() else {
            return Vec::new();
        };
        self.take_due(last)
    }

    /// Puts offsets [`Self::take_due`] returned back at the front, due at
    /// `retry_at` (or later, if that is before an entry still held), after
    /// their store or commit failed.
    pub fn requeue(&mut self, offsets: Vec<Offset>, retry_at: Instant) {
        if offsets.is_empty() {
            return;
        }
        self.pending.push_front((retry_at, offsets));
    }
}

/// Where [`DurableOffsets::flush`] stores and commits offsets: the relay's
/// Kafka consumer.
pub trait OffsetSink {
    type Error;
    /// Stores `offset` for the next commit. `Ok(false)`: its partition is
    /// no longer assigned to this consumer, so it cannot be stored and is
    /// skipped (the partition's new owner re-reads from the last commit).
    fn store(&self, offset: &Offset) -> Result<bool, Self::Error>;
    /// Commits what is stored.
    fn commit(&self) -> Result<(), Self::Error>;
}

/// What one [`DurableOffsets::flush`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Flushed {
    /// Offsets stored and committed.
    pub stored: usize,
    /// Offsets skipped because their partition was revoked.
    pub revoked: Vec<Offset>,
}

impl DurableOffsets {
    /// Stores, then commits, the offsets durable at `now`. On a store
    /// failure that offset and the rest are put back, due at once, and the
    /// error returned (what was stored before it is still committed, best
    /// effort). A failed commit needs no requeue: stored offsets stay
    /// stored in librdkafka and go out with the next commit.
    pub fn flush<S: OffsetSink>(&mut self, sink: &S, now: Instant) -> Result<Flushed, S::Error> {
        let due = self.take_due(now);
        let mut flushed = Flushed::default();
        for (i, offset) in due.iter().enumerate() {
            match sink.store(offset) {
                Ok(true) => flushed.stored += 1,
                Ok(false) => flushed.revoked.push(offset.clone()),
                Err(err) => {
                    self.requeue(due[i..].to_vec(), now);
                    if flushed.stored > 0 {
                        // Best effort; the store error is the one to report.
                        let _ = sink.commit();
                    }
                    return Err(err);
                }
            }
        }
        if flushed.stored > 0 {
            sink.commit()?;
        }
        Ok(flushed)
    }
}

/// Parses `--kafka-commit-delay-secs`: the hold in seconds, at most an
/// hour (Kafka's `max.poll.interval.ms` and a rebalance re-read are both
/// far shorter; a longer hold only replays more after a crash).
pub fn parse_delay_secs(value: &str) -> Result<u64, String> {
    let secs: u64 = value
        .parse()
        .map_err(|err| format!("not a whole number of seconds: {err}"))?;
    if secs > MAX_DELAY_SECS {
        return Err(format!("at most {MAX_DELAY_SECS} seconds"));
    }
    Ok(secs)
}

/// The largest hold [`parse_delay_secs`] accepts.
pub const MAX_DELAY_SECS: u64 = 3600;

#[cfg(test)]
mod tests {
    use super::*;

    fn o(partition: i32, offset: i64) -> Offset {
        ("t".to_owned(), partition, offset)
    }

    #[test]
    fn nothing_is_due_before_the_delay_has_passed() {
        let start = Instant::now();
        let mut held = DurableOffsets::new(Duration::from_secs(10));
        held.written(vec![o(0, 5)], start);
        assert_eq!(held.next_due(), Some(start + Duration::from_secs(10)));
        assert_eq!(held.take_due(start + Duration::from_secs(9)), vec![]);
        assert_eq!(held.len(), 1);
        assert_eq!(
            held.take_due(start + Duration::from_secs(10)),
            vec![o(0, 5)]
        );
        assert!(held.is_empty());
        assert_eq!(held.next_due(), None);
    }

    #[test]
    fn only_the_due_prefix_is_taken_merged_per_partition() {
        let start = Instant::now();
        let mut held = DurableOffsets::new(Duration::from_secs(10));
        held.written(vec![o(0, 5), o(1, 7)], start);
        held.written(vec![o(0, 6)], start + Duration::from_secs(1));
        held.written(vec![o(0, 9)], start + Duration::from_secs(5));
        assert_eq!(
            held.take_due(start + Duration::from_secs(11)),
            vec![o(0, 6), o(1, 7)]
        );
        assert_eq!(held.len(), 1);
        assert_eq!(
            held.take_due(start + Duration::from_secs(15)),
            vec![o(0, 9)]
        );
    }

    #[test]
    fn a_zero_delay_is_due_at_once() {
        let start = Instant::now();
        let mut held = DurableOffsets::new(Duration::ZERO);
        held.written(vec![o(0, 1)], start);
        assert_eq!(held.take_due(start), vec![o(0, 1)]);
    }

    #[test]
    fn a_requeued_offset_is_retried_first_at_its_retry_time() {
        let start = Instant::now();
        let mut held = DurableOffsets::new(Duration::from_secs(10));
        held.written(vec![o(0, 1)], start);
        held.written(vec![o(0, 2)], start + Duration::from_secs(1));
        let due = held.take_due(start + Duration::from_secs(10));
        assert_eq!(due, vec![o(0, 1)]);
        held.requeue(due, start + Duration::from_secs(12));
        // Not before its retry time, and the later write waits behind it.
        assert_eq!(held.take_due(start + Duration::from_secs(11)), vec![]);
        assert_eq!(
            held.take_due(start + Duration::from_secs(12)),
            vec![o(0, 2)]
        );
    }

    #[test]
    fn take_all_ignores_the_delay() {
        let start = Instant::now();
        let mut held = DurableOffsets::new(Duration::from_secs(10));
        held.written(vec![o(0, 1)], start);
        held.written(vec![o(2, 4)], start + Duration::from_secs(3));
        assert_eq!(held.last_due(), Some(start + Duration::from_secs(13)));
        assert_eq!(held.take_all(), vec![o(0, 1), o(2, 4)]);
        assert!(held.is_empty());
    }

    /// A fake consumer: partition 9 is revoked, partition 13 fails to
    /// store.
    #[derive(Default)]
    struct Sink {
        stored: std::cell::RefCell<Vec<Offset>>,
        commits: std::cell::Cell<usize>,
    }

    impl OffsetSink for Sink {
        type Error = String;
        fn store(&self, offset: &Offset) -> Result<bool, String> {
            match offset.1 {
                9 => Ok(false),
                13 => Err("store failed".to_owned()),
                _ => {
                    self.stored.borrow_mut().push(offset.clone());
                    Ok(true)
                }
            }
        }
        fn commit(&self) -> Result<(), String> {
            self.commits.set(self.commits.get() + 1);
            Ok(())
        }
    }

    #[test]
    fn a_flush_stores_and_commits_only_what_is_durable() {
        let start = Instant::now();
        let sink = Sink::default();
        let mut held = DurableOffsets::new(Duration::from_secs(10));
        held.written(vec![o(0, 1), o(9, 3)], start);
        held.written(vec![o(0, 2)], start + Duration::from_secs(5));

        assert_eq!(
            held.flush(&sink, start + Duration::from_secs(9)),
            Ok(Flushed::default())
        );
        assert_eq!(sink.commits.get(), 0, "nothing stored, nothing committed");

        let flushed = held.flush(&sink, start + Duration::from_secs(10)).unwrap();
        assert_eq!(flushed.stored, 1);
        assert_eq!(flushed.revoked, vec![o(9, 3)]);
        assert_eq!(*sink.stored.borrow(), vec![o(0, 1)]);
        assert_eq!(sink.commits.get(), 1);
        assert_eq!(held.len(), 1);
    }

    #[test]
    fn a_failed_store_is_kept_for_the_next_flush() {
        let start = Instant::now();
        let sink = Sink::default();
        let mut held = DurableOffsets::new(Duration::ZERO);
        held.written(vec![o(0, 1), o(13, 2), o(14, 3)], start);

        assert_eq!(held.flush(&sink, start), Err("store failed".to_owned()));
        assert_eq!(*sink.stored.borrow(), vec![o(0, 1)]);
        assert_eq!(sink.commits.get(), 1, "what was stored is still committed");
        assert_eq!(
            held.take_due(start),
            vec![o(13, 2), o(14, 3)],
            "kept, due at once"
        );
    }

    #[test]
    fn the_delay_parses_within_bounds() {
        assert_eq!(parse_delay_secs("0"), Ok(0));
        assert_eq!(parse_delay_secs("10"), Ok(10));
        assert!(parse_delay_secs("3601").is_err());
        assert!(parse_delay_secs("-1").is_err());
    }
}
