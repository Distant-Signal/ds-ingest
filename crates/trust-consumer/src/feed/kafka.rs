//! Production `MovementFeed`: wraps `rdkafka`'s `StreamConsumer` against
//! RDM's Kafka Train Movements product. SASL_SSL is assumed (RDM's Kafka
//! products are described as SASL-authenticated in the design doc's
//! research; the exact mechanism is a startup-time GAP, see `config.rs`).

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;

use health_http::ConnectionState;

use super::MovementFeed;
use crate::config::Config;

/// How long to let librdkafka take over a `seek` before giving up on it.
/// `seek` is a local operation (it repositions this consumer's own fetch
/// position, it talks to no broker), so this is a generous upper bound on
/// something that normally returns immediately, not a network timeout.
const SEEK_TIMEOUT: Duration = Duration::from_secs(5);

pub struct KafkaMovementFeed {
    consumer: StreamConsumer,
    connection_state: ConnectionState,
    /// `(topic, partition, offset)` of the message the most recent
    /// successful `next_batch` returned, held until `commit` stores it (the
    /// caller confirmed the batch reached `api`).
    ///
    /// **A `Some` here at the top of `next_batch` means the previous cycle
    /// never confirmed** -- `commit` is the only thing that ever clears it
    /// -- so `next_batch` seeks back to that offset instead of reading on.
    /// See [`OffsetTracker`] for that rule and finding #3 of the 2026-09-25
    /// review for why it matters.
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
    last_received: OffsetTracker,
}

/// The offset bookkeeping half of `KafkaMovementFeed`, split out as a plain
/// value type so the ONE rule that matters -- **never advance past a record
/// whose cycle didn't confirm** -- is unit-testable without a broker, in the
/// same spirit as `main.rs` testing the commit rule against
/// `FakeMovementFeed`. The `seek`/`recv`/`store_offset` calls themselves
/// still need a real Kafka and are exercised only in deployment.
///
/// # Why this exists (finding #3 of the 2026-09-25 review)
///
/// `enable.auto.offset.store=false` already meant a failed cycle stored
/// nothing. But nothing made the consumer re-read the failed record either:
/// the next `recv()` simply returned the NEXT record, overwriting the held
/// offset, and the next successful `commit` stored that one -- which
/// librdkafka commits as "offset + 1", implicitly marking the skipped record
/// as processed too. A single transient `api` failure sandwiched between
/// successes therefore lost its whole batch permanently, silently. If that
/// batch held a pin's origin-departure Movement, nothing ever re-resolved
/// the pin: it sat `pending` in the database forever.
///
/// The fix is to treat "an offset is still held when the next read starts"
/// as proof the previous cycle failed, and `seek` back to it so the same
/// record is delivered again. Replay is safe: `trust_schema::dedup` keys
/// make the downstream write idempotent, and `process::ProcessorState`'s
/// journal rolls the in-memory maps back on the same failure (finding #4),
/// so the retry resolves identically rather than against half-applied state.
#[derive(Debug, Default, PartialEq, Eq)]
struct OffsetTracker {
    held: Option<(String, i32, i64)>,
}

impl OffsetTracker {
    /// The record `next_batch` just handed out, now awaiting confirmation.
    fn hold(&mut self, topic: String, partition: i32, offset: i64) {
        self.held = Some((topic, partition, offset));
    }

    /// Where the next `recv` must be repositioned to before reading, or
    /// `None` when the previous cycle confirmed (or there hasn't been one).
    /// Borrowed, not taken: the record is still unconfirmed after a seek --
    /// only a successful `commit` may release it.
    fn replay_target(&self) -> Option<(&str, i32, i64)> {
        self.held
            .as_ref()
            .map(|(topic, partition, offset)| (topic.as_str(), *partition, *offset))
    }

    /// The caller confirmed the batch and its offset has been stored and
    /// committed. Only now may the feed advance.
    fn release(&mut self) {
        self.held = None;
    }
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
            last_received: OffsetTracker::default(),
        })
    }

    /// Repositions this consumer back onto a record whose previous cycle
    /// never confirmed, so `recv` re-delivers it rather than the feed
    /// silently advancing past it (finding #3). No-op when there is nothing
    /// unconfirmed.
    ///
    /// A failed `seek` is logged and counted, NOT propagated: the realistic
    /// cause is a rebalance having taken this partition away, and returning
    /// `Err` here would fail every cycle from then on -- turning "one batch
    /// may be lost" into "this consumer stops consuming", which is strictly
    /// worse. Whatever happens, the offset stays held (only `commit`
    /// releases it), so the attempt repeats on the next cycle.
    fn replay_unconfirmed(&mut self) {
        let Some((topic, partition, offset)) = self.last_received.replay_target() else {
            return;
        };
        tracing::warn!(
            topic,
            partition,
            offset,
            "the previous cycle never confirmed this record; seeking back to re-deliver it \
             rather than advancing past it"
        );
        if let Err(err) = self.consumer.seek(
            topic,
            partition,
            rdkafka::Offset::Offset(offset),
            SEEK_TIMEOUT,
        ) {
            tracing::error!(
                error = ?err,
                topic,
                partition,
                offset,
                "failed to seek back to an unconfirmed record; it may be skipped. Continuing \
                 rather than failing the consumer outright -- the seek is retried next cycle"
            );
            metrics::counter!(
                common::metrics::metric_name("trust_consumer_errors_total"),
                "operation" => "seek_unconfirmed_offset"
            )
            .increment(1);
        }
    }
}

#[async_trait]
impl MovementFeed for KafkaMovementFeed {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        // Before reading anything new: if the previous cycle's record is
        // still unconfirmed, go back and read it again (finding #3).
        self.replay_unconfirmed();

        match self.consumer.recv().await {
            Ok(message) => {
                health_http::set_connected(&self.connection_state, "trust_consumer_ready", true);
                let payload = message
                    .payload()
                    .ok_or_else(|| anyhow::anyhow!("empty Kafka message payload"))?;
                let batch = String::from_utf8_lossy(payload).into_owned();
                // Recorded only once the payload is in hand: an empty
                // payload returns `Err` above, and an errored batch must
                // not leave an offset behind for `commit` to store.
                self.last_received.hold(
                    message.topic().to_string(),
                    message.partition(),
                    message.offset(),
                );
                Ok(vec![batch])
            }
            Err(err) => {
                health_http::set_connected(&self.connection_state, "trust_consumer_ready", false);
                Err(err.into())
            }
        }
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        // Nothing received since the last commit -- there is no offset to
        // advance, so this is a no-op rather than a redundant broker
        // round-trip.
        let Some((topic, partition, offset)) = self.last_received.replay_target() else {
            return Ok(());
        };

        // Store first, then commit: with `enable.auto.offset.store=false`
        // this is the only thing that ever marks an offset committable, so
        // it happens here -- on the caller's confirmation of success --
        // and nowhere else. `commit_consumer_state` then writes whatever
        // is stored across the whole assignment.
        self.consumer.store_offset(topic, partition, offset)?;
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Async)?;

        // Released only once both steps succeeded, so a failed store or
        // commit leaves the offset in hand -- which now also means the next
        // `next_batch` seeks back to it rather than reading on past it.
        self.last_received.release();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The invariant finding #3 is about, stated over the bookkeeping that
    /// decides it: an unconfirmed record is replayed, a confirmed one is not.
    /// Constructing a real `KafkaMovementFeed` needs a broker, so this tests
    /// the rule rather than the `seek` syscall -- exactly the split
    /// `main.rs`'s own commit-rule tests already use.
    #[test]
    fn an_unheld_tracker_has_nothing_to_replay() {
        let tracker = OffsetTracker::default();
        assert_eq!(tracker.replay_target(), None);
    }

    #[test]
    fn a_received_record_is_replayed_until_it_is_confirmed() {
        let mut tracker = OffsetTracker::default();
        tracker.hold("movement-events".to_string(), 3, 4242);
        assert_eq!(
            tracker.replay_target(),
            Some(("movement-events", 3, 4242)),
            "a record whose cycle has not confirmed must be re-delivered, not skipped"
        );
        // Still held after the seek -- a seek is not a confirmation.
        assert_eq!(tracker.replay_target(), Some(("movement-events", 3, 4242)));

        tracker.release();
        assert_eq!(
            tracker.replay_target(),
            None,
            "once committed, the feed may finally advance"
        );
    }

    /// The exact shape of the permanent data loss finding #3 describes: a
    /// failed cycle followed by a successful one. Before the fix the second
    /// `recv` overwrote the held offset and its commit stored `offset + 1`,
    /// implicitly marking the failed record processed; now the failed record
    /// is what the next read is positioned back onto, so the offset that
    /// eventually commits is that record's own.
    #[test]
    fn a_failed_cycle_does_not_let_the_next_record_commit_past_it() {
        let mut tracker = OffsetTracker::default();
        tracker.hold("movement-events".to_string(), 0, 100);
        // Cycle fails: no `release`. The next read must be repositioned.
        let replay = tracker.replay_target();
        assert_eq!(replay, Some(("movement-events", 0, 100)));
        // The redelivered record is the same one, so holding it again is a
        // no-op in effect.
        tracker.hold("movement-events".to_string(), 0, 100);
        assert_eq!(tracker.replay_target(), Some(("movement-events", 0, 100)));
        // It only advances once that record itself is confirmed.
        tracker.release();
        tracker.hold("movement-events".to_string(), 0, 101);
        assert_eq!(tracker.replay_target(), Some(("movement-events", 0, 101)));
    }
}
