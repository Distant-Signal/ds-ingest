//! Raw Kafka source for movement-relay. Structurally close to
//! `trust-consumer/src/feed/kafka.rs` (same ClientConfig shape, same
//! store-then-commit offset discipline) but deliberately NOT shared via
//! `crates/movement-feed` -- see
//! docs/superpowers/plans/2026-09-04-movement-relay-plan.md Task 6 for why:
//! `movement-feed` is scoped to the two downstream Redis Streams *readers*,
//! while this crate is a raw-Kafka-payload consumer / Redis *producer*, a
//! structurally different role (Decision 3's own tree sketch: "movement-relay's
//! OWN Kafka consume loop does NOT depend on this crate"). Do not "fix" this
//! duplication by merging it into `movement-feed` -- it is a small,
//! deliberate exception to this repo's usual DRY instinct, justified by
//! crate-boundary purity. `trust-consumer`'s own copy is deleted in Deploy C
//! (Task 13); this crate's copy is permanent.
//!
//! Returns RAW record payloads (unclassified) -- classification into
//! confirmed/unknown message types happens in `main.rs` via
//! `trust_schema::schema::confirmed_envelope_bodies`, not here.

use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;

use crate::config::Config;
use crate::health::RelayContext;

#[async_trait]
pub trait RawKafkaSource: Send {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>>;

    /// Hands a batch whose downstream write failed back to the source, so
    /// the NEXT `next_batch` call re-delivers exactly that batch instead of
    /// fetching anything new.
    ///
    /// This exists because `recv()`-then-`store_offset()` is a
    /// receive/confirm split with NO rollback of its own: every
    /// `consumer.recv()` advances librdkafka's own fetch position and
    /// overwrites `last_received`, whether or not the previously received
    /// record was ever confirmed. Without this hand-back, a cycle that
    /// returned `Cycle::Failed` because a downstream XADD failed would have
    /// its record silently skipped -- the next cycle's `recv()` moves the
    /// fetch position past it, and the next SUCCESSFUL `commit()` stores
    /// that later offset, implicitly committing past the record that never
    /// made it into `movement-events`. During a Redis outage that is one
    /// permanently dropped record per retry cycle, from the single feed
    /// `trust-consumer`, `full-coverage-consumer` and `trust-event-backlog`
    /// all read.
    ///
    /// Retaining the batch in memory (rather than `seek()`ing the consumer
    /// back to `last_received`) keeps the re-delivery synchronous and
    /// independent of librdkafka's asynchronous seek/fetch-position
    /// machinery: the payload we already have is exactly the payload we
    /// need, and `last_received` still points at it, so a later successful
    /// `commit()` stores the right offset.
    fn retain_for_retry(&mut self, batch: Vec<String>);

    async fn commit(&mut self) -> anyhow::Result<()>;
}

pub struct KafkaRawSource {
    consumer: StreamConsumer<RelayContext>,
    /// `(topic, partition, offset)` of the message the most recent
    /// successful `next_batch` returned, held until `commit` either stores
    /// it or it's replaced by the next received message -- same
    /// receive/confirm split `trust-consumer/src/feed/kafka.rs`'s
    /// `KafkaMovementFeed::last_received` already established.
    last_received: Option<(String, i32, i64)>,
    /// Batches handed back by `retain_for_retry` because their downstream
    /// write failed, in redelivery order (front = next to redeliver). While
    /// this is non-empty, `next_batch` re-delivers its front entry instead
    /// of fetching anything new via a real `consumer.recv()` -- `last_received`
    /// must keep pointing at that entry's own offset until it is either
    /// committed or handed back again.
    ///
    /// A `VecDeque` rather than a single slot because of the keepalive poll
    /// `next_batch` performs while this is non-empty (see that function's
    /// own doc, and `paused`'s): partitions are paused for the whole time
    /// there is a retained batch, so that poll should never surface a real
    /// message, but librdkafka may still have a few messages already
    /// buffered locally from just before the pause took effect. Rather than
    /// silently dropping one of those (this pipeline's whole point is that
    /// no movement is ever silently dropped), it is queued behind whatever
    /// is already retained and delivered once its turn comes, in the same
    /// order Kafka produced it.
    pending_retry: VecDeque<PendingRecord>,
    /// Whether this consumer's assigned partitions are currently paused
    /// because `pending_retry` holds (or recently held) a batch awaiting
    /// redelivery. Paused for the WHOLE time `pending_retry` is non-empty --
    /// set by `retain_for_retry` the moment it first has something to
    /// retain, cleared by `commit` once a successful commit leaves
    /// `pending_retry` empty again -- so the keepalive `consumer.recv()`
    /// call `next_batch` makes while retrying can never race a genuinely
    /// NEW message ahead of the batch(es) still waiting to be redelivered:
    /// with fetching paused, `recv()` cannot return a new message from the
    /// broker, only (rarely) one already buffered locally beforehand, which
    /// `next_batch` queues rather than drops -- see `pending_retry`'s own
    /// doc.
    paused: bool,
}

/// One retained-or-incidentally-received Kafka record awaiting redelivery,
/// paired with the offset it must eventually be committed under.
struct PendingRecord {
    batch: Vec<String>,
    offset: (String, i32, i64),
}

/// Upper bound on the keepalive `consumer.recv()` call `next_batch` makes
/// while redelivering a retained batch. It only needs to touch `recv()`
/// once to service librdkafka's own `max.poll.interval.ms` liveness check
/// (see `MAX_POLL_INTERVAL_MS`'s own doc for why that budget exists and
/// what it is sized for) -- not actually wait for a message, since
/// partitions are paused for the whole retry window (see `paused`'s doc).
/// Short enough that it never meaningfully delays redelivery of the
/// retained batch relative to `movement-relay::main::ERROR_BACKOFF`, the
/// flat backoff between retry cycles this keepalive call rides along with.
const KEEPALIVE_POLL_TIMEOUT: Duration = Duration::from_millis(200);

/// Worst-case time `run_cycle` can spend doing inline downstream work
/// between one `consumer.recv()` and the next, so librdkafka's own
/// `max.poll.interval.ms` liveness timeout is set to comfortably outlast it
/// instead of quietly relying on the library's raw 300_000ms (5 minute)
/// default being enough.
///
/// **The shape of the risk**: `next_batch` fetches exactly ONE Kafka record
/// per call, but that record's body is a JSON array of individual TRUST
/// envelopes -- `main.rs::publish_batch` then issues one synchronous Redis
/// `XADD` per surviving envelope, sequentially, with no batching, all before
/// `run_cycle` returns control to the outer loop and `next_batch` (and so
/// `consumer.recv()`, which is this consumer's only "I'm alive" signal to
/// librdkafka) is called again. A real TRUST batch has been observed
/// containing on the order of ~218 envelopes in a single Kafka record. At a
/// (deliberately pessimistic, not average-case) 2 seconds per XADD under a
/// genuinely degraded-but-not-fully-down Redis -- a full outage instead
/// fails each XADD fast and is handled by `retain_for_retry`, not this path
/// -- that is ~436 seconds (~7.3 minutes) of inline work with no intervening
/// poll, which already exceeds librdkafka's stock 300_000ms
/// `max.poll.interval.ms`. Once that timeout is exceeded mid-batch, the
/// broker considers this consumer dead and triggers a group rebalance,
/// which both interrupts the in-flight batch and briefly stops movement
/// data from flowing to every downstream consumer group.
///
/// 900_000ms (15 minutes) is set explicitly here -- roughly double the
/// pessimistic worst-case estimate above -- so there is headroom for an even
/// slower downstream without depending on the library default happening to
/// be enough, and so the reasoning is visible next to the value rather than
/// left implicit.
const MAX_POLL_INTERVAL_MS: &str = "900000";

impl KafkaRawSource {
    /// Split out from `connect` so the config values themselves --
    /// `max.poll.interval.ms` in particular -- are unit-testable without a
    /// broker: `ClientConfig::set`/`get` only ever touch an in-memory map,
    /// and only `create_with_context` below actually talks to librdkafka.
    fn client_config(config: &Config) -> ClientConfig {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.kafka.kafka_brokers)
            .set("group.id", &config.kafka_consumer_group)
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanisms", &config.kafka.kafka_sasl_mechanism)
            .set("sasl.username", &config.kafka.kafka_sasl_username)
            .set("sasl.password", &config.kafka.kafka_sasl_password)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("max.poll.interval.ms", MAX_POLL_INTERVAL_MS);
        client_config
    }

    /// Readiness is entirely owned by `RelayContext`'s rebalance callback
    /// (Task 5), NOT by an `Err` path in this module's own `next_batch` the
    /// way `trust-consumer`'s `KafkaMovementFeed` uses its
    /// `connection_state` flag -- the one structural divergence from that
    /// crate's copy beyond the return-shape difference.
    pub fn connect(config: &Config, ready: health_http::ConnectionState) -> anyhow::Result<Self> {
        let context = RelayContext { ready };
        let consumer: StreamConsumer<RelayContext> =
            Self::client_config(config).create_with_context(context)?;

        consumer.subscribe(&[&config.kafka.kafka_topic])?;

        Ok(Self {
            consumer,
            last_received: None,
            pending_retry: VecDeque::new(),
            paused: false,
        })
    }

    /// Pauses fetching on this consumer's CURRENT partition assignment --
    /// best-effort, logged rather than propagated, since a failure here
    /// should not itself abort the retry it's meant to protect (worst case
    /// without it: the narrow already-buffered-message race `pending_retry`
    /// already tolerates becomes more likely, not a new failure mode).
    fn pause_assigned_partitions(&self) {
        match self.consumer.assignment() {
            Ok(assignment) => {
                if let Err(err) = self.consumer.pause(&assignment) {
                    tracing::warn!(error = ?err, "failed to pause Kafka partitions for a Redis retry; a new message could race the retained batch");
                }
            }
            Err(err) => {
                tracing::warn!(error = ?err, "failed to read Kafka partition assignment to pause for a Redis retry");
            }
        }
    }

    /// Resumes fetching on this consumer's current partition assignment --
    /// same best-effort posture as `pause_assigned_partitions`: a failure
    /// here means this consumer stays paused (a stall, caught by the
    /// existing stream-lag alerting) rather than something worse.
    fn resume_assigned_partitions(&self) {
        match self.consumer.assignment() {
            Ok(assignment) => {
                if let Err(err) = self.consumer.resume(&assignment) {
                    tracing::warn!(error = ?err, "failed to resume Kafka partitions after a successful Redis retry commit");
                }
            }
            Err(err) => {
                tracing::warn!(error = ?err, "failed to read Kafka partition assignment to resume after a successful Redis retry commit");
            }
        }
    }
}

#[async_trait]
impl RawKafkaSource for KafkaRawSource {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        // A previously-received batch whose downstream write failed is
        // re-delivered before anything new is fetched: a real `recv()` here
        // would advance the fetch position past it and overwrite
        // `last_received`, which is exactly how such a record used to be
        // dropped.
        if let Some(record) = self.pending_retry.pop_front() {
            // M5 (Repeater Signal review): this branch used to return
            // immediately, never touching `consumer.recv()` at all. Every
            // cycle spent here (which, across a long Redis outage, is EVERY
            // cycle -- `main::run_cycle` retries every
            // `main::ERROR_BACKOFF`, and each retry lands right back in
            // this branch as long as the batch keeps failing to publish)
            // meant librdkafka's own liveness check
            // (`max.poll.interval.ms`, `MAX_POLL_INTERVAL_MS` above --
            // sized only for inline XADD work, 900s) went unserviced. A
            // Redis outage longer than that budget could then ALSO trigger
            // a Kafka consumer-group rebalance, on top of the outage
            // itself, for no reason related to Kafka at all.
            //
            // This keepalive call fixes that: partitions are paused for
            // the whole time `pending_retry` is non-empty (see `paused`'s
            // own doc, set by `retain_for_retry` below), so `recv()` cannot
            // return a genuinely NEW message from the broker -- it can
            // only touch librdkafka's internal queue-poll (which is what
            // actually resets the liveness timer, on every call, whether or
            // not anything was found) and, rarely, surface a message that
            // was already buffered locally an instant before the pause took
            // effect. That rare case is queued behind whatever is already
            // retained (same order Kafka produced it in) rather than
            // dropped -- see `pending_retry`'s own doc for why silently
            // dropping it is not an acceptable trade merely to service a
            // liveness check.
            match tokio::time::timeout(KEEPALIVE_POLL_TIMEOUT, self.consumer.recv()).await {
                Ok(Ok(message)) => {
                    let offset = (
                        message.topic().to_string(),
                        message.partition(),
                        message.offset(),
                    );
                    match message.payload() {
                        Some(payload) => {
                            let payload = String::from_utf8_lossy(payload).into_owned();
                            tracing::warn!(
                                ?offset,
                                "keepalive Kafka poll during a Redis retry unexpectedly received \
                                 a message despite paused partitions (a message already buffered \
                                 locally before the pause took effect); queuing it behind the \
                                 retained batch rather than dropping it"
                            );
                            self.pending_retry.push_back(PendingRecord {
                                batch: vec![payload],
                                offset,
                            });
                        }
                        None => {
                            tracing::error!(
                                ?offset,
                                "keepalive Kafka poll during a Redis retry received a message with \
                                 an empty payload; dropping it (same as an ordinary empty-payload \
                                 message, which `next_batch`'s normal path treats as unrecoverable)"
                            );
                        }
                    }
                }
                Ok(Err(err)) => {
                    tracing::warn!(error = ?err, "keepalive Kafka poll during a Redis retry failed; will retry next cycle");
                }
                Err(_timeout_elapsed) => {
                    // Expected common case: partitions are paused, so there
                    // is nothing to receive. The `recv()` call above still
                    // ran and touched librdkafka's queue poll before this
                    // timeout fired, which is all this keepalive needs to
                    // do.
                }
            }

            self.last_received = Some(record.offset);
            return Ok(record.batch);
        }
        let message = self.consumer.recv().await?;
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

    fn retain_for_retry(&mut self, batch: Vec<String>) {
        let offset = self
            .last_received
            .take()
            .expect("retain_for_retry is only ever called with a batch next_batch just returned, and next_batch always records that batch's offset in last_received before returning it");
        self.pending_retry
            .push_front(PendingRecord { batch, offset });
        if !self.paused {
            self.pause_assigned_partitions();
            self.paused = true;
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
        // Only safe to resume once nothing is left awaiting redelivery --
        // `pending_retry` can still hold a keepalive-caught message even
        // after the originally retained batch commits (see `next_batch`'s
        // own doc).
        if self.paused && self.pending_retry.is_empty() {
            self.resume_assigned_partitions();
            self.paused = false;
        }
        Ok(())
    }
}

#[cfg(test)]
fn test_config() -> Config {
    Config {
        kafka: common::service_args::KafkaConnectionArgs {
            kafka_brokers: "kafka.example:9094".to_string(),
            kafka_topic: "test-topic".to_string(),
            kafka_sasl_username: "user".to_string(),
            kafka_sasl_password: "pass".to_string(),
            kafka_sasl_mechanism: "PLAIN".to_string(),
        },
        kafka_consumer_group: "test-group".to_string(),
        redis_url: "redis://localhost:6379".to_string(),
        health_bind_url: "0.0.0.0:8083".to_string(),
        metrics_port: 9094,
        metrics_enabled: false,
        stream_lag_poll_secs: 30,
    }
}

/// Regression test for the Signal Box Audit's "no poll-interval override"
/// finding: no `max.poll.interval.ms` override meant this consumer relied
/// entirely on librdkafka's stock 300_000ms default, which
/// `MAX_POLL_INTERVAL_MS`'s own doc comment shows a single slow-downstream
/// batch can plausibly exceed.
#[test]
fn max_poll_interval_is_configured_with_headroom_over_the_librdkafka_default() {
    let config = test_config();
    let client_config = KafkaRawSource::client_config(&config);

    assert_eq!(
        client_config.get("max.poll.interval.ms"),
        Some(MAX_POLL_INTERVAL_MS),
        "must be set explicitly, not left to librdkafka's 300_000ms default"
    );

    let configured_ms: u64 = client_config
        .get("max.poll.interval.ms")
        .unwrap()
        .parse()
        .unwrap();
    const LIBRDKAFKA_DEFAULT_MS: u64 = 300_000;
    assert!(
        configured_ms > LIBRDKAFKA_DEFAULT_MS,
        "the override must be more generous than the default it replaces"
    );
}

/// Test double modelling librdkafka's real position semantics, not a
/// forgiving abstraction over them: every `next_batch` that actually
/// fetches advances an internal fetch position by one record and
/// OVERWRITES `last_received`, and `commit` stores only whatever
/// `last_received` currently holds. That is what makes the
/// dropped-record failure this fake is used to test reproducible at all --
/// a `run_cycle` that fails a downstream write and does not hand its batch
/// back will see the next cycle fetch record N+1 and then commit N+1's
/// offset, silently skipping N.
#[cfg(test)]
pub struct FakeRawSource {
    /// Records not yet fetched, in offset order -- one element per Kafka
    /// record. An empty element models a poll that returned nothing.
    batches: std::collections::VecDeque<Vec<String>>,
    /// The offset the next fetched record will carry.
    next_offset: i64,
    /// Offset of the most recently fetched record, cleared by a successful
    /// `commit` -- the fake's stand-in for `KafkaRawSource::last_received`.
    last_received: Option<i64>,
    pending_retry: Option<Vec<String>>,
    /// When true, the next `commit` call fails (and leaves
    /// `last_received` in place, exactly as the real `store_offset`/
    /// `commit_consumer_state` error path does).
    pub fail_next_commit: bool,
    pub committed_count: usize,
    /// Every offset a successful `commit` stored, in order -- so a test can
    /// assert WHICH record's offset was committed, not merely how many
    /// commits happened.
    pub committed_offsets: Vec<i64>,
}

#[cfg(test)]
impl FakeRawSource {
    pub fn new(batches: Vec<Vec<String>>) -> Self {
        Self {
            batches: batches.into(),
            next_offset: 0,
            last_received: None,
            pending_retry: None,
            fail_next_commit: false,
            committed_count: 0,
            committed_offsets: Vec::new(),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl RawKafkaSource for FakeRawSource {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        if let Some(batch) = self.pending_retry.take() {
            return Ok(batch);
        }
        let batch = self.batches.pop_front().unwrap_or_default();
        if !batch.is_empty() {
            self.last_received = Some(self.next_offset);
            self.next_offset += 1;
        }
        Ok(batch)
    }

    fn retain_for_retry(&mut self, batch: Vec<String>) {
        self.pending_retry = Some(batch);
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        if self.fail_next_commit {
            self.fail_next_commit = false;
            return Err(anyhow::anyhow!("simulated offset commit failure"));
        }
        let Some(offset) = self.last_received else {
            return Ok(());
        };
        self.committed_offsets.push(offset);
        self.committed_count += 1;
        self.last_received = None;
        Ok(())
    }
}
