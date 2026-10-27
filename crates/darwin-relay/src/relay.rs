//! One relay cycle: read Kafka records into a batch for up to
//! `batch_max_secs` (from its first record) or `batch_max_records`, decode
//! each ([`crate::pushport`]), merge them ([`Batch::merge`]), produce the
//! batch on `ds:ingest:darwin` and wait until Redis has it, then commit the
//! records' offsets.
//!
//! **Nothing is committed before it is in Redis.** The sink is an
//! [`ingest_stream::Producer`] with the event policy: it buffers and
//! retries a Redis outage for as long as it lasts, and resolves its
//! receipt once the entry is written. A sink error (the producer closed,
//! or a batch that cannot be encoded) ends the process instead of
//! committing past the batch; the restart re-reads it from the last
//! commit. A record that cannot be decoded is permanent for its bytes: it
//! is logged, counted and committed past, as movement-relay does.
//!
//! **Duplicates** (a re-read after a restart or a rebalance) are harmless:
//! every stored row is guarded by its Darwin timestamp.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use common::darwin::Batch;
use ingest_stream::{EncodedEntry, Envelope, EnvelopeError, SchemaId};

use crate::kafka::{Record, RecordSource};
use crate::metrics;
use crate::pushport::{self, Decoded};

/// Where a batch goes; [`ProducerSink`] in production, a fake in tests.
#[async_trait]
pub(crate) trait BatchSink: Send {
    /// Writes `batch` under idempotency key `key`, returning once it is in
    /// Redis.
    async fn publish(&mut self, key: &str, batch: &Batch) -> anyhow::Result<()>;
}

/// What one cycle did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Cycle {
    /// Records were read (and, if any held rows, published) and committed.
    Committed { records: usize, rows: usize },
}

/// Limits of one batch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub max_age: Duration,
    pub max_records: usize,
}

#[derive(Default)]
struct Accumulator {
    batch: Batch,
    /// The highest offset read per `(topic, partition)`.
    offsets: BTreeMap<(String, i32), (i64, i64)>,
    records: usize,
    newest: Option<DateTime<Utc>>,
}

impl Accumulator {
    fn add(&mut self, record: &Record) {
        self.records += 1;
        self.offsets
            .entry((record.topic.clone(), record.partition))
            .and_modify(|(first, last)| {
                *first = (*first).min(record.offset);
                *last = (*last).max(record.offset);
            })
            .or_insert((record.offset, record.offset));
        match pushport::decode_record(&record.payload) {
            Ok(Decoded::Document(batch)) => {
                metrics::record("document");
                self.newest = self.newest.max(batch.newest_ts());
                self.batch.merge(batch);
            }
            Ok(Decoded::Heartbeat) => metrics::record("heartbeat"),
            Ok(Decoded::Xml) => {
                metrics::record("xml");
                tracing::warn!(
                    partition = record.partition,
                    offset = record.offset,
                    "skipping an XML Push Port record: this relay reads the JSON topic"
                );
            }
            Err(err) => {
                metrics::record("malformed");
                tracing::error!(
                    partition = record.partition,
                    offset = record.offset,
                    error = %err,
                    bytes = record.payload.len(),
                    "skipping a Push Port record that cannot be decoded"
                );
            }
        }
    }

    /// The batch's idempotency key: the schema, then each partition's
    /// offset range (`darwin-pushport:0@120-180`).
    fn key(&self) -> String {
        let ranges: Vec<String> = self
            .offsets
            .iter()
            .map(|((_, partition), (first, last))| format!("{partition}@{first}-{last}"))
            .collect();
        format!("{}:{}", common::darwin::SCHEMA, ranges.join(","))
    }

    fn commit_offsets(&self) -> Vec<(String, i32, i64)> {
        self.offsets
            .iter()
            .map(|((topic, partition), (_, last))| (topic.clone(), *partition, *last))
            .collect()
    }
}

/// One cycle. See the module docs.
pub(crate) async fn run_cycle<S: RecordSource, K: BatchSink>(
    source: &mut S,
    sink: &mut K,
    limits: Limits,
) -> anyhow::Result<Cycle> {
    let mut acc = Accumulator::default();
    // The first record may take as long as the topic is quiet.
    if let Some(record) = source.recv(None).await? {
        acc.add(&record);
    }
    let deadline = tokio::time::Instant::now() + limits.max_age;
    while acc.records > 0 && acc.records < limits.max_records {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        match source.recv(Some(left)).await {
            Ok(Some(record)) => acc.add(&record),
            Ok(None) => break,
            // The records already read are good: publish and commit them,
            // and let the next cycle retry the receive.
            Err(err) => {
                tracing::warn!(error = ?err, "Kafka receive failed mid-batch; publishing what was read");
                metrics::error("kafka");
                break;
            }
        }
    }
    let rows = acc.batch.len();
    if !acc.batch.is_empty() {
        sink.publish(&acc.key(), &acc.batch).await?;
        metrics::batch_published();
        metrics::rows("services", acc.batch.services.len());
        metrics::rows("schedule_calls", acc.batch.schedule_calls.len());
        metrics::rows("status_calls", acc.batch.status_calls.len());
        metrics::rows("associations", acc.batch.associations.len());
        metrics::rows("deactivated", acc.batch.deactivated.len());
    }
    if let Some(newest) = acc.newest {
        metrics::last_message(newest);
    }
    if acc.records > 0 {
        source.commit(&acc.commit_offsets())?;
    }
    Ok(Cycle::Committed {
        records: acc.records,
        rows,
    })
}

/// Encodes `batch` as one entry, or, when it is over the 512 KiB entry cap
/// after gzip, as halves (recursively), keyed `<key>/<i>`.
pub(crate) fn encode(
    schema: &SchemaId,
    producer: &str,
    key: &str,
    produced_at: DateTime<Utc>,
    batch: &Batch,
) -> anyhow::Result<Vec<EncodedEntry>> {
    let entry = Envelope::new(schema.clone(), producer, key, produced_at, batch)?;
    match entry.encode() {
        Ok(encoded) => Ok(vec![encoded]),
        Err(EnvelopeError::TooLarge { .. }) if batch.len() > 1 => {
            let (a, b) = split(batch);
            let mut parts = encode(schema, producer, &format!("{key}/1"), produced_at, &a)?;
            parts.extend(encode(
                schema,
                producer,
                &format!("{key}/2"),
                produced_at,
                &b,
            )?);
            Ok(parts)
        }
        Err(err) => Err(err.into()),
    }
}

/// Each kind's rows halved. Keys stay unique per half, so each half is a
/// valid batch on its own.
fn split(batch: &Batch) -> (Batch, Batch) {
    fn halves<T: Clone>(rows: &[T]) -> (Vec<T>, Vec<T>) {
        let (a, b) = rows.split_at(rows.len() / 2);
        (a.to_vec(), b.to_vec())
    }
    let (sa, sb) = halves(&batch.services);
    let (ca, cb) = halves(&batch.schedule_calls);
    let (ta, tb) = halves(&batch.status_calls);
    let (aa, ab) = halves(&batch.associations);
    let (da, db) = halves(&batch.deactivated);
    (
        Batch {
            services: sa,
            schedule_calls: ca,
            status_calls: ta,
            associations: aa,
            deactivated: da,
        },
        Batch {
            services: sb,
            schedule_calls: cb,
            status_calls: tb,
            associations: ab,
            deactivated: db,
        },
    )
}

/// The production sink: `ds:ingest:darwin` through an event-policy
/// producer.
pub(crate) struct ProducerSink {
    pub producer: ingest_stream::Producer,
    pub schema: SchemaId,
    pub producer_id: String,
    /// Waiting out a Redis outage is the process alive and retrying, not
    /// a stall.
    pub progress: health_http::Progress,
}

#[async_trait]
impl BatchSink for ProducerSink {
    async fn publish(&mut self, key: &str, batch: &Batch) -> anyhow::Result<()> {
        let parts = encode(&self.schema, &self.producer_id, key, Utc::now(), batch)?;
        let receipt = self.progress.idle(self.producer.submit(parts)).await?;
        self.progress.idle(receipt.written()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    struct FakeSource {
        records: VecDeque<Record>,
        committed: Vec<Vec<(String, i32, i64)>>,
    }

    #[async_trait]
    impl RecordSource for FakeSource {
        async fn recv(&mut self, timeout: Option<Duration>) -> anyhow::Result<Option<Record>> {
            match self.records.pop_front() {
                Some(record) => Ok(Some(record)),
                None if timeout.is_some() => Ok(None),
                None => anyhow::bail!("no more records (the real source would wait)"),
            }
        }

        fn commit(&mut self, offsets: &[(String, i32, i64)]) -> anyhow::Result<()> {
            self.committed.push(offsets.to_vec());
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeSink {
        published: Vec<(String, Batch)>,
        fail: bool,
    }

    #[async_trait]
    impl BatchSink for FakeSink {
        async fn publish(&mut self, key: &str, batch: &Batch) -> anyhow::Result<()> {
            if self.fail {
                anyhow::bail!("producer closed");
            }
            self.published.push((key.to_owned(), batch.clone()));
            Ok(())
        }
    }

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures")
                .join(name),
        )
        .expect("fixture")
    }

    fn record(offset: i64, payload: Vec<u8>) -> Record {
        Record {
            topic: "t".to_owned(),
            partition: 0,
            offset,
            payload,
        }
    }

    const LIMITS: Limits = Limits {
        max_age: Duration::from_secs(5),
        max_records: 3,
    };

    #[tokio::test]
    async fn a_batch_is_published_before_its_offsets_commit() {
        let mut source = FakeSource {
            records: VecDeque::from([
                record(10, fixture("schedule.json")),
                record(11, fixture("heartbeat.json")),
                record(12, fixture("ts.json")),
                record(13, fixture("association.json")),
            ]),
            committed: Vec::new(),
        };
        let mut sink = FakeSink::default();
        let cycle = run_cycle(&mut source, &mut sink, LIMITS)
            .await
            .expect("cycle");
        assert!(matches!(cycle, Cycle::Committed { records: 3, .. }));
        assert_eq!(sink.published.len(), 1);
        let (key, batch) = &sink.published[0];
        assert_eq!(key, "darwin-pushport:0@10-12");
        // The schedule and the TS for the same RID merged to one service
        // that kept its schedule fields.
        assert_eq!(batch.services.len(), 1);
        assert!(batch.services[0].schedule.is_some());
        assert_eq!(source.committed, vec![vec![("t".to_owned(), 0, 12)]]);
        // The fourth record waits for the next cycle.
        assert_eq!(source.records.len(), 1);
    }

    #[tokio::test]
    async fn a_failed_publish_commits_nothing() {
        let mut source = FakeSource {
            records: VecDeque::from([record(10, fixture("ts.json"))]),
            committed: Vec::new(),
        };
        let mut sink = FakeSink {
            fail: true,
            ..FakeSink::default()
        };
        assert!(run_cycle(&mut source, &mut sink, LIMITS).await.is_err());
        assert!(source.committed.is_empty());
    }

    #[tokio::test]
    async fn records_with_nothing_to_keep_are_committed_without_a_publish() {
        let mut source = FakeSource {
            records: VecDeque::from([
                record(10, fixture("heartbeat.json")),
                record(11, b"not json".to_vec()),
                record(12, b"<Pport/>".to_vec()),
            ]),
            committed: Vec::new(),
        };
        let mut sink = FakeSink::default();
        run_cycle(&mut source, &mut sink, LIMITS)
            .await
            .expect("cycle");
        assert!(sink.published.is_empty());
        assert_eq!(source.committed, vec![vec![("t".to_owned(), 0, 12)]]);
    }

    #[test]
    fn an_oversized_batch_is_split_into_entries_that_fit() {
        let mut batch = Batch::default();
        let Decoded::Document(ts) = pushport::decode_record(&fixture("ts.json")).expect("ts")
        else {
            panic!("a document");
        };
        // Enough calls with pseudo-random keys to pass 512 KiB gzipped.
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        let mut noise = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            format!("{state:016x}")
        };
        for _ in 0..30_000 {
            let mut call = ts.status_calls[0].clone();
            call.key.rid = format!("{}{}", noise(), noise());
            call.key.tiploc = noise();
            batch.status_calls.push(call);
        }
        let schema = SchemaId::new(common::darwin::SCHEMA, 1).expect("schema");
        let parts = encode(
            &schema,
            "darwin-relay/test",
            "darwin-pushport:0@1-2",
            Utc::now(),
            &batch,
        )
        .expect("encodes");
        assert!(parts.len() >= 2, "split into {} parts", parts.len());
    }
}
