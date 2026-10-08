//! Where each stats write goes (ingest architecture plan 3a.8, spec §13.1),
//! chosen by `INGEST_SINK`:
//!
//! | Sink | Each stats write |
//! |---|---|
//! | `http` (default) | today's three POSTs (`/private/full-coverage-stats`, `/private/full-coverage-window-stats` when windowed, `/private/station-full-coverage-samples`) |
//! | `http+shadow` | the same POSTs, then the outputs the api accepted `XADD`ed to `ds:ingest:full-coverage`, best effort |
//! | `stream` | XADD only |
//!
//! **One snapshot per stats write**: the three outputs
//! (`full-coverage-stats/1`, `full-coverage-window-stats/1`,
//! `station-full-coverage-samples/1`, each body today's POST body) share
//! one producer item, with the write's `now` as `produced_at`, so under the
//! latest-only policy a newer write replaces an unsent one whole, never one
//! output of it. The write never waits for the XADD: the consume loop must
//! keep up with the movement feed, and the producer retries on its own (an
//! outage shows as `DistantSignalIngestProducerXaddFailing`, not here).
//!
//! `full_coverage_consumer_window_rows_posted_total` (what
//! `DistantSignalFullCoverageWindowStatsStalled` watches) counts window rows
//! delivered either way: on the POST's success, or under `stream` once the
//! snapshot is written.

use chrono::{DateTime, Utc};
use common::{FullCoverageLineStatsRow, FullCoverageWindowStatsRow, StationFullCoverageSample};
use ingest_stream::SchemaId;
use ingest_stream::envelope::EnvelopeError;
use ingest_stream::snapshot::{ROWS_PER_PART, SinkMode, Snapshot, SnapshotProducer};

use crate::config::Config;

/// The three schemas' names (`/1`), for [`StatsSink::accepted`].
pub(crate) const LINE_STATS: &str = "full-coverage-stats";
pub(crate) const WINDOW_STATS: &str = "full-coverage-window-stats";
pub(crate) const STATION_SAMPLES: &str = "station-full-coverage-samples";

fn schema(name: &str) -> SchemaId {
    #[expect(clippy::expect_used, reason = "the three names are constant and valid")]
    SchemaId::new(name, 1).expect("valid schema id")
}

/// One stats write's outputs, as the api accepted them (`http+shadow`) or
/// as built (`stream`).
#[derive(Default)]
pub(crate) struct StatsOutputs<'a> {
    pub lines: &'a [FullCoverageLineStatsRow],
    pub windows: &'a [FullCoverageWindowStatsRow],
    pub stations: &'a [StationFullCoverageSample],
}

/// The configured sink.
pub(crate) struct StatsSink {
    mode: SinkMode,
    stream: Option<SnapshotProducer>,
}

impl StatsSink {
    /// From `INGEST_SINK`, over the consumer's own Redis (`REDIS_URL`,
    /// the `full-coverage-consumer` ACL user, which may XADD and XREVRANGE
    /// `ds:ingest:full-coverage`).
    pub(crate) fn from_config(config: &Config) -> anyhow::Result<Self> {
        let stream = if config.ingest_sink.produces() {
            let url = common::redis_auth::redis_url_with_credentials(
                &config.redis_url,
                config.redis_username.as_deref(),
                config.redis_password.as_ref(),
            )?;
            let client = redis::Client::open(url.expose()).map_err(|_| {
                anyhow::anyhow!("REDIS_URL is not a valid Redis URL (value not shown)")
            })?;
            Some(SnapshotProducer::new(
                client,
                ingest_stream::streams::FULL_COVERAGE,
                "full-coverage-consumer",
                vec![
                    schema(LINE_STATS),
                    schema(WINDOW_STATS),
                    schema(STATION_SAMPLES),
                ],
            ))
        } else {
            None
        };
        Ok(Self {
            mode: config.ingest_sink,
            stream,
        })
    }

    /// The `http` sink, for tests.
    #[cfg(test)]
    pub(crate) fn http() -> Self {
        Self {
            mode: SinkMode::Http,
            stream: None,
        }
    }

    /// A sink in `mode` on test stream `stream` at `redis_url`.
    #[cfg(test)]
    pub(crate) fn on_stream(mode: SinkMode, redis_url: &str, stream: &str) -> Self {
        Self {
            mode,
            stream: Some(tests::producer(redis_url, stream)),
        }
    }

    pub(crate) fn mode(&self) -> SinkMode {
        self.mode
    }

    /// Whether the api gets the `POST`s (`http`, `http+shadow`).
    pub(crate) fn posts_http(&self) -> bool {
        self.mode.posts_http()
    }

    /// Counts rows the api accepted (`ingest_stream_sink_rows_total`,
    /// `sink="http"`), for the rollout's compare step.
    pub(crate) fn accepted(schema: &'static str, rows: usize) {
        if rows > 0 {
            ingest_stream::metrics::sink_rows(
                ingest_stream::streams::FULL_COVERAGE,
                schema,
                "http",
                rows,
            );
        }
    }

    /// XADDs one stats write (`http+shadow`: what the api accepted;
    /// `stream`: everything), as one snapshot at `produced_at`. Never
    /// waits for the XADD and never fails the write: an encoding failure
    /// is logged (and counted as `oversize` when it is one).
    pub(crate) async fn produce(&self, produced_at: DateTime<Utc>, outputs: &StatsOutputs<'_>) {
        let Some(stream) = self.stream.as_ref().filter(|_| self.mode.produces()) else {
            return;
        };
        let snapshot = match encode(stream, produced_at, outputs) {
            Ok(snapshot) if snapshot.is_empty() => return,
            Ok(snapshot) => snapshot,
            Err(err) => {
                tracing::error!(error = %err, "could not encode the full-coverage stats snapshot; not sent");
                return;
            }
        };
        let receipt = match stream.submit(snapshot).await {
            Ok(receipt) => receipt,
            Err(err) => {
                tracing::warn!(error = %err, "full-coverage stats snapshot not queued");
                return;
            }
        };
        // Under `stream`, the stalled-windows alert's counter moves when
        // the window rows are delivered, as it does on a POST's success.
        let windows = outputs.windows.len();
        if self.mode == SinkMode::Stream && windows > 0 {
            tokio::spawn(async move {
                if receipt.written().await.is_ok() {
                    metrics::counter!(common::metrics::metric_name(
                        "full_coverage_consumer_window_rows_posted_total"
                    ))
                    .increment(windows as u64);
                }
            });
        }
    }
}

/// One stats write as a snapshot of up to three schemas.
fn encode(
    stream: &SnapshotProducer,
    produced_at: DateTime<Utc>,
    outputs: &StatsOutputs<'_>,
) -> Result<Snapshot, EnvelopeError> {
    let mut snapshot = Snapshot::new();
    let (name, producer) = (stream.stream(), stream.producer_id());
    snapshot.add(
        name,
        &schema(LINE_STATS),
        producer,
        produced_at,
        outputs.lines,
        ROWS_PER_PART,
    )?;
    snapshot.add(
        name,
        &schema(WINDOW_STATS),
        producer,
        produced_at,
        outputs.windows,
        ROWS_PER_PART,
    )?;
    snapshot.add(
        name,
        &schema(STATION_SAMPLES),
        producer,
        produced_at,
        outputs.stations,
        ROWS_PER_PART,
    )?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use ingest_stream::Envelope;

    use super::*;

    pub(crate) fn producer(redis_url: &str, stream: &str) -> SnapshotProducer {
        SnapshotProducer::new(
            redis::Client::open(redis_url).unwrap(),
            Box::leak(stream.to_owned().into_boxed_str()),
            "full-coverage-consumer",
            vec![
                schema(LINE_STATS),
                schema(WINDOW_STATS),
                schema(STATION_SAMPLES),
            ],
        )
    }

    fn line(line_id: &str) -> FullCoverageLineStatsRow {
        serde_json::from_value(serde_json::json!({
            "line_id": line_id,
            "service_date": "2026-10-08",
            "availability": "pending",
            "stats": {"total": 1, "delayed": 0, "cancelled": 0, "skipped": 0, "avg_delay_minutes": 0.0},
            "partial": false
        }))
        .unwrap()
    }

    fn station(crs: &str, at: DateTime<Utc>) -> StationFullCoverageSample {
        StationFullCoverageSample {
            crs: crs.to_owned(),
            operator: "SW".to_owned(),
            resolved_at: at,
            stats: common::SampleStats {
                total: 2,
                delayed: 1,
                cancelled: 0,
                skipped: 0,
                avg_delay_minutes: 1.0,
            },
        }
    }

    #[test]
    fn a_stats_write_is_one_snapshot_of_its_outputs_at_its_time() {
        let at: DateTime<Utc> = "2026-10-08T12:00:00Z".parse().unwrap();
        let lines: Vec<_> = (0..150).map(|i| line(&format!("line-{i}"))).collect();
        let stations = [station("WAT", at)];
        let snapshot = encode(
            &producer("redis://127.0.0.1:1", "unused"),
            at,
            &StatsOutputs {
                lines: &lines,
                windows: &[],
                stations: &stations,
            },
        )
        .unwrap();
        let parts: Vec<Envelope> = snapshot
            .parts()
            .iter()
            .map(|p| Envelope::decode(&p.fields).unwrap())
            .collect();
        let keys: Vec<&str> = parts.iter().map(|p| p.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "full-coverage-stats:2026-10-08T12:00:00.000Z:1/2",
                "full-coverage-stats:2026-10-08T12:00:00.000Z:2/2",
                "station-full-coverage-samples:2026-10-08T12:00:00.000Z:1/1",
            ]
        );
        assert!(parts.iter().all(|p| p.produced_at == at));
        // Today's POST body for the chunk.
        let body: Vec<StationFullCoverageSample> = parts[2].payload_as().unwrap();
        assert_eq!(body[0].crs, "WAT");
        assert_eq!(
            snapshot.rows(),
            [
                ("full-coverage-stats".to_owned(), 150),
                ("station-full-coverage-samples".to_owned(), 1)
            ]
        );
    }

    /// Against a live Redis (`REDIS_URL`, default local valkey): `stream`
    /// XADDs every part of the write without the caller waiting.
    #[tokio::test]
    #[ignore = "requires Redis; run with --ignored (REDIS_URL, default redis://127.0.0.1:6379)"]
    async fn stream_xadds_the_write() {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
        let stream = format!(
            "test-{}:ds:ingest:full-coverage",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let sink = StatsSink {
            mode: SinkMode::Stream,
            stream: Some(producer(&redis_url, &stream)),
        };
        let at: DateTime<Utc> = "2026-10-08T12:00:00.500Z".parse().unwrap();
        let lines = [line("a"), line("b")];
        let stations = [station("WAT", at)];
        sink.produce(
            at,
            &StatsOutputs {
                lines: &lines,
                windows: &[],
                stations: &stations,
            },
        )
        .await;
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let len: u64 = redis::cmd("XLEN")
                .arg(&stream)
                .query_async(&mut conn)
                .await
                .unwrap();
            if len == 2 {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "not written: {len}");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(
            sink.stream.as_ref().unwrap().cursor().await.unwrap(),
            Some(at)
        );
        let _: () = redis::cmd("DEL")
            .arg(&stream)
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}
