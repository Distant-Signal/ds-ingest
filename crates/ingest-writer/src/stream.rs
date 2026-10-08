//! The writer's stream half (spec §7.3, §10; plan 3a.3): one
//! [`StreamConsumer`] task per stream not `off`, whose [`Handler`] is
//! [`WriterHandler`], plus the hourly `MINID` trim of the dead-letter
//! streams.
//!
//! Modes (`INGEST_WRITER_STREAMS=station-samples:apply,full-coverage:shadow`,
//! chart `ingestWriter.streams.<name>`; every stream `off` by default):
//!
//! - **`off`**: the stream is not read;
//! - **`shadow`**: decode and validate each entry, export metrics, ACK, and
//!   write nothing (`ingest_stream_consumed_total{outcome="skipped"}`);
//! - **`apply`**: write, with the `ingest_dedup` insert in the same
//!   transaction.
//!
//! The Redis half (groups, PEL first, `XAUTOCLAIM`, dead letters, retries,
//! gauges) is `ingest_stream::consumer`.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use common::progress::Progress;
use common::redis_conn::RedisConn;
use ingest_stream::{
    ConsumerConfig, Handled, Handler, HandlerError, StreamConsumer, StreamEntry, budget, streams,
};
use sqlx::PgPool;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::dedup;
use crate::handlers::{Applied, Registry, classify};
use crate::observed::Observed;

/// A stream's mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mode {
    #[default]
    Off,
    Shadow,
    Apply,
}

impl FromStr for Mode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "off" => Ok(Self::Off),
            "shadow" => Ok(Self::Shadow),
            "apply" => Ok(Self::Apply),
            other => bail!("unknown ingest stream mode {other:?} (off, shadow or apply)"),
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::Apply => "apply",
        })
    }
}

/// One stream the writer can read: its short name (the mode key and chart
/// value), its key and the schemas it carries (spec §7.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamSpec {
    pub name: &'static str,
    pub stream: &'static str,
    pub schemas: &'static [&'static str],
}

/// The ingest streams (spec §7.1; D1: no `train-events`).
pub const STREAMS: [StreamSpec; 5] = [
    StreamSpec {
        name: "station-samples",
        stream: streams::STATION_SAMPLES,
        schemas: &["station-samples"],
    },
    StreamSpec {
        name: "full-coverage",
        stream: streams::FULL_COVERAGE,
        schemas: &[
            "full-coverage-stats",
            "full-coverage-window-stats",
            "station-full-coverage-samples",
        ],
    },
    StreamSpec {
        name: "tfl",
        stream: streams::TFL,
        schemas: &["tfl-line-status"],
    },
    StreamSpec {
        name: "reference",
        stream: streams::REFERENCE,
        schemas: &["tocs"],
    },
    StreamSpec {
        name: "island-of-ireland",
        stream: streams::ISLAND_OF_IRELAND,
        schemas: &["ioi-stations", "ioi-lines", "ioi-station-samples"],
    },
];

/// Every stream's mode (`INGEST_WRITER_STREAMS`): comma-separated
/// `<name>:<mode>`; a stream not listed is `off`, and the empty string
/// (the default) turns every stream off.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamModes(BTreeMap<&'static str, Mode>);

impl FromStr for StreamModes {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut modes = BTreeMap::new();
        for item in s.split(',').map(str::trim).filter(|item| !item.is_empty()) {
            let (name, mode) = item.split_once(':').with_context(|| {
                format!("INGEST_WRITER_STREAMS: {item:?} is not <stream>:<mode>")
            })?;
            let name = name.trim();
            let Some(spec) = STREAMS.iter().find(|spec| spec.name == name) else {
                bail!(
                    "INGEST_WRITER_STREAMS: unknown stream {name:?} (one of {})",
                    STREAMS.map(|spec| spec.name).join(", ")
                );
            };
            let mode: Mode = mode.trim().parse()?;
            if modes.insert(spec.name, mode).is_some() {
                bail!("INGEST_WRITER_STREAMS: {name:?} given twice");
            }
        }
        Ok(Self(modes))
    }
}

impl StreamModes {
    pub fn mode(&self, name: &str) -> Mode {
        self.0.get(name).copied().unwrap_or_default()
    }

    /// The streams not `off`, with their modes.
    pub fn active(&self) -> Vec<(StreamSpec, Mode)> {
        STREAMS
            .iter()
            .map(|spec| (*spec, self.mode(spec.name)))
            .filter(|(_, mode)| *mode != Mode::Off)
            .collect()
    }

    pub fn any_active(&self) -> bool {
        !self.active().is_empty()
    }

    /// Whether any stream writes (and so needs `ingest_dedup` pruned).
    pub fn any_apply(&self) -> bool {
        self.active().iter().any(|(_, mode)| *mode == Mode::Apply)
    }

    /// The active streams' schemas with no handler in `registry`, as
    /// `stream: schema`. Every entry of such a schema would be dead-lettered
    /// as an unknown name, so the writer refuses to start them.
    pub fn uncovered(&self, registry: &Registry) -> Vec<String> {
        self.active()
            .iter()
            .flat_map(|(spec, _)| {
                spec.schemas
                    .iter()
                    .filter(|schema| !registry.knows(schema))
                    .map(|schema| format!("{}: {schema}", spec.name))
            })
            .collect()
    }
}

impl fmt::Display for StreamModes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let items: Vec<String> = STREAMS
            .iter()
            .map(|spec| format!("{}:{}", spec.name, self.mode(spec.name)))
            .collect();
        f.write_str(&items.join(","))
    }
}

/// The writer's [`Handler`] for one stream in `shadow` or `apply`.
pub struct WriterHandler {
    pool: PgPool,
    registry: Arc<Registry>,
    mode: Mode,
}

impl WriterHandler {
    pub fn new(pool: PgPool, registry: Arc<Registry>, mode: Mode) -> Self {
        Self {
            pool,
            registry,
            mode,
        }
    }

    async fn apply(&self, entry: &StreamEntry) -> Result<Handled, HandlerError> {
        let handler = self.registry.lookup(&entry.envelope.schema)?;
        let mut tx = self.pool.begin().await.map_err(|err| classify(&err))?;
        let claimed = dedup::claim(&mut tx, &entry.envelope.key, &entry.stream)
            .await
            .map_err(|err| classify(&err))?;
        if !claimed {
            // Rolled back on drop: nothing was written.
            return Ok(Handled::Duplicate);
        }
        let observed = Observed::for_entry(entry);
        let applied = handler.apply(&mut tx, entry, &observed).await?;
        tx.commit().await.map_err(|err| classify(&err))?;
        Ok(match applied {
            Applied::All => Handled::Applied,
            Applied::PartiallyRejected { reason, rejected } => {
                Handled::PartiallyRejected { reason, rejected }
            }
        })
    }
}

impl Handler for WriterHandler {
    async fn handle(&self, entry: &StreamEntry) -> Result<Handled, HandlerError> {
        match self.mode {
            Mode::Apply => self.apply(entry).await,
            // `Off` streams get no consumer; treat a stray one as shadow.
            Mode::Shadow | Mode::Off => {
                self.registry.lookup(&entry.envelope.schema)?.check(entry)?;
                Ok(Handled::Skipped)
            }
        }
    }
}

/// Dead-letter entries are kept 7 days (spec §7.1), as well as capped at
/// the source's `MAXLEN` on every XADD (I3).
pub const DEAD_LETTER_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);

/// How often the dead-letter streams are trimmed by age.
pub const DEAD_LETTER_TRIM_INTERVAL: Duration = Duration::from_secs(3600);

/// `XTRIM <dead-letter stream> MINID ~ <now - retention>`: drops dead-letter
/// entries older than `retention` (their ids are their dead-letter times).
/// Returns how many were removed.
pub async fn trim_dead_letters(
    conn: &mut RedisConn,
    dead_letter_stream: &str,
    now: DateTime<Utc>,
    retention: Duration,
) -> redis::RedisResult<u64> {
    let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
    let min_id = now.timestamp_millis().saturating_sub(retention_ms).max(0);
    redis::cmd("XTRIM")
        .arg(dead_letter_stream)
        .arg("MINID")
        .arg("~")
        .arg(min_id)
        .query_async(conn)
        .await
}

/// What [`spawn`] needs.
pub struct StreamRuntime {
    pub client: redis::Client,
    pub pool: PgPool,
    pub registry: Arc<Registry>,
    /// This consumer's name in each group: the pod name.
    pub consumer: String,
    pub stall_after: Duration,
}

/// The spawned stream tasks.
pub struct RunningStreams {
    tasks: JoinSet<()>,
    progress: Vec<(&'static str, Progress)>,
    shutdown: watch::Sender<bool>,
}

/// Spawns one consumer task per stream not `off` in `modes`, and the
/// dead-letter trim task. Each task connects to Redis itself (waiting for
/// it, beating its progress), so a Redis outage at startup does not hold
/// up the loops.
pub fn spawn(runtime: &StreamRuntime, modes: &StreamModes) -> RunningStreams {
    let (shutdown, _) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut progress = Vec::new();
    let active = modes.active();
    for (spec, mode) in &active {
        let task_progress = Progress::new(runtime.stall_after);
        progress.push((spec.name, task_progress.clone()));
        let handler =
            WriterHandler::new(runtime.pool.clone(), Arc::clone(&runtime.registry), *mode);
        // Every stream is declared (`every_stream_is_declared_in_the_budget`);
        // the fallback is the spec's original dead-letter cap.
        let dead_letter_maxlen =
            budget::decl(spec.stream).map_or(10_000, budget::StreamDecl::dead_letter_maxlen);
        let config = ConsumerConfig::new(spec.stream, &runtime.consumer, dead_letter_maxlen);
        let client = runtime.client.clone();
        let stop = stopped(shutdown.subscribe());
        let mode = *mode;
        tasks.spawn(async move {
            let conn = common::redis_conn::connect_until_ready(
                "Redis (ingest stream)",
                &client,
                common::startup::CONNECT_BACKOFF,
                Some(&task_progress),
            )
            .await;
            tracing::info!(stream = %config.stream, %mode, "ingest stream consumer started");
            StreamConsumer::new(conn, config)
                .with_progress(task_progress)
                .run(&handler, stop)
                .await;
        });
    }
    if !active.is_empty() {
        let dead_letter_streams: Vec<String> = active
            .iter()
            .map(|(spec, _)| ingest_stream::dead_letter_stream(spec.stream))
            .collect();
        let client = runtime.client.clone();
        let stop = stopped(shutdown.subscribe());
        tasks.spawn(trim_task(client, dead_letter_streams, stop));
    }
    RunningStreams {
        tasks,
        progress,
        shutdown,
    }
}

/// Trims every dead-letter stream by age once an hour (the first right
/// after startup).
async fn trim_task(
    client: redis::Client,
    dead_letter_streams: Vec<String>,
    stop: impl Future<Output = ()>,
) {
    let mut stop = std::pin::pin!(stop);
    let connect = common::redis_conn::connect_until_ready(
        "Redis (dead-letter trim)",
        &client,
        common::startup::CONNECT_BACKOFF,
        None,
    );
    let mut conn = tokio::select! {
        () = stop.as_mut() => return,
        conn = connect => conn,
    };
    let mut interval = tokio::time::interval(DEAD_LETTER_TRIM_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.as_mut() => return,
            _ = interval.tick() => {}
        }
        for dead_letter_stream in &dead_letter_streams {
            match trim_dead_letters(
                &mut conn,
                dead_letter_stream,
                Utc::now(),
                DEAD_LETTER_RETENTION,
            )
            .await
            {
                Ok(0) => {}
                Ok(removed) => {
                    tracing::info!(%dead_letter_stream, removed, "trimmed dead-letter entries older than 7 days");
                }
                Err(err) => {
                    tracing::warn!(%dead_letter_stream, error = %err, "dead-letter MINID trim failed; will retry next hour");
                }
            }
        }
    }
}

/// Resolves once `rx` sees `true` (or its sender is gone).
async fn stopped(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

impl RunningStreams {
    /// The stream tasks that have made no progress within the stall window
    /// (a dead task stops beating, so it goes stalled too).
    pub fn stalled(&self) -> Vec<&'static str> {
        self.progress
            .iter()
            .filter(|(_, progress)| progress.is_stalled())
            .map(|(name, _)| *name)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.progress.len()
    }

    pub fn is_empty(&self) -> bool {
        self.progress.is_empty()
    }

    /// Asks every task to stop between entries (a running handler is never
    /// cancelled), waits up to `grace`, then aborts what is left (an entry
    /// not yet acked is simply redelivered).
    pub async fn shutdown(mut self, grace: Duration) {
        let _ = self.shutdown.send(true);
        let drained = tokio::time::timeout(grace, async {
            while self.tasks.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            tracing::warn!("ingest stream tasks did not stop within the grace period; aborting");
            self.tasks.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_parse_and_default_to_off() {
        let modes: StreamModes = "".parse().unwrap();
        assert!(!modes.any_active());
        assert_eq!(modes, StreamModes::default());
        assert_eq!(
            modes.to_string(),
            "station-samples:off,full-coverage:off,tfl:off,reference:off,island-of-ireland:off"
        );

        let modes: StreamModes = " station-samples:apply, full-coverage : shadow,tfl:off"
            .parse()
            .unwrap();
        assert_eq!(modes.mode("station-samples"), Mode::Apply);
        assert_eq!(modes.mode("full-coverage"), Mode::Shadow);
        assert_eq!(modes.mode("tfl"), Mode::Off);
        assert_eq!(modes.mode("reference"), Mode::Off);
        assert!(modes.any_apply());
        let active: Vec<_> = modes
            .active()
            .iter()
            .map(|(spec, mode)| (spec.stream, *mode))
            .collect();
        assert_eq!(
            active,
            [
                ("ds:ingest:station-samples", Mode::Apply),
                ("ds:ingest:full-coverage", Mode::Shadow)
            ]
        );

        let shadow_only: StreamModes = "tfl:shadow".parse().unwrap();
        assert!(shadow_only.any_active());
        assert!(!shadow_only.any_apply());
    }

    #[test]
    fn bad_modes_are_refused() {
        for bad in [
            "station-samples",
            "station-samples:on",
            "train-events:apply",
            "tfl:apply,tfl:shadow",
        ] {
            assert!(bad.parse::<StreamModes>().is_err(), "{bad}");
        }
    }

    #[test]
    fn every_stream_is_declared_in_the_budget() {
        for spec in STREAMS {
            assert!(budget::decl(spec.stream).is_some(), "{}", spec.stream);
            assert_eq!(spec.stream, format!("ds:ingest:{}", spec.name));
        }
    }

    #[test]
    fn a_stream_with_no_handler_is_uncovered() {
        let modes: StreamModes = "station-samples:shadow".parse().unwrap();
        assert_eq!(
            modes.uncovered(&crate::handlers::registry()),
            ["station-samples: station-samples"]
        );
        let off = StreamModes::default();
        assert!(off.uncovered(&crate::handlers::registry()).is_empty());
    }
}
