//! `movement-relay`: the sole real Kafka client against RDM's Train
//! Movements product from Deploy B onward, fanning out into the
//! `movement-events` Redis Stream that `trust-consumer`,
//! `full-coverage-consumer` and `trust-backlog-consumer` each read from
//! under their own consumer group. See
//! docs/superpowers/specs/2026-09-04-movement-relay-design.md and
//! docs/superpowers/plans/2026-09-04-movement-relay-plan.md.

mod config;
mod deadletter;
mod event_sink;
mod health;
mod kafka_source;

use std::time::Duration;

use clap::Parser;
use config::Config;
use event_sink::{EventSink, RedisEventSink};
use kafka_source::{KafkaRawSource, RawKafkaSource};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();
    if config.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }

    // Readiness (`/healthz`) stays "confirmed Kafka partition assignment"
    // (`health::RelayContext`); liveness (`/livez`) is loop progress only, so
    // neither a slow group rebalance nor Redis being unavailable (the Redis
    // pod being recreated by the same rollout) gets this pod killed. Same
    // split as the movement-stream consumers (SVC-08/INF-9).
    let (ready, progress) = health_http::spawn_with_progress(
        config.health_bind_url.clone(),
        "partitions assigned",
        "no confirmed partition assignment",
        Duration::from_secs(config.progress_stall_secs),
    );

    // REDIS_PASSWORD, when set, is applied here (common::redis_auth). The
    // result carries the password: pass it on, never log it.
    let redis_url = common::redis_auth::redis_url_with_password(
        &config.redis_url,
        config.redis_password.as_ref(),
    )?;
    // Redis first: joining the Kafka group (which only happens once the
    // loop polls) is pointless until there is somewhere to publish to.
    let mut sink = RedisEventSink::connect_until_ready(
        redis_url.expose(),
        config.movement_stream_maxlen,
        config.movement_consumer_groups.clone(),
        common::startup::CONNECT_BACKOFF,
        &progress,
    )
    .await?;
    // D2: a fresh (missing or empty) stream gets every consumer group at
    // its start before the first entry. Best effort: a failure is logged,
    // and a missing stream is still handled by the first publish.
    if let Err(err) = sink.prepare_stream().await {
        tracing::warn!(error = ?err, "could not create the consumer groups on a fresh movement-events stream");
    }
    let mut source = KafkaRawSource::connect(&config, ready, progress.clone())?;

    tokio::spawn(stream_lag_loop::<common::redis_conn::RedisConn>(
        redis_url.expose().to_owned(),
        Duration::from_secs(config.stream_lag_poll_secs),
        config.movement_stream_maxlen,
        Duration::from_secs(config.deadletter_max_age_secs),
    ));

    loop {
        match run_cycle(&mut source, &mut sink).await {
            Cycle::Committed => {}
            Cycle::Failed => tokio::time::sleep(ERROR_BACKOFF).await,
        }
        // One loop iteration completed, however it went -- a failed cycle
        // (Redis down) is the process alive and retrying, not a wedge. See
        // `health_http::Progress`.
        progress.beat();
    }
}

/// How long to wait before retrying after a failed cycle -- flat, not
/// exponential, same reasoning as `trust-consumer::main::ERROR_BACKOFF`:
/// Kafka holds the backlog, so there's nothing to drain, only a log/Redis
/// to avoid hammering.
const ERROR_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq, Eq)]
enum Cycle {
    Committed,
    Failed,
}

/// One consume -> classify -> XADD -> commit cycle. Only commits the Kafka
/// offset once EVERY surviving envelope from this record has been
/// durably XADDed -- mirrors trust-consumer's own never-commit-on-a-
/// failed-downstream-write discipline, substituting "every XADD in this
/// record succeeded" for "the HTTP POST succeeded".
///
/// "Doesn't commit" is not on its own enough to keep a record: a bare
/// `Cycle::Failed` used to leave the record behind entirely, because the
/// next cycle's `consumer.recv()` advances librdkafka's fetch position and
/// overwrites `last_received`, so the next SUCCESSFUL commit stored an
/// offset PAST the record whose XADD had failed. Every failure path that
/// happens after a batch was received therefore hands that batch back via
/// `RawKafkaSource::retain_for_retry`, which makes the next `next_batch`
/// re-deliver exactly it before fetching anything new. The one deliberate
/// exception is a record that cannot be classified at all -- see
/// `publish_batch`: it is skipped and the offset commits past it.
async fn run_cycle<S, K>(source: &mut S, sink: &mut K) -> Cycle
where
    S: RawKafkaSource,
    K: EventSink,
{
    let batch = match source.next_batch().await {
        Ok(batch) => batch,
        Err(err) => {
            tracing::error!(error = ?err, "error receiving from Kafka");
            metrics::counter!(
                common::metrics::metric_name("movement_relay_errors_total"),
                "operation" => "kafka_receive"
            )
            .increment(1);
            return Cycle::Failed;
        }
    };

    match publish_batch(sink, &batch).await {
        BatchOutcome::Published => {}
        BatchOutcome::PublishFailed => {
            // The downstream write failed, so this record has NOT reached
            // `movement-events` in full. Hand it back so the next cycle
            // re-delivers exactly it: without this, the next `recv()`
            // advances past it and the next successful commit silently
            // commits over it -- one permanently dropped record per retry
            // cycle for the whole duration of a Redis outage.
            source.retain_for_retry(batch);
            return Cycle::Failed;
        }
    }

    if let Err(err) = source.commit().await {
        tracing::error!(error = ?err, "failed to commit Kafka offset");
        metrics::counter!(
            common::metrics::metric_name("movement_relay_errors_total"),
            "operation" => "commit_offsets"
        )
        .increment(1);
        // Published but uncommitted: retained too, so the record is
        // re-published on the next cycle rather than being skipped by the
        // following commit. That re-publish is a duplicate `movement-events`
        // entry, which is the at-least-once posture every consumer of that
        // stream already handles (`MovementFeed`'s own doc: redelivery "is
        // made safe by the `dedup_key` path") -- a duplicate is recoverable
        // downstream, a dropped movement is not.
        source.retain_for_retry(batch);
        return Cycle::Failed;
    }
    Cycle::Committed
}

/// Whether every envelope in `batch` made it downstream -- split out of
/// `run_cycle` so the failing paths there can hand the owned `batch` back to
/// the source (they cannot while a `&batch` iteration is still live).
enum BatchOutcome {
    Published,
    /// A downstream XADD failed: transient, and the record must be retried.
    PublishFailed,
}

async fn publish_batch<K>(sink: &mut K, batch: &[String]) -> BatchOutcome
where
    K: EventSink,
{
    for raw in batch {
        let envelopes = match trust_schema::schema::confirmed_envelope_bodies(raw) {
            Ok(classified) => {
                // PL-4: an envelope with no `header.msg_type` is skipped on
                // its own; the rest of the record still publishes.
                if classified.malformed > 0 {
                    tracing::warn!(
                        malformed = classified.malformed,
                        raw = %raw,
                        "skipping TRUST envelopes with no header.msg_type; publishing the rest of the record"
                    );
                    metrics::counter!(
                        common::metrics::metric_name("movement_relay_errors_total"),
                        "operation" => "classify_envelope"
                    )
                    .increment(classified.malformed as u64);
                }
                classified.envelopes
            }
            Err(err) => {
                // Permanent for these bytes: every retry would fail the same
                // way, and retaining it would wedge the sole movement feed.
                // Skip it and let the offset commit past it (PL-15c): it
                // used to return a failed cycle, which cost an ERROR_BACKOFF
                // sleep per bad record and throttled a burst of them to
                // 0.5 records/s for all three consumer groups. It is logged
                // with its raw payload and counted here.
                tracing::error!(error = ?err, raw = %raw, "failed to classify Kafka record; skipping it");
                metrics::counter!(
                    common::metrics::metric_name("movement_relay_errors_total"),
                    "operation" => "classify_record"
                )
                .increment(1);
                continue;
            }
        };
        for (msg_type, payload) in &envelopes {
            if let Err(err) = sink.publish(msg_type, payload).await {
                tracing::error!(error = ?err, msg_type, "failed to XADD envelope; not committing this record's offset, and holding it for redelivery");
                metrics::counter!(
                    common::metrics::metric_name("movement_relay_errors_total"),
                    "operation" => "publish_event"
                )
                .increment(1);
                return BatchOutcome::PublishFailed;
            }
            metrics::counter!(
                common::metrics::metric_name("movement_relay_events_published_total"),
                "msg_type" => msg_type.clone()
            )
            .increment(1);
        }
    }
    BatchOutcome::Published
}

/// Every consumer group that reads `movement-events`, and so every group
/// this leading-indicator lag gauge must report on.
///
/// **Bug this fixes**: this used to be an inline two-element array,
/// `["trust-consumer", "full-coverage-consumer"]`, omitting
/// `"trust-event-backlog"` -- the third, real consumer group
/// `trust-backlog-consumer` connects under (see its own `main.rs`'s
/// `RedisStreamMovementFeed::connect(.., "trust-event-backlog", ..)` call).
/// That group's lag was silently never reported: not an error, just a gap
/// nobody watching the gauge would notice was missing.
const STREAM_LAG_GROUPS: [&str; 3] = event_sink::DEFAULT_CONSUMER_GROUPS;

/// A Redis connection capable of computing `movement-events` consumer-group
/// lag -- split out from a concrete `common::redis_conn::RedisConn` purely so
/// `stream_lag_loop`'s retry state machine (`run_lag_tick`) is unit-testable
/// against a fake that can be told to fail its first N connect attempts,
/// without a real Redis. Same fake-behind-a-trait shape `RawKafkaSource` /
/// `EventSink` already use elsewhere in this crate.
#[async_trait::async_trait]
trait LagConnection: Sized + Send + 'static {
    async fn connect(redis_url: &str) -> anyhow::Result<Self>;
    async fn group_lag(&mut self, group: &str) -> anyhow::Result<Option<i64>>;
    /// The group's `pending` count (`XINFO GROUPS`): entries delivered but
    /// not yet ACKed. `None` when the group doesn't exist.
    async fn group_pending(&mut self, group: &str) -> anyhow::Result<Option<i64>>;
    /// `XLEN movement-events` -- 0 for a stream that doesn't exist yet.
    async fn stream_len(&mut self) -> anyhow::Result<u64>;
    /// `XLEN movement-events-deadletter` -- 0 while nothing was ever
    /// dead-lettered (the stream does not exist).
    async fn deadletter_len(&mut self) -> anyhow::Result<u64>;
    /// The raw `INFO persistence` reply (see [`parse_persistence_info`]).
    async fn persistence_info(&mut self) -> anyhow::Result<String>;
    /// `XTRIM movement-events-deadletter MINID <min_id>`: how many records
    /// were removed (see `deadletter::trim_older_than`).
    async fn trim_deadletter(&mut self, min_id: &str) -> anyhow::Result<u64>;
    /// The oldest dead-letter record's id, `None` when there is none.
    async fn deadletter_oldest_id(&mut self) -> anyhow::Result<Option<String>>;
}

#[async_trait::async_trait]
impl LagConnection for common::redis_conn::RedisConn {
    async fn connect(redis_url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        // One bounded attempt per tick (see `common::redis_conn`):
        // `run_lag_tick` already retries every tick.
        Ok(common::redis_conn::connect(&client).await?)
    }

    async fn group_lag(&mut self, group: &str) -> anyhow::Result<Option<i64>> {
        group_info_field(self, group, "lag").await
    }

    async fn group_pending(&mut self, group: &str) -> anyhow::Result<Option<i64>> {
        group_info_field(self, group, "pending").await
    }

    async fn stream_len(&mut self) -> anyhow::Result<u64> {
        Ok(redis::cmd("XLEN")
            .arg("movement-events")
            .query_async(self)
            .await?)
    }

    async fn deadletter_len(&mut self) -> anyhow::Result<u64> {
        Ok(redis::cmd("XLEN")
            .arg(DEADLETTER_STREAM)
            .query_async(self)
            .await?)
    }

    async fn persistence_info(&mut self) -> anyhow::Result<String> {
        Ok(redis::cmd("INFO")
            .arg("persistence")
            .query_async(self)
            .await?)
    }

    async fn trim_deadletter(&mut self, min_id: &str) -> anyhow::Result<u64> {
        deadletter::trim_older_than(self, DEADLETTER_STREAM, min_id).await
    }

    async fn deadletter_oldest_id(&mut self) -> anyhow::Result<Option<String>> {
        deadletter::oldest_id(self, DEADLETTER_STREAM).await
    }
}

use deadletter::DEADLETTER_STREAM;

/// What `INFO persistence` says about the AOF, the only persistence this
/// chart's Redis runs (`--appendonly yes`, RDB snapshots off).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistenceStatus {
    /// `aof_enabled:1`.
    aof_enabled: bool,
    /// `aof_last_write_status:ok`. A failed AOF write or fsync (a full or
    /// failing disk) makes Redis refuse every write until one succeeds, so
    /// movement-relay stops publishing.
    aof_last_write_ok: bool,
    /// `aof_last_bgrewrite_status:ok`. A failed rewrite leaves the
    /// incremental AOF growing (Redis retries with a backoff), which is how
    /// a disk fills.
    aof_last_bgrewrite_ok: bool,
}

/// Parses the three [`PersistenceStatus`] fields out of an `INFO
/// persistence` reply (`key:value` lines, CRLF-separated, `#` section
/// headers). `None` if any is missing, so a reply this cannot read never
/// reports a healthy AOF.
fn parse_persistence_info(info: &str) -> Option<PersistenceStatus> {
    let mut aof_enabled = None;
    let mut last_write = None;
    let mut last_bgrewrite = None;
    for line in info.lines() {
        let Some((key, value)) = line.trim().split_once(':') else {
            continue;
        };
        match key {
            "aof_enabled" => aof_enabled = Some(value == "1"),
            "aof_last_write_status" => last_write = Some(value == "ok"),
            "aof_last_bgrewrite_status" => last_bgrewrite = Some(value == "ok"),
            _ => {}
        }
    }
    Some(PersistenceStatus {
        aof_enabled: aof_enabled?,
        aof_last_write_ok: last_write?,
        aof_last_bgrewrite_ok: last_bgrewrite?,
    })
}

/// Per-group `XINFO GROUPS` lag, labelled `group`.
const STREAM_LAG_METRIC: &str = "movement_relay_stream_lag";
/// Per-group `XINFO GROUPS` pending count, labelled `group`. `lag` counts
/// only entries not yet DELIVERED: a consumer whose downstream is failing
/// keeps reading new entries (each failed batch stays pending, to be
/// reclaimed), so its lag stays near 0 while the un-ACKed entries it will
/// have to retry pile up behind it. Those are trimmed by MAXLEN just the
/// same, so the chart's lag alerts divide `lag + pending` by the cap.
const STREAM_PENDING_METRIC: &str = "movement_relay_stream_pending";
/// `XLEN movement-events` -- how full the stream currently is.
const STREAM_LENGTH_METRIC: &str = "movement_relay_stream_length";
/// The configured `MAXLEN ~` cap (`--movement-stream-maxlen`). Exported so
/// an alert can express lag as a fraction of the cap without hard-coding
/// the cap into the rule (the chart's `templates/prometheusrule.yaml`
/// divides `movement_relay_stream_lag` by this).
const STREAM_MAXLEN_METRIC: &str = "movement_relay_stream_maxlen";
/// `XLEN movement-events-deadletter`, labelled `stream`. Read fresh every
/// tick, unlike the consumers' own `movement_feed_deadletter_length`,
/// which is only set when a consumer dead-letters something: that one is
/// absent after every restart and stays at its last value after an
/// operator drains the stream, so DistantSignalDeadLetterNearFull reads
/// this one instead.
const DEADLETTER_LENGTH_METRIC: &str = "movement_relay_deadletter_length";
/// Counter of dead-letter records removed for being older than
/// `--deadletter-max-age-secs` (D5; see `deadletter`).
const DEADLETTER_TRIMMED_METRIC: &str = "movement_relay_deadletter_trimmed_total";
/// Age in seconds of the oldest dead-letter record (0 when there is
/// none), for DistantSignalDeadLetterExpiring: it fires hours before the
/// trim removes the record, while it can still be re-injected.
const DEADLETTER_OLDEST_AGE_METRIC: &str = "movement_relay_deadletter_oldest_age_seconds";
/// `INFO persistence` as 1/0 gauges (see [`PersistenceStatus`]), for the
/// chart's DistantSignalRedisPersistenceFailing alert. The chart ships no
/// redis_exporter, so without these nothing in it can see a failing AOF.
const AOF_ENABLED_METRIC: &str = "redis_aof_enabled";
const AOF_LAST_WRITE_OK_METRIC: &str = "redis_aof_last_write_ok";
const AOF_LAST_BGREWRITE_OK_METRIC: &str = "redis_aof_last_bgrewrite_ok";

/// What one `run_lag_tick` observed -- returned rather than only written
/// to gauges so the tick's behaviour is testable without a metrics
/// recorder (this crate has no `metrics-util` dev-dependency).
#[derive(Debug, Default, PartialEq, Eq)]
struct LagSample {
    /// `None` when there was no connection this tick or `XLEN` failed.
    stream_length: Option<u64>,
    /// Only the groups that exist and reported a lag, in
    /// `STREAM_LAG_GROUPS` order.
    group_lags: Vec<(&'static str, i64)>,
    /// Each existing group's pending (delivered, un-ACKed) count, in
    /// `STREAM_LAG_GROUPS` order.
    group_pending: Vec<(&'static str, i64)>,
    /// `None` when there was no connection this tick or `XLEN` failed.
    deadletter_length: Option<u64>,
    /// Records the age trim removed this tick; `None` if it failed.
    deadletter_trimmed: Option<u64>,
    /// The oldest remaining record's age (0 when there is none); `None`
    /// if it could not be read.
    deadletter_oldest_age_secs: Option<u64>,
    /// `None` when there was no connection this tick, `INFO` failed or its
    /// reply could not be parsed.
    persistence: Option<PersistenceStatus>,
}

/// Writes one tick's `LagSample` plus the configured cap to the gauges.
/// The cap is written every tick, connected or not, so it is present from
/// the first tick onward even while Redis is unreachable.
fn publish_lag_sample(sample: &LagSample, maxlen: u64) {
    metrics::gauge!(common::metrics::metric_name(STREAM_MAXLEN_METRIC)).set(maxlen as f64);
    if let Some(len) = sample.stream_length {
        metrics::gauge!(common::metrics::metric_name(STREAM_LENGTH_METRIC)).set(len as f64);
    }
    for (group, lag) in &sample.group_lags {
        metrics::gauge!(
            common::metrics::metric_name(STREAM_LAG_METRIC),
            "group" => *group
        )
        .set(*lag as f64);
    }
    for (group, pending) in &sample.group_pending {
        metrics::gauge!(
            common::metrics::metric_name(STREAM_PENDING_METRIC),
            "group" => *group
        )
        .set(*pending as f64);
    }
    if let Some(trimmed) = sample.deadletter_trimmed {
        metrics::counter!(common::metrics::metric_name(DEADLETTER_TRIMMED_METRIC))
            .increment(trimmed);
    }
    if let Some(age) = sample.deadletter_oldest_age_secs {
        metrics::gauge!(common::metrics::metric_name(DEADLETTER_OLDEST_AGE_METRIC)).set(age as f64);
    }
    if let Some(len) = sample.deadletter_length {
        metrics::gauge!(
            common::metrics::metric_name(DEADLETTER_LENGTH_METRIC),
            "stream" => DEADLETTER_STREAM
        )
        .set(len as f64);
    }
    if let Some(status) = sample.persistence {
        let flag = |ok: bool| if ok { 1.0 } else { 0.0 };
        metrics::gauge!(common::metrics::metric_name(AOF_ENABLED_METRIC))
            .set(flag(status.aof_enabled));
        metrics::gauge!(common::metrics::metric_name(AOF_LAST_WRITE_OK_METRIC))
            .set(flag(status.aof_last_write_ok));
        metrics::gauge!(common::metrics::metric_name(AOF_LAST_BGREWRITE_OK_METRIC))
            .set(flag(status.aof_last_bgrewrite_ok));
    }
}

/// Leading-indicator lag gauge (design doc Decision 2) -- polls `XINFO
/// GROUPS movement-events` for every group in `STREAM_LAG_GROUPS` on its own
/// timer, independent of the main consume loop. Reuses the same `XINFO
/// GROUPS` field-walk shape `movement_feed::redis_stream::check_gap`'s own
/// `find_group_field` helper uses -- NOT re-exported from that crate
/// (`movement-relay` deliberately doesn't depend on `movement-feed`, Task
/// 6's own note) -- a small, independent copy here instead.
///
/// **Bug this fixes**: a connection failure on the very first tick used to
/// disable this gauge for the rest of the process's life (an early `return`
/// out of the whole function, before the loop even started) -- most likely
/// to happen exactly when it matters least to be permanent: Redis simply not
/// up yet during this service's own startup, racing against Redis's own pod
/// coming up. `run_lag_tick` now (re)attempts the connection on demand, once
/// per tick, whenever it doesn't already have one -- a failed tick logs a
/// warning, counts it, and tries again next tick, forever, instead of giving
/// up once.
///
/// Also exports the stream's current length (`XLEN`) and its configured
/// cap (`maxlen`) alongside the per-group lag, so lag can be alerted on as
/// a fraction of the cap; the dead-letter stream's length; and the AOF's
/// health from `INFO persistence` (see [`PersistenceStatus`]).
///
/// Each tick also trims dead letters older than `deadletter_max_age` and
/// reports the oldest remaining one's age (see `deadletter`).
async fn stream_lag_loop<C: LagConnection>(
    redis_url: String,
    interval: Duration,
    maxlen: u64,
    deadletter_max_age: Duration,
) {
    let mut conn: Option<C> = None;
    // Registered at 0 so a trim shows up in `increase()` from the first one.
    metrics::counter!(common::metrics::metric_name(DEADLETTER_TRIMMED_METRIC)).increment(0);
    loop {
        tokio::time::sleep(interval).await;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        let sample = run_lag_tick(&redis_url, &mut conn, deadletter_max_age, now_ms).await;
        publish_lag_sample(&sample, maxlen);
    }
}

/// One tick's worth of `stream_lag_loop` work, split out so it's callable
/// (and its retry behaviour testable) without an actual `sleep`.
async fn run_lag_tick<C: LagConnection>(
    redis_url: &str,
    conn: &mut Option<C>,
    deadletter_max_age: Duration,
    now_ms: u64,
) -> LagSample {
    let mut sample = LagSample::default();
    if conn.is_none() {
        match C::connect(redis_url).await {
            Ok(c) => *conn = Some(c),
            Err(err) => {
                tracing::warn!(error = ?err, "stream_lag_loop: failed to (re)connect; will retry next tick");
                metrics::counter!(
                    common::metrics::metric_name("movement_relay_errors_total"),
                    "operation" => "redis_connect"
                )
                .increment(1);
                return sample;
            }
        }
    }
    // `conn` was just proven `Some` above (either already, or by the
    // successful `connect` arm) -- the only early return is the `Err` arm.
    let active = conn.as_mut().expect("connection ensured Some above");
    match active.stream_len().await {
        Ok(len) => sample.stream_length = Some(len),
        Err(err) => {
            tracing::warn!(error = ?err, "stream_lag_loop: failed to fetch XLEN");
        }
    }
    for group in STREAM_LAG_GROUPS {
        match active.group_lag(group).await {
            Ok(Some(lag)) => sample.group_lags.push((group, lag)),
            Ok(None) => {} // group doesn't exist yet -- nothing to report.
            Err(err) => {
                tracing::warn!(error = ?err, group, "stream_lag_loop: failed to fetch XINFO GROUPS");
            }
        }
        match active.group_pending(group).await {
            Ok(Some(pending)) => sample.group_pending.push((group, pending)),
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(error = ?err, group, "stream_lag_loop: failed to fetch XINFO GROUPS");
            }
        }
    }
    // Trim first, so the length and oldest age below are what remains.
    match active
        .trim_deadletter(&deadletter::min_id(now_ms, deadletter_max_age))
        .await
    {
        Ok(trimmed) => {
            if trimmed > 0 {
                tracing::warn!(
                    stream = DEADLETTER_STREAM,
                    trimmed,
                    max_age_secs = deadletter_max_age.as_secs(),
                    "deleted dead-letter records older than the TRUST retention limit"
                );
            }
            sample.deadletter_trimmed = Some(trimmed);
        }
        Err(err) => {
            tracing::warn!(error = ?err, "stream_lag_loop: failed to trim old dead letters");
        }
    }
    match active.deadletter_oldest_id().await {
        Ok(id) => {
            sample.deadletter_oldest_age_secs = Some(
                id.as_deref()
                    .and_then(|id| deadletter::age_secs(id, now_ms))
                    .unwrap_or(0),
            );
        }
        Err(err) => {
            tracing::warn!(error = ?err, "stream_lag_loop: failed to read the oldest dead letter");
        }
    }
    match active.deadletter_len().await {
        Ok(len) => sample.deadletter_length = Some(len),
        Err(err) => {
            tracing::warn!(error = ?err, "stream_lag_loop: failed to fetch the dead-letter XLEN");
        }
    }
    match active.persistence_info().await {
        Ok(info) => {
            sample.persistence = parse_persistence_info(&info);
            if sample.persistence.is_none() {
                tracing::warn!("stream_lag_loop: INFO persistence reply had no AOF status fields");
            }
        }
        Err(err) => {
            tracing::warn!(error = ?err, "stream_lag_loop: failed to fetch INFO persistence");
        }
    }
    sample
}

/// One integer field (`lag`, `pending`) of `XINFO GROUPS movement-events`
/// for one named group -- same reply-walk shape as
/// `crates/enricher/src/stream.rs::group_lag`, generalized over group name
/// (this function serves three group names from one binary; enricher's own
/// copy only ever serves one, `"enricher"`) and field.
async fn group_info_field(
    conn: &mut common::redis_conn::RedisConn,
    group: &str,
    field: &str,
) -> anyhow::Result<Option<i64>> {
    let reply: Vec<redis::Value> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg("movement-events")
        .query_async(conn)
        .await?;
    for entry in reply {
        let redis::Value::Array(fields) = entry else {
            continue;
        };
        let mut name: Option<String> = None;
        let mut value: Option<i64> = None;
        let mut it = fields.into_iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            let k: String = redis::from_redis_value(&k)?;
            if k == "name" {
                name = redis::from_redis_value(&v).ok();
            } else if k == field {
                value = redis::from_redis_value(&v).ok();
            }
        }
        if name.as_deref() == Some(group) {
            return Ok(value);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_sink::FakeEventSink;
    use crate::kafka_source::FakeRawSource;

    // `0008` ("Change of Location"), not `0005`, is the still-genuinely-
    // unconfirmed type here as of the H4 fix (2026-09-26 review): `0005`
    // (Reinstatement) moved into the confirmed set, see
    // `trust_schema::schema`'s own `CONFIRMED` list and module doc.
    const CONFIRMED_AND_UNKNOWN: &str = r#"[
        {"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
            "loc_stanox":"87701","variation_status":"LATE"
        }},
        {"header":{"msg_type":"0008"},"body":{"anything":"goes"}}
    ]"#;

    #[tokio::test]
    async fn a_batch_with_confirmed_and_unknown_types_publishes_only_confirmed() {
        let mut source = FakeRawSource::new(vec![vec![CONFIRMED_AND_UNKNOWN.to_string()]]);
        let mut sink = FakeEventSink::default();

        let outcome = run_cycle(&mut source, &mut sink).await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(sink.published.len(), 1);
        assert_eq!(sink.published[0].0, "0003");
    }

    #[tokio::test]
    async fn every_envelope_in_a_record_must_publish_before_the_offset_commits() {
        const TWO_CONFIRMED: &str = r#"[
            {"header":{"msg_type":"0001"},"body":{
                "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
                "train_service_code":"22345000","schedule_wtt_id":"WTT1",
                "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
            }},
            {"header":{"msg_type":"0003"},"body":{
                "train_id":"221832406","event_type":"DEPARTURE",
                "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
                "loc_stanox":"87701","variation_status":"LATE"
            }}
        ]"#;
        let mut source = FakeRawSource::new(vec![vec![TWO_CONFIRMED.to_string()]]);

        // Fails on the SECOND publish -- the first envelope's XADD succeeds
        // in isolation, but the whole record must still not commit, since
        // this record has another envelope that never made it through.
        // FakeEventSink's own `fail_next` only fails the very next call, so
        // a thin local wrapper is used to fail specifically on call #2.
        struct FailSecond {
            inner: FakeEventSink,
            calls: usize,
        }
        #[async_trait::async_trait]
        impl EventSink for FailSecond {
            async fn publish(&mut self, msg_type: &str, payload: &str) -> anyhow::Result<()> {
                self.calls += 1;
                if self.calls == 2 {
                    return Err(anyhow::anyhow!("simulated publish failure"));
                }
                self.inner.publish(msg_type, payload).await
            }
        }
        let mut sink = FailSecond {
            inner: FakeEventSink::default(),
            calls: 0,
        };

        let outcome = run_cycle(&mut source, &mut sink).await;

        assert_eq!(outcome, Cycle::Failed);
        assert_eq!(
            source.committed_count, 0,
            "a record with any envelope that failed to publish must not commit"
        );
    }

    /// PL-15c: an unclassifiable record commits (no retry, no backoff).
    #[tokio::test]
    async fn an_unclassifiable_record_is_committed_past_without_a_failed_cycle() {
        let mut source = FakeRawSource::new(vec![vec!["not json".to_string()]]);
        let mut sink = FakeEventSink::default();

        let outcome = run_cycle(&mut source, &mut sink).await;

        assert_eq!(
            outcome,
            Cycle::Committed,
            "no ERROR_BACKOFF for a skipped record"
        );
        assert_eq!(source.committed_offsets, vec![0]);
        assert!(sink.published.is_empty());
    }

    #[tokio::test]
    async fn a_clean_batch_commits_and_publishes() {
        let mut source = FakeRawSource::new(vec![vec![CONFIRMED_AND_UNKNOWN.to_string()]]);
        let mut sink = FakeEventSink::default();

        let outcome = run_cycle(&mut source, &mut sink).await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(source.committed_count, 1);
        assert_eq!(sink.published.len(), 1);
        // The movement_relay_events_published_total counter is incremented
        // alongside this, but not independently asserted here -- no
        // recorder is installed in this unit test, matching how
        // full-coverage-consumer/src/main.rs's own existing tests already
        // treat their metrics::counter! calls.
    }

    /// One confirmed (`0003`) movement record, distinguishable from the
    /// next by its `train_id` -- so a test can prove WHICH record reached
    /// `movement-events`, not merely how many did.
    fn movement_record(train_id: &str) -> String {
        format!(
            r#"[{{"header":{{"msg_type":"0003"}},"body":{{
                "train_id":"{train_id}","event_type":"DEPARTURE",
                "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
                "loc_stanox":"87701","variation_status":"LATE"
            }}}}]"#
        )
    }

    fn published_train_ids(published: &[(String, String)]) -> Vec<String> {
        published
            .iter()
            .map(|(_, payload)| {
                serde_json::from_str::<serde_json::Value>(payload).expect("payload is JSON")["body"]
                    ["train_id"]
                    .as_str()
                    .expect("train_id is a string")
                    .to_string()
            })
            .collect()
    }

    /// A sink that fails its first `fails_remaining` publishes, modelling a
    /// Redis outage that spans several retry cycles rather than exactly one.
    struct OutageSink {
        fails_remaining: usize,
        published: Vec<(String, String)>,
    }

    #[async_trait::async_trait]
    impl EventSink for OutageSink {
        async fn publish(&mut self, msg_type: &str, payload: &str) -> anyhow::Result<()> {
            if self.fails_remaining > 0 {
                self.fails_remaining -= 1;
                return Err(anyhow::anyhow!("simulated Redis outage"));
            }
            self.published
                .push((msg_type.to_string(), payload.to_string()));
            Ok(())
        }
    }

    /// Regression test for the dropped-record bug: a record whose
    /// downstream XADD failed must be RE-DELIVERED by the next cycle, not
    /// stepped over.
    ///
    /// Before the fix, `Cycle::Failed` left the record behind entirely:
    /// `next_batch` called `consumer.recv()` again on the next cycle, which
    /// advanced librdkafka's fetch position to the FOLLOWING record and
    /// overwrote `last_received`, so cycle 2 published record B and then
    /// committed B's offset -- implicitly committing past record A, which
    /// never reached `movement-events` and could never be re-read. This
    /// test asserts on the published train_ids and the committed offsets,
    /// both of which pinned that skip precisely: it used to see
    /// `["B"]` / `[1]` instead of `["A", "B"]` / `[0, 1]`.
    #[tokio::test]
    async fn a_record_whose_xadd_failed_is_redelivered_not_skipped() {
        let mut source = FakeRawSource::new(vec![
            vec![movement_record("AAA")],
            vec![movement_record("BBB")],
        ]);
        let mut sink = OutageSink {
            fails_remaining: 1,
            published: Vec::new(),
        };

        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Failed);
        assert!(
            sink.published.is_empty(),
            "the XADD failed, so nothing left"
        );
        assert_eq!(source.committed_count, 0);

        // Redis is back. This cycle must re-deliver AAA, NOT fetch BBB.
        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Committed);
        assert_eq!(
            published_train_ids(&sink.published),
            vec!["AAA".to_string()],
            "the record whose XADD failed must be the one re-delivered"
        );
        assert_eq!(
            source.committed_offsets,
            vec![0],
            "the commit must store the RETRIED record's own offset, never the next record's"
        );

        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Committed);
        assert_eq!(
            published_train_ids(&sink.published),
            vec!["AAA".to_string(), "BBB".to_string()],
            "and only then does the feed move on to the following record"
        );
        assert_eq!(source.committed_offsets, vec![0, 1]);
    }

    /// The same failure sustained across several retry cycles -- the real
    /// shape of a Redis outage, which used to lose roughly one record per
    /// `ERROR_BACKOFF` interval for as long as it lasted. Every record must
    /// survive, in order, with no gap in the committed offsets.
    #[tokio::test]
    async fn a_multi_cycle_downstream_outage_loses_no_record_at_all() {
        let ids = ["R0", "R1", "R2", "R3"];
        let mut source =
            FakeRawSource::new(ids.iter().map(|id| vec![movement_record(id)]).collect());
        // Fails the first three publish attempts: R0 is refused three times
        // over three consecutive cycles before the outage clears.
        let mut sink = OutageSink {
            fails_remaining: 3,
            published: Vec::new(),
        };

        for _ in 0..3 {
            assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Failed);
        }
        assert!(sink.published.is_empty());
        assert_eq!(source.committed_count, 0);

        for _ in 0..ids.len() {
            assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Committed);
        }

        assert_eq!(
            published_train_ids(&sink.published),
            ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
            "every record touched during the outage must still be published, in order"
        );
        assert_eq!(
            source.committed_offsets,
            vec![0, 1, 2, 3],
            "and every offset must be committed in sequence -- no skipped offset"
        );
    }

    /// A record that published but whose OFFSET commit failed is also
    /// retained: the following cycle re-publishes it (an accepted duplicate
    /// on an at-least-once stream) and commits its own offset. Before the
    /// fix, that record's offset was overwritten by the next `recv()` and
    /// the record's own offset was never committed, so a restart replayed
    /// from an older position -- and, worse, a failed publish in the same
    /// window was lost outright.
    #[tokio::test]
    async fn a_record_whose_offset_commit_failed_is_republished_then_committed() {
        let mut source = FakeRawSource::new(vec![vec![movement_record("AAA")]]);
        source.fail_next_commit = true;
        let mut sink = FakeEventSink::default();

        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Failed);
        assert_eq!(source.committed_offsets, Vec::<i64>::new());

        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Committed);
        assert_eq!(
            published_train_ids(&sink.published),
            vec!["AAA".to_string(), "AAA".to_string()],
            "a duplicate XADD is the accepted at-least-once outcome here"
        );
        assert_eq!(source.committed_offsets, vec![0]);
    }

    /// The deliberate exception: an unclassifiable record is NOT retained,
    /// because its bytes can never yield an envelope to publish -- retrying
    /// it forever would wedge the sole movement feed. The feed must make
    /// progress onto the next record instead.
    #[tokio::test]
    async fn an_unclassifiable_record_does_not_wedge_the_feed() {
        let mut source = FakeRawSource::new(vec![
            vec!["not json".to_string()],
            vec![movement_record("BBB")],
        ]);
        let mut sink = FakeEventSink::default();

        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Committed);
        assert_eq!(source.committed_offsets, vec![0]);

        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Committed);
        assert_eq!(
            published_train_ids(&sink.published),
            vec!["BBB".to_string()],
            "the poison record must not be re-delivered forever"
        );
        assert_eq!(source.committed_offsets, vec![0, 1]);
    }

    /// PL-4: one envelope with no `header.msg_type` costs only itself; the
    /// record's other envelopes still publish and the offset commits.
    #[tokio::test]
    async fn an_envelope_missing_msg_type_does_not_drop_its_neighbours() {
        let raw = r#"[
            {"header":{},"body":{"train_id":"XXX"}},
            {"header":{"msg_type":"0003"},"body":{"train_id":"AAA","event_type":"DEPARTURE"}}
        ]"#;
        let mut source = FakeRawSource::new(vec![vec![raw.to_string()]]);
        let mut sink = FakeEventSink::default();

        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Committed);
        assert_eq!(
            published_train_ids(&sink.published),
            vec!["AAA".to_string()]
        );
        assert_eq!(source.committed_count, 1);
    }

    #[tokio::test]
    async fn an_empty_poll_commits_nothing() {
        let mut source = FakeRawSource::new(vec![vec![]]);
        let mut sink = FakeEventSink::default();

        let outcome = run_cycle(&mut source, &mut sink).await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(source.committed_count, 0);
    }

    /// Regression test for the Signal Box Audit's "lag gauge omits a real
    /// consumer group" finding: it must monitor every real consumer group,
    /// `trust-event-backlog` included.
    #[test]
    fn stream_lag_groups_includes_all_three_real_consumer_groups() {
        assert_eq!(
            STREAM_LAG_GROUPS,
            [
                "trust-consumer",
                "full-coverage-consumer",
                "trust-event-backlog"
            ],
            "trust-event-backlog was silently missing from the lag gauge before this fix"
        );
    }

    /// A `LagConnection` fake that can be told to fail its first N connect
    /// attempts before succeeding -- models a Redis that isn't up yet when
    /// this gauge's own service starts, or a transient network blip.
    struct FakeLagConnection {
        lag_by_group: std::collections::HashMap<&'static str, i64>,
        /// Every group `group_lag` was actually called with, in call order --
        /// proves a group was queried, not merely that the fake knows a lag
        /// value for it.
        queried: Vec<&'static str>,
        /// `None` makes `stream_len` fail, modelling an `XLEN` error.
        stream_len: Option<u64>,
        /// `None` makes `deadletter_len` fail.
        deadletter_len: Option<u64>,
        /// `None` makes `persistence_info` fail.
        persistence_info: Option<String>,
        /// Every `min_id` `trim_deadletter` was called with.
        trimmed_with: Vec<String>,
        /// `None` makes `trim_deadletter` fail.
        trim_result: Option<u64>,
        /// `Err` (as `None`) makes `deadletter_oldest_id` fail.
        oldest_id: Option<Option<String>>,
    }

    const TEST_MAX_AGE: Duration = Duration::from_secs(86_400);
    const TEST_NOW_MS: u64 = 1_790_000_000_000;

    // `LagConnection::connect` is an associated function (no `&self`), so it
    // cannot read per-test instance state; these live in thread-locals
    // instead of process-wide statics so the two tests below (each on its
    // own OS thread, per the standard Rust test harness, and each driven by
    // a `#[tokio::test]` current-thread runtime that never hops threads) can
    // configure independent failure counts without racing each other.
    thread_local! {
        static CONNECT_FAILURES_REMAINING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static CONNECT_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    #[async_trait::async_trait]
    impl LagConnection for FakeLagConnection {
        async fn connect(_redis_url: &str) -> anyhow::Result<Self> {
            CONNECT_ATTEMPTS.with(|c| c.set(c.get() + 1));
            let remaining = CONNECT_FAILURES_REMAINING.with(|c| c.get());
            if remaining > 0 {
                CONNECT_FAILURES_REMAINING.with(|c| c.set(remaining - 1));
                return Err(anyhow::anyhow!("simulated Redis not up yet"));
            }
            let mut lag_by_group = std::collections::HashMap::new();
            lag_by_group.insert("trust-consumer", 5);
            lag_by_group.insert("full-coverage-consumer", 7);
            lag_by_group.insert("trust-event-backlog", 9);
            Ok(Self {
                lag_by_group,
                queried: Vec::new(),
                stream_len: Some(282),
                deadletter_len: Some(3),
                persistence_info: Some(HEALTHY_INFO_PERSISTENCE.to_string()),
                trimmed_with: Vec::new(),
                trim_result: Some(2),
                // 20 hours old at TEST_NOW_MS.
                oldest_id: Some(Some(format!("{}-0", TEST_NOW_MS - 20 * 3_600_000))),
            })
        }

        async fn group_lag(&mut self, group: &str) -> anyhow::Result<Option<i64>> {
            let owned = STREAM_LAG_GROUPS
                .iter()
                .find(|g| **g == group)
                .copied()
                .expect("test only ever queries groups from STREAM_LAG_GROUPS");
            self.queried.push(owned);
            Ok(self.lag_by_group.get(group).copied())
        }

        async fn group_pending(&mut self, group: &str) -> anyhow::Result<Option<i64>> {
            // Ten times the lag, so the two readings are distinguishable.
            Ok(self.lag_by_group.get(group).map(|lag| lag * 10))
        }

        async fn stream_len(&mut self) -> anyhow::Result<u64> {
            self.stream_len
                .ok_or_else(|| anyhow::anyhow!("simulated XLEN failure"))
        }

        async fn deadletter_len(&mut self) -> anyhow::Result<u64> {
            self.deadletter_len
                .ok_or_else(|| anyhow::anyhow!("simulated dead-letter XLEN failure"))
        }

        async fn persistence_info(&mut self) -> anyhow::Result<String> {
            self.persistence_info
                .clone()
                .ok_or_else(|| anyhow::anyhow!("simulated INFO failure"))
        }

        async fn trim_deadletter(&mut self, min_id: &str) -> anyhow::Result<u64> {
            self.trimmed_with.push(min_id.to_string());
            self.trim_result
                .ok_or_else(|| anyhow::anyhow!("simulated XTRIM failure"))
        }

        async fn deadletter_oldest_id(&mut self) -> anyhow::Result<Option<String>> {
            self.oldest_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("simulated XRANGE failure"))
        }
    }

    /// An abridged `INFO persistence` reply, in the field order and CRLF
    /// line endings redis 7.4.11 sends (taken from production, 2026-09-30).
    const HEALTHY_INFO_PERSISTENCE: &str = "# Persistence\r\nloading:0\r\n\
        rdb_last_bgsave_status:ok\r\naof_enabled:1\r\naof_rewrite_in_progress:0\r\n\
        aof_last_rewrite_time_sec:3\r\naof_last_bgrewrite_status:ok\r\n\
        aof_rewrites:4\r\naof_last_write_status:ok\r\naof_delayed_fsync:0\r\n";

    const HEALTHY: PersistenceStatus = PersistenceStatus {
        aof_enabled: true,
        aof_last_write_ok: true,
        aof_last_bgrewrite_ok: true,
    };

    #[test]
    fn a_healthy_info_persistence_reply_parses_as_healthy() {
        assert_eq!(
            parse_persistence_info(HEALTHY_INFO_PERSISTENCE),
            Some(HEALTHY)
        );
    }

    #[test]
    fn failed_aof_writes_and_rewrites_parse_as_not_ok() {
        let info = HEALTHY_INFO_PERSISTENCE
            .replace("aof_last_write_status:ok", "aof_last_write_status:err")
            .replace(
                "aof_last_bgrewrite_status:ok",
                "aof_last_bgrewrite_status:err",
            );
        assert_eq!(
            parse_persistence_info(&info),
            Some(PersistenceStatus {
                aof_enabled: true,
                aof_last_write_ok: false,
                aof_last_bgrewrite_ok: false,
            })
        );
    }

    #[test]
    fn aof_off_parses_as_disabled() {
        let info = HEALTHY_INFO_PERSISTENCE.replace("aof_enabled:1", "aof_enabled:0");
        assert_eq!(
            parse_persistence_info(&info).map(|s| s.aof_enabled),
            Some(false)
        );
    }

    /// A reply without the fields (another server, a renamed field) must
    /// report nothing rather than a healthy AOF.
    #[test]
    fn a_reply_missing_a_field_parses_as_unknown_not_healthy() {
        let info = HEALTHY_INFO_PERSISTENCE.replace("aof_last_write_status:ok\r\n", "");
        assert_eq!(parse_persistence_info(&info), None);
        assert_eq!(parse_persistence_info(""), None);
    }

    /// Regression test for the Signal Box Audit's "lag metric disables
    /// itself permanently on an initial connect failure" finding: a failed
    /// initial connection must NOT permanently disable the gauge. Before the
    /// fix, `stream_lag_loop` returned out of the whole function on its very
    /// first connect failure and never tried again; `run_lag_tick` must
    /// instead retry on a later tick and succeed once the fake stops
    /// simulating "Redis not up yet".
    #[tokio::test]
    async fn a_failed_initial_connection_is_retried_on_the_next_tick_not_disabled_forever() {
        CONNECT_FAILURES_REMAINING.with(|c| c.set(2));
        CONNECT_ATTEMPTS.with(|c| c.set(0));
        let mut conn: Option<FakeLagConnection> = None;

        // Two ticks fail to connect at all -- the gauge must not give up.
        run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;
        assert!(conn.is_none());
        run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;
        assert!(conn.is_none());

        // Third tick: the simulated outage has cleared.
        run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;
        assert!(
            conn.is_some(),
            "a later tick must succeed instead of the gauge staying disabled forever"
        );
        assert_eq!(
            CONNECT_ATTEMPTS.with(|c| c.get()),
            3,
            "every tick without a connection must attempt one"
        );

        // And once connected, it stays connected across ticks rather than
        // reconnecting every time.
        run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;
        assert_eq!(
            CONNECT_ATTEMPTS.with(|c| c.get()),
            3,
            "an already-open connection must not be re-established every tick"
        );
    }

    /// Once connected, every group in `STREAM_LAG_GROUPS` -- all three of
    /// them -- must actually be queried, not just the first two.
    #[tokio::test]
    async fn every_configured_group_is_queried_once_connected() {
        CONNECT_FAILURES_REMAINING.with(|c| c.set(0));
        let mut conn: Option<FakeLagConnection> = None;

        run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;

        let conn = conn.expect("connect succeeds immediately here");
        assert_eq!(
            conn.queried,
            STREAM_LAG_GROUPS.to_vec(),
            "every group in STREAM_LAG_GROUPS -- trust-event-backlog included -- \
             must actually be queried in one tick, not just the first two"
        );
    }

    /// A tick reports the stream's length alongside every group's lag --
    /// the length/cap gauges the chart's lag alerts divide by.
    #[tokio::test]
    async fn a_connected_tick_reports_stream_length_and_every_group_lag() {
        CONNECT_FAILURES_REMAINING.with(|c| c.set(0));
        let mut conn: Option<FakeLagConnection> = None;

        let sample = run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;

        assert_eq!(
            sample,
            LagSample {
                stream_length: Some(282),
                group_lags: vec![
                    ("trust-consumer", 5),
                    ("full-coverage-consumer", 7),
                    ("trust-event-backlog", 9),
                ],
                group_pending: vec![
                    ("trust-consumer", 50),
                    ("full-coverage-consumer", 70),
                    ("trust-event-backlog", 90),
                ],
                deadletter_length: Some(3),
                deadletter_trimmed: Some(2),
                deadletter_oldest_age_secs: Some(20 * 3600),
                persistence: Some(HEALTHY),
            }
        );
    }

    /// D5: every tick trims at exactly `now - max age`, and an empty
    /// dead-letter stream reports an oldest age of 0.
    #[tokio::test]
    async fn every_tick_trims_dead_letters_older_than_the_max_age() {
        CONNECT_FAILURES_REMAINING.with(|c| c.set(0));
        let mut conn: Option<FakeLagConnection> = None;
        run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;
        conn.as_mut().expect("connected").oldest_id = Some(None);

        let sample = run_lag_tick(
            "redis://fake",
            &mut conn,
            Duration::from_secs(3_600),
            TEST_NOW_MS,
        )
        .await;

        assert_eq!(
            conn.expect("connected").trimmed_with,
            [
                format!("{}-0", TEST_NOW_MS - 86_400_000),
                format!("{}-0", TEST_NOW_MS - 3_600_000),
            ]
        );
        assert_eq!(sample.deadletter_oldest_age_secs, Some(0));
    }

    /// A failed trim or oldest-record read costs only its own reading.
    #[tokio::test]
    async fn a_failed_trim_still_reports_the_rest() {
        CONNECT_FAILURES_REMAINING.with(|c| c.set(0));
        let mut conn: Option<FakeLagConnection> = None;
        run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;
        let active = conn.as_mut().expect("connected");
        active.trim_result = None;
        active.oldest_id = None;

        let sample = run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;

        assert_eq!(sample.deadletter_trimmed, None);
        assert_eq!(sample.deadletter_oldest_age_secs, None);
        assert_eq!(sample.deadletter_length, Some(3));
    }

    /// A failed dead-letter XLEN or INFO costs only its own reading.
    #[tokio::test]
    async fn a_failed_deadletter_xlen_or_info_still_reports_the_rest() {
        CONNECT_FAILURES_REMAINING.with(|c| c.set(0));
        let mut conn: Option<FakeLagConnection> = None;
        run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;
        let active = conn.as_mut().expect("connected");
        active.deadletter_len = None;
        active.persistence_info = None;

        let sample = run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;

        assert_eq!(sample.deadletter_length, None);
        assert_eq!(sample.persistence, None);
        assert_eq!(sample.stream_length, Some(282));
        assert_eq!(sample.group_lags.len(), 3);
    }

    /// An `XLEN` failure must not cost the tick its lag readings.
    #[tokio::test]
    async fn an_xlen_failure_still_reports_group_lag() {
        CONNECT_FAILURES_REMAINING.with(|c| c.set(0));
        let mut conn: Option<FakeLagConnection> = None;
        run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;
        conn.as_mut().expect("connected").stream_len = None;

        let sample = run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;

        assert_eq!(sample.stream_length, None);
        assert_eq!(sample.group_lags.len(), 3);
    }

    /// No connection this tick: nothing observed, so nothing to report
    /// (the configured cap is still published by `publish_lag_sample`).
    #[tokio::test]
    async fn a_failed_connect_reports_an_empty_sample() {
        CONNECT_FAILURES_REMAINING.with(|c| c.set(1));
        let mut conn: Option<FakeLagConnection> = None;

        let sample = run_lag_tick("redis://fake", &mut conn, TEST_MAX_AGE, TEST_NOW_MS).await;

        assert_eq!(sample, LagSample::default());
    }

    /// The chart's PrometheusRule (charts/distant-signal/templates/
    /// prometheusrule.yaml) references these exact names; renaming one
    /// silently breaks an alert.
    #[test]
    fn stream_gauge_names_match_the_chart_alert_rules() {
        assert_eq!(
            common::metrics::metric_name(STREAM_LAG_METRIC),
            "distant_signal_movement_relay_stream_lag"
        );
        assert_eq!(
            common::metrics::metric_name(STREAM_LENGTH_METRIC),
            "distant_signal_movement_relay_stream_length"
        );
        assert_eq!(
            common::metrics::metric_name(STREAM_MAXLEN_METRIC),
            "distant_signal_movement_relay_stream_maxlen"
        );
        assert_eq!(
            common::metrics::metric_name(STREAM_PENDING_METRIC),
            "distant_signal_movement_relay_stream_pending"
        );
        assert_eq!(
            common::metrics::metric_name(DEADLETTER_LENGTH_METRIC),
            "distant_signal_movement_relay_deadletter_length"
        );
        assert_eq!(
            common::metrics::metric_name(DEADLETTER_TRIMMED_METRIC),
            "distant_signal_movement_relay_deadletter_trimmed_total"
        );
        assert_eq!(
            common::metrics::metric_name(DEADLETTER_OLDEST_AGE_METRIC),
            "distant_signal_movement_relay_deadletter_oldest_age_seconds"
        );
        assert_eq!(
            common::metrics::metric_name(AOF_ENABLED_METRIC),
            "distant_signal_redis_aof_enabled"
        );
        assert_eq!(
            common::metrics::metric_name(AOF_LAST_WRITE_OK_METRIC),
            "distant_signal_redis_aof_last_write_ok"
        );
        assert_eq!(
            common::metrics::metric_name(AOF_LAST_BGREWRITE_OK_METRIC),
            "distant_signal_redis_aof_last_bgrewrite_ok"
        );
    }

    /// `publish_lag_sample` must be callable without a recorder installed
    /// (the `metrics` macros are no-ops then) -- guards against a panic in
    /// the tick path when `--metrics-enabled` is off.
    #[test]
    fn publishing_a_sample_without_a_recorder_is_a_no_op() {
        publish_lag_sample(
            &LagSample {
                stream_length: Some(1),
                group_lags: vec![("trust-consumer", 1)],
                group_pending: vec![("trust-consumer", 1)],
                deadletter_length: Some(0),
                deadletter_trimmed: Some(0),
                deadletter_oldest_age_secs: Some(0),
                persistence: Some(HEALTHY),
            },
            1_048_576,
        );
    }
}
