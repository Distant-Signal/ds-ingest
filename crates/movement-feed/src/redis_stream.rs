//! `RedisStreamMovementFeed`: the real, production `MovementFeed`
//! implementation from Deploy B onward -- reads the `movement-events`
//! stream `movement-relay` writes to, as one of its two fixed consumer
//! groups (`trust-consumer` or `full-coverage-consumer`).
//! See docs/superpowers/specs/2026-09-04-movement-relay-design.md
//! Decision 2 for the full reasoning; this module implements it, it does
//! not re-argue it.

use std::time::Duration;

use async_trait::async_trait;
use common::redis_conn::RedisConn;
use redis::AsyncCommands;

use crate::{DeadLetter, DeadLetterSink, MovementFeed};

const STREAM: &str = "movement-events";

/// Suffix of the dead-letter stream, so the production one is
/// `movement-events-deadletter` and each test stream gets its own.
const DEAD_LETTER_SUFFIX: &str = "-deadletter";

/// Hard cap on the dead-letter stream's length. It is **never trimmed to
/// this cap** (by count): it only ever holds genuinely poison records (explicit data rejections
/// and malformed entries, see [`RedisStreamMovementFeed::reject_batch`]),
/// so a full stream means something systemic is wrong, and silently
/// evicting the oldest poison record to make room would lose it. Instead a
/// write that would exceed the cap is refused (`Err`, logged, counted as
/// `distant_signal_movement_feed_deadletter_full_total`), which leaves the
/// entry pending in its consumer group -- delayed, not lost -- until an
/// operator drains the dead-letter stream (see
/// `docs/movement-events-deadletter.md`). Records are well under 1KB, so
/// this bounds it to roughly 10MB. Shared by all three consumer groups;
/// each record carries its `group`.
///
/// Records are trimmed by AGE, though: movement-relay deletes any record
/// older than `--deadletter-max-age-secs` (at most 24h, the TRUST 1-day
/// retention safeguard; `crates/movement-relay/src/deadletter.rs`), and
/// alerts hours beforehand so it can be re-injected first.
const DEAD_LETTER_MAX_LEN: usize = 10_000;

/// Counter (labelled `group`) of consumer groups recreated after `NOGROUP`
/// ([`RedisStreamMovementFeed::recreate_group`]): Redis came back without
/// its data (a lost or unreadable AOF, a replaced volume) or someone deleted
/// the group. Either way the group's pending entries and, after an empty
/// restart, every entry it had not yet read are gone, and `check_gap`
/// cannot see it (the new stream has no trimmed range). Registered at 0 so
/// the chart's `DistantSignalMovementGroupRecreated` alert can use a plain
/// `increase()`.
const GROUP_RECREATED_METRIC: &str = "movement_feed_group_recreated_total";

/// Counter (labelled `group`) of consumer groups found BEHIND the last entry
/// this consumer was handed after a failed read, and moved forward to it
/// (`XGROUP SETID`; [`restore_group_position`]): Redis came back with an
/// older copy of its data, so the group still exists but would hand out
/// again entries already processed. Registered at 0.
const GROUP_RESTORED_METRIC: &str = "movement_feed_group_position_restored_total";

/// Counter (labelled `group`) of delivered entries whose stream id has no
/// parseable millisecond part, so their arrival time is unknown and the
/// consumer stamps them with its own clock instead (see
/// [`crate::FeedEntry::received_at`]). Redis only ever generates `<ms>-<seq>`
/// ids, so anything above 0 means something unexpected wrote the stream.
const ENTRY_ID_UNPARSEABLE_METRIC: &str = "movement_feed_entry_id_unparseable_total";

/// Delivery count past which a pending entry the PEL replay hands out again
/// is reported as long-pending (a warn log plus
/// `distant_signal_movement_feed_long_pending_total`). **Visibility only**:
/// nothing is ever dead-lettered for its delivery count.
///
/// Delivery counts climb by about 2 per `autoclaim_min_idle` sweep while an
/// entry keeps failing (`XAUTOCLAIM` adds one and the id-`0` PEL re-read
/// adds another -- checked against a local valkey 9), so at the deployed
/// 30s sweep this is roughly an hour of continuous failure. The count
/// cannot tell a poison entry from a healthy one stuck behind a downstream
/// outage, which is why it used to be a dead-letter trigger (moving every
/// healthy entry of a >1h `api` outage out of all three groups) and is now
/// only a signal.
pub const LONG_PENDING_DELIVERIES: u64 = 240;

/// How many pending entries a single id=`0` PEL-replay read (`next_batch`)
/// or a single `XAUTOCLAIM` round-trip (`reclaim_stale`) asks Redis for at
/// once.
///
/// **Why this was raised from 100** (Repeater Signal review, finding M15):
/// `next_batch` used to perform only ONE id=`0` read per replay activation
/// before switching back to `>` -- so the number of entries a single PEL
/// replay actually delivered was capped by this count, not by how many
/// were sitting in the PEL. (It now keeps reading until a short batch;
/// see `pel_replay_cursor`. The rest of this note is the history of why
/// the count was raised as well.) After a
/// consumer rename (or any event that reassigns a large backlog to a fresh
/// consumer name via `reclaim_stale`), each further batch only becomes
/// deliverable once it has aged past `autoclaim_min_idle` again and the
/// next periodic sweep re-claims it -- i.e. at most one `count`-sized batch
/// per sweep interval. At the old 100 and a 30s sweep interval (this
/// crate's real deployed default -- see e.g. `trust-consumer`'s
/// `redis_autoclaim_min_idle_secs`), a 5,000-entry backlog took roughly 25
/// minutes (50 sweeps * 30s) of delayed, out-of-order replay to fully
/// drain, confirmed against a live cluster.
///
/// 1000 was chosen, not just doubled or left at 100:
/// - It cuts that same 5,000-entry drain to ~5 sweeps (~2.5 minutes),
///   a real improvement rather than a marginal one.
/// - Individual `movement-events` entries are small TRUST envelopes (a
///   handful of string/optional-string fields per `trust-schema::schema`,
///   typically well under 1KB serialized) -- 1000 of them in memory at
///   once is on the order of hundreds of KB to ~1MB, not a memory
///   concern, and nowhere close to problematic relative to the stream's
///   own hard `MAXLEN 500_000` cap.
/// - It does not require also decoupling the sweep-check interval from
///   `autoclaim_min_idle`: that interval already fires as promptly as an
///   entry can become re-eligible (an entry re-claimed at a sweep is only
///   re-claimable once it has been idle for another full
///   `autoclaim_min_idle`, so a same-length check interval already catches
///   it essentially as soon as it is eligible) -- the batch count, not the
///   interval, was the actual bottleneck.
const PEL_REPLAY_BATCH_COUNT: usize = 1000;

/// Batch size for ordinary `>` (new-entry) reads -- unchanged from the
/// original 100; see `PEL_REPLAY_BATCH_COUNT` for why only the replay path
/// was raised.
const LIVE_READ_BATCH_COUNT: usize = 100;

/// How long an ordinary `>` read waits server-side for new entries
/// (`XREADGROUP ... BLOCK`). PEL-replay reads pass `BLOCK 0`, which Redis
/// ignores for an explicit id.
const LIVE_READ_BLOCK_MS: usize = 5000;

// Every command is bounded by `common::redis_conn::RESPONSE_TIMEOUT`, so a
// blocking read must return well within it or a quiet stream would look
// like a dead connection.
const _: () =
    assert!((LIVE_READ_BLOCK_MS as u128) * 2 < common::redis_conn::RESPONSE_TIMEOUT.as_millis());

pub struct RedisStreamMovementFeed {
    conn: RedisConn,
    stream: String,
    group: String,
    consumer: String,
    /// `Some(id)` while this consumer is replaying its own pending-entries
    /// list: the next read asks for pending entries after `id` (`"0"` to
    /// start from the beginning) instead of new entries (`>`). Starts as
    /// `Some("0")` for the startup replay, and is reset to `Some("0")` by
    /// `reclaim_stale` after a non-empty `XAUTOCLAIM` claim (so a reclaimed
    /// entry is picked up through the same code path -- see that
    /// function's own doc) and by `reject_batch` when it starts isolation.
    ///
    /// **M15 (Repeater Signal review).** An ordinary replay advances this
    /// cursor past each full batch and keeps reading until a short batch,
    /// rather than doing a single `0` read and switching to `>`. With the
    /// single read, only `PEL_REPLAY_BATCH_COUNT` entries were delivered per
    /// replay; the rest waited for the next `XAUTOCLAIM` sweep (one
    /// `autoclaim_min_idle` later), so a large reclaimed backlog drained one
    /// batch per sweep. Isolation (see `isolating`) does not advance it: it
    /// keeps re-reading id `0` one entry at a time until the PEL is empty.
    pel_replay_cursor: Option<String>,
    /// `(id, payload)` of every entry returned by the most recent
    /// `next_batch` call, held until `commit` XACKs them, `reject_batch`
    /// acts on them, or they're replaced by the next call -- the
    /// receive/confirm split the removed Kafka backend's `last_received`
    /// established, generalized to a `Vec` since one Redis Streams read
    /// can return more than one entry per call. The payload is kept so a
    /// rejected entry can be dead-lettered intact.
    pending: Vec<(String, String)>,
    /// Set by `reject_batch` on a multi-entry batch: the downstream refused
    /// the batch's data but cannot say which entry was bad, so the pending
    /// entries are re-read ONE AT A TIME (id-`0` reads with `COUNT 1`)
    /// until the pending-entries list is empty. A rejection of a
    /// single-entry batch is then attributable to that entry alone.
    isolating: bool,
    last_autoclaim_sweep: std::time::Instant,
    autoclaim_min_idle: Duration,
    /// This process's copy of the group's `last-delivered-id`: seeded from
    /// `XINFO GROUPS` at connect (and after [`Self::recreate_group`]) and
    /// advanced by every `>` read to the last id it returned, exactly as
    /// Redis advances the group's own. `None` only when `XINFO GROUPS`
    /// could not be read. It is what a group lost to `NOGROUP` is
    /// recreated at -- see [`recreate_start_id`] -- and what a group that
    /// went backwards is moved forward to (see `verify_position`).
    last_delivered_id: Option<String>,
    /// Set by a failed read other than `NOGROUP`: before the next read,
    /// [`restore_group_position`] checks the group did not go backwards
    /// (2026-10-09 redelivery review). A Redis restart that reloads an older
    /// copy of its data keeps the group but rewinds its `last-delivered-id`,
    /// and the restart is what failed the read.
    verify_position: bool,
}

impl RedisStreamMovementFeed {
    /// `group` is one of a small number of fixed literals
    /// (`"trust-consumer"` / `"full-coverage-consumer"` /
    /// `"trust-event-backlog"`, one per consumer crate) -- see each
    /// crate's own `main.rs` call site (Task 4;
    /// docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md
    /// Task 10 for the third). `consumer` is a fixed per-deployment name (e.g.
    /// `"trust-consumer-1"`), matching `enricher::stream::CONSUMER`'s own
    /// one-fixed-name convention and this design's own
    /// single-replica constraint (design doc Decision 2).
    ///
    /// One bounded attempt (see `common::redis_conn`): errors within
    /// seconds if Redis is unreachable. Services start through
    /// [`Self::connect_until_ready`] instead.
    pub async fn connect(
        redis_url: &str,
        group: impl Into<String>,
        consumer: impl Into<String>,
        autoclaim_min_idle: Duration,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Self::connect_to_stream(&client, STREAM, group, consumer, autoclaim_min_idle).await
    }

    /// [`Self::connect`], retried on `backoff` until it succeeds (INF-5):
    /// Redis unreachable, still loading its AOF (`LOADING`), or failing
    /// the consumer-group setup is waited for, not exited on. Each failed
    /// attempt is logged and beats `progress`, so the worker's `/livez`
    /// stays 200 while this waits and a Redis outage at startup cannot get
    /// the pod killed. Pass `common::startup::CONNECT_BACKOFF` outside
    /// tests. Only an unparseable `redis_url` is an error.
    ///
    /// Once connected, every command is bounded (one reconnect attempt of
    /// at most `common::redis_conn::CONNECT_TIMEOUT`, a reply within
    /// `RESPONSE_TIMEOUT`) and returns its error to the caller, whose loop
    /// backs off and retries -- redis-rs never retries internally.
    pub async fn connect_until_ready(
        redis_url: &str,
        group: &str,
        consumer: &str,
        autoclaim_min_idle: Duration,
        backoff: common::backoff::Backoff,
        progress: &health_http::Progress,
    ) -> anyhow::Result<Self> {
        Self::connect_stream_until_ready(
            redis_url,
            STREAM,
            group,
            consumer,
            autoclaim_min_idle,
            backoff,
            progress,
        )
        .await
    }

    async fn connect_stream_until_ready(
        redis_url: &str,
        stream: &str,
        group: &str,
        consumer: &str,
        autoclaim_min_idle: Duration,
        backoff: common::backoff::Backoff,
        progress: &health_http::Progress,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(common::startup::retry_until_ready(
            "Redis movement-events stream",
            backoff,
            Some(progress),
            || Self::connect_to_stream(&client, stream, group, consumer, autoclaim_min_idle),
        )
        .await)
    }

    /// Test-only constructor: connects against an explicit stream name
    /// rather than the fixed `movement-events` literal, so each
    /// `#[ignore]`-gated integration test in `redis_tests` below can use
    /// its own uniquely-named stream/group and never collide with another
    /// test (or a real deployment) sharing the same Redis instance.
    #[cfg(test)]
    async fn connect_for_test(
        redis_url: &str,
        stream: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
        autoclaim_min_idle: Duration,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Self::connect_to_stream(&client, &stream.into(), group, consumer, autoclaim_min_idle).await
    }

    /// [`Self::connect`] against an explicitly named stream, for other
    /// crates' own `#[ignore]`-gated stream tests (each wants a unique
    /// stream so it never collides with another test or a deployment).
    #[cfg(any(test, feature = "test-util"))]
    pub async fn connect_to_named_stream(
        redis_url: &str,
        stream: &str,
        group: impl Into<String>,
        consumer: impl Into<String>,
        autoclaim_min_idle: Duration,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Self::connect_to_stream(&client, stream, group, consumer, autoclaim_min_idle).await
    }

    /// One bounded connect attempt plus the consumer-group setup; any
    /// failure is returned (see [`Self::connect_until_ready`] for the
    /// retrying form).
    #[expect(
        clippy::unwrap_used,
        reason = "Instant::now() is far past any autoclaim idle time"
    )]
    async fn connect_to_stream(
        client: &redis::Client,
        stream: &str,
        group: impl Into<String>,
        consumer: impl Into<String>,
        autoclaim_min_idle: Duration,
    ) -> anyhow::Result<Self> {
        let mut conn = common::redis_conn::connect(client).await?;
        let group = group.into();

        ensure_group(&mut conn, stream, &group, "$").await?;
        let last_delivered_id = seed_last_delivered_id(&mut conn, stream, &group).await;

        // Register the dead-letter counters at 0 for this group, so the
        // DistantSignalDeadLetterGrowing/Full alerts can use a plain
        // `increase()` instead of also firing on a new series (which, with
        // a fresh pod's series, fired on every rollout).
        for reason in crate::DEAD_LETTER_REASONS {
            metrics::counter!(
                common::metrics::metric_name("movement_feed_deadlettered_total"),
                "group" => group.clone(),
                "reason" => reason
            )
            .increment(0);
        }
        metrics::counter!(
            common::metrics::metric_name("movement_feed_deadletter_full_total"),
            "group" => group.clone()
        )
        .increment(0);
        metrics::counter!(
            common::metrics::metric_name(GROUP_RECREATED_METRIC),
            "group" => group.clone()
        )
        .increment(0);
        metrics::counter!(
            common::metrics::metric_name(GROUP_RESTORED_METRIC),
            "group" => group.clone()
        )
        .increment(0);
        // ...and the long-pending counter, for DistantSignalMovementFeedLongPending.
        metrics::counter!(
            common::metrics::metric_name("movement_feed_long_pending_total"),
            "group" => group.clone()
        )
        .increment(0);
        metrics::counter!(
            common::metrics::metric_name(ENTRY_ID_UNPARSEABLE_METRIC),
            "group" => group.clone()
        )
        .increment(0);

        Ok(Self {
            conn,
            stream: stream.to_string(),
            group,
            consumer: consumer.into(),
            pel_replay_cursor: Some("0".to_string()),
            pending: Vec::new(),
            isolating: false,
            last_autoclaim_sweep: std::time::Instant::now()
                .checked_sub(autoclaim_min_idle)
                .unwrap(),
            autoclaim_min_idle,
            last_delivered_id,
            verify_position: false,
        })
    }

    fn dead_letter_stream(&self) -> String {
        format!("{}{DEAD_LETTER_SUFFIX}", self.stream)
    }

    /// Delivery counts, from `XPENDING`'s extended form, for this
    /// consumer's pending entries between `first` and `last` inclusive.
    async fn delivery_counts(
        &mut self,
        first: &str,
        last: &str,
        count: usize,
    ) -> anyhow::Result<std::collections::HashMap<String, u64>> {
        let reply: redis::streams::StreamPendingCountReply = self
            .conn
            .xpending_consumer_count(
                &self.stream,
                &self.group,
                first,
                last,
                count,
                &self.consumer,
            )
            .await?;
        Ok(reply
            .ids
            .into_iter()
            .map(|p| (p.id, p.times_delivered as u64))
            .collect())
    }

    /// Visibility for entries stuck pending: logs (and counts) every
    /// PEL-replay entry already delivered more than
    /// [`LONG_PENDING_DELIVERIES`] times. Never diverts or ACKs anything --
    /// a high delivery count is what a healthy entry behind a long `api`
    /// outage looks like too. Best effort: an `XPENDING` failure is only
    /// logged, it does not fail the read.
    async fn report_long_pending(&mut self, entries: &[(String, String)]) {
        let (Some((first, _)), Some((last, _))) = (entries.first(), entries.last()) else {
            return;
        };
        let (first, last) = (first.clone(), last.clone());
        let counts = match self
            .delivery_counts(&first, &last, PEL_REPLAY_BATCH_COUNT)
            .await
        {
            Ok(counts) => counts,
            Err(err) => {
                tracing::warn!(error = ?err, group = %self.group, "XPENDING failed; skipping the long-pending check");
                return;
            }
        };
        let long_pending = long_pending_entries(entries, &counts, LONG_PENDING_DELIVERIES);
        if long_pending.is_empty() {
            return;
        }
        let max_delivered = long_pending.iter().map(|(_, n)| *n).max().unwrap_or(0);
        tracing::warn!(
            stream = %self.stream,
            group = %self.group,
            entries = long_pending.len(),
            max_delivered,
            oldest = %long_pending[0].0,
            "pending entries have been redelivered for over an hour; still retrying them \
             (a downstream outage, or a failure the consumer cannot classify as a data rejection)"
        );
        metrics::counter!(
            common::metrics::metric_name("movement_feed_long_pending_total"),
            "group" => self.group.clone()
        )
        .increment(long_pending.len() as u64);
    }

    /// The downstream explicitly rejected the data in the batch the last
    /// `next_batch` returned (a 400/413/422, per
    /// `common::ingest::classify_failure`) -- as opposed to a transient
    /// failure, after which the caller simply does not `commit` and the
    /// entries stay pending to be retried indefinitely.
    ///
    /// - A single-entry batch: that entry is the poison. It is written to
    ///   the dead-letter stream (reason `rejected_by_api`, payload intact)
    ///   and `XACKed`. If the dead-letter write fails, nothing is `ACKed`.
    /// - A multi-entry batch: the bad entry cannot be identified, so none is
    ///   dead-lettered. The feed switches to isolation (see `isolating`),
    ///   re-reading its pending entries one at a time so the next rejection
    ///   is attributable; healthy batch-mates are simply committed on their
    ///   own.
    pub async fn reject_batch(&mut self, detail: &str) -> anyhow::Result<()> {
        match self.pending.len() {
            0 => Ok(()),
            1 => {
                let (id, payload) = self.pending[0].clone();
                self.dead_letter(&[DeadLetter {
                    reason: "rejected_by_api",
                    source_id: Some(id.clone()),
                    delivery_count: None,
                    payload,
                    detail: detail.to_string(),
                }])
                .await?;
                let _: i64 = self.conn.xack(&self.stream, &self.group, &[&id]).await?;
                self.pending.clear();
                Ok(())
            }
            n => {
                tracing::warn!(
                    group = %self.group,
                    entries = n,
                    detail,
                    "downstream rejected a multi-entry batch; re-reading its entries one at a \
                     time to find the one it refuses"
                );
                self.pending.clear();
                self.isolating = true;
                self.pel_replay_cursor = Some("0".to_string());
                Ok(())
            }
        }
    }

    /// Dead-letters malformed entries (no usable `payload` field), so the
    /// record survives the XACK that retires them.
    async fn dead_letter_malformed(
        &mut self,
        malformed: &[(String, String)],
    ) -> anyhow::Result<()> {
        let records: Vec<DeadLetter> = malformed
            .iter()
            .map(|(id, fields)| DeadLetter {
                reason: "malformed_entry",
                source_id: Some(id.clone()),
                delivery_count: None,
                payload: fields.clone(),
                detail: "stream entry has no usable `payload` field".to_string(),
            })
            .collect();
        self.dead_letter(&records).await
    }
}

/// Pure half of `report_long_pending`: the entries XPENDING reports as
/// delivered more than `threshold` times, with their counts. An entry
/// XPENDING did not report (acked meanwhile, or beyond the reply) is not
/// long-pending.
fn long_pending_entries(
    entries: &[(String, String)],
    counts: &std::collections::HashMap<String, u64>,
    threshold: u64,
) -> Vec<(String, u64)> {
    entries
        .iter()
        .filter_map(|(id, _)| match counts.get(id) {
            Some(&delivered) if delivered > threshold => Some((id.clone(), delivered)),
            _ => None,
        })
        .collect()
}

#[async_trait]
impl DeadLetterSink for RedisStreamMovementFeed {
    /// `XADD <stream>-deadletter` per record (one pipeline, no trimming --
    /// see [`DEAD_LETTER_MAX_LEN`]), each with its `group`, `consumer`,
    /// `reason`, `source_id`, `delivery_count`, `detail` and `payload`, plus
    /// a warn log and a
    /// `distant_signal_movement_feed_deadlettered_total{group, reason}`
    /// increment. The Redis stream is the record of truth (it keeps the
    /// payload, so an operator can inspect it with `XRANGE` and re-inject
    /// it once the cause is fixed -- `docs/movement-events-deadletter.md`);
    /// the log and metric are for alerting. Refuses (`Err`, nothing
    /// written) when the stream is already at its cap.
    #[expect(
        clippy::cast_precision_loss,
        reason = "metric gauges take f64, and these counts and timestamps stay far below 2^52"
    )]
    async fn dead_letter(&mut self, records: &[DeadLetter]) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let stream = self.dead_letter_stream();
        let len: usize = self.conn.xlen(&stream).await?;
        if len + records.len() > DEAD_LETTER_MAX_LEN {
            tracing::error!(
                stream = %stream,
                group = %self.group,
                len,
                cap = DEAD_LETTER_MAX_LEN,
                refused = records.len(),
                "dead-letter stream is full; refusing to trim it, so these records stay \
                 pending until it is drained (see docs/movement-events-deadletter.md)"
            );
            metrics::counter!(
                common::metrics::metric_name("movement_feed_deadletter_full_total"),
                "group" => self.group.clone()
            )
            .increment(records.len() as u64);
            anyhow::bail!("dead-letter stream {stream} is full ({len}/{DEAD_LETTER_MAX_LEN})");
        }
        let mut pipe = redis::pipe();
        for record in records {
            pipe.cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("group")
                .arg(&self.group)
                .arg("consumer")
                .arg(&self.consumer)
                .arg("reason")
                .arg(record.reason)
                .arg("source_id")
                .arg(record.source_id.as_deref().unwrap_or(""))
                .arg("delivery_count")
                .arg(
                    record
                        .delivery_count
                        .map(|c| c.to_string())
                        .unwrap_or_default(),
                )
                .arg("detail")
                .arg(&record.detail)
                .arg("payload")
                .arg(&record.payload)
                .ignore();
        }
        let _: () = pipe.query_async(&mut self.conn).await?;
        for record in records {
            tracing::warn!(
                stream = %stream,
                group = %self.group,
                reason = record.reason,
                source_id = ?record.source_id,
                delivery_count = ?record.delivery_count,
                detail = %record.detail,
                payload = %record.payload,
                "dead-lettered a poison record instead of retrying it forever"
            );
            metrics::counter!(
                common::metrics::metric_name("movement_feed_deadlettered_total"),
                "group" => self.group.clone(),
                "reason" => record.reason
            )
            .increment(1);
        }
        metrics::gauge!(
            common::metrics::metric_name("movement_feed_deadletter_length"),
            "stream" => stream
        )
        .set((len + records.len()) as f64);
        Ok(())
    }
}

/// Idempotent group creation, `MKSTREAM`-backed -- verbatim in spirit from
/// `crates/enricher/src/stream.rs::ensure_group`, generalized over the
/// stream/group name (this crate serves two different group names -- and,
/// in tests, many different stream names -- from one implementation,
/// unlike enricher's single hardcoded `STREAM`/`GROUP`). A new group starts
/// after `start_id`: `$` (the stream's tail) at startup, see
/// [`recreate_start_id`] after `NOGROUP`.
async fn ensure_group(
    conn: &mut RedisConn,
    stream: &str,
    group: &str,
    start_id: &str,
) -> anyhow::Result<()> {
    let result: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(group)
        .arg(start_id)
        .arg("MKSTREAM")
        .query_async(conn)
        .await;

    match result {
        Ok(()) => Ok(()),
        Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// The group's `last-delivered-id` from `XINFO GROUPS`, or `None` (logged)
/// if it cannot be read -- the caller then only loses the precise
/// `NOGROUP` recovery position, see [`recreate_start_id`].
async fn seed_last_delivered_id(conn: &mut RedisConn, stream: &str, group: &str) -> Option<String> {
    let result: anyhow::Result<Option<String>> = async {
        let groups: Vec<redis::Value> = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(stream)
            .query_async(conn)
            .await?;
        let fields = find_group_fields(&groups, group)?;
        match fields.as_ref().and_then(|f| f.get("last-delivered-id")) {
            Some(value) => optional_string(value),
            None => Ok(None),
        }
    }
    .await;
    match result {
        Ok(id) => id,
        Err(err) => {
            tracing::warn!(error = ?err, stream, group, "could not read the consumer group's last-delivered-id");
            None
        }
    }
}

/// The stream's `last-generated-id` (`XINFO STREAM`), or `None` when the
/// stream does not exist.
async fn stream_last_generated_id(
    conn: &mut RedisConn,
    stream: &str,
) -> anyhow::Result<Option<String>> {
    let exists: bool = conn.exists(stream).await?;
    if !exists {
        return Ok(None);
    }
    let info: Vec<redis::Value> = redis::cmd("XINFO")
        .arg("STREAM")
        .arg(stream)
        .query_async(conn)
        .await?;
    let fields = flat_fields(&info)?;
    let id = match fields.get("last-generated-id") {
        Some(value) => optional_string(value)?,
        None => None,
    };
    // Every existing stream reports one (`0-0` when nothing was ever
    // added); treat a missing field like that.
    Ok(Some(id.unwrap_or_else(|| "0-0".to_string())))
}

/// Where a consumer group lost to `NOGROUP` is recreated (the id passed to
/// `XGROUP CREATE`; the group then delivers every entry AFTER it).
///
/// - `last_delivered`: this consumer's copy of the lost group's
///   `last-delivered-id` (`None` if unknown).
/// - `stream_last_generated`: the stream's `last-generated-id` now, `None`
///   when the stream does not exist.
///
/// The two ways a group goes missing:
///
/// - **Redis came back empty** (lost AOF, or no persistence). The stream
///   is gone, or was recreated by the producer's next `XADD`, so everything
///   in it is new to this group. Its ids come from the server's clock, so
///   they are normally all later than `last_delivered`, and resuming after
///   `last_delivered` delivers them all (a missing stream: `0`). If the
///   stream's `last-generated-id` is instead BEHIND `last_delivered`, it
///   cannot be the stream that was read (a stream's ids never go
///   backwards): a new stream on a server whose clock is behind, so start
///   at `0`.
/// - **The group was deleted by hand**, stream intact.
///   `last-generated-id >= last_delivered`, so the group resumes after the
///   last entry it had been handed: nothing it already read is replayed
///   (as `0` would), nothing added since is skipped (as `$` would).
///
/// With no `last_delivered` known (it could not be read at connect) this
/// falls back to `$`, the stream's tail, as at startup.
///
/// Not recoverable either way: the lost group's pending-entries list.
/// Entries delivered but not yet `ACKed` are not redelivered (after an empty
/// restart they are gone anyway). Nor is one narrow case: a recreated
/// stream on a server whose clock is behind, whose `last-generated-id`
/// has already passed `last_delivered` -- its entries up to
/// `last_delivered` are skipped.
fn recreate_start_id(last_delivered: Option<&str>, stream_last_generated: Option<&str>) -> String {
    let Some(last_generated) = stream_last_generated else {
        return "0".to_string();
    };
    match last_delivered {
        None => "$".to_string(),
        Some(last) if stream_id_less_than(last_generated, last) => "0".to_string(),
        Some(last) => last.to_string(),
    }
}

/// Splits a raw `XREAD`/`XREADGROUP` reply into entries with a usable
/// `payload` field and the ids of entries that don't have one.
///
/// **Bug this fixes**: an entry missing the expected `"payload"` field (or
/// whose value isn't a UTF-8 string) used to be silently dropped by a
/// `filter_map` in `next_batch`, with no XACK ever sent for its id. That left
/// it in the consumer group's pending-entries list forever: it is delivered
/// under the startup/XAUTOCLAIM PEL-replay path (`next_batch`'s `id = "0"`
/// read) exactly like any other pending entry, hits the same missing-field
/// case, and gets dropped again -- an infinite loop that never advances,
/// relying on an operator noticing or a Redis-version-specific trim
/// side-effect to ever clear it.
///
/// This is the same "poison message must not wedge the consumer" principle
/// `movement-relay::main::run_cycle` already applies to its own Kafka source
/// (`BatchOutcome::Unclassifiable` is logged and NOT retried, versus
/// `BatchOutcome::PublishFailed` which IS retried) -- just applied here to a
/// genuinely-can-never-succeed malformed entry rather than a transient
/// downstream failure. The caller (`next_batch`) XACKs `malformed_ids` right
/// after calling this, once a warning has been logged, so the entry is
/// permanently retired instead of retried.
///
/// Each malformed entry comes back as `(id, fields)`, `fields` being its
/// field/value pairs rendered as text, so it can be dead-lettered.
#[expect(
    clippy::type_complexity,
    reason = "two (id, fields) lists, spelled out at their one use"
)]
fn split_deliverable_and_malformed(
    ids: Vec<redis::streams::StreamId>,
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let mut entries = Vec::new();
    let mut malformed = Vec::new();
    for entry in ids {
        if let Some(payload) = entry
            .map
            .get("payload")
            .and_then(|v| redis::from_redis_value::<String>(v).ok())
        {
            entries.push((entry.id, payload));
        } else {
            let mut fields: Vec<String> = entry
                .map
                .iter()
                .map(|(k, v)| {
                    let v =
                        redis::from_redis_value::<String>(v).unwrap_or_else(|_| format!("{v:?}"));
                    format!("{k}={v}")
                })
                .collect();
            fields.sort();
            malformed.push((entry.id, fields.join("\n")));
        }
    }
    (entries, malformed)
}

/// Where the next PEL-replay read should start, given the raw size and last
/// id of the batch just read with `count = batch_count`: after that last id
/// when the batch was full (more may follow), or `None` (switch to `>`)
/// when it was short. See `RedisStreamMovementFeed::pel_replay_cursor`.
fn next_pel_replay_cursor(
    last_raw_id: Option<String>,
    raw_len: usize,
    batch_count: usize,
) -> Option<String> {
    if raw_len >= batch_count {
        last_raw_id
    } else {
        None
    }
}

/// After a failed read: moves `group` forward to `last_delivered_id` if
/// Redis reports it behind (`common::redis_group::restore_group_position`),
/// logging a warning and counting [`GROUP_RESTORED_METRIC`]; a group at or
/// past it is adopted as the new `last_delivered_id`. Returns whether the
/// check is done: `false` (logged) when Redis could not be asked, so the
/// next read tries again. A missing group is done here: the read will fail
/// `NOGROUP` and recreate it.
///
/// What is not undone: entries the consumer `ACKed` after the reloaded copy
/// was written are pending again and are redelivered once idle; every
/// consumer's writes are idempotent per entry.
async fn restore_group_position<C: redis::aio::ConnectionLike + Send>(
    conn: &mut C,
    stream: &str,
    group: &str,
    last_delivered_id: &mut Option<String>,
) -> bool {
    let Some(remembered) = last_delivered_id.clone() else {
        // Nothing to compare with; seed it, as at connect.
        if let Ok(Some(at)) =
            common::redis_group::group_last_delivered_id(conn, stream, group).await
        {
            *last_delivered_id = Some(at);
        }
        return true;
    };
    match common::redis_group::restore_group_position(conn, stream, group, &remembered).await {
        Ok(common::redis_group::GroupPosition::Restored { was }) => {
            tracing::warn!(
                stream,
                group,
                was,
                restored_to = %remembered,
                "consumer group went backwards (Redis reloaded an older copy of its data); \
                 moved it forward to the last entry this consumer was handed, so entries \
                 already processed are not delivered again"
            );
            metrics::counter!(
                common::metrics::metric_name(GROUP_RESTORED_METRIC),
                "group" => group.to_string()
            )
            .increment(1);
            true
        }
        Ok(common::redis_group::GroupPosition::Current { at }) => {
            *last_delivered_id = Some(at);
            true
        }
        Ok(common::redis_group::GroupPosition::Missing) => true,
        Err(err) => {
            tracing::warn!(error = ?err, stream, group, "could not check the consumer group's position; retrying before the next read");
            false
        }
    }
}

/// Whether a feed error is Redis reporting that the stream or this
/// consumer group does not exist (`-NOGROUP ...`).
fn is_nogroup(err: &anyhow::Error) -> bool {
    err.downcast_ref::<redis::RedisError>()
        .and_then(redis::RedisError::code)
        == Some("NOGROUP")
}

#[async_trait]
impl MovementFeed for RedisStreamMovementFeed {
    /// See [`Self::read_next_batch`]. A `NOGROUP` failure (Redis came back
    /// from an outage without its data: the stream, and with it this
    /// consumer group, is gone; or someone deleted the group) also
    /// recreates the group, so the next call reads again instead of every
    /// call failing `NOGROUP` until someone restarts the pod -- which
    /// `/livez` never would, since each failed cycle beats progress. It is
    /// recreated where the lost one stood, not at the stream's tail as a
    /// restart would, so entries added before the recreate are not skipped
    /// (see [`recreate_start_id`]). The error is still returned, so the
    /// caller's loop backs off as for any failed read.
    ///
    /// Any other failure makes the next call first check that the group did
    /// not go BACKWARDS (see `verify_position` and
    /// [`restore_group_position`]).
    async fn next_batch(&mut self) -> anyhow::Result<Vec<crate::FeedEntry>> {
        if self.verify_position {
            self.verify_position = !restore_group_position(
                &mut self.conn,
                &self.stream,
                &self.group,
                &mut self.last_delivered_id,
            )
            .await;
        }
        let result = self.read_next_batch().await;
        if let Err(err) = &result {
            if is_nogroup(err) {
                self.recreate_group().await;
            } else {
                self.verify_position = true;
            }
        }
        result
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = std::mem::take(&mut self.pending)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let _: i64 = self.conn.xack(&self.stream, &self.group, &ids).await?;
        Ok(())
    }

    async fn reject_batch(&mut self, detail: &str) -> anyhow::Result<()> {
        RedisStreamMovementFeed::reject_batch(self, detail).await
    }
}

impl RedisStreamMovementFeed {
    /// After `NOGROUP`: recreates the consumer group (and the stream, if
    /// missing) at [`recreate_start_id`] and starts over from an empty
    /// pending-entries list. A failure is only logged; the next `NOGROUP`
    /// read tries again.
    async fn recreate_group(&mut self) {
        self.pending.clear();
        self.isolating = false;
        self.pel_replay_cursor = Some("0".to_string());
        let stream_last_generated = match stream_last_generated_id(&mut self.conn, &self.stream)
            .await
        {
            Ok(id) => id,
            Err(err) => {
                tracing::error!(error = ?err, group = %self.group, "failed to inspect the stream to recreate the consumer group");
                return;
            }
        };
        let start_id = recreate_start_id(
            self.last_delivered_id.as_deref(),
            stream_last_generated.as_deref(),
        );
        tracing::warn!(
            stream = %self.stream,
            group = %self.group,
            last_delivered_id = ?self.last_delivered_id,
            stream_last_generated_id = ?stream_last_generated,
            start_id,
            "consumer group is missing (Redis lost its data, or it was deleted); recreating it \
             after the last entry it delivered (entries delivered but not ACKed are not redelivered)"
        );
        if let Err(err) = ensure_group(&mut self.conn, &self.stream, &self.group, &start_id).await {
            tracing::error!(error = ?err, group = %self.group, "failed to recreate the consumer group");
            return;
        }
        metrics::counter!(
            common::metrics::metric_name(GROUP_RECREATED_METRIC),
            "group" => self.group.clone()
        )
        .increment(1);
        if let Some(id) = seed_last_delivered_id(&mut self.conn, &self.stream, &self.group).await {
            self.last_delivered_id = Some(id);
        }
    }

    async fn read_next_batch(&mut self) -> anyhow::Result<Vec<crate::FeedEntry>> {
        // Periodic XAUTOCLAIM sweep, checked once per call -- cheap
        // (skips immediately if not due) and keeps this on the same
        // "checked every loop iteration" shape every existing multi-cadence
        // main.rs loop in this repo already uses, rather than a second
        // spawned task racing this one's own Redis connection.
        if self.last_autoclaim_sweep.elapsed() >= self.autoclaim_min_idle {
            self.reclaim_stale().await?;
            self.last_autoclaim_sweep = std::time::Instant::now();
        }

        let replaying_pel = self.pel_replay_cursor.is_some();
        let id_arg = if self.isolating {
            "0"
        } else {
            self.pel_replay_cursor.as_deref().unwrap_or(">")
        };
        let count = if self.isolating {
            1
        } else if replaying_pel {
            PEL_REPLAY_BATCH_COUNT
        } else {
            LIVE_READ_BATCH_COUNT
        };
        let reply: redis::streams::StreamReadReply = self
            .conn
            .xread_options(
                &[&self.stream],
                &[id_arg],
                &redis::streams::StreamReadOptions::default()
                    .group(&self.group, &self.consumer)
                    // The larger replay count applies only to the id=`0`
                    // PEL-replay read it was sized for; ordinary `>` reads
                    // keep their original batch size, so this change does
                    // not also widen every live batch (and with it the
                    // un-XACKed window a crash would redeliver).
                    // (and isolation reads one entry at a time, see
                    // `isolating`).
                    .count(count)
                    .block(if replaying_pel { 0 } else { LIVE_READ_BLOCK_MS }),
            )
            .await?;

        let raw: Vec<redis::streams::StreamId> =
            reply.keys.into_iter().flat_map(|k| k.ids).collect();
        let pel_drained = raw.is_empty();
        let last_raw_id = raw.last().map(|entry| entry.id.clone());
        let raw_len = raw.len();
        // A `>` read advanced the group's last-delivered-id to its last
        // entry; keep the copy a `NOGROUP` recreate resumes from in step.
        if id_arg == ">"
            && let Some(id) = &last_raw_id
        {
            self.last_delivered_id = Some(id.clone());
        }
        let (entries, malformed) = split_deliverable_and_malformed(raw);

        // Only a PEL replay can return an entry that has been delivered
        // before; a `>` read is always a first delivery.
        if replaying_pel {
            self.report_long_pending(&entries).await;
        }

        // A malformed entry can never become processable -- see
        // `split_deliverable_and_malformed`'s own doc for why it is XACKed
        // here rather than left for a future PEL replay. It is
        // dead-lettered first; if that fails, nothing is ACKed and the
        // whole read stays pending, to be retried.
        if !malformed.is_empty() {
            self.dead_letter_malformed(&malformed).await?;
            let malformed_ids: Vec<&String> = malformed.iter().map(|(id, _)| id).collect();
            let _: i64 = self
                .conn
                .xack(&self.stream, &self.group, &malformed_ids)
                .await?;
        }

        // Advance the PEL replay (M15, see `pel_replay_cursor`'s doc). A
        // full batch means more of this consumer's own pending entries may
        // follow, so the next call continues after the last id returned. A
        // short (or empty) batch means the pending-entries list is
        // exhausted: "no more of MY OWN old pending entries," not "no more
        // entries in the stream" (there could be plenty ahead of `>`), so
        // switch to `>`. Counted on the raw reply, before malformed entries
        // were split off, so a batch of only malformed entries still
        // advances.
        //
        // Isolation is the exception: it keeps re-reading id `0` one entry
        // at a time until that read comes back with nothing at all.
        if self.isolating {
            if pel_drained {
                tracing::info!(group = %self.group, "isolation finished: no pending entries left");
                self.isolating = false;
                self.pel_replay_cursor = None;
            }
        } else if replaying_pel {
            self.pel_replay_cursor =
                next_pel_replay_cursor(last_raw_id, raw_len, PEL_REPLAY_BATCH_COUNT);
        }

        let delivered = entries
            .iter()
            .map(|(id, payload)| crate::FeedEntry {
                payload: payload.clone(),
                received_at: self.entry_arrival_time(id),
            })
            .collect();
        self.pending = entries;
        Ok(delivered)
    }

    /// The arrival time of the entry `id` (see [`crate::FeedEntry::received_at`]),
    /// or `None` -- logged and counted -- if the id has no parseable time.
    fn entry_arrival_time(&self, id: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        let time = crate::stream_id_time(id);
        if time.is_none() {
            tracing::warn!(
                stream = %self.stream,
                group = %self.group,
                id,
                "stream entry id has no parseable time; using this consumer's clock as its arrival time"
            );
            metrics::counter!(
                common::metrics::metric_name(ENTRY_ID_UNPARSEABLE_METRIC),
                "group" => self.group.clone()
            )
            .increment(1);
        }
        time
    }

    /// Reclaims entries that have sat unacked in the consumer group's
    /// pending-entries list for at least `autoclaim_min_idle` -- the
    /// general safety net for entries stuck under a genuinely dead
    /// consumer name (a crashed pod that never restarts under the same
    /// name), layered on top of `next_batch`'s own startup-PEL-replay step.
    /// Cursor-until-`"0-0"` loop shape copied from
    /// `enricher::stream::claim_stale` (`crates/enricher/src/stream.rs`),
    /// generalized over group name.
    ///
    /// `XAUTOCLAIM` re-assigns ownership of stale entries to THIS
    /// consumer, but does not itself deliver their payloads -- claimed
    /// entries simply become part of this consumer's own pending-entries
    /// list. So after a non-empty claim, `next_batch`'s own PEL-replay
    /// step (an `id = "0"` read) is re-entered to actually retrieve them,
    /// the exact same path startup replay already uses -- no separate
    /// delivery mechanism needed.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "these durations are seconds to hours, far below u64::MAX milliseconds"
    )]
    async fn reclaim_stale(&mut self) -> anyhow::Result<()> {
        let mut cursor = "0-0".to_string();
        let mut claimed_any = false;
        loop {
            let reply: redis::streams::StreamAutoClaimReply = self
                .conn
                .xautoclaim_options(
                    &self.stream,
                    &self.group,
                    &self.consumer,
                    self.autoclaim_min_idle.as_millis() as u64,
                    cursor,
                    // Same `PEL_REPLAY_BATCH_COUNT` as `next_batch`'s own
                    // id=`0` read, for consistency; unlike that read this
                    // loop already continues until `next_stream_id ==
                    // "0-0"` regardless of count, so raising it here only
                    // reduces the number of `XAUTOCLAIM` round-trips a full
                    // sweep needs, it does not by itself change how much a
                    // single `next_batch` call can deliver.
                    redis::streams::StreamAutoClaimOptions::default().count(PEL_REPLAY_BATCH_COUNT),
                )
                .await?;

            if !reply.claimed.is_empty() {
                claimed_any = true;
            }
            if reply.next_stream_id == "0-0" {
                break;
            }
            cursor = reply.next_stream_id;
        }
        if claimed_any {
            self.pel_replay_cursor = Some("0".to_string());
        }
        Ok(())
    }

    /// Reports whether events were lost to trimming before this group
    /// could finish with them -- a provable loss, not a suspicion. Two
    /// round trips for `XINFO GROUPS`/`XINFO STREAM` plus one `XPENDING`
    /// summary, via [`Self::stream_positions`]; see [`detect_gap`] for the
    /// exact rule.
    ///
    /// **Changed 2026-09-27** (full-coverage lag review): this used to flag
    /// a gap whenever `last-delivered-id < first-entry`, which is also true
    /// when the first retained entry is simply the group's next unread one
    /// (a false alarm), and it ignored entries that had been delivered but
    /// not yet `ACKed` and were then trimmed out from under the PEL.
    pub async fn check_gap(&mut self) -> anyhow::Result<Option<GapInfo>> {
        let positions = self.stream_positions().await?;
        Ok(detect_gap(&positions))
    }

    /// Where this group and the stream stand, from `XINFO GROUPS`,
    /// `XINFO STREAM` and the `XPENDING` summary form. Used by
    /// [`Self::check_gap`] and by `full-coverage-consumer`'s startup replay
    /// (which needs the group's read position and whether the start of the
    /// rail day is still retained).
    pub async fn stream_positions(&mut self) -> anyhow::Result<StreamPositions> {
        let groups: Vec<redis::Value> = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&self.stream)
            .query_async(&mut self.conn)
            .await?;
        let group = find_group_fields(&groups, &self.group)?;
        let group_field = |name: &str| group.as_ref().and_then(|fields| fields.get(name));

        let stream_info: Vec<redis::Value> = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(&self.stream)
            .query_async(&mut self.conn)
            .await?;
        let stream = flat_fields(&stream_info)?;
        let stream_field = |name: &str| stream.get(name);

        let (pending_count, pending_min_id) = if group.is_some() {
            let summary: Vec<redis::Value> = redis::cmd("XPENDING")
                .arg(&self.stream)
                .arg(&self.group)
                .query_async(&mut self.conn)
                .await?;
            let count = summary
                .first()
                .map(optional_u64)
                .transpose()?
                .flatten()
                .unwrap_or(0);
            let min = summary.get(1).map(optional_string).transpose()?.flatten();
            (count, min)
        } else {
            (0, None)
        };

        Ok(StreamPositions {
            group_last_delivered_id: group_field("last-delivered-id")
                .map(optional_string)
                .transpose()?
                .flatten(),
            group_entries_read: group_field("entries-read")
                .map(optional_u64)
                .transpose()?
                .flatten(),
            pending_count,
            pending_min_id,
            stream_length: stream_field("length")
                .map(optional_u64)
                .transpose()?
                .flatten()
                .unwrap_or(0),
            stream_first_entry_id: find_stream_first_entry_id(&stream_info)?,
            stream_last_generated_id: stream_field("last-generated-id")
                .map(optional_string)
                .transpose()?
                .flatten(),
            stream_entries_added: stream_field("entries-added")
                .map(optional_u64)
                .transpose()?
                .flatten(),
            stream_max_deleted_entry_id: stream_field("max-deleted-entry-id")
                .map(optional_string)
                .transpose()?
                .flatten(),
        })
    }

    /// Group-less `XRANGE start end COUNT count` over this feed's stream --
    /// NOT a consumer-group read: nothing is delivered, claimed or `ACKed`,
    /// and the group's position is untouched. `start`/`end` take any
    /// `XRANGE` bound, including an exclusive `(<id>` (Redis >= 6.2).
    ///
    /// Returns `(id, payload)` for every entry with a usable `payload`
    /// field, plus the id of the last entry read at all (so a caller paging
    /// through the range can continue after a page made entirely of
    /// malformed entries). Malformed entries are only skipped here -- the
    /// consumer-group path is what dead-letters them.
    pub async fn read_range(
        &mut self,
        start: &str,
        end: &str,
        count: usize,
    ) -> anyhow::Result<RangePage> {
        let reply: redis::streams::StreamRangeReply = self
            .conn
            .xrange_count(&self.stream, start, end, count)
            .await?;
        let last_id = reply.ids.last().map(|entry| entry.id.clone());
        let (entries, _malformed) = split_deliverable_and_malformed(reply.ids);
        Ok(RangePage { entries, last_id })
    }

    /// Every entry id currently in this GROUP's pending-entries list (all
    /// consumer names, not just this one), paged `PEL_REPLAY_BATCH_COUNT`
    /// at a time. These are the entries the group will hand out again
    /// (startup PEL replay or `XAUTOCLAIM`), so a caller replaying the
    /// stream by `XRANGE` skips them to avoid dispatching them twice.
    pub async fn group_pending_ids(&mut self) -> anyhow::Result<std::collections::HashSet<String>> {
        let mut ids = std::collections::HashSet::new();
        let mut start = "-".to_string();
        loop {
            let reply: redis::streams::StreamPendingCountReply = self
                .conn
                .xpending_count(
                    &self.stream,
                    &self.group,
                    &start,
                    "+",
                    PEL_REPLAY_BATCH_COUNT,
                )
                .await?;
            let page = reply.ids.len();
            let Some(last) = reply.ids.last().map(|p| p.id.clone()) else {
                break;
            };
            ids.extend(reply.ids.into_iter().map(|p| p.id));
            if page < PEL_REPLAY_BATCH_COUNT {
                break;
            }
            start = format!("({last}");
        }
        Ok(ids)
    }
}

/// One page of [`RedisStreamMovementFeed::read_range`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangePage {
    /// `(id, payload)` of every deliverable entry, in stream order.
    pub entries: Vec<(String, String)>,
    /// The id of the last entry `XRANGE` returned, deliverable or not --
    /// `None` only when the range held nothing at all.
    pub last_id: Option<String>,
}

/// Snapshot of a consumer group's and its stream's positions -- see
/// [`RedisStreamMovementFeed::stream_positions`]. Every field Redis may
/// report as nil (or an older server may omit) is an `Option`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamPositions {
    /// `None` when the group does not exist.
    pub group_last_delivered_id: Option<String>,
    /// The group's logical read counter; nil in Redis when it cannot be
    /// known (e.g. a group created at an arbitrary id).
    pub group_entries_read: Option<u64>,
    /// Entries delivered to the group but not yet `ACKed`.
    pub pending_count: u64,
    /// The smallest id in the group's PEL, if any.
    pub pending_min_id: Option<String>,
    pub stream_length: u64,
    /// `None` when the stream is empty.
    pub stream_first_entry_id: Option<String>,
    pub stream_last_generated_id: Option<String>,
    /// Every entry ever added (Redis >= 7.0).
    pub stream_entries_added: Option<u64>,
    /// Highest id removed by `XDEL` (Redis >= 7.0). **Not** advanced by
    /// `MAXLEN`/`MINID` trimming -- checked against valkey 9 -- so it says
    /// nothing about trimming.
    pub stream_max_deleted_entry_id: Option<String>,
}

impl StreamPositions {
    /// How many entries have ever left the stream (trimmed or deleted),
    /// when the server reports `entries-added`.
    pub fn entries_removed(&self) -> Option<u64> {
        self.stream_entries_added
            .map(|added| added.saturating_sub(self.stream_length))
    }
}

/// The exact gap rule behind [`RedisStreamMovementFeed::check_gap`]. A gap
/// is either or both of:
///
/// - **Unread entries trimmed.** The group's read position is older than
///   the stream's first retained entry AND more entries were added after
///   that position than the stream still holds:
///   `entries-added - entries-read > length`; the difference is the number
///   lost. The id comparison alone is not enough: when the first retained
///   entry is the group's very next unread one, nothing was lost (the old
///   check's false alarm). When the server cannot report `entries-read`
///   the count is unknown and the rule falls back to "the position is
///   older than the first entry and something has been removed" --
///   conservative, `unread_entries_lost: None`. The id condition is kept
///   even when the counters are known: Redis's `entries-read` accounting
///   drifts by a few hundred across AOF reloads (seen in production), and
///   that drift alone must not raise an alarm while the group is inside
///   the retained window.
/// - **Pending entries trimmed.** The group's PEL holds an id older than
///   the first retained entry: delivered, never `ACKed`, and now gone, so a
///   redelivery can only hand back an empty entry.
pub fn detect_gap(positions: &StreamPositions) -> Option<GapInfo> {
    let last_delivered = positions.group_last_delivered_id.as_deref()?;
    // An empty stream: nothing retained to compare against. Unchanged from
    // the previous behaviour -- reported as no gap.
    let first_entry = positions.stream_first_entry_id.as_deref()?;

    let behind_first_entry = stream_id_less_than(last_delivered, first_entry);
    let unread_lost: Option<Option<u64>> = if behind_first_entry {
        match (positions.stream_entries_added, positions.group_entries_read) {
            (Some(added), Some(read)) => {
                let lost = added
                    .saturating_sub(read)
                    .saturating_sub(positions.stream_length);
                (lost > 0).then_some(Some(lost))
            }
            _ => match positions.entries_removed() {
                Some(0) => None,
                _ => Some(None),
            },
        }
    } else {
        None
    };

    let pending_trimmed = positions
        .pending_min_id
        .as_deref()
        .is_some_and(|min| stream_id_less_than(min, first_entry));

    if unread_lost.is_none() && !pending_trimmed {
        return None;
    }
    Some(GapInfo {
        group_last_delivered_id: last_delivered.to_string(),
        stream_first_entry_id: first_entry.to_string(),
        unread_entries_lost: unread_lost.flatten(),
        pending_entries_trimmed: pending_trimmed,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapInfo {
    pub group_last_delivered_id: String,
    pub stream_first_entry_id: String,
    /// How many never-read entries were trimmed, when the server's
    /// counters allow an exact figure; `None` when the loss is certain or
    /// suspected but its size unknown (or when only the PEL was hit).
    pub unread_entries_lost: Option<u64>,
    /// At least one delivered-but-unACKed entry was trimmed.
    pub pending_entries_trimmed: bool,
}

/// `XINFO`'s flat `[name, value, name, value, ...]` array as a map.
fn flat_fields(
    values: &[redis::Value],
) -> anyhow::Result<std::collections::HashMap<String, redis::Value>> {
    let mut map = std::collections::HashMap::new();
    let mut it = values.iter();
    while let (Some(k), Some(v)) = (it.next(), it.next()) {
        let k: String = redis::from_redis_value(k)?;
        map.insert(k, v.clone());
    }
    Ok(map)
}

/// The field map of the group named `group` in an `XINFO GROUPS` reply.
fn find_group_fields(
    groups: &[redis::Value],
    group: &str,
) -> anyhow::Result<Option<std::collections::HashMap<String, redis::Value>>> {
    for entry in groups {
        // RESP2 replies each group as a flat array; RESP3 as a map.
        let fields = match entry {
            redis::Value::Array(fields) => flat_fields(fields)?,
            redis::Value::Map(pairs) => {
                let mut map = std::collections::HashMap::new();
                for (k, v) in pairs {
                    map.insert(redis::from_redis_value::<String>(k)?, v.clone());
                }
                map
            }
            _ => continue,
        };
        let name: Option<String> = fields
            .get("name")
            .and_then(|v| redis::from_redis_value(v).ok());
        if name.as_deref() == Some(group) {
            return Ok(Some(fields));
        }
    }
    Ok(None)
}

fn optional_string(value: &redis::Value) -> anyhow::Result<Option<String>> {
    Ok(redis::from_redis_value::<Option<String>>(value)?)
}

fn optional_u64(value: &redis::Value) -> anyhow::Result<Option<u64>> {
    Ok(redis::from_redis_value::<Option<u64>>(value)?)
}

/// `XINFO STREAM`'s reply is itself a flat array of alternating field
/// name/value pairs; its `first-entry` field is a 2-element array `[id,
/// fields]` (or a Redis nil/bulk-nil when the stream is empty) -- confirmed
/// against a real local Redis (`redis-cli XINFO STREAM <stream>`) while
/// implementing this, not assumed from documentation alone.
fn find_stream_first_entry_id(stream_info: &[redis::Value]) -> anyhow::Result<Option<String>> {
    let mut it = stream_info.iter();
    while let (Some(k), Some(v)) = (it.next(), it.next()) {
        let k: String = redis::from_redis_value(k)?;
        if k != "first-entry" {
            continue;
        }
        let redis::Value::Array(entry) = v else {
            return Ok(None); // nil -- empty stream.
        };
        let Some(id_value) = entry.first() else {
            return Ok(None);
        };
        let id: String = redis::from_redis_value(id_value)?;
        return Ok(Some(id));
    }
    Ok(None)
}

/// Stream IDs are `<ms>-<seq>` pairs, monotonic and directly comparable as
/// a pair of integers (never as a bare string -- `"9-0" < "10-0"`
/// lexicographically is false but numerically true, so this must NOT be a
/// plain string `<` comparison).
pub fn stream_id_less_than(a: &str, b: &str) -> bool {
    fn parts(id: &str) -> (u64, u64) {
        let mut it = id.splitn(2, '-');
        let ms = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let seq = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        (ms, seq)
    }
    parts(a) < parts(b)
}

/// Pure-logic tests for `split_deliverable_and_malformed` -- no Redis
/// connection needed, unlike everything in `redis_tests` below.
#[cfg(test)]
mod split_deliverable_and_malformed_tests {
    use std::collections::HashMap;

    use super::*;

    fn stream_id(id: &str, map: HashMap<String, redis::Value>) -> redis::streams::StreamId {
        redis::streams::StreamId {
            id: id.to_string(),
            map,
        }
    }

    #[test]
    fn entries_with_a_payload_field_are_deliverable() {
        let mut map = HashMap::new();
        map.insert(
            "payload".to_string(),
            redis::Value::BulkString(b"hello".to_vec()),
        );
        let (entries, malformed_ids) = split_deliverable_and_malformed(vec![stream_id("1-0", map)]);

        assert_eq!(entries, vec![("1-0".to_string(), "hello".to_string())]);
        assert!(malformed_ids.is_empty());
    }

    #[test]
    fn an_entry_missing_the_payload_field_is_reported_malformed_not_dropped() {
        let mut map = HashMap::new();
        map.insert(
            "not_payload".to_string(),
            redis::Value::BulkString(b"whatever".to_vec()),
        );
        let (entries, malformed_ids) = split_deliverable_and_malformed(vec![stream_id("2-0", map)]);

        assert!(
            entries.is_empty(),
            "an entry with no payload field yields no deliverable payload"
        );
        assert_eq!(
            malformed_ids,
            vec![("2-0".to_string(), "not_payload=whatever".to_string())],
            "the entry's id must still be surfaced so the caller can XACK it -- \
             this is the fix: it used to be silently dropped by a filter_map \
             with no id ever reaching an XACK, leaving it pending forever -- \
             and its fields so it can be dead-lettered"
        );
    }

    #[test]
    fn a_mixed_batch_keeps_the_good_entry_and_flags_only_the_bad_one() {
        let mut good = HashMap::new();
        good.insert(
            "payload".to_string(),
            redis::Value::BulkString(b"ok".to_vec()),
        );
        let bad = HashMap::new(); // no fields at all -- e.g. a corrupted write.

        let (entries, malformed_ids) =
            split_deliverable_and_malformed(vec![stream_id("1-0", good), stream_id("2-0", bad)]);

        assert_eq!(entries, vec![("1-0".to_string(), "ok".to_string())]);
        assert_eq!(malformed_ids, vec![("2-0".to_string(), String::new())]);
    }
}

#[cfg(test)]
mod detect_gap_tests {
    use super::*;

    /// A group that has read up to `last` of a stream now starting at
    /// `first`, with exact counters.
    fn positions(last: &str, first: &str, added: u64, read: u64, length: u64) -> StreamPositions {
        StreamPositions {
            group_last_delivered_id: Some(last.to_string()),
            group_entries_read: Some(read),
            stream_length: length,
            stream_first_entry_id: Some(first.to_string()),
            stream_entries_added: Some(added),
            ..StreamPositions::default()
        }
    }

    /// The old check's false alarm: everything the group read has been
    /// trimmed, and the first retained entry is simply its next unread one.
    /// `last-delivered-id < first-entry`, but nothing was lost.
    #[test]
    fn the_immediate_successor_being_first_is_not_a_gap() {
        assert_eq!(detect_gap(&positions("3-0", "4-0", 4, 3, 1)), None);
    }

    #[test]
    fn unread_entries_trimmed_are_counted_exactly() {
        let gap = detect_gap(&positions("2-0", "12-0", 12, 2, 1)).expect("9 unread entries lost");
        assert_eq!(gap.unread_entries_lost, Some(9));
        assert!(!gap.pending_entries_trimmed);
    }

    /// Redis's `entries-read` drifts by a few hundred across AOF reloads
    /// (a lag floor of ~282 with the group fully caught up, seen in
    /// production). Counters alone would call that loss on a short stream;
    /// the id condition keeps it quiet while the group is inside the
    /// retained window.
    #[test]
    fn counter_drift_inside_the_retained_window_is_not_a_gap() {
        assert_eq!(detect_gap(&positions("50-0", "10-0", 1000, 400, 100)), None);
    }

    #[test]
    fn a_trimmed_pending_entry_is_a_gap_even_with_nothing_unread_lost() {
        let mut p = positions("2-0", "3-0", 3, 2, 1);
        p.pending_count = 2;
        p.pending_min_id = Some("1-0".to_string());
        let gap = detect_gap(&p).expect("the PEL points at trimmed entries");
        assert!(gap.pending_entries_trimmed);
        assert_eq!(gap.unread_entries_lost, None);
    }

    #[test]
    fn unknown_entries_read_falls_back_to_the_id_comparison() {
        let mut p = positions("2-0", "12-0", 12, 0, 1);
        p.group_entries_read = None;
        let gap = detect_gap(&p).expect("conservative: position older than the first entry");
        assert_eq!(gap.unread_entries_lost, None);

        // ...but not when nothing has ever been removed from the stream.
        let mut p = positions("0-0", "12-0", 1, 0, 1);
        p.group_entries_read = None;
        assert_eq!(detect_gap(&p), None);
    }

    #[test]
    fn no_group_or_an_empty_stream_is_not_a_gap() {
        assert_eq!(detect_gap(&StreamPositions::default()), None);
        let mut p = positions("2-0", "3-0", 3, 2, 1);
        p.stream_first_entry_id = None;
        assert_eq!(detect_gap(&p), None);
    }
}

#[cfg(test)]
mod recreate_start_id_tests {
    use super::*;

    #[test]
    fn an_intact_stream_resumes_after_the_last_delivered_entry() {
        // Group deleted by hand: nothing already read is replayed.
        assert_eq!(recreate_start_id(Some("100-3"), Some("250-0")), "100-3");
        // Caught up: the stream's tail is the last delivered entry.
        assert_eq!(recreate_start_id(Some("250-0"), Some("250-0")), "250-0");
    }

    #[test]
    fn a_stream_recreated_after_an_empty_restart_is_read_in_full() {
        // Gone entirely: everything a producer adds from now on is new.
        assert_eq!(recreate_start_id(Some("100-3"), None), "0");
        assert_eq!(recreate_start_id(None, None), "0");
        // Recreated by a producer on a server clock behind the old one:
        // its ids can't be the stream that was read, so all of it is new.
        assert_eq!(recreate_start_id(Some("100-3"), Some("90-0")), "0");
        assert_eq!(recreate_start_id(Some("100-3"), Some("100-2")), "0");
        // Recreated but still empty (`0-0`): same.
        assert_eq!(recreate_start_id(Some("100-3"), Some("0-0")), "0");
    }

    /// charts/distant-signal/templates/prometheusrule.yaml's
    /// `DistantSignalMovementGroupRecreated` reads this exact name.
    #[test]
    fn the_group_recreated_counter_name_matches_the_chart_alert() {
        assert_eq!(
            common::metrics::metric_name(GROUP_RECREATED_METRIC),
            "distant_signal_movement_feed_group_recreated_total"
        );
    }

    #[test]
    fn ids_compare_numerically_not_as_strings() {
        // "9-0" < "10-0" numerically; as strings it would pick `0`.
        assert_eq!(recreate_start_id(Some("9-0"), Some("10-0")), "9-0");
        assert_eq!(recreate_start_id(Some("10-0"), Some("9-0")), "0");
    }

    #[test]
    fn an_unknown_position_falls_back_to_the_tail_while_the_stream_exists() {
        assert_eq!(recreate_start_id(None, Some("250-0")), "$");
    }

    #[test]
    fn a_group_that_never_delivered_anything_resumes_from_its_creation_point() {
        // Created with `$` on an empty stream, nothing read yet.
        assert_eq!(recreate_start_id(Some("0-0"), Some("5-0")), "0-0");
    }
}

/// [`restore_group_position`] against a scripted Redis
/// (`common::redis_group::fake::FakeConn`).
#[cfg(test)]
mod restore_group_position_tests {
    use common::redis_group::fake::FakeConn;
    use redis::Value;

    use super::*;

    fn groups(last_delivered_id: &str) -> redis::RedisResult<Value> {
        Ok(Value::Array(vec![FakeConn::group_info(
            "trust-consumer",
            last_delivered_id,
        )]))
    }

    /// Redis reloaded an older copy: the group stands at 100-0 though this
    /// consumer was handed 250-1. It is moved forward; the remembered id
    /// stays.
    #[tokio::test]
    async fn a_group_that_went_backwards_is_moved_forward() {
        let mut conn = FakeConn::new([groups("100-0"), Ok(Value::Okay)]);
        let mut last = Some("250-1".to_string());
        assert!(restore_group_position(&mut conn, "s", "trust-consumer", &mut last).await);
        assert_eq!(last.as_deref(), Some("250-1"));
        assert_eq!(
            conn.sent[1],
            ["XGROUP", "SETID", "s", "trust-consumer", "250-1"]
        );
    }

    /// A group ahead of this consumer's copy (a read whose reply was lost)
    /// is adopted, not moved.
    #[tokio::test]
    async fn a_group_ahead_is_adopted() {
        let mut conn = FakeConn::new([groups("300-0")]);
        let mut last = Some("250-1".to_string());
        assert!(restore_group_position(&mut conn, "s", "trust-consumer", &mut last).await);
        assert_eq!(last.as_deref(), Some("300-0"));
        assert_eq!(conn.sent.len(), 1);
    }

    /// Redis still unreachable: not done, so the next read checks again.
    #[tokio::test]
    async fn an_unreachable_redis_leaves_the_check_for_the_next_read() {
        let mut conn = FakeConn::new([Err(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "connection refused",
        )))]);
        let mut last = Some("250-1".to_string());
        assert!(!restore_group_position(&mut conn, "s", "trust-consumer", &mut last).await);
        assert_eq!(last.as_deref(), Some("250-1"));
    }

    /// A missing group is left to the `NOGROUP` recreate.
    #[tokio::test]
    async fn a_missing_group_is_left_to_the_nogroup_recreate() {
        let mut conn = FakeConn::new([Ok(Value::Array(vec![]))]);
        let mut last = Some("250-1".to_string());
        assert!(restore_group_position(&mut conn, "s", "trust-consumer", &mut last).await);
        assert_eq!(conn.sent.len(), 1, "no SETID");
    }

    /// With no remembered id there is nothing to restore; the group's own
    /// is adopted.
    #[tokio::test]
    async fn no_remembered_id_seeds_it_from_the_group() {
        let mut conn = FakeConn::new([groups("42-0")]);
        let mut last = None;
        assert!(restore_group_position(&mut conn, "s", "trust-consumer", &mut last).await);
        assert_eq!(last.as_deref(), Some("42-0"));
    }
}

#[cfg(test)]
mod long_pending_entries_tests {
    use std::collections::HashMap;

    use super::*;

    fn entry(id: &str) -> (String, String) {
        (id.to_string(), format!("payload-{id}"))
    }

    #[test]
    fn only_entries_over_the_threshold_are_reported() {
        let counts: HashMap<String, u64> = [
            ("1-0".to_string(), 51),
            ("2-0".to_string(), 50),
            ("3-0".to_string(), 3),
        ]
        .into_iter()
        .collect();

        let long = long_pending_entries(&[entry("1-0"), entry("2-0"), entry("3-0")], &counts, 50);

        assert_eq!(long, vec![("1-0".to_string(), 51)]);
    }

    #[test]
    fn an_entry_xpending_did_not_report_is_not_long_pending() {
        assert!(long_pending_entries(&[entry("9-0")], &HashMap::new(), 1).is_empty());
    }

    #[test]
    fn a_full_pel_replay_batch_continues_after_its_last_id() {
        assert_eq!(
            next_pel_replay_cursor(Some("7-0".to_string()), 3, 3),
            Some("7-0".to_string())
        );
    }

    #[test]
    fn a_short_or_empty_pel_replay_batch_ends_the_replay() {
        assert_eq!(next_pel_replay_cursor(Some("7-0".to_string()), 2, 3), None);
        assert_eq!(next_pel_replay_cursor(None, 0, 3), None);
    }
}

#[cfg(test)]
#[expect(
    clippy::items_after_statements,
    reason = "test code: fixtures sit next to their use"
)]
mod redis_tests {
    use super::*;

    /// Just the payloads of a delivered batch, for the assertions below that
    /// only care about which entries came back.
    fn payloads(batch: Vec<crate::FeedEntry>) -> Vec<String> {
        batch.into_iter().map(|entry| entry.payload).collect()
    }

    /// Each delivered entry carries the time its stream id was generated
    /// (the relay's `XADD`), not the time it was read -- so a consumer that
    /// reads it long after still sees when it arrived.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_delivered_entry_carries_its_stream_id_time() {
        let stream = unique_stream("arrival_time");
        cleanup(&stream).await;
        let mut feed = connect(&stream).await;
        assert!(payloads(feed.next_batch().await.unwrap()).is_empty()); // drain empty startup PEL

        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        // An explicit id 2026-10-01T00:59:00Z (01:59 London): long before
        // this read, as for a lagging consumer.
        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("1790816340000-0")
            .arg("payload")
            .arg("late")
            .query_async(&mut conn)
            .await
            .unwrap();

        let batch = feed.next_batch().await.unwrap();
        assert_eq!(
            batch,
            vec![crate::FeedEntry {
                payload: "late".to_string(),
                received_at: Some("2026-10-01T00:59:00Z".parse().unwrap()),
            }]
        );
        cleanup(&stream).await;
    }

    /// `REDIS_URL` plus `REDIS_PASSWORD` (when set), combined exactly as the
    /// services do, so this suite also runs against a Redis started with
    /// `--requirepass` (the chart's `redis.auth`).
    fn redis_url() -> String {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
        let password = std::env::var("REDIS_PASSWORD")
            .ok()
            .map(common::secret::Secret::from);
        common::redis_auth::redis_url_with_password(&url, password.as_ref())
            .expect("REDIS_URL/REDIS_PASSWORD combine")
            .expose()
            .to_owned()
    }

    /// Rollout step 1 of the chart's `redis.auth` (clients get a password
    /// before the server requires one) must not break anything: the
    /// `AUTH default <password>` the clients send is accepted by a Redis 6+
    /// whose default user has no password yet. With no `REDIS_PASSWORD` set
    /// this runs against the local password-less Redis with an arbitrary
    /// password; with one set it checks that password against a
    /// `--requirepass` server.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_client_with_a_password_connects_whether_or_not_the_server_requires_one() {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
        let password = std::env::var("REDIS_PASSWORD")
            .unwrap_or_else(|_| "rollout-step-1-server-has-no-password-yet".into());
        let url = common::redis_auth::redis_url_with_password(
            &url,
            Some(&common::secret::Secret::new(password)),
        )
        .unwrap();
        let stream = unique_stream("auth");
        let mut feed = RedisStreamMovementFeed::connect_for_test(
            url.expose(),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .expect("AUTH default <password> accepted");
        assert!(payloads(feed.next_batch().await.unwrap()).is_empty());
        cleanup(&stream).await;
    }

    /// A fresh, unique stream/group namespace per test, so concurrent runs
    /// (or leftover state from a prior failed run) never collide. Cleans up
    /// unconditionally at the end of each test that uses it, mirroring this
    /// repo's "delete the fixture row at the end" DB-test convention.
    fn unique_stream(test_name: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("movement-events-test-{test_name}-{nanos}")
    }

    async fn cleanup(stream: &str) {
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let _: redis::RedisResult<i64> = redis::cmd("DEL")
            .arg(stream)
            .arg(format!("{stream}{DEAD_LETTER_SUFFIX}"))
            .query_async(&mut conn)
            .await;
    }

    /// `(field -> value)` maps of every entry in `stream`'s dead-letter stream.
    async fn dead_letters(stream: &str) -> Vec<std::collections::HashMap<String, String>> {
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let reply: redis::streams::StreamRangeReply = conn
            .xrange_all(format!("{stream}{DEAD_LETTER_SUFFIX}"))
            .await
            .unwrap();
        reply
            .ids
            .into_iter()
            .map(|entry| {
                entry
                    .map
                    .iter()
                    .map(|(k, v)| (k.clone(), redis::from_redis_value(v).unwrap()))
                    .collect()
            })
            .collect()
    }

    async fn connect(stream: &str) -> RedisStreamMovementFeed {
        RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .unwrap()
    }

    async fn pending_ids(feed: &mut RedisStreamMovementFeed, stream: &str) -> Vec<String> {
        let pending: redis::streams::StreamPendingCountReply = feed
            .conn
            .xpending_count(stream, "test-group", "-", "+", 100)
            .await
            .unwrap();
        pending.ids.into_iter().map(|p| p.id).collect()
    }

    /// PL-2: an entry that only ever fails transiently (the downstream is
    /// unreachable, times out or 5xxs -- the caller just doesn't commit) is
    /// NEVER dead-lettered, however many times it has been delivered. The
    /// old guard moved everything pending for over ~240 deliveries (about
    /// an hour of `api` outage) out of all three groups.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_transiently_failing_entry_is_never_dead_lettered_past_240_deliveries() {
        let stream = unique_stream("transient-forever");
        let mut feed = connect(&stream).await;
        xadd(&stream, "healthy").await;
        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["healthy".to_string()]
        );
        let id = pending_ids(&mut feed, &stream).await.remove(0);
        // Fast-forward the delivery counter well past the old limit, as
        // hours of a failing `api` would.
        let _: redis::Value = redis::cmd("XCLAIM")
            .arg(&stream)
            .arg("test-group")
            .arg("test-consumer")
            .arg(0)
            .arg(&id)
            .arg("RETRYCOUNT")
            .arg(LONG_PENDING_DELIVERIES * 4)
            .query_async(&mut feed.conn)
            .await
            .unwrap();
        drop(feed); // downstream failed transiently: never committed

        // ...and a few more real failed redeliveries on top.
        for _ in 0..3 {
            let mut feed = connect(&stream).await;
            assert_eq!(
                payloads(feed.next_batch().await.unwrap()),
                vec!["healthy".to_string()],
                "still handed out for retry, not diverted"
            );
            drop(feed);
        }

        let mut feed = connect(&stream).await;
        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["healthy".to_string()]
        );
        assert!(
            dead_letters(&stream).await.is_empty(),
            "nothing is dead-lettered for its delivery count"
        );
        assert_eq!(pending_ids(&mut feed, &stream).await, vec![id.clone()]);

        // Once the outage ends it commits normally.
        feed.commit().await.unwrap();
        assert!(pending_ids(&mut feed, &stream).await.is_empty());
        cleanup(&stream).await;
    }

    /// PL-2: a downstream data rejection of a multi-entry batch isolates
    /// the entries one at a time; only the one that is rejected on its own
    /// is dead-lettered (payload intact) and `ACKed`, and its healthy
    /// batch-mate is committed normally.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_rejected_batch_is_isolated_and_only_the_poison_entry_is_dead_lettered() {
        let stream = unique_stream("rejected-isolated");
        let mut feed = connect(&stream).await;
        xadd(&stream, "good").await;
        xadd(&stream, "poison").await;
        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["good".to_string(), "poison".to_string()]
        );
        feed.reject_batch("422 Unprocessable Entity").await.unwrap();
        assert!(
            dead_letters(&stream).await.is_empty(),
            "a multi-entry rejection cannot say which entry was bad"
        );
        assert_eq!(pending_ids(&mut feed, &stream).await.len(), 2);

        // Isolation: one entry at a time.
        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["good".to_string()]
        );
        feed.commit().await.unwrap();
        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["poison".to_string()]
        );
        feed.reject_batch("422 Unprocessable Entity: bad row")
            .await
            .unwrap();

        assert!(pending_ids(&mut feed, &stream).await.is_empty());
        let letters = dead_letters(&stream).await;
        assert_eq!(letters.len(), 1);
        assert_eq!(letters[0]["payload"], "poison");
        assert_eq!(letters[0]["reason"], "rejected_by_api");
        assert_eq!(letters[0]["group"], "test-group");
        assert_eq!(letters[0]["detail"], "422 Unprocessable Entity: bad row");
        assert!(!letters[0]["source_id"].is_empty());

        // The PEL is empty, so isolation ends and live reads resume.
        assert!(payloads(feed.next_batch().await.unwrap()).is_empty());
        assert!(!feed.isolating);
        xadd(&stream, "later").await;
        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["later".to_string()]
        );
        cleanup(&stream).await;
    }

    /// A transient failure during isolation leaves the entry pending, to be
    /// re-read (not dead-lettered).
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_transient_failure_during_isolation_retries_the_same_entry() {
        let stream = unique_stream("isolation-transient");
        let mut feed = connect(&stream).await;
        xadd(&stream, "a").await;
        xadd(&stream, "b").await;
        payloads(feed.next_batch().await.unwrap());
        payloads(feed.next_batch().await.unwrap());
        feed.reject_batch("400 Bad Request").await.unwrap();

        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["a".to_string()]
        );
        // Transient failure: no commit, no reject.
        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["a".to_string()]
        );
        assert!(dead_letters(&stream).await.is_empty());
        assert_eq!(pending_ids(&mut feed, &stream).await.len(), 2);
        cleanup(&stream).await;
    }

    /// The dead-letter stream is never trimmed: once full, a write is
    /// refused and the entry stays pending rather than being lost.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_full_dead_letter_stream_refuses_instead_of_trimming() {
        let stream = unique_stream("dead-letter-full");
        let mut feed = connect(&stream).await;
        let dl = format!("{stream}{DEAD_LETTER_SUFFIX}");
        let mut pipe = redis::pipe();
        for i in 0..DEAD_LETTER_MAX_LEN {
            pipe.cmd("XADD")
                .arg(&dl)
                .arg("*")
                .arg("payload")
                .arg(i)
                .ignore();
        }
        let _: () = pipe.query_async(&mut feed.conn).await.unwrap();

        xadd(&stream, "poison").await;
        payloads(feed.next_batch().await.unwrap());
        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["poison".to_string()]
        );
        assert!(feed.reject_batch("422").await.is_err());
        assert_eq!(
            pending_ids(&mut feed, &stream).await.len(),
            1,
            "refused, so still pending"
        );
        let len: usize = feed.conn.xlen(&dl).await.unwrap();
        assert_eq!(len, DEAD_LETTER_MAX_LEN, "nothing was trimmed");
        cleanup(&stream).await;
    }

    /// `dead_letter` (used by trust-backlog-consumer for rows `api`
    /// rejected) writes every field to the capped dead-letter stream.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn dead_letter_writes_the_record_to_the_dead_letter_stream() {
        let stream = unique_stream("dead-letter-write");
        let mut feed = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

        feed.dead_letter(&[DeadLetter {
            reason: "rejected_by_api",
            source_id: None,
            delivery_count: None,
            payload: r#"{"msg_type":"0009"}"#.to_string(),
            detail: "23514 check_violation".to_string(),
        }])
        .await
        .unwrap();

        let letters = dead_letters(&stream).await;
        assert_eq!(letters.len(), 1);
        assert_eq!(letters[0]["reason"], "rejected_by_api");
        assert_eq!(letters[0]["payload"], r#"{"msg_type":"0009"}"#);
        assert_eq!(letters[0]["detail"], "23514 check_violation");
        assert_eq!(letters[0]["consumer"], "test-consumer");
        assert_eq!(letters[0]["source_id"], "");

        cleanup(&stream).await;
    }

    async fn xadd(stream: &str, payload: &str) {
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let _: String = redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("payload")
            .arg(payload)
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    /// Regression test for the pending-forever bug: a stream entry with no
    /// `payload` field must be `XACKed` by `next_batch` itself, not left
    /// dangling in the pending-entries list for a future PEL replay to trip
    /// over again.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_malformed_entry_missing_payload_is_acked_not_left_pending_forever() {
        let stream = unique_stream("malformed-entry");

        let mut feed = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

        // An entry with no "payload" field at all -- e.g. a corrupted write,
        // or a producer bug that used the wrong field name. This is
        // deliberately written with a raw XADD rather than this module's own
        // `xadd` helper, which always sets "payload".
        let client = redis::Client::open(redis_url()).unwrap();
        let mut raw_conn = common::redis_conn::connect(&client).await.unwrap();
        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("not_payload")
            .arg("whatever")
            .query_async(&mut raw_conn)
            .await
            .unwrap();

        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        let batch = payloads(feed.next_batch().await.unwrap());
        assert!(
            batch.is_empty(),
            "a malformed entry yields no deliverable payload"
        );

        let pending: redis::streams::StreamPendingCountReply = feed
            .conn
            .xpending_count(&stream, "test-group", "-", "+", 10)
            .await
            .unwrap();
        assert_eq!(
            pending.ids.len(),
            0,
            "the malformed entry must be XACKed by next_batch itself, not left \
             pending forever -- before the fix, this stayed pending even across \
             the PEL replay a fresh connect below would trigger"
        );
        let letters = dead_letters(&stream).await;
        assert_eq!(
            letters.len(),
            1,
            "and it is dead-lettered, not just dropped"
        );
        assert_eq!(letters[0]["reason"], "malformed_entry");
        assert_eq!(letters[0]["payload"], "not_payload=whatever");

        cleanup(&stream).await;
    }

    /// `XGROUP CREATE ... $ MKSTREAM` starts a fresh group's
    /// `last-delivered-id` at the stream's CURRENT tail -- so an entry
    /// added before the group is ever created is never visible to that
    /// group's `>` reads (this is real Redis Streams semantics, not a bug
    /// in this crate). Every test below therefore connects (which creates
    /// the group) BEFORE `xadd`ing, mirroring the real deployment order
    /// (movement-relay creates/joins the group long before any consumer
    /// connects) rather than the reverse.
    ///
    /// Separately: `next_batch`'s first-ever call on a fresh connection
    /// always does the id=`0` PEL-replay pass first (Decision 2's startup
    /// replay) -- for a brand new consumer that pass is legitimately
    /// empty, and only the FOLLOWING call switches to `>` and picks up new
    /// entries. So most tests below call `next_batch` twice: once to drain
    /// the (empty) startup PEL, once to actually read what was `xadd`ed.
    /// This is expected behavior per the design doc's Decision 2, not
    /// worked around here.

    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn startup_replay_delivers_a_prior_consumers_unacked_entry() {
        let stream = unique_stream("startup-replay");

        let mut feed = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
        xadd(&stream, "payload-1").await;

        assert!(
            payloads(feed.next_batch().await.unwrap()).is_empty(),
            "startup PEL drain: nothing pending yet"
        );
        let batch = payloads(feed.next_batch().await.unwrap());
        assert_eq!(batch, vec!["payload-1".to_string()]);
        // Deliberately NOT acked, then dropped -- simulates a crash before
        // XACK.
        drop(feed);

        let mut feed2 = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
        let replayed = payloads(feed2.next_batch().await.unwrap());
        assert_eq!(
            replayed,
            vec!["payload-1".to_string()],
            "a fresh connect for the same (group, consumer) must replay the unacked entry"
        );

        cleanup(&stream).await;
    }

    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn commit_acks_only_after_being_called_not_on_receipt() {
        let stream = unique_stream("commit-timing");

        let mut feed = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
        xadd(&stream, "payload-1").await;
        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        let delivered = payloads(feed.next_batch().await.unwrap());
        assert_eq!(delivered, vec!["payload-1".to_string()]);

        let pending_before: redis::streams::StreamPendingCountReply = feed
            .conn
            .xpending_count(&stream, "test-group", "-", "+", 10)
            .await
            .unwrap();
        assert_eq!(pending_before.ids.len(), 1, "not yet acked");

        feed.commit().await.unwrap();

        let pending_after: redis::streams::StreamPendingCountReply = feed
            .conn
            .xpending_count(&stream, "test-group", "-", "+", 10)
            .await
            .unwrap();
        assert_eq!(pending_after.ids.len(), 0, "acked after commit");

        cleanup(&stream).await;
    }

    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn next_batch_never_redelivers_its_own_in_flight_batch_before_a_reconnect() {
        let stream = unique_stream("no-double-read");

        let mut feed = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
        xadd(&stream, "payload-1").await;
        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        let first = payloads(feed.next_batch().await.unwrap());
        assert_eq!(first, vec!["payload-1".to_string()]);

        // A third call, still without committing the second's delivery --
        // must NOT redeliver payload-1 via `>` (it's legitimately
        // "delivered, not yet acked", which only a reconnect's PEL replay
        // should surface). Nothing new is in the stream, so this blocks on
        // `>` for up to 5s and then returns empty.
        let third = payloads(feed.next_batch().await.unwrap());
        assert!(
            third.is_empty(),
            "an already-connected feed must not re-deliver its own undelivered batch"
        );

        cleanup(&stream).await;
    }

    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn xautoclaim_reclaims_an_entry_stuck_under_a_different_consumer_name() {
        let stream = unique_stream("autoclaim");

        // Deliver to "dead-consumer", then drop without acking -- simulates
        // a crashed pod that never restarts under the same name.
        let mut dead = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "dead-consumer",
            Duration::from_millis(50),
        )
        .await
        .unwrap();
        xadd(&stream, "payload-1").await;
        payloads(dead.next_batch().await.unwrap()); // drain empty startup PEL
        let to_dead = payloads(dead.next_batch().await.unwrap());
        assert_eq!(
            to_dead,
            vec!["payload-1".to_string()],
            "delivered to dead-consumer, never acked"
        );
        drop(dead);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut live = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "live-consumer",
            Duration::from_millis(50),
        )
        .await
        .unwrap();
        // The sweep runs at the top of next_batch; the entry is stuck under
        // dead-consumer at this point (idle well past autoclaim_min_idle),
        // so the sweep should reclaim it and this same call should return
        // it via the reclaim -> PEL-replay path.
        let reclaimed = payloads(live.next_batch().await.unwrap());
        assert_eq!(
            reclaimed,
            vec!["payload-1".to_string()],
            "the stale entry should be reclaimed and delivered to live-consumer"
        );

        cleanup(&stream).await;
    }

    /// Regression test for Repeater Signal finding M15: a reclaimed backlog
    /// used to replay only 100 entries per sweep (the old `.count(100)`),
    /// so a 5,000-entry backlog took ~25 minutes to drain. A single reclaim
    /// + replay must now deliver a full `PEL_REPLAY_BATCH_COUNT` batch.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_reclaimed_backlog_replays_a_full_pel_replay_batch_per_sweep() {
        let stream = unique_stream("backlog-drain");
        const BACKLOG: usize = PEL_REPLAY_BATCH_COUNT + 200;

        let mut dead = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "dead-consumer",
            // Long enough that this consumer never reclaims (and so
            // re-replays) its OWN pending entries while reading the backlog
            // below -- only `live` should sweep.
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

        let client = redis::Client::open(redis_url()).unwrap();
        let mut raw_conn = common::redis_conn::connect(&client).await.unwrap();
        let mut pipe = redis::pipe();
        for i in 0..BACKLOG {
            pipe.cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("payload")
                .arg(format!("payload-{i}"))
                .ignore();
        }
        let _: () = pipe.query_async(&mut raw_conn).await.unwrap();

        // Deliver the whole backlog to "dead-consumer" (live `>` reads, 100
        // at a time) and never ack it -- a consumer rename / dead pod.
        payloads(dead.next_batch().await.unwrap()); // drain empty startup PEL
        let mut delivered_to_dead = 0;
        while delivered_to_dead < BACKLOG {
            let batch = payloads(dead.next_batch().await.unwrap());
            assert!(!batch.is_empty(), "backlog should still be readable");
            delivered_to_dead += batch.len();
        }
        drop(dead);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut live = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "live-consumer",
            Duration::from_millis(50),
        )
        .await
        .unwrap();
        let replayed = payloads(live.next_batch().await.unwrap());
        assert_eq!(
            replayed.len(),
            PEL_REPLAY_BATCH_COUNT,
            "one reclaim + replay must deliver a full PEL_REPLAY_BATCH_COUNT batch, \
             not the old 100-entry trickle"
        );
        assert!(
            replayed.len() >= 500,
            "a regression back toward the old 100-per-sweep replay must fail this test \
             (got {})",
            replayed.len()
        );
        assert_eq!(replayed.first().map(String::as_str), Some("payload-0"));

        cleanup(&stream).await;
    }

    /// M15 (Repeater Signal review): a pending-entries list larger than one
    /// replay batch is delivered in full by consecutive `next_batch` calls,
    /// not one batch per `XAUTOCLAIM` sweep. Before the fix the second call
    /// switched to `>` and the remaining entries waited for a later sweep.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_pel_larger_than_one_batch_is_replayed_in_full_without_waiting_for_a_sweep() {
        let stream = unique_stream("pel-multi-batch");
        let total = PEL_REPLAY_BATCH_COUNT * 2 + 7;
        let connect = || async {
            RedisStreamMovementFeed::connect_for_test(
                &redis_url(),
                &stream,
                "test-group",
                "test-consumer",
                Duration::from_secs(3600),
            )
            .await
            .unwrap()
        };

        // Creates the group (at `$`) before anything is added.
        drop(connect().await);
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let mut pipe = redis::pipe();
        for i in 0..total {
            pipe.cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("payload")
                .arg(format!("p{i}"))
                .ignore();
        }
        let _: () = pipe.query_async(&mut conn).await.unwrap();
        // Deliver everything to this consumer name without acking, as a
        // crashed previous run would have.
        let _: redis::streams::StreamReadReply = conn
            .xread_options(
                &[&stream],
                &[">"],
                &redis::streams::StreamReadOptions::default()
                    .group("test-group", "test-consumer")
                    .count(total),
            )
            .await
            .unwrap();

        let mut feed = connect().await;
        let mut replayed = Vec::new();
        for _ in 0..3 {
            let batch = payloads(feed.next_batch().await.unwrap());
            assert!(
                !batch.is_empty(),
                "every replay call must deliver pending entries"
            );
            replayed.extend(batch);
            feed.commit().await.unwrap();
        }
        assert_eq!(replayed.len(), total);
        assert_eq!(replayed.first().map(String::as_str), Some("p0"));
        assert_eq!(
            replayed.last().cloned(),
            Some(format!("p{}", total - 1)),
            "entries are replayed in order"
        );
        let pending: redis::streams::StreamPendingReply =
            conn.xpending(&stream, "test-group").await.unwrap();
        assert_eq!(pending.count(), 0, "every replayed entry was acked");

        cleanup(&stream).await;
    }

    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn check_gap_detects_a_trimmed_range_the_group_never_read() {
        let stream = unique_stream("gap-detected");

        let mut feed = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

        xadd(&stream, "payload-1").await;
        xadd(&stream, "payload-2").await;
        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        let delivered = payloads(feed.next_batch().await.unwrap());
        assert_eq!(delivered.len(), 2);
        feed.commit().await.unwrap();

        // Force aggressive trimming past what the group has read, via a
        // separate client (MAXLEN on XADD only trims on write).
        let client = redis::Client::open(redis_url()).unwrap();
        let mut raw_conn = common::redis_conn::connect(&client).await.unwrap();
        for i in 0..10 {
            let _: String = redis::cmd("XADD")
                .arg(&stream)
                .arg("MAXLEN")
                .arg(1)
                .arg("*")
                .arg("payload")
                .arg(format!("filler-{i}"))
                .query_async(&mut raw_conn)
                .await
                .unwrap();
        }

        let gap = feed.check_gap().await.unwrap();
        let gap = gap.expect("a gap should be detected");
        assert_eq!(
            gap.unread_entries_lost,
            Some(9),
            "filler-0..=filler-8 were trimmed before the group read them"
        );
        assert!(
            stream_id_less_than(&gap.group_last_delivered_id, &gap.stream_first_entry_id),
            "last-delivered-id ({}) must be provably older than the new first-entry ({})",
            gap.group_last_delivered_id,
            gap.stream_first_entry_id
        );

        cleanup(&stream).await;
    }

    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn check_gap_reports_none_when_the_group_is_caught_up() {
        let stream = unique_stream("gap-none");

        let mut feed = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

        xadd(&stream, "payload-1").await;
        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        let delivered = payloads(feed.next_batch().await.unwrap());
        assert_eq!(delivered, vec!["payload-1".to_string()]);
        feed.commit().await.unwrap();

        let gap = feed.check_gap().await.unwrap();
        assert_eq!(gap, None, "no trimming has happened, so there is no gap");

        cleanup(&stream).await;
    }

    /// The old check's false alarm, against a real server: the group has
    /// read and `ACKed` everything, then those entries are trimmed. The first
    /// retained entry is the group's next unread one -- nothing was lost.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn check_gap_is_quiet_when_only_already_read_entries_were_trimmed() {
        let stream = unique_stream("gap-successor");
        let mut feed = connect(&stream).await;
        for i in 0..3 {
            xadd(&stream, &format!("read-{i}")).await;
        }
        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        assert_eq!(payloads(feed.next_batch().await.unwrap()).len(), 3);
        feed.commit().await.unwrap();

        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("MAXLEN")
            .arg(1)
            .arg("*")
            .arg("payload")
            .arg("unread")
            .query_async(&mut feed.conn)
            .await
            .unwrap();

        let positions = feed.stream_positions().await.unwrap();
        assert!(
            stream_id_less_than(
                positions.group_last_delivered_id.as_deref().unwrap(),
                positions.stream_first_entry_id.as_deref().unwrap()
            ),
            "the shape the old id-only check flagged as a gap"
        );
        assert_eq!(feed.check_gap().await.unwrap(), None);
        cleanup(&stream).await;
    }

    /// Delivered, never `ACKed`, then trimmed: the PEL now points at entries
    /// that no longer exist. The old check could not see this at all.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn check_gap_reports_pending_entries_trimmed_before_their_ack() {
        let stream = unique_stream("gap-pending");
        let mut feed = connect(&stream).await;
        xadd(&stream, "a").await;
        xadd(&stream, "b").await;
        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        assert_eq!(payloads(feed.next_batch().await.unwrap()).len(), 2);
        // No commit: both stay pending.
        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("MAXLEN")
            .arg(1)
            .arg("*")
            .arg("payload")
            .arg("c")
            .query_async(&mut feed.conn)
            .await
            .unwrap();

        let gap = feed
            .check_gap()
            .await
            .unwrap()
            .expect("pending entries were trimmed");
        assert!(gap.pending_entries_trimmed);
        assert_eq!(
            gap.unread_entries_lost, None,
            "every unread entry is still there"
        );
        cleanup(&stream).await;
    }

    /// `read_range` is a group-less XRANGE: it pages the stream by id
    /// without delivering anything to the group, and `group_pending_ids`
    /// lists exactly the group's PEL.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn read_range_pages_without_touching_the_group() {
        let stream = unique_stream("read-range");
        let mut feed = connect(&stream).await;
        for i in 0..5 {
            xadd(&stream, &format!("p{i}")).await;
        }
        let first = feed.read_range("-", "+", 2).await.unwrap();
        assert_eq!(
            first
                .entries
                .iter()
                .map(|(_, p)| p.as_str())
                .collect::<Vec<_>>(),
            vec!["p0", "p1"]
        );
        let after = format!("({}", first.last_id.unwrap());
        let rest = feed.read_range(&after, "+", 10).await.unwrap();
        assert_eq!(
            rest.entries
                .iter()
                .map(|(_, p)| p.as_str())
                .collect::<Vec<_>>(),
            vec!["p2", "p3", "p4"]
        );

        let positions = feed.stream_positions().await.unwrap();
        assert_eq!(positions.pending_count, 0, "XRANGE delivers nothing");
        assert_eq!(positions.stream_length, 5);
        assert_eq!(positions.stream_entries_added, Some(5));

        payloads(feed.next_batch().await.unwrap()); // drain empty startup PEL
        assert_eq!(payloads(feed.next_batch().await.unwrap()).len(), 5);
        let pending = feed.group_pending_ids().await.unwrap();
        assert_eq!(pending.len(), 5);
        let positions = feed.stream_positions().await.unwrap();
        assert_eq!(positions.pending_count, 5);
        assert!(pending.contains(positions.pending_min_id.as_deref().unwrap()));
        cleanup(&stream).await;
    }

    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn three_independent_consumer_groups_each_receive_every_entry_independently() {
        let stream = unique_stream("three-groups");

        // Mirrors this plan's real deployment shape: trust-consumer,
        // full-coverage-consumer, and trust-backlog-consumer are three
        // independent named groups on the SAME stream.
        let mut trust_consumer = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "trust-consumer",
            "trust-consumer-1",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
        let mut full_coverage_consumer = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "full-coverage-consumer",
            "full-coverage-consumer-1",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
        let mut trust_backlog_consumer = RedisStreamMovementFeed::connect_for_test(
            &redis_url(),
            &stream,
            "trust-event-backlog",
            "trust-event-backlog-1",
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

        xadd(&stream, "payload-1").await;

        // Each group's own startup PEL-replay pass is legitimately empty
        // first (see this module's own doc comment on every other test here),
        // then the SAME entry is delivered to all three independently.
        payloads(trust_consumer.next_batch().await.unwrap());
        payloads(full_coverage_consumer.next_batch().await.unwrap());
        payloads(trust_backlog_consumer.next_batch().await.unwrap());

        let a = payloads(trust_consumer.next_batch().await.unwrap());
        let b = payloads(full_coverage_consumer.next_batch().await.unwrap());
        let c = payloads(trust_backlog_consumer.next_batch().await.unwrap());

        assert_eq!(
            a,
            vec!["payload-1".to_string()],
            "trust-consumer must see the entry"
        );
        assert_eq!(
            b,
            vec!["payload-1".to_string()],
            "full-coverage-consumer must ALSO see the same entry"
        );
        assert_eq!(
            c,
            vec!["payload-1".to_string()],
            "trust-backlog-consumer must ALSO see the same entry -- proving the third group does not steal it from, or split it with, the other two"
        );

        // Each group acks independently -- one group's XACK must not affect
        // another's own pending-entries list.
        trust_consumer.commit().await.unwrap();
        let pending_full_coverage: redis::streams::StreamPendingCountReply = full_coverage_consumer
            .conn
            .xpending_count(&stream, "full-coverage-consumer", "-", "+", 10)
            .await
            .unwrap();
        assert_eq!(
            pending_full_coverage.ids.len(),
            1,
            "full-coverage-consumer's own pending entry must be unaffected by trust-consumer's ack"
        );

        cleanup(&stream).await;
    }

    /// Redis back from an outage without its data (stream and group gone):
    /// the read that hits `NOGROUP` fails, recreates the group, and the
    /// following read works -- no restart needed.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_lost_consumer_group_is_recreated_after_nogroup() {
        let stream = unique_stream("nogroup");
        let mut feed = connect(&stream).await;
        assert!(payloads(feed.next_batch().await.unwrap()).is_empty());

        cleanup(&stream).await;
        let err = feed.next_batch().await.expect_err("the group is gone");
        assert!(is_nogroup(&err), "{err:?}");

        assert!(payloads(feed.next_batch().await.unwrap()).is_empty());
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("payload")
            .arg("after-recreate")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            payloads(feed.next_batch().await.unwrap()),
            vec!["after-recreate"]
        );
        feed.commit().await.unwrap();
        cleanup(&stream).await;
    }

    /// Reads until a `>` read comes back empty (the first call after a
    /// connect or recreate is the PEL replay), committing every batch.
    async fn drain(feed: &mut RedisStreamMovementFeed) -> Vec<String> {
        let mut out = Vec::new();
        let mut empty_reads = 0;
        while empty_reads < 2 {
            let batch = payloads(feed.next_batch().await.unwrap());
            if batch.is_empty() {
                empty_reads += 1;
            }
            out.extend(batch);
            feed.commit().await.unwrap();
        }
        out
    }

    async fn xadd_with_id(stream: &str, id: &str, payload: &str) {
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = client.get_connection_manager().await.unwrap();
        let _: String = redis::cmd("XADD")
            .arg(stream)
            .arg(id)
            .arg("payload")
            .arg(payload)
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    /// Redis back empty and a producer already writing before this
    /// consumer's next read: the stream is recreated by `XADD`, without the
    /// group. Those entries used to be skipped (the group was recreated at
    /// `$`); now the group resumes after the last entry it delivered, so
    /// every one of them is read.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn entries_written_before_a_lost_group_is_recreated_are_not_lost() {
        let stream = unique_stream("nogroup-recreated-stream");
        let mut feed = connect(&stream).await;
        xadd(&stream, "before-1").await;
        xadd(&stream, "before-2").await;
        assert_eq!(drain(&mut feed).await, vec!["before-1", "before-2"]);

        // Redis loses everything; the producer recreates the stream.
        cleanup(&stream).await;
        xadd(&stream, "after-1").await;
        xadd(&stream, "after-2").await;

        let err = feed.next_batch().await.expect_err("the group is gone");
        assert!(is_nogroup(&err), "{err:?}");
        assert_eq!(drain(&mut feed).await, vec!["after-1", "after-2"]);
        cleanup(&stream).await;
    }

    /// Same, on a server whose clock is behind the old one: the recreated
    /// stream's ids are all older than the last one delivered, which an
    /// intact stream can never be, so it is read from the start.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_recreated_stream_with_older_ids_is_read_from_the_start() {
        let stream = unique_stream("nogroup-clock-behind");
        let mut feed = connect(&stream).await;
        xadd(&stream, "before").await;
        assert_eq!(drain(&mut feed).await, vec!["before"]);

        cleanup(&stream).await;
        xadd_with_id(&stream, "1-0", "after-1").await;
        xadd_with_id(&stream, "2-0", "after-2").await;

        let err = feed.next_batch().await.expect_err("the group is gone");
        assert!(is_nogroup(&err), "{err:?}");
        assert_eq!(drain(&mut feed).await, vec!["after-1", "after-2"]);
        cleanup(&stream).await;
    }

    /// The group deleted by hand while the stream keeps its data: the
    /// recreated group resumes after the last entry it had delivered -- no
    /// replay of the whole stream (as `0` would), no skipping of what was
    /// added meanwhile (as `$` would).
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_group_deleted_by_hand_resumes_without_replaying_the_stream() {
        let stream = unique_stream("nogroup-destroyed");
        let mut feed = connect(&stream).await;
        for i in 0..5 {
            xadd(&stream, &format!("read-{i}")).await;
        }
        assert_eq!(drain(&mut feed).await.len(), 5);

        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let destroyed: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&stream)
            .arg("test-group")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(destroyed, 1);
        xadd(&stream, "unread").await;

        let err = feed.next_batch().await.expect_err("the group is gone");
        assert!(is_nogroup(&err), "{err:?}");
        assert_eq!(drain(&mut feed).await, vec!["unread"]);
        cleanup(&stream).await;
    }

    /// A group that is lost again before this process has read anything
    /// still resumes where it stood: the position is seeded at connect from
    /// `XINFO GROUPS`, not only learnt from reads.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn the_recovery_position_is_seeded_at_connect() {
        let stream = unique_stream("nogroup-seeded");
        xadd(&stream, "old").await; // before the group: never visible to it
        let mut feed = connect(&stream).await;
        assert!(feed.last_delivered_id.is_some());

        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let _: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&stream)
            .arg("test-group")
            .query_async(&mut conn)
            .await
            .unwrap();
        xadd(&stream, "new").await;

        // The startup PEL replay hits NOGROUP first.
        let err = feed.next_batch().await.expect_err("the group is gone");
        assert!(is_nogroup(&err), "{err:?}");
        assert_eq!(drain(&mut feed).await, vec!["new"]);
        cleanup(&stream).await;
    }
}

/// A Redis outage must never park a consumer inside redis-rs: startup waits
/// visibly (logged, beating progress), and once running every command fails
/// within seconds so the consumer's own loop retries. See
/// `common::redis_conn`.
#[cfg(test)]
mod outage_tests {

    use redis::IntoConnectionInfo;

    use super::*;

    /// A local port with nothing listening on it (bound, then released).
    fn closed_local_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// Redis unreachable at startup: `connect_until_ready` keeps retrying
    /// (it does not return an error for the consumer to exit on) and every
    /// failed attempt beats progress, so a stall watchdog never fires.
    #[tokio::test]
    async fn connect_until_ready_waits_for_an_unreachable_redis_and_beats_progress() {
        let redis_url = format!("redis://127.0.0.1:{}", closed_local_port());
        let progress = health_http::Progress::new(Duration::from_millis(500));
        let watcher = progress.clone();
        let connecting = tokio::spawn(async move {
            RedisStreamMovementFeed::connect_until_ready(
                &redis_url,
                "trust-consumer",
                "trust-consumer-1",
                Duration::from_secs(30),
                common::backoff::Backoff::new(
                    Duration::from_millis(20),
                    Duration::from_millis(100),
                ),
                &progress,
            )
            .await
            .map(|_| ())
        });
        // 4x the stall window.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!watcher.is_stalled(), "failed attempts must beat progress");
        }
        assert!(
            !connecting.is_finished(),
            "an unreachable Redis is waited for, not exited on"
        );
        connecting.abort();
    }

    /// Only an unparseable URL is an error; it is not retried forever.
    #[tokio::test]
    async fn connect_until_ready_rejects_an_unparseable_url() {
        let progress = health_http::Progress::new(Duration::from_secs(60));
        let result = RedisStreamMovementFeed::connect_until_ready(
            "not a url",
            "g",
            "c",
            Duration::from_secs(30),
            common::startup::CONNECT_BACKOFF,
            &progress,
        )
        .await;
        assert!(result.is_err());
    }

    /// A TCP proxy in front of the real Redis that the test can take down
    /// (closing every proxied connection, then refusing new ones) and bring
    /// back on the same port: a Redis pod restart, without touching the
    /// shared local Redis.
    struct Proxy {
        port: u16,
        tasks: tokio::task::JoinSet<()>,
        upstream: String,
    }

    impl Proxy {
        async fn start(upstream: String) -> Self {
            let mut proxy = Self {
                port: closed_local_port(),
                tasks: tokio::task::JoinSet::new(),
                upstream,
            };
            proxy.bring_up().await;
            proxy
        }

        async fn bring_up(&mut self) {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", self.port))
                .await
                .expect("rebind the proxy port");
            let upstream = self.upstream.clone();
            self.tasks.spawn(async move {
                // Dropped (aborting, and so closing, every proxied
                // connection) together with the listener when this task is
                // aborted by `take_down`.
                let mut connections = tokio::task::JoinSet::new();
                while let Ok((mut client, _)) = listener.accept().await {
                    let upstream = upstream.clone();
                    connections.spawn(async move {
                        if let Ok(mut server) = tokio::net::TcpStream::connect(&upstream).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                        }
                    });
                }
            });
        }

        /// Stops listening and closes every proxied connection.
        async fn take_down(&mut self) {
            self.tasks.shutdown().await;
        }
    }

    /// `REDIS_URL` (plus `REDIS_PASSWORD`), as the services combine them.
    fn direct_redis_url() -> String {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
        with_password(&url)
    }

    fn with_password(url: &str) -> String {
        let password = std::env::var("REDIS_PASSWORD")
            .ok()
            .map(common::secret::Secret::from);
        common::redis_auth::redis_url_with_password(url, password.as_ref())
            .unwrap()
            .expose()
            .to_owned()
    }

    fn upstream_addr() -> String {
        match direct_redis_url().into_connection_info().unwrap().addr {
            redis::ConnectionAddr::Tcp(host, port) => format!("{host}:{port}"),
            other => panic!("this test needs a TCP REDIS_URL, got {other:?}"),
        }
    }

    /// Redis goes away mid-run and comes back. While it is down every read
    /// fails within seconds (one bounded reconnect attempt, no redis-rs
    /// backoff), so the consumer loop can log, back off and beat progress;
    /// once it is back the same feed reads again, with no restart.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn reads_fail_fast_during_a_redis_outage_and_recover_after_it() {
        let mut proxy = Proxy::start(upstream_addr()).await;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let stream = format!("movement-events-test-outage-{nanos}");
        let mut feed = RedisStreamMovementFeed::connect_for_test(
            &with_password(&format!("redis://127.0.0.1:{}", proxy.port)),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .expect("connects through the proxy");
        assert!(feed.next_batch().await.unwrap().is_empty());

        proxy.take_down().await;
        for attempt in 0..4 {
            let started = std::time::Instant::now();
            let result = feed.next_batch().await;
            let took = started.elapsed();
            assert!(result.is_err(), "attempt {attempt}: Redis is down");
            assert!(
                took < common::redis_conn::CONNECT_TIMEOUT + Duration::from_secs(1),
                "attempt {attempt} took {took:?}: a read during an outage must fail fast"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        proxy.bring_up().await;
        // The first read after the outage may still see the last failed
        // reconnect attempt; the consumer loop's retry reconnects.
        let mut recovered = false;
        for _ in 0..10 {
            if feed.next_batch().await.is_ok() {
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(recovered, "the same feed reads again once Redis is back");

        let client = redis::Client::open(direct_redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let _: redis::RedisResult<i64> =
            redis::cmd("DEL").arg(&stream).query_async(&mut conn).await;
    }

    /// Redis restarts and comes back with an older copy of its data: the
    /// group exists but stands before entries this consumer already read
    /// (simulated by an `XGROUP SETID` back to the start while the proxy is
    /// down). The first read after the outage moves it forward again, so
    /// nothing already read is delivered again; a new entry still is.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_group_rewound_by_a_redis_restart_is_moved_forward_again() {
        let mut proxy = Proxy::start(upstream_addr()).await;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let stream = format!("movement-events-test-rewind-{nanos}");
        let client = redis::Client::open(direct_redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let mut feed = RedisStreamMovementFeed::connect_for_test(
            &with_password(&format!("redis://127.0.0.1:{}", proxy.port)),
            &stream,
            "test-group",
            "test-consumer",
            Duration::from_secs(3600),
        )
        .await
        .expect("connects through the proxy");
        assert!(feed.next_batch().await.unwrap().is_empty());

        for payload in ["a", "b"] {
            let _: String = redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("payload")
                .arg(payload)
                .query_async(&mut conn)
                .await
                .unwrap();
        }
        let read: Vec<String> = feed
            .next_batch()
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.payload)
            .collect();
        assert_eq!(read, ["a", "b"]);
        feed.commit().await.unwrap();
        let read_up_to = feed
            .last_delivered_id
            .clone()
            .expect("advanced by the read");

        proxy.take_down().await;
        let () = redis::cmd("XGROUP")
            .arg("SETID")
            .arg(&stream)
            .arg("test-group")
            .arg("0")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(feed.next_batch().await.is_err(), "Redis is down");
        proxy.bring_up().await;

        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("payload")
            .arg("c")
            .query_async(&mut conn)
            .await
            .unwrap();
        let mut read = Vec::new();
        for _ in 0..10 {
            if let Ok(batch) = feed.next_batch().await {
                read.extend(batch.into_iter().map(|entry| entry.payload));
                if !read.is_empty() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_eq!(read, ["c"], "a and b must not be delivered again");
        let group_at =
            common::redis_group::group_last_delivered_id(&mut conn, &stream, "test-group")
                .await
                .unwrap()
                .unwrap();
        assert!(
            !stream_id_less_than(&group_at, &read_up_to),
            "the group is back at or past {read_up_to}, not {group_at}"
        );

        let _: redis::RedisResult<i64> =
            redis::cmd("DEL").arg(&stream).query_async(&mut conn).await;
    }
}
