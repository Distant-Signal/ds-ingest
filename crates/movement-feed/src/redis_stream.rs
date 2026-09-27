//! `RedisStreamMovementFeed`: the real, production `MovementFeed`
//! implementation from Deploy B onward -- reads the `movement-events`
//! stream `movement-relay` writes to, as one of its two fixed consumer
//! groups (`trust-consumer` or `full-coverage-consumer`).
//! See docs/superpowers/specs/2026-09-04-movement-relay-design.md
//! Decision 2 for the full reasoning; this module implements it, it does
//! not re-argue it.

use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::{DeadLetter, DeadLetterSink, MovementFeed};

const STREAM: &str = "movement-events";

/// Suffix of the dead-letter stream, so the production one is
/// `movement-events-deadletter` and each test stream gets its own.
const DEAD_LETTER_SUFFIX: &str = "-deadletter";

/// Approximate (`MAXLEN ~`) cap on the dead-letter stream. Entries are
/// small TRUST envelopes or single backlog rows (well under 1KB), so this
/// bounds it to roughly 10MB of Redis memory: big enough to hold a whole
/// PEL-replay batch several times over (the 2026-09 incident had 1,679
/// stuck entries), small enough that a runaway source cannot grow it
/// without limit. It is shared by all three consumer groups; each record
/// carries its `group`.
const DEAD_LETTER_MAXLEN: usize = 10_000;

/// Default for [`RedisStreamMovementFeed::with_max_deliveries`].
///
/// Delivery counts climb by about 2 per `autoclaim_min_idle` sweep while an
/// entry keeps failing (`XAUTOCLAIM` adds one and the id-`0` PEL re-read
/// adds another -- checked against a local valkey 9), so at the deployed
/// 30s sweep this is roughly an hour of continuous failure. That is long
/// enough that an ordinary `api` restart or short outage never
/// dead-letters healthy data, and far short of the ~92 hours (22,047
/// deliveries) the 2026-09 poison batch sat retrying.
pub const DEFAULT_MAX_DELIVERIES: u64 = 240;

/// How many pending entries a single id=`0` PEL-replay read (`next_batch`)
/// or a single `XAUTOCLAIM` round-trip (`reclaim_stale`) asks Redis for at
/// once.
///
/// **Why this was raised from 100** (Repeater Signal review, finding M15):
/// `next_batch` only ever performs ONE id=`0` read per `replaying_pel`
/// activation before switching back to `>` (see that field's own doc) --
/// so the number of entries a single PEL replay actually delivers is
/// capped by this count, not by how many are sitting in the PEL. After a
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

pub struct RedisStreamMovementFeed {
    conn: ConnectionManager,
    stream: String,
    group: String,
    consumer: String,
    /// Startup replay of this consumer's own pending-entries list (`0`,
    /// not `>`) happens exactly once, before the first `>` read -- see
    /// `next_batch`'s own doc. Also flipped back to `true` by
    /// `reclaim_stale` after a non-empty `XAUTOCLAIM` claim, so a
    /// reclaimed entry is picked up through the same code path -- see that
    /// function's own doc.
    replaying_pel: bool,
    /// IDs returned by the most recent `next_batch` call, held until
    /// `commit` XACKs them or they're replaced by the next call -- same
    /// receive/confirm split `KafkaMovementFeed::last_received` already
    /// established, generalized to a `Vec` since one Redis Streams read
    /// can return more than one entry per call (unlike the Kafka feed,
    /// which only ever returned one message per `next_batch`).
    pending_ack: Vec<String>,
    last_autoclaim_sweep: std::time::Instant,
    autoclaim_min_idle: Duration,
    /// See [`Self::with_max_deliveries`]. `None` disables the guard.
    max_deliveries: Option<u64>,
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
    pub async fn connect(
        redis_url: &str,
        group: impl Into<String>,
        consumer: impl Into<String>,
        autoclaim_min_idle: Duration,
    ) -> anyhow::Result<Self> {
        Self::connect_to_stream(redis_url, STREAM, group, consumer, autoclaim_min_idle).await
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
        Self::connect_to_stream(
            redis_url,
            &stream.into(),
            group,
            consumer,
            autoclaim_min_idle,
        )
        .await
    }

    async fn connect_to_stream(
        redis_url: &str,
        stream: &str,
        group: impl Into<String>,
        consumer: impl Into<String>,
        autoclaim_min_idle: Duration,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let mut conn = client.get_connection_manager().await?;
        let group = group.into();

        ensure_group(&mut conn, stream, &group).await?;

        Ok(Self {
            conn,
            stream: stream.to_string(),
            group,
            consumer: consumer.into(),
            replaying_pel: true,
            pending_ack: Vec::new(),
            last_autoclaim_sweep: std::time::Instant::now() - autoclaim_min_idle,
            autoclaim_min_idle,
            max_deliveries: Some(DEFAULT_MAX_DELIVERIES),
        })
    }

    /// Poison-entry guard: an entry the PEL replay finds has already been
    /// delivered more than `max` times is written to the dead-letter stream
    /// and XACKed instead of being handed to the caller again. Without it,
    /// one entry that always makes the caller's downstream write fail (and
    /// with it every entry in the same batch) is replayed forever -- the
    /// 2026-09 trust-backlog-consumer incident. `0` disables the guard.
    /// Defaults to [`DEFAULT_MAX_DELIVERIES`].
    pub fn with_max_deliveries(mut self, max: u64) -> Self {
        self.max_deliveries = (max > 0).then_some(max);
        self
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

    /// Splits PEL-replay entries into those still deliverable and those
    /// over the delivery limit; the latter are dead-lettered and XACKed
    /// here. A dead-letter write failure returns `Err` with nothing ACKed,
    /// so the entries stay pending rather than being lost.
    async fn divert_over_delivered(
        &mut self,
        entries: Vec<(String, String)>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let (Some(max), Some(first), Some(last)) = (
            self.max_deliveries,
            entries.first().map(|(id, _)| id.clone()),
            entries.last().map(|(id, _)| id.clone()),
        ) else {
            return Ok(entries);
        };
        // PEL_REPLAY_BATCH_COUNT, not entries.len(): the same range can also
        // hold this read's malformed entries, which are XACKed only later.
        let counts = self
            .delivery_counts(&first, &last, PEL_REPLAY_BATCH_COUNT)
            .await?;
        let (over, keep) = partition_over_delivered(entries, &counts, max);
        if over.is_empty() {
            return Ok(keep);
        }
        let records: Vec<DeadLetter> = over
            .iter()
            .map(|(id, payload, delivered)| DeadLetter {
                reason: "max_deliveries_exceeded",
                source_id: Some(id.clone()),
                delivery_count: Some(*delivered),
                payload: payload.clone(),
                detail: format!("delivered {delivered} times (limit {max})"),
            })
            .collect();
        self.dead_letter(&records).await?;
        let ids: Vec<&String> = over.iter().map(|(id, _, _)| id).collect();
        let _: i64 = self.conn.xack(&self.stream, &self.group, &ids).await?;
        Ok(keep)
    }
}

/// Pure half of `divert_over_delivered`: `(over_limit, keep)`, where
/// over-limit entries carry their delivery count. An entry XPENDING did not
/// report (acked meanwhile, or beyond the reply) is kept.
#[allow(clippy::type_complexity)]
fn partition_over_delivered(
    entries: Vec<(String, String)>,
    counts: &std::collections::HashMap<String, u64>,
    max: u64,
) -> (Vec<(String, String, u64)>, Vec<(String, String)>) {
    let mut over = Vec::new();
    let mut keep = Vec::new();
    for (id, payload) in entries {
        match counts.get(&id) {
            Some(&delivered) if delivered > max => over.push((id, payload, delivered)),
            _ => keep.push((id, payload)),
        }
    }
    (over, keep)
}

#[async_trait]
impl DeadLetterSink for RedisStreamMovementFeed {
    /// `XADD <stream>-deadletter MAXLEN ~ 10000` per record (one pipeline),
    /// each with its `group`, `consumer`, `reason`, `source_id`,
    /// `delivery_count`, `detail` and `payload`, plus a warn log and a
    /// `distant_signal_movement_feed_deadlettered_total{group, reason}`
    /// increment. The Redis stream is the record of truth (it keeps the
    /// payload, so an operator can inspect it with `XRANGE` and re-inject
    /// it once the cause is fixed); the log and metric are for alerting.
    async fn dead_letter(&mut self, records: &[DeadLetter]) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let stream = self.dead_letter_stream();
        let mut pipe = redis::pipe();
        for record in records {
            pipe.cmd("XADD")
                .arg(&stream)
                .arg("MAXLEN")
                .arg("~")
                .arg(DEAD_LETTER_MAXLEN)
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
                "dead-lettered a record instead of retrying it forever"
            );
            metrics::counter!(
                common::metrics::metric_name("movement_feed_deadlettered_total"),
                "group" => self.group.clone(),
                "reason" => record.reason
            )
            .increment(1);
        }
        Ok(())
    }
}

/// Idempotent group creation, `MKSTREAM`-backed -- verbatim in spirit from
/// `crates/enricher/src/stream.rs::ensure_group`, generalized over the
/// stream/group name (this crate serves two different group names -- and,
/// in tests, many different stream names -- from one implementation,
/// unlike enricher's single hardcoded `STREAM`/`GROUP`).
async fn ensure_group(
    conn: &mut ConnectionManager,
    stream: &str,
    group: &str,
) -> anyhow::Result<()> {
    let result: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(group)
        .arg("$")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;

    match result {
        Ok(()) => Ok(()),
        Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
        Err(err) => Err(err.into()),
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
fn split_deliverable_and_malformed(
    ids: Vec<redis::streams::StreamId>,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut entries = Vec::new();
    let mut malformed_ids = Vec::new();
    for entry in ids {
        match entry
            .map
            .get("payload")
            .and_then(|v| redis::from_redis_value::<String>(v).ok())
        {
            Some(payload) => entries.push((entry.id, payload)),
            None => malformed_ids.push(entry.id),
        }
    }
    (entries, malformed_ids)
}

#[async_trait]
impl MovementFeed for RedisStreamMovementFeed {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<String>> {
        // Periodic XAUTOCLAIM sweep, checked once per call -- cheap
        // (skips immediately if not due) and keeps this on the same
        // "checked every loop iteration" shape every existing multi-cadence
        // main.rs loop in this repo already uses, rather than a second
        // spawned task racing this one's own Redis connection.
        if self.last_autoclaim_sweep.elapsed() >= self.autoclaim_min_idle {
            self.reclaim_stale().await?;
            self.last_autoclaim_sweep = std::time::Instant::now();
        }

        let id_arg = if self.replaying_pel { "0" } else { ">" };
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
                    .count(if self.replaying_pel {
                        PEL_REPLAY_BATCH_COUNT
                    } else {
                        LIVE_READ_BATCH_COUNT
                    })
                    .block(if self.replaying_pel { 0 } else { 5000 }),
            )
            .await?;

        let (entries, malformed_ids) =
            split_deliverable_and_malformed(reply.keys.into_iter().flat_map(|k| k.ids).collect());

        // Only a PEL replay can return an entry that has been delivered
        // before; a `>` read is always a first delivery.
        let entries = if self.replaying_pel {
            self.divert_over_delivered(entries).await?
        } else {
            entries
        };

        // A malformed entry can never become processable -- see
        // `split_deliverable_and_malformed`'s own doc for why it is XACKed
        // here rather than left for a future PEL replay.
        if !malformed_ids.is_empty() {
            tracing::warn!(
                ids = ?malformed_ids,
                stream = %self.stream,
                group = %self.group,
                "stream entry missing expected `payload` field; acknowledging so it does not linger in the pending-entries list forever"
            );
            let _: i64 = self
                .conn
                .xack(&self.stream, &self.group, &malformed_ids)
                .await?;
        }

        // The PEL replay pass (id `0`) returns however many pending
        // entries this consumer name left unacked last time -- possibly
        // zero (a clean prior shutdown, or a first-ever run). EITHER WAY
        // it only ever runs once: a `0`-id read that returns nothing still
        // means "no more of MY OWN old pending entries," not "no more
        // entries in the stream" (there could be plenty ahead of `>` from
        // other consumers' progress) -- switching to `>` after exactly one
        // empty (or non-empty) `0`-read is correct regardless of which.
        if self.replaying_pel {
            self.replaying_pel = false;
        }

        self.pending_ack = entries.iter().map(|(id, _)| id.clone()).collect();
        Ok(entries.into_iter().map(|(_, payload)| payload).collect())
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        if self.pending_ack.is_empty() {
            return Ok(());
        }
        let ids = std::mem::take(&mut self.pending_ack);
        let _: i64 = self.conn.xack(&self.stream, &self.group, &ids).await?;
        Ok(())
    }
}

impl RedisStreamMovementFeed {
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
            self.replaying_pel = true;
        }
        Ok(())
    }

    /// Compares this group's `last-delivered-id` (via `XINFO GROUPS`)
    /// against the stream's current oldest retained entry (via `XINFO
    /// STREAM`'s `first-entry`). `Some(GapInfo)` means entries between
    /// those two IDs were trimmed (`MAXLEN`) before this group ever read
    /// them -- a provable gap, not a suspicion. Call on the same cadence
    /// this crate's caller already reloads its other periodic state (see
    /// Task 4) -- cheap, two Redis round-trips, no new polling loop.
    pub async fn check_gap(&mut self) -> anyhow::Result<Option<GapInfo>> {
        let groups: Vec<redis::Value> = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&self.stream)
            .query_async(&mut self.conn)
            .await?;
        let Some(last_delivered_id) = find_group_field(&groups, &self.group, "last-delivered-id")?
        else {
            return Ok(None); // group doesn't exist yet -- nothing to compare.
        };

        let stream_info: Vec<redis::Value> = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(&self.stream)
            .query_async(&mut self.conn)
            .await?;
        let Some(first_entry_id) = find_stream_first_entry_id(&stream_info)? else {
            return Ok(None); // empty stream -- nothing trimmed yet.
        };

        if stream_id_less_than(&last_delivered_id, &first_entry_id) {
            Ok(Some(GapInfo {
                group_last_delivered_id: last_delivered_id,
                stream_first_entry_id: first_entry_id,
            }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapInfo {
    pub group_last_delivered_id: String,
    pub stream_first_entry_id: String,
}

/// `XINFO GROUPS`'s reply is an array of per-group entries, each a flat
/// array of alternating field name/value pairs -- same shape
/// `enricher::stream::group_lag` already parses. Pulls out `field` for the
/// group named `group`.
fn find_group_field(
    groups: &[redis::Value],
    group: &str,
    field: &str,
) -> anyhow::Result<Option<String>> {
    for entry in groups {
        let redis::Value::Array(fields) = entry else {
            continue;
        };
        let mut name: Option<String> = None;
        let mut value: Option<String> = None;
        let mut it = fields.iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            let k: String = redis::from_redis_value(k)?;
            if k == "name" {
                name = redis::from_redis_value(v).ok();
            } else if k == field {
                value = redis::from_redis_value(v).ok();
            }
        }
        if name.as_deref() == Some(group) {
            return Ok(value);
        }
    }
    Ok(None)
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
fn stream_id_less_than(a: &str, b: &str) -> bool {
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
            vec!["2-0".to_string()],
            "the entry's id must still be surfaced so the caller can XACK it -- \
             this is the fix: it used to be silently dropped by a filter_map \
             with no id ever reaching an XACK, leaving it pending forever"
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
        assert_eq!(malformed_ids, vec!["2-0".to_string()]);
    }
}

#[cfg(test)]
mod partition_over_delivered_tests {
    use std::collections::HashMap;

    use super::*;

    fn entry(id: &str) -> (String, String) {
        (id.to_string(), format!("payload-{id}"))
    }

    #[test]
    fn only_entries_over_the_limit_are_diverted() {
        let counts: HashMap<String, u64> = [
            ("1-0".to_string(), 51),
            ("2-0".to_string(), 50),
            ("3-0".to_string(), 3),
        ]
        .into_iter()
        .collect();

        let (over, keep) =
            partition_over_delivered(vec![entry("1-0"), entry("2-0"), entry("3-0")], &counts, 50);

        assert_eq!(
            over,
            vec![("1-0".to_string(), "payload-1-0".to_string(), 51)]
        );
        assert_eq!(keep, vec![entry("2-0"), entry("3-0")]);
    }

    #[test]
    fn an_entry_xpending_did_not_report_is_kept() {
        let (over, keep) = partition_over_delivered(vec![entry("9-0")], &HashMap::new(), 1);
        assert!(over.is_empty());
        assert_eq!(keep, vec![entry("9-0")]);
    }
}

#[cfg(test)]
mod redis_tests {
    use super::*;

    fn redis_url() -> String {
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into())
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
        let mut conn = client.get_connection_manager().await.unwrap();
        let _: redis::RedisResult<i64> = redis::cmd("DEL")
            .arg(stream)
            .arg(format!("{stream}{DEAD_LETTER_SUFFIX}"))
            .query_async(&mut conn)
            .await;
    }

    /// `(field -> value)` maps of every entry in `stream`'s dead-letter stream.
    async fn dead_letters(stream: &str) -> Vec<std::collections::HashMap<String, String>> {
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = client.get_connection_manager().await.unwrap();
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

    /// The poison-entry guard: an entry that keeps failing downstream is
    /// replayed until its delivery count passes the limit, then
    /// dead-lettered (payload intact) and XACKed instead of replayed again.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn an_over_delivered_entry_is_dead_lettered_and_acked() {
        let stream = unique_stream("over-delivered");
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
            .with_max_deliveries(2)
        };

        let mut feed = connect().await;
        xadd(&stream, "poison").await;
        feed.next_batch().await.unwrap(); // drain empty startup PEL
        assert_eq!(feed.next_batch().await.unwrap(), vec!["poison".to_string()]); // delivery 1
        drop(feed); // downstream failed: never committed

        let mut feed = connect().await;
        assert_eq!(
            feed.next_batch().await.unwrap(),
            vec!["poison".to_string()],
            "delivery 2 is still within the limit"
        );
        drop(feed);

        let mut feed = connect().await;
        assert!(
            feed.next_batch().await.unwrap().is_empty(),
            "delivery 3 is over the limit: diverted, not handed out again"
        );
        let pending: redis::streams::StreamPendingCountReply = feed
            .conn
            .xpending_count(&stream, "test-group", "-", "+", 10)
            .await
            .unwrap();
        assert!(pending.ids.is_empty(), "the diverted entry is XACKed");

        let letters = dead_letters(&stream).await;
        assert_eq!(letters.len(), 1);
        assert_eq!(letters[0]["payload"], "poison");
        assert_eq!(letters[0]["reason"], "max_deliveries_exceeded");
        assert_eq!(letters[0]["group"], "test-group");
        assert_eq!(letters[0]["delivery_count"], "3");

        cleanup(&stream).await;
    }

    /// With the guard disabled (`0`), the same entry keeps being replayed.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn max_deliveries_zero_disables_the_guard() {
        let stream = unique_stream("guard-disabled");
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
            .with_max_deliveries(0)
        };
        let mut feed = connect().await;
        xadd(&stream, "payload-1").await;
        feed.next_batch().await.unwrap();
        feed.next_batch().await.unwrap();
        for _ in 0..3 {
            drop(feed);
            feed = connect().await;
            assert_eq!(
                feed.next_batch().await.unwrap(),
                vec!["payload-1".to_string()]
            );
        }
        assert!(dead_letters(&stream).await.is_empty());
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
        let mut conn = client.get_connection_manager().await.unwrap();
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
    /// `payload` field must be XACKed by `next_batch` itself, not left
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
        let mut raw_conn = client.get_connection_manager().await.unwrap();
        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("not_payload")
            .arg("whatever")
            .query_async(&mut raw_conn)
            .await
            .unwrap();

        feed.next_batch().await.unwrap(); // drain empty startup PEL
        let batch = feed.next_batch().await.unwrap();
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
            feed.next_batch().await.unwrap().is_empty(),
            "startup PEL drain: nothing pending yet"
        );
        let batch = feed.next_batch().await.unwrap();
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
        let replayed = feed2.next_batch().await.unwrap();
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
        feed.next_batch().await.unwrap(); // drain empty startup PEL
        let delivered = feed.next_batch().await.unwrap();
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
        feed.next_batch().await.unwrap(); // drain empty startup PEL
        let first = feed.next_batch().await.unwrap();
        assert_eq!(first, vec!["payload-1".to_string()]);

        // A third call, still without committing the second's delivery --
        // must NOT redeliver payload-1 via `>` (it's legitimately
        // "delivered, not yet acked", which only a reconnect's PEL replay
        // should surface). Nothing new is in the stream, so this blocks on
        // `>` for up to 5s and then returns empty.
        let third = feed.next_batch().await.unwrap();
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
        dead.next_batch().await.unwrap(); // drain empty startup PEL
        let to_dead = dead.next_batch().await.unwrap();
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
        let reclaimed = live.next_batch().await.unwrap();
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
        let mut raw_conn = client.get_connection_manager().await.unwrap();
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
        dead.next_batch().await.unwrap(); // drain empty startup PEL
        let mut delivered_to_dead = 0;
        while delivered_to_dead < BACKLOG {
            let batch = dead.next_batch().await.unwrap();
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
        let replayed = live.next_batch().await.unwrap();
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
        feed.next_batch().await.unwrap(); // drain empty startup PEL
        let delivered = feed.next_batch().await.unwrap();
        assert_eq!(delivered.len(), 2);
        feed.commit().await.unwrap();

        // Force aggressive trimming past what the group has read, via a
        // separate client (MAXLEN on XADD only trims on write).
        let client = redis::Client::open(redis_url()).unwrap();
        let mut raw_conn = client.get_connection_manager().await.unwrap();
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
        feed.next_batch().await.unwrap(); // drain empty startup PEL
        let delivered = feed.next_batch().await.unwrap();
        assert_eq!(delivered, vec!["payload-1".to_string()]);
        feed.commit().await.unwrap();

        let gap = feed.check_gap().await.unwrap();
        assert_eq!(gap, None, "no trimming has happened, so there is no gap");

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
        trust_consumer.next_batch().await.unwrap();
        full_coverage_consumer.next_batch().await.unwrap();
        trust_backlog_consumer.next_batch().await.unwrap();

        let a = trust_consumer.next_batch().await.unwrap();
        let b = full_coverage_consumer.next_batch().await.unwrap();
        let c = trust_backlog_consumer.next_batch().await.unwrap();

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
}
