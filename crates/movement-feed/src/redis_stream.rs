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

use crate::MovementFeed;

const STREAM: &str = "movement-events";

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
}

impl RedisStreamMovementFeed {
    /// `group` is one of the two fixed literals (`"trust-consumer"` /
    /// `"full-coverage-consumer"`) -- see each crate's own `main.rs` call
    /// site (Task 4). `consumer` is a fixed per-deployment name (e.g.
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
        })
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
                    .count(100)
                    .block(if self.replaying_pel { 0 } else { 5000 }),
            )
            .await?;

        let entries: Vec<(String, String)> = reply
            .keys
            .into_iter()
            .flat_map(|k| k.ids)
            .filter_map(|entry| {
                let payload: String = entry
                    .map
                    .get("payload")
                    .and_then(|v| redis::from_redis_value(v).ok())?;
                Some((entry.id, payload))
            })
            .collect();

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
                    redis::streams::StreamAutoClaimOptions::default().count(100),
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
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(stream).query_async(&mut conn).await;
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
}
