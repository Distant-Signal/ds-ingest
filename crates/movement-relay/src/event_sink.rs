//! `EventSink`: the one thing `movement-relay`'s main loop needs from
//! Redis -- XADD-ing surviving envelopes into `movement-events`. Kept as
//! a trait (not inlined into main.rs) so tests can substitute a
//! `FakeEventSink` -- this repo's established "no wiremock, use a fake
//! trait impl" convention (see e.g.
//! crates/trust-consumer/src/feed/mod.rs's now-shared `FakeMovementFeed`),
//! applied here on the producer side for the first time in this codebase.

use async_trait::async_trait;
use common::redis_conn::RedisConn;

const STREAM: &str = "movement-events";

/// Every consumer group that reads `movement-events` -- the default for
/// `--movement-consumer-groups` (the chart passes only the groups whose
/// consumer actually reads the stream).
pub(crate) const DEFAULT_CONSUMER_GROUPS: [&str; 3] = [
    "trust-consumer",
    "full-coverage-consumer",
    "trust-event-backlog",
];

/// Counter of `movement-events` streams this relay found missing while
/// running and recreated, together with every consumer group (see
/// [`RedisEventSink::publish`]). Registered at 0 on connect, so the chart's
/// `DistantSignalMovementGroupRecreated` alert can use a plain `increase()`.
const STREAM_CREATED_METRIC: &str = "movement_relay_stream_created_total";

#[async_trait]
pub(crate) trait EventSink: Send {
    /// XADDs one surviving envelope. `msg_type` is the redundant
    /// introspection field (Decision 2's field-layout choice);
    /// `payload` is the envelope's own raw JSON bytes, unchanged.
    ///
    /// `msg_type` is kept on purpose. Nothing reads it (every consumer
    /// reads only `payload`, see `movement_feed::redis_stream::
    /// split_deliverable_and_malformed`), but measured on 100k real 0003
    /// envelopes it costs nothing: the stream used 82.71 MB with it and
    /// 82.72 MB without (~827 B/entry either way; the listpack stores the
    /// field name once per node and the 4-byte value disappears in node
    /// packing). Dropping it would save no memory and lose the
    /// `XRANGE`-friendly type column.
    async fn publish(&mut self, msg_type: &str, payload: &str) -> anyhow::Result<()>;
}

/// The `XADD` this sink issues, split out so its arguments are testable
/// without a Redis. `MAXLEN ~` (approximate) trims whole stream nodes
/// from the head, oldest first -- Decision 2's deliberate eviction
/// direction -- at no per-write O(removed) cost.
///
/// `NOMKSTREAM`: an `XADD` never creates the stream. When it is missing
/// (Redis lost its data, or a fresh install) the reply is nil and
/// [`RedisEventSink::publish`] creates it together with every consumer
/// group first; see [`create_groups`].
///
/// `create_stream` drops `NOMKSTREAM`: only the retry right after
/// [`create_groups`] uses it, so an empty group list still gets its stream.
fn xadd_cmd(
    stream: &str,
    maxlen: u64,
    create_stream: bool,
    msg_type: &str,
    payload: &str,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(stream);
    if !create_stream {
        cmd.arg("NOMKSTREAM");
    }
    cmd.arg("MAXLEN")
        .arg("~")
        .arg(maxlen)
        .arg("*")
        .arg("payload")
        .arg(payload)
        .arg("msg_type")
        .arg(msg_type);
    cmd
}

/// The `XTRIM` issued after an `OOM` rejection. Redis flags `XADD` as
/// `denyoom` but not `XTRIM`, so this still runs once `used_memory` is
/// past `maxmemory`.
fn xtrim_cmd(stream: &str, maxlen: u64) -> redis::Cmd {
    let mut cmd = redis::cmd("XTRIM");
    cmd.arg(stream).arg("MAXLEN").arg("~").arg(maxlen);
    cmd
}

/// Whether a Redis error is `maxmemory` rejecting a write (`-OOM command
/// not allowed when used memory > 'maxmemory'`).
fn is_oom(err: &redis::RedisError) -> bool {
    err.code() == Some("OOM")
}

/// `XGROUP CREATE <stream> <group> 0 MKSTREAM` for every group, so each one
/// reads the stream from its very first entry. `BUSYGROUP` (the group
/// already exists) is success: an existing group keeps its position.
/// Returns how many groups were newly created.
///
/// **Why the relay does this (D2, 2026-09-30).** A consumer creates its
/// own group at startup at `$`, the stream's tail
/// (`movement_feed::redis_stream`). After Redis loses its data, the relay's
/// next `XADD` used to recreate the stream with no groups at all; a
/// consumer (re)started after that created its group at `$` and silently
/// skipped everything the relay had published in between -- after an
/// outage, a Kafka backlog drained as fast as Redis would take it. With
/// the groups created at `0` before the first entry, no entry predates
/// them.
pub(crate) async fn create_groups<C: redis::aio::ConnectionLike + Send>(
    conn: &mut C,
    stream: &str,
    groups: &[String],
) -> anyhow::Result<usize> {
    let mut created = 0;
    for group in groups {
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(conn)
            .await;
        match result {
            Ok(()) => created += 1,
            Err(err) if err.code() == Some("BUSYGROUP") => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(created)
}

pub(crate) struct RedisEventSink {
    conn: RedisConn,
    stream: String,
    maxlen: u64,
    groups: Vec<String>,
}

impl RedisEventSink {
    /// Connects to Redis, retrying on `backoff` until it is reachable
    /// (INF-5: an unreachable Redis at startup -- e.g. its pod being
    /// recreated by the same rollout -- is waited for, not exited on).
    /// Every failed attempt beats `progress`, so `/livez` stays 200 while
    /// this waits. Only an unparseable `redis_url` is an error.
    ///
    /// `groups` are the consumer groups created at `0` whenever the stream
    /// is fresh (see [`create_groups`] and [`Self::prepare_stream`]).
    pub(crate) async fn connect_until_ready(
        redis_url: &str,
        maxlen: u64,
        groups: Vec<String>,
        backoff: common::backoff::Backoff,
        progress: &health_http::Progress,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        // Bounded attempts (`common::redis_conn`): a failed attempt is
        // logged and beats `progress` instead of redis-rs retrying for
        // minutes, unlogged. Later reconnects ride on `main::run_cycle`'s
        // `ERROR_BACKOFF` (each failed XADD starts one background
        // reconnect, which the next cycle's XADD awaits; a timed-out one
        // -- possibly a half-open connection -- makes the next XADD open a
        // new connection, see `common::redis_conn::RedisConn`).
        let conn =
            common::redis_conn::connect_until_ready("Redis", &client, backoff, Some(progress))
                .await;
        metrics::counter!(common::metrics::metric_name(STREAM_CREATED_METRIC)).increment(0);
        Ok(Self::new(conn, STREAM, maxlen, groups))
    }

    fn new(conn: RedisConn, stream: &str, maxlen: u64, groups: Vec<String>) -> Self {
        let groups = groups.into_iter().filter(|g| !g.is_empty()).collect();
        Self {
            conn,
            stream: stream.to_string(),
            maxlen,
            groups,
        }
    }

    /// Startup step, before the first publish: when the stream is missing
    /// or empty, create every consumer group at `0` (a consumer that
    /// started first may have created the stream with only its own
    /// group). A stream that already holds entries is left alone: a group
    /// missing from it is a consumer that has not been deployed yet, which
    /// must start at the tail, not replay the whole stream.
    ///
    /// Not counted as a recreation: a fresh install looks the same.
    pub(crate) async fn prepare_stream(&mut self) -> anyhow::Result<()> {
        let len: u64 = redis::cmd("XLEN")
            .arg(&self.stream)
            .query_async(&mut self.conn)
            .await?;
        if len > 0 {
            return Ok(());
        }
        let created = create_groups(&mut self.conn, &self.stream, &self.groups).await?;
        if created > 0 {
            tracing::info!(
                stream = %self.stream,
                created,
                groups = ?self.groups,
                "movement-events is empty; created the missing consumer groups at its start"
            );
        }
        Ok(())
    }

    /// One `XADD`: `Ok(None)` when the stream does not exist
    /// (`NOMKSTREAM`). See [`EventSink::publish`] for the `OOM` path.
    async fn xadd(
        &mut self,
        create_stream: bool,
        msg_type: &str,
        payload: &str,
    ) -> anyhow::Result<Option<String>> {
        let result: redis::RedisResult<Option<String>> =
            xadd_cmd(&self.stream, self.maxlen, create_stream, msg_type, payload)
                .query_async(&mut self.conn)
                .await;
        match result {
            Ok(id) => Ok(id),
            Err(err) if is_oom(&err) => {
                let trimmed: redis::RedisResult<i64> = xtrim_cmd(&self.stream, self.maxlen)
                    .query_async(&mut self.conn)
                    .await;
                tracing::warn!(
                    maxlen = self.maxlen,
                    trimmed = ?trimmed,
                    "Redis rejected XADD at maxmemory; trimmed movement-events to its cap, \
                     holding the Kafka record for retry"
                );
                metrics::counter!(
                    common::metrics::metric_name("movement_relay_errors_total"),
                    "operation" => "redis_oom"
                )
                .increment(1);
                Err(err.into())
            }
            Err(err) => Err(err.into()),
        }
    }
}

#[async_trait]
impl EventSink for RedisEventSink {
    /// **Backpressure, not a crash**: when Redis is at `maxmemory` (chart:
    /// `redis.maxmemory`, `noeviction`) this `XADD` fails with `OOM`, and
    /// the error goes back to `main::run_cycle` like any other failed
    /// publish: the Kafka record is retained, partitions are paused, the
    /// offset is not committed, and the cycle is retried every
    /// `ERROR_BACKOFF`. Kafka holds the backlog; nothing is dropped and the
    /// process does not exit.
    ///
    /// `XADD ... MAXLEN ~` only trims when the `XADD` itself is accepted,
    /// so a stream that is over `maxmemory` at its cap could never shrink
    /// on its own. The explicit `XTRIM` here is what lets an operator
    /// recover by LOWERING `--movement-stream-maxlen`: the next rejected
    /// write trims the stream to the new cap, memory frees, and the retry
    /// succeeds. At an unchanged cap it is a cheap no-op.
    ///
    /// **A missing stream** (Redis lost its data while this relay was
    /// running): the stream is recreated with every consumer group at `0`
    /// before the entry is added, so no consumer that (re)starts later can
    /// skip it (see [`create_groups`]). Logged, and counted as
    /// `distant_signal_movement_relay_stream_created_total`.
    async fn publish(&mut self, msg_type: &str, payload: &str) -> anyhow::Result<()> {
        if self.xadd(false, msg_type, payload).await?.is_some() {
            return Ok(());
        }
        let created = create_groups(&mut self.conn, &self.stream, &self.groups).await?;
        tracing::warn!(
            stream = %self.stream,
            created,
            groups = ?self.groups,
            "movement-events is missing (Redis lost its data?); recreated it with every \
             consumer group at its start"
        );
        metrics::counter!(common::metrics::metric_name(STREAM_CREATED_METRIC)).increment(1);
        self.xadd(true, msg_type, payload).await?;
        Ok(())
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct FakeEventSink {
    pub published: Vec<(String, String)>,
    pub fail_next: bool,
}

#[cfg(test)]
#[async_trait]
impl EventSink for FakeEventSink {
    async fn publish(&mut self, msg_type: &str, payload: &str) -> anyhow::Result<()> {
        if self.fail_next {
            self.fail_next = false;
            return Err(anyhow::anyhow!("simulated publish failure"));
        }
        self.published
            .push((msg_type.to_string(), payload.to_string()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A local port with nothing listening on it (bound, then released).
    fn closed_local_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// `GET path` against the health listener, as `(status, body)`. Retries
    /// the connect briefly, since the listener binds in a spawned task.
    async fn get(port: u16, path: &str) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = None;
        for _ in 0..100 {
            if let Ok(s) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
                stream = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut stream = stream.expect("health listener never came up");
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        let status = response
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("malformed response: {response:?}"));
        let body = response
            .split_once("\r\n\r\n")
            .map_or("", |(_, body)| body)
            .to_string();
        (status, body)
    }

    /// The production incident: the relay started while the Redis pod was
    /// being recreated, sat in its initial Redis connect, and the liveness
    /// probe (then on `/healthz`) killed it. While Redis is unreachable,
    /// `/livez` must stay 200 -- well past the stall window -- and only
    /// readiness (`/healthz`) may be 503.
    #[tokio::test]
    async fn livez_stays_ok_while_redis_is_unreachable() {
        let health_port = closed_local_port();
        let redis_url = format!("redis://127.0.0.1:{}", closed_local_port());
        let stall_after = Duration::from_secs(1);
        let (_ready, progress) = health_http::spawn_with_progress(
            format!("127.0.0.1:{health_port}"),
            "partitions assigned",
            "no confirmed partition assignment",
            stall_after,
        );

        let connecting = tokio::spawn(async move {
            RedisEventSink::connect_until_ready(
                &redis_url,
                1_000,
                Vec::new(),
                common::backoff::Backoff::new(
                    Duration::from_millis(50),
                    Duration::from_millis(200),
                ),
                &progress,
            )
            .await
        });

        // 4x the stall window.
        for _ in 0..16 {
            assert_eq!(
                get(health_port, "/livez").await,
                (200, "alive".to_string()),
                "liveness must not depend on Redis"
            );
            assert_eq!(get(health_port, "/healthz").await.0, 503, "not ready");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(
            !connecting.is_finished(),
            "an unreachable Redis is waited for, not exited on"
        );

        // Control: the same window with nothing beating IS a stall, so the
        // loop above really was kept alive by the connect retries.
        connecting.abort();
        tokio::time::sleep(stall_after * 2).await;
        assert_eq!(
            get(health_port, "/livez").await,
            (503, "stalled".to_string())
        );
    }

    fn args(cmd: &redis::Cmd) -> Vec<String> {
        cmd.args_iter()
            .map(|arg| match arg {
                redis::Arg::Simple(bytes) => String::from_utf8(bytes.to_vec()).unwrap(),
                redis::Arg::Cursor => "<cursor>".to_string(),
            })
            .collect()
    }

    #[test]
    fn xadd_uses_the_configured_approximate_maxlen_and_both_fields() {
        let cmd = xadd_cmd(STREAM, 524_288, false, "0003", r#"{"header":{}}"#);
        assert_eq!(
            args(&cmd),
            [
                "XADD",
                "movement-events",
                "NOMKSTREAM",
                "MAXLEN",
                "~",
                "524288",
                "*",
                "payload",
                r#"{"header":{}}"#,
                "msg_type",
                "0003",
            ]
        );
    }

    #[test]
    fn xadd_carries_a_non_default_cap_through_verbatim() {
        let cmd = xadd_cmd(STREAM, 250_000, false, "0001", "{}");
        assert_eq!(args(&cmd)[5], "250000");
    }

    /// Only the retry right after the groups were created may create the
    /// stream itself.
    #[test]
    fn only_the_retry_after_creating_the_groups_may_create_the_stream() {
        assert!(
            !args(&xadd_cmd(STREAM, 1_000, true, "0001", "{}")).contains(&"NOMKSTREAM".to_string())
        );
    }

    #[test]
    fn xtrim_after_oom_uses_the_same_cap() {
        assert_eq!(
            args(&xtrim_cmd(STREAM, 250_000)),
            ["XTRIM", "movement-events", "MAXLEN", "~", "250000"]
        );
    }

    #[test]
    fn only_an_oom_reply_counts_as_oom() {
        // Parsed from real RESP error lines, so this exercises the same
        // code path a live `-OOM` reply takes through redis-rs.
        let parse = |line: &[u8]| match redis::parse_redis_value(line).unwrap() {
            redis::Value::ServerError(err) => redis::RedisError::from(err),
            other => panic!("expected a server error, got {other:?}"),
        };
        let oom = parse(b"-OOM command not allowed when used memory > 'maxmemory'.\r\n");
        let other = parse(b"-ERR something else\r\n");
        assert!(is_oom(&oom));
        assert!(!is_oom(&other));
    }

    /// `#[ignore]`d tests against a real Redis/valkey (`REDIS_URL`, default
    /// the local one), each on its own uniquely named stream:
    /// `cargo test -p movement-relay -- --ignored --test-threads=1`.
    mod redis_tests {
        use super::super::*;

        fn redis_url() -> String {
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
        }

        fn unique_stream(name: &str) -> String {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("relay-test-{name}-{nanos}")
        }

        async fn conn() -> RedisConn {
            let client = redis::Client::open(redis_url()).unwrap();
            common::redis_conn::connect(&client).await.unwrap()
        }

        fn groups() -> Vec<String> {
            ["g-a", "g-b", "g-c"].map(String::from).to_vec()
        }

        async fn sink(stream: &str) -> RedisEventSink {
            RedisEventSink::new(conn().await, stream, 1_000, groups())
        }

        /// What `XREADGROUP ... >` hands `group` now, as payloads.
        async fn read_new(stream: &str, group: &str) -> Vec<String> {
            let mut conn = conn().await;
            let reply: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(group)
                .arg("test-consumer")
                .arg("STREAMS")
                .arg(stream)
                .arg(">")
                .query_async(&mut conn)
                .await
                .unwrap();
            reply
                .keys
                .into_iter()
                .flat_map(|k| k.ids)
                .map(|id| redis::from_redis_value::<String>(&id.map["payload"]).unwrap())
                .collect()
        }

        async fn group_names(stream: &str) -> Vec<String> {
            let mut conn = conn().await;
            let reply: redis::streams::StreamInfoGroupsReply = redis::cmd("XINFO")
                .arg("GROUPS")
                .arg(stream)
                .query_async(&mut conn)
                .await
                .unwrap();
            let mut names: Vec<String> = reply.groups.into_iter().map(|g| g.name).collect();
            names.sort();
            names
        }

        async fn xgroup_create(stream: &str, group: &str, start: &str) {
            let mut conn = conn().await;
            let _: () = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream)
                .arg(group)
                .arg(start)
                .arg("MKSTREAM")
                .query_async(&mut conn)
                .await
                .unwrap();
        }

        async fn cleanup(stream: &str) {
            let mut conn = conn().await;
            let _: i64 = redis::cmd("DEL")
                .arg(stream)
                .query_async(&mut conn)
                .await
                .unwrap();
        }

        /// D2: Redis lost its data while the relay ran. The next publish
        /// recreates the stream with every group at `0`, so the entry (and
        /// everything after it) reaches every group, however late its
        /// consumer (re)starts.
        #[tokio::test]
        #[ignore = "requires a live Redis/valkey at REDIS_URL"]
        async fn publishing_to_a_missing_stream_creates_every_group_at_its_start() {
            let stream = unique_stream("missing");
            let mut sink = sink(&stream).await;

            sink.publish("0003", "first").await.unwrap();
            sink.publish("0003", "second").await.unwrap();

            assert_eq!(group_names(&stream).await, groups());
            for group in groups() {
                assert_eq!(
                    read_new(&stream, &group).await,
                    ["first", "second"],
                    "{group}"
                );
            }
            cleanup(&stream).await;
        }

        /// A consumer that started first created the (empty) stream with
        /// only its own group. The relay's startup step adds the others at
        /// `0` and leaves the existing one where it was.
        #[tokio::test]
        #[ignore = "requires a live Redis/valkey at REDIS_URL"]
        async fn prepare_adds_the_missing_groups_to_an_empty_stream() {
            let stream = unique_stream("empty");
            xgroup_create(&stream, "g-a", "$").await;
            let mut sink = sink(&stream).await;

            sink.prepare_stream().await.unwrap();
            sink.publish("0003", "first").await.unwrap();

            assert_eq!(group_names(&stream).await, groups());
            for group in groups() {
                assert_eq!(read_new(&stream, &group).await, ["first"], "{group}");
            }
            cleanup(&stream).await;
        }

        /// A stream that already holds entries is left alone: a group
        /// missing from it belongs to a consumer not deployed yet, which
        /// must not replay the whole stream.
        #[tokio::test]
        #[ignore = "requires a live Redis/valkey at REDIS_URL"]
        async fn prepare_leaves_a_populated_stream_alone() {
            let stream = unique_stream("populated");
            xgroup_create(&stream, "g-a", "$").await;
            let mut sink = sink(&stream).await;
            sink.publish("0003", "already-there").await.unwrap();

            sink.prepare_stream().await.unwrap();

            assert_eq!(group_names(&stream).await, ["g-a"]);
            cleanup(&stream).await;
        }

        /// `BUSYGROUP` is success, and an existing group keeps its
        /// position (it is not moved back to `0`).
        #[tokio::test]
        #[ignore = "requires a live Redis/valkey at REDIS_URL"]
        async fn creating_groups_again_keeps_existing_positions() {
            let stream = unique_stream("busy");
            let mut sink = sink(&stream).await;
            sink.publish("0003", "first").await.unwrap();
            assert_eq!(read_new(&stream, "g-a").await, ["first"]);

            let mut conn = conn().await;
            let created = create_groups(&mut conn, &stream, &groups()).await.unwrap();

            assert_eq!(created, 0);
            assert!(
                read_new(&stream, "g-a").await.is_empty(),
                "g-a must not be rewound"
            );
            cleanup(&stream).await;
        }

        /// With no groups configured the stream is still created.
        #[tokio::test]
        #[ignore = "requires a live Redis/valkey at REDIS_URL"]
        async fn a_missing_stream_is_created_even_with_no_groups() {
            let stream = unique_stream("nogroups");
            let mut sink = RedisEventSink::new(conn().await, &stream, 1_000, vec![String::new()]);

            sink.publish("0003", "first").await.unwrap();

            let mut conn = conn().await;
            let len: u64 = redis::cmd("XLEN")
                .arg(&stream)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(len, 1);
            cleanup(&stream).await;
        }
    }
}
