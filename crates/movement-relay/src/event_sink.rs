//! `EventSink`: the one thing `movement-relay`'s main loop needs from
//! Redis -- XADD-ing surviving envelopes into `movement-events`. Kept as
//! a trait (not inlined into main.rs) so tests can substitute a
//! `FakeEventSink` -- this repo's established "no wiremock, use a fake
//! trait impl" convention (see e.g.
//! crates/trust-consumer/src/feed/mod.rs's now-shared `FakeMovementFeed`),
//! applied here on the producer side for the first time in this codebase.

use async_trait::async_trait;
use redis::aio::ConnectionManager;

const STREAM: &str = "movement-events";

#[async_trait]
pub trait EventSink: Send {
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
fn xadd_cmd(maxlen: u64, msg_type: &str, payload: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(STREAM)
        .arg("MAXLEN")
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
fn xtrim_cmd(maxlen: u64) -> redis::Cmd {
    let mut cmd = redis::cmd("XTRIM");
    cmd.arg(STREAM).arg("MAXLEN").arg("~").arg(maxlen);
    cmd
}

/// Whether a Redis error is `maxmemory` rejecting a write (`-OOM command
/// not allowed when used memory > 'maxmemory'`).
fn is_oom(err: &redis::RedisError) -> bool {
    err.code() == Some("OOM")
}

pub struct RedisEventSink {
    conn: ConnectionManager,
    maxlen: u64,
}

impl RedisEventSink {
    pub async fn connect(redis_url: &str, maxlen: u64) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            conn: client.get_connection_manager().await?,
            maxlen,
        })
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
    async fn publish(&mut self, msg_type: &str, payload: &str) -> anyhow::Result<()> {
        let result: redis::RedisResult<String> = xadd_cmd(self.maxlen, msg_type, payload)
            .query_async(&mut self.conn)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(err) if is_oom(&err) => {
                let trimmed: redis::RedisResult<i64> =
                    xtrim_cmd(self.maxlen).query_async(&mut self.conn).await;
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

#[cfg(test)]
#[derive(Default)]
pub struct FakeEventSink {
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
    use super::*;

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
        let cmd = xadd_cmd(524_288, "0003", r#"{"header":{}}"#);
        assert_eq!(
            args(&cmd),
            [
                "XADD",
                "movement-events",
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
        let cmd = xadd_cmd(250_000, "0001", "{}");
        assert_eq!(args(&cmd)[4], "250000");
    }

    #[test]
    fn xtrim_after_oom_uses_the_same_cap() {
        assert_eq!(
            args(&xtrim_cmd(250_000)),
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
}
