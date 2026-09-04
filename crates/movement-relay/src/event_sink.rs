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
// See docs/superpowers/specs/2026-09-04-movement-relay-design.md Decision
// 2's sizing rationale: ~630k entries/day of real, cited average volume,
// N=500,000 chosen as ~19h of full-volume headroom -- a starting figure,
// not empirically tuned, same posture as this repo's other first-guess
// cadence constants.
const MAXLEN: usize = 500_000;

#[async_trait]
pub trait EventSink: Send {
    /// XADDs one surviving envelope. `msg_type` is the redundant
    /// introspection field (Decision 2's field-layout choice);
    /// `payload` is the envelope's own raw JSON bytes, unchanged.
    async fn publish(&mut self, msg_type: &str, payload: &str) -> anyhow::Result<()>;
}

pub struct RedisEventSink {
    conn: ConnectionManager,
}

impl RedisEventSink {
    pub async fn connect(redis_url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            conn: client.get_connection_manager().await?,
        })
    }
}

#[async_trait]
impl EventSink for RedisEventSink {
    async fn publish(&mut self, msg_type: &str, payload: &str) -> anyhow::Result<()> {
        let _: String = redis::cmd("XADD")
            .arg(STREAM)
            .arg("MAXLEN")
            .arg("~")
            .arg(MAXLEN)
            .arg("*")
            .arg("payload")
            .arg(payload)
            .arg("msg_type")
            .arg(msg_type)
            .query_async(&mut self.conn)
            .await?;
        Ok(())
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
