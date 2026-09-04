//! `movement-relay`: the sole real Kafka client against RDM's Train
//! Movements product from Deploy B onward, fanning out into the
//! `movement-events` Redis Stream both `trust-consumer` and
//! `full-coverage-consumer` read from. See
//! docs/superpowers/specs/2026-09-04-movement-relay-design.md and
//! docs/superpowers/plans/2026-09-04-movement-relay-plan.md.

mod config;
mod event_sink;
mod health;
mod kafka_source;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
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

    let ready: health::ReadyState = Arc::new(AtomicBool::new(false));
    health::spawn(config.health_bind_url.clone(), Arc::clone(&ready));

    let mut source = KafkaRawSource::connect(&config, ready)?;
    let mut sink = RedisEventSink::connect(&config.redis_url).await?;

    tokio::spawn(stream_lag_loop(
        config.redis_url.clone(),
        Duration::from_secs(config.stream_lag_poll_secs),
    ));

    loop {
        match run_cycle(&mut source, &mut sink).await {
            Cycle::Committed => {}
            Cycle::Failed => tokio::time::sleep(ERROR_BACKOFF).await,
        }
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
async fn run_cycle<S, K>(source: &mut S, sink: &mut K) -> Cycle
where
    S: RawKafkaSource,
    K: EventSink,
{
    let batch = match source.next_batch().await {
        Ok(batch) => batch,
        Err(err) => {
            tracing::error!(error = ?err, "error receiving from Kafka");
            return Cycle::Failed;
        }
    };

    for raw in &batch {
        let envelopes = match trust_schema::schema::confirmed_envelope_bodies(raw) {
            Ok(envelopes) => envelopes,
            Err(err) => {
                tracing::error!(error = ?err, raw = %raw, "failed to classify Kafka record; not committing this record's offset");
                return Cycle::Failed;
            }
        };
        for (msg_type, payload) in &envelopes {
            if let Err(err) = sink.publish(msg_type, payload).await {
                tracing::error!(error = ?err, msg_type, "failed to XADD envelope; not committing this record's offset");
                return Cycle::Failed;
            }
            metrics::counter!(
                common::metrics::metric_name("movement_relay_events_published_total"),
                "msg_type" => msg_type.clone()
            )
            .increment(1);
        }
    }

    if let Err(err) = source.commit().await {
        tracing::error!(error = ?err, "failed to commit Kafka offset");
        return Cycle::Failed;
    }
    Cycle::Committed
}

/// Leading-indicator lag gauge (design doc Decision 2) -- polls `XINFO
/// GROUPS movement-events` for both downstream groups on its own timer,
/// independent of the main consume loop. Reuses the same `XINFO GROUPS`
/// field-walk shape `movement_feed::redis_stream::check_gap`'s own
/// `find_group_field` helper uses -- NOT re-exported from that crate
/// (`movement-relay` deliberately doesn't depend on `movement-feed`, Task
/// 6's own note) -- a small, independent copy here instead.
async fn stream_lag_loop(redis_url: String, interval: Duration) {
    let Ok(client) = redis::Client::open(redis_url) else {
        tracing::error!("stream_lag_loop: failed to build Redis client; lag gauge disabled");
        return;
    };
    let mut conn = match client.get_connection_manager().await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::error!(error = ?err, "stream_lag_loop: failed to connect; lag gauge disabled");
            return;
        }
    };
    loop {
        tokio::time::sleep(interval).await;
        for group in ["trust-consumer", "full-coverage-consumer"] {
            match group_lag(&mut conn, group).await {
                Ok(Some(lag)) => {
                    metrics::gauge!(
                        common::metrics::metric_name("movement_relay_stream_lag"),
                        "group" => group
                    )
                    .set(lag as f64);
                }
                Ok(None) => {} // group doesn't exist yet -- nothing to report.
                Err(err) => {
                    tracing::warn!(error = ?err, group, "stream_lag_loop: failed to fetch XINFO GROUPS");
                }
            }
        }
    }
}

/// `XINFO GROUPS movement-events`'s `lag` field for one named group --
/// same reply-walk shape as `crates/enricher/src/stream.rs::group_lag`,
/// generalized over group name (this function serves two group names from
/// one binary; enricher's own copy only ever serves one, `"enricher"`).
async fn group_lag(
    conn: &mut redis::aio::ConnectionManager,
    group: &str,
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
        let mut lag: Option<i64> = None;
        let mut it = fields.into_iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            let k: String = redis::from_redis_value(&k)?;
            match k.as_str() {
                "name" => name = redis::from_redis_value(&v).ok(),
                "lag" => lag = redis::from_redis_value(&v).ok(),
                _ => {}
            }
        }
        if name.as_deref() == Some(group) {
            return Ok(lag);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_sink::FakeEventSink;
    use crate::kafka_source::FakeRawSource;

    const CONFIRMED_AND_UNKNOWN: &str = r#"[
        {"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
            "loc_stanox":"87701","variation_status":"LATE"
        }},
        {"header":{"msg_type":"0005"},"body":{"anything":"goes"}}
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

    #[tokio::test]
    async fn an_unclassifiable_record_does_not_commit() {
        let mut source = FakeRawSource::new(vec![vec![r#"{"not_an_envelope": true}"#.to_string()]]);
        let mut sink = FakeEventSink::default();

        let outcome = run_cycle(&mut source, &mut sink).await;

        assert_eq!(outcome, Cycle::Failed);
        assert_eq!(source.committed_count, 0);
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

    #[tokio::test]
    async fn an_empty_poll_commits_nothing() {
        let mut source = FakeRawSource::new(vec![vec![]]);
        let mut sink = FakeEventSink::default();

        let outcome = run_cycle(&mut source, &mut sink).await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(source.committed_count, 0);
    }
}
