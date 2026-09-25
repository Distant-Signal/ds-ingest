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
use health_http::ConnectionState;
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

    let ready: ConnectionState = Arc::new(AtomicBool::new(false));
    health_http::spawn_with_state(
        config.health_bind_url.clone(),
        Arc::clone(&ready),
        "partitions assigned",
        "no confirmed partition assignment",
    );

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
///
/// "Doesn't commit" is not on its own enough to keep a record: a bare
/// `Cycle::Failed` used to leave the record behind entirely, because the
/// next cycle's `consumer.recv()` advances librdkafka's fetch position and
/// overwrites `last_received`, so the next SUCCESSFUL commit stored an
/// offset PAST the record whose XADD had failed. Every failure path that
/// happens after a batch was received therefore hands that batch back via
/// `RawKafkaSource::retain_for_retry`, which makes the next `next_batch`
/// re-deliver exactly it before fetching anything new. The one deliberate
/// exception is a classification failure -- see its own comment below.
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
        BatchOutcome::Unclassifiable => {
            // Deliberately NOT retained. A record that
            // `confirmed_envelope_bodies` rejects is unprocessable by
            // construction -- the bytes are fixed, so every retry fails
            // identically and can never produce an envelope to publish.
            // Retaining it would wedge the sole movement feed forever on a
            // single malformed record (a far larger outage than the record
            // itself). It is logged with its raw payload and counted under
            // `operation = "classify_record"` above, and the next cycle
            // moves on past it.
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
    /// The record could not be classified at all: permanent for these bytes.
    Unclassifiable,
}

async fn publish_batch<K>(sink: &mut K, batch: &[String]) -> BatchOutcome
where
    K: EventSink,
{
    for raw in batch {
        let envelopes = match trust_schema::schema::confirmed_envelope_bodies(raw) {
            Ok(envelopes) => envelopes,
            Err(err) => {
                tracing::error!(error = ?err, raw = %raw, "failed to classify Kafka record; not committing this record's offset");
                metrics::counter!(
                    common::metrics::metric_name("movement_relay_errors_total"),
                    "operation" => "classify_record"
                )
                .increment(1);
                return BatchOutcome::Unclassifiable;
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
        metrics::counter!(
            common::metrics::metric_name("movement_relay_errors_total"),
            "operation" => "redis_client_build"
        )
        .increment(1);
        return;
    };
    let mut conn = match client.get_connection_manager().await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::error!(error = ?err, "stream_lag_loop: failed to connect; lag gauge disabled");
            metrics::counter!(
                common::metrics::metric_name("movement_relay_errors_total"),
                "operation" => "redis_connect"
            )
            .increment(1);
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
            vec![r#"{"not_an_envelope": true}"#.to_string()],
            vec![movement_record("BBB")],
        ]);
        let mut sink = FakeEventSink::default();

        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Failed);
        assert_eq!(source.committed_count, 0);

        assert_eq!(run_cycle(&mut source, &mut sink).await, Cycle::Committed);
        assert_eq!(
            published_train_ids(&sink.published),
            vec!["BBB".to_string()],
            "the poison record must not be re-delivered forever"
        );
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
