//! Where each batch goes (ingest architecture plan 3b.1, spec R1):
//! [`DbSink`], straight into Postgres as the `trust_backlog` role.
//!
//! | | [`DbSink`] |
//! |---|---|
//! | backlog events | `ds_store::backlog::ingest_trust_event_backlog` |
//! | reason codes | `ds_store::backlog::reasons::upsert_reasons` |
//! | STANOX/CRS reload | `ds_store::reference::list_stanox_crs` |
//!
//! The api's retired `POST /private/trust-event-backlog` and
//! `/private/train-reasons` (and `GET /private/stanox-crs`) called exactly
//! those functions; the HTTP sink went in release B (ingest phase 5,
//! docs/ingest-phase5-runbook.md). The batch is `XACKed` only after the
//! write committed, and a transient failure leaves it pending and backs off
//! before the next read (`main.rs`'s `deliver_batch` and `delivery_wait`).

use common::{
    StanoxCrsRecord, TrainReasonMessage, TrustBacklogEventMessage, TrustBacklogIngestResponse,
};
use sqlx::PgPool;

/// What a sink's failures count as: `trust_backlog_consumer_errors_total`
/// operations (each in `API_CALL_OPERATIONS`, so the chart's
/// `DistantSignalConsumerApiCallsFailing` sums it) and the dead-letter
/// reason of a row the write refused for a data error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Operations {
    /// A failed backlog write: transient, the batch stays pending.
    pub write: &'static str,
    /// A failed reasons write: best effort, the batch goes on.
    pub write_reasons: &'static str,
    /// The `DeadLetter::reason` of a rejected row.
    pub rejected: &'static str,
}

/// [`DbSink`]'s names (plan 3b.2).
pub(crate) const DB_OPERATIONS: Operations = Operations {
    write: "db_write",
    write_reasons: "db_write_reasons",
    rejected: "rejected_by_db",
};

/// One destination for the consumer's writes and its STANOX/CRS reload.
pub(crate) trait BacklogSink {
    /// See [`Operations`].
    const OPERATIONS: Operations;

    /// The live STANOX/CRS table.
    async fn stanox_crs(&self) -> anyhow::Result<Vec<StanoxCrsRecord>>;

    /// Files the batch's reason codes. An empty batch writes nothing.
    async fn write_reasons(&self, reasons: &[TrainReasonMessage]) -> anyhow::Result<()>;

    /// Writes one batch of backlog events: `Ok` with the rows refused for
    /// a data error (every other row landed), or `Err` for anything
    /// transient (nothing to `XACK`). An empty batch writes nothing.
    async fn write_backlog(
        &self,
        events: &[TrustBacklogEventMessage],
    ) -> anyhow::Result<TrustBacklogIngestResponse>;
}

/// [`DbSink`]'s `db_writes_total{operation}` (`ds_store::writes`) for a
/// backlog batch: `ok` once committed (rows refused for a data error
/// included: the rest landed), else its failure class.
pub(crate) const DB_WRITE_BACKLOG: &str = "trust_event_backlog";
/// As [`DB_WRITE_BACKLOG`], for a batch's reason codes.
pub(crate) const DB_WRITE_REASONS: &str = "train_reasons";

/// Registers both operations' `db_writes_total` series at 0.
pub(crate) fn register_db_write_metrics() {
    ds_store::writes::register(&[DB_WRITE_BACKLOG, DB_WRITE_REASONS]);
}

/// Runs `write`, recording it as one `operation` write.
async fn recorded<T>(
    operation: &'static str,
    write: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    let started = std::time::Instant::now();
    let result = write.await;
    ds_store::writes::record(
        operation,
        result.as_ref().err().map(ds_store::writes::classify),
        started.elapsed(),
    );
    result
}

/// Straight into Postgres, as the `trust_backlog` role.
pub(crate) struct DbSink {
    pub pool: PgPool,
}

impl BacklogSink for DbSink {
    const OPERATIONS: Operations = DB_OPERATIONS;

    async fn stanox_crs(&self) -> anyhow::Result<Vec<StanoxCrsRecord>> {
        ds_store::reference::list_stanox_crs(&self.pool).await
    }

    async fn write_reasons(&self, reasons: &[TrainReasonMessage]) -> anyhow::Result<()> {
        if reasons.is_empty() {
            return Ok(());
        }
        recorded(DB_WRITE_REASONS, async {
            ds_store::backlog::reasons::upsert_reasons(&self.pool, reasons).await?;
            Ok(())
        })
        .await
    }

    async fn write_backlog(
        &self,
        events: &[TrustBacklogEventMessage],
    ) -> anyhow::Result<TrustBacklogIngestResponse> {
        if events.is_empty() {
            return Ok(TrustBacklogIngestResponse::default());
        }
        let outcome = recorded(
            DB_WRITE_BACKLOG,
            ds_store::backlog::ingest_trust_event_backlog(&self.pool, events),
        )
        .await?;
        Ok(TrustBacklogIngestResponse {
            upserted: outcome.inserted,
            rejected: outcome.rejected,
        })
    }
}

/// Database-gated (`DATABASE_URL`, a migrated database; run with
/// `--ignored --test-threads=1`). They also run as the narrow
/// `trust_backlog` role (CI's per-service step, plan 3b.2), which has no
/// `DELETE`: every run writes rows under its own tag (no fixture is
/// reused, so nothing has to be deleted first), and the final cleanup is
/// best effort.
#[cfg(test)]
mod db_tests {
    use std::time::Duration;

    use movement_feed::{FakeMovementFeed, MovementFeed};
    use sqlx::postgres::PgPoolOptions;

    use super::*;
    use crate::{Delivery, deliver_batch};

    fn database_url() -> String {
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test")
    }

    async fn pool() -> PgPool {
        PgPoolOptions::new()
            .max_connections(3)
            .connect(&database_url())
            .await
            .expect("connect to postgres")
    }

    /// A tag no other run uses, for every key a test writes.
    fn tag(kind: &str) -> String {
        format!("p3b{kind}{}", chrono::Utc::now().timestamp_micros())
    }

    /// A rail day no live data uses.
    fn date() -> chrono::NaiveDate {
        "2099-04-04".parse().unwrap()
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "the fixtures' timestamp fields are Options"
    )]
    fn at(time: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        Some(format!("2099-04-04T{time}:00Z").parse().unwrap())
    }

    fn event(tag: &str, uid: bool, msg_type: &str, key: &str) -> TrustBacklogEventMessage {
        TrustBacklogEventMessage {
            crs: Some("EUS".to_string()),
            train_uid: uid.then(|| format!("{tag}U")),
            train_id: format!("{tag}T"),
            service_date: date(),
            msg_type: msg_type.to_string(),
            event_type: None,
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            dedup_key: format!("{tag}-{key}"),
            gbtt_timestamp: None,
        }
    }

    /// One train's batch: its Activation, a departure carrying its GBTT
    /// time (the 2026-10-07 carry-through), a uid-less arrival whose
    /// identity comes from that Activation (`infer_train_identities`), and
    /// at index 3 a row the table's `msg_type` CHECK refuses.
    fn fixture_batch(tag: &str) -> Vec<TrustBacklogEventMessage> {
        let departure = TrustBacklogEventMessage {
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: at("08:00"),
            actual_timestamp: at("08:02"),
            gbtt_timestamp: at("07:59"),
            variation_status: Some("LATE".to_string()),
            delay_minutes: Some(2),
            ..event(tag, true, "0003", "dep")
        };
        let arrival = TrustBacklogEventMessage {
            crs: Some("CRE".to_string()),
            event_type: Some("ARRIVAL".to_string()),
            planned_timestamp: at("09:30"),
            actual_timestamp: at("09:30"),
            variation_status: Some("ON TIME".to_string()),
            delay_minutes: Some(0),
            ..event(tag, false, "0003", "arr")
        };
        vec![
            event(tag, true, "0001", "act"),
            departure,
            arrival,
            event(tag, true, "0009", "bad"),
        ]
    }

    fn fixture_reasons(tag: &str) -> Vec<TrainReasonMessage> {
        vec![TrainReasonMessage {
            train_id: format!("{tag}T"),
            train_uid: Some(format!("{tag}U")),
            service_date: date(),
            msg_type: "0002".to_string(),
            reason_code: "YI".to_string(),
            canx_type: Some("AT ORIGIN".to_string()),
            loc_stanox: Some("72410".to_string()),
            event_at: at("07:50"),
        }]
    }

    /// Every row the fixture wrote, across the tables the sink writes, with surrogate ids and write times left out and the tag
    /// replaced, sorted.
    async fn snapshot(pool: &PgPool, tag: &str) -> Vec<String> {
        let rows: Vec<String> = sqlx::query_scalar(
            "WITH t AS (SELECT * FROM trains WHERE train_uid LIKE $1 OR train_id LIKE $1) \
             SELECT 'backlog ' || (to_jsonb(b) - ARRAY['id', 'received_at'])::text \
               FROM trust_event_backlog b WHERE b.dedup_key LIKE $1 \
             UNION ALL \
             SELECT 'train ' || (to_jsonb(t) - ARRAY['id', 'created_at', 'resolved_at'])::text FROM t \
             UNION ALL \
             SELECT 'movement ' || (to_jsonb(m) - ARRAY['id', 'trains_id', 'received_at'])::text \
               FROM train_movement_events m JOIN t ON t.id = m.trains_id \
             UNION ALL \
             SELECT 'state ' || (to_jsonb(s) - ARRAY['id', 'trains_id', 'updated_at'])::text \
               FROM train_current_state s JOIN t ON t.id = s.trains_id \
             UNION ALL \
             SELECT 'reason ' || (to_jsonb(r) - ARRAY['trains_id', 'received_at'])::text \
               FROM train_reasons r JOIN t ON t.id = r.trains_id",
        )
        .bind(format!("%{tag}%"))
        .fetch_all(pool)
        .await
        .expect("snapshot");
        let mut rows: Vec<String> = rows.iter().map(|row| row.replace(tag, "TAG")).collect();
        rows.sort();
        rows
    }

    /// Best effort: the superuser and the app role may delete; the narrow
    /// role may not (see the module docs), and its rows stay, under a tag
    /// nothing reuses. Deleting the `trains` rows cascades to their
    /// movements, state and reasons.
    async fn cleanup(pool: &PgPool, tag: &str) {
        let pattern = format!("%{tag}%");
        for sql in [
            "DELETE FROM trust_event_backlog WHERE dedup_key LIKE $1",
            "DELETE FROM trains WHERE train_uid LIKE $1 OR train_id LIKE $1",
        ] {
            if let Err(err) = sqlx::query(sql).bind(&pattern).execute(pool).await {
                eprintln!("leaving the {tag} rows in place: {err}");
                return;
            }
        }
    }

    fn normalise(rejected: &[common::RejectedTrustBacklogRow], tag: &str) -> Vec<String> {
        rejected
            .iter()
            .map(|row| format!("{row:?}").replace(tag, "TAG"))
            .collect()
    }

    /// Plan 3b.1: a fixture batch through the sink leaves the rows the api's
    /// retired routes left (the same `ds_store` calls), and reports the
    /// row the table refuses.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn the_db_sink_writes_a_fixture_batch() {
        let pool = pool().await;
        let db_tag = tag("d");

        let db = DbSink { pool: pool.clone() };
        db.write_reasons(&fixture_reasons(&db_tag)).await.unwrap();
        let db = DbSink { pool: pool.clone() };
        db.write_reasons(&fixture_reasons(&db_tag)).await.unwrap();
        let via_db = db.write_backlog(&fixture_batch(&db_tag)).await.unwrap();

        let rows_db = snapshot(&pool, &db_tag).await;
        cleanup(&pool, &db_tag).await;

        // Three backlog rows, one train, two movements, its state, the reason.
        assert_eq!(rows_db.len(), 8, "{rows_db:#?}");
        assert!(
            rows_db
                .iter()
                .any(|row| row.starts_with("movement ") && row.contains("2099-04-04T07:59:00")),
            "the departure's gbtt_timestamp reaches train_movement_events: {rows_db:#?}"
        );
        assert_eq!(via_db.upserted, 3);
        assert_eq!(
            normalise(&via_db.rejected, &db_tag).len(),
            1,
            "{:?}",
            via_db.rejected
        );
        assert_eq!(via_db.rejected[0].index, 3);
        assert_eq!(via_db.rejected[0].sqlstate, "23514");
    }

    /// A feed that has handed out one batch, so the XACK has something to
    /// confirm.
    async fn feed_with_one_batch() -> FakeMovementFeed {
        let mut feed = FakeMovementFeed::new(vec![vec!["entry".to_string()]]);
        feed.next_batch().await.unwrap();
        feed
    }

    /// Plan 3b.1 through `deliver_batch`: the data-error row is
    /// dead-lettered (`rejected_by_db`, with its SQLSTATE), every other row
    /// lands, and the batch is `XACKed` after the transaction.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn a_data_error_row_is_dead_lettered_and_the_rest_land() {
        let pool = pool().await;
        let tag = tag("r");
        let sink = DbSink { pool: pool.clone() };
        let mut feed = feed_with_one_batch().await;
        let events = fixture_batch(&tag);

        let delivery = deliver_batch(&mut feed, &events, &[], &DB_OPERATIONS, async |events| {
            sink.write_backlog(events).await
        })
        .await;
        let rows = snapshot(&pool, &tag).await;
        cleanup(&pool, &tag).await;

        assert_eq!(delivery, Delivery::Committed);
        assert_eq!(feed.committed_count, 1, "XACKed after the write");
        assert_eq!(feed.dead_lettered.len(), 1);
        let letter = &feed.dead_lettered[0];
        assert_eq!(letter.reason, "rejected_by_db");
        assert!(letter.detail.contains("23514"), "{}", letter.detail);
        assert!(
            letter.payload.contains(&format!("{tag}-bad")),
            "{}",
            letter.payload
        );
        assert_eq!(
            rows.iter()
                .filter(|row| row.starts_with("backlog "))
                .count(),
            3,
            "{rows:#?}"
        );
        assert!(!rows.iter().any(|row| row.contains("TAG-bad")), "{rows:#?}");
    }

    /// Plan 3b.1: a transient failure (a real lock timeout on the train's
    /// row, in the shared-movement write after the backlog insert landed)
    /// leaves the batch un-`XACKed` and un-dead-lettered, and backs off
    /// (from 1 s) before the redelivery. The redelivery lands it, and the
    /// backlog rows already written conflict harmlessly.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn a_transient_error_leaves_the_batch_un_acked_and_backs_off() {
        let pool = pool().await;
        let tag = tag("l");
        let trains_id = ds_store::trains::find_or_create_train(&pool, &format!("{tag}U"), date())
            .await
            .unwrap();
        let impatient = PgPoolOptions::new()
            .max_connections(3)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SET lock_timeout = '300ms'")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url())
            .await
            .unwrap();
        let sink = DbSink { pool: impatient };
        let events: Vec<_> = fixture_batch(&tag).into_iter().take(2).collect();

        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM trains WHERE id = $1 FOR UPDATE")
            .bind(trains_id)
            .execute(&mut *blocker)
            .await
            .unwrap();
        let mut feed = feed_with_one_batch().await;
        let delivery = deliver_batch(&mut feed, &events, &[], &DB_OPERATIONS, async |events| {
            sink.write_backlog(events).await
        })
        .await;
        blocker.rollback().await.unwrap();

        assert_eq!(delivery, Delivery::PostFailed);
        assert_eq!(feed.committed_count, 0, "nothing XACKed");
        assert!(feed.dead_lettered.is_empty());
        assert!(feed.rejected_batches.is_empty());
        let mut failures = common::backoff::FailureStreak::new(crate::DELIVERY_RETRY_BACKOFF);
        let wait = crate::delivery_wait(&delivery, &mut failures).unwrap();
        assert!(wait <= Duration::from_secs(1), "{wait:?}");

        let mut redelivered = feed_with_one_batch().await;
        let retry = deliver_batch(
            &mut redelivered,
            &events,
            &[],
            &DB_OPERATIONS,
            async |events| sink.write_backlog(events).await,
        )
        .await;
        let rows = snapshot(&pool, &tag).await;
        cleanup(&pool, &tag).await;

        assert_eq!(retry, Delivery::Committed);
        assert_eq!(redelivered.committed_count, 1);
        assert_eq!(
            rows.iter()
                .filter(|row| row.starts_with("movement "))
                .count(),
            1,
            "the redelivery landed the movement once: {rows:#?}"
        );
    }

    /// The DB sink's reads and writes work as whichever role runs the
    /// tests (in CI's per-service step, `trust_backlog`): the STANOX/CRS
    /// reload, and empty batches write nothing.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn the_db_sink_reloads_stanox_crs_and_skips_empty_batches() {
        let sink = DbSink { pool: pool().await };
        sink.stanox_crs().await.unwrap();
        sink.write_reasons(&[]).await.unwrap();
        let response = sink.write_backlog(&[]).await.unwrap();
        assert_eq!(response.upserted, 0);
        assert!(response.rejected.is_empty());
    }
}
