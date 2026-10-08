//! Where each batch goes (ingest architecture plan 3b.1, spec R1), chosen
//! by `INGEST_SINK`:
//!
//! | | [`HttpSink`] (`http`, the default) | [`DbSink`] (`db`) |
//! |---|---|---|
//! | backlog events | `POST /private/trust-event-backlog` | `ds_store::backlog::ingest_trust_event_backlog` |
//! | reason codes | `POST /private/train-reasons` | `ds_store::backlog::reasons::upsert_reasons` |
//! | STANOX/CRS reload | `GET /private/stanox-crs` | `ds_store::reference::list_stanox_crs` |
//!
//! The api's two POST handlers call exactly those ds-store functions, so
//! the rows are the same either way, and so is the `rejected` list the
//! consumer dead-letters. Either way the batch is `XACKed` only after the
//! write returned (for `db`: after its commit), and a transient failure
//! leaves it pending and backs off before the next read (`main.rs`'s
//! `deliver_batch` and `delivery_wait`).

use common::oauth_client::OAuthTokenCache;
use common::{
    StanoxCrsRecord, TrainReasonMessage, TrustBacklogEventMessage, TrustBacklogIngestResponse,
};
use sqlx::PgPool;

use crate::queries;

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

/// [`HttpSink`]'s names: today's, unchanged.
pub(crate) const HTTP_OPERATIONS: Operations = Operations {
    write: "post_batch",
    write_reasons: "post_train_reasons",
    rejected: "rejected_by_api",
};

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

/// `INGEST_SINK=http`: today's path through the api.
pub(crate) struct HttpSink {
    pub client: reqwest::Client,
    /// `API_INGEST_URL`, `.../private/trust-event-backlog`.
    pub ingest_url: String,
    /// `.../private/train-reasons`, beside `ingest_url`; `None` when that
    /// URL does not end in `/trust-event-backlog` (warned at startup):
    /// reasons are then not sent.
    pub reasons_url: Option<String>,
    pub stanox_crs_url: String,
    pub tokens: OAuthTokenCache,
}

impl BacklogSink for HttpSink {
    const OPERATIONS: Operations = HTTP_OPERATIONS;

    async fn stanox_crs(&self) -> anyhow::Result<Vec<StanoxCrsRecord>> {
        queries::fetch_stanox_crs(&self.client, &self.stanox_crs_url, &self.tokens).await
    }

    async fn write_reasons(&self, reasons: &[TrainReasonMessage]) -> anyhow::Result<()> {
        match &self.reasons_url {
            Some(url) => {
                queries::post_train_reasons(&self.client, url, &self.tokens, reasons).await
            }
            None => Ok(()),
        }
    }

    async fn write_backlog(
        &self,
        events: &[TrustBacklogEventMessage],
    ) -> anyhow::Result<TrustBacklogIngestResponse> {
        queries::post_trust_event_backlog(&self.client, &self.ingest_url, &self.tokens, events)
            .await
    }
}

/// `INGEST_SINK=db`: straight into Postgres, as the `trust_backlog` role.
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
        ds_store::backlog::reasons::upsert_reasons(&self.pool, reasons).await?;
        Ok(())
    }

    async fn write_backlog(
        &self,
        events: &[TrustBacklogEventMessage],
    ) -> anyhow::Result<TrustBacklogIngestResponse> {
        if events.is_empty() {
            return Ok(TrustBacklogIngestResponse::default());
        }
        let outcome = ds_store::backlog::ingest_trust_event_backlog(&self.pool, events).await?;
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

    use common::oauth_client::OAuthCredentials;
    use movement_feed::{FakeMovementFeed, MovementFeed};
    use sqlx::postgres::PgPoolOptions;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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

    /// Every row the fixture wrote, across the tables the route and the
    /// sink write, with surrogate ids and write times left out and the tag
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

    /// Plan 3b.1: the same batch through either sink leaves the same rows
    /// in every table, and reports the same rejected rows. The HTTP side is
    /// what the api does with the two POST bodies: decode them and call
    /// `ingest_trust_event_backlog` and `upsert_reasons`, exactly as
    /// `post_trust_event_backlog` and `post_train_reasons` do.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn both_sinks_write_the_same_rows_for_a_fixture_batch() {
        let pool = pool().await;
        let (http_tag, db_tag) = (tag("h"), tag("d"));

        let api = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fake-jwt",
                "expires_in": 300,
            })))
            .mount(&api)
            .await;
        Mock::given(method("POST"))
            .and(path("/private/trust-event-backlog"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"upserted": 0, "rejected": []})),
            )
            .expect(1)
            .mount(&api)
            .await;
        Mock::given(method("POST"))
            .and(path("/private/train-reasons"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"upserted": 0})),
            )
            .expect(1)
            .mount(&api)
            .await;
        let ingest_url = format!("{}/private/trust-event-backlog", api.uri());
        let http = HttpSink {
            client: reqwest::Client::new(),
            reasons_url: queries::train_reasons_url(&ingest_url),
            ingest_url,
            stanox_crs_url: format!("{}/private/stanox-crs", api.uri()),
            tokens: OAuthTokenCache::new(OAuthCredentials {
                token_url: format!("{}/token/", api.uri()),
                client_id: "test".to_owned(),
                scope: "groups".to_owned(),
                username: "test".to_owned(),
                password: "test".to_owned(),
            }),
        };
        http.write_reasons(&fixture_reasons(&http_tag))
            .await
            .unwrap();
        http.write_backlog(&fixture_batch(&http_tag)).await.unwrap();
        let requests = api.received_requests().await.unwrap();
        let body = |route: &str| {
            requests
                .iter()
                .find(|request| request.url.path() == route)
                .unwrap_or_else(|| panic!("no POST {route}"))
                .body
                .clone()
        };
        // The api's two handlers, on the bodies it was sent.
        let reasons: Vec<TrainReasonMessage> =
            serde_json::from_slice(&body("/private/train-reasons")).unwrap();
        ds_store::backlog::reasons::upsert_reasons(&pool, &reasons)
            .await
            .unwrap();
        let events: Vec<TrustBacklogEventMessage> =
            serde_json::from_slice(&body("/private/trust-event-backlog")).unwrap();
        let via_api = ds_store::backlog::ingest_trust_event_backlog(&pool, &events)
            .await
            .unwrap();

        let db = DbSink { pool: pool.clone() };
        db.write_reasons(&fixture_reasons(&db_tag)).await.unwrap();
        let via_db = db.write_backlog(&fixture_batch(&db_tag)).await.unwrap();

        let rows_http = snapshot(&pool, &http_tag).await;
        let rows_db = snapshot(&pool, &db_tag).await;
        cleanup(&pool, &http_tag).await;
        cleanup(&pool, &db_tag).await;

        assert_eq!(rows_db, rows_http);
        // Three backlog rows, one train, two movements, its state, the reason.
        assert_eq!(rows_db.len(), 8, "{rows_db:#?}");
        assert!(
            rows_db
                .iter()
                .any(|row| row.starts_with("movement ") && row.contains("2099-04-04T07:59:00")),
            "the departure's gbtt_timestamp reaches train_movement_events: {rows_db:#?}"
        );
        assert_eq!(via_db.upserted, via_api.inserted);
        assert_eq!(
            normalise(&via_db.rejected, &db_tag),
            normalise(&via_api.rejected, &http_tag)
        );
        assert_eq!(via_db.rejected.len(), 1);
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
        let mut failures = common::backoff::FailureStreak::new(crate::delivery_backoff(
            crate::config::IngestSink::Db,
        ));
        let wait = crate::delivery_wait(&delivery, &mut failures, None).unwrap();
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
