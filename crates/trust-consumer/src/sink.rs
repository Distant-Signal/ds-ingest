//! Where each batch of train events goes (ingest architecture plan 3b.3,
//! decision D1), chosen by `INGEST_SINK`:
//!
//! | | [`HttpSink`] (`http`, the default) | [`DbSink`] (`db`) |
//! |---|---|---|
//! | train events | `POST /private/train-events` | `ds_store::tracking::outbox::write_train_events_deferring`: most directly (`upsert_train_events_batch_in`); a resolution, cancellation or reinstatement, and the later events of its subscription, into `train_event_outbox` for the ingest-writer's loop |
//! | forward signals | `POST /private/train-forward-signals` (best effort) | `insert_forward_signals_on`, in the same transaction (a deferred event's in its outbox row) |
//!
//! The `trust_consumer` role may only read `train_subscriptions` (decided
//! 2026-10-08), so the events that update a subscription wait for the
//! writer's `train_event_outbox` loop (5 s); see `ds_store::tracking::outbox`.
//!
//! The api's two handlers call the same ds-store writers, so the rows are
//! the same either way, and so is the `rejected` list the consumer
//! dead-letters. The forward signals are built the same way for both
//! (`process::build_forward_signals`, for the events that were written),
//! each keyed by its movement's `dedup_key`, so a redelivered entry queues
//! its signal once. `main.rs`'s `run_cycle` `XACKs` the batch only after
//! [`TrainEventSink::write`] returned (for `db`: after its commit).
//!
//! The reads (active tracked trains, STANOX/CRS) stay on the api for both
//! sinks until phase 4 (spec §11.2).

use std::collections::HashMap;

use common::oauth_client::OAuthTokenCache;
use common::{RejectedTrustBacklogRow, TrainMovementEventMessage};
use sqlx::PgPool;

use crate::{process, queries};

/// What a sink's failures count as: the `trust_consumer_errors_total`
/// operation of a failed write (in `API_CALL_OPERATIONS`, so the chart's
/// `DistantSignalConsumerApiCallsFailing` sums it) and the dead-letter
/// reason of an event the write refused for a data error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Operations {
    /// A failed write: transient, the batch stays pending.
    pub write: &'static str,
    /// The `DeadLetter::reason` of a rejected event.
    pub rejected: &'static str,
}

/// [`HttpSink`]'s names: today's, unchanged.
pub(crate) const HTTP_OPERATIONS: Operations = Operations {
    write: "post_train_events",
    rejected: "rejected_by_api",
};

/// [`DbSink`]'s names (plan 3b.4, as the backlog's 3b.2).
pub(crate) const DB_OPERATIONS: Operations = Operations {
    write: "db_write",
    rejected: "rejected_by_db",
};

/// One destination for a batch's train events and forward signals.
pub(crate) trait TrainEventSink {
    /// See [`Operations`].
    fn operations(&self) -> Operations;

    /// Writes `events` and the forward signals of those written
    /// (`trains_id_by_tracked_train_id` resolves each event's train): `Ok`
    /// with the events refused for a data error (every other one landed),
    /// or `Err` for anything transient (nothing to `XACK`). An empty batch
    /// writes nothing.
    async fn write(
        &self,
        events: &[TrainMovementEventMessage],
        trains_id_by_tracked_train_id: &HashMap<i64, i64>,
    ) -> anyhow::Result<Vec<RejectedTrustBacklogRow>>;
}

/// The sink `INGEST_SINK` chose, for the main loop.
pub(crate) enum ActiveSink {
    /// Boxed: the HTTP sink (a client, URLs and a token cache) is much
    /// larger than the pool.
    Http(Box<HttpSink>),
    Db(DbSink),
}

impl TrainEventSink for ActiveSink {
    fn operations(&self) -> Operations {
        match self {
            Self::Http(sink) => sink.operations(),
            Self::Db(sink) => sink.operations(),
        }
    }

    async fn write(
        &self,
        events: &[TrainMovementEventMessage],
        trains_id_by_tracked_train_id: &HashMap<i64, i64>,
    ) -> anyhow::Result<Vec<RejectedTrustBacklogRow>> {
        match self {
            Self::Http(sink) => sink.write(events, trains_id_by_tracked_train_id).await,
            Self::Db(sink) => sink.write(events, trains_id_by_tracked_train_id).await,
        }
    }
}

/// The events not in `rejected`, in order: the ones whose writes landed.
fn written<'a>(
    events: &'a [TrainMovementEventMessage],
    rejected: &[RejectedTrustBacklogRow],
) -> Vec<&'a TrainMovementEventMessage> {
    let rejected: std::collections::HashSet<usize> = rejected.iter().map(|row| row.index).collect();
    events
        .iter()
        .enumerate()
        .filter(|(index, _)| !rejected.contains(index))
        .map(|(_, event)| event)
        .collect()
}

/// `INGEST_SINK=http`: today's path through the api.
pub(crate) struct HttpSink {
    pub client: reqwest::Client,
    /// `API_INGEST_URL`, `.../private/train-events`.
    pub ingest_url: String,
    /// `FORWARD_SIGNALS_URL`, `.../private/train-forward-signals`.
    pub forward_signals_url: String,
    pub tokens: OAuthTokenCache,
}

impl TrainEventSink for HttpSink {
    fn operations(&self) -> Operations {
        HTTP_OPERATIONS
    }

    async fn write(
        &self,
        events: &[TrainMovementEventMessage],
        trains_id_by_tracked_train_id: &HashMap<i64, i64>,
    ) -> anyhow::Result<Vec<RejectedTrustBacklogRow>> {
        let response =
            queries::post_train_events(&self.client, &self.ingest_url, &self.tokens, events)
                .await?;
        // Forward signals only for events api actually wrote. Best effort:
        // a signal only makes notifier look sooner.
        let signals = process::build_forward_signals(
            written(events, &response.rejected),
            trains_id_by_tracked_train_id,
        );
        if let Err(err) = queries::post_train_forward_signals(
            &self.client,
            &self.forward_signals_url,
            &self.tokens,
            &signals,
        )
        .await
        {
            tracing::warn!(error = ?err, "failed to post train forward signals");
        }
        Ok(response.rejected)
    }
}

/// [`DbSink`]'s `db_writes_total{operation}` (`ds_store::writes`): one per
/// batch transaction, `ok` once committed (rows refused for a data error
/// included: the rest landed), else its failure class.
pub(crate) const DB_WRITE_OPERATION: &str = "train_events";

/// Registers [`DB_WRITE_OPERATION`]'s `db_writes_total` series at 0.
pub(crate) fn register_db_write_metrics() {
    ds_store::writes::register(&[DB_WRITE_OPERATION]);
}

/// `INGEST_SINK=db`: straight into Postgres, as the `trust_consumer` role
/// (D1). The events and their forward signals go in one transaction.
pub(crate) struct DbSink {
    pub pool: PgPool,
}

impl TrainEventSink for DbSink {
    fn operations(&self) -> Operations {
        DB_OPERATIONS
    }

    async fn write(
        &self,
        events: &[TrainMovementEventMessage],
        trains_id_by_tracked_train_id: &HashMap<i64, i64>,
    ) -> anyhow::Result<Vec<RejectedTrustBacklogRow>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let started = std::time::Instant::now();
        let result: anyhow::Result<_> = async {
            let mut tx = ds_store::pool::begin(&self.pool).await?;
            let outcome =
                ds_store::tracking::outbox::write_train_events_deferring(&mut tx, events, |kept| {
                    process::build_forward_signals(
                        kept.iter().copied(),
                        trains_id_by_tracked_train_id,
                    )
                })
                .await?;
            tx.commit().await?;
            Ok(outcome)
        }
        .await;
        ds_store::writes::record(
            DB_WRITE_OPERATION,
            result.as_ref().err().map(ds_store::writes::classify),
            started.elapsed(),
        );
        let outcome = result?;
        let rejected = outcome.written.rejected;
        for row in &rejected {
            // As `post_train_events` logs each one.
            tracing::warn!(
                index = row.index,
                dedup_key = %row.dedup_key,
                sqlstate = %row.sqlstate,
                reason = %row.reason,
                constraint = ?row.constraint,
                message = %row.message,
                event = ?events.get(row.index),
                "rejected train event for a data error; wrote the rest of its batch"
            );
        }
        if !outcome.deferred.is_empty() {
            tracing::debug!(
                deferred = outcome.deferred.len(),
                "train events that change a subscription queued for the ingest-writer"
            );
        }
        Ok(rejected)
    }
}

/// Database-gated (`DATABASE_URL`, a migrated database; run with
/// `--ignored --test-threads=1`). They also run as the narrow
/// `trust_consumer` role (CI's per-service step, plan 3b.4), which cannot
/// create the users and subscriptions events are written for, nor delete
/// anything: those fixtures go through `DATABASE_URL_API` when it is set
/// (the per-service step sets it; the api role may), else `DATABASE_URL`.
/// The writer's outbox loop runs as `DATABASE_URL_WRITER` when set (the
/// writer role), else `DATABASE_URL`. Every run writes under its own tag,
/// so nothing has to be deleted first.
#[cfg(test)]
mod db_tests {
    use common::oauth_client::OAuthCredentials;
    use sqlx::postgres::PgPoolOptions;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn database_url() -> String {
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test")
    }

    async fn connect(url: &str) -> PgPool {
        PgPoolOptions::new()
            .max_connections(2)
            .connect(url)
            .await
            .expect("connect to postgres")
    }

    /// The sink's pool: the role under test.
    async fn pool() -> PgPool {
        connect(&database_url()).await
    }

    /// Fixtures, snapshots and cleanup: see the module docs.
    async fn fixture_pool() -> PgPool {
        connect(&std::env::var("DATABASE_URL_API").unwrap_or_else(|_| database_url())).await
    }

    /// The ingest-writer's outbox loop: see the module docs.
    async fn writer_pool() -> PgPool {
        connect(&std::env::var("DATABASE_URL_WRITER").unwrap_or_else(|_| database_url())).await
    }

    /// One tick of the writer's `train_event_outbox` loop.
    async fn tick(writer: &PgPool) -> ds_store::tracking::outbox::OutboxTick {
        ds_store::tracking::outbox::apply_train_event_outbox(writer)
            .await
            .unwrap()
    }

    /// One subscriber's train: a user, a `trains` row and a subscription
    /// (linked to it unless pending), all under `tag`.
    struct Fixture {
        tag: String,
        user_id: String,
        trains_id: i64,
        tracked_train_id: i64,
    }

    impl Fixture {
        async fn new(pool: &PgPool, kind: &str) -> Self {
            Self::with_status(pool, kind, "schedule_matched").await
        }

        /// `pending` leaves the subscription unlinked, for a resolution to
        /// link; any other status links it to the train.
        async fn with_status(pool: &PgPool, kind: &str, status: &str) -> Self {
            let tag = format!("p3bc{kind}{}", chrono::Utc::now().timestamp_micros());
            let user_id = format!("{tag}-user");
            let date: chrono::NaiveDate = "2099-05-05".parse().unwrap();
            ds_store::test_support::seed_user(pool, &user_id).await;
            let trains_id = ds_store::trains::find_or_create_train(pool, &format!("{tag}U"), date)
                .await
                .unwrap();
            let tracked_train_id = ds_store::test_support::seed_backlog_candidate_pin(
                pool,
                &user_id,
                date,
                Some("EUS"),
                Some("2099-05-05T18:15:00Z".parse().unwrap()),
                status,
                (status != "pending").then_some(trains_id),
            )
            .await;
            Self {
                tag,
                user_id,
                trains_id,
                tracked_train_id,
            }
        }

        fn trains_id_by_tracked_train_id(&self) -> HashMap<i64, i64> {
            HashMap::from([(self.tracked_train_id, self.trains_id)])
        }

        /// A departure, an arrival, and at index 1 an event whose status
        /// `train_current_state`'s CHECK refuses.
        fn events(&self) -> Vec<TrainMovementEventMessage> {
            let event = |key: &str| {
                ds_store::test_support::fixture_event(
                    self.tracked_train_id,
                    &format!("{}-{key}", self.tag),
                )
            };
            let departure = TrainMovementEventMessage {
                gbtt_timestamp: Some("2099-05-05T18:14:00Z".parse().unwrap()),
                ..event("dep")
            };
            let bad = TrainMovementEventMessage {
                status: "bogus".to_string(),
                ..event("bad")
            };
            let arrival = TrainMovementEventMessage {
                event_type: Some("ARRIVAL".to_string()),
                loc_crs: Some("CRE".to_string()),
                last_reported_location: Some("CRE".to_string()),
                last_event_type: Some("ARRIVAL".to_string()),
                actual_timestamp: Some("2099-05-05T19:45:00Z".parse().unwrap()),
                ..event("arr")
            };
            vec![departure, bad, arrival]
        }

        /// Every row written for this train, ids, write times and the tag
        /// normalised, sorted.
        async fn snapshot(&self, pool: &PgPool) -> Vec<String> {
            let rows: Vec<String> = sqlx::query_scalar(
                "SELECT 'movement ' || (to_jsonb(m) - ARRAY['id', 'trains_id', 'received_at'])::text \
                   FROM train_movement_events m WHERE m.trains_id = $1 \
                 UNION ALL \
                 SELECT 'state ' || (to_jsonb(s) - ARRAY['id', 'trains_id', 'updated_at'])::text \
                   FROM train_current_state s WHERE s.trains_id = $1 \
                 UNION ALL \
                 SELECT 'signal ' || (to_jsonb(q) - ARRAY['id', 'trains_id', 'created_at'])::text \
                   FROM notifier_forward_queue q WHERE q.trains_id = $1 \
                 UNION ALL \
                 SELECT 'subscription ' || resolution_status || ' ' \
                        || coalesce(unresolved_from, '-') || ' ' \
                        || coalesce((trains_id = $1)::text, 'unlinked') \
                   FROM train_subscriptions WHERE id = $2",
            )
            .bind(self.trains_id)
            .bind(self.tracked_train_id)
            .fetch_all(pool)
            .await
            .expect("snapshot");
            let mut rows: Vec<String> = rows
                .iter()
                .map(|row| {
                    row.replace(&self.tag, "TAG")
                        .replace(&format!("\"{}:", self.trains_id), "\"ID:")
                })
                .collect();
            rows.sort();
            rows
        }

        /// The events of a train resolved by its origin departure: the
        /// resolving departure, an arrival behind it, and a cancellation and
        /// reinstatement of the same subscription afterwards.
        fn resolving_events(&self) -> Vec<TrainMovementEventMessage> {
            let event = |key: &str| {
                ds_store::test_support::fixture_event(
                    self.tracked_train_id,
                    &format!("{}-{key}", self.tag),
                )
            };
            let mut events = self.events();
            events.remove(1);
            events[0].resolved_train_uid = Some(format!("{}U", self.tag));
            events[0].resolved_train_id = Some(format!("{}T", self.tag));
            events[0].identity_date = Some("2099-05-05".parse().unwrap());
            events.push(TrainMovementEventMessage {
                msg_type: "0002".to_string(),
                event_type: None,
                status: "cancelled".to_string(),
                ..event("canx")
            });
            events.push(TrainMovementEventMessage {
                msg_type: "0005".to_string(),
                event_type: None,
                ..event("reinst")
            });
            events
        }

        /// The outbox rows still waiting for this subscription.
        async fn pending(&self, pool: &PgPool) -> i64 {
            sqlx::query_scalar(
                "SELECT count(*) FROM train_event_outbox \
                 WHERE tracked_train_id = $1 AND rejected_at IS NULL",
            )
            .bind(self.tracked_train_id)
            .fetch_one(pool)
            .await
            .unwrap()
        }

        /// Deleting the train cascades to its movements, state and signals.
        /// The outbox rows go through the writer, which may delete them.
        async fn cleanup(&self, pool: &PgPool) {
            if let Err(err) =
                sqlx::query("DELETE FROM train_event_outbox WHERE tracked_train_id = $1")
                    .bind(self.tracked_train_id)
                    .execute(&writer_pool().await)
                    .await
            {
                eprintln!("leaving the {} outbox rows in place: {err}", self.tag);
            }
            ds_store::test_support::cleanup_user(pool, &self.user_id).await;
            sqlx::query("DELETE FROM trains WHERE id = $1")
                .bind(self.trains_id)
                .execute(pool)
                .await
                .expect("cleanup fixture train");
        }
    }

    fn normalise(rejected: &[RejectedTrustBacklogRow], tag: &str) -> Vec<String> {
        rejected
            .iter()
            .map(|row| format!("{row:?}").replace(tag, "TAG"))
            .collect()
    }

    /// Plan 3b.3: the same batch through either sink leaves the same
    /// movements, current state, forward signal and subscription, and
    /// reports the same rejected event. The HTTP side is what the api does
    /// with the two POST bodies: `upsert_train_events_batch` and
    /// `insert_forward_signals`, as `post_train_events` and
    /// `post_train_forward_signals` do.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    #[expect(
        clippy::too_many_lines,
        reason = "one fixture batch through both sinks, then every table compared"
    )]
    async fn both_sinks_write_the_same_rows_for_a_fixture_batch() {
        let fixtures = fixture_pool().await;
        let via_http = Fixture::new(&fixtures, "h").await;
        let via_db = Fixture::new(&fixtures, "d").await;
        let pool = pool().await;

        // The api's answer to the POST: what `upsert_train_events_batch`
        // reports for this batch (checked against the real one below).
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
            .and(path("/private/train-events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "upserted": 2,
                "rejected": [{
                    "index": 1,
                    "dedup_key": format!("{}-bad", via_http.tag),
                    "sqlstate": "23514",
                    "reason": "check_violation",
                    "constraint": "train_current_state_status_check",
                    "message": "refused",
                }],
            })))
            .expect(1)
            .mount(&api)
            .await;
        Mock::given(method("POST"))
            .and(path("/private/train-forward-signals"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"upserted": 1})),
            )
            .expect(1)
            .mount(&api)
            .await;
        let http = HttpSink {
            client: reqwest::Client::new(),
            ingest_url: format!("{}/private/train-events", api.uri()),
            forward_signals_url: format!("{}/private/train-forward-signals", api.uri()),
            tokens: OAuthTokenCache::new(OAuthCredentials {
                token_url: format!("{}/token/", api.uri()),
                client_id: "test".to_owned(),
                scope: "groups".to_owned(),
                username: "test".to_owned(),
                password: "test".to_owned(),
            }),
        };
        http.write(
            &via_http.events(),
            &via_http.trains_id_by_tracked_train_id(),
        )
        .await
        .unwrap();
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
        let events: Vec<TrainMovementEventMessage> =
            serde_json::from_slice(&body("/private/train-events")).unwrap();
        let api_outcome = ds_store::tracking::upsert_train_events_batch(&pool, &events)
            .await
            .unwrap();
        let signals: Vec<common::TrainForwardSignalMessage> =
            serde_json::from_slice(&body("/private/train-forward-signals")).unwrap();
        ds_store::tracking::insert_forward_signals(&pool, &signals)
            .await
            .unwrap();

        let db = DbSink { pool: pool.clone() };
        let db_rejected = db
            .write(&via_db.events(), &via_db.trains_id_by_tracked_train_id())
            .await
            .unwrap();

        let rows_http = via_http.snapshot(&fixtures).await;
        let rows_db = via_db.snapshot(&fixtures).await;
        via_http.cleanup(&fixtures).await;
        via_db.cleanup(&fixtures).await;

        assert_eq!(rows_db, rows_http);
        // Two movements, the state, one signal, the subscription.
        assert_eq!(rows_db.len(), 5, "{rows_db:#?}");
        assert!(
            rows_db
                .iter()
                .any(|row| row.contains("\"dedup_key\": \"ID:TAG-dep\"")),
            "the signal is keyed by its movement: {rows_db:#?}"
        );
        assert!(
            rows_db
                .iter()
                .any(|row| row.starts_with("movement ") && row.contains("2099-05-05T18:14:00")),
            "gbtt_timestamp carried: {rows_db:#?}"
        );
        assert_eq!(
            normalise(&db_rejected, &via_db.tag),
            normalise(&api_outcome.rejected, &via_http.tag)
        );
        assert_eq!(db_rejected.len(), 1);
        assert_eq!(db_rejected[0].index, 1);
        assert_eq!(db_rejected[0].sqlstate, "23514");
    }

    /// Plan 3b.3: an entry redelivered after its write landed (a crash or
    /// a failed XACK) writes its movements idempotently and queues its
    /// forward signal once (keyed by the movement's `dedup_key`).
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn a_redelivered_entry_queues_one_forward_signal() {
        let fixtures = fixture_pool().await;
        let fixture = Fixture::new(&fixtures, "r").await;
        let db = DbSink { pool: pool().await };
        let events = fixture.events();
        let map = fixture.trains_id_by_tracked_train_id();

        db.write(&events, &map).await.unwrap();
        let first = fixture.snapshot(&fixtures).await;
        db.write(&events, &map).await.unwrap();
        let again = fixture.snapshot(&fixtures).await;
        fixture.cleanup(&fixtures).await;

        assert_eq!(again, first);
        assert_eq!(
            again
                .iter()
                .filter(|row| row.starts_with("signal "))
                .count(),
            1,
            "{again:#?}"
        );
    }

    /// Plan 3b.3: a DB failure (a real lock timeout on the train's row)
    /// fails the write as transient and rolls the whole batch back,
    /// the forward signal with it, so `run_cycle` does not `XACK` it
    /// (`main.rs`'s `a_db_sink_failure_does_not_commit_the_batch`).
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn a_db_failure_rolls_the_whole_batch_back() {
        let fixtures = fixture_pool().await;
        let fixture = Fixture::new(&fixtures, "l").await;
        let impatient = PgPoolOptions::new()
            .max_connections(2)
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
        let events = fixture.events();
        let map = fixture.trains_id_by_tracked_train_id();

        // Locks the train's row: the first movement's foreign-key check
        // (FOR KEY SHARE) waits on it.
        let before = fixture.snapshot(&fixtures).await;
        let mut blocker = fixtures.begin().await.unwrap();
        sqlx::query("SELECT 1 FROM trains WHERE id = $1 FOR UPDATE")
            .bind(fixture.trains_id)
            .execute(&mut *blocker)
            .await
            .unwrap();
        let written = sink.write(&events, &map).await;
        blocker.rollback().await.unwrap();
        let after = fixture.snapshot(&fixtures).await;
        fixture.cleanup(&fixtures).await;

        let err = written.expect_err("the lock timeout fails the write");
        assert!(
            ds_store::backlog::classify_anyhow_data_error(&err).is_none(),
            "transient, not a data error: {err:?}"
        );
        assert_eq!(after, before, "nothing of the batch was left behind");
    }

    /// Decided 2026-10-08: a resolution, a cancellation and a reinstatement
    /// (and the events queued behind them) wait in the outbox, and one tick
    /// of the writer's loop leaves the subscription, the train and its
    /// movements, state and forward signal exactly as the api's HTTP path
    /// does. Also checks the writer role can do what the loop needs.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn subscription_changes_after_one_writer_tick_match_the_http_path() {
        let fixtures = fixture_pool().await;
        let writer = writer_pool().await;
        let via_http = Fixture::with_status(&fixtures, "rh", "pending").await;
        let via_db = Fixture::with_status(&fixtures, "rd", "pending").await;
        let pool = pool().await;

        // The api's POST /private/train-events and train-forward-signals,
        // as the api role (`fixtures`).
        let events = via_http.resolving_events();
        let api_outcome = ds_store::tracking::upsert_train_events_batch(&fixtures, &events)
            .await
            .unwrap();
        let signals = process::build_forward_signals(
            written(&events, &api_outcome.rejected),
            &via_http.trains_id_by_tracked_train_id(),
        );
        ds_store::tracking::insert_forward_signals(&fixtures, &signals)
            .await
            .unwrap();

        let db = DbSink { pool };
        let before = via_db.snapshot(&fixtures).await;
        let rejected = db
            .write(
                &via_db.resolving_events(),
                &via_db.trains_id_by_tracked_train_id(),
            )
            .await
            .unwrap();
        let queued = via_db.snapshot(&fixtures).await;
        let waiting = via_db.pending(&fixtures).await;
        let applied = tick(&writer).await;
        let rows_http = via_http.snapshot(&fixtures).await;
        let rows_db = via_db.snapshot(&fixtures).await;
        let left = via_db.pending(&fixtures).await;
        via_http.cleanup(&fixtures).await;
        via_db.cleanup(&fixtures).await;

        assert!(rejected.is_empty() && api_outcome.rejected.is_empty());
        assert_eq!(
            queued, before,
            "nothing of the subscription's events lands before the tick"
        );
        assert_eq!(
            waiting, 4,
            "the resolution and every later event of it wait"
        );
        assert!(
            applied.applied >= 4 && applied.rejected.is_empty(),
            "{applied:?}"
        );
        assert_eq!(left, 0);
        assert_eq!(rows_db, rows_http);
        assert!(
            rows_db.contains(&"subscription resolved - true".to_string()),
            "{rows_db:#?}"
        );
        assert_eq!(
            rows_db
                .iter()
                .filter(|row| row.starts_with("signal "))
                .count(),
            1,
            "{rows_db:#?}"
        );
    }

    /// A cancellation closes the subscription and a reinstatement reopens
    /// it, through the outbox; neither lands before the writer's tick.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn a_cancellation_and_a_reinstatement_update_the_subscription_on_the_tick() {
        let fixtures = fixture_pool().await;
        let writer = writer_pool().await;
        let fixture = Fixture::new(&fixtures, "c").await;
        let db = DbSink { pool: pool().await };
        let map = fixture.trains_id_by_tracked_train_id();
        let event = |msg_type: &str, status: &str, key: &str| TrainMovementEventMessage {
            msg_type: msg_type.to_string(),
            event_type: None,
            status: status.to_string(),
            ..ds_store::test_support::fixture_event(
                fixture.tracked_train_id,
                &format!("{}-{key}", fixture.tag),
            )
        };
        let status = async || -> String {
            sqlx::query_scalar("SELECT resolution_status FROM train_subscriptions WHERE id = $1")
                .bind(fixture.tracked_train_id)
                .fetch_one(&fixtures)
                .await
                .unwrap()
        };

        let cancelled = db
            .write(&[event("0002", "cancelled", "canx")], &map)
            .await
            .unwrap();
        let before_tick = status().await;
        tick(&writer).await;
        let after_cancellation = status().await;
        let reinstated = db
            .write(&[event("0005", "en_route", "reinst")], &map)
            .await
            .unwrap();
        tick(&writer).await;
        let after_reinstatement = status().await;
        fixture.cleanup(&fixtures).await;

        assert!(cancelled.is_empty() && reinstated.is_empty());
        assert_eq!(before_tick, "schedule_matched");
        assert_eq!(after_cancellation, "unresolved");
        assert_eq!(after_reinstatement, "schedule_matched");
    }

    /// A redelivered entry whose events wait in the outbox adds no second
    /// row, and one redelivered after the tick applied it is re-applied
    /// harmlessly: the same rows and one forward signal either way.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn a_redelivered_deferred_entry_is_applied_once() {
        let fixtures = fixture_pool().await;
        let writer = writer_pool().await;
        let fixture = Fixture::with_status(&fixtures, "rr", "pending").await;
        let db = DbSink { pool: pool().await };
        let events = fixture.resolving_events();
        let map = fixture.trains_id_by_tracked_train_id();

        db.write(&events, &map).await.unwrap();
        db.write(&events, &map).await.unwrap();
        let waiting = fixture.pending(&fixtures).await;
        tick(&writer).await;
        let first = fixture.snapshot(&fixtures).await;
        db.write(&events, &map).await.unwrap();
        tick(&writer).await;
        let again = fixture.snapshot(&fixtures).await;
        fixture.cleanup(&fixtures).await;

        assert_eq!(waiting, 4, "the redelivery queued nothing new");
        assert_eq!(again, first);
        assert_eq!(
            again
                .iter()
                .filter(|row| row.starts_with("signal "))
                .count(),
            1,
            "{again:#?}"
        );
    }

    /// Decided 2026-10-08: the `trust_consumer` role reads
    /// `train_subscriptions` and cannot update it. Checked when the tests
    /// run as that role (CI's per-service step); any other role skips.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn the_trust_consumer_role_cannot_update_subscriptions() {
        let pool = pool().await;
        let user: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&pool)
            .await
            .unwrap();
        if !user.contains("trust_consumer") {
            eprintln!("running as {user}, not the trust_consumer role: skipped");
            return;
        }
        let err =
            sqlx::query("UPDATE train_subscriptions SET resolution_status = resolution_status")
                .execute(&pool)
                .await
                .expect_err("no UPDATE on train_subscriptions");
        assert_eq!(
            err.as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .as_deref(),
            Some("42501"),
            "{err}"
        );
        let can_read: bool =
            sqlx::query_scalar("SELECT has_table_privilege('train_subscriptions', 'SELECT')")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(can_read);
    }

    /// Decided 2026-10-08: the `trust_consumer` role only reads `trains`
    /// (a train's row is created and marked by a resolution, which the
    /// outbox path applies), and a normal batch still lands. The refusals
    /// are checked when the tests run as that role (CI's per-service step).
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn the_trust_consumer_role_cannot_write_trains_and_a_batch_still_lands() {
        let fixtures = fixture_pool().await;
        let fixture = Fixture::new(&fixtures, "t").await;
        let pool = pool().await;
        let user: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&pool)
            .await
            .unwrap();
        let refusals = if user.contains("trust_consumer") {
            let insert = sqlx::query(
                "INSERT INTO trains (train_uid, service_date) VALUES ($1, '2099-05-05')",
            )
            .bind(format!("{}X", fixture.tag))
            .execute(&pool)
            .await;
            let update = sqlx::query("UPDATE trains SET train_id = train_id WHERE id = $1")
                .bind(fixture.trains_id)
                .execute(&pool)
                .await;
            Some([insert, update].map(|result| {
                result
                    .expect_err("no INSERT or UPDATE on trains")
                    .as_database_error()
                    .and_then(sqlx::error::DatabaseError::code)
                    .map(std::borrow::Cow::into_owned)
            }))
        } else {
            eprintln!("running as {user}, not the trust_consumer role: refusals not checked");
            None
        };
        let db = DbSink { pool };
        let mut events = fixture.events();
        events.remove(1);
        let rejected = db
            .write(&events, &fixture.trains_id_by_tracked_train_id())
            .await;
        let rows = fixture.snapshot(&fixtures).await;
        fixture.cleanup(&fixtures).await;

        if let Some(codes) = refusals {
            assert_eq!(
                codes,
                [Some("42501".to_string()), Some("42501".to_string())]
            );
        }
        assert!(rejected.unwrap().is_empty());
        assert_eq!(
            rows.iter()
                .filter(|row| row.starts_with("movement "))
                .count(),
            2,
            "{rows:#?}"
        );
    }

    /// The narrow role's grants cover an empty batch's no-op and the
    /// schema the sink writes (CI's per-service step).
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn an_empty_batch_writes_nothing() {
        let db = DbSink { pool: pool().await };
        assert!(db.write(&[], &HashMap::new()).await.unwrap().is_empty());
    }
}
