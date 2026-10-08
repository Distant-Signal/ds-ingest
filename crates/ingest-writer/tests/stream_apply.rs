//! The writer's stream half against a live database (plan 3a.3): dedup,
//! per-row isolation, shadow mode, and the D13 observed-time guard, through
//! [`WriterHandler`] with a test-only schema handler (the product handlers
//! are plan 3a.6). The last two tests also need Redis (`REDIS_URL`): one
//! entry end to end through `StreamConsumer`, and the dead-letter `MINID`
//! trim.
//!
//! The test handler writes TEMP tables (the writer role creates no tables),
//! so the pool has one connection: the tables live on it. Keys in
//! `ingest_dedup` carry a random prefix and are deleted afterwards.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code: a panic is the right failure in a test"
)]

use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use ingest_stream::{
    Envelope, Handled, Handler, HandlerError, SchemaId, Step, StreamConsumer, StreamEntry,
};
use ingest_writer::handlers::{Applied, BoxFuture, Registry, SchemaHandler, apply_rows, decode};
use ingest_writer::observed::{Observed, guard};
use ingest_writer::stream::{Mode, WriterHandler};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool};

// ---------------------------------------------------------------------------
// The test handlers.

/// `writer-test-rows/1`: appends each row (not idempotent, so a second
/// apply would show), one savepoint per row; `value < 0` breaks a CHECK.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Row {
    name: String,
    value: i32,
}

struct AppendRows;

impl SchemaHandler for AppendRows {
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        decode::<Vec<Row>>(entry).map(|_| ())
    }

    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        _: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let rows: Vec<Row> = decode(entry)?;
            let outcome = apply_rows(conn, rows, |conn, row| {
                Box::pin(async move {
                    sqlx::query("INSERT INTO writer_test_rows (name, value) VALUES ($1, $2)")
                        .bind(&row.name)
                        .bind(row.value)
                        .execute(conn)
                        .await
                        .map(|_| ())
                })
            })
            .await?;
            outcome.into_applied(|rows| rows)
        })
    }
}

/// `writer-test-snapshot/1`: upserts each row by id under the observed-time
/// guard; `at` is the row's own time (else `produced_at`).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SnapshotRow {
    id: String,
    value: i32,
    at: Option<DateTime<Utc>>,
}

struct Snapshot;

impl SchemaHandler for Snapshot {
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        decode::<Vec<SnapshotRow>>(entry).map(|_| ())
    }

    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let rows: Vec<SnapshotRow> = decode(entry)?;
            let sql = format!(
                "INSERT INTO writer_test_snapshot AS t (id, value, observed_at) VALUES ($1, $2, $3) \
                 ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, observed_at = EXCLUDED.observed_at \
                 WHERE {}",
                guard("t", "observed_at")
            );
            for row in rows {
                sqlx::query(&sql)
                    .bind(&row.id)
                    .bind(row.value)
                    .bind(observed.observed_at(row.at))
                    .execute(&mut *conn)
                    .await
                    .map_err(|err| ingest_writer::handlers::classify(&err))?;
            }
            Ok(Applied::All)
        })
    }
}

fn rows_schema() -> SchemaId {
    SchemaId::new("writer-test-rows", 1).unwrap()
}

fn snapshot_schema() -> SchemaId {
    SchemaId::new("writer-test-snapshot", 1).unwrap()
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry.register(&rows_schema(), AppendRows).unwrap();
    registry.register(&snapshot_schema(), Snapshot).unwrap();
    Arc::new(registry)
}

// ---------------------------------------------------------------------------
// Harness.

fn rand_suffix() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    );
    format!("{:016x}", hasher.finish())
}

struct Db {
    pool: PgPool,
    prefix: String,
}

impl Db {
    /// One connection, with the test's TEMP tables on it.
    async fn new() -> Self {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect(&url)
            .await
            .expect("connect");
        for sql in [
            "CREATE TEMP TABLE writer_test_rows (name TEXT NOT NULL, value INT NOT NULL CHECK (value >= 0))",
            "CREATE TEMP TABLE writer_test_snapshot (id TEXT PRIMARY KEY, value INT NOT NULL, observed_at TIMESTAMPTZ)",
        ] {
            sqlx::query(sql).execute(&pool).await.unwrap();
        }
        Self {
            pool,
            prefix: format!("writer-test-{}:", rand_suffix()),
        }
    }

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    fn entry<T: Serialize>(
        &self,
        schema: SchemaId,
        key: &str,
        produced_at: DateTime<Utc>,
        payload: &T,
    ) -> StreamEntry {
        StreamEntry {
            stream: "ds:ingest:writer-test".to_owned(),
            id: "1-0".to_owned(),
            envelope: Envelope::new(schema, "test/pod", self.key(key), produced_at, payload)
                .unwrap(),
        }
    }

    async fn rows(&self) -> Vec<(String, i32)> {
        sqlx::query_as("SELECT name, value FROM writer_test_rows ORDER BY name")
            .fetch_all(&self.pool)
            .await
            .unwrap()
    }

    async fn snapshot(&self, id: &str) -> Option<(i32, Option<DateTime<Utc>>)> {
        sqlx::query_as("SELECT value, observed_at FROM writer_test_snapshot WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .unwrap()
    }

    async fn dedup_keys(&self) -> Vec<String> {
        sqlx::query_scalar("SELECT key FROM ingest_dedup WHERE key LIKE $1 || '%' ORDER BY key")
            .bind(&self.prefix)
            .fetch_all(&self.pool)
            .await
            .unwrap()
    }

    async fn cleanup(&self) {
        sqlx::query("DELETE FROM ingest_dedup WHERE key LIKE $1 || '%'")
            .bind(&self.prefix)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    fn handler(&self, mode: Mode) -> WriterHandler {
        WriterHandler::new(self.pool.clone(), registry(), mode)
    }
}

fn row(name: &str, value: i32) -> Row {
    Row {
        name: name.to_owned(),
        value,
    }
}

fn snap(id: &str, value: i32) -> SnapshotRow {
    SnapshotRow {
        id: id.to_owned(),
        value,
        at: None,
    }
}

// ---------------------------------------------------------------------------
// DB-gated.

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn an_entry_is_applied_once_with_dedup() {
    let db = Db::new().await;
    let handler = db.handler(Mode::Apply);
    let entry = db.entry(rows_schema(), "k1", Utc::now(), &[row("a", 1), row("b", 2)]);

    assert!(matches!(handler.handle(&entry).await, Ok(Handled::Applied)));
    // Redelivered (a lost XACK, an XAUTOCLAIM): the key is already applied.
    assert!(matches!(
        handler.handle(&entry).await,
        Ok(Handled::Duplicate)
    ));
    assert_eq!(db.rows().await, [("a".to_owned(), 1), ("b".to_owned(), 2)]);
    assert_eq!(db.dedup_keys().await, [db.key("k1")]);

    // A new key applies.
    let next = db.entry(rows_schema(), "k2", Utc::now(), &[row("c", 3)]);
    assert!(matches!(handler.handle(&next).await, Ok(Handled::Applied)));
    assert_eq!(db.rows().await.len(), 3);

    // Pruning keeps keys younger than the retention, then drops them.
    assert_eq!(
        ingest_writer::dedup::prune(&db.pool, ingest_writer::dedup::RETENTION)
            .await
            .unwrap(),
        0
    );
    assert_eq!(db.dedup_keys().await.len(), 2);
    sqlx::query("UPDATE ingest_dedup SET applied_at = now() - interval '49 hours' WHERE key = $1")
        .bind(db.key("k1"))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        ingest_writer::dedup::prune(&db.pool, ingest_writer::dedup::RETENTION)
            .await
            .unwrap()
            >= 1
    );
    assert_eq!(db.dedup_keys().await, [db.key("k2")]);
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn a_data_error_rejects_its_row_and_the_rest_commit() {
    let db = Db::new().await;
    let handler = db.handler(Mode::Apply);
    let entry = db.entry(
        rows_schema(),
        "k1",
        Utc::now(),
        &[row("a", 1), row("bad", -1), row("c", 3)],
    );

    let Ok(Handled::PartiallyRejected { reason, rejected }) = handler.handle(&entry).await else {
        panic!("expected PartiallyRejected");
    };
    assert!(reason.contains("1 row(s) refused"), "{reason}");
    assert!(reason.contains("check constraint"), "{reason}");
    let rejected: Vec<Row> = serde_json::from_str(rejected.get()).unwrap();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].name, "bad");
    // The rest committed, with the dedup key.
    assert_eq!(db.rows().await, [("a".to_owned(), 1), ("c".to_owned(), 3)]);
    assert_eq!(db.dedup_keys().await, [db.key("k1")]);

    // A payload that does not decode is poison, and writes nothing.
    let garbage = db.entry(rows_schema(), "k2", Utc::now(), &"not rows");
    assert!(matches!(
        handler.handle(&garbage).await,
        Err(HandlerError::Poison(_))
    ));
    // An unknown name is poison; a known name's unknown version stays pending.
    let unknown = db.entry(SchemaId::new("nope", 1).unwrap(), "k3", Utc::now(), &[0]);
    assert!(matches!(
        handler.handle(&unknown).await,
        Err(HandlerError::Poison(_))
    ));
    let newer = db.entry(
        SchemaId::new("writer-test-rows", 2).unwrap(),
        "k4",
        Utc::now(),
        &[0],
    );
    assert!(matches!(
        handler.handle(&newer).await,
        Err(HandlerError::UnsupportedSchema(_))
    ));
    assert_eq!(db.dedup_keys().await, [db.key("k1")]);
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn shadow_mode_writes_nothing() {
    let db = Db::new().await;
    let handler = db.handler(Mode::Shadow);
    let entry = db.entry(rows_schema(), "k1", Utc::now(), &[row("a", 1)]);
    assert!(matches!(handler.handle(&entry).await, Ok(Handled::Skipped)));
    let snapshot = db.entry(snapshot_schema(), "k2", Utc::now(), &[snap("x", 1)]);
    assert!(matches!(
        handler.handle(&snapshot).await,
        Ok(Handled::Skipped)
    ));
    assert!(db.rows().await.is_empty());
    assert_eq!(db.snapshot("x").await, None);
    assert!(
        db.dedup_keys().await.is_empty(),
        "shadow claims no dedup key"
    );
    // Shadow still validates: a bad payload is poison.
    let garbage = db.entry(rows_schema(), "k3", Utc::now(), &"not rows");
    assert!(matches!(
        handler.handle(&garbage).await,
        Err(HandlerError::Poison(_))
    ));
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn an_observed_time_ahead_is_clamped_when_written() {
    let db = Db::new().await;
    let handler = db.handler(Mode::Apply);
    let before = Utc::now();
    // produced_at an hour ahead (a producer's clock jumped).
    let entry = db.entry(
        snapshot_schema(),
        "k1",
        before + TimeDelta::hours(1),
        &[snap("x", 1)],
    );
    assert!(matches!(handler.handle(&entry).await, Ok(Handled::Applied)));
    let (_, at) = db.snapshot("x").await.unwrap();
    let at = at.unwrap();
    assert!(at <= Utc::now() + TimeDelta::minutes(2), "{at}");
    assert!(
        at >= before + TimeDelta::minutes(2) - TimeDelta::seconds(1),
        "{at}"
    );
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn a_row_stamped_in_the_future_is_healed_by_the_next_snapshot() {
    let db = Db::new().await;
    // Written before the clamp existed, by a clock an hour ahead.
    sqlx::query(
        "INSERT INTO writer_test_snapshot (id, value, observed_at) \
         VALUES ('x', 1, now() + interval '1 hour')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let handler = db.handler(Mode::Apply);
    let now = Utc::now();
    let entry = db.entry(snapshot_schema(), "k1", now, &[snap("x", 2)]);
    assert!(matches!(handler.handle(&entry).await, Ok(Handled::Applied)));
    let (value, at) = db.snapshot("x").await.unwrap();
    assert_eq!(
        value, 2,
        "the next snapshot overwrites the future-stamped row"
    );
    assert_eq!(
        at.unwrap().timestamp_millis(),
        now.timestamp_millis(),
        "stamped with produced_at"
    );
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn an_older_snapshot_changes_nothing() {
    let db = Db::new().await;
    // A row from before the guard column existed: NULL counts as older.
    sqlx::query("INSERT INTO writer_test_snapshot (id, value, observed_at) VALUES ('y', 0, NULL)")
        .execute(&db.pool)
        .await
        .unwrap();
    let handler = db.handler(Mode::Apply);
    let now = Utc::now();
    let newer = db.entry(snapshot_schema(), "new", now, &[snap("x", 2), snap("y", 2)]);
    assert!(matches!(handler.handle(&newer).await, Ok(Handled::Applied)));
    assert_eq!(db.snapshot("y").await.unwrap().0, 2, "NULL is older");

    // Redelivered out of order after an XAUTOCLAIM: an older snapshot.
    let older = db.entry(
        snapshot_schema(),
        "old",
        now - TimeDelta::minutes(10),
        &[snap("x", 1), snap("y", 1)],
    );
    assert!(matches!(handler.handle(&older).await, Ok(Handled::Applied)));
    for id in ["x", "y"] {
        let (value, at) = db.snapshot(id).await.unwrap();
        assert_eq!(value, 2, "{id}: the older snapshot changes nothing");
        assert_eq!(at.unwrap().timestamp_millis(), now.timestamp_millis());
    }

    // The row's own time is the observed time when it has one, so a row of
    // an older snapshot stamped newer still applies; and an equal time
    // applies (>=), so a replay of the same snapshot is a no-op rewrite.
    let own_time = db.entry(
        snapshot_schema(),
        "own",
        now - TimeDelta::minutes(20),
        &[SnapshotRow {
            id: "x".to_owned(),
            value: 3,
            at: Some(now + TimeDelta::seconds(1)),
        }],
    );
    assert!(matches!(
        handler.handle(&own_time).await,
        Ok(Handled::Applied)
    ));
    assert_eq!(db.snapshot("x").await.unwrap().0, 3);
    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// DB- and Redis-gated.

fn admin_url() -> String {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let password = std::env::var("REDIS_PASSWORD")
        .ok()
        .map(common::secret::Secret::from);
    common::redis_auth::redis_url_with_password(&url, password.as_ref())
        .unwrap()
        .expose()
        .to_owned()
}

async fn redis() -> common::redis_conn::RedisConn {
    common::redis_conn::connect(&redis::Client::open(admin_url()).unwrap())
        .await
        .expect("Redis must be reachable (REDIS_URL) to run this test")
}

async fn delete_keys(conn: &mut common::redis_conn::RedisConn, keys: &[&str]) {
    let _: i64 = redis::cmd("DEL").arg(keys).query_async(conn).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live database and Redis; run with DATABASE_URL and REDIS_URL set and --ignored"]
async fn an_entry_flows_from_the_stream_to_the_database_once() {
    let db = Db::new().await;
    let stream = format!("t{}:ds:ingest:writer-test", rand_suffix());
    let dead_letters = ingest_stream::dead_letter_stream(&stream);
    let mut conn = redis().await;
    let mut consumer = StreamConsumer::new(
        redis().await,
        ingest_stream::ConsumerConfig::new(&stream, "pod-a", 100),
    );
    consumer.ensure_group().await.unwrap();

    let entry = db.entry(
        rows_schema(),
        "k1",
        Utc::now(),
        &[row("a", 1), row("bad", -1)],
    );
    let encoded = entry.envelope.encode().unwrap();
    // The same entry twice: the second is a redelivery of an applied key.
    for _ in 0..2 {
        ingest_stream::xadd_entry(&mut conn, &stream, 100, &encoded)
            .await
            .unwrap();
    }
    let handler = db.handler(Mode::Apply);
    let mut steps = Vec::new();
    for _ in 0..4 {
        let step = consumer
            .step(&handler, std::pin::pin!(std::future::pending::<()>()))
            .await
            .unwrap();
        steps.push(step.clone());
        if step == Step::Processed(2) {
            break;
        }
    }
    assert!(steps.contains(&Step::Processed(2)), "{steps:?}");
    assert_eq!(db.rows().await, [("a".to_owned(), 1)], "applied once");
    // The refused row went to the dead-letter stream.
    let dead: Vec<redis::Value> = redis::cmd("XRANGE")
        .arg(&dead_letters)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(dead.len(), 1);
    let pending: Vec<redis::Value> = redis::cmd("XPENDING")
        .arg(&stream)
        .arg(ingest_stream::WRITER_GROUP)
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(pending.is_empty(), "both entries acked");

    delete_keys(&mut conn, &[&stream, &dead_letters]).await;
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires Redis; run with REDIS_URL set and --ignored"]
async fn dead_letters_older_than_seven_days_are_trimmed() {
    const OLD: i64 = 250;
    let mut conn = redis().await;
    let dead_letters = format!("t{}:ds:dlq:writer-test", rand_suffix());
    let now = Utc::now();
    let old_start = (now - TimeDelta::days(8)).timestamp_millis();
    let recent = (now - TimeDelta::days(1)).timestamp_millis();
    // `MINID ~` trims whole radix-tree nodes only (100 entries by default),
    // so add enough old entries to fill at least one node.
    for ms in (old_start..old_start + OLD).chain([recent]) {
        let _: String = redis::cmd("XADD")
            .arg(&dead_letters)
            .arg(format!("{ms}-0"))
            .arg("f")
            .arg("v")
            .query_async(&mut conn)
            .await
            .unwrap();
    }
    let removed = ingest_writer::stream::trim_dead_letters(
        &mut conn,
        &dead_letters,
        now,
        ingest_writer::stream::DEAD_LETTER_RETENTION,
    )
    .await
    .unwrap();
    let newest: Vec<(String, Vec<(String, String)>)> = redis::cmd("XREVRANGE")
        .arg(&dead_letters)
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap();
    let left: u64 = redis::cmd("XLEN")
        .arg(&dead_letters)
        .query_async(&mut conn)
        .await
        .unwrap();
    delete_keys(&mut conn, &[&dead_letters]).await;
    assert!(removed > 0, "at least one node of old entries trimmed");
    assert!(
        removed <= u64::try_from(OLD).unwrap(),
        "never a recent entry"
    );
    assert_eq!(
        newest[0].0,
        format!("{recent}-0"),
        "the recent entry is kept"
    );
    assert_eq!(removed + left, u64::try_from(OLD).unwrap() + 1);
}
