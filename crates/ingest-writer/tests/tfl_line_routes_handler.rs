//! `tfl-line-routes/1` against a live database (`DATABASE_URL`, a migrated
//! database; `cargo test -p ingest-writer --test tfl_line_routes_handler --
//! --ignored --test-threads=1`), through [`WriterHandler`] in `apply` mode
//! with the writer's real [`registry`] (DS-151). CI also runs it as the
//! writer's own role (the per-service step), which proves the grants and
//! the `tfl-line-routes` freshness row policy.
//!
//! A snapshot replaces the whole of `tfl_line_routes`, so this test owns
//! that table while it runs. Fixture ids are invented (`940GZZTEST..`,
//! `testline`).

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code: a panic is the right failure in a test"
)]

use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use common::tfl_routes::TflLineRoute;
use ingest_stream::{Envelope, Handled, Handler, HandlerError, SchemaId, StreamEntry};
use ingest_writer::handlers::registry;
use ingest_writer::stream::{StreamSpec, WriterHandler};
use sqlx::PgPool;

const STREAM: &str = "ds:ingest:tfl-stops";

struct Db {
    pool: PgPool,
    /// Setup and cleanup: `MIGRATION_DATABASE_URL` (the owner, under
    /// scripts/test-postgres-roles.py) when set, else `DATABASE_URL`.
    admin: PgPool,
    prefix: String,
}

impl Db {
    async fn new() -> Self {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect");
        let admin = match std::env::var("MIGRATION_DATABASE_URL") {
            Ok(admin_url) => sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&admin_url)
                .await
                .expect("connect MIGRATION_DATABASE_URL"),
            Err(_) => pool.clone(),
        };
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self {
            pool,
            admin,
            prefix: format!("tfl-routes-test-{nanos:x}:"),
        }
    }

    fn entry(&self, key: &str, produced_at: DateTime<Utc>, routes: &[TflLineRoute]) -> StreamEntry {
        StreamEntry {
            stream: STREAM.to_owned(),
            id: "1-0".to_owned(),
            envelope: Envelope::new(
                SchemaId::new(common::tfl_routes::SCHEMA, 1).unwrap(),
                "test/pod",
                format!("{}:{}{key}", common::tfl_routes::SCHEMA, self.prefix),
                produced_at,
                &routes,
            )
            .unwrap(),
        }
    }

    async fn apply(&self, entry: &StreamEntry) -> Result<Handled, HandlerError> {
        let spec = StreamSpec::by_stream(&entry.stream).expect("a writer stream");
        WriterHandler::new(self.pool.clone(), Arc::new(registry()), spec)
            .handle(entry)
            .await
    }

    async fn applied(&self, entry: &StreamEntry) {
        let result = self.apply(entry).await;
        assert!(
            matches!(result, Ok(Handled::Applied)),
            "{}: {result:?}",
            entry.envelope.key
        );
    }

    async fn exec(&self, sql: &str) {
        sqlx::query(sql)
            .execute(&self.admin)
            .await
            .unwrap_or_else(|err| panic!("{sql}: {err}"));
    }

    async fn setup(&self) {
        self.cleanup().await;
        self.exec(
            "UPDATE ingest_freshness SET fetched_at = '2000-01-01T00:00:00Z' \
             WHERE source = 'tfl-line-routes'",
        )
        .await;
    }

    async fn cleanup(&self) {
        self.exec("DELETE FROM tfl_line_routes").await;
        sqlx::query("DELETE FROM ingest_dedup WHERE key LIKE '%' || $1 || '%'")
            .bind(&self.prefix)
            .execute(&self.admin)
            .await
            .unwrap();
    }

    async fn routes(&self) -> Vec<(String, String, i16, String, Vec<String>, DateTime<Utc>)> {
        sqlx::query_as(
            "SELECT line_id, direction, route_index, name, stop_ids, fetched_at \
             FROM tfl_line_routes ORDER BY line_id, direction, route_index",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }
}

fn at_millis(at: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(at.timestamp_millis()).unwrap()
}

fn route(line: &str, direction: &str, name: &str, stops: &[&str]) -> TflLineRoute {
    TflLineRoute {
        line_id: line.into(),
        direction: direction.into(),
        name: name.into(),
        stop_ids: stops.iter().map(|s| (*s).to_owned()).collect(),
        step_seconds: None,
    }
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn a_snapshot_replaces_the_routes() {
    let db = Db::new().await;
    db.setup().await;

    let branch_a = route(
        "testline",
        "outbound",
        "Test A ↔ Test End",
        &["940GZZTESTA", "940GZZTESTJ", "940GZZTESTE"],
    );
    let first = vec![
        branch_a.clone(),
        route(
            "testline",
            "outbound",
            "Test B ↔ Test End",
            &["940GZZTESTB", "940GZZTESTJ", "940GZZTESTE"],
        ),
        route(
            "testline",
            "inbound",
            "Test End ↔ Test A",
            &["940GZZTESTE", "940GZZTESTJ", "940GZZTESTA"],
        ),
        // An exact repeat is kept once; a bad one is refused.
        branch_a,
        route(
            "Test Line",
            "inbound",
            "Bad",
            &["940GZZTESTA", "940GZZTESTB"],
        ),
    ];
    let t1 = at_millis(Utc::now() - TimeDelta::hours(2));
    let entry = db.entry("one", t1, &first);
    db.applied(&entry).await;
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Duplicate)));

    let rows = db.routes().await;
    let keys: Vec<(&str, &str, i16)> = rows
        .iter()
        .map(|r| (r.0.as_str(), r.1.as_str(), r.2))
        .collect();
    assert_eq!(
        keys,
        [
            ("testline", "inbound", 0),
            ("testline", "outbound", 0),
            ("testline", "outbound", 1),
        ]
    );
    assert_eq!(rows[1].3, "Test A ↔ Test End");
    assert_eq!(rows[1].4, ["940GZZTESTA", "940GZZTESTJ", "940GZZTESTE"]);
    assert!(rows.iter().all(|r| r.5 == t1));
    let fresh: DateTime<Utc> = sqlx::query_scalar(
        "SELECT fetched_at FROM ingest_freshness WHERE source = 'tfl-line-routes'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(fresh, t1);

    // The next day one branch is gone.
    let t2 = t1 + TimeDelta::hours(1);
    let second = vec![route(
        "testline",
        "outbound",
        "Test B ↔ Test End",
        &["940GZZTESTB", "940GZZTESTJ", "940GZZTESTE"],
    )];
    db.applied(&db.entry("two", t2, &second)).await;
    let rows = db.routes().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].3, "Test B ↔ Test End");
    assert_eq!(rows[0].2, 0, "renumbered from 0");

    // An older snapshot (a late redelivery) and an empty one change nothing.
    db.applied(&db.entry("old", t1, &first)).await;
    db.applied(&db.entry("empty", t2 + TimeDelta::hours(1), &[]))
        .await;
    assert_eq!(db.routes().await.len(), 1);

    db.cleanup().await;
}

/// `(direction, route_index, step_seconds)`.
type TimedRow = (String, i16, Option<Vec<Option<i32>>>);

/// DS-206: each route's running times are written with it, a step without
/// one as NULL; times that do not fit the stops are dropped, the route kept.
#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn a_snapshot_writes_the_running_times() {
    let db = Db::new().await;
    db.setup().await;

    let stops = ["940GZZTESTA", "940GZZTESTJ", "940GZZTESTE"];
    let timed = TflLineRoute {
        step_seconds: Some(vec![Some(150), None]),
        ..route("testline", "outbound", "Test A ↔ Test End", &stops)
    };
    let misfit = TflLineRoute {
        step_seconds: Some(vec![Some(150)]),
        ..route("testline", "inbound", "Test End ↔ Test A", &stops)
    };
    let untimed = route("testline", "outbound", "Test B ↔ Test End", &stops[1..]);
    let at = at_millis(Utc::now() - TimeDelta::hours(1));
    db.applied(&db.entry("timed", at, &[timed, misfit, untimed]))
        .await;

    let rows: Vec<TimedRow> = sqlx::query_as(
        "SELECT direction, route_index, step_seconds FROM tfl_line_routes \
         ORDER BY direction, route_index",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        rows,
        [
            ("inbound".to_owned(), 0, None),
            ("outbound".to_owned(), 0, Some(vec![Some(150), None])),
            ("outbound".to_owned(), 1, None),
        ]
    );

    db.cleanup().await;
}
