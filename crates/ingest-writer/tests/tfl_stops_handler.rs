//! `tfl-stops/1` against a live database (`DATABASE_URL`, a migrated
//! database; `cargo test -p ingest-writer --test tfl_stops_handler --
//! --ignored --test-threads=1`), through [`WriterHandler`] in `apply` mode
//! with the writer's real [`registry`] (docs/superpowers/specs/
//! 2026-10-20-tfl-stop-lookup-design.md §2). CI also runs it as the
//! writer's own role (the per-service step), which proves the grants and
//! the `tfl-stops` freshness row policy.
//!
//! A snapshot replaces the whole of `tfl_stops`, so this test owns that
//! table while it runs. Fixture ids are invented (`940GZZTEST..`), the CRS
//! codes `Z9A`/`Z9B` and the `TIPLOC` `ZZTSTTPL` too.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code: a panic is the right failure in a test"
)]

use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use common::tfl_stops::{TflStop, TflStopLine};
use ingest_stream::{Envelope, Handled, Handler, HandlerError, SchemaId, StreamEntry};
use ingest_writer::handlers::registry;
use ingest_writer::stream::{StreamSpec, WriterHandler};
use sqlx::PgPool;

const STREAM: &str = "ds:ingest:tfl-stops";

struct Db {
    pool: PgPool,
    /// Fixture setup and cleanup: `MIGRATION_DATABASE_URL` (the owner,
    /// under scripts/test-postgres-roles.py) when set, since the narrow
    /// writer role may not write `stations` or `tiploc_crs`; else
    /// `DATABASE_URL`.
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
            prefix: format!("tfl-stops-test-{nanos:x}:"),
        }
    }

    fn entry(&self, key: &str, produced_at: DateTime<Utc>, stops: &[TflStop]) -> StreamEntry {
        StreamEntry {
            stream: STREAM.to_owned(),
            id: "1-0".to_owned(),
            envelope: Envelope::new(
                SchemaId::new(common::tfl_stops::SCHEMA, 1).unwrap(),
                "test/pod",
                format!("{}:{}{key}", common::tfl_stops::SCHEMA, self.prefix),
                produced_at,
                &stops,
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
            "INSERT INTO stations (crs, name) VALUES ('Z9A', 'Testbury Rail'), ('Z9B', 'Testbury Low Level') \
             ON CONFLICT (crs) DO NOTHING",
        )
        .await;
        self.exec(
            "INSERT INTO tiploc_crs (tiploc, crs, station_name, stanox, source_sequence) \
             VALUES ('ZZTSTTPL', 'Z9B', 'Testbury Low Level', '99999', 0) \
             ON CONFLICT (tiploc) DO NOTHING",
        )
        .await;
        self.exec(
            "UPDATE ingest_freshness SET fetched_at = '2000-01-01T00:00:00Z' WHERE source = 'tfl-stops'",
        )
        .await;
    }

    async fn cleanup(&self) {
        for sql in [
            "DELETE FROM tfl_stops",
            "DELETE FROM stations WHERE crs IN ('Z9A', 'Z9B')",
            "DELETE FROM tiploc_crs WHERE tiploc = 'ZZTSTTPL'",
        ] {
            self.exec(sql).await;
        }
        sqlx::query("DELETE FROM ingest_dedup WHERE key LIKE '%' || $1 || '%'")
            .bind(&self.prefix)
            .execute(&self.admin)
            .await
            .unwrap();
    }

    async fn stops(&self) -> Vec<(String, String, String, Vec<String>, DateTime<Utc>)> {
        sqlx::query_as(
            "SELECT stop_id, name, search_name, modes, fetched_at FROM tfl_stops ORDER BY stop_id",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    async fn links(&self) -> Vec<(String, String, String)> {
        sqlx::query_as("SELECT stop_id, crs::text, source FROM tfl_stop_crs ORDER BY stop_id, crs")
            .fetch_all(&self.pool)
            .await
            .unwrap()
    }
}

fn at_millis(at: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(at.timestamp_millis()).unwrap()
}

fn stop(id: &str, name: &str, modes: &[&str]) -> TflStop {
    TflStop {
        id: id.to_owned(),
        name: name.to_owned(),
        modes: modes.iter().map(|m| (*m).to_owned()).collect(),
        lines: vec![TflStopLine {
            id: "testline".into(),
            name: "Testline".into(),
        }],
        hub_code: None,
        lat: Some(51.5),
        lon: Some(-0.1),
        crs: vec![],
        rail_tiplocs: vec![],
    }
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn a_snapshot_replaces_the_stops_and_their_rail_links() {
    let db = Db::new().await;
    db.setup().await;

    let mut hub = stop("940GZZTESTA", "King's Testbury", &["tube"]);
    hub.hub_code = Some("HUBZ9A".into());
    // The hub's own CRS (known to `stations`), one it does not know, and a
    // TIPLOC `tiploc_crs` resolves.
    hub.crs = vec!["Z9A".into(), "Z9Q".into()];
    hub.rail_tiplocs = vec!["ZZTSTTPL".into(), "ZZNOWHER".into()];
    let first = vec![
        hub,
        stop("940GZZTESTB", "Elephant & Testle", &["tube", "dlr"]),
        stop("940GZZTESTC", "Testmere", &["tram"]),
    ];
    let t1 = at_millis(Utc::now() - TimeDelta::hours(2));
    let entry = db.entry("one", t1, &first);
    db.applied(&entry).await;
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Duplicate)));

    let stops = db.stops().await;
    assert_eq!(stops.len(), 3);
    assert_eq!(stops[0].0, "940GZZTESTA");
    assert_eq!(stops[0].2, "kings testbury", "the search form");
    assert_eq!(stops[1].2, "elephant and testle");
    assert_eq!(stops[1].3, ["tube", "dlr"]);
    assert!(stops.iter().all(|row| row.4 == t1));
    assert_eq!(
        db.links().await,
        [
            ("940GZZTESTA".into(), "Z9A".into(), "hub".into()),
            ("940GZZTESTA".into(), "Z9B".into(), "tiploc".into()),
        ]
    );
    let lines: serde_json::Value =
        sqlx::query_scalar("SELECT lines FROM tfl_stops WHERE stop_id = '940GZZTESTA'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        lines,
        serde_json::json!([{"id": "testline", "name": "Testline"}])
    );
    let fresh: DateTime<Utc> =
        sqlx::query_scalar("SELECT fetched_at FROM ingest_freshness WHERE source = 'tfl-stops'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(fresh, t1);

    // The next day: C left TfL's data, B was renamed, A lost its hub.
    let t2 = t1 + TimeDelta::hours(1);
    let second = vec![
        stop("940GZZTESTA", "King's Testbury", &["tube"]),
        stop("940GZZTESTB", "Elephant and Testle", &["tube", "dlr"]),
        // Refused before writing, the rest written.
        stop("not-a-naptan-id", "Nowhere", &["tube"]),
    ];
    db.applied(&db.entry("two", t2, &second)).await;
    let stops = db.stops().await;
    let ids: Vec<&str> = stops.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(ids, ["940GZZTESTA", "940GZZTESTB"]);
    assert_eq!(stops[1].1, "Elephant and Testle");
    assert!(db.links().await.is_empty());

    // An older snapshot (a late redelivery) changes nothing.
    db.applied(&db.entry("old", t1, &first)).await;
    assert_eq!(db.stops().await.len(), 2);

    // An empty snapshot changes nothing either.
    db.applied(&db.entry("empty", t2 + TimeDelta::hours(1), &[]))
        .await;
    assert_eq!(db.stops().await.len(), 2);

    db.cleanup().await;
}
