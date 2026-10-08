//! Plan 3c.1's handlers against a live database (`DATABASE_URL`, a
//! migrated database; `cargo test -p ingest-writer --test phase3c_handlers
//! -- --ignored --test-threads=1`), through [`WriterHandler`] in `apply`
//! mode with the writer's real [`registry`]: `tfl-line-status/1`, `tocs/1`
//! and the three island-of-Ireland schemas.
//!
//! CI also runs them as the writer's own role (the per-service step), where
//! `line_status`'s row policy (plan 3c.3) applies: they only write `TfL`
//! rows, and the one that seeds an aggregator row checks the policy's
//! refusal instead when it cannot.
//!
//! Fixture keys: line ids `TEST-3C-*`, TOC `9Z` (`atoc_code` is two
//! characters; no real ATOC code starts with a digit), island-of-Ireland
//! ids `TEST-3C-*`; `ingest_dedup` keys carry a random prefix. Note that a
//! `TfL` snapshot prunes every other `TfL` line, as on the api's route.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code: a panic is the right failure in a test"
)]

use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use common::island_of_ireland::{
    IslandOfIrelandLineDefinition, IslandOfIrelandNetwork, IslandOfIrelandStation,
    IslandOfIrelandStationSample,
};
use common::{LineStatus, LineStatusReport, TocReference};
use ingest_stream::{Envelope, Handled, Handler, HandlerError, SchemaId, StreamEntry};
use ingest_writer::handlers::registry;
use ingest_writer::stream::{Mode, WriterHandler};
use serde::Serialize;
use sqlx::PgPool;

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
    handler: WriterHandler,
}

impl Db {
    async fn new() -> Self {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect");
        let handler = WriterHandler::new(pool.clone(), Arc::new(registry()), Mode::Apply);
        Self {
            pool,
            prefix: format!("p3c-test-{}:", rand_suffix()),
            handler,
        }
    }

    fn entry<T: Serialize>(
        &self,
        stream: &str,
        schema: &str,
        key: &str,
        produced_at: DateTime<Utc>,
        payload: &T,
    ) -> StreamEntry {
        StreamEntry {
            stream: stream.to_owned(),
            id: "1-0".to_owned(),
            envelope: Envelope::new(
                SchemaId::new(schema, 1).unwrap(),
                "test/pod",
                format!("{}{key}", self.prefix),
                produced_at,
                payload,
            )
            .unwrap(),
        }
    }

    async fn apply(&self, entry: &StreamEntry) -> Result<Handled, HandlerError> {
        self.handler.handle(entry).await
    }

    /// Applies `entry`, which must be [`Handled::Applied`].
    async fn applied(&self, entry: &StreamEntry) {
        let result = self.apply(entry).await;
        assert!(
            matches!(result, Ok(Handled::Applied)),
            "{}: {result:?}",
            entry.envelope.key
        );
    }

    /// Moves `source`'s freshness marker far into the past, so the marker
    /// a test then writes is its own (the marker never moves backwards).
    async fn age_freshness(&self, source: &str) {
        sqlx::query(
            "UPDATE ingest_freshness SET fetched_at = '2000-01-01T00:00:00Z' WHERE source = $1",
        )
        .bind(source)
        .execute(&self.pool)
        .await
        .unwrap();
    }

    async fn freshness(&self, source: &str) -> Option<DateTime<Utc>> {
        sqlx::query_scalar("SELECT fetched_at FROM ingest_freshness WHERE source = $1")
            .bind(source)
            .fetch_optional(&self.pool)
            .await
            .unwrap()
    }

    async fn cleanup(&self) {
        for sql in [
            "DELETE FROM ingest_dedup WHERE key LIKE $1 || '%'",
            "DELETE FROM line_status_history WHERE line_id LIKE 'TEST-3C-%' AND $1 <> ''",
            "DELETE FROM line_status WHERE line_id LIKE 'TEST-3C-%' AND $1 <> ''",
            "DELETE FROM tocs WHERE atoc_code = '9Z' AND $1 <> ''",
            "DELETE FROM island_of_ireland_stations WHERE id LIKE 'TEST-3C-%' AND $1 <> ''",
            "DELETE FROM island_of_ireland_lines WHERE id LIKE 'TEST-3C-%' AND $1 <> ''",
            "DELETE FROM island_of_ireland_station_samples WHERE station_id LIKE 'TEST-3C-%' AND $1 <> ''",
        ] {
            sqlx::query(sql)
                .bind(&self.prefix)
                .execute(&self.pool)
                .await
                .unwrap_or_else(|err| panic!("{sql}: {err}"));
        }
    }
}

/// The envelope's precision: milliseconds.
fn at_millis(at: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(at.timestamp_millis()).unwrap()
}

fn statuses(severity: u8) -> Vec<LineStatus> {
    serde_json::from_value(serde_json::json!([{
        "severity": severity,
        "reason": format!("severity {severity}"),
        "validity": { "from_date": "2026-10-08T02:00:00Z", "to_date": null, "is_now": true },
        "data_quality": "tfl"
    }]))
    .unwrap()
}

fn report(id: &str, severity: u8) -> LineStatusReport {
    LineStatusReport {
        id: id.to_owned(),
        name: format!("{id} name"),
        mode_name: "tube".to_owned(),
        operators: vec!["TfL".to_owned()],
        statuses: statuses(severity),
    }
}

/// `(computed_at, source_updated_at, severity)` of a line.
async fn line(db: &Db, id: &str) -> Option<(DateTime<Utc>, Option<DateTime<Utc>>, String)> {
    sqlx::query_as(
        "SELECT computed_at, source_updated_at, statuses->0->>'severity' \
         FROM line_status WHERE line_id = $1",
    )
    .bind(id)
    .fetch_optional(&db.pool)
    .await
    .unwrap()
}

async fn history(db: &Db, id: &str) -> Vec<DateTime<Utc>> {
    sqlx::query_scalar("SELECT computed_at FROM line_status_history WHERE line_id = $1 ORDER BY id")
        .bind(id)
        .fetch_all(&db.pool)
        .await
        .unwrap()
}

fn tfl_entry(db: &Db, key: &str, at: DateTime<Utc>, reports: &[LineStatusReport]) -> StreamEntry {
    db.entry("ds:ingest:tfl", "tfl-line-status", key, at, &reports)
}

/// Plan 3c.1 (D13): a snapshot applied an hour late stamps
/// `line_status.computed_at`, `source_updated_at`, the history row and the
/// freshness marker with its `produced_at`.
#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn tfl_snapshot_applied_late_is_stamped_with_its_produced_at() {
    let db = Db::new().await;
    db.cleanup().await;
    db.age_freshness("tfl").await;
    let produced_at = at_millis(Utc::now() - TimeDelta::hours(1));
    let entry = tfl_entry(&db, "late", produced_at, &[report("TEST-3C-LATE", 10)]);

    db.applied(&entry).await;

    let (computed_at, source_updated_at, _) = line(&db, "TEST-3C-LATE").await.unwrap();
    assert_eq!(computed_at, produced_at);
    assert_eq!(source_updated_at, Some(produced_at));
    assert_eq!(history(&db, "TEST-3C-LATE").await, vec![produced_at]);
    assert_eq!(db.freshness("tfl").await, Some(produced_at));

    // A redelivery is a duplicate and writes nothing.
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Duplicate)));
    assert_eq!(history(&db, "TEST-3C-LATE").await.len(), 1);
    db.cleanup().await;
}

/// Plan 3c.1: an older snapshot after a newer one changes neither table (and
/// does not prune the newer snapshot's lines, or move freshness back); it is
/// applied, not an error. A newer one after that writes again.
#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn tfl_older_snapshot_after_a_newer_one_changes_nothing() {
    let db = Db::new().await;
    db.cleanup().await;
    db.age_freshness("tfl").await;
    let t0 = at_millis(Utc::now() - TimeDelta::minutes(5));
    let newer = tfl_entry(
        &db,
        "newer",
        t0,
        &[report("TEST-3C-A", 10), report("TEST-3C-B", 10)],
    );
    db.applied(&newer).await;
    let a_before = line(&db, "TEST-3C-A").await.unwrap();

    // Older, with a different status for A and without B.
    let older = tfl_entry(
        &db,
        "older",
        t0 - TimeDelta::minutes(10),
        &[report("TEST-3C-A", 6)],
    );
    db.applied(&older).await;
    assert_eq!(line(&db, "TEST-3C-A").await.unwrap(), a_before);
    assert!(line(&db, "TEST-3C-B").await.is_some(), "B is not pruned");
    assert_eq!(history(&db, "TEST-3C-A").await, vec![t0]);
    assert_eq!(db.freshness("tfl").await, Some(t0));

    // Newer again: A changes (a history row) and B, absent, is pruned.
    let t1 = t0 + TimeDelta::minutes(2);
    let newest = tfl_entry(&db, "newest", t1, &[report("TEST-3C-A", 6)]);
    db.applied(&newest).await;
    let (computed_at, _, severity) = line(&db, "TEST-3C-A").await.unwrap();
    assert_eq!((computed_at, severity.as_str()), (t1, "6"));
    assert_eq!(history(&db, "TEST-3C-A").await, vec![t0, t1]);
    assert!(line(&db, "TEST-3C-B").await.is_none(), "B left the feed");
    assert_eq!(db.freshness("tfl").await, Some(t1));
    db.cleanup().await;
}

/// One part of a split snapshot does not prune the lines it lacks.
#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn tfl_a_part_of_a_split_snapshot_prunes_nothing() {
    let db = Db::new().await;
    db.cleanup().await;
    let t0 = at_millis(Utc::now() - TimeDelta::minutes(3));
    let whole = tfl_entry(
        &db,
        "whole",
        t0,
        &[report("TEST-3C-P1", 10), report("TEST-3C-P2", 10)],
    );
    db.applied(&whole).await;
    let mut part = tfl_entry(
        &db,
        "part",
        t0 + TimeDelta::minutes(1),
        &[report("TEST-3C-P1", 10)],
    );
    part.envelope = part.envelope.with_batch(ingest_stream::BatchPart {
        batch: "b".into(),
        part: 1,
        parts: 2,
    });
    db.applied(&part).await;
    assert!(line(&db, "TEST-3C-P2").await.is_some());
    db.cleanup().await;
}

/// A `TfL` line id owned by the aggregator: the whole entry is poison (as
/// the api's route refuses it) and the aggregator's row is untouched. As
/// the writer's role, the row policy refuses the aggregator row's seed
/// itself (plan 3c.3), which is checked instead.
#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn tfl_a_line_owned_by_the_aggregator_is_poison() {
    let db = Db::new().await;
    db.cleanup().await;
    let seeded = sqlx::query(
        "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
         VALUES ('TEST-3C-AGG', 'aggregator owns this', 'national-rail', '{NT}', '[]', 'aggregator')",
    )
    .execute(&db.pool)
    .await;
    if let Err(err) = seeded {
        let code = err
            .as_database_error()
            .and_then(|db_err| db_err.code().map(|c| c.into_owned()));
        assert_eq!(
            code.as_deref(),
            Some("42501"),
            "only the writer role's row policy may refuse the seed: {err}"
        );
        eprintln!("running as the writer role: the row policy refused the aggregator seed");
        db.cleanup().await;
        return;
    }
    let entry = tfl_entry(&db, "steal", Utc::now(), &[report("TEST-3C-AGG", 10)]);
    assert!(matches!(
        db.apply(&entry).await,
        Err(HandlerError::Poison(_))
    ));
    let (source, name): (String, String) =
        sqlx::query_as("SELECT source, name FROM line_status WHERE line_id = 'TEST-3C-AGG'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        (source.as_str(), name.as_str()),
        ("aggregator", "aggregator owns this")
    );
    db.cleanup().await;
}

/// `tocs/1`: written once (a redelivery is a duplicate), with the freshness
/// marker at `produced_at`.
#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn tocs_are_applied_once_with_freshness_at_produced_at() {
    let db = Db::new().await;
    db.cleanup().await;
    db.age_freshness("tocs").await;
    let produced_at = at_millis(Utc::now() - TimeDelta::minutes(30));
    let tocs = vec![TocReference {
        atoc_code: "9Z".into(),
        name: "Test 3c Trains".into(),
        legal_name: "Test 3c Trains Ltd".into(),
        atoc_member: Some(true),
        station_operator: None,
    }];
    let entry = db.entry("ds:ingest:reference", "tocs", "tocs", produced_at, &tocs);
    db.applied(&entry).await;
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Duplicate)));
    let (name, fetched_at): (String, DateTime<Utc>) =
        sqlx::query_as("SELECT name, fetched_at FROM tocs WHERE atoc_code = '9Z'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(name, "Test 3c Trains");
    assert_eq!(fetched_at, produced_at);
    assert_eq!(db.freshness("tocs").await, Some(produced_at));
    db.cleanup().await;
}

/// The three island-of-Ireland schemas: written, stamped with the observed
/// time, and an older sample never overwrites a newer one.
#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn island_of_ireland_schemas_are_applied_under_the_guard() {
    let db = Db::new().await;
    db.cleanup().await;
    db.age_freshness("island_of_ireland_stations").await;
    let stream = "ds:ingest:island-of-ireland";
    let t0 = at_millis(Utc::now() - TimeDelta::minutes(20));
    let station = |name: &str| IslandOfIrelandStation {
        id: "TEST-3C-STN".into(),
        name: name.into(),
        network: IslandOfIrelandNetwork::NorthernIreland,
        latitude: Some(54.6),
        longitude: Some(-5.9),
    };
    let new_stations = db.entry(stream, "ioi-stations", "stn-new", t0, &[station("New")]);
    assert!(matches!(
        db.apply(&new_stations).await,
        Ok(Handled::Applied)
    ));
    let old_stations = db.entry(
        stream,
        "ioi-stations",
        "stn-old",
        t0 - TimeDelta::minutes(5),
        &[station("Old")],
    );
    assert!(matches!(
        db.apply(&old_stations).await,
        Ok(Handled::Applied)
    ));
    let (name, fetched_at): (String, DateTime<Utc>) = sqlx::query_as(
        "SELECT name, fetched_at FROM island_of_ireland_stations WHERE id = 'TEST-3C-STN'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!((name.as_str(), fetched_at), ("New", t0));
    assert_eq!(db.freshness("island_of_ireland_stations").await, Some(t0));

    let lines = [IslandOfIrelandLineDefinition {
        id: "TEST-3C-LINE".into(),
        name: "Test line".into(),
        network: IslandOfIrelandNetwork::NorthernIreland,
        stations: vec!["TEST-3C-STN".into()],
    }];
    let lines_entry = db.entry(stream, "ioi-lines", "lines", t0, &lines);
    db.applied(&lines_entry).await;
    let line_fetched: DateTime<Utc> = sqlx::query_scalar(
        "SELECT fetched_at FROM island_of_ireland_lines WHERE id = 'TEST-3C-LINE'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(line_fetched, t0);

    let sample = |polled_at: DateTime<Utc>| IslandOfIrelandStationSample {
        station_id: "TEST-3C-STN".into(),
        network: IslandOfIrelandNetwork::NorthernIreland,
        polled_at,
        departures: vec![],
    };
    let newer = db.entry(stream, "ioi-station-samples", "s-new", t0, &[sample(t0)]);
    db.applied(&newer).await;
    let older = db.entry(
        stream,
        "ioi-station-samples",
        "s-old",
        t0,
        &[sample(t0 - TimeDelta::minutes(1))],
    );
    db.applied(&older).await;
    // A sample stamped an hour ahead is clamped to the writer's now + 2 min.
    let ahead = db.entry(
        stream,
        "ioi-station-samples",
        "s-ahead",
        t0,
        &[sample(Utc::now() + TimeDelta::hours(1))],
    );
    let polled_at = |db: &Db| {
        let pool = db.pool.clone();
        async move {
            sqlx::query_scalar::<_, DateTime<Utc>>(
                "SELECT polled_at FROM island_of_ireland_station_samples \
                 WHERE station_id = 'TEST-3C-STN'",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    assert_eq!(polled_at(&db).await, t0, "the older sample changed nothing");
    db.applied(&ahead).await;
    assert!(polled_at(&db).await <= Utc::now() + TimeDelta::minutes(2));
    db.cleanup().await;
}
