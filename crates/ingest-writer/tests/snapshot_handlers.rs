//! The four snapshot handlers against a live database (plan 3a.6), through
//! [`WriterHandler`] in `apply` mode with the real registry. One test per
//! handler, each checking:
//!
//! - the entry is applied once (a redelivery is a `Duplicate`), writing the
//!   same rows as the api route's `ds-store` call for the same body;
//! - an older snapshot applied after a newer one changes nothing;
//! - a row time an hour in the future is clamped to the writer's
//!   `now() + 2 min`;
//! - `ingest_freshness` records the entry's `produced_at` and never moves
//!   back.
//!
//! They also run as the narrow writer role (no `DELETE` on these tables):
//! fixed test keys are reset with an `UPDATE` first, random keys are left
//! behind, and every delete is best effort.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code: a panic is the right failure in a test"
)]

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use common::{
    FullCoverageLineStatsRow, FullCoverageWindowCounts, FullCoverageWindowKind,
    FullCoverageWindowStatsRow, SampleStats, StationFullCoverageSample, StationSample,
};
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
    format!("{:012x}", hasher.finish() & 0xffff_ffff_ffff)
}

struct Db {
    pool: PgPool,
    run: String,
}

impl Db {
    async fn new() -> Self {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect");
        Self {
            pool,
            run: rand_suffix(),
        }
    }

    fn handler(&self) -> WriterHandler {
        WriterHandler::new(self.pool.clone(), Arc::new(registry()), Mode::Apply)
    }

    fn entry<T: Serialize + ?Sized>(
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
                format!("test-3a6-{}:{key}", self.run),
                produced_at,
                payload,
            )
            .unwrap(),
        }
    }

    async fn apply(&self, entry: &StreamEntry) -> Result<Handled, HandlerError> {
        self.handler().handle(entry).await
    }

    async fn freshness(&self, source: &str) -> Option<DateTime<Utc>> {
        sqlx::query_scalar("SELECT fetched_at FROM ingest_freshness WHERE source = $1")
            .bind(source)
            .fetch_optional(&self.pool)
            .await
            .unwrap()
    }

    /// Moves `source`'s freshness far back, so this run's times win.
    async fn reset_freshness(&self, source: &str) {
        sqlx::query("UPDATE ingest_freshness SET fetched_at = '2000-01-01Z' WHERE source = $1")
            .bind(source)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    /// Best effort: the writer role has no DELETE.
    async fn cleanup(&self, sql: &str, bind: &str) {
        let _ = sqlx::query(sql).bind(bind).execute(&self.pool).await;
        let _ = sqlx::query("DELETE FROM ingest_dedup WHERE key LIKE $1 || '%'")
            .bind(format!("test-3a6-{}:", self.run))
            .execute(&self.pool)
            .await;
    }
}

/// Millisecond precision, as the envelope carries `produced_at`.
fn now_ms() -> DateTime<Utc> {
    DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap()
}

fn assert_clamped(stored: DateTime<Utc>, before: DateTime<Utc>) {
    let after = Utc::now();
    assert!(
        stored >= before + TimeDelta::minutes(2) && stored <= after + TimeDelta::minutes(2),
        "{stored} is not clamped to now + 2 min ({before}..{after})"
    );
}

// ---------------------------------------------------------------------------
// station-samples/1

const SAMPLES: &str = "ds:ingest:station-samples";

/// One departure whose `delay_minutes` tells snapshots apart.
fn station_sample(crs: &str, polled_at: DateTime<Utc>, delay: &str) -> StationSample {
    StationSample {
        crs: crs.to_owned(),
        polled_at,
        departures: vec![
            serde_json::from_value(serde_json::json!({
                "service_id": "1234567PADTON__",
                "operator": "GW",
                "destination_crs": "RDG",
                "scheduled": "12:00",
                "estimated": "On time",
                "is_cancelled": false,
                "delay_minutes": delay.parse::<i32>().unwrap(),
            }))
            .unwrap(),
        ],
    }
}

type SampleRow = (String, DateTime<Utc>, serde_json::Value, Vec<String>);

async fn station_sample_rows(pool: &PgPool, codes: &[&str]) -> Vec<SampleRow> {
    sqlx::query_as(
        "SELECT crs::text, polled_at, departures, tiplocs FROM station_samples \
         WHERE crs = ANY($1::bpchar[]) ORDER BY crs",
    )
    .bind(codes)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn station_samples_apply_once_in_order_clamped_with_freshness() {
    let db = Db::new().await;
    let codes = ["ZQA", "ZQB"];
    // Fixed codes (CRS is three letters): move any earlier run's rows back.
    sqlx::query(
        "UPDATE station_samples SET polled_at = '2000-01-01Z' WHERE crs = ANY($1::bpchar[])",
    )
    .bind(&codes[..])
    .execute(&db.pool)
    .await
    .unwrap();
    db.reset_freshness("station-samples").await;

    let t1 = now_ms() - TimeDelta::minutes(10);
    let body = vec![
        station_sample("ZQA", t1, "1"),
        station_sample("ZQB", t1, "2"),
    ];

    // The same body through the api route's call, then through the writer:
    // the same rows.
    ds_store::samples::upsert_station_samples(&db.pool, &body)
        .await
        .unwrap();
    let via_route = station_sample_rows(&db.pool, &codes).await;
    sqlx::query(
        "UPDATE station_samples SET polled_at = '2000-01-01Z' WHERE crs = ANY($1::bpchar[])",
    )
    .bind(&codes[..])
    .execute(&db.pool)
    .await
    .unwrap();
    let entry = db.entry(SAMPLES, "station-samples", "t1", t1, &body);
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Applied)));
    assert_eq!(station_sample_rows(&db.pool, &codes).await, via_route);
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Duplicate)));
    assert_eq!(db.freshness("station-samples").await, Some(t1));

    // A newer snapshot, then an older one (reclaimed late): no change, and
    // freshness stays at the newer produced_at.
    let t2 = t1 + TimeDelta::minutes(5);
    let newer = db.entry(
        SAMPLES,
        "station-samples",
        "t2",
        t2,
        &[station_sample("ZQA", t2, "3")],
    );
    assert!(matches!(db.apply(&newer).await, Ok(Handled::Applied)));
    let after_newer = station_sample_rows(&db.pool, &codes).await;
    let older_at = t1 + TimeDelta::minutes(1);
    let older = db.entry(
        SAMPLES,
        "station-samples",
        "t1b",
        older_at,
        &[station_sample("ZQA", older_at, "9")],
    );
    assert!(matches!(db.apply(&older).await, Ok(Handled::Applied)));
    assert_eq!(station_sample_rows(&db.pool, &codes).await, after_newer);
    assert_eq!(after_newer[0].1, t2);
    assert_eq!(db.freshness("station-samples").await, Some(t2));

    // A row an hour in the future is clamped to the writer's now + 2 min.
    let before = Utc::now();
    let future = now_ms() + TimeDelta::hours(1);
    let ahead = db.entry(
        SAMPLES,
        "station-samples",
        "ahead",
        future,
        &[station_sample("ZQB", future, "4")],
    );
    assert!(matches!(db.apply(&ahead).await, Ok(Handled::Applied)));
    let rows = station_sample_rows(&db.pool, &codes).await;
    assert_clamped(rows[1].1, before);
    // Freshness is clamped too.
    assert_clamped(db.freshness("station-samples").await.unwrap(), before);

    db.cleanup(
        "DELETE FROM station_samples WHERE crs::text = ANY(string_to_array($1, ','))",
        "ZQA,ZQB",
    )
    .await;
}

/// A row the database refuses (a CRS too long for `bpchar(3)`, 22001) fails
/// the batch; the handler isolates it and the rest commit.
#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn a_refused_row_is_isolated_and_the_rest_of_the_snapshot_commits() {
    let db = Db::new().await;
    sqlx::query("UPDATE station_samples SET polled_at = '2000-01-01Z' WHERE crs = 'ZQD'")
        .execute(&db.pool)
        .await
        .unwrap();
    let at = now_ms();
    let entry = db.entry(
        SAMPLES,
        "station-samples",
        "bad",
        at,
        &[
            station_sample("ZQD", at, "1"),
            station_sample("ZQDXX", at, "2"),
        ],
    );
    let Ok(Handled::PartiallyRejected { reason, rejected }) = db.apply(&entry).await else {
        panic!("expected PartiallyRejected");
    };
    assert!(reason.contains("1 row(s) refused"), "{reason}");
    let rejected: Vec<StationSample> = serde_json::from_str(rejected.get()).unwrap();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].crs, "ZQDXX");
    assert_eq!(station_sample_rows(&db.pool, &["ZQD"]).await[0].1, at);
    db.cleanup("DELETE FROM station_samples WHERE crs::text = $1", "ZQD")
        .await;
}

// ---------------------------------------------------------------------------
// station-full-coverage-samples/1

const FULL_COVERAGE: &str = "ds:ingest:full-coverage";

fn fc_sample(
    operator: &str,
    resolved_at: DateTime<Utc>,
    total: usize,
) -> StationFullCoverageSample {
    StationFullCoverageSample {
        crs: "ZQC".to_owned(),
        operator: operator.to_owned(),
        resolved_at,
        stats: SampleStats {
            total,
            delayed: 1,
            cancelled: 0,
            skipped: 0,
            avg_delay_minutes: 1.5,
        },
    }
}

async fn fc_sample_rows(
    pool: &PgPool,
    operator: &str,
) -> Vec<(String, DateTime<Utc>, serde_json::Value)> {
    sqlx::query_as(
        "SELECT operator, resolved_at, stats FROM station_full_coverage_samples \
         WHERE crs = 'ZQC' AND operator LIKE $1 || '%' ORDER BY operator",
    )
    .bind(operator)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn station_full_coverage_samples_apply_once_in_order_clamped_with_freshness() {
    let db = Db::new().await;
    let source = "station-full-coverage-samples";
    db.reset_freshness(source).await;
    let route_op = format!("R{}", db.run);
    let op = format!("W{}", db.run);

    let t1 = now_ms() - TimeDelta::minutes(10);
    // The api route's call for one operator, the writer for another: the
    // same row apart from the key.
    ds_store::samples::upsert_station_full_coverage_samples(
        &db.pool,
        &[fc_sample(&route_op, t1, 5)],
    )
    .await
    .unwrap();
    let entry = db.entry(FULL_COVERAGE, source, "t1", t1, &[fc_sample(&op, t1, 5)]);
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Applied)));
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Duplicate)));
    let via_route = fc_sample_rows(&db.pool, &route_op).await;
    let via_writer = fc_sample_rows(&db.pool, &op).await;
    assert_eq!(via_route[0].1, via_writer[0].1);
    assert_eq!(via_route[0].2, via_writer[0].2);
    assert_eq!(db.freshness(source).await, Some(t1));

    let t2 = t1 + TimeDelta::minutes(5);
    let newer = db.entry(FULL_COVERAGE, source, "t2", t2, &[fc_sample(&op, t2, 6)]);
    assert!(matches!(db.apply(&newer).await, Ok(Handled::Applied)));
    let older_at = t1 + TimeDelta::minutes(1);
    let older = db.entry(
        FULL_COVERAGE,
        source,
        "t1b",
        older_at,
        &[fc_sample(&op, older_at, 99)],
    );
    assert!(matches!(db.apply(&older).await, Ok(Handled::Applied)));
    let rows = fc_sample_rows(&db.pool, &op).await;
    assert_eq!(rows[0].1, t2);
    assert_eq!(rows[0].2["total"], 6);
    assert_eq!(db.freshness(source).await, Some(t2));

    let before = Utc::now();
    let future = now_ms() + TimeDelta::hours(1);
    let ahead = db.entry(
        FULL_COVERAGE,
        source,
        "ahead",
        future,
        &[fc_sample(&op, future, 7)],
    );
    assert!(matches!(db.apply(&ahead).await, Ok(Handled::Applied)));
    assert_clamped(fc_sample_rows(&db.pool, &op).await[0].1, before);

    db.cleanup(
        "DELETE FROM station_full_coverage_samples WHERE crs = 'ZQC' AND operator LIKE '_' || $1",
        &db.run,
    )
    .await;
}

// ---------------------------------------------------------------------------
// full-coverage-stats/1: no row time, so source_updated_at := produced_at.

fn line_row(line_id: &str, total: usize) -> FullCoverageLineStatsRow {
    FullCoverageLineStatsRow {
        line_id: line_id.to_owned(),
        service_date: NaiveDate::from_ymd_opt(2099, 2, 1).unwrap(),
        availability: "pending".to_owned(),
        stats: SampleStats {
            total,
            delayed: 2,
            cancelled: 1,
            skipped: 0,
            avg_delay_minutes: 3.5,
        },
        partial: false,
        breakdown: None,
        stats_version: None,
    }
}

type LineRow = (i32, i32, String, i16, DateTime<Utc>, Option<DateTime<Utc>>);

async fn line_rows(pool: &PgPool, line_id: &str) -> Vec<LineRow> {
    sqlx::query_as(
        "SELECT total, delayed, availability, stats_version, updated_at, source_updated_at \
         FROM full_coverage_line_stats WHERE line_id = $1",
    )
    .bind(line_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn full_coverage_stats_apply_once_in_order_clamped_with_freshness() {
    let db = Db::new().await;
    let source = "full-coverage-stats";
    db.reset_freshness(source).await;
    let route_line = format!("test-3a6-route-{}", db.run);
    let line = format!("test-3a6-{}", db.run);

    ds_store::samples::upsert_full_coverage_line_stats(&db.pool, &[line_row(&route_line, 5)])
        .await
        .unwrap();
    let t1 = now_ms() - TimeDelta::minutes(10);
    let entry = db.entry(FULL_COVERAGE, source, "t1", t1, &[line_row(&line, 5)]);
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Applied)));
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Duplicate)));
    let via_route = line_rows(&db.pool, &route_line).await;
    let via_writer = line_rows(&db.pool, &line).await;
    // The same stats columns.
    assert_eq!(
        (
            via_route[0].0,
            via_route[0].1,
            &via_route[0].2,
            via_route[0].3
        ),
        (
            via_writer[0].0,
            via_writer[0].1,
            &via_writer[0].2,
            via_writer[0].3
        )
    );
    // The route leaves source_updated_at NULL; the writer stamps produced_at.
    assert_eq!(via_route[0].5, None);
    assert_eq!(via_writer[0].5, Some(t1));
    assert_eq!(db.freshness(source).await, Some(t1));

    // Unchanged stats, newer snapshot: source_updated_at advances, updated_at
    // ("last changed") does not.
    let t2 = t1 + TimeDelta::minutes(5);
    let same = db.entry(FULL_COVERAGE, source, "t2", t2, &[line_row(&line, 5)]);
    assert!(matches!(db.apply(&same).await, Ok(Handled::Applied)));
    let rows = line_rows(&db.pool, &line).await;
    assert_eq!(rows[0].5, Some(t2));
    assert_eq!(rows[0].4, via_writer[0].4);

    // An older snapshot with different stats changes nothing.
    let older_at = t1 + TimeDelta::minutes(1);
    let older = db.entry(
        FULL_COVERAGE,
        source,
        "t1b",
        older_at,
        &[line_row(&line, 99)],
    );
    assert!(matches!(db.apply(&older).await, Ok(Handled::Applied)));
    assert_eq!(line_rows(&db.pool, &line).await, rows);
    assert_eq!(db.freshness(source).await, Some(t2));

    // A produced_at an hour ahead is clamped; a row stamped in the future
    // is then overwritten by the next real snapshot (the healing arm).
    let before = Utc::now();
    let ahead = db.entry(
        FULL_COVERAGE,
        source,
        "ahead",
        now_ms() + TimeDelta::hours(1),
        &[line_row(&line, 7)],
    );
    assert!(matches!(db.apply(&ahead).await, Ok(Handled::Applied)));
    let rows = line_rows(&db.pool, &line).await;
    assert_eq!(rows[0].0, 7);
    assert_clamped(rows[0].5.unwrap(), before);
    assert_clamped(db.freshness(source).await.unwrap(), before);

    db.cleanup(
        "DELETE FROM full_coverage_line_stats WHERE line_id LIKE '%' || $1",
        &db.run,
    )
    .await;
}

// ---------------------------------------------------------------------------
// full-coverage-window-stats/1, validated like the route.

fn window_row(line_id: &str, computed_at: DateTime<Utc>, total: u32) -> FullCoverageWindowStatsRow {
    FullCoverageWindowStatsRow {
        line_id: line_id.to_owned(),
        window_kind: FullCoverageWindowKind::Recent,
        service_date: NaiveDate::from_ymd_opt(2099, 2, 1).unwrap(),
        window_start: computed_at - TimeDelta::hours(1),
        window_end: computed_at,
        computed_at,
        counts: FullCoverageWindowCounts {
            total,
            on_time: total,
            ..Default::default()
        },
        relevance: "full".to_owned(),
        presumed_enabled: true,
        partial: false,
        feed_stale: false,
        stats_version: 2,
    }
}

async fn window_rows(pool: &PgPool, line_id: &str) -> Vec<(DateTime<Utc>, i32, DateTime<Utc>)> {
    sqlx::query_as(
        "SELECT computed_at, total, bucket_start FROM full_coverage_line_window_stats \
         WHERE line_id = $1 ORDER BY bucket_start",
    )
    .bind(line_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn full_coverage_window_stats_apply_once_in_order_clamped_with_freshness() {
    let db = Db::new().await;
    let source = "full-coverage-window-stats";
    db.reset_freshness(source).await;
    let route_line = format!("test-3a6-route-{}", db.run);
    let line = format!("test-3a6-{}", db.run);

    // Inside one 15-minute bucket, so later snapshots hit the same row.
    let bucket =
        ds_store::samples::full_coverage_window::bucket_start(now_ms() - TimeDelta::hours(1));
    let t1 = bucket + TimeDelta::minutes(2);
    ds_store::samples::full_coverage_window::upsert_full_coverage_window_stats(
        &db.pool,
        &[window_row(&route_line, t1, 5)],
    )
    .await
    .unwrap();
    let entry = db.entry(FULL_COVERAGE, source, "t1", t1, &[window_row(&line, t1, 5)]);
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Applied)));
    assert!(matches!(db.apply(&entry).await, Ok(Handled::Duplicate)));
    assert_eq!(
        window_rows(&db.pool, &route_line).await,
        window_rows(&db.pool, &line).await
    );
    assert_eq!(db.freshness(source).await, Some(t1));

    let t2 = bucket + TimeDelta::minutes(10);
    let newer = db.entry(FULL_COVERAGE, source, "t2", t2, &[window_row(&line, t2, 6)]);
    assert!(matches!(db.apply(&newer).await, Ok(Handled::Applied)));
    let older_at = bucket + TimeDelta::minutes(5);
    let older = db.entry(
        FULL_COVERAGE,
        source,
        "t1b",
        older_at,
        &[window_row(&line, older_at, 99)],
    );
    assert!(matches!(db.apply(&older).await, Ok(Handled::Applied)));
    assert_eq!(window_rows(&db.pool, &line).await, [(t2, 6, bucket)]);
    assert_eq!(db.freshness(source).await, Some(t2));

    // An invalid row is the whole entry's fault, as it is a 400 for the route.
    let mut bad = window_row(&line, t2, 1);
    bad.relevance = "everything".to_owned();
    let invalid = db.entry(FULL_COVERAGE, source, "bad", t2, &[bad]);
    assert!(matches!(
        db.apply(&invalid).await,
        Err(HandlerError::Poison(_))
    ));

    let before = Utc::now();
    let future = now_ms() + TimeDelta::hours(1);
    let ahead = db.entry(
        FULL_COVERAGE,
        source,
        "ahead",
        future,
        &[window_row(&line, future, 7)],
    );
    assert!(matches!(db.apply(&ahead).await, Ok(Handled::Applied)));
    let rows = window_rows(&db.pool, &line).await;
    let (computed_at, total, _) = rows.last().copied().unwrap();
    assert_eq!(total, 7);
    assert_clamped(computed_at, before);

    db.cleanup(
        "DELETE FROM full_coverage_line_window_stats WHERE line_id LIKE '%' || $1",
        &db.run,
    )
    .await;
}

// ---------------------------------------------------------------------------
// Plan 3a.9: changed rows only (INGEST_WRITER_CHANGED_ROWS_ONLY).

fn changed_rows_only() -> ingest_writer::handlers::snapshots::Options {
    ingest_writer::handlers::snapshots::Options {
        changed_rows_only: true,
    }
}

impl Db {
    async fn apply_with(
        &self,
        options: ingest_writer::handlers::snapshots::Options,
        entry: &StreamEntry,
    ) -> Result<Handled, HandlerError> {
        WriterHandler::new(
            self.pool.clone(),
            Arc::new(ingest_writer::handlers::registry_with(options)),
            Mode::Apply,
        )
        .handle(entry)
        .await
    }
}

fn fc_row(
    crs: &str,
    operator: &str,
    resolved_at: DateTime<Utc>,
    total: usize,
) -> StationFullCoverageSample {
    StationFullCoverageSample {
        crs: crs.to_owned(),
        ..fc_sample(operator, resolved_at, total)
    }
}

type FcState = (String, String, DateTime<Utc>, DateTime<Utc>, i64);

/// `(operator, xmin, resolved_at, derived resolved_at, total)` for `crs`'s
/// rows of this run, the derived time as every reader reads it.
async fn fc_state(pool: &PgPool, crs: &str, run: &str) -> Vec<FcState> {
    sqlx::query_as(&format!(
        "SELECT operator, xmin::text, resolved_at, {}, (stats->>'total')::int8 \
         FROM station_full_coverage_samples WHERE crs = $1 AND operator LIKE '_' || $2 \
         ORDER BY operator",
        ds_store::samples::STATION_FULL_COVERAGE_RESOLVED_AT_SQL
    ))
    .bind(crs)
    .bind(run)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Rows the writer counted as `outcome` for station-full-coverage-samples.
fn row_writes(rendered: &str, outcome: &str) -> u64 {
    rendered
        .lines()
        .filter(|line| {
            line.starts_with("distant_signal_ingest_stream_row_writes_total{")
                && line.contains("schema=\"station-full-coverage-samples\"")
                && line.contains(&format!("outcome=\"{outcome}\""))
        })
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

/// With the switch on: an identical snapshot writes nothing (`xmin`
/// unchanged) while the readers' derived `resolved_at` follows the feed; a
/// changed row is still written; an older snapshot redelivered after a
/// skipped newer one changes nothing; the skipped and written rows are
/// counted.
#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn changed_rows_only_skips_unchanged_station_full_coverage_rows() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let metrics = recorder.handle();
    let installed = metrics::set_global_recorder(recorder).is_ok();

    let db = Db::new().await;
    let source = "station-full-coverage-samples";
    db.reset_freshness(source).await;
    let crs = "ZQE";
    let (a, b) = (format!("A{}", db.run), format!("B{}", db.run));
    let on = changed_rows_only();

    // Old row times: what a row the writer keeps skipping looks like.
    let t1 = now_ms() - TimeDelta::minutes(30);
    let first = db.entry(
        FULL_COVERAGE,
        source,
        "t1",
        t1,
        &[fc_row(crs, &a, t1, 5), fc_row(crs, &b, t1, 7)],
    );
    assert!(matches!(
        db.apply_with(on, &first).await,
        Ok(Handled::Applied)
    ));
    let inserted = fc_state(&db.pool, crs, &db.run).await;
    assert_eq!(inserted.len(), 2);

    // The same stats, recent feed time: zero rows written, the row time
    // stays old, and the readers report the feed's time.
    let t2 = now_ms() - TimeDelta::seconds(5);
    let same = db.entry(
        FULL_COVERAGE,
        source,
        "t2",
        t2,
        &[fc_row(crs, &a, t2, 5), fc_row(crs, &b, t2, 7)],
    );
    assert!(matches!(
        db.apply_with(on, &same).await,
        Ok(Handled::Applied)
    ));
    let unchanged = fc_state(&db.pool, crs, &db.run).await;
    for (before, after) in inserted.iter().zip(&unchanged) {
        assert_eq!(after.1, before.1, "xmin: {} was rewritten", after.0);
        assert_eq!(after.2, t1, "the row's own time is not bumped");
        assert_eq!(after.3, t2, "the derived time is the feed's");
    }
    assert_eq!(db.freshness(source).await, Some(t2));
    let fetched = ds_store::freshness::last_station_full_coverage_samples_fetch(&db.pool)
        .await
        .unwrap()
        .unwrap();
    assert!(
        fetched >= t2,
        "the freshness read follows the feed: {fetched}"
    );

    // One changed row: written, the other still skipped.
    let t3 = now_ms();
    let changed = db.entry(
        FULL_COVERAGE,
        source,
        "t3",
        t3,
        &[fc_row(crs, &a, t3, 6), fc_row(crs, &b, t3, 7)],
    );
    assert!(matches!(
        db.apply_with(on, &changed).await,
        Ok(Handled::Applied)
    ));
    let after_change = fc_state(&db.pool, crs, &db.run).await;
    assert_ne!(after_change[0].1, unchanged[0].1, "A changed: written");
    assert_eq!((after_change[0].2, after_change[0].4), (t3, 6));
    assert_eq!(after_change[1].1, unchanged[1].1, "B unchanged: skipped");
    assert_eq!((after_change[1].2, after_change[1].3), (t1, t3));

    // An older snapshot redelivered after the skipped newer ones: B's own
    // time (t1) is older than it, but the derived time (t3) is not, so
    // nothing changes.
    let older_at = t2 + TimeDelta::milliseconds(1);
    let older = db.entry(
        FULL_COVERAGE,
        source,
        "t2b",
        older_at,
        &[fc_row(crs, &a, older_at, 99), fc_row(crs, &b, older_at, 99)],
    );
    assert!(matches!(
        db.apply_with(on, &older).await,
        Ok(Handled::Applied)
    ));
    assert_eq!(fc_state(&db.pool, crs, &db.run).await, after_change);
    assert_eq!(db.freshness(source).await, Some(t3));

    if installed {
        let rendered = metrics.render();
        // t1: 2 written; t2: 2 skipped; t3: 1 and 1; t2b: 2 skipped.
        assert_eq!(row_writes(&rendered, "written"), 3, "{rendered}");
        assert_eq!(row_writes(&rendered, "skipped"), 5, "{rendered}");
    }

    db.cleanup(
        "DELETE FROM station_full_coverage_samples WHERE crs = 'ZQE' AND operator LIKE '_' || $1",
        &db.run,
    )
    .await;
}

/// With the switch off (the default), the writer is today's: an identical
/// snapshot still advances every row's `resolved_at`, as the api route
/// does, and the derived time equals the row's own.
#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn changed_rows_only_off_keeps_the_per_row_bump() {
    let db = Db::new().await;
    let source = "station-full-coverage-samples";
    db.reset_freshness(source).await;
    let crs = "ZQF";
    let (writer_op, route_op) = (format!("W{}", db.run), format!("R{}", db.run));
    let off = ingest_writer::handlers::snapshots::Options::default();

    let t1 = now_ms() - TimeDelta::minutes(30);
    let t2 = now_ms();
    for (key, at) in [("t1", t1), ("t2", t2)] {
        let entry = db.entry(
            FULL_COVERAGE,
            source,
            key,
            at,
            &[fc_row(crs, &writer_op, at, 5)],
        );
        assert!(matches!(
            db.apply_with(off, &entry).await,
            Ok(Handled::Applied)
        ));
        ds_store::samples::upsert_station_full_coverage_samples(
            &db.pool,
            &[fc_row(crs, &route_op, at, 5)],
        )
        .await
        .unwrap();
    }
    let rows = fc_state(&db.pool, crs, &db.run).await;
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!((row.2, row.3, row.4), (t2, t2, 5), "{}", row.0);
    }

    db.cleanup(
        "DELETE FROM station_full_coverage_samples WHERE crs = 'ZQF' AND operator LIKE '_' || $1",
        &db.run,
    )
    .await;
}

/// The tables that fail plan 3a.9's every-key check keep their per-row
/// time with the switch on: an identical snapshot still advances
/// `station_samples.polled_at`, `full_coverage_line_window_stats.computed_at`
/// and `full_coverage_line_stats.source_updated_at`.
#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn changed_rows_only_leaves_the_other_tables_per_row() {
    let db = Db::new().await;
    for source in [
        "station-samples",
        "full-coverage-window-stats",
        "full-coverage-stats",
    ] {
        db.reset_freshness(source).await;
    }
    let on = changed_rows_only();
    let line = format!("test-3a9-{}", db.run);
    sqlx::query("UPDATE station_samples SET polled_at = '2000-01-01Z' WHERE crs = 'ZQG'")
        .execute(&db.pool)
        .await
        .unwrap();

    let bucket =
        ds_store::samples::full_coverage_window::bucket_start(now_ms() - TimeDelta::hours(1));
    let t1 = bucket + TimeDelta::minutes(2);
    let t2 = bucket + TimeDelta::minutes(3);
    for (key, at) in [("t1", t1), ("t2", t2)] {
        let samples = db.entry(
            SAMPLES,
            "station-samples",
            &format!("s{key}"),
            at,
            &[station_sample("ZQG", at, "1")],
        );
        // The same counts; window_end moves with computed_at, as it does
        // in production.
        let windows = db.entry(
            FULL_COVERAGE,
            "full-coverage-window-stats",
            &format!("w{key}"),
            at,
            &[window_row(&line, at, 5)],
        );
        let stats = db.entry(
            FULL_COVERAGE,
            "full-coverage-stats",
            &format!("l{key}"),
            at,
            &[line_row(&line, 5)],
        );
        for entry in [&samples, &windows, &stats] {
            assert!(matches!(
                db.apply_with(on, entry).await,
                Ok(Handled::Applied)
            ));
        }
    }
    assert_eq!(station_sample_rows(&db.pool, &["ZQG"]).await[0].1, t2);
    assert_eq!(window_rows(&db.pool, &line).await, [(t2, 5, bucket)]);
    assert_eq!(line_rows(&db.pool, &line).await[0].5, Some(t2));

    db.cleanup("DELETE FROM station_samples WHERE crs::text = $1", "ZQG")
        .await;
    db.cleanup(
        "DELETE FROM full_coverage_line_window_stats WHERE line_id LIKE '%' || $1",
        &db.run,
    )
    .await;
    db.cleanup(
        "DELETE FROM full_coverage_line_stats WHERE line_id LIKE '%' || $1",
        &db.run,
    )
    .await;
}
