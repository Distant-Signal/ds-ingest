//! `darwin-pushport/1` against a live database (`DATABASE_URL`, a migrated
//! database; `cargo test -p ingest-writer --test darwin_handler --
//! --ignored --test-threads=1`), through [`WriterHandler`] in `apply` mode
//! with the writer's real [`registry`] (DS-152). CI also runs it as the
//! writer's own role (the per-service step), which proves the grants and
//! the `darwin-pushport` freshness row policy.
//!
//! Fixture RIDs carry `8Z8` and dates in 2099, so the cleanup touches no
//! real row.

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure in a test"
)]

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use common::darwin::{Batch, CallKey, Platform, Service, StatusCall};
use ingest_stream::{Envelope, Handled, Handler, SchemaId, StreamEntry};
use ingest_writer::handlers::registry;
use ingest_writer::stream::{StreamSpec, WriterHandler};
use sqlx::PgPool;

const STREAM: &str = "ds:ingest:darwin";
const RID: &str = "209907118Z80001";

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .unwrap()
        .with_timezone(&Utc)
}

fn batch(at: &str, platform: &str) -> Batch {
    let ssd = NaiveDate::from_ymd_opt(2099, 7, 11).unwrap();
    Batch {
        services: vec![Service {
            rid: RID.to_owned(),
            uid: "Z80001".to_owned(),
            ssd,
            schedule: None,
            ts: ts(at),
        }],
        status_calls: vec![StatusCall {
            key: CallKey {
                rid: RID.to_owned(),
                tiploc: "YORK".to_owned(),
                wta: String::new(),
                wtd: "10:00".to_owned(),
                wtp: String::new(),
            },
            ssd,
            pta: None,
            ptd: Some("10:00".to_owned()),
            platform: Some(Platform {
                number: platform.to_owned(),
                confirmed: true,
                suppressed: false,
                cis_suppressed: false,
                source: Some('M'),
            }),
            arrival: None,
            departure: None,
            pass: None,
            suppressed: false,
            ts: ts(at),
        }],
        ..Batch::default()
    }
}

fn entry(key: &str, batch: &Batch) -> StreamEntry {
    StreamEntry {
        stream: STREAM.to_owned(),
        id: "1-0".to_owned(),
        envelope: Envelope::new(
            SchemaId::new(common::darwin::SCHEMA, 1).unwrap(),
            "test/pod",
            format!("{}:test-{key}", common::darwin::SCHEMA),
            Utc::now(),
            batch,
        )
        .unwrap(),
    }
}

async fn admin_pool(pool: &PgPool) -> PgPool {
    match std::env::var("MIGRATION_DATABASE_URL") {
        Ok(url) => sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap(),
        Err(_) => pool.clone(),
    }
}

async fn cleanup(admin: &PgPool) {
    for sql in [
        "DELETE FROM darwin_calls WHERE rid LIKE '%8Z8%' AND ssd >= '2099-01-01'",
        "DELETE FROM darwin_services WHERE rid LIKE '%8Z8%' AND ssd >= '2099-01-01'",
        "DELETE FROM ingest_dedup WHERE key LIKE 'darwin-pushport:test-%'",
    ] {
        sqlx::query(sql).execute(admin).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires a live database: DATABASE_URL=... cargo test -p ingest-writer --test \
            darwin_handler -- --ignored --test-threads=1"]
async fn a_batch_is_applied_once_and_an_older_one_writes_nothing() {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let admin = admin_pool(&pool).await;
    cleanup(&admin).await;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let handler = WriterHandler::new(
        pool.clone(),
        Arc::new(registry()),
        StreamSpec::by_stream(STREAM).expect("the darwin stream"),
    );

    let first = entry(&format!("{nanos:x}-a"), &batch("2099-07-11T09:00:00Z", "4"));
    assert!(matches!(handler.handle(&first).await, Ok(Handled::Applied)));
    assert!(matches!(
        handler.handle(&first).await,
        Ok(Handled::Duplicate)
    ));
    let older = entry(&format!("{nanos:x}-b"), &batch("2099-07-11T08:00:00Z", "9"));
    assert!(matches!(handler.handle(&older).await, Ok(Handled::Applied)));

    let (platform, basis): (String, String) = sqlx::query_as(
        "SELECT platform, platform_basis FROM darwin_calls WHERE rid = $1 AND tiploc = 'YORK'",
    )
    .bind(RID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((platform.as_str(), basis.as_str()), ("4", "confirmed"));

    cleanup(&admin).await;
    clear_future_freshness(&admin).await;
}

/// The fixture times are in 2099: the freshness marker is clamped to the
/// writer's `now() + 2 min` (D13), not left in 2099 where `GREATEST` would
/// keep it and silence the staleness alert; a marker a skewed message left
/// in the future before the clamp is healed by the next batch.
#[tokio::test]
#[ignore = "requires a live database: DATABASE_URL=... cargo test -p ingest-writer --test \
            darwin_handler -- --ignored --test-threads=1"]
async fn the_freshness_marker_is_clamped_and_a_future_one_healed() {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let admin = admin_pool(&pool).await;
    cleanup(&admin).await;
    // A marker a future-stamped message wrote before the clamp existed.
    sqlx::query(
        "INSERT INTO ingest_freshness (source, fetched_at) \
         VALUES ('darwin-pushport', '2099-12-31T00:00:00Z') \
         ON CONFLICT (source) DO UPDATE SET fetched_at = EXCLUDED.fetched_at",
    )
    .execute(&admin)
    .await
    .unwrap();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let handler = WriterHandler::new(
        pool.clone(),
        Arc::new(registry()),
        StreamSpec::by_stream(STREAM).expect("the darwin stream"),
    );

    let before = Utc::now();
    let first = entry(&format!("{nanos:x}-f"), &batch("2099-07-11T09:00:00Z", "4"));
    assert!(matches!(handler.handle(&first).await, Ok(Handled::Applied)));
    let after = Utc::now();

    let fresh: DateTime<Utc> = sqlx::query_scalar(
        "SELECT fetched_at FROM ingest_freshness WHERE source = 'darwin-pushport'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    // Postgres keeps microseconds: allow the truncation.
    let ahead = chrono::TimeDelta::minutes(2);
    let slack = chrono::TimeDelta::milliseconds(1);
    assert!(
        fresh >= before + ahead - slack && fresh <= after + ahead,
        "freshness {fresh} should be the writer's clock + 2 min ({before}..{after})"
    );

    cleanup(&admin).await;
    clear_future_freshness(&admin).await;
}

/// Removes a `darwin-pushport` marker the tests left ahead of the clock
/// (the clamp puts it up to 2 minutes ahead).
async fn clear_future_freshness(admin: &PgPool) {
    sqlx::query(
        "DELETE FROM ingest_freshness WHERE source = 'darwin-pushport' AND fetched_at > now()",
    )
    .execute(admin)
    .await
    .unwrap();
}
