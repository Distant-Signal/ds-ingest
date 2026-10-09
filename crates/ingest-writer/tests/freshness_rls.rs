//! Row-level security on `ingest_freshness` (security review L4,
//! 2026-10-08), as each role: the migration
//! `20261009170000_ingest_freshness_rls.sql` (RLS on, plus a permissive
//! policy for everyone) and each producer's RESTRICTIVE write policies from
//! `postgres-grants.sql` (db-grants.yaml `row_policies`).
//!
//! Needs the per-service roles, so it runs under
//! `scripts/test-postgres-roles.py --mode per-service` (CI's per-service
//! step runs every `ingest-writer` DB test that way), which sets
//! `DATABASE_URL_<ROLE>` for the writer, stations, incidents, api and
//! aggregator roles. Without them it says so and passes: a plain superuser
//! connection is exempt from the policies, so there is nothing to check.
//!
//! Every write runs in a transaction that is rolled back, so the real
//! freshness rows are never moved.
//!
//! ```text
//! uv run scripts/test-postgres-roles.py --mode per-service -- \
//!   cargo test -p ingest-writer --test freshness_rls -- --ignored
//! ```

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure in a test"
)]

use sqlx::PgPool;

async fn pool(var: &str) -> Option<PgPool> {
    let url = std::env::var(var).ok()?;
    Some(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap_or_else(|err| panic!("connect with {var}: {err}")),
    )
}

/// SQLSTATE of a failed call, or `None` if it succeeded.
fn sqlstate<T>(result: &anyhow::Result<T>) -> Option<String> {
    match result {
        Ok(_) => None,
        Err(err) => Some(
            err.downcast_ref::<sqlx::Error>()
                .and_then(sqlx::Error::as_database_error)
                .and_then(|db| db.code().map(std::borrow::Cow::into_owned))
                .unwrap_or_else(|| format!("not a database error: {err:#}")),
        ),
    }
}

/// `record_ingest(source)` as `pool`'s role, rolled back.
async fn record(pool: &PgPool, source: &str) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let result = ds_store::freshness::record_ingest(&mut tx, source, None).await;
    tx.rollback().await?;
    result
}

/// A plain `UPDATE` of `source`'s row as `pool`'s role, rolled back: the
/// rows it touched.
async fn update(pool: &PgPool, source: &str) -> u64 {
    let mut tx = pool.begin().await.unwrap();
    let done = sqlx::query(
        "UPDATE ingest_freshness SET fetched_at = fetched_at + interval '1 day' WHERE source = $1",
    )
    .bind(source)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    done.rows_affected()
}

async fn visible(pool: &PgPool, source: &str) -> bool {
    sqlx::query_scalar::<_, i32>("SELECT 1 FROM ingest_freshness WHERE source = $1")
        .bind(source)
        .fetch_optional(pool)
        .await
        .unwrap()
        .is_some()
}

/// A source no producer owns, present for the whole test.
const FIXTURE: &str = "TEST-L4-not-yours";

#[tokio::test]
#[ignore = "requires the per-service roles: run under scripts/test-postgres-roles.py --mode per-service"]
async fn each_producer_writes_only_its_own_freshness_rows() {
    let (Some(writer), Some(stations), Some(incidents), Some(api), Some(aggregator)) = (
        pool("DATABASE_URL_WRITER").await,
        pool("DATABASE_URL_STATIONS").await,
        pool("DATABASE_URL_INCIDENTS").await,
        pool("DATABASE_URL_API").await,
        pool("DATABASE_URL_AGGREGATOR").await,
    ) else {
        eprintln!(
            "DATABASE_URL_WRITER/_STATIONS/_INCIDENTS/_API/_AGGREGATOR not set: run under \
             scripts/test-postgres-roles.py --mode per-service; nothing checked"
        );
        return;
    };
    let policies: Vec<String> = sqlx::query_scalar(
        "SELECT policyname::text FROM pg_policies \
         WHERE tablename = 'ingest_freshness' AND permissive = 'RESTRICTIVE' ORDER BY 1",
    )
    .fetch_all(&api)
    .await
    .unwrap();
    for role in ["incidents", "stations", "writer"] {
        for command in ["delete", "insert", "update"] {
            let name = format!("ds_grants_{role}_{command}");
            assert!(policies.contains(&name), "{name} missing from {policies:?}");
        }
    }

    // The fixture row, written as the api (unrestricted, as before).
    let mut tx = api.begin().await.unwrap();
    ds_store::freshness::record_ingest(&mut tx, FIXTURE, None)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Each narrow role: its own source(s) succeed ...
    let own: [(&str, &PgPool, &[&str]); 3] = [
        ("stations", &stations, &["stations"]),
        ("incidents", &incidents, &["incidents"]),
        (
            "writer",
            &writer,
            &[
                "tfl",
                "tocs",
                ds_store::samples::sources::STATION_SAMPLES,
                ds_store::samples::sources::FULL_COVERAGE_STATS,
                ds_store::samples::sources::FULL_COVERAGE_WINDOW_STATS,
                ds_store::samples::sources::STATION_FULL_COVERAGE_SAMPLES,
                ds_store::samples::island_of_ireland::sources::STATIONS_GTFS,
                ds_store::samples::island_of_ireland::sources::LINES_GTFS,
                ds_store::samples::island_of_ireland::sources::STATIONS_NIR,
                ds_store::samples::island_of_ireland::sources::LINES_NIR,
            ],
        ),
    ];
    for (role, pool, sources) in own {
        for source in sources {
            record(pool, source)
                .await
                .unwrap_or_else(|err| panic!("{role} writing {source}: {err:#}"));
        }
        // ... another feed's new row fails (WITH CHECK) ...
        let other = if role == "stations" {
            "incidents"
        } else {
            "stations"
        };
        let insert = record(pool, other).await;
        assert_eq!(
            sqlstate(&insert).as_deref(),
            Some("42501"),
            "{role} inserting {other}: {insert:?}"
        );
        // ... the upsert onto another feed's existing row fails loudly ...
        let upsert = record(pool, FIXTURE).await;
        assert_eq!(
            sqlstate(&upsert).as_deref(),
            Some("42501"),
            "{role} upserting {FIXTURE}: {upsert:?}"
        );
        // ... a plain UPDATE of it touches nothing ...
        assert_eq!(update(pool, FIXTURE).await, 0, "{role}");
        // ... and every row stays readable (the writer's gauge reads all).
        assert!(visible(pool, FIXTURE).await, "{role} reads every row");
    }

    // The shared island-of-Ireland names from before the per-network split
    // were dropped a release later: the writer may no longer record them.
    for legacy in ["island_of_ireland_stations", "island_of_ireland_lines"] {
        let insert = record(&writer, legacy).await;
        assert_eq!(
            sqlstate(&insert).as_deref(),
            Some("42501"),
            "writer inserting {legacy}: {insert:?}"
        );
    }

    // Every other role is unaffected: the api and the aggregator still
    // update any source's row.
    record(&api, "stations").await.unwrap();
    assert_eq!(update(&api, FIXTURE).await, 1);
    assert_eq!(update(&aggregator, FIXTURE).await, 1);

    let mut tx = api.begin().await.unwrap();
    sqlx::query("DELETE FROM ingest_freshness WHERE source = $1")
        .bind(FIXTURE)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
