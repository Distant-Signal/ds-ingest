//! full-coverage-consumer's direct reads (ingest plan 4.3) as whichever
//! role `DATABASE_URL` is: CI runs it as the superuser and, in the
//! per-service step, as the narrow read-only `full_coverage_ro` role
//! itself (plan 4.7), so the schema gate and every query
//! `POPULATION_SOURCE=db` and `STANOX_CRS_SOURCE=db` run are proven against
//! exactly that role's `db-grants.yaml` grants. Read-only: it writes
//! nothing, so it needs no fixtures.

use std::time::Duration;

use ds_store::schema::{DbRole, SchemaGate, wait_for_schema_with};

async fn pool() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    sqlx::PgPool::connect(&url).await.expect("connect")
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn the_schema_gate_passes_for_the_full_coverage_ro_role() {
    let gate = SchemaGate {
        deadline: Duration::ZERO,
        ..SchemaGate::for_role(DbRole::FullCoverageRo)
    };
    wait_for_schema_with(&pool().await, &gate, None)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn the_population_and_stanox_crs_reads_run_as_the_role() {
    let pool = pool().await;
    let today = chrono::Utc::now().date_naive();
    ds_store::reads::list_population_versions(&pool, &["no-such-line".to_owned()], &[today])
        .await
        .expect("SELECT on schedule_line_population");
    // Both CORPUS fallback settings: the fallback also reads tiploc_crs and
    // corpus_stanox_crs.
    for corpus_fallback in [false, true] {
        ds_store::reference::list_stanox_crs_with(&pool, corpus_fallback)
            .await
            .unwrap_or_else(|err| panic!("corpus_fallback={corpus_fallback}: {err:#}"));
    }
}
