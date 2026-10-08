//! Row-level security on `line_status` (plan 3c.3, decision D10), as each
//! role: the migration `20261009131300_line_status_rls.sql` (RLS on, plus a
//! permissive policy for everyone) and the writer's RESTRICTIVE policy from
//! `postgres-grants.sql` (db-grants.yaml `row_policies`).
//!
//! Needs the per-service roles, so it runs under
//! `scripts/test-postgres-roles.py --mode per-service` (CI's per-service
//! step runs every `ingest-writer` DB test that way), which sets
//! `DATABASE_URL_WRITER`, `DATABASE_URL_AGGREGATOR` and `DATABASE_URL_API`.
//! Without them it says so and passes: a plain superuser connection is
//! exempt from the writer's policy, so there is nothing to check.
//!
//! ```text
//! uv run scripts/test-postgres-roles.py --mode per-service -- \
//!   cargo test -p ingest-writer --test line_status_rls -- --ignored
//! ```

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
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

/// SQLSTATE of a failed statement, or `None` if it succeeded.
fn sqlstate<T>(result: &Result<T, sqlx::Error>) -> Option<String> {
    match result {
        Ok(_) => None,
        Err(err) => Some(
            err.as_database_error()
                .and_then(|db| db.code().map(|code| code.into_owned()))
                .unwrap_or_else(|| format!("not a database error: {err}")),
        ),
    }
}

async fn rows_affected(pool: &PgPool, sql: &str) -> Result<u64, sqlx::Error> {
    sqlx::query(sql)
        .execute(pool)
        .await
        .map(|done| done.rows_affected())
}

async fn visible(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT line_id FROM line_status WHERE line_id LIKE 'TEST-RLS-%' ORDER BY line_id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

const INSERT_AGG: &str = "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
     VALUES ('TEST-RLS-AGG', 'aggregator line', 'national-rail', '{NT}', '[]', 'aggregator')";
const INSERT_TFL: &str = "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
     VALUES ('TEST-RLS-TFL', 'tfl line', 'tube', '{TfL}', '[]', 'tfl')";
const CLEANUP: &str = "DELETE FROM line_status WHERE line_id LIKE 'TEST-RLS-%'";

#[tokio::test]
#[ignore = "requires the per-service roles: run under scripts/test-postgres-roles.py --mode per-service"]
async fn the_writer_is_pinned_to_tfl_rows_and_every_other_role_is_unchanged() {
    let (Some(writer), Some(aggregator), Some(api)) = (
        pool("DATABASE_URL_WRITER").await,
        pool("DATABASE_URL_AGGREGATOR").await,
        pool("DATABASE_URL_API").await,
    ) else {
        eprintln!(
            "DATABASE_URL_WRITER/_AGGREGATOR/_API not set: run under \
             scripts/test-postgres-roles.py --mode per-service; nothing checked"
        );
        return;
    };
    let policy: Option<String> = sqlx::query_scalar(
        "SELECT permissive FROM pg_policies \
         WHERE tablename = 'line_status' AND policyname = 'ds_grants_writer'",
    )
    .fetch_optional(&aggregator)
    .await
    .unwrap();
    assert_eq!(
        policy.as_deref(),
        Some("RESTRICTIVE"),
        "postgres-grants.sql must have created the writer's policy"
    );

    rows_affected(&aggregator, CLEANUP).await.unwrap();

    // The aggregator: unchanged. It writes its own rows and sees every row.
    assert_eq!(rows_affected(&aggregator, INSERT_AGG).await.unwrap(), 1);

    // The writer: a TfL insert, update and delete succeed.
    assert_eq!(rows_affected(&writer, INSERT_TFL).await.unwrap(), 1);
    assert_eq!(
        rows_affected(
            &writer,
            "UPDATE line_status SET name = 'tfl line, renamed' WHERE line_id = 'TEST-RLS-TFL'"
        )
        .await
        .unwrap(),
        1
    );
    // ... it cannot see the aggregator's row ...
    assert_eq!(visible(&writer).await, ["TEST-RLS-TFL"]);
    // ... a non-TfL insert fails (WITH CHECK) ...
    let insert = rows_affected(
        &writer,
        "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
         VALUES ('TEST-RLS-W-AGG', 'not mine', 'national-rail', '{NT}', '[]', 'aggregator')",
    )
    .await;
    assert_eq!(sqlstate(&insert).as_deref(), Some("42501"), "{insert:?}");
    // ... so does turning its TfL row into a non-TfL one ...
    let hand_over = rows_affected(
        &writer,
        "UPDATE line_status SET source = 'aggregator' WHERE line_id = 'TEST-RLS-TFL'",
    )
    .await;
    assert_eq!(
        sqlstate(&hand_over).as_deref(),
        Some("42501"),
        "{hand_over:?}"
    );
    // ... an update or delete of the aggregator's row touches nothing ...
    assert_eq!(
        rows_affected(
            &writer,
            "UPDATE line_status SET name = 'stolen' WHERE line_id = 'TEST-RLS-AGG'"
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        rows_affected(
            &writer,
            "DELETE FROM line_status WHERE line_id = 'TEST-RLS-AGG'"
        )
        .await
        .unwrap(),
        0
    );
    // ... and the TfL upsert's ON CONFLICT path onto it fails loudly
    // instead of stealing it.
    let upsert = rows_affected(
        &writer,
        "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
         VALUES ('TEST-RLS-AGG', 'stolen', 'tube', '{TfL}', '[]', 'tfl') \
         ON CONFLICT (line_id) DO UPDATE SET name = EXCLUDED.name, source = 'tfl'",
    )
    .await;
    assert_eq!(sqlstate(&upsert).as_deref(), Some("42501"), "{upsert:?}");
    // With the TfL upsert's own `WHERE line_status.source = 'tfl'` the
    // conflicting row is left alone (no row written), which
    // `upsert_tfl_line_status_observed` reports as an ownership refusal.
    let guarded = rows_affected(
        &writer,
        "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
         VALUES ('TEST-RLS-AGG', 'stolen', 'tube', '{TfL}', '[]', 'tfl') \
         ON CONFLICT (line_id) DO UPDATE SET name = EXCLUDED.name, source = 'tfl' \
         WHERE line_status.source = 'tfl'",
    )
    .await;
    assert_eq!(guarded.unwrap(), 0);
    // The writer's real TfL upsert: refused as owned elsewhere (poison for
    // the stream handler), the aggregator's row untouched.
    let mut tx = writer.begin().await.unwrap();
    let collide = ds_store::samples::upsert_tfl_line_status_observed(
        &mut tx,
        &[common::LineStatusReport {
            id: "TEST-RLS-AGG".into(),
            name: "stolen".into(),
            mode_name: "tube".into(),
            operators: vec![],
            statuses: vec![],
        }],
        chrono::Utc::now(),
        false,
    )
    .await;
    let err = collide.expect_err("a collision with the aggregator's line must fail");
    assert!(
        err.downcast_ref::<ds_store::samples::TflLineOwnedElsewhere>()
            .is_some(),
        "{err:#}"
    );
    tx.rollback().await.unwrap();

    // The aggregator and the api still see both rows; the aggregator still
    // updates and deletes either (its access is unchanged).
    assert_eq!(visible(&aggregator).await, ["TEST-RLS-AGG", "TEST-RLS-TFL"]);
    assert_eq!(visible(&api).await, ["TEST-RLS-AGG", "TEST-RLS-TFL"]);
    let (agg_name,): (String,) =
        sqlx::query_as("SELECT name FROM line_status WHERE line_id = 'TEST-RLS-AGG'")
            .fetch_one(&api)
            .await
            .unwrap();
    assert_eq!(agg_name, "aggregator line", "the writer changed nothing");
    assert_eq!(
        rows_affected(
            &aggregator,
            "UPDATE line_status SET name = name WHERE line_id LIKE 'TEST-RLS-%'"
        )
        .await
        .unwrap(),
        2
    );

    // The writer deletes its own TfL row.
    assert_eq!(
        rows_affected(
            &writer,
            "DELETE FROM line_status WHERE line_id = 'TEST-RLS-TFL'"
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(rows_affected(&aggregator, CLEANUP).await.unwrap(), 1);
}
