//! The narrow writer role (security review M1, 2026-10-08) against a
//! migrated database with the per-service roles: every loop the writer
//! registers, and its dedup prune, run one tick as `distant_signal_writer`
//! over fixture subscriptions, and no service role can `SET ROLE` into a
//! role it belongs to (the app role, its groups), which would shed the row
//! policies written for it.
//!
//! The stream handlers' writes run as the writer in the other test files
//! (CI's per-service step runs every `ingest-writer` DB test with
//! `DATABASE_URL=$DATABASE_URL_WRITER`), and the train-event outbox
//! applier's in `trust-consumer`'s sink tests (`DATABASE_URL_WRITER`).
//!
//! Needs `DATABASE_URL_WRITER` and `DATABASE_URL_API` (set by
//! `scripts/test-postgres-roles.py --mode per-service`); without them it
//! says so and passes.
//!
//! ```text
//! uv run scripts/test-postgres-roles.py --mode per-service -- sh -c \
//!   'DATABASE_URL="$DATABASE_URL_WRITER" cargo test -p ingest-writer --test writer_role -- --ignored'
//! ```

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure in a test"
)]

use clap::Parser;
use ds_store::loops::{LockSession, LoopRunner};
use ingest_writer::config::Config;
use sqlx::PgPool;

async fn pool(var: &str) -> Option<PgPool> {
    let url = std::env::var(var).ok()?;
    Some(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap_or_else(|err| panic!("connect with {var}: {err}")),
    )
}

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

/// Every registered loop body, once, as the writer, over a pending pin for
/// today (schedule-match and backlog-match candidates), a resolved one
/// with a cancelled train (reconciliation), and an unresolved one.
#[tokio::test]
#[ignore = "requires the per-service roles: run under scripts/test-postgres-roles.py --mode per-service"]
#[expect(
    clippy::too_many_lines,
    reason = "fixtures, one tick of every loop, then cleanup, in order"
)]
async fn every_writer_loop_runs_as_the_writer_role() {
    let (Some(writer), Some(api)) = (
        pool("DATABASE_URL_WRITER").await,
        pool("DATABASE_URL_API").await,
    ) else {
        eprintln!(
            "DATABASE_URL_WRITER/_API not set: run under scripts/test-postgres-roles.py \
             --mode per-service; nothing checked"
        );
        return;
    };
    let user = format!("writer-role-{}", rand_suffix());
    sqlx::query("INSERT INTO users (id, email, name) VALUES ($1, $1 || '@example.com', $1)")
        .bind(&user)
        .execute(&api)
        .await
        .unwrap();
    let train_uid = format!("W{}", &rand_suffix()[..5]);
    let (trains_id,): (i64,) = sqlx::query_as(
        "INSERT INTO trains (train_uid, service_date, train_id) \
         VALUES ($1, CURRENT_DATE, '1A00') RETURNING id",
    )
    .bind(&train_uid)
    .fetch_one(&api)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO train_current_state (trains_id, status, updated_at) \
         VALUES ($1, 'cancelled', now())",
    )
    .bind(trains_id)
    .execute(&api)
    .await
    .unwrap();
    for (status, linked) in [
        ("pending", None),
        ("resolved", Some(trains_id)),
        ("unresolved", None),
    ] {
        sqlx::query(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, \
                 pin_destination_crs, resolution_status, trains_id) \
             VALUES ($1, CURRENT_DATE, 'KGX', now() + interval '1 hour', 'EDB', $2, $3)",
        )
        .bind(&user)
        .bind(status)
        .bind(linked)
        .execute(&api)
        .await
        .unwrap();
    }

    // A CORPUS delivery newer than the stored crosswalk, so the crosswalk
    // loop rebuilds it (DELETE and INSERT on corpus_*_crs, the
    // corpus_crosswalk_build upsert) as the writer.
    let delivered_at: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "INSERT INTO corpus_deliveries (delivered_at, source_file, row_count) \
         VALUES (now(), 'writer-role-test', 0) RETURNING delivered_at",
    )
    .fetch_one(&api)
    .await
    .unwrap();

    let lines = common::manifest_dir!().join("../../lines");
    let config = Config::try_parse_from([
        "ingest-writer",
        "--database-url",
        "postgres://writer@localhost/db",
        "--lines-dir",
        lines.to_str().unwrap(),
    ])
    .unwrap();
    let mut runner = LoopRunner::new(writer.clone(), LockSession::new(writer.clone(), "t"));
    ingest_writer::loops::register(&mut runner, &config).unwrap();
    let mut ran = Vec::new();
    for spec in runner.loops() {
        let outcome = spec.run_body(writer.clone()).await;
        assert!(
            outcome.is_ok(),
            "loop {} failed as the writer role: {outcome:?}",
            spec.name()
        );
        ran.push(spec.name());
    }
    assert_eq!(ran.len(), 6, "{ran:?}");
    let built: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT delivered_at FROM corpus_crosswalk_build")
            .fetch_optional(&writer)
            .await
            .unwrap();
    assert_eq!(built, Some(delivered_at), "the crosswalk loop rebuilt it");
    // Leave no CORPUS delivery behind (schedule-ingest's sink test refuses
    // a database holding another).
    for sql in [
        "DELETE FROM corpus_crosswalk_build WHERE delivered_at = $1",
        "DELETE FROM corpus_deliveries WHERE delivered_at = $1",
    ] {
        sqlx::query(sql)
            .bind(delivered_at)
            .execute(&api)
            .await
            .unwrap();
    }
    ingest_writer::dedup::prune(&writer, ingest_writer::dedup::RETENTION)
        .await
        .unwrap();

    // Cleanup through the api role (the writer deletes no subscription).
    sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
        .bind(&user)
        .execute(&api)
        .await
        .unwrap();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&user)
        .execute(&api)
        .await
        .unwrap();
    sqlx::query("DELETE FROM trains WHERE id = $1")
        .bind(trains_id)
        .execute(&api)
        .await
        .unwrap();
}

/// No service role may `SET ROLE` to any role it is a member of: the app
/// role (an observed role inherits its privileges, `SET FALSE`) or a group.
/// A RESTRICTIVE row policy binds the role a session runs as, so
/// `SET ROLE app` would shed the writer's `line_status` policy (security
/// review M1).
#[tokio::test]
#[ignore = "requires the per-service roles: run under scripts/test-postgres-roles.py --mode per-service"]
async fn no_service_role_can_set_role_into_its_memberships() {
    let mut checked = 0;
    for var in [
        "DATABASE_URL_WRITER",
        "DATABASE_URL_API",
        "DATABASE_URL_AGGREGATOR",
        "DATABASE_URL_NOTIFIER",
        "DATABASE_URL_ENRICHER",
    ] {
        let Some(pool) = pool(var).await else {
            continue;
        };
        let memberships: Vec<String> = sqlx::query_scalar(
            "SELECT r.rolname FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid \
             WHERE m.member = (SELECT oid FROM pg_roles WHERE rolname = current_user)",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(!memberships.is_empty(), "{var}: no memberships at all");
        for role in memberships {
            let mut conn = pool.acquire().await.unwrap();
            let set = sqlx::query(&format!("SET ROLE \"{}\"", role.replace('"', "\"\"")))
                .execute(&mut *conn)
                .await;
            let code = set
                .as_ref()
                .err()
                .and_then(|err| err.as_database_error())
                .and_then(|db| db.code().map(std::borrow::Cow::into_owned));
            assert_eq!(
                code.as_deref(),
                Some("42501"),
                "{var} may SET ROLE {role}: {set:?}"
            );
            checked += 1;
        }
    }
    if checked == 0 {
        eprintln!("no DATABASE_URL_<ROLE> set: run under test-postgres-roles.py; nothing checked");
    }
}
