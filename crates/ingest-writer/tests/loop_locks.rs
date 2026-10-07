//! The loop runner against a real Postgres (plan 1B.6's DB tests). Advisory
//! locks are per database, so these fixed test keys only meet each other.
//!
//! ```text
//! DATABASE_URL=postgres://... cargo test -p ingest-writer --test loop_locks \
//!     -- --ignored --test-threads=1
//! ```

#![expect(
    clippy::expect_used,
    reason = "test helpers outside #[test] fns: a panic is the failure"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::advisory_locks::LoopLock;
use ingest_writer::loop_runner::{LockSession, LoopRunner, LoopSpec, TickOutcome};
use sqlx::{Connection, PgConnection, PgPool};

const SERVICE: &str = "ingest_writer_test";

fn database_url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test")
}

async fn pool() -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("connect to postgres")
}

/// A runner with one loop on `lock` whose body counts its runs.
async fn counting_runner(
    lock: LoopLock,
    interval: Duration,
    application_name: &str,
) -> (LoopRunner, Arc<AtomicUsize>) {
    let pool = pool().await;
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);
    let mut runner = LoopRunner::new(
        SERVICE,
        pool.clone(),
        LockSession::new(pool, application_name),
    );
    runner
        .register(LoopSpec::new(lock, interval, move |pool| {
            let counter = Arc::clone(&counter);
            async move {
                // A real round trip on the pool, as a sweep would make.
                sqlx::query("SELECT 1").execute(&pool).await?;
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }))
        .expect("register");
    (runner, runs)
}

fn test_lock(name: &'static str, ascii: [u8; 8]) -> LoopLock {
    LoopLock {
        name,
        key: i64::from_be_bytes(ascii),
    }
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn two_runners_on_one_lock_run_the_body_once_per_tick() {
    let lock = test_lock("two_runners", *b"dststtwo");
    let (first, first_runs) =
        counting_runner(lock, Duration::from_secs(60), "ingest-writer-test-a").await;
    let (second, second_runs) =
        counting_runner(lock, Duration::from_secs(60), "ingest-writer-test-b").await;

    for round in 1..=3 {
        let outcomes = [first.tick(0).await, second.tick(0).await];
        assert_eq!(
            outcomes,
            [TickOutcome::Ran, TickOutcome::Skipped],
            "round {round}: the first runner took the lock and keeps it"
        );
        assert_eq!(first_runs.load(Ordering::SeqCst), round);
        assert_eq!(second_runs.load(Ordering::SeqCst), 0);
    }
    assert!(first.session().holds(lock.key).await);
    assert!(!second.session().holds(lock.key).await);

    // The holder goes away (a writer shut down): the standby takes over on
    // its next tick, and the old holder, reconnecting, now skips.
    first.session().close().await;
    assert_eq!(second.tick(0).await, TickOutcome::Ran);
    assert_eq!(first.tick(0).await, TickOutcome::Skipped);
    assert_eq!(second_runs.load(Ordering::SeqCst), 1);
    assert_eq!(first_runs.load(Ordering::SeqCst), 3);
    second.session().close().await;
    first.session().close().await;
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn a_tick_is_skipped_while_another_session_holds_the_lock() {
    let lock = test_lock("held_elsewhere", *b"dststhld");
    let (runner, runs) =
        counting_runner(lock, Duration::from_secs(60), "ingest-writer-test-held").await;

    // Another holder: the api's loops during the cutover, say.
    let mut other = PgConnection::connect(&database_url()).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock.key)
        .execute(&mut other)
        .await
        .unwrap();

    assert_eq!(runner.tick(0).await, TickOutcome::Skipped);
    assert_eq!(runner.tick(0).await, TickOutcome::Skipped);
    assert_eq!(runs.load(Ordering::SeqCst), 0, "the body never ran");

    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
        .bind(lock.key)
        .fetch_one(&mut other)
        .await
        .unwrap();
    assert!(released);
    assert_eq!(runner.tick(0).await, TickOutcome::Ran);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Now the runner holds it, and the other session cannot take it.
    let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(lock.key)
        .fetch_one(&mut other)
        .await
        .unwrap();
    assert!(!taken, "the runner holds the lock between ticks");
    runner.session().close().await;
    let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(lock.key)
        .fetch_one(&mut other)
        .await
        .unwrap();
    assert!(taken, "closing the session released it");
    other.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn spawned_loops_tick_on_their_interval_and_only_one_runner_sweeps() {
    let lock = test_lock("interval", *b"dststint");
    let interval = Duration::from_millis(250);
    let (first, first_runs) = counting_runner(lock, interval, "ingest-writer-test-i1").await;
    let (second, second_runs) = counting_runner(lock, interval, "ingest-writer-test-i2").await;

    let first = first.spawn(Duration::from_secs(60));
    let second = second.spawn(Duration::from_secs(60));
    // Ticks at 0, 250, 500, 750, 1000 and 1250 ms.
    tokio::time::sleep(Duration::from_millis(1400)).await;
    first.shutdown().await;
    second.shutdown().await;

    let (first_runs, second_runs) = (
        first_runs.load(Ordering::SeqCst),
        second_runs.load(Ordering::SeqCst),
    );
    assert!(
        first_runs == 0 || second_runs == 0,
        "only the lock holder sweeps: {first_runs} and {second_runs}"
    );
    let total = first_runs + second_runs;
    assert!(
        (4..=7).contains(&total),
        "about six ticks in 1.4 s at 250 ms: {total}"
    );
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn a_lost_lock_connection_is_reopened_and_the_lock_retaken() {
    let lock = test_lock("lost_session", *b"dststlst");
    let application_name = "ingest-writer-test-lost";
    let (runner, runs) = counting_runner(lock, Duration::from_secs(60), application_name).await;
    assert_eq!(runner.tick(0).await, TickOutcome::Ran);

    // Kill the lock session's backend: Postgres drops the lock with it.
    let mut admin = PgConnection::connect(&database_url()).await.unwrap();
    let terminated: Vec<bool> = sqlx::query_scalar(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE application_name = $1 AND datname = current_database()",
    )
    .bind(application_name)
    .fetch_all(&mut admin)
    .await
    .unwrap();
    assert_eq!(terminated, [true]);
    // pg_terminate_backend only signals; wait for the backend to exit.
    for _ in 0..50 {
        let left: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = $1")
                .bind(application_name)
                .fetch_one(&mut admin)
                .await
                .unwrap();
        if left == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    admin.close().await.unwrap();

    // The next tick notices (the ping fails), reconnects and retakes it.
    assert_eq!(runner.tick(0).await, TickOutcome::Ran);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert!(runner.session().holds(lock.key).await);
    runner.session().close().await;
}
