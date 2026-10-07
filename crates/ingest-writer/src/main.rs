//! `ingest-writer` (spec §10, plan 1B.6): today a skeleton that connects as
//! the writer role, loads the line catalogue, serves health and metrics,
//! and (with `INGEST_WRITER_LOOPS` on) runs its loops under their advisory
//! locks. See `loops.rs` for what is still to come.

use std::sync::atomic::Ordering;
use std::time::Duration;

use clap::Parser;
use ingest_writer::config::Config;
use ingest_writer::loop_runner::{LockSession, LoopRunner};

/// The metric prefix: `distant_signal_ingest_writer_*`.
const SERVICE: &str = "ingest_writer";
const APPLICATION_NAME: &str = "distant-signal-ingest-writer";
/// The lock session's `pg_stat_activity.application_name`, so whoever holds
/// a loop lock is visible beside `pg_locks`.
const LOCK_APPLICATION_NAME: &str = "distant-signal-ingest-writer-locks";
/// Spec §6.6: pool 6, role limit 7 (the lock session is the seventh).
const DEFAULT_MAX_CONNECTIONS: u32 = 6;
/// How often `/livez`'s watchdog checks the loops.
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    let config = Config::parse();
    config.validate()?;
    common::logging::init("ingest-writer");

    if config.metrics.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }

    let (ready, progress) = health_http::spawn_worker(&config.health);
    // INF-5: wait for Postgres instead of exiting into CrashLoopBackOff.
    // `/healthz` stays 503 until this returns; `/livez` stays 200.
    common::startup::retry_until_ready(
        "Postgres",
        common::startup::CONNECT_BACKOFF,
        Some(&progress),
        || async {
            use sqlx::Connection;
            sqlx::PgConnection::connect(config.database_url.expose())
                .await?
                .close()
                .await
        },
    )
    .await;
    let pool = common::pg::PoolSettings::from_env(APPLICATION_NAME, DEFAULT_MAX_CONNECTIONS)?
        .connect(config.database_url.expose())
        .await?;
    // The schema gate (spec §12.2, plan 1B.2): before readiness and before
    // any loop starts; exits after 15 minutes.
    ds_store::schema::wait_for_schema(&pool, ds_store::schema::DbRole::Writer, Some(&progress))
        .await?;
    ready.store(true, Ordering::Relaxed);
    tracing::info!(
        lines = config.lines.len(),
        loops_enabled = config.loops_enabled,
        "ingest-writer started"
    );

    let mut runner = LoopRunner::new(
        SERVICE,
        pool.clone(),
        LockSession::new(pool, LOCK_APPLICATION_NAME),
    );
    if config.loops_enabled {
        ingest_writer::loops::register(&mut runner, &config)?;
    }
    let running = runner.spawn(config.health.stall_after());
    tracing::info!(loops = running.len(), "background loops started");

    // `/livez` follows the loops: the shared Progress is beaten only while
    // no loop is stalled, so one wedged loop fails liveness even though the
    // others keep ticking (spec §10, Health).
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut watchdog = tokio::time::interval(WATCHDOG_INTERVAL);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            _ = watchdog.tick() => {
                let stalled = running.stalled();
                if stalled.is_empty() {
                    progress.beat();
                } else {
                    tracing::warn!(?stalled, "a background loop has made no progress within the stall window");
                }
            }
        }
    }
    tracing::info!("shutdown signal received; releasing the loop locks");
    running.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = sigterm.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            Err(err) => {
                tracing::warn!(error = ?err, "could not install a SIGTERM handler; Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
