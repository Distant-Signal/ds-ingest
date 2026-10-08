//! `ingest-writer` (spec §10, plan 1B.6 and 3a.3): connects as the writer
//! role, loads the line catalogue, serves health and metrics, (with
//! `INGEST_WRITER_LOOPS` on) runs the train-domain loops under their
//! advisory locks, and (with any `INGEST_WRITER_STREAMS` stream not `off`)
//! consumes the Redis ingest streams. See `loops.rs` and `stream.rs`.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use clap::Parser;
use ds_store::loops::{LockSession, LoopRunner};
use ingest_writer::config::Config;

const APPLICATION_NAME: &str = "distant-signal-ingest-writer";
/// The lock session's `pg_stat_activity.application_name`, so whoever holds
/// a loop lock is visible beside `pg_locks`.
const LOCK_APPLICATION_NAME: &str = "distant-signal-ingest-writer-locks";
/// Spec §6.6: pool 6, role limit 7 (the lock session is the seventh).
const DEFAULT_MAX_CONNECTIONS: u32 = 6;
/// How often `/livez`'s watchdog checks the loops.
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(10);
/// How long the stream tasks get to finish their current entry on
/// shutdown, inside the pod's 30 s termination grace.
const STREAM_SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

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

    let streams = start_streams(&config, &pool)?;

    let mut runner = LoopRunner::new(pool.clone(), LockSession::new(pool, LOCK_APPLICATION_NAME));
    if config.loops_enabled {
        ingest_writer::loops::register(&mut runner, &config)?;
    }
    if config.streams.any_apply() {
        // Whatever INGEST_WRITER_LOOPS says: the keys belong to the streams.
        runner.register(ingest_writer::dedup::prune_loop(
            ingest_writer::dedup::PRUNE_INTERVAL,
        ))?;
    }
    let running = runner.spawn(config.health.stall_after());
    tracing::info!(loops = running.len(), "background loops started");

    // `/livez` follows the loops and the stream tasks: the shared Progress
    // is beaten only while none is stalled, so one wedged loop or stream
    // fails liveness even though the others keep ticking (spec §10,
    // Health).
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut watchdog = tokio::time::interval(WATCHDOG_INTERVAL);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            _ = watchdog.tick() => {
                let mut stalled = running.stalled();
                if let Some(streams) = &streams {
                    stalled.extend(streams.stalled());
                }
                if stalled.is_empty() {
                    progress.beat();
                } else {
                    tracing::warn!(?stalled, "a background loop or ingest stream has made no progress within the stall window");
                }
            }
        }
    }
    tracing::info!("shutdown signal received; stopping the streams and releasing the loop locks");
    if let Some(streams) = streams {
        streams.shutdown(STREAM_SHUTDOWN_GRACE).await;
    }
    running.shutdown().await;
    Ok(())
}

/// Spawns the ingest stream consumers (plan 3a.3) when any stream is not
/// `off`; `None` otherwise (the default), without touching Redis.
fn start_streams(
    config: &Config,
    pool: &sqlx::PgPool,
) -> anyhow::Result<Option<ingest_writer::stream::RunningStreams>> {
    if !config.streams.any_active() {
        return Ok(None);
    }
    let registry = ingest_writer::handlers::registry();
    let uncovered = config.streams.uncovered(&registry);
    anyhow::ensure!(
        uncovered.is_empty(),
        "INGEST_WRITER_STREAMS ({}) turns on streams whose schemas this writer has no handler \
         for ({}); every entry would be dead-lettered as an unknown schema. Set them to off.",
        config.streams,
        uncovered.join(", ")
    );
    // REDIS_USERNAME and REDIS_PASSWORD, when set, are applied here
    // (common::redis_auth). The result carries the password: never log it.
    let redis_url = common::redis_auth::redis_url_with_credentials(
        config.redis_url.as_deref().unwrap_or_default(),
        config.redis_username.as_deref(),
        config.redis_password.as_ref(),
    )?;
    let runtime = ingest_writer::stream::StreamRuntime {
        client: redis::Client::open(redis_url.expose())?,
        pool: pool.clone(),
        registry: Arc::new(registry),
        consumer: config.consumer_name(),
        stall_after: config.health.stall_after(),
    };
    let streams = ingest_writer::stream::spawn(&runtime, &config.streams);
    tracing::info!(
        modes = %config.streams,
        consumer = %runtime.consumer,
        tasks = streams.len(),
        "ingest stream consumers started"
    );
    Ok(Some(streams))
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
