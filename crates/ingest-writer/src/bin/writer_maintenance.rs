//! `writer-maintenance`: the one-off data repairs that write ingest tables,
//! moved out of the api image (ingest phase 5 prep, Q2 of
//! docs/ingest-phase5-runbook.md: they must never run with the api's
//! credentials, which cannot write these tables once narrowed in 5.5).
//! Built into the ingest-writer image as `/usr/local/bin/writer-maintenance`.
//!
//! ```text
//!   DATABASE_URL=<writer role> writer-maintenance replay-uidless-movements [--since RFC3339]
//!   DATABASE_URL=<incidents role> LINES_DIR=/app/lines \
//!     writer-maintenance backfill-incident-lines
//! ```
//!
//! - `replay-uidless-movements` (formerly the api's
//!   `replay_uidless_movements`) writes `train_movement_events` /
//!   `train_current_state` for uid-less `trust_event_backlog` rows received
//!   since `--since` (default: 24 hours ago, the backlog's retention). Run it
//!   as the writer role (`distant_signal_writer`, the ingest-writer pod's own
//!   `DATABASE_URL`). Logic: `ds_store::backlog::replay_uidless_backlog`.
//! - `backfill-incident-lines` (formerly the api's
//!   `backfill_incident_lines`) recomputes `incidents.affected_lines` from the
//!   line catalogue. It needs `UPDATE (affected_lines)` on `incidents`, which
//!   the writer role does not hold, so run it as the `incidents` role
//!   (`distant_signal_incidents`, poller-incidents' credentials); it checks
//!   its grants first. Logic: `ds_store::incidents::line_backfill`. Runbook:
//!   docs/incident-affected-lines-backfill.md.
//!
//! The first connection is retried for up to
//! `BACKFILL_CONNECT_DEADLINE_SECS` (default 120), then exits 1.
//!
//! Both are idempotent and refuse the api's role. The old api binaries
//! still work, deprecated, until phase 5 step 5.4b deletes them.
//!
//! Exit status 0 on success, 1 on any error (logged as one line).

use anyhow::Context;
use clap::{Parser, Subcommand};
use common::secret::Secret;
use ds_store::maintenance::Needs;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

#[derive(Debug, Parser)]
#[command(
    name = "writer-maintenance",
    about = "Distant Signal's one-off ingest data repairs"
)]
struct Cli {
    /// The database, as the role the command names (never the api's).
    #[arg(long, env, hide_env_values = true, global = true)]
    database_url: Option<Secret>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write the movement rows a trust-backlog-consumer restart left out for
    /// uid-less TRUST events (as the writer role).
    ReplayUidlessMovements {
        /// The earliest `received_at` to replay (RFC 3339). Default: 24
        /// hours ago, the backlog's retention.
        #[arg(long)]
        since: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// Recompute `incidents.affected_lines` from the line catalogue (as the
    /// `incidents` role).
    BackfillIncidentLines {
        /// The line catalogue: the deployed image's.
        #[arg(long, env = "LINES_DIR", default_value = "/app/lines")]
        lines_dir: String,
    },
}

/// Rows per `ingest_shared_movements_batch` call (about the size of the
/// consumer's own batches), as the api binary used.
const REPLAY_CHUNK: i64 = 500;

/// The application name reported in `pg_stat_activity`.
const APPLICATION_NAME: &str = "writer-maintenance";

/// What `backfill-incident-lines` reads and writes.
const INCIDENT_LINES_NEEDS: &[Needs] = &[
    Needs::Table {
        table: "incidents",
        privilege: "SELECT",
    },
    Needs::Table {
        table: "stations",
        privilege: "SELECT",
    },
    Needs::Column {
        table: "incidents",
        column: "affected_lines",
        privilege: "UPDATE",
    },
];

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(dispatch(Cli::parse()).await)
}

async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    common::logging::init_with_filter(
        "writer-maintenance",
        common::logging::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| common::logging::EnvFilter::new("info")),
    );
    let url = cli
        .database_url
        .as_ref()
        .map(Secret::expose)
        .filter(|url| !url.trim().is_empty())
        .context("set DATABASE_URL (or --database-url)")?;
    let options: PgConnectOptions = url.parse().context("could not parse DATABASE_URL")?;
    let options = options
        .options(common::pg::DEAD_CLIENT_DETECTION_SETTINGS)
        .application_name(APPLICATION_NAME);
    let deadline = common::startup::connect_deadline_from_env(
        common::startup::BACKFILL_CONNECT_DEADLINE_ENV,
        common::startup::DEFAULT_CONNECT_DEADLINE,
    )?;
    // The first connection is retried for up to
    // BACKFILL_CONNECT_DEADLINE_SECS, as the api image's backfills do.
    let connect = || async {
        common::startup::retry_until_ready_within(
            "postgres",
            common::startup::CONNECT_BACKOFF,
            deadline,
            || {
                PgPoolOptions::new()
                    .max_connections(2)
                    .connect_with(options.clone())
            },
        )
        .await
        .context("could not connect with DATABASE_URL")
    };
    match cli.command {
        Command::ReplayUidlessMovements { since } => {
            let pool = connect().await?;
            replay_uidless_movements(&pool, since).await
        }
        Command::BackfillIncidentLines { lines_dir } => {
            // The catalogue first: an empty one would clear every row.
            let lines = common::config::parse_lines(&lines_dir)?;
            anyhow::ensure!(
                !lines.is_empty(),
                "no line definitions found in {lines_dir} -- refusing to run, since an empty \
                 catalogue would clear affected_lines on every row. Set LINES_DIR to the \
                 repository's lines/ directory."
            );
            tracing::info!(count = lines.len(), lines_dir, "loaded line catalogue");
            let pool = connect().await?;
            backfill_incident_lines(&pool, &common::matcher::LineMatcher::new(&lines)).await
        }
    }
}

async fn replay_uidless_movements(
    pool: &PgPool,
    since: Option<chrono::DateTime<chrono::Utc>>,
) -> anyhow::Result<()> {
    const TOOL: &str = "writer-maintenance replay-uidless-movements";
    let user = ds_store::maintenance::refuse_api_role(
        pool,
        TOOL,
        "the writer role (distant_signal_writer)",
    )
    .await?;
    let since = since.unwrap_or_else(|| chrono::Utc::now() - chrono::Duration::hours(24));
    tracing::info!(user, %since, "replaying uid-less backlog rows");
    let report = ds_store::backlog::replay_uidless_backlog(pool, since, REPLAY_CHUNK).await?;
    tracing::info!(
        rows = report.rows,
        failed = report.failed,
        "replayed uid-less backlog rows (re-run to retry failures)"
    );
    anyhow::ensure!(report.failed == 0, "{} rows failed", report.failed);
    Ok(())
}

async fn backfill_incident_lines(
    pool: &PgPool,
    matcher: &common::matcher::LineMatcher,
) -> anyhow::Result<()> {
    const TOOL: &str = "writer-maintenance backfill-incident-lines";
    const ROLE: &str = "the incidents role (distant_signal_incidents)";
    let user = ds_store::maintenance::refuse_api_role(pool, TOOL, ROLE).await?;
    ds_store::maintenance::require(pool, TOOL, ROLE, INCIDENT_LINES_NEEDS).await?;
    tracing::info!(user, "recomputing incidents.affected_lines");
    let report = ds_store::incidents::line_backfill::run_backfill(pool, matcher).await?;
    tracing::info!(
        rows_examined = report.rows_examined,
        rows_never_computed = report.rows_never_computed,
        rows_updated = report.rows_updated,
        rows_matching_no_line = report.rows_matching_no_line,
        "incident affected_lines backfill complete (an incident matching no line is expected: \
         the archive's Operator filter still finds it)"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn the_cli_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn subcommands_parse() {
        let cli = Cli::try_parse_from([
            "writer-maintenance",
            "replay-uidless-movements",
            "--since",
            "2026-10-01T00:00:00Z",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::ReplayUidlessMovements { since: Some(_) }
        ));
        let cli = Cli::try_parse_from([
            "writer-maintenance",
            "backfill-incident-lines",
            "--lines-dir",
            "/x",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::BackfillIncidentLines { lines_dir } if lines_dir == "/x"
        ));
    }

    /// As whatever role `DATABASE_URL` is: the incidents role (and the
    /// owner, and today any member of the app role) passes the preflight;
    /// the writer role, which holds no UPDATE on incidents, is refused
    /// with the missing grant named.
    #[tokio::test]
    #[ignore = "requires a live database (DATABASE_URL)"]
    async fn the_incident_lines_preflight_matches_the_roles_grants() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let pool = PgPoolOptions::new().connect(&url).await.unwrap();
        let (can_update,): (bool,) = sqlx::query_as(
            "SELECT has_column_privilege('public.incidents', 'affected_lines', 'UPDATE')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let result = ds_store::maintenance::require(&pool, "t", "r", INCIDENT_LINES_NEEDS).await;
        if can_update {
            result.unwrap();
        } else {
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("UPDATE (affected_lines) on incidents"),
                "{err}"
            );
        }
    }
}
