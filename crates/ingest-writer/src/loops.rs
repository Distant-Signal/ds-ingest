//! The writer's loops (spec §12.3), registered when `INGEST_WRITER_LOOPS`
//! is on.
//!
//! | Loop | Lock ([`common::advisory_locks`]) | Body | Interval env (default) |
//! |---|---|---|---|
//! | schedule-match | `SCHEDULE_MATCH_SWEEP` | [`ds_store::loops::schedule_match`] | `SCHEDULE_MATCH_INTERVAL_SECS` (300) |
//! | reconciliation | `RECONCILIATION_SWEEP` | [`ds_store::loops::reconciliation`] | `RECONCILIATION_SWEEP_INTERVAL_SECS` (300), grace `SCHEDULE_ENRICHMENT_GRACE_MINUTES` (30) |
//! | backlog-match | `BACKLOG_MATCH_SWEEP` | [`ds_store::loops::backlog_match`] | `BACKLOG_MATCH_SWEEP_INTERVAL_SECS` (300) |
//! | CORPUS crosswalk | `CORPUS_CROSSWALK` | [`ds_store::loops::corpus_crosswalk`] | `INGEST_WRITER_CORPUS_CROSSWALK_INTERVAL_SECS` (600) |
//! | canary | `WRITER_CANARY` | [`canary`] (`SELECT 1`) | `INGEST_WRITER_CANARY_INTERVAL_SECS` (60) |
//!
//! The first three are the api's loops (same functions, names and defaults
//! of the interval variables, same log messages, the same
//! `distant_signal_loop_*` metrics); the api runs them under the same locks
//! while `API_BACKGROUND_LOOPS` is on (plan 1B.7), so each sweep runs in
//! one process at a time. The CORPUS check runs here every 10 minutes; the
//! api runs it once at startup, under the same lock, and after each
//! stations or CORPUS POST.
//!
//! The `CronJob` sweeps (expired sessions, dead links, personal data) never
//! come here: they need `DELETE` on user tables the writer role must not
//! have (spec R3). They run from the api image's `maintenance` binary.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use common::advisory_locks;
use ds_store::loops::{CrsLineIndex, LoopRunner, LoopSpec};

use crate::config::Config;

/// Registers every loop the writer runs. Called only when
/// `INGEST_WRITER_LOOPS` is on.
pub fn register(runner: &mut LoopRunner, config: &Config) -> Result<()> {
    runner.register(canary(Duration::from_secs(config.canary_interval_secs)))?;
    let index: CrsLineIndex = Arc::new(ds_store::sweeps::schedule_matching::crs_to_line_ids(
        &config.lines,
    ));
    for spec in ds_store::loops::periodic_sweeps(config.train_loop_intervals(), &index) {
        runner.register(spec)?;
    }
    runner.register(ds_store::loops::corpus_crosswalk(Duration::from_secs(
        config.corpus_crosswalk_interval_secs,
    )))?;
    Ok(())
}

/// The no-op canary: `SELECT 1` under its own lock. Shows in production
/// that the runner takes its lock, ticks and exports its metrics.
pub fn canary(interval: Duration) -> LoopSpec {
    LoopSpec::new(advisory_locks::WRITER_CANARY, interval, |pool| async move {
        if let Err(err) = sqlx::query("SELECT 1").execute(&pool).await {
            tracing::error!(error = ?err, "canary loop failed; will retry next interval");
            return Err(err.into());
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use ds_store::loops::LockSession;

    use super::*;

    #[tokio::test]
    async fn registers_the_canary_and_the_four_train_loops() {
        let lines = common::manifest_dir!().join("../../lines");
        let config = Config::try_parse_from([
            "ingest-writer",
            "--database-url",
            "postgres://writer@localhost/db",
            "--lines-dir",
            lines.to_str().unwrap(),
        ])
        .unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@localhost/none")
            .unwrap();
        let mut runner = LoopRunner::new(pool.clone(), LockSession::new(pool, "t"));
        register(&mut runner, &config).unwrap();
        let got: Vec<_> = runner
            .loops()
            .iter()
            .map(|spec| (spec.lock, spec.interval.as_secs()))
            .collect();
        assert_eq!(
            got,
            [
                (advisory_locks::WRITER_CANARY, 60),
                (advisory_locks::SCHEDULE_MATCH_SWEEP, 300),
                (advisory_locks::RECONCILIATION_SWEEP, 300),
                (advisory_locks::BACKLOG_MATCH_SWEEP, 300),
                (advisory_locks::CORPUS_CROSSWALK, 600),
            ]
        );
    }
}
