//! The writer's loops (spec §12.3), registered when `INGEST_WRITER_LOOPS`
//! is on.
//!
//! Only the no-op canary ships today. The four train-domain sweeps are api
//! code that moves into `ds-store` in plan 1A.6/1A.10, and the writer must
//! not depend on the api crate. Each one plugs in below, at its marked
//! registration point, once `ds-store` exists. The api keeps running them
//! until then, so nothing is lost while they are absent here.
//!
//! # Where each future loop plugs in
//!
//! | Loop | Lock ([`common::advisory_locks`]) | Body (after the move) | Today, in the api | Interval env (same name and default as the api's) |
//! |---|---|---|---|---|
//! | schedule-match | `SCHEDULE_MATCH_SWEEP` | `ds_store::sweeps::run_schedule_match_sweep(&pool, &index)` | `main.rs` `schedule_match_sweep_loop` → `data::schedule_matching` | `SCHEDULE_MATCH_INTERVAL_SECS` |
//! | reconciliation | `RECONCILIATION_SWEEP` | `ds_store::sweeps::run_reconciliation_sweep(&pool, &index, grace)` | `reconciliation_sweep_loop` → `data::reconciliation` | `RECONCILIATION_SWEEP_INTERVAL_SECS`, plus `SCHEDULE_ENRICHMENT_GRACE_MINUTES` |
//! | backlog-match | `BACKLOG_MATCH_SWEEP` | `ds_store::backlog::run_backlog_match_sweep(&pool)` | `backlog_match_sweep_loop` → `data::trust_event_backlog_match` | `BACKLOG_MATCH_SWEEP_INTERVAL_SECS` |
//! | CORPUS crosswalk | `CORPUS_CROSSWALK` | `ds_store::corpus_crosswalk::rebuild_if_stale(&pool)`, then `ds_store::corpus::refresh_last_delivery_metric(&pool)` | the one-shot task in `spawn_background_loops` | new: `INGEST_WRITER_CORPUS_CROSSWALK_INTERVAL_SECS`, 600 |
//!
//! `index` is the api's `schedule_crs_line_index`, built from the line
//! catalogue (`Config::lines`, loaded at startup already) and the
//! schedule tables; its builder moves with the sweeps. Each body is a
//! closure over its inputs returning the sweep's `Result`, logging its
//! count at info when it did something, as the api loops do now.
//!
//! The `CronJob` sweeps (expired sessions, dead links, personal data) never
//! come here: they need `DELETE` on user tables the writer role must not
//! have (spec R3). They run from the api image's `maintenance` binary.

use std::time::Duration;

use anyhow::Result;
use common::advisory_locks;

use crate::config::Config;
use crate::loop_runner::{LoopRunner, LoopSpec};

/// Registers every loop the writer runs. Called only when
/// `INGEST_WRITER_LOOPS` is on.
pub fn register(runner: &mut LoopRunner, config: &Config) -> Result<()> {
    runner.register(canary(Duration::from_secs(config.canary_interval_secs)))?;

    // Registration point: schedule-match sweep (advisory_locks::SCHEDULE_MATCH_SWEEP),
    // after ds-store 1A.10.
    // Registration point: reconciliation sweep (advisory_locks::RECONCILIATION_SWEEP),
    // after ds-store 1A.10.
    // Registration point: backlog-match sweep (advisory_locks::BACKLOG_MATCH_SWEEP),
    // after ds-store 1A.10.
    // Registration point: CORPUS crosswalk rebuild + freshness gauge
    // (advisory_locks::CORPUS_CROSSWALK), after ds-store 1A.6.
    Ok(())
}

/// The no-op canary: `SELECT 1` under its own lock. Shows in production
/// that the runner takes its lock, ticks and exports its metrics before any
/// real sweep moves over, and gives the 1B.7 cutover test a loop to watch.
pub fn canary(interval: Duration) -> LoopSpec {
    LoopSpec::new(advisory_locks::WRITER_CANARY, interval, |pool| async move {
        sqlx::query("SELECT 1").execute(&pool).await?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::loop_runner::LockSession;

    #[tokio::test]
    async fn registers_only_the_canary_for_now() {
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
        let mut runner = LoopRunner::new(
            "ingest_writer_test",
            pool.clone(),
            LockSession::new(pool, "t"),
        );
        register(&mut runner, &config).unwrap();
        let names: Vec<_> = runner.loops().iter().map(LoopSpec::name).collect();
        assert_eq!(names, ["canary"]);
        assert_eq!(runner.loops()[0].interval, Duration::from_secs(60));
    }
}
