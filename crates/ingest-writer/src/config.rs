//! CLI/env configuration for the `ingest-writer` service.

use std::time::Duration;

use clap::Parser;
use common::config::{LineCatalogue, parse_lines};
use common::secret::Secret;

/// `Debug` is safe to log: the one credential is a [`Secret`] (SVC-12).
#[derive(Debug, Parser)]
#[command(name = "ingest-writer")]
pub struct Config {
    /// The writer role's connection (`distant_signal_writer`, spec §6.6).
    /// Pool size and timeouts come from the shared `DATABASE_*` variables
    /// (`common::pg`); the default pool is 6, plus one connection for the
    /// loop locks.
    #[arg(long, env, hide_env_values = true)]
    pub database_url: Secret,

    /// Directory of line-catalogue TOML files, loaded once at startup (the
    /// same default as the api's). The schedule-match and reconciliation
    /// sweeps build their CRS-to-line index from it, as the api does.
    #[arg(long = "lines-dir", env = "LINES_DIR", default_value = "/app/lines", value_parser = parse_lines)]
    pub lines: LineCatalogue,

    /// Run the background loops (`ingestWriter.loops.enabled`). **Off by
    /// default**: until the cutover the api runs the sweeps (spec §12.3).
    /// Each loop holds its advisory lock, and the api's loops take the same
    /// locks (plan 1B.7), so turning this on while the api's loops still
    /// run is safe: each sweep runs in one process at a time.
    #[arg(long, env = "INGEST_WRITER_LOOPS", default_value_t = false)]
    pub loops_enabled: bool,

    /// Interval of the no-op canary loop (`SELECT 1` under its own lock),
    /// which runs whenever the loops are on.
    #[arg(long, env = "INGEST_WRITER_CANARY_INTERVAL_SECS", default_value_t = 60)]
    pub canary_interval_secs: u64,

    /// The schedule-match sweep's interval. The api's variable, name and
    /// default (unwired in the chart, as the api's is).
    #[arg(long, env = "SCHEDULE_MATCH_INTERVAL_SECS", default_value_t = 300)]
    pub schedule_match_interval_secs: u64,

    /// The reconciliation sweep's interval. The api's variable, name and
    /// default; the chart renders it from `api.reconciliationSweepIntervalSecs`.
    #[arg(
        long,
        env = "RECONCILIATION_SWEEP_INTERVAL_SECS",
        default_value_t = 300
    )]
    pub reconciliation_sweep_interval_secs: u64,

    /// The reconciliation sweep's schedule-enrichment grace period. The
    /// api's variable, name and default.
    #[arg(long, env = "SCHEDULE_ENRICHMENT_GRACE_MINUTES", default_value_t = 30)]
    pub schedule_enrichment_grace_minutes: i64,

    /// The backlog-match sweep's interval. The api's variable, name and
    /// default.
    #[arg(long, env = "BACKLOG_MATCH_SWEEP_INTERVAL_SECS", default_value_t = 300)]
    pub backlog_match_sweep_interval_secs: u64,

    /// The CORPUS crosswalk `rebuild_if_stale` and freshness-gauge loop's
    /// interval (spec §12.3: 10 minutes). The api only runs this check at
    /// startup and after a stations or CORPUS POST.
    #[arg(
        long,
        env = "INGEST_WRITER_CORPUS_CROSSWALK_INTERVAL_SECS",
        default_value_t = 600
    )]
    pub corpus_crosswalk_interval_secs: u64,

    /// Port for the Prometheus `/metrics` listener (the workers' default;
    /// the chart sets it from `metrics.port`).
    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,

    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,

    /// `/livez` (every loop making progress) and `/healthz` (also 503 until
    /// the database pool is up). SVC-08/INF-5.
    #[command(flatten)]
    pub health: common::service_args::HealthArgs,
}

impl Config {
    /// Refuses a zero interval, which `tokio::time::interval` would panic
    /// on with no hint of the setting behind it.
    pub fn validate(&self) -> anyhow::Result<()> {
        for (value, name) in [
            (
                self.canary_interval_secs,
                "INGEST_WRITER_CANARY_INTERVAL_SECS",
            ),
            (
                self.schedule_match_interval_secs,
                "SCHEDULE_MATCH_INTERVAL_SECS",
            ),
            (
                self.reconciliation_sweep_interval_secs,
                "RECONCILIATION_SWEEP_INTERVAL_SECS",
            ),
            (
                self.backlog_match_sweep_interval_secs,
                "BACKLOG_MATCH_SWEEP_INTERVAL_SECS",
            ),
            (
                self.corpus_crosswalk_interval_secs,
                "INGEST_WRITER_CORPUS_CROSSWALK_INTERVAL_SECS",
            ),
        ] {
            anyhow::ensure!(value > 0, "{name} must be greater than zero");
        }
        Ok(())
    }

    /// The three periodic sweeps' intervals, as `ds_store::loops` takes
    /// them.
    pub fn train_loop_intervals(&self) -> ds_store::loops::TrainLoopIntervals {
        ds_store::loops::TrainLoopIntervals {
            schedule_match: Duration::from_secs(self.schedule_match_interval_secs),
            reconciliation: Duration::from_secs(self.reconciliation_sweep_interval_secs),
            schedule_enrichment_grace: chrono::Duration::minutes(
                self.schedule_enrichment_grace_minutes,
            ),
            backlog_match: Duration::from_secs(self.backlog_match_sweep_interval_secs),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(extra: &[&str]) -> Vec<String> {
        let lines = common::manifest_dir!().join("../../lines");
        let mut args = vec![
            "ingest-writer".to_owned(),
            "--database-url".to_owned(),
            "postgres://writer@localhost/db".to_owned(),
            "--lines-dir".to_owned(),
            lines.to_str().unwrap().to_owned(),
        ];
        args.extend(extra.iter().map(ToString::to_string));
        args
    }

    #[test]
    fn defaults_leave_the_loops_off() {
        let config = Config::try_parse_from(args(&[])).unwrap();
        assert!(!config.loops_enabled);
        assert_eq!(config.canary_interval_secs, 60);
        // The api's defaults (crates/api/src/data/config.rs).
        assert_eq!(config.schedule_match_interval_secs, 300);
        assert_eq!(config.reconciliation_sweep_interval_secs, 300);
        assert_eq!(config.schedule_enrichment_grace_minutes, 30);
        assert_eq!(config.backlog_match_sweep_interval_secs, 300);
        assert_eq!(config.corpus_crosswalk_interval_secs, 600);
        assert_eq!(config.metrics_port, 9091);
        assert!(config.metrics.metrics_enabled);
        assert_eq!(config.health.health_bind_url, "0.0.0.0:8090");
        assert!(!config.lines.is_empty());
        config.validate().unwrap();
    }

    #[test]
    fn the_loops_switch_and_a_zero_interval() {
        let config = Config::try_parse_from(args(&["--loops-enabled"])).unwrap();
        assert!(config.loops_enabled);
        for flag in [
            "--canary-interval-secs",
            "--schedule-match-interval-secs",
            "--reconciliation-sweep-interval-secs",
            "--backlog-match-sweep-interval-secs",
            "--corpus-crosswalk-interval-secs",
        ] {
            let config = Config::try_parse_from(args(&[flag, "0"])).unwrap();
            assert!(config.validate().is_err(), "{flag} 0");
        }
    }

    /// The chart sets these names (`ingestWriter.*`, plan 1B.9).
    #[test]
    fn env_names_are_stable() {
        use clap::CommandFactory;
        let command = Config::command();
        let env = |id: &str| {
            command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(clap::Arg::get_env)
                .and_then(|name| name.to_str())
                .map(str::to_owned)
        };
        assert_eq!(env("loops_enabled").as_deref(), Some("INGEST_WRITER_LOOPS"));
        assert_eq!(
            env("canary_interval_secs").as_deref(),
            Some("INGEST_WRITER_CANARY_INTERVAL_SECS")
        );
        assert_eq!(env("database_url").as_deref(), Some("DATABASE_URL"));
        assert_eq!(env("lines").as_deref(), Some("LINES_DIR"));
        // The api's names, so the chart sets both from one value.
        assert_eq!(
            env("schedule_match_interval_secs").as_deref(),
            Some("SCHEDULE_MATCH_INTERVAL_SECS")
        );
        assert_eq!(
            env("reconciliation_sweep_interval_secs").as_deref(),
            Some("RECONCILIATION_SWEEP_INTERVAL_SECS")
        );
        assert_eq!(
            env("schedule_enrichment_grace_minutes").as_deref(),
            Some("SCHEDULE_ENRICHMENT_GRACE_MINUTES")
        );
        assert_eq!(
            env("backlog_match_sweep_interval_secs").as_deref(),
            Some("BACKLOG_MATCH_SWEEP_INTERVAL_SECS")
        );
        assert_eq!(
            env("corpus_crosswalk_interval_secs").as_deref(),
            Some("INGEST_WRITER_CORPUS_CROSSWALK_INTERVAL_SECS")
        );
        assert_eq!(env("metrics_port").as_deref(), Some("METRICS_PORT"));
    }

    #[test]
    fn a_missing_catalogue_fails_to_parse() {
        let result = Config::try_parse_from([
            "ingest-writer",
            "--database-url",
            "postgres://writer@localhost/db",
            "--lines-dir",
            "/nonexistent/lines",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn debug_hides_the_database_url() {
        let config = Config::try_parse_from(args(&[])).unwrap();
        assert!(!format!("{config:?}").contains("writer@localhost"));
    }
}
