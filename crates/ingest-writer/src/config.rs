//! CLI/env configuration for the `ingest-writer` service.

use std::time::Duration;

use clap::Parser;
use common::config::{LineCatalogue, parse_lines};
use common::secret::Secret;

use crate::stream::StreamModes;

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

    /// Each ingest stream's mode (spec §10, plan 3a.3;
    /// `ingestWriter.streams.<name>`): comma-separated `<stream>:<mode>`,
    /// mode `off`, `shadow` or `apply`, e.g.
    /// `station-samples:apply,full-coverage:shadow`. A stream not listed is
    /// `off`; the default (empty) reads no stream and needs no Redis.
    #[arg(long, env = "INGEST_WRITER_STREAMS", default_value = "")]
    pub streams: StreamModes,

    /// Changed rows only (plan 3a.9, spec §7.8;
    /// `ingestWriter.changedRowsOnly`): the `station-full-coverage-samples/1`
    /// handler stops advancing `resolved_at` on rows whose stats are
    /// unchanged, and its readers derive the age from the feed's observed
    /// time instead. **Off by default** (every row written as today). Turn
    /// it on only once `full-coverage` is on `apply` and soaked, and after
    /// the api with the derived readers is deployed.
    #[arg(long, env = "INGEST_WRITER_CHANGED_ROWS_ONLY", default_value_t = false)]
    pub changed_rows_only: bool,

    /// The Redis holding the ingest streams. Required once any stream is not
    /// `off`; never carries a credential (those are `REDIS_USERNAME` and
    /// `REDIS_PASSWORD`).
    #[arg(long, env)]
    pub redis_url: Option<String>,

    /// Redis AUTH password (chart `redis.auth`, or the `ingest-writer` ACL
    /// user's under `redis.acl`). Never logged.
    #[arg(long, env, hide_env_values = true)]
    pub redis_password: Option<Secret>,

    /// Redis ACL user (`ingest-writer` under `redis.acl.clients.ingestWriter`).
    /// Unset: the `default` user.
    #[arg(long, env)]
    pub redis_username: Option<String>,

    /// This writer's consumer name in each stream's group: the pod name
    /// (`POD_NAME`, else `HOSTNAME`, which Kubernetes sets to it).
    #[arg(long, env = "POD_NAME")]
    pub pod_name: Option<String>,
    /// The train-event outbox loop's interval (plan 3b.3; see
    /// `ds_store::loops::TRAIN_EVENT_OUTBOX_DEFAULT_INTERVAL` for the 5 s).
    #[arg(
        long,
        env = "INGEST_WRITER_TRAIN_EVENT_OUTBOX_INTERVAL_SECS",
        default_value_t = 5
    )]
    pub train_event_outbox_interval_secs: u64,

    /// A train-event outbox row whose apply fails with an error that is not
    /// a data error on this many ticks is marked rejected (security review
    /// L1), so it cannot hold up the rows behind it forever.
    #[arg(long, env = "INGEST_WRITER_OUTBOX_MAX_ATTEMPTS", default_value_t = 5)]
    pub outbox_max_attempts: u32,

    /// Rejected train-event outbox rows are deleted this many days after
    /// they were rejected (security review L1).
    #[arg(
        long,
        env = "INGEST_WRITER_OUTBOX_REJECTED_RETENTION_DAYS",
        default_value_t = 14
    )]
    pub outbox_rejected_retention_days: u64,

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
    /// The train-event outbox loop's handling of rows it cannot apply.
    pub fn outbox_policy(&self) -> ds_store::tracking::outbox::OutboxPolicy {
        ds_store::tracking::outbox::OutboxPolicy {
            max_attempts: self.outbox_max_attempts,
            rejected_retention: Duration::from_secs(
                self.outbox_rejected_retention_days
                    .saturating_mul(24 * 3600),
            ),
        }
    }

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
            (
                self.train_event_outbox_interval_secs,
                "INGEST_WRITER_TRAIN_EVENT_OUTBOX_INTERVAL_SECS",
            ),
            (
                u64::from(self.outbox_max_attempts),
                "INGEST_WRITER_OUTBOX_MAX_ATTEMPTS",
            ),
            (
                self.outbox_rejected_retention_days,
                "INGEST_WRITER_OUTBOX_REJECTED_RETENTION_DAYS",
            ),
        ] {
            anyhow::ensure!(value > 0, "{name} must be greater than zero");
        }
        if self.streams.any_active() {
            anyhow::ensure!(
                self.redis_url.as_deref().is_some_and(|url| !url.is_empty()),
                "INGEST_WRITER_STREAMS turns on {} but REDIS_URL is not set",
                self.streams
            );
        }
        Ok(())
    }

    /// The consumer name: `POD_NAME`, else `HOSTNAME`, else
    /// `ingest-writer`.
    pub fn consumer_name(&self) -> String {
        self.pod_name
            .clone()
            .filter(|name| !name.is_empty())
            .or_else(|| {
                std::env::var("HOSTNAME")
                    .ok()
                    .filter(|name| !name.is_empty())
            })
            .unwrap_or_else(|| "ingest-writer".to_owned())
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
        assert!(!config.streams.any_active(), "every stream off by default");
        // Security review L1: 5 attempts, 14 days.
        assert_eq!(
            config.outbox_policy(),
            ds_store::tracking::outbox::OutboxPolicy::default()
        );
        assert!(!config.changed_rows_only, "plan 3a.9 off by default");
        assert_eq!(config.canary_interval_secs, 60);
        // The api's defaults (crates/api/src/data/config.rs).
        assert_eq!(config.schedule_match_interval_secs, 300);
        assert_eq!(config.reconciliation_sweep_interval_secs, 300);
        assert_eq!(config.schedule_enrichment_grace_minutes, 30);
        assert_eq!(config.backlog_match_sweep_interval_secs, 300);
        assert_eq!(config.corpus_crosswalk_interval_secs, 600);
        assert_eq!(config.train_event_outbox_interval_secs, 5);
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
        assert_eq!(
            env("train_event_outbox_interval_secs").as_deref(),
            Some("INGEST_WRITER_TRAIN_EVENT_OUTBOX_INTERVAL_SECS")
        );
        assert_eq!(
            env("outbox_max_attempts").as_deref(),
            Some("INGEST_WRITER_OUTBOX_MAX_ATTEMPTS")
        );
        assert_eq!(
            env("outbox_rejected_retention_days").as_deref(),
            Some("INGEST_WRITER_OUTBOX_REJECTED_RETENTION_DAYS")
        );
        assert_eq!(env("metrics_port").as_deref(), Some("METRICS_PORT"));
        assert_eq!(env("streams").as_deref(), Some("INGEST_WRITER_STREAMS"));
        assert_eq!(
            env("changed_rows_only").as_deref(),
            Some("INGEST_WRITER_CHANGED_ROWS_ONLY")
        );
        assert_eq!(env("redis_url").as_deref(), Some("REDIS_URL"));
        assert_eq!(env("redis_username").as_deref(), Some("REDIS_USERNAME"));
        assert_eq!(env("redis_password").as_deref(), Some("REDIS_PASSWORD"));
        assert_eq!(env("pod_name").as_deref(), Some("POD_NAME"));
    }

    #[test]
    fn a_stream_on_needs_redis() {
        // An empty REDIS_URL, so a REDIS_URL in the environment cannot pass.
        let config = Config::try_parse_from(args(&[
            "--streams",
            "station-samples:shadow",
            "--redis-url",
            "",
        ]))
        .unwrap();
        assert!(config.validate().is_err());
        let config = Config::try_parse_from(args(&[
            "--streams",
            "station-samples:shadow",
            "--redis-url",
            "redis://redis:6379",
            "--pod-name",
            "writer-0",
        ]))
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.consumer_name(), "writer-0");
        assert!(Config::try_parse_from(args(&["--streams", "nope:apply"])).is_err());
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
