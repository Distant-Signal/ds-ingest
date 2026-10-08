use std::path::Path;

use clap::Parser;
use common::config::{LineCatalogue, parse_lines};

use crate::stanox_crs::StanoxCrsTable;

fn parse_stanox_crs(path: &str) -> anyhow::Result<StanoxCrsTable> {
    StanoxCrsTable::from_file(Path::new(path))
}

/// CLI/env configuration for the `trust-backlog-consumer` service -- a
/// third, independent consumer group on the same `movement-events` Redis
/// Stream `trust-consumer`/`full-coverage-consumer` already read. See
/// docs/superpowers/specs/2026-09-05-trust-event-backlog-design.md and
/// docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md.
///
/// Deliberately Redis-Streams-only, unlike `trust-consumer`/
/// `full-coverage-consumer` (which both still support a legacy direct-
/// Kafka backend from before Deploy A). This crate is new, built after
/// `movement-relay`'s own Redis Streams design was already the
/// established path -- there is no legacy Kafka deployment of this
/// consumer to keep compatible with, so it only ever speaks to the
/// `movement-events` Redis Stream directly via `movement_feed::redis_stream::RedisStreamMovementFeed`.
#[derive(Debug, Parser)]
pub(crate) struct Config {
    #[arg(long, env, default_value = "redis://redis:6379")]
    pub redis_url: String,

    /// Redis AUTH password (chart `redis.auth`, from a Secret). Unset or
    /// empty: no AUTH, `redis_url` is used as-is. Applied to `redis_url` by
    /// `common::redis_auth::redis_url_with_password`, never logged.
    #[arg(long, env, hide_env_values = true)]
    pub redis_password: Option<common::secret::Secret>,

    /// Redis ACL user (chart `redis.acl`; ingest architecture phase 0c).
    /// Unset or empty: the `default` user, exactly as before. Combined with
    /// `redis_password` by `common::redis_auth::redis_url_with_credentials`.
    #[arg(long, env)]
    pub redis_username: Option<String>,

    /// How long an entry may sit unacked in this consumer's own
    /// pending-entries list before its periodic sweep reclaims it. Same
    /// default/reasoning as `trust-consumer`'s identical field.
    #[arg(long, env, default_value_t = 30)]
    pub redis_autoclaim_min_idle_secs: u64,

    /// How often (seconds) this crate compares its own consumer group's
    /// `last-delivered-id` against the stream's oldest retained entry.
    /// Same cadence/reasoning as `trust-consumer`'s identical field.
    #[arg(long, env, default_value_t = 60)]
    pub redis_gap_check_secs: u64,

    /// The `api` crate's ingestion endpoint for this crate's own event
    /// batches. Used only with `INGEST_SINK=http`.
    #[arg(
        long,
        env,
        default_value = "http://api:8080/private/trust-event-backlog"
    )]
    pub api_ingest_url: String,

    /// Where each batch goes (ingest architecture plan 3b.1, spec R1):
    /// `http` (the default, today's behaviour) POSTs the backlog events,
    /// the reason codes and the STANOX/CRS reload to the api's `/private`
    /// routes; `db` writes and reads Postgres directly
    /// (`ds_store::backlog::ingest_trust_event_backlog`,
    /// `ds_store::backlog::reasons::upsert_reasons`,
    /// `ds_store::reference::list_stanox_crs`), as the `trust_backlog`
    /// role. See `sink.rs`.
    #[arg(long, env, value_enum, default_value_t = IngestSink::Http)]
    pub ingest_sink: IngestSink,

    /// Postgres, for `INGEST_SINK=db` (required then, unused otherwise).
    /// Pool size and timeouts come from the shared `DATABASE_*` variables
    /// (`common::pg`); the default pool is 3 (spec §6.6, role limit 4).
    #[arg(long, env, hide_env_values = true)]
    pub database_url: Option<common::secret::Secret>,

    #[command(flatten)]
    pub internal_oauth: common::oauth_client::InternalOAuthArgs,

    /// STANOX->CRS translation table, loaded once at startup. Same file
    /// format/provenance as `trust-consumer`'s identical field --
    /// deliberately a separate, crate-local copy of that logic (see
    /// `stanox_crs`'s own module doc), matching this codebase's own
    /// existing precedent of NOT sharing this kind of small,
    /// crate-specific reference-table logic across consumer crates
    /// (`full-coverage-consumer`'s own `stanox_tiploc.rs` is a third,
    /// independent, differently-shaped implementation of the same idea).
    #[arg(
        long = "stanox-crs-file",
        env = "STANOX_CRS_FILE",
        default_value = "/app/reference-data/stanox-crs.csv",
        value_parser = parse_stanox_crs,
        value_name = "FILE"
    )]
    pub stanox_crs: StanoxCrsTable,

    #[arg(long, env, default_value_t = 3600)]
    pub stanox_crs_reload_secs: u64,

    #[arg(long, env, default_value = "http://api:8080/private/stanox-crs")]
    pub stanox_crs_url: String,

    /// Static line catalogue, needed to build the CRS reverse index this
    /// consumer scopes its writes by (Task 8) -- built independently of,
    /// and with zero dependency on,
    /// docs/superpowers/plans/2026-09-05-schedule-first-train-tracking-plan.md's
    /// own equivalent index (see this plan's "Dependency on the
    /// schedule-first plan" section for the full reasoning).
    #[arg(long = "lines-dir", env = "LINES_DIR", default_value = "/app/lines", value_parser = parse_lines)]
    pub lines: LineCatalogue,

    #[arg(long, env, default_value = "0.0.0.0:8083")]
    pub health_bind_url: String,
    /// Liveness watchdog: `/healthz` answers 503 ("stalled") once no
    /// consume-loop iteration has completed for this many seconds, so a
    /// loop wedged inside an `await` gets restarted by the liveness probe
    /// (which still needs its own `failureThreshold * periodSeconds` on top
    /// of this). A healthy iteration takes a few seconds (the `XREADGROUP`
    /// blocks for at most 5s); this is sized well above the worst
    /// legitimate one, every HTTP call in it being bounded by
    /// `common::ingest::CONSUMER_REQUEST_TIMEOUT` (60s).
    #[arg(long, env, default_value_t = 300)]
    pub progress_stall_secs: u64,
    #[arg(long, env, default_value_t = 9096)]
    pub metrics_port: u16,
    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,

    /// Global kill switch for
    /// `common::trust_timestamp::parse_trust_epoch_millis_pair`'s
    /// Europe/London-mislabelling correction. Identical field, reasoning,
    /// and default (`true`) as `crates/trust-consumer/src/config.rs`'s own
    /// `trust_timestamp_correction_enabled` -- see that field's doc
    /// comment for the full rationale, mirrored from
    /// `crates/aggregator/src/config.rs`'s `full_coverage_enabled_default`
    /// pattern.
    #[arg(long, env, default_value_t = true)]
    pub trust_timestamp_correction_enabled: bool,
}

/// `INGEST_SINK`: see [`Config::ingest_sink`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum IngestSink {
    #[default]
    Http,
    Db,
}

impl Config {
    /// Cross-field checks clap cannot express: `db` needs a `DATABASE_URL`.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.ingest_sink == IngestSink::Db
            && self
                .database_url
                .as_ref()
                .is_none_or(common::secret::Secret::is_empty)
        {
            anyhow::bail!("INGEST_SINK=db needs DATABASE_URL");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Config, IngestSink};

    fn parse(extra: &[&str]) -> Result<Config, clap::Error> {
        let stanox = common::manifest_dir!().join("../../reference-data/stanox-crs.csv");
        let lines = common::manifest_dir!().join("../../lines");
        let base = [
            "trust-backlog-consumer".to_owned(),
            "--stanox-crs-file".to_owned(),
            stanox.display().to_string(),
            "--lines-dir".to_owned(),
            lines.display().to_string(),
            "--internal-oauth-token-url".to_owned(),
            "http://authentik.example/token".to_owned(),
            "--internal-oauth-client-id".to_owned(),
            "client-id".to_owned(),
            "--internal-oauth-username".to_owned(),
            "svc".to_owned(),
            "--internal-oauth-password".to_owned(),
            "pw".to_owned(),
        ];
        Config::try_parse_from(
            base.into_iter()
                .chain(extra.iter().map(|s| (*s).to_owned())),
        )
    }

    /// The default is today's behaviour: POST to the api, no database.
    #[test]
    fn the_sink_defaults_to_http_and_db_needs_a_database_url() {
        // `--database-url ""`: the test environment may set DATABASE_URL.
        let config = parse(&["--database-url", ""]).unwrap();
        assert_eq!(config.ingest_sink, IngestSink::Http);
        config.validate().unwrap();

        let db = parse(&["--ingest-sink", "db", "--database-url", ""]).unwrap();
        assert_eq!(db.ingest_sink, IngestSink::Db);
        assert!(db.validate().is_err(), "db without DATABASE_URL");

        let db = parse(&[
            "--ingest-sink",
            "db",
            "--database-url",
            "postgres://distant_signal_trust_backlog:pw@postgres/ds",
        ])
        .unwrap();
        db.validate().unwrap();
        assert!(
            !format!("{db:?}").contains(":pw@"),
            "the database URL must not appear in Debug output"
        );

        assert!(parse(&["--ingest-sink", "stream"]).is_err());
    }

    /// The chart sets these names (templates/trust-backlog-consumer-deployment.yaml).
    #[test]
    fn the_new_settings_read_the_chart_s_env_names() {
        use clap::CommandFactory;
        let command = Config::command();
        let env = |id: &str| {
            command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(|arg| arg.get_env())
                .map(|env| env.to_string_lossy().into_owned())
        };
        assert_eq!(env("ingest_sink").as_deref(), Some("INGEST_SINK"));
        assert_eq!(env("database_url").as_deref(), Some("DATABASE_URL"));
    }
}
