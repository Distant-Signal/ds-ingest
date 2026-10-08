use std::path::Path;

use clap::{Parser, ValueHint};

use crate::stanox_crs::StanoxCrsTable;

fn parse_stanox_crs(path: &str) -> anyhow::Result<StanoxCrsTable> {
    StanoxCrsTable::from_file(Path::new(path))
}

/// Which transport this crate's `MovementFeed` uses -- now defined once in
/// `movement_feed`, re-exported here so every existing
/// `use config::{Config, MovementFeedBackend};` import keeps resolving
/// unchanged.
pub(crate) use movement_feed::MovementFeedBackend;

/// CLI/env configuration for the `trust-consumer` service. It reads the
/// `movement-events` Redis stream movement-relay publishes and has no Kafka
/// connection of its own (Deploy C, PL-15a).
#[derive(Debug, Parser)]
pub(crate) struct Config {
    /// The `api` crate's ingestion endpoint for train movement events.
    /// Used only with `INGEST_SINK=http`.
    #[arg(long, env, default_value = "http://api:8080/private/train-events")]
    pub api_ingest_url: String,

    /// Where each batch's train events and forward signals go (ingest
    /// architecture plan 3b.3, decision D1): `http` (the default, today's
    /// behaviour) POSTs them to `API_INGEST_URL` and `FORWARD_SIGNALS_URL`;
    /// `db` writes both to Postgres directly in one transaction, as the
    /// `trust_consumer` role. The reads (tracked trains, STANOX/CRS) switch
    /// separately (`reads`, plan 4.4). See `sink.rs`.
    #[arg(long, env, value_enum, default_value_t = IngestSink::Http)]
    pub ingest_sink: IngestSink,

    /// Postgres, for `INGEST_SINK=db` or a `db` read source (required then,
    /// unused otherwise): one URL and one pool for both, as the
    /// `trust_consumer` role. Pool size and timeouts come from the shared
    /// `DATABASE_*` variables (`common::pg`); the default pool is 2 (spec
    /// §6.6, role limit 3).
    #[arg(long, env, hide_env_values = true)]
    pub database_url: Option<common::secret::Secret>,

    /// The `api` crate's endpoint listing active tracked trains.
    #[arg(long, env, default_value = "http://api:8080/private/tracked-trains")]
    pub api_tracked_trains_url: String,

    /// The `api` crate's ingestion endpoint for the notifier-forwarding
    /// queue (Task 17) -- fast-path signals so `notifier` can poll for
    /// newly-arrived shared-store events sooner than its existing slower
    /// poll of `train_movement_events`.
    #[arg(
        long,
        env,
        default_value = "http://api:8080/private/train-forward-signals"
    )]
    pub forward_signals_url: String,

    /// Shared, non-secret `OAuth2` client-credentials config (same value
    /// across all 9 real callers).
    #[command(flatten)]
    pub internal_oauth: common::oauth_client::InternalOAuthArgs,

    /// How often to reload the active-tracked-trains reference set from
    /// `api` -- picks up newly created pins and pins that resolved on a
    /// prior run before this process restarted.
    #[arg(long, env, default_value_t = 60)]
    pub reference_reload_secs: u64,

    /// How long to keep `train_movement_events` rows before pruning.
    /// `tracked_trains`/`train_current_state` are kept indefinitely (see
    /// this plan's Global Constraints).
    #[arg(long, env, default_value_t = 90)]
    pub retention_days: i64,

    /// Bind address for the `/healthz` liveness endpoint (Task 7's
    /// `health.rs`). A persistent stream consumer needs
    /// connected/reconnecting/disconnected health semantics, not the
    /// "last poll succeeded at T" shape every cron-style poller uses --
    /// see docs/superpowers/specs/2026-08-28-train-tracking-design.md's
    /// Open Questions #6.
    #[arg(long, env, default_value = "0.0.0.0:8081")]
    pub health_bind_url: String,
    /// Liveness watchdog: `/healthz` answers 503 ("stalled") once no
    /// consume-loop iteration has completed for this many seconds, so a
    /// loop wedged inside an `await` gets restarted by the liveness probe
    /// (which still needs its own `failureThreshold * periodSeconds` on top
    /// of this). A healthy iteration takes a few seconds (the `XREADGROUP`
    /// blocks for at most 5s); this is sized well above the worst
    /// legitimate one, every HTTP call in it being bounded by
    /// `common::ingest::CONSUMER_REQUEST_TIMEOUT` (60s) and every Redis
    /// command by `common::redis_conn` (one reconnect attempt of at most
    /// 5s, a reply within 30s; a failed command ends the cycle). A Redis
    /// outage is therefore a run of fast failed cycles, each beating
    /// progress, never one stalled iteration.
    #[arg(long, env, default_value_t = 300)]
    pub progress_stall_secs: u64,

    /// STANOX->CRS translation table, loaded once at startup. See
    /// `crate::stanox_crs`'s module doc for the file format and
    /// `reference-data/stanox-crs.md` for full provenance. Baked into the
    /// image at `/app/reference-data/stanox-crs.csv` by
    /// `docker/trust-consumer.Dockerfile`, same pattern as `aggregator`'s
    /// `--lines-dir`/`LINES_DIR` (`crates/aggregator/src/config.rs`) --
    /// though unlike that one this is a single file, not a directory, so
    /// no `LineCatalogue`-style `Vec`-shaped newtype is needed:
    /// `StanoxCrsTable` isn't a `Vec<T>`, so `clap_derive` doesn't
    /// misinfer its arg-collection behaviour the way it would for one.
    #[arg(
        long = "stanox-crs-file",
        env = "STANOX_CRS_FILE",
        default_value = "/app/reference-data/stanox-crs.csv",
        value_parser = parse_stanox_crs,
        value_hint = ValueHint::FilePath,
        value_name = "FILE"
    )]
    pub stanox_crs: StanoxCrsTable,

    /// How often to reload the live STANOX->CRS table from `api`. Deliberately
    /// coarser than `reference_reload_secs`'s 60s default -- the underlying
    /// data changes roughly daily (Decision 4), so "promptly" only matters
    /// relative to that, not to a human creating a pin. UNRESEARCHED
    /// starting figure, same posture as `MINE_LIST_LIMIT`/`MAX_PIN_AGE`
    /// elsewhere in this codebase (see the spec's Open questions #1).
    #[arg(long, env, default_value_t = 3600)]
    pub stanox_crs_reload_secs: u64,

    /// The `api` crate's endpoint for the live STANOX/CRS table.
    #[arg(long, env, default_value = "http://api:8080/private/stanox-crs")]
    pub stanox_crs_url: String,

    /// Which transport this crate's `MovementFeed` uses. See
    /// `MovementFeedBackend`'s own doc. Only `redis-stream` (the
    /// `movement-events` stream movement-relay publishes) remains. The
    /// legacy direct-Kafka backend was removed in Deploy C (PL-15a, R-101)
    /// and an explicit `kafka` now fails startup: the chart had passed this
    /// consumer movement-relay's own RDM consumer group, so honouring it
    /// would have split the relay's partitions.
    #[arg(long, env, default_value_t = MovementFeedBackend::RedisStream)]
    pub movement_feed_backend: MovementFeedBackend,

    /// The `movement-events` Redis stream's server.
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
    /// pending-entries list before `RedisStreamMovementFeed`'s periodic
    /// sweep reclaims it. Sized small relative to `enricher`'s own
    /// `reclaimMinIdleSecs` (1000s) -- see
    /// docs/superpowers/specs/2026-09-04-movement-relay-design.md
    /// Decision 2's own note: this crate's cycle latency (consume ->
    /// derive -> POST to api -> ack) should be sub-second in the healthy
    /// case, unlike enricher's slower LLM-call latency.
    #[arg(long, env, default_value_t = 30)]
    pub redis_autoclaim_min_idle_secs: u64,

    /// How often (seconds) this crate compares the `trust-consumer` Redis Streams consumer group's
    /// `last-delivered-id` against the stream's oldest retained entry
    /// (`RedisStreamMovementFeed::check_gap`) -- the design doc Decision
    /// 2's "definitive gap detection" mechanism. Same cadence shape as
    /// `reference_reload_secs`/`stanox_crs_reload_secs`.
    #[arg(long, env, default_value_t = 60)]
    pub redis_gap_check_secs: u64,

    /// Prometheus metrics port. Off (`metrics_enabled: false`) by default
    /// today because this crate has never needed one before this plan --
    /// added here so `trust_consumer_stream_gap_detected_total` (Task 4)
    /// has somewhere real to be scraped from. Mirrors
    /// `full-coverage-consumer/src/config.rs`'s identical pair of fields.
    #[arg(long, env, default_value_t = 9095)]
    pub metrics_port: u16,
    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,

    /// Global kill switch for
    /// `common::trust_timestamp::parse_trust_epoch_millis_pair`'s
    /// Europe/London-mislabelling correction (Finding #2 of the
    /// TRUST-timestamp-correction fix). The hypothesis this correction
    /// applies is well-evidenced but NOT vendor-confirmed, and the
    /// plausibility guard it's built on can only catch under-correction,
    /// never over-correction (a real, wrong-direction failure mode if the
    /// upstream feed vendor silently fixes their own bug tomorrow -- see
    /// that function's own module doc). Default `true` (correction on,
    /// since this codebase is choosing to ship it) mirrors
    /// `crates/aggregator/src/config.rs`'s `full_coverage_enabled_default`
    /// pattern exactly: a config struct field, an env var read at startup,
    /// defaulting to today's chosen behavior so nothing changes for a
    /// deployment that doesn't explicitly set it, with an operator able to
    /// flip it to `false` the instant the correction itself becomes the
    /// suspect, without a rebuild.
    #[arg(long, env, default_value_t = true)]
    pub trust_timestamp_correction_enabled: bool,

    /// `TRACKED_TRAINS_SOURCE` and `STANOX_CRS_SOURCE` (ingest architecture
    /// plan 4.4; their database is `database_url`): see `reads.rs`.
    #[command(flatten)]
    pub reads: crate::reads::InternalReadArgs,
}

/// `INGEST_SINK`: see [`Config::ingest_sink`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum IngestSink {
    #[default]
    Http,
    Db,
}

impl Config {
    /// Whether anything reads or writes Postgres: the sink or a source is
    /// `db`.
    pub(crate) fn needs_database(&self) -> bool {
        self.ingest_sink == IngestSink::Db || self.reads.any_db()
    }

    /// Cross-field checks clap cannot express: a `db` sink or source needs
    /// a `DATABASE_URL`.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.needs_database()
            && self
                .database_url
                .as_ref()
                .is_none_or(common::secret::Secret::is_empty)
        {
            anyhow::bail!(
                "INGEST_SINK=db, TRACKED_TRAINS_SOURCE=db or STANOX_CRS_SOURCE=db needs DATABASE_URL"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PL-15a: with no backend flag, the consumer reads the Redis stream,
    /// never Kafka directly.
    #[test]
    fn movement_feed_backend_defaults_to_redis_stream_when_unset() {
        // The real, checked-in reference-data/stanox-crs.csv, since
        // --stanox-crs-file's default value is parsed eagerly (its
        // value_parser opens and parses the file) even when this test never
        // touches STANOX/CRS behavior -- mirrors main.rs's own
        // TEST_STANOX_CRS test fixture path.
        let stanox_crs_file = common::manifest_dir!().join("../../reference-data/stanox-crs.csv");

        let config = Config::try_parse_from([
            "trust-consumer",
            "--internal-oauth-token-url",
            "http://auth.example.com/token",
            "--internal-oauth-client-id",
            "client-id",
            "--internal-oauth-username",
            "svc-user",
            "--internal-oauth-password",
            "svc-pass",
            "--stanox-crs-file",
            stanox_crs_file.to_str().unwrap(),
        ])
        .expect("minimal required args should parse");

        assert_eq!(
            config.movement_feed_backend,
            MovementFeedBackend::RedisStream
        );
    }

    /// R-101 / Deploy C: an explicit `MOVEMENT_FEED_BACKEND=kafka` refuses
    /// to start, with the reason, instead of being ignored (or, as before,
    /// joining movement-relay's own Kafka consumer group).
    #[test]
    fn an_explicit_kafka_backend_fails_startup() {
        let stanox_crs_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../reference-data/stanox-crs.csv");
        let parse = |backend: &str| {
            Config::try_parse_from([
                "trust-consumer",
                "--internal-oauth-token-url",
                "http://auth.example.com/token",
                "--internal-oauth-client-id",
                "client-id",
                "--internal-oauth-username",
                "svc-user",
                "--internal-oauth-password",
                "svc-pass",
                "--stanox-crs-file",
                stanox_crs_file.to_str().unwrap(),
                "--movement-feed-backend",
                backend,
            ])
        };

        let err = parse("kafka").expect_err("the kafka backend was removed");
        assert!(
            err.to_string()
                .contains(movement_feed::active_feed::KAFKA_BACKEND_REMOVED),
            "{err}"
        );
        assert_eq!(
            parse("redis-stream").unwrap().movement_feed_backend,
            MovementFeedBackend::RedisStream
        );
    }

    fn parse(extra: &[&str]) -> Result<Config, clap::Error> {
        let stanox_crs_file = common::manifest_dir!().join("../../reference-data/stanox-crs.csv");
        let base = [
            "trust-consumer".to_owned(),
            "--internal-oauth-token-url".to_owned(),
            "http://auth.example.com/token".to_owned(),
            "--internal-oauth-client-id".to_owned(),
            "client-id".to_owned(),
            "--internal-oauth-username".to_owned(),
            "svc-user".to_owned(),
            "--internal-oauth-password".to_owned(),
            "svc-pass".to_owned(),
            "--stanox-crs-file".to_owned(),
            stanox_crs_file.display().to_string(),
        ];
        Config::try_parse_from(
            base.into_iter()
                .chain(extra.iter().map(|s| (*s).to_owned())),
        )
    }

    /// Plan 3b.3: the default is today's behaviour, POSTs to the api.
    #[test]
    fn the_sink_defaults_to_http_and_db_needs_a_database_url() {
        // `--database-url ""`: the test environment may set DATABASE_URL.
        let config = parse(&["--database-url", ""]).unwrap();
        assert_eq!(config.ingest_sink, IngestSink::Http);
        config.validate().unwrap();

        let db = parse(&["--ingest-sink", "db", "--database-url", ""]).unwrap();
        assert!(db.validate().is_err(), "db without DATABASE_URL");

        let db = parse(&[
            "--ingest-sink",
            "db",
            "--database-url",
            "postgres://distant_signal_trust_consumer:pw@postgres/ds",
        ])
        .unwrap();
        assert_eq!(db.ingest_sink, IngestSink::Db);
        db.validate().unwrap();
        assert!(
            !format!("{db:?}").contains(":pw@"),
            "the database URL must not appear in Debug output"
        );
        assert!(parse(&["--ingest-sink", "stream"]).is_err());
    }

    /// The chart sets these names (templates/trust-consumer-deployment.yaml).
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
