use std::path::Path;

use clap::{Parser, ValueHint};

use crate::stanox_crs::StanoxCrsTable;

fn parse_stanox_crs(path: &str) -> anyhow::Result<StanoxCrsTable> {
    StanoxCrsTable::from_file(Path::new(path))
}

/// Which transport this crate's `MovementFeed` uses -- now defined once in
/// `movement_feed`, re-exported here so every existing
/// `use config::{Config, MovementFeedBackend};` import (and this file's own
/// `#[arg(..., value_enum, default_value_t = MovementFeedBackend::Kafka)]`)
/// keeps resolving unchanged.
pub use movement_feed::MovementFeedBackend;

/// CLI/env configuration for the `trust-consumer` service.
///
/// `kafka_topic` and `kafka_sasl_mechanism` deliberately have no default:
/// the exact RDM Train Movements topic name and SASL mechanism (PLAIN vs
/// SCRAM) were not confirmed against a live RDM catalogue entry in this
/// feature's design research (see
/// docs/superpowers/specs/2026-08-28-train-tracking-design.md's Open
/// Questions #1-#3) -- this must be supplied out of band once a real RDM
/// Train Movements subscription exists, not guessed. Same posture as
/// `crates/poller-tocs/src/config.rs`'s `rdm_tocs_base_url`.
#[derive(Debug, Parser)]
pub struct Config {
    /// RDM Kafka broker/topic/SASL connection config, shared across
    /// `trust-consumer`/`full-coverage-consumer`/`movement-relay`.
    #[command(flatten)]
    pub kafka: common::service_args::KafkaConnectionArgs,

    /// Consumer group id. Fixed per deployment, not per-process -- multiple
    /// trust-consumer replicas sharing one group would each get a subset
    /// of partitions, which is fine for horizontal scaling but NOT this
    /// plan's v1 (single replica; see Helm chart task).
    #[arg(long, env, default_value = "distant-signal-trust-consumer")]
    pub kafka_consumer_group: String,

    /// The `api` crate's ingestion endpoint for train movement events.
    #[arg(long, env, default_value = "http://api:8080/private/train-events")]
    pub api_ingest_url: String,

    /// The `api` crate's endpoint listing active tracked trains.
    #[arg(long, env, default_value = "http://api:8080/private/tracked-trains")]
    pub api_tracked_trains_url: String,

    /// Shared, non-secret OAuth2 client-credentials config (same value
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
    /// `health.rs`). A persistent Kafka consumer needs
    /// connected/reconnecting/disconnected health semantics, not the
    /// "last poll succeeded at T" shape every cron-style poller uses --
    /// see docs/superpowers/specs/2026-08-28-train-tracking-design.md's
    /// Open Questions #6.
    #[arg(long, env, default_value = "0.0.0.0:8081")]
    pub health_bind_url: String,

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
    /// `MovementFeedBackend`'s own doc. Defaults to `kafka` -- Deploy A
    /// (docs/superpowers/plans/2026-09-04-movement-relay-plan.md) changes
    /// nothing about production behavior until this is explicitly flipped.
    #[arg(long, env, value_enum, default_value_t = MovementFeedBackend::Kafka)]
    pub movement_feed_backend: MovementFeedBackend,

    /// Only read when `movement_feed_backend = redis-stream`. Always
    /// required regardless of the selected backend -- Redis is already an
    /// always-on chart-level dependency (`redis.enabled: true`), so
    /// requiring this unconditionally is simpler than making it
    /// conditionally required, and costs nothing when unused under the
    /// `kafka` backend.
    #[arg(long, env, default_value = "redis://redis:6379")]
    pub redis_url: String,

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

    /// How often (seconds), under the `redis-stream` backend only, this
    /// crate compares the `trust-consumer` Redis Streams consumer group's
    /// `last-delivered-id` against the stream's oldest retained entry
    /// (`RedisStreamMovementFeed::check_gap`) -- the design doc Decision
    /// 2's "definitive gap detection" mechanism. Same cadence shape as
    /// `reference_reload_secs`/`stanox_crs_reload_secs`. A no-op timer
    /// under the `kafka` backend.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The concrete regression test for "Deploy A changes nothing about
    /// default production behavior" (docs/superpowers/plans/2026-09-04-movement-relay-plan.md
    /// Task 4): parsing only the pre-existing required arguments -- none of
    /// this plan's new flags -- must still yield `MovementFeedBackend::Kafka`.
    #[test]
    fn movement_feed_backend_defaults_to_kafka_when_unset() {
        // The real, checked-in reference-data/stanox-crs.csv, since
        // --stanox-crs-file's default value is parsed eagerly (its
        // value_parser opens and parses the file) even when this test never
        // touches STANOX/CRS behavior -- mirrors main.rs's own
        // TEST_STANOX_CRS test fixture path.
        let stanox_crs_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../reference-data/stanox-crs.csv");

        let config = Config::try_parse_from([
            "trust-consumer",
            "--kafka-brokers",
            "kafka.example.com:9092",
            "--kafka-topic",
            "test-topic",
            "--kafka-sasl-username",
            "user",
            "--kafka-sasl-password",
            "pass",
            "--kafka-sasl-mechanism",
            "PLAIN",
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

        assert_eq!(config.movement_feed_backend, MovementFeedBackend::Kafka);
    }
}
