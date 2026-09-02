use std::path::Path;

use clap::{Parser, ValueHint};

use crate::stanox_crs::StanoxCrsTable;

fn parse_stanox_crs(path: &str) -> anyhow::Result<StanoxCrsTable> {
    StanoxCrsTable::from_file(Path::new(path))
}

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
    /// RDM Kafka broker address(es), comma-separated, e.g.
    /// `kafka.raildata.org.uk:9094`. GAP: unconfirmed hostname.
    #[arg(long, env)]
    pub kafka_brokers: String,

    /// GAP: unconfirmed exact topic name for the Train Movements product.
    #[arg(long, env)]
    pub kafka_topic: String,

    /// Consumer group id. Fixed per deployment, not per-process -- multiple
    /// trust-consumer replicas sharing one group would each get a subset
    /// of partitions, which is fine for horizontal scaling but NOT this
    /// plan's v1 (single replica; see Helm chart task).
    #[arg(long, env, default_value = "distant-signal-trust-consumer")]
    pub kafka_consumer_group: String,

    /// RDM's "Consumer key" for this product (SASL username).
    #[arg(long, env)]
    pub kafka_sasl_username: String,

    /// RDM's "Consumer secret" for this product (SASL password).
    #[arg(long, env)]
    pub kafka_sasl_password: String,

    /// GAP: unconfirmed whether RDM's Kafka product uses PLAIN or a SCRAM
    /// variant. PLAIN is `librdkafka`'s simplest, most common default for
    /// managed Kafka-as-a-service offerings, but this is an assumption,
    /// not a confirmed fact -- reject silently guessing wrong by requiring
    /// this be set explicitly rather than defaulting it.
    #[arg(long, env)]
    pub kafka_sasl_mechanism: String,

    /// The `api` crate's ingestion endpoint for train movement events.
    #[arg(long, env, default_value = "http://api:8080/private/train-events")]
    pub api_ingest_url: String,

    /// The `api` crate's endpoint listing active tracked trains.
    #[arg(long, env, default_value = "http://api:8080/private/tracked-trains")]
    pub api_tracked_trains_url: String,

    /// Shared, non-secret OAuth2 client-credentials config (same value
    /// across all 8 real callers) -- see
    /// docs/superpowers/specs/2026-09-02-internal-service-oauth2-design.md
    /// Decision 6.
    #[arg(long, env)]
    pub internal_oauth_token_url: String,
    #[arg(long, env)]
    pub internal_oauth_client_id: String,
    #[arg(long, env, default_value = "groups")]
    pub internal_oauth_scope: String,
    /// This service's own Authentik service-account credential --
    /// per-service, distinct from every other caller's. `username` is
    /// identifying, not itself the secret; `password` (an Authentik
    /// app-password) is the actual secret.
    #[arg(long, env)]
    pub internal_oauth_username: String,
    #[arg(long, env)]
    pub internal_oauth_password: String,

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
}
