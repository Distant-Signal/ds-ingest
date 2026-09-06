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
pub struct Config {
    #[arg(long, env, default_value = "redis://redis:6379")]
    pub redis_url: String,

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
    /// batches.
    #[arg(
        long,
        env,
        default_value = "http://api:8080/private/trust-event-backlog"
    )]
    pub api_ingest_url: String,

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
    #[arg(long, env, default_value_t = 9096)]
    pub metrics_port: u16,
    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,
}
