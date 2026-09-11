use clap::Parser;
use common::config::{LineCatalogue, parse_lines};

/// Which transport this crate's `MovementFeed` uses -- now defined once in
/// `movement_feed`, re-exported here so every existing
/// `use config::{Config, MovementFeedBackend};` import keeps resolving
/// unchanged.
pub use movement_feed::MovementFeedBackend;

/// CLI/env configuration for the `full-coverage-consumer` service -- a
/// second, independent Kafka consumer against the same RDM Train
/// Movements feed `trust-consumer` reads, correlating every event against
/// the FULL scheduled population of every shadow-computed line. See
/// docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md and
/// docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md Task 8.
#[derive(Debug, Parser)]
pub struct Config {
    // Kafka: brokers/topic/consumer-group/sasl -- REUSES trustConsumer's
    // broker/topic/mechanism values at the Helm layer (Task 15), but
    // still has its own consumer_group default and its own env var
    // names at the crate/binary layer, per Decision 1's "connection vs.
    // group membership" reasoning.
    #[command(flatten)]
    pub kafka: common::service_args::KafkaConnectionArgs,
    #[arg(long, env, default_value = "distant-signal-full-coverage-consumer")]
    pub kafka_consumer_group: String,

    // api endpoints
    #[arg(
        long,
        env,
        default_value = "http://api:8080/private/schedule-line-population"
    )]
    pub schedule_line_population_url: String,
    #[arg(
        long,
        env,
        default_value = "http://api:8080/private/full-coverage-stats"
    )]
    pub full_coverage_stats_url: String,
    /// The OTHER chain's own endpoint -- see this plan's Non-goals for the
    /// merge-order dependency this URL implies (per-station-full-coverage-stats
    /// plan owns the migration/route; this crate is only ever an HTTP
    /// client of it).
    #[arg(
        long,
        env,
        default_value = "http://api:8080/private/station-full-coverage-samples"
    )]
    pub station_full_coverage_stats_url: String,
    #[arg(long, env, default_value = "http://api:8080/private/stanox-crs")]
    pub stanox_crs_url: String,

    // Shared+distinct OAuth2 (Decision 5 -- same shape as every other caller)
    #[command(flatten)]
    pub internal_oauth: common::oauth_client::InternalOAuthArgs,

    // Reload cadences
    #[arg(long, env, default_value_t = 300)]
    pub population_reload_secs: u64,
    #[arg(long, env, default_value_t = 3600)]
    pub stanox_crs_reload_secs: u64,
    #[arg(long, env, default_value_t = 60)]
    pub stats_write_interval_secs: u64,

    /// Decision 4 -- comma-separated line ids to shadow-compute, or "*"
    /// (default) for every catalogued line with at least one station
    /// resolving to a real, CIF-derived TIPLOC via `stanox_crs` (see
    /// `shadow_line_ids`'s own doc for why this is no longer the
    /// hand-curated `lines/*.toml` `tiploc` field).
    /// Does NOT gate whether a line's stats are ever shown/escalated --
    /// that's `LineDefinition.full_coverage_enabled`, unchanged, in
    /// `aggregator`.
    #[arg(long, env, default_value = "*")]
    pub shadow_lines: String,

    // Static line catalogue, same value_parser pattern as aggregator's
    // own --lines-dir (needed to build the reverse tiploc->line index,
    // Task 9).
    #[arg(long = "lines-dir", env = "LINES_DIR", default_value = "/app/lines", value_parser = parse_lines)]
    pub lines: LineCatalogue,

    #[arg(long, env, default_value = "0.0.0.0:8082")]
    pub health_bind_url: String,
    #[arg(long, env, default_value_t = 9093)]
    pub metrics_port: u16,
    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,

    /// Which transport this crate's `MovementFeed` uses. Defaults to
    /// `kafka` -- Deploy A (docs/superpowers/plans/2026-09-04-movement-relay-plan.md)
    /// changes nothing about production behavior until this is explicitly
    /// flipped. See `MovementFeedBackend`'s own doc.
    #[arg(long, env, value_enum, default_value_t = MovementFeedBackend::Kafka)]
    pub movement_feed_backend: MovementFeedBackend,

    /// Only read when `movement_feed_backend = redis-stream`. See
    /// `trust-consumer/src/config.rs`'s identical field for the full
    /// reasoning on why this is always required regardless of backend.
    #[arg(long, env, default_value = "redis://redis:6379")]
    pub redis_url: String,

    /// See `trust-consumer/src/config.rs`'s identical field.
    #[arg(long, env, default_value_t = 30)]
    pub redis_autoclaim_min_idle_secs: u64,

    /// How often (seconds), under the `redis-stream` backend only, this
    /// crate compares the `full-coverage-consumer` Redis Streams consumer
    /// group's `last-delivered-id` against the stream's oldest retained
    /// entry (`RedisStreamMovementFeed::check_gap`). A no-op timer under
    /// the `kafka` backend.
    #[arg(long, env, default_value_t = 60)]
    pub redis_gap_check_secs: u64,
}

impl Config {
    /// Resolves `shadow_lines` against the real catalogue: `"*"` means
    /// every line with at least one station resolving to a real,
    /// CIF-derived TIPLOC via `stanox_crs_records`; otherwise, only the
    /// comma-separated ids named, intersected with the real catalogue (an
    /// unknown id in the list is silently ignored, not an error -- an
    /// operator typo here should degrade to "shadow fewer lines than
    /// intended", never crash-loop the consumer).
    ///
    /// As of the 2026-09-11 tiploc-schedule-matching-gap fix, the `"*"`
    /// branch no longer gates on the hand-curated `lines/*.toml`
    /// `Station.tiploc` field (`.is_some()`): that field is optional and
    /// mostly absent (40 of 109 `lines/*.toml` files have ZERO manually-
    /// filled `tiploc` entries at all), so gating on it silently excluded
    /// ~37% of the line catalogue from shadow/full-coverage computation
    /// entirely at startup -- this crate's fifth independent copy of the
    /// same bug already fixed (fourth time, in this very crate) in
    /// `population::build_tiploc_index`. A line's real TIPLOC coverage is
    /// now determined by whether any of its stations' CRS codes appear in
    /// a live `stanox_crs` snapshot (always present for a real station,
    /// unlike the TOML field), mirroring `population::crs_to_tiploc_map`'s
    /// reasoning. Callers must fetch `stanox_crs_records` before relying on
    /// this for `"*"` resolution -- see `main.rs`'s startup sequencing.
    pub fn shadow_line_ids(&self, stanox_crs_records: &[common::StanoxCrsRecord]) -> Vec<String> {
        if self.shadow_lines.trim() == "*" {
            let known_crs: std::collections::HashSet<String> = stanox_crs_records
                .iter()
                .map(|r| r.crs.to_uppercase())
                .collect();
            return self
                .lines
                .iter()
                .filter(|l| {
                    l.stations
                        .iter()
                        .any(|s| known_crs.contains(&s.crs.to_uppercase()))
                })
                .map(|l| l.id.clone())
                .collect();
        }
        let requested: std::collections::HashSet<&str> = self
            .shadow_lines
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        self.lines
            .iter()
            .filter(|l| requested.contains(l.id.as_str()))
            .map(|l| l.id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use common::LineDefinition;

    use super::*;

    fn fixture_line(id: &str, tiploc: Option<&str>) -> LineDefinition {
        fixture_line_with_crs(id, "ZZZ", tiploc)
    }

    /// Like `fixture_line`, but with a caller-chosen CRS -- needed to
    /// exercise `stanox_crs_records`-based wildcard resolution, where
    /// different lines must resolve to different real/no-real CIF
    /// coverage outcomes.
    fn fixture_line_with_crs(id: &str, crs: &str, tiploc: Option<&str>) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "rail".to_string(),
            category: "national-rail".to_string(),
            operators: vec![],
            stations: vec![common::Station {
                crs: crs.to_string(),
                tiploc: tiploc.map(str::to_string),
                role: "minor".to_string(),
                segment: None,
            }],
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: std::collections::HashMap::new(),
            exclusive_segments: vec![],
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
        }
    }

    fn fixture_stanox_crs_record(crs: &str) -> common::StanoxCrsRecord {
        common::StanoxCrsRecord {
            stanox: format!("STANOX-{crs}"),
            crs: crs.to_string(),
            tiploc: format!("{crs}TPL"),
            station_name: format!("{crs} STATION"),
            source_sequence: 1,
        }
    }

    fn base_config(lines: Vec<LineDefinition>, shadow_lines: &str) -> Config {
        Config {
            kafka: common::service_args::KafkaConnectionArgs {
                kafka_brokers: String::new(),
                kafka_topic: String::new(),
                kafka_sasl_username: String::new(),
                kafka_sasl_password: String::new(),
                kafka_sasl_mechanism: String::new(),
            },
            kafka_consumer_group: String::new(),
            schedule_line_population_url: String::new(),
            full_coverage_stats_url: String::new(),
            station_full_coverage_stats_url: String::new(),
            stanox_crs_url: String::new(),
            internal_oauth: common::oauth_client::InternalOAuthArgs {
                internal_oauth_token_url: String::new(),
                internal_oauth_client_id: String::new(),
                internal_oauth_scope: String::new(),
                internal_oauth_username: String::new(),
                internal_oauth_password: String::new(),
            },
            population_reload_secs: 300,
            stanox_crs_reload_secs: 3600,
            stats_write_interval_secs: 60,
            shadow_lines: shadow_lines.to_string(),
            lines: LineCatalogue(lines),
            health_bind_url: String::new(),
            metrics_port: 9093,
            metrics: common::service_args::MetricsArgs {
                metrics_enabled: false,
            },
            movement_feed_backend: MovementFeedBackend::Kafka,
            redis_url: String::new(),
            redis_autoclaim_min_idle_secs: 30,
            redis_gap_check_secs: 60,
        }
    }

    /// The regression test for this crate's fifth independent instance of
    /// the tiploc-schedule-matching-gap bug (2026-09-11): `"*"` wildcard
    /// resolution used to gate on the hand-curated `lines/*.toml`
    /// `Station.tiploc` field (`.is_some()`), which 40 of 109
    /// `lines/*.toml` files never fill in for even one station -- so this
    /// startup gate silently excluded them from shadow/full-coverage
    /// computation entirely, even though the (already-fixed)
    /// `population::build_tiploc_index` matching logic underneath would
    /// have worked correctly for them once selected. A line with no TOML
    /// `tiploc` on any station, but real CIF-confirmed TIPLOC coverage via
    /// `stanox_crs`, must now be included; a line with genuinely no
    /// matching `stanox_crs` record for any of its stations' CRS codes
    /// must still be excluded.
    #[test]
    fn wildcard_shadow_lines_includes_a_line_with_no_toml_tiploc_via_real_cif_data() {
        let config = base_config(
            vec![
                fixture_line_with_crs("with-cif-coverage", "ZNT", None),
                fixture_line_with_crs("without-cif-coverage", "ZZZ", None),
            ],
            "*",
        );
        let records = vec![fixture_stanox_crs_record("ZNT")];
        assert_eq!(
            config.shadow_line_ids(&records),
            vec!["with-cif-coverage".to_string()]
        );
    }

    #[test]
    fn explicit_shadow_lines_intersects_with_the_real_catalogue() {
        let config = base_config(
            vec![
                fixture_line("line-a", Some("AAATPL")),
                fixture_line("line-b", Some("BBBTPL")),
            ],
            "line-b, line-unknown",
        );
        assert_eq!(config.shadow_line_ids(&[]), vec!["line-b".to_string()]);
    }

    /// The concrete regression test for "Deploy A changes nothing about
    /// default production behavior" (docs/superpowers/plans/2026-09-04-movement-relay-plan.md
    /// Task 4): parsing only the pre-existing required arguments -- none of
    /// this plan's new flags -- must still yield `MovementFeedBackend::Kafka`.
    #[test]
    fn movement_feed_backend_defaults_to_kafka_when_unset() {
        let lines_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lines");

        let config = Config::try_parse_from([
            "full-coverage-consumer",
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
            "--lines-dir",
            lines_dir.to_str().unwrap(),
        ])
        .expect("minimal required args should parse");

        assert_eq!(config.movement_feed_backend, MovementFeedBackend::Kafka);
    }
}
