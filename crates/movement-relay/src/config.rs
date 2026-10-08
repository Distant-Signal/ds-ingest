use clap::Parser;

/// CLI/env configuration for `movement-relay` -- the sole real Kafka
/// client against RDM's Train Movements product from Deploy B onward. See
/// docs/superpowers/specs/2026-09-04-movement-relay-design.md and
/// docs/superpowers/plans/2026-09-04-movement-relay-plan.md.
#[derive(Debug, Parser)]
pub(crate) struct Config {
    /// GAP: unconfirmed hostname until Deploy B's real credential is in
    /// hand -- same posture as trust-consumer/src/config.rs's own
    /// identical field.
    #[command(flatten)]
    pub kafka: common::service_args::KafkaConnectionArgs,

    /// The one real, RDM-issued group -- `SC-c4d90f8e-...` in production,
    /// per the design doc's "Why this exists" section. Deliberately no
    /// default: unlike trust-consumer's own `kafka_consumer_group` (which
    /// DOES have a sensible per-deployment default,
    /// "distant-signal-trust-consumer"), this crate's group id is a fixed,
    /// externally-issued, unforgeable identity -- guessing wrong here is
    /// worse than refusing to start.
    #[arg(long, env)]
    pub kafka_consumer_group: String,

    /// librdkafka's `auto.offset.reset`: where the relay starts reading a
    /// partition for which `kafka_consumer_group` has no committed offset
    /// (the broker expired the group's offsets, or they were deleted).
    /// `earliest` (the oldest message Kafka still retains) rather than
    /// librdkafka's default `latest`, which would silently skip everything
    /// published while the offset was missing. Why replaying is safe: see
    /// `kafka_source::KafkaRawSource::client_config`. `latest` is accepted
    /// only as an operator override (e.g. to skip a very long backlog).
    #[arg(
        long,
        env,
        default_value = "earliest",
        value_parser = clap::builder::PossibleValuesParser::new(["earliest", "latest"])
    )]
    pub kafka_auto_offset_reset: String,

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

    #[arg(long, env, default_value = "0.0.0.0:8083")]
    pub health_bind_url: String,
    /// Liveness watchdog: `/livez` (and `/healthz`) answer 503 ("stalled")
    /// once no relay-loop iteration has completed for this many seconds, so
    /// a loop wedged inside an `await` gets restarted by the liveness probe
    /// (which still needs its own `failureThreshold * periodSeconds` on top
    /// of this). Waiting on Kafka for the next record never counts
    /// (`Progress::idle`), and a failed cycle (Redis down) still completes
    /// and beats, so neither a traffic lull nor a Redis outage is a stall.
    ///
    /// 900s, not trust-consumer's 300s: one Kafka record fans out into
    /// ~218 sequential XADDs, which `kafka_source::MAX_POLL_INTERVAL_MS`
    /// (also 900s) budgets at up to ~7 minutes under a degraded Redis. A
    /// cycle longer than that has already cost this consumer its group
    /// membership, so it is a genuine wedge.
    #[arg(long, env, default_value_t = 900)]
    pub progress_stall_secs: u64,
    #[arg(long, env, default_value_t = 9094)]
    pub metrics_port: u16,
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,
    /// How often the leading-indicator lag gauge (`main::stream_lag_loop`)
    /// polls `XINFO GROUPS` for both downstream groups. UNRESEARCHED
    /// starting figure, same posture as every other first-guess cadence
    /// constant in this codebase (see trust-consumer/src/config.rs's own
    /// `stanox_crs_reload_secs` comment).
    #[arg(long, env, default_value_t = 30)]
    pub stream_lag_poll_secs: u64,

    /// `MAXLEN ~` cap on the `movement-events` stream, in entries. The
    /// default is derived from a memory budget, not picked as a count --
    /// see `DEFAULT_STREAM_MAXLEN`. Raise it only together with the Redis
    /// pod's `maxmemory` and memory limit (chart: `redis.maxmemory` /
    /// `redis.resources`), at roughly 1 KiB of Redis memory per entry.
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_STREAM_MAXLEN,
        value_parser = clap::value_parser!(u64).range(MIN_STREAM_MAXLEN..)
    )]
    pub movement_stream_maxlen: u64,

    /// Consumer groups created at the start of a fresh `movement-events`
    /// stream (missing, or empty at startup), comma-separated -- see
    /// `event_sink::create_groups`. Only groups whose consumer actually
    /// reads the stream belong here: a group nobody reads would show the
    /// whole stream as lag. The chart derives the list from each
    /// consumer's `movementFeed`.
    #[arg(
        long,
        env,
        value_delimiter = ',',
        default_values_t = crate::event_sink::DEFAULT_CONSUMER_GROUPS.map(String::from)
    )]
    pub movement_consumer_groups: Vec<String>,

    /// Dead-letter records older than this are deleted (D5; see
    /// `deadletter`). At most 24 hours -- the TRUST 1-day retention
    /// safeguard -- and at least one hour.
    #[arg(
        long,
        env,
        default_value_t = crate::deadletter::MAX_DEADLETTER_AGE_SECS,
        value_parser = clap::value_parser!(u64).range(
            crate::deadletter::MIN_DEADLETTER_AGE_SECS..=crate::deadletter::MAX_DEADLETTER_AGE_SECS
        )
    )]
    pub deadletter_max_age_secs: u64,
}

/// Memory the `movement-events` stream is allowed to occupy in Redis at
/// its cap: 512 MiB.
///
/// Sized for time, then bounded by bytes. The stream is the ONLY replay
/// source for its three consumer groups (trust-consumer,
/// full-coverage-consumer, trust-event-backlog): RDM issues a single Kafka
/// group, which movement-relay holds, so a downstream consumer cannot fall
/// back to Kafka. A downstream outage longer than the stream's window is a
/// permanent, detected-but-unrecoverable gap (`check_gap`). Restarts and
/// the 2026-09-26 node reboot cost minutes; the case worth covering is a
/// crash-looping consumer nobody notices overnight or across a working
/// day, so the target is about 12 hours of daytime traffic. Production
/// measured on 2026-09-26: ~1M entries/day (16.45M added in total), 500k
/// entries spanning 11.7h of daytime traffic (~43k/h), 920 bytes of Redis
/// memory per entry. 512 MiB / 1 KiB = 524,288 entries, about 12.3h at that
/// daytime rate and longer overnight, using ~460 MiB at 920 B/entry.
///
/// A count cap (`MAXLEN`), not a time cap (`MINID`), because the failure
/// being guarded against is memory: under `MINID` a busier day or larger
/// payloads grow the stream without bound, which is exactly how the
/// original 500,000 "~19h" figure (sized for ~630k/day) silently became
/// 11.7h. With `MAXLEN` the memory stays bounded and the time window is
/// what varies, which the lag gauge and `check_gap` already report.
pub(crate) const STREAM_MEMORY_BUDGET_BYTES: u64 = 512 * 1024 * 1024;

/// Planning figure for Redis memory per `movement-events` entry: 1 KiB.
/// Measured at 920 B in production (a ~750 B raw TRUST envelope in
/// `payload`, the 4-byte `msg_type`, and listpack/radix-tree overhead),
/// rounded up so that bigger payloads (activations) still fit the budget.
pub(crate) const STREAM_ENTRY_BYTES_ESTIMATE: u64 = 1024;

/// 524,288 entries. See `STREAM_MEMORY_BUDGET_BYTES`.
pub(crate) const DEFAULT_STREAM_MAXLEN: u64 =
    STREAM_MEMORY_BUDGET_BYTES / STREAM_ENTRY_BYTES_ESTIMATE;

/// Floor for `--movement-stream-maxlen`. `MAXLEN ~` only trims whole
/// stream nodes (`stream-node-max-entries`, 100 by default), so a
/// smaller cap is not honoured precisely. Any cap this small would also be
/// minutes of traffic, which is almost certainly a typo rather than a
/// deliberate setting.
pub(crate) const MIN_STREAM_MAXLEN: u64 = 1_000;

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    const REQUIRED: [&str; 11] = [
        "movement-relay",
        "--kafka-brokers",
        "k:9094",
        "--kafka-topic",
        "t",
        "--kafka-sasl-username",
        "u",
        "--kafka-sasl-password",
        "p",
        "--kafka-sasl-mechanism",
        "PLAIN",
    ];

    fn parse(extra: &[&str]) -> Result<Config, clap::Error> {
        let mut args: Vec<&str> = REQUIRED.to_vec();
        args.extend(["--kafka-consumer-group", "g"]);
        args.extend(extra);
        Config::try_parse_from(args)
    }

    #[test]
    fn default_stream_maxlen_is_the_memory_budget_divided_by_the_entry_estimate() {
        assert_eq!(DEFAULT_STREAM_MAXLEN, 524_288);
        assert_eq!(
            DEFAULT_STREAM_MAXLEN * STREAM_ENTRY_BYTES_ESTIMATE,
            512 * 1024 * 1024
        );
    }

    #[test]
    fn stream_maxlen_defaults_when_unset() {
        // Only asserts on the CLI default; an ambient
        // MOVEMENT_STREAM_MAXLEN in the test environment would be a
        // misconfigured test run, not a code path worth guarding.
        if std::env::var_os("MOVEMENT_STREAM_MAXLEN").is_some() {
            return;
        }
        let config = parse(&[]).expect("required args only");
        assert_eq!(config.movement_stream_maxlen, DEFAULT_STREAM_MAXLEN);
    }

    #[test]
    fn stream_maxlen_is_overridable_from_the_cli() {
        let config = parse(&["--movement-stream-maxlen", "250000"]).unwrap();
        assert_eq!(config.movement_stream_maxlen, 250_000);
    }

    #[test]
    fn auto_offset_reset_defaults_to_earliest() {
        // Same posture as `stream_maxlen_defaults_when_unset`.
        if std::env::var_os("KAFKA_AUTO_OFFSET_RESET").is_some() {
            return;
        }
        let config = parse(&[]).expect("required args only");
        assert_eq!(config.kafka_auto_offset_reset, "earliest");
    }

    #[test]
    fn auto_offset_reset_is_overridable_but_only_to_earliest_or_latest() {
        let config = parse(&["--kafka-auto-offset-reset", "latest"]).unwrap();
        assert_eq!(config.kafka_auto_offset_reset, "latest");
        assert!(parse(&["--kafka-auto-offset-reset", "error"]).is_err());
        assert!(parse(&["--kafka-auto-offset-reset", ""]).is_err());
    }

    #[test]
    fn stream_maxlen_rejects_values_below_the_floor() {
        assert!(parse(&["--movement-stream-maxlen", "999"]).is_err());
        assert!(parse(&["--movement-stream-maxlen", "0"]).is_err());
        assert!(parse(&["--movement-stream-maxlen", "1000"]).is_ok());
    }
}

/// Every env var `Config` declares must be set on the `movement-relay`
/// container in `charts/distant-signal/templates/movement-relay-deployment.yaml`
/// (and vice versa), and its probes must keep the readiness/liveness split.
/// Same raw-template-text check as `crates/notifier/src/config.rs`'s own
/// `chart_env_wiring_tests`.
#[cfg(test)]
mod chart_env_wiring_tests {
    use clap::CommandFactory;

    use super::Config;

    /// Read by `tracing_subscriber::EnvFilter`, not by `Config`.
    const NOT_CONFIG_ENV_VARS: &[&str] = &["RUST_LOG"];

    /// The container's slice of the template, from its `- name:` line to EOF
    /// (it is the only container), so a var named only in a leading comment
    /// cannot satisfy the check.
    fn relay_container_block() -> String {
        let chart = common::manifest_dir!()
            .join("../../charts/distant-signal/templates/movement-relay-deployment.yaml");
        let rendered = std::fs::read_to_string(&chart)
            .unwrap_or_else(|err| panic!("read {}: {err}", chart.display()));
        let marker = "- name: movement-relay\n";
        let start = rendered
            .find(marker)
            .expect("the Deployment must still declare a container named `movement-relay`");
        rendered[start..].to_string()
    }

    fn declared_env_vars() -> Vec<String> {
        Config::command()
            .get_arguments()
            .filter_map(|arg| arg.get_env().and_then(|env| env.to_str()))
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn every_env_var_this_config_declares_is_set_on_the_charts_container() {
        let block = relay_container_block();
        let declared = declared_env_vars();
        assert!(
            declared.iter().any(|env| env == "PROGRESS_STALL_SECS"),
            "sanity check: {declared:?}"
        );
        // REDIS_USERNAME and REDIS_PASSWORD are rendered by the shared
        // `distant-signal.redisClientAuthEnv` helper (only with redis.auth or
        // redis.acl on), so they never appear literally.
        let missing: Vec<&String> = declared
            .iter()
            .filter(|env| !block.contains(&format!("- name: {env}\n")))
            .filter(|env| {
                !(matches!(env.as_str(), "REDIS_PASSWORD" | "REDIS_USERNAME")
                    && block.contains(
                        r#"include "distant-signal.redisClientAuthEnv" (dict "root" . "client" "movementRelay" "user" "movement-relay")"#,
                    ))
            })
            .collect();
        assert!(
            missing.is_empty(),
            "declared by crates/movement-relay/src/config.rs but never set on the movement-relay \
             container, so the chart's value has no effect: {missing:?}"
        );
    }

    #[test]
    fn the_chart_sets_no_env_var_this_config_does_not_declare() {
        let block = relay_container_block();
        let declared = declared_env_vars();
        let stale: Vec<&str> = block
            .lines()
            .filter_map(|line| line.trim().strip_prefix("- name: "))
            .filter(|env| env.chars().all(|c| c.is_ascii_uppercase() || c == '_'))
            .filter(|env| !NOT_CONFIG_ENV_VARS.contains(env))
            .filter(|env| !declared.iter().any(|d| d == env))
            .collect();
        assert!(
            stale.is_empty(),
            "set on the movement-relay container but not declared by Config: {stale:?}"
        );
    }

    /// Liveness on `/healthz` (readiness: a confirmed Kafka partition
    /// assignment) got the relay `SIGKILLed` whenever it was unready for
    /// ~2 minutes, e.g. while the Redis pod was being recreated by the same
    /// rollout. Liveness must be the dependency-free `/livez`.
    #[test]
    fn liveness_probes_livez_and_readiness_probes_healthz() {
        let block = relay_container_block();
        let probe_path = |probe: &str| -> String {
            let start = block
                .find(&format!("{probe}:\n"))
                .unwrap_or_else(|| panic!("no {probe}"));
            block[start..]
                .lines()
                .find_map(|line| line.trim().strip_prefix("path: "))
                .unwrap_or_else(|| panic!("{probe} has no httpGet path"))
                .to_string()
        };
        assert_eq!(probe_path("livenessProbe"), "/livez");
        assert_eq!(probe_path("readinessProbe"), "/healthz");
    }
}
