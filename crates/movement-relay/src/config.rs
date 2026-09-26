use clap::Parser;

/// CLI/env configuration for `movement-relay` -- the sole real Kafka
/// client against RDM's Train Movements product from Deploy B onward. See
/// docs/superpowers/specs/2026-09-04-movement-relay-design.md and
/// docs/superpowers/plans/2026-09-04-movement-relay-plan.md.
#[derive(Debug, Parser)]
pub struct Config {
    /// GAP: unconfirmed hostname until Deploy B's real credential is in
    /// hand -- same posture as trust-consumer/src/config.rs's own
    /// identical field.
    #[command(flatten)]
    pub kafka: common::service_args::KafkaConnectionArgs,

    /// The one real, RDM-issued group -- `SC-c4d90f8e-...` in production,
    /// per the design doc's "Why this exists" section. Deliberately no
    /// default: unlike trust-consumer's own kafka_consumer_group (which
    /// DOES have a sensible per-deployment default,
    /// "distant-signal-trust-consumer"), this crate's group id is a fixed,
    /// externally-issued, unforgeable identity -- guessing wrong here is
    /// worse than refusing to start.
    #[arg(long, env)]
    pub kafka_consumer_group: String,

    #[arg(long, env, default_value = "redis://redis:6379")]
    pub redis_url: String,

    #[arg(long, env, default_value = "0.0.0.0:8083")]
    pub health_bind_url: String,
    #[arg(long, env, default_value_t = 9094)]
    pub metrics_port: u16,
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,
    /// How often the leading-indicator lag gauge (`main::stream_lag_loop`)
    /// polls `XINFO GROUPS` for both downstream groups. UNRESEARCHED
    /// starting figure, same posture as every other first-guess cadence
    /// constant in this codebase (see trust-consumer/src/config.rs's own
    /// stanox_crs_reload_secs comment).
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
pub const STREAM_MEMORY_BUDGET_BYTES: u64 = 512 * 1024 * 1024;

/// Planning figure for Redis memory per `movement-events` entry: 1 KiB.
/// Measured at 920 B in production (a ~750 B raw TRUST envelope in
/// `payload`, the 4-byte `msg_type`, and listpack/radix-tree overhead),
/// rounded up so that bigger payloads (activations) still fit the budget.
pub const STREAM_ENTRY_BYTES_ESTIMATE: u64 = 1024;

/// 524,288 entries. See `STREAM_MEMORY_BUDGET_BYTES`.
pub const DEFAULT_STREAM_MAXLEN: u64 = STREAM_MEMORY_BUDGET_BYTES / STREAM_ENTRY_BYTES_ESTIMATE;

/// Floor for `--movement-stream-maxlen`. `MAXLEN ~` only trims whole
/// stream nodes (`stream-node-max-entries`, 100 by default), so a
/// smaller cap is not honoured precisely. Any cap this small would also be
/// minutes of traffic, which is almost certainly a typo rather than a
/// deliberate setting.
pub const MIN_STREAM_MAXLEN: u64 = 1_000;

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
    fn stream_maxlen_rejects_values_below_the_floor() {
        assert!(parse(&["--movement-stream-maxlen", "999"]).is_err());
        assert!(parse(&["--movement-stream-maxlen", "0"]).is_err());
        assert!(parse(&["--movement-stream-maxlen", "1000"]).is_ok());
    }
}
