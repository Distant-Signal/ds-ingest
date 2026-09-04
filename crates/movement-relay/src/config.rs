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
    #[arg(long, env)]
    pub kafka_brokers: String,
    #[arg(long, env)]
    pub kafka_topic: String,
    /// The one real, RDM-issued group -- `SC-c4d90f8e-...` in production,
    /// per the design doc's "Why this exists" section. Deliberately no
    /// default: unlike trust-consumer's own kafka_consumer_group (which
    /// DOES have a sensible per-deployment default,
    /// "distant-signal-trust-consumer"), this crate's group id is a fixed,
    /// externally-issued, unforgeable identity -- guessing wrong here is
    /// worse than refusing to start.
    #[arg(long, env)]
    pub kafka_consumer_group: String,
    #[arg(long, env)]
    pub kafka_sasl_username: String,
    #[arg(long, env)]
    pub kafka_sasl_password: String,
    #[arg(long, env)]
    pub kafka_sasl_mechanism: String,

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
}
