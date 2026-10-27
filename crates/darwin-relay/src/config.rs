//! darwin-relay's configuration, from the environment the chart sets
//! (`darwinRelay.*`; the Kafka values from the `distant-signal-darwin-pushport`
//! Secret).

use clap::Parser;
use common::secret::Secret;

#[derive(Debug, Parser)]
#[expect(
    clippy::struct_field_names,
    reason = "clap derives each env var from the field name, so the prefix is part of the interface"
)]
pub(crate) struct Config {
    /// The Push Port JSON topic's bootstrap server(s) (secret key
    /// `kafka-bootstrap-server`).
    #[arg(long, env)]
    pub kafka_brokers: String,
    /// The Push Port JSON topic (secret key `kafka-topic`).
    #[arg(long, env)]
    pub kafka_topic: String,
    /// The RDM-issued consumer group (secret key `consumer-group`).
    #[arg(long, env)]
    pub kafka_consumer_group: String,
    /// SASL user name and password (secret keys `username`, `password`).
    /// When either is empty they are read from `kafka_sasl_jaas_config`.
    #[arg(long, env, default_value = "")]
    pub kafka_sasl_username: String,
    #[arg(long, env, hide_env_values = true)]
    pub kafka_sasl_password: Option<Secret>,
    /// The Java client's JAAS line (secret key `sasl-config`), e.g.
    /// `...PlainLoginModule required username="..." password="...";`.
    /// librdkafka has no JAAS: it is only a fallback source of the user
    /// name and password above.
    #[arg(long, env, hide_env_values = true)]
    pub kafka_sasl_jaas_config: Option<Secret>,
    /// librdkafka `sasl.mechanisms` (secret key `sasl-mechanism`).
    #[arg(long, env, default_value = "PLAIN")]
    pub kafka_sasl_mechanism: String,
    /// librdkafka `security.protocol` (secret key `security-protocol`).
    #[arg(long, env, default_value = "SASL_SSL")]
    pub kafka_security_protocol: String,
    /// `auto.offset.reset` for a partition the group has no committed
    /// offset for. `latest` by default, unlike movement-relay: the Push
    /// Port is current state, not a log of events DS must not miss, and a
    /// first start from `earliest` would replay the topic's whole retention
    /// (days of messages) into the writer. A gap is closed by Darwin's own
    /// updates within minutes for running trains; the snapshot product
    /// (not built) is the full resync.
    #[arg(
        long,
        env,
        default_value = "latest",
        value_parser = clap::builder::PossibleValuesParser::new(["earliest", "latest"])
    )]
    pub kafka_auto_offset_reset: String,

    #[arg(long, env, default_value = "redis://redis:6379")]
    pub redis_url: String,
    #[arg(long, env)]
    pub redis_username: Option<String>,
    #[arg(long, env, hide_env_values = true)]
    pub redis_password: Option<Secret>,
    /// `XADD ds:ingest:darwin MAXLEN ~`; 0 means the stream's budgeted cap
    /// (`ingest_stream::budget`).
    #[arg(long, env, default_value_t = 0)]
    pub darwin_stream_maxlen: u64,

    /// A batch is published this long after its first record ... The
    /// stream's memory budget assumes no more than one batch this often
    /// (`ingest_stream::budget::DARWIN_BATCH_SECS`): a shorter one writes
    /// more entries than its `MAXLEN` keeps for a two-hour writer outage.
    #[arg(long, env, default_value_t = ingest_stream::budget::DARWIN_BATCH_SECS)]
    pub batch_max_secs: u64,
    /// ... or once it holds this many records, whichever comes first.
    #[arg(long, env, default_value_t = 5000)]
    pub batch_max_records: usize,

    #[command(flatten)]
    pub health: common::service_args::HealthArgs,
    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,
    #[arg(long, env, default_value_t = 9095)]
    pub metrics_port: u16,
}

impl Config {
    /// The SASL user name and password: the explicit ones, else those in
    /// the JAAS line. `None` when neither gives both.
    pub(crate) fn sasl_credentials(&self) -> Option<(String, Secret)> {
        let password = self.kafka_sasl_password.as_ref().filter(|p| !p.is_empty());
        if !self.kafka_sasl_username.is_empty()
            && let Some(password) = password
        {
            return Some((self.kafka_sasl_username.clone(), password.clone()));
        }
        let jaas = self.kafka_sasl_jaas_config.as_ref()?.expose();
        Some((
            jaas_value(jaas, "username")?,
            Secret::new(jaas_value(jaas, "password")?),
        ))
    }
}

/// `key="value"` out of a JAAS line.
fn jaas_value(jaas: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=\"");
    let start = jaas.find(&needle)? + needle.len();
    let end = jaas[start..].find('"')? + start;
    Some(jaas[start..end].to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(extra: &[&str]) -> Config {
        let mut args = vec![
            "darwin-relay",
            "--kafka-brokers",
            "kafka.example:9093",
            "--kafka-topic",
            "example-topic",
            "--kafka-consumer-group",
            "example-group",
        ];
        args.extend_from_slice(extra);
        Config::try_parse_from(args).expect("parses")
    }

    #[test]
    fn defaults_suit_the_rdm_topic() {
        let config = parse(&[]);
        assert_eq!(config.kafka_sasl_mechanism, "PLAIN");
        assert_eq!(config.kafka_security_protocol, "SASL_SSL");
        assert_eq!(config.kafka_auto_offset_reset, "latest");
        assert_eq!(config.batch_max_secs, 30);
        assert!(config.sasl_credentials().is_none());
    }

    #[test]
    fn credentials_come_from_the_explicit_values_first() {
        let config = parse(&[
            "--kafka-sasl-username",
            "user-a",
            "--kafka-sasl-password",
            "not-a-real-password",
            "--kafka-sasl-jaas-config",
            r#"x required username="user-b" password="other";"#,
        ]);
        let (user, password) = config.sasl_credentials().expect("credentials");
        assert_eq!(user, "user-a");
        assert_eq!(password.expose(), "not-a-real-password");
    }

    #[test]
    fn credentials_fall_back_to_the_jaas_line() {
        let config = parse(&[
            "--kafka-sasl-jaas-config",
            r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="user-b" password="fake-secret";"#,
        ]);
        let (user, password) = config.sasl_credentials().expect("credentials");
        assert_eq!(user, "user-b");
        assert_eq!(password.expose(), "fake-secret");
    }
}
