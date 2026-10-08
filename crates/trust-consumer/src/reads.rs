//! Where this consumer reads its reference data (ingest architecture spec
//! §11.2, plan 4.4): the active tracked trains (`TRACKED_TRAINS_SOURCE`)
//! and the STANOX/CRS table (`STANOX_CRS_SOURCE`), each from the api's
//! `/private` route (`http`, the default, today's behaviour) or from
//! Postgres directly (`db`): the view `ingest_active_tracked_trains` (no
//! user ids) and `stanox_crs`, as the `trust_consumer` role.
//!
//! Only the reads switch here. The writes (`INGEST_SINK`, plan 3b.3) are
//! the sink's; both share the one `DATABASE_URL` ([`Config::database_url`])
//! and the one pool `main.rs` connects when either needs Postgres.
//!
//! [`Config::database_url`]: crate::config::Config::database_url

use common::ingest::ReadSource;
use common::oauth_client::OAuthTokenCache;
use common::{StanoxCrsRecord, TrackedTrainRef};

/// The `*_SOURCE` switches. Their database is [`Config::database_url`].
///
/// [`Config::database_url`]: crate::config::Config::database_url
#[derive(Debug, Clone, Default, clap::Args)]
pub(crate) struct InternalReadArgs {
    /// Where the active tracked trains come from: `http` (the default) is
    /// `GET /private/tracked-trains` (`API_TRACKED_TRAINS_URL`); `db` is the
    /// view `ingest_active_tracked_trains`, the same SELECT.
    #[arg(long, env, value_enum, default_value_t = ReadSource::Http)]
    pub tracked_trains_source: ReadSource,

    /// Where the live STANOX/CRS table comes from: `http` (the default) is
    /// `GET /private/stanox-crs` (`STANOX_CRS_URL`); `db` reads `stanox_crs`
    /// (`ds_store::reads::list_stanox_crs`, with the api's CORPUS fallback
    /// setting, `CORPUS_FALLBACK_ENABLED`).
    #[arg(long, env, value_enum, default_value_t = ReadSource::Http)]
    pub stanox_crs_source: ReadSource,
}

impl InternalReadArgs {
    /// True when either source reads Postgres.
    pub(crate) fn any_db(&self) -> bool {
        self.tracked_trains_source == ReadSource::Db || self.stanox_crs_source == ReadSource::Db
    }
}

/// The two reference reads, each from its configured source.
pub(crate) struct Reads<'a> {
    pub http: &'a reqwest::Client,
    pub tokens: &'a OAuthTokenCache,
    pub tracked_trains_url: &'a str,
    pub stanox_crs_url: &'a str,
    /// The consumer's one pool, set when a source (or the sink) is `db`
    /// (`main.rs`'s `connect_database`).
    pub pool: Option<&'a sqlx::PgPool>,
    pub tracked_trains_source: ReadSource,
    pub stanox_crs_source: ReadSource,
}

impl Reads<'_> {
    fn pool_for(&self, source: ReadSource) -> Option<&sqlx::PgPool> {
        match source {
            ReadSource::Db => self.pool,
            ReadSource::Http => None,
        }
    }

    /// The active tracked trains, as `GET /private/tracked-trains` answers.
    pub(crate) async fn tracked_trains(&self) -> anyhow::Result<Vec<TrackedTrainRef>> {
        match self.pool_for(self.tracked_trains_source) {
            Some(pool) => ds_store::reads::list_active_tracked_trains(pool).await,
            None => {
                crate::queries::fetch_active_tracked_trains(
                    self.http,
                    self.tracked_trains_url,
                    self.tokens,
                )
                .await
            }
        }
    }

    /// The live STANOX/CRS table, as `GET /private/stanox-crs` answers.
    pub(crate) async fn stanox_crs(&self) -> anyhow::Result<Vec<StanoxCrsRecord>> {
        match self.pool_for(self.stanox_crs_source) {
            Some(pool) => ds_store::reads::list_stanox_crs(pool).await,
            None => {
                crate::queries::fetch_stanox_crs(self.http, self.stanox_crs_url, self.tokens).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::config::Config;

    use super::*;

    fn parse(extra: &[&str]) -> Config {
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
        // The test environment may set DATABASE_URL.
        let no_database = if extra.contains(&"--database-url") {
            vec![]
        } else {
            vec!["--database-url".to_owned(), String::new()]
        };
        Config::try_parse_from(
            base.into_iter()
                .chain(no_database)
                .chain(extra.iter().map(|s| (*s).to_owned())),
        )
        .unwrap()
    }

    /// Plan 4.4: both sources default to the api (today's behaviour), and a
    /// `db` source needs the (shared) database.
    #[test]
    fn the_sources_default_to_http_and_db_needs_a_database_url() {
        let config = parse(&[]);
        assert_eq!(config.reads.tracked_trains_source, ReadSource::Http);
        assert_eq!(config.reads.stanox_crs_source, ReadSource::Http);
        config.validate().unwrap();
        for flag in ["--tracked-trains-source", "--stanox-crs-source"] {
            let config = parse(&[flag, "db"]);
            assert!(config.reads.any_db());
            assert!(config.validate().is_err(), "{flag} without DATABASE_URL");
        }
        let config = parse(&[
            "--tracked-trains-source",
            "db",
            "--database-url",
            "postgres://distant_signal_trust_consumer:pw@postgres/ds",
        ]);
        config.validate().unwrap();
        assert!(!format!("{config:?}").contains(":pw@"));
    }

    #[test]
    fn the_sources_read_the_chart_s_env_names() {
        use clap::CommandFactory;
        let command = Config::command();
        let env = |id: &str| {
            command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(|arg| arg.get_env())
                .map(|env| env.to_string_lossy().into_owned())
        };
        assert_eq!(
            env("tracked_trains_source").as_deref(),
            Some("TRACKED_TRAINS_SOURCE")
        );
        assert_eq!(
            env("stanox_crs_source").as_deref(),
            Some("STANOX_CRS_SOURCE")
        );
        assert_eq!(env("database_url").as_deref(), Some("DATABASE_URL"));
        // The template passes them to `distant-signal.internalReadsEnv`.
        let template = std::fs::read_to_string(
            common::manifest_dir!()
                .join("../../charts/distant-signal/templates/trust-consumer-deployment.yaml"),
        )
        .unwrap();
        for name in ["TRACKED_TRAINS_SOURCE", "STANOX_CRS_SOURCE"] {
            assert!(template.contains(&format!("\"{name}\"")), "{name}");
        }
    }

    /// `db` reads the view and `stanox_crs`; `http` would call the api,
    /// which here does not exist, so a `db` read that succeeded never
    /// touched it.
    #[sqlx::test(migrations = "../ds-store/migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn db_sources_read_postgres_not_the_api(pool: sqlx::PgPool) {
        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('87219', 'CLJ', 'CLPHMJN', 'Clapham Junction', 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let http = reqwest::Client::new();
        let tokens = OAuthTokenCache::new(common::oauth_client::OAuthCredentials {
            token_url: "http://127.0.0.1:9/token".to_string(),
            client_id: "c".to_string(),
            scope: "groups".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
        });
        let reads = Reads {
            http: &http,
            tokens: &tokens,
            tracked_trains_url: "http://127.0.0.1:9/private/tracked-trains",
            stanox_crs_url: "http://127.0.0.1:9/private/stanox-crs",
            pool: Some(&pool),
            tracked_trains_source: ReadSource::Db,
            stanox_crs_source: ReadSource::Db,
        };
        assert!(reads.tracked_trains().await.unwrap().is_empty());
        let stanox = reads.stanox_crs().await.unwrap();
        assert_eq!(stanox.len(), 1);
        assert_eq!(stanox[0].crs, "CLJ");

        let http_only = Reads {
            tracked_trains_source: ReadSource::Http,
            ..reads
        };
        assert!(
            http_only.tracked_trains().await.is_err(),
            "http goes to the (absent) api"
        );
    }
}
