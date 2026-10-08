//! Where this consumer reads its reference data (ingest architecture spec
//! §11.1 and §11.2, plan 4.3): the schedule line populations
//! (`POPULATION_SOURCE`) and the STANOX/CRS table (`STANOX_CRS_SOURCE`),
//! each from the api's `/private` route (`http`, the default, today's
//! behaviour) or from Postgres directly (`db`), as the read-only
//! `full_coverage_ro` role.
//!
//! The population's two sources are `population_reload::HttpSource` and
//! `population_reload::DbSource`; the reload loop around them (the
//! first-load wait, the abort after three failures, the jittered backoff)
//! is the same for both.

use std::sync::Arc;

use common::ingest::ReadSource;

/// `pg_stat_activity.application_name` when a source is `db`.
const APPLICATION_NAME: &str = "distant-signal-full-coverage-consumer";
/// Spec §6.6: pool 2, role limit 3. The population reload and the
/// STANOX/CRS reload are the only users, one query at a time each.
const DEFAULT_MAX_CONNECTIONS: u32 = 2;

/// The `*_SOURCE` switches and the database they need.
#[derive(Debug, Clone, Default, clap::Args)]
pub(crate) struct InternalReadArgs {
    /// Where the schedule line populations come from: `http` (the default)
    /// is one conditional `GET /private/schedule-line-population` per
    /// `(line, date)` (`SCHEDULE_LINE_POPULATION_URL`); `db` is one version
    /// query per cycle, then a fetch of only the changed pairs
    /// (`ds_store::reads::list_population_versions`).
    #[arg(long, env, value_enum, default_value_t = ReadSource::Http)]
    pub population_source: ReadSource,

    /// Where the STANOX/CRS table comes from: `http` (the default) is
    /// `GET /private/stanox-crs` (`STANOX_CRS_URL`); `db` reads `stanox_crs`
    /// (`ds_store::reads::list_stanox_crs`, with the api's CORPUS fallback
    /// setting, `CORPUS_FALLBACK_ENABLED`).
    #[arg(long, env, value_enum, default_value_t = ReadSource::Http)]
    pub stanox_crs_source: ReadSource,

    /// Postgres, for a `db` source (required then, unused otherwise). Pool
    /// size and timeouts come from the shared `DATABASE_*` variables
    /// (`common::pg`); the default pool is 2 (spec §6.6, role limit 3).
    #[arg(long, env, hide_env_values = true)]
    pub database_url: Option<common::secret::Secret>,
}

impl InternalReadArgs {
    /// True when either source reads Postgres.
    pub(crate) fn any_db(&self) -> bool {
        self.population_source == ReadSource::Db || self.stanox_crs_source == ReadSource::Db
    }

    /// A `db` source needs a `DATABASE_URL`.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.any_db()
            && self
                .database_url
                .as_ref()
                .is_none_or(common::secret::Secret::is_empty)
        {
            anyhow::bail!("POPULATION_SOURCE=db or STANOX_CRS_SOURCE=db needs DATABASE_URL");
        }
        Ok(())
    }

    /// With a `db` source: waits for Postgres (INF-5, beating `progress`),
    /// connects the pool (with the `db_pool_*` metrics) and passes the
    /// schema gate as `full_coverage_ro` (spec §12.2). `None` when both
    /// sources are `http`: nothing connects to Postgres, as today.
    pub(crate) async fn connect(
        &self,
        progress: &health_http::Progress,
    ) -> anyhow::Result<Option<sqlx::PgPool>> {
        if !self.any_db() {
            return Ok(None);
        }
        let url = self
            .database_url
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("a db source needs DATABASE_URL"))?;
        ds_store::corpus::init_fallback_from_env()?;
        common::startup::retry_until_ready(
            "Postgres",
            common::startup::CONNECT_BACKOFF,
            Some(progress),
            || async {
                use sqlx::Connection;
                sqlx::PgConnection::connect(url.expose())
                    .await?
                    .close()
                    .await
            },
        )
        .await;
        ds_store::pool::register_metrics();
        let pool =
            ds_store::pool::PoolSettings::from_env(APPLICATION_NAME, DEFAULT_MAX_CONNECTIONS)?
                .connect(url.expose())
                .await?;
        ds_store::schema::wait_for_schema(
            &pool,
            ds_store::schema::DbRole::FullCoverageRo,
            Some(progress),
        )
        .await?;
        tracing::info!(
            population = ?self.population_source,
            stanox_crs = ?self.stanox_crs_source,
            "reading reference data from Postgres directly"
        );
        Ok(Some(pool))
    }
}

/// Where the STANOX/CRS reload reads (`STANOX_CRS_SOURCE`).
pub(crate) enum StanoxCrsSource {
    Http {
        client: reqwest::Client,
        url: String,
        tokens: Arc<common::oauth_client::OAuthTokenCache>,
    },
    Db(sqlx::PgPool),
}

impl StanoxCrsSource {
    /// The whole table, as `GET /private/stanox-crs` answers it.
    pub(crate) async fn fetch(&self) -> anyhow::Result<Vec<common::StanoxCrsRecord>> {
        match self {
            Self::Http {
                client,
                url,
                tokens,
            } => crate::queries::fetch_stanox_crs(client, url, tokens).await,
            Self::Db(pool) => ds_store::reads::list_stanox_crs(pool).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::config::Config;

    use super::*;

    fn parse(extra: &[&str]) -> Config {
        let lines_dir = common::manifest_dir!().join("../../lines");
        let base = [
            "full-coverage-consumer".to_owned(),
            "--internal-oauth-token-url".to_owned(),
            "http://auth.example.com/token".to_owned(),
            "--internal-oauth-client-id".to_owned(),
            "client-id".to_owned(),
            "--internal-oauth-username".to_owned(),
            "svc-user".to_owned(),
            "--internal-oauth-password".to_owned(),
            "svc-pass".to_owned(),
            "--lines-dir".to_owned(),
            lines_dir.display().to_string(),
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

    /// Plan 4.3: both sources default to the api (today's behaviour), and a
    /// `db` source needs a database.
    #[test]
    fn the_sources_default_to_http_and_db_needs_a_database_url() {
        let config = parse(&[]);
        assert_eq!(config.reads.population_source, ReadSource::Http);
        assert_eq!(config.reads.stanox_crs_source, ReadSource::Http);
        assert!(!config.reads.any_db());
        config.reads.validate().unwrap();

        for flag in ["--population-source", "--stanox-crs-source"] {
            let config = parse(&[flag, "db"]);
            assert!(config.reads.any_db());
            assert!(
                config.reads.validate().is_err(),
                "{flag} without DATABASE_URL"
            );
        }
        let config = parse(&[
            "--population-source",
            "db",
            "--database-url",
            "postgres://distant_signal_full_coverage_ro:pw@postgres/ds",
        ]);
        config.reads.validate().unwrap();
        assert!(
            !format!("{config:?}").contains(":pw@"),
            "the database URL must not appear in Debug output"
        );
    }

    /// The chart sets these names (templates/full-coverage-consumer-deployment.yaml).
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
            env("population_source").as_deref(),
            Some("POPULATION_SOURCE")
        );
        assert_eq!(
            env("stanox_crs_source").as_deref(),
            Some("STANOX_CRS_SOURCE")
        );
        assert_eq!(env("database_url").as_deref(), Some("DATABASE_URL"));
        // The template passes them to `distant-signal.internalReadsEnv`.
        let template =
            std::fs::read_to_string(common::manifest_dir!().join(
                "../../charts/distant-signal/templates/full-coverage-consumer-deployment.yaml",
            ))
            .unwrap();
        for name in ["POPULATION_SOURCE", "STANOX_CRS_SOURCE"] {
            assert!(template.contains(&format!("\"{name}\"")), "{name}");
        }
    }
}
