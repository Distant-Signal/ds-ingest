//! Where this consumer reads its reference data (ingest architecture spec
//! §11.1 and §11.2, plan 4.3): the schedule line populations and the
//! STANOX/CRS table, from Postgres directly, as the read-only
//! `full_coverage_ro` role.
//!
//! The population's source is `population_reload::DbSource`; the reload
//! loop around it (the first-load wait, the abort after three failures, the
//! jittered backoff) is `population_reload`'s. Release B (ingest phase 5)
//! removed the api's `/private` GETs these replaced.

/// `pg_stat_activity.application_name`.
const APPLICATION_NAME: &str = "distant-signal-full-coverage-consumer";
/// Spec §6.6: pool 2, role limit 3. The population reload and the
/// STANOX/CRS reload are the only users, one query at a time each.
const DEFAULT_MAX_CONNECTIONS: u32 = 2;

/// The retired `*_SOURCE` switches and the database the reads use.
/// Release B (ingest phase 5, docs/ingest-phase5-runbook.md) removed their
/// `http` value: each accepts only `db` (the default), so an old `http`
/// fails at startup instead of being ignored. The next release drops both
/// variables.
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct InternalReadArgs {
    /// `POPULATION_SOURCE`: only `db`, one version query per cycle, then a
    /// fetch of only the changed pairs
    /// (`ds_store::reads::list_population_versions`).
    #[arg(long, env, default_value = "db", value_parser = ["db"])]
    pub population_source: String,

    /// `STANOX_CRS_SOURCE`: only `db`, `stanox_crs`
    /// (`ds_store::reads::list_stanox_crs`, with the api's CORPUS fallback
    /// setting, `CORPUS_FALLBACK_ENABLED`).
    #[arg(long, env, default_value = "db", value_parser = ["db"])]
    pub stanox_crs_source: String,

    /// Postgres, as the `full_coverage_ro` role. Pool size and timeouts come
    /// from the shared `DATABASE_*` variables (`common::pg`); the default
    /// pool is 2 (spec §6.6, role limit 3).
    #[arg(long, env, hide_env_values = true)]
    pub database_url: common::secret::Secret,
}

impl InternalReadArgs {
    /// `DATABASE_URL` must not be empty.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.database_url.is_empty() {
            anyhow::bail!(
                "full-coverage-consumer needs DATABASE_URL (its populations and STANOX/CRS come from Postgres)"
            );
        }
        Ok(())
    }

    /// Waits for Postgres (INF-5, beating `progress`), connects the pool
    /// (with the `db_pool_*` metrics) and passes the schema gate as
    /// `full_coverage_ro` (spec §12.2).
    pub(crate) async fn connect(
        &self,
        progress: &health_http::Progress,
    ) -> anyhow::Result<sqlx::PgPool> {
        let url = &self.database_url;
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
        tracing::info!("reading reference data from Postgres directly");
        Ok(pool)
    }
}

/// Where the STANOX/CRS reload reads.
pub(crate) struct StanoxCrsSource(pub sqlx::PgPool);

impl StanoxCrsSource {
    /// The whole table.
    pub(crate) async fn fetch(&self) -> anyhow::Result<Vec<common::StanoxCrsRecord>> {
        ds_store::reads::list_stanox_crs(&self.0).await
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::config::Config;

    fn parse(extra: &[&str]) -> Result<Config, clap::Error> {
        let lines_dir = common::manifest_dir!().join("../../lines");
        let base = [
            "full-coverage-consumer".to_owned(),
            "--lines-dir".to_owned(),
            lines_dir.display().to_string(),
        ];
        // The test environment may set DATABASE_URL.
        let database = if extra.contains(&"--database-url") {
            vec![]
        } else {
            vec![
                "--database-url".to_owned(),
                "postgres://distant_signal_full_coverage_ro:pw@postgres/ds".to_owned(),
            ]
        };
        Config::try_parse_from(
            base.into_iter()
                .chain(database)
                .chain(extra.iter().map(|s| (*s).to_owned())),
        )
    }

    /// Release B: both sources are `db` only (`http` fails at startup), and
    /// the database URL must not be empty.
    #[test]
    fn the_sources_are_db_only_and_need_a_database_url() {
        let config = parse(&[]).unwrap();
        assert_eq!(config.reads.population_source, "db");
        assert_eq!(config.reads.stanox_crs_source, "db");
        config.reads.validate().unwrap();
        assert!(
            !format!("{config:?}").contains(":pw@"),
            "the database URL must not appear in Debug output"
        );
        for flag in ["--population-source", "--stanox-crs-source"] {
            assert!(parse(&[flag, "db"]).is_ok(), "{flag} db");
            assert!(parse(&[flag, "http"]).is_err(), "{flag} http");
        }
        let empty = parse(&["--database-url", ""]).unwrap();
        assert!(empty.reads.validate().is_err(), "an empty DATABASE_URL");
    }

    #[test]
    fn the_sources_read_their_env_names() {
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
    }
}
