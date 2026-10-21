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

/// The database the reads use. Release D (ingest phase 5,
/// docs/ingest-phase5-runbook.md) removed the `POPULATION_SOURCE` and
/// `STANOX_CRS_SOURCE` switches, which accepted only `db` since release B.
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct InternalReadArgs {
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
        ds_store::pool::register_metrics();
        let pool =
            ds_store::pool::PoolSettings::from_env(APPLICATION_NAME, DEFAULT_MAX_CONNECTIONS)?
                .connect_until_ready(url.expose(), Some(progress))
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

    /// The database URL must not be empty, nor appear in `Debug`.
    #[test]
    fn the_reads_need_a_database_url() {
        let config = parse(&[]).unwrap();
        config.reads.validate().unwrap();
        assert!(
            !format!("{config:?}").contains(":pw@"),
            "the database URL must not appear in Debug output"
        );
        let empty = parse(&["--database-url", ""]).unwrap();
        assert!(empty.reads.validate().is_err(), "an empty DATABASE_URL");
    }

    /// `DATABASE_URL`, and no retired switch: release D (ingest phase 5,
    /// Q8) removed `INGEST_SINK`, `POPULATION_SOURCE` and
    /// `STANOX_CRS_SOURCE`.
    #[test]
    fn the_reads_read_their_env_names() {
        use clap::CommandFactory;
        let command = Config::command();
        let env = |id: &str| {
            command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(|arg| arg.get_env())
                .map(|env| env.to_string_lossy().into_owned())
        };
        assert_eq!(env("database_url").as_deref(), Some("DATABASE_URL"));
        for arg in command.get_arguments() {
            let name = arg.get_env().map(|env| env.to_string_lossy().into_owned());
            assert!(
                !matches!(
                    name.as_deref(),
                    Some("INGEST_SINK" | "POPULATION_SOURCE" | "STANOX_CRS_SOURCE")
                ),
                "{name:?} was removed in release D"
            );
        }
    }
}
