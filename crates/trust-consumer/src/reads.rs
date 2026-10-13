//! Where this consumer reads its reference data (ingest architecture spec
//! §11.2, plan 4.4): the active tracked trains and the STANOX/CRS table,
//! from Postgres directly: the view `ingest_active_tracked_trains` (no user
//! ids) and `stanox_crs`, as the `trust_consumer` role.
//!
//! The reads share the sink's one `DATABASE_URL` ([`Config::database_url`])
//! and the one pool `main.rs` connects. Release B (ingest phase 5) removed
//! the api's `/private` GETs these replaced.
//!
//! [`Config::database_url`]: crate::config::Config::database_url

use common::{StanoxCrsRecord, TrackedTrainRef};

/// The retired `*_SOURCE` switches. Release B (ingest phase 5,
/// docs/ingest-phase5-runbook.md) removed their `http` value: each accepts
/// only `db` (the default), so an old `http` fails at startup instead of
/// being ignored. The next release drops both variables.
#[derive(Debug, Clone, Default, clap::Args)]
pub(crate) struct InternalReadArgs {
    /// `TRACKED_TRAINS_SOURCE`: only `db`, the view
    /// `ingest_active_tracked_trains`.
    #[arg(long, env, default_value = "db", value_parser = ["db"])]
    pub tracked_trains_source: String,

    /// `STANOX_CRS_SOURCE`: only `db`, `stanox_crs`
    /// (`ds_store::reads::list_stanox_crs`, with the api's CORPUS fallback
    /// setting, `CORPUS_FALLBACK_ENABLED`).
    #[arg(long, env, default_value = "db", value_parser = ["db"])]
    pub stanox_crs_source: String,
}

/// The two reference reads, from the consumer's one pool.
pub(crate) struct Reads<'a> {
    pub pool: &'a sqlx::PgPool,
}

impl Reads<'_> {
    /// The active tracked trains (the view `ingest_active_tracked_trains`).
    pub(crate) async fn tracked_trains(&self) -> anyhow::Result<Vec<TrackedTrainRef>> {
        ds_store::reads::list_active_tracked_trains(self.pool).await
    }

    /// The live STANOX/CRS table.
    pub(crate) async fn stanox_crs(&self) -> anyhow::Result<Vec<StanoxCrsRecord>> {
        ds_store::reads::list_stanox_crs(self.pool).await
    }
}

#[cfg(test)]
mod tests {
    use crate::config::tests::parse;

    use super::*;

    /// Release B: both sources are `db` only; `http` fails at startup.
    #[test]
    fn the_sources_are_db_only() {
        let config = parse(&[]).unwrap();
        assert_eq!(config.reads.tracked_trains_source, "db");
        assert_eq!(config.reads.stanox_crs_source, "db");
        for flag in ["--tracked-trains-source", "--stanox-crs-source"] {
            assert!(parse(&[flag, "db"]).is_ok(), "{flag} db");
            assert!(parse(&[flag, "http"]).is_err(), "{flag} http");
        }
    }

    #[test]
    fn the_sources_read_their_env_names() {
        use clap::CommandFactory;
        let command = crate::config::Config::command();
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
    }

    /// The reads go to the view and `stanox_crs`.
    #[sqlx::test(migrations = "../ds-store/migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_reads_read_postgres(pool: sqlx::PgPool) {
        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('87219', 'CLJ', 'CLPHMJN', 'Clapham Junction', 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let reads = Reads { pool: &pool };
        assert!(reads.tracked_trains().await.unwrap().is_empty());
        let stanox = reads.stanox_crs().await.unwrap();
        assert_eq!(stanox.len(), 1);
        assert_eq!(stanox[0].crs, "CLJ");
    }
}
