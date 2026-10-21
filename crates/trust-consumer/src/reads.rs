//! Where this consumer reads its reference data (ingest architecture spec
//! §11.2, plan 4.4): the active tracked trains and the STANOX/CRS table,
//! from Postgres directly: the view `ingest_active_tracked_trains` (no user
//! ids) and `stanox_crs`, as the `trust_consumer` role.
//!
//! The reads share the sink's one `DATABASE_URL` ([`Config::database_url`])
//! and the one pool `main.rs` connects. Release B (ingest phase 5) removed
//! the api's `/private` GETs these replaced, and release D the
//! `TRACKED_TRAINS_SOURCE` / `STANOX_CRS_SOURCE` switches.
//!
//! [`Config::database_url`]: crate::config::Config::database_url

use common::{StanoxCrsRecord, TrackedTrainRef};

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
    use super::*;

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
