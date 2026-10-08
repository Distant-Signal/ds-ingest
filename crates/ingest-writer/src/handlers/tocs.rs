//! `tocs/1` (stream `ds:ingest:reference`, plan 3c.1, decision D8): the
//! body is today's `POST /private/tocs` body, a `Vec<TocReference>`,
//! written by `ds_store::reference::upsert_tocs_observed`.
//!
//! Tocs need no ordering guard (spec §7.4: "dedup only"): the writer's
//! `ingest_dedup` row, in the same transaction, makes a redelivery a no-op.
//! A changed row's `fetched_at` and the freshness marker are the
//! envelope's `produced_at` (D13).

use common::TocReference;
use ingest_stream::{HandlerError, StreamEntry};
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, classify_anyhow, decode};
use crate::observed::Observed;

/// The `tocs/1` handler.
pub struct Tocs;

impl SchemaHandler for Tocs {
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        decode::<Vec<TocReference>>(entry).map(|_| ())
    }

    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let tocs: Vec<TocReference> = decode(entry)?;
            let received =
                ds_store::reference::upsert_tocs_observed(conn, &tocs, observed.produced_at())
                    .await
                    .map_err(|err| classify_anyhow(&err))?;
            tracing::debug!(key = %entry.envelope.key, received, "applied a tocs snapshot");
            Ok(Applied::All)
        })
    }
}
