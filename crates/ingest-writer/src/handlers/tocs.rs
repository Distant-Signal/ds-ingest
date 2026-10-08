//! `tocs/1` (stream `ds:ingest:reference`, plan 3c.1, decision D8): the
//! body is today's `POST /private/tocs` body, a `Vec<TocReference>`,
//! written by `ds_store::reference::upsert_tocs_observed`.
//!
//! Tocs need no ordering guard (spec §7.4: "dedup only"): the writer's
//! `ingest_dedup` row, in the same transaction, makes a redelivery a no-op.
//! A changed row's `fetched_at` and the freshness marker are the
//! envelope's `produced_at` (D13). Rows are counted as the snapshot
//! handlers count them; an unchanged TOC is `skipped`, so a daily snapshot
//! that changes nothing writes 0 rows.

use common::TocReference;
use ingest_stream::{HandlerError, StreamEntry};
use sqlx::PgConnection;

use super::{
    Applied, BoxFuture, SchemaHandler, classify_anyhow, count_apply, count_shadow, decode,
};
use crate::observed::Observed;

/// The `tocs/1` handler.
pub struct Tocs;

impl SchemaHandler for Tocs {
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        let tocs: Vec<TocReference> = decode(entry)?;
        count_shadow(entry, tocs.len());
        Ok(())
    }

    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let tocs: Vec<TocReference> = decode(entry)?;
            let written =
                ds_store::reference::upsert_tocs_observed(conn, &tocs, observed.produced_at())
                    .await
                    .map_err(|err| classify_anyhow(&err))?;
            count_apply(entry, tocs.len(), written);
            tracing::debug!(key = %entry.envelope.key, received = tocs.len(), written, "applied a tocs snapshot");
            Ok(Applied::All)
        })
    }
}
