//! `darwin-pushport/1` (stream `ds:ingest:darwin`, DS-152): the body is a
//! [`common::darwin::Batch`], a few seconds of Darwin Push Port messages
//! from darwin-relay, written by [`ds_store::darwin::apply`] and recorded
//! in `ingest_freshness` as `darwin-pushport` at the batch's newest Darwin
//! timestamp.
//!
//! Every row is guarded by its Darwin timestamp, so a duplicate entry
//! (which the dedup key absorbs anyway) or an older one writes nothing. A
//! batch the relay split for size arrives as several entries, each a valid
//! batch on its own.

use common::darwin::Batch;
use ingest_stream::{HandlerError, StreamEntry};
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, classify_anyhow, count_apply, decode};
use crate::observed::Observed;

/// The `darwin-pushport/1` handler.
pub struct DarwinPushport;

impl SchemaHandler for DarwinPushport {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let batch: Batch = decode(entry)?;
            if batch.is_empty() {
                return Ok(Applied::All);
            }
            let applied = ds_store::darwin::apply(conn, &batch)
                .await
                .map_err(|err| classify_anyhow(&err))?;
            count_apply(entry, batch.len(), applied.total());
            ds_store::freshness::record_ingest(
                conn,
                common::darwin::FRESHNESS_SOURCE,
                Some(batch.newest_ts().unwrap_or_else(|| observed.produced_at())),
            )
            .await
            .map_err(|err| classify_anyhow(&err))?;
            tracing::debug!(
                key = %entry.envelope.key,
                rows = batch.len(),
                services = applied.services,
                calls = applied.calls,
                associations = applied.associations,
                deactivated = applied.deactivated,
                "applied a Darwin Push Port batch"
            );
            Ok(Applied::All)
        })
    }
}
