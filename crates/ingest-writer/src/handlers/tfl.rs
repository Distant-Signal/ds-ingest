//! `tfl-line-status/1` (stream `ds:ingest:tfl`, plan 3c.1): the body is
//! today's `POST /private/tfl-line-status` body, a `Vec<LineStatusReport>`,
//! written by `ds_store::samples::upsert_tfl_line_status_observed`.
//!
//! Decision D13: `line_status.computed_at`, `source_updated_at` and the
//! new `line_status_history.computed_at` are the envelope's `produced_at`
//! (clamped, [`Observed::produced_at`]), and freshness records it. A line
//! whose stored row is from a newer snapshot is skipped and writes no
//! history row; that is not an error. A `line_id` owned by another source
//! (the aggregator) is: the whole entry is poison, as the api's route
//! refuses it today.
//!
//! The prune of lines that left the feed runs only for a whole snapshot (no
//! `batch`, or `parts == 1`): one part of a split snapshot does not list
//! every line. poller-tfl never splits (about 20 lines).

use common::LineStatusReport;
use ingest_stream::{HandlerError, StreamEntry};
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, classify_anyhow, decode};
use crate::observed::Observed;

/// The `tfl-line-status/1` handler.
pub struct TflLineStatus;

impl SchemaHandler for TflLineStatus {
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        decode::<Vec<LineStatusReport>>(entry).map(|_| ())
    }

    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let reports: Vec<LineStatusReport> = decode(entry)?;
            if reports.is_empty() {
                // The poller never sends one: "TfL returned nothing" is a
                // fault, not an instruction to forget every line.
                tracing::warn!(key = %entry.envelope.key, "empty TfL snapshot; nothing written");
                return Ok(Applied::All);
            }
            let whole = entry
                .envelope
                .batch
                .as_ref()
                .is_none_or(|batch| batch.parts == 1);
            let applied = ds_store::samples::upsert_tfl_line_status_observed(
                conn,
                &reports,
                observed.produced_at(),
                whole,
            )
            .await
            .map_err(|err| classify_anyhow(&err))?;
            tracing::debug!(
                key = %entry.envelope.key,
                written = applied.written,
                skipped_older = applied.skipped_older.len(),
                history = applied.history,
                pruned = applied.pruned,
                "applied a TfL line-status snapshot"
            );
            Ok(Applied::All)
        })
    }
}
