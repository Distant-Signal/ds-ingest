//! `tfl-stops/1` (stream `ds:ingest:tfl-stops`; docs/superpowers/specs/
//! 2026-10-20-tfl-stop-lookup-design.md §2): the body is a
//! `Vec<common::tfl_stops::TflStop>`, poller-tfl's whole daily stops
//! snapshot in one part, written by `ds_store::tfl_stops::replace`.
//!
//! The snapshot replaces `tfl_stops` and rebuilds `tfl_stop_crs`, so it is
//! applied only whole (no `batch`, or `parts == 1`): a part of a split
//! snapshot would delete every stop the other parts carry. A split one is
//! poison (the producer never splits it). An empty snapshot, and one older
//! than the stored stops, write nothing and are not errors. A row the
//! store refuses before writing (not a `NaPTAN` id, no name, no mode) is left
//! out and logged.

use common::tfl_stops::TflStop;
use ingest_stream::{HandlerError, StreamEntry};
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, classify_anyhow, count_apply, decode};
use crate::observed::Observed;

/// The `tfl-stops/1` handler.
pub struct TflStops;

impl SchemaHandler for TflStops {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            if entry
                .envelope
                .batch
                .as_ref()
                .is_some_and(|batch| batch.parts != 1)
            {
                return Err(HandlerError::Poison(
                    "a tfl-stops snapshot must be one part: a part alone would delete the \
                     other parts' stops"
                        .to_owned(),
                ));
            }
            let stops: Vec<TflStop> = decode(entry)?;
            if stops.is_empty() {
                tracing::warn!(key = %entry.envelope.key, "empty TfL stops snapshot; nothing written");
                return Ok(Applied::All);
            }
            let replaced = ds_store::tfl_stops::replace(conn, &stops, observed.produced_at())
                .await
                .map_err(|err| classify_anyhow(&err))?;
            count_apply(entry, stops.len(), replaced.written);
            if !replaced.invalid.is_empty() {
                tracing::warn!(
                    key = %entry.envelope.key,
                    invalid = ?replaced.invalid,
                    "left out TfL stops that are not valid rows"
                );
            }
            tracing::info!(
                key = %entry.envelope.key,
                written = replaced.written,
                deleted = replaced.deleted,
                links = replaced.links,
                skipped_older = replaced.skipped_older,
                "applied a TfL stops snapshot"
            );
            Ok(Applied::All)
        })
    }
}
