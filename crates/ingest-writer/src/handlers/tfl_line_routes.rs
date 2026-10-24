//! `tfl-line-routes/1` (stream `ds:ingest:tfl-stops`; DS-151,
//! docs/superpowers/specs/2026-10-23-trips-plan-tfl-stop-ends-design.md
//! §3): the body is a `Vec<common::tfl_routes::TflLineRoute>`, poller-tfl's
//! whole daily route-sequences snapshot in one part, written by
//! `ds_store::tfl_line_routes::replace`.
//!
//! The snapshot replaces `tfl_line_routes`, so it is applied only whole (no
//! `batch`, or `parts == 1`): a part alone would delete the other parts'
//! routes. A split one is poison (the producer never splits it). An empty
//! snapshot, and one older than the stored routes, write nothing and are
//! not errors. A route the store refuses before writing is left out and
//! logged.

use common::tfl_routes::TflLineRoute;
use ingest_stream::{HandlerError, StreamEntry};
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, classify_anyhow, count_apply, decode};
use crate::observed::Observed;

/// The `tfl-line-routes/1` handler.
pub struct TflLineRoutes;

impl SchemaHandler for TflLineRoutes {
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
                    "a tfl-line-routes snapshot must be one part: a part alone would delete \
                     the other parts' routes"
                        .to_owned(),
                ));
            }
            let routes: Vec<TflLineRoute> = decode(entry)?;
            if routes.is_empty() {
                tracing::warn!(key = %entry.envelope.key, "empty TfL routes snapshot; nothing written");
                return Ok(Applied::All);
            }
            let replaced =
                ds_store::tfl_line_routes::replace(conn, &routes, observed.produced_at())
                    .await
                    .map_err(|err| classify_anyhow(&err))?;
            count_apply(entry, routes.len(), replaced.written);
            if !replaced.invalid.is_empty() {
                tracing::warn!(
                    key = %entry.envelope.key,
                    invalid = ?replaced.invalid,
                    "left out TfL routes that are not valid rows"
                );
            }
            tracing::info!(
                key = %entry.envelope.key,
                written = replaced.written,
                deleted = replaced.deleted,
                skipped_older = replaced.skipped_older,
                "applied a TfL routes snapshot"
            );
            Ok(Applied::All)
        })
    }
}
