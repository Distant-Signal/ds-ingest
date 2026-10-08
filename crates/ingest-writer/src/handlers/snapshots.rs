//! The snapshot handlers (plan 3a.6, spec §7.4 and §7.8): `station-samples/1`
//! on `ds:ingest:station-samples`, and `full-coverage-stats/1`,
//! `full-coverage-window-stats/1` and `station-full-coverage-samples/1` on
//! `ds:ingest:full-coverage`.
//!
//! Each `/1` body is today's api route body (a JSON array of rows), and each
//! handler is the `ds-store` upsert that route calls, on the writer's
//! transaction, with:
//!
//! - **the observed time** of each row (D13): its own time where it has one
//!   (`polled_at`, `resolved_at`, `computed_at`), clamped to the writer's
//!   `now() + 2 min`; `full_coverage_line_stats` has none, so it gets
//!   `source_updated_at := produced_at` (clamped);
//! - **the ordering guard** on that column (`observed::guard`), so an older
//!   snapshot applied after a newer one (a reclaimed entry, a dead-letter
//!   re-injection) changes nothing;
//! - **per-row isolation**: the batch runs in a savepoint, and only on a
//!   data error is it retried a row at a time (`apply_batch`);
//! - **freshness as "data as of"**: `record_ingest(source, produced_at)`,
//!   which never moves backwards (`ds_store::samples::sources`);
//! - `full-coverage-window-stats/1` is validated as the api route validates
//!   it (`full_coverage_window::validate`): an invalid row is the whole
//!   entry's fault (poison), as it is a 400 for the route.
//!
//! Rows are counted in `ingest_stream_rows_total{mode}` (`shadow` from
//! [`SchemaHandler::check`], `apply` from [`SchemaHandler::apply`]) for the
//! rollout's compare step.

use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};

use chrono::{DateTime, Utc};
use common::{
    FullCoverageLineStatsRow, FullCoverageWindowStatsRow, StationFullCoverageSample, StationSample,
};
use ds_store::samples::{self, SourceOrdering, full_coverage_window, sources};
use ingest_stream::{HandlerError, SchemaId, StreamEntry};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, apply_batch, classify_anyhow, decode};
use crate::observed::{Observed, guard};

/// Writes a batch of rows (already given their observed times) on the
/// writer's transaction; `produced_at` is the entry's clamped `produced_at`.
type WriteFn<T> =
    for<'c> fn(&'c mut PgConnection, &'c [T], DateTime<Utc>) -> BoxFuture<'c, sqlx::Result<()>>;

/// One snapshot schema's handler. See the module docs.
pub struct Snapshot<T> {
    /// The `ingest_freshness` source.
    source: &'static str,
    /// Clamps the row's own observed time in place (none for line stats).
    observe: fn(&mut T, &Observed),
    /// Refuses a body the api route would refuse (a 400): poison.
    validate: fn(&[T]) -> Result<(), String>,
    write: WriteFn<T>,
    rows: PhantomData<fn() -> T>,
}

impl<T> Snapshot<T>
where
    T: DeserializeOwned + Serialize + Send + Sync + 'static,
{
    fn decode_valid(&self, entry: &StreamEntry) -> Result<Vec<T>, HandlerError> {
        let rows: Vec<T> = decode(entry)?;
        (self.validate)(&rows).map_err(|problem| {
            HandlerError::Poison(format!("{}: {problem}", entry.envelope.schema))
        })?;
        Ok(rows)
    }
}

impl<T> SchemaHandler for Snapshot<T>
where
    T: DeserializeOwned + Serialize + Send + Sync + 'static,
{
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        let rows = self.decode_valid(entry)?;
        ingest_stream::metrics::rows(
            &entry.stream,
            entry.envelope.schema.name(),
            "shadow",
            rows.len(),
        );
        Ok(())
    }

    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let mut rows = self.decode_valid(entry)?;
            for row in &mut rows {
                (self.observe)(row, observed);
            }
            let produced_at = observed.produced_at();
            let write = self.write;
            let outcome =
                apply_batch(conn, rows, move |conn, rows| write(conn, rows, produced_at)).await?;
            if outcome.applied > 0 {
                ds_store::freshness::record_ingest(conn, self.source, Some(produced_at))
                    .await
                    .map_err(|err| classify_anyhow(&err))?;
            }
            ingest_stream::metrics::rows(
                &entry.stream,
                entry.envelope.schema.name(),
                "apply",
                outcome.applied,
            );
            outcome.into_applied(|rows| rows)
        })
    }
}

fn no_validation<T>(_: &[T]) -> Result<(), String> {
    Ok(())
}

fn schema(name: &str) -> SchemaId {
    #[expect(
        clippy::expect_used,
        reason = "the names below are constant and valid; a unit test builds each"
    )]
    SchemaId::new(name, 1).expect("a valid schema id")
}

/// Every snapshot schema and its handler, for `handlers::registry`.
pub fn handlers() -> Vec<(SchemaId, Arc<dyn SchemaHandler>)> {
    vec![
        (schema("station-samples"), Arc::new(station_samples())),
        (
            schema("full-coverage-stats"),
            Arc::new(full_coverage_stats()),
        ),
        (
            schema("full-coverage-window-stats"),
            Arc::new(full_coverage_window_stats()),
        ),
        (
            schema("station-full-coverage-samples"),
            Arc::new(station_full_coverage_samples()),
        ),
    ]
}

// ---------------------------------------------------------------------------
// station-samples/1 → station_samples, on polled_at.

static STATION_SAMPLES_GUARD: LazyLock<String> =
    LazyLock::new(|| guard("station_samples", "polled_at"));

/// `station-samples/1`.
pub fn station_samples() -> Snapshot<StationSample> {
    Snapshot {
        source: sources::STATION_SAMPLES,
        observe: |row, observed| row.polled_at = observed.observed_at(Some(row.polled_at)),
        validate: no_validation,
        write: |conn, rows, _| {
            Box::pin(async move {
                samples::upsert_station_samples_on(conn, rows, Some(&STATION_SAMPLES_GUARD))
                    .await
                    .map(drop)
            })
        },
        rows: PhantomData,
    }
}

// ---------------------------------------------------------------------------
// station-full-coverage-samples/1 → station_full_coverage_samples, on
// resolved_at.

static STATION_FULL_COVERAGE_GUARD: LazyLock<String> =
    LazyLock::new(|| guard("station_full_coverage_samples", "resolved_at"));

/// `station-full-coverage-samples/1`.
pub fn station_full_coverage_samples() -> Snapshot<StationFullCoverageSample> {
    Snapshot {
        source: sources::STATION_FULL_COVERAGE_SAMPLES,
        observe: |row, observed| row.resolved_at = observed.observed_at(Some(row.resolved_at)),
        validate: no_validation,
        write: |conn, rows, _| {
            Box::pin(async move {
                samples::upsert_station_full_coverage_samples_on(
                    conn,
                    rows,
                    Some(&STATION_FULL_COVERAGE_GUARD),
                )
                .await
                .map(drop)
            })
        },
        rows: PhantomData,
    }
}

// ---------------------------------------------------------------------------
// full-coverage-stats/1 → full_coverage_line_stats, on source_updated_at
// (the body carries no time: produced_at, plan 3a.5).

static FULL_COVERAGE_STATS_GUARD: LazyLock<String> =
    LazyLock::new(|| guard("full_coverage_line_stats", "source_updated_at"));

/// `full-coverage-stats/1`.
pub fn full_coverage_stats() -> Snapshot<FullCoverageLineStatsRow> {
    Snapshot {
        source: sources::FULL_COVERAGE_STATS,
        observe: |_, _| {},
        validate: no_validation,
        write: |conn, rows, produced_at| {
            Box::pin(async move {
                samples::upsert_full_coverage_line_stats_on(
                    conn,
                    rows,
                    Some(SourceOrdering {
                        source_updated_at: produced_at,
                        guard: &FULL_COVERAGE_STATS_GUARD,
                    }),
                )
                .await
                .map(drop)
            })
        },
        rows: PhantomData,
    }
}

// ---------------------------------------------------------------------------
// full-coverage-window-stats/1 → full_coverage_line_window_stats, on
// computed_at.

static FULL_COVERAGE_WINDOW_GUARD: LazyLock<String> =
    LazyLock::new(|| guard("full_coverage_line_window_stats", "computed_at"));

/// `full-coverage-window-stats/1`.
pub fn full_coverage_window_stats() -> Snapshot<FullCoverageWindowStatsRow> {
    Snapshot {
        source: sources::FULL_COVERAGE_WINDOW_STATS,
        observe: |row, observed| row.computed_at = observed.observed_at(Some(row.computed_at)),
        validate: |rows| full_coverage_window::validate(rows).map_err(|err| err.to_string()),
        write: |conn, rows, _| {
            Box::pin(async move {
                full_coverage_window::upsert_full_coverage_window_stats_on(
                    conn,
                    rows,
                    Some(&FULL_COVERAGE_WINDOW_GUARD),
                )
                .await
                .map(drop)
            })
        },
        rows: PhantomData,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_snapshot_schema_is_a_stream_schema() {
        for (schema, _) in handlers() {
            assert_eq!(schema.version(), 1);
            assert!(
                crate::stream::STREAMS
                    .iter()
                    .any(|spec| spec.schemas.contains(&schema.name())),
                "{schema}"
            );
        }
    }

    #[test]
    fn the_guards_name_each_table_s_observed_time() {
        assert!(STATION_SAMPLES_GUARD.contains("station_samples.polled_at"));
        assert!(STATION_FULL_COVERAGE_GUARD.contains("station_full_coverage_samples.resolved_at"));
        assert!(FULL_COVERAGE_STATS_GUARD.contains("full_coverage_line_stats.source_updated_at"));
        assert!(FULL_COVERAGE_WINDOW_GUARD.contains("full_coverage_line_window_stats.computed_at"));
    }
}
