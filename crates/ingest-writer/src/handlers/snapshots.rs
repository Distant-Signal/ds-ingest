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
//! rollout's compare step, and applied rows again in
//! `ingest_stream_row_writes_total{outcome}`: `written` (inserted or
//! updated) or `skipped` (unchanged, or refused by the guard as older).
//!
//! **Changed rows only** ([`Options::changed_rows_only`],
//! `INGEST_WRITER_CHANGED_ROWS_ONLY`, plan 3a.9, D12/D13, spec §7.8): only
//! `station-full-coverage-samples/1` changes. A row whose `stats` are
//! unchanged is not written (its `resolved_at` stays at the snapshot that
//! last changed it), its readers derive its age as
//! `GREATEST(resolved_at, the feed's observed time)`
//! (`ds_store::samples::STATION_FULL_COVERAGE_RESOLVED_AT_SQL`), and the
//! guard compares against the same derivation
//! ([`crate::observed::derived_guard`]). The other three keep their per-row
//! time, because the derivation needs every snapshot to carry every live
//! key and theirs do not (plan 3a.9's check):
//!
//! - `station_samples`: LDBWS visits about 255 of 560 stations a cycle;
//! - `full_coverage_line_window_stats`: a line with no population for the
//!   day gets no window rows, and the aggregator's 30-minute look-back would
//!   then read its last row as fresh; and `window_start`/`window_end` move
//!   on every write, so no write is a timestamp-only bump;
//! - `full_coverage_line_stats`: a snapshot carries only the current rail
//!   day, so after the rollover a reordered entry with the previous day's
//!   closing row would be refused against the new day's feed time.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use chrono::{DateTime, Utc};
use common::{
    FullCoverageLineStatsRow, FullCoverageWindowStatsRow, StationFullCoverageSample, StationSample,
};
use ds_store::samples::{self, RowWrites, SourceOrdering, full_coverage_window, sources};
use ingest_stream::{HandlerError, SchemaId, StreamEntry};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, apply_batch, classify_anyhow, decode};
use crate::observed::{Observed, derived_guard, guard};

/// The handlers' settings, from the writer's configuration.
#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
    /// `INGEST_WRITER_CHANGED_ROWS_ONLY` (plan 3a.9). **Off by default**:
    /// every handler writes as the api route does.
    pub changed_rows_only: bool,
}

/// Writes a batch of rows (already given their observed times) on the
/// writer's transaction, returning the rows written; `produced_at` is the
/// entry's clamped `produced_at`.
type WriteFn<T> = for<'c> fn(
    &'c mut PgConnection,
    &'c [T],
    DateTime<Utc>,
    Options,
) -> BoxFuture<'c, sqlx::Result<u64>>;

/// One snapshot schema's handler. See the module docs.
pub struct Snapshot<T> {
    /// The `ingest_freshness` source.
    source: &'static str,
    /// Clamps the row's own observed time in place (none for line stats).
    observe: fn(&mut T, &Observed),
    /// Refuses a body the api route would refuse (a 400): poison.
    validate: fn(&[T]) -> Result<(), String>,
    write: WriteFn<T>,
    options: Options,
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
            let (write, options) = (self.write, self.options);
            let written = Arc::new(AtomicU64::new(0));
            let counter = Arc::clone(&written);
            let outcome = apply_batch(conn, rows, move |conn, rows| {
                let counter = Arc::clone(&counter);
                Box::pin(async move {
                    let count = write(conn, rows, produced_at, options).await?;
                    counter.fetch_add(count, Ordering::Relaxed);
                    Ok(())
                })
            })
            .await?;
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
            let written = written.load(Ordering::Relaxed);
            ingest_stream::metrics::row_writes(
                &entry.stream,
                entry.envelope.schema.name(),
                written,
                u64::try_from(outcome.applied)
                    .unwrap_or(u64::MAX)
                    .saturating_sub(written),
            );
            outcome.into_applied(|rows| rows)
        })
    }
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "the handlers' validate fn-pointer type returns a Result"
)]
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
pub fn handlers(options: Options) -> Vec<(SchemaId, Arc<dyn SchemaHandler>)> {
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
            Arc::new(station_full_coverage_samples(options)),
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
        write: |conn, rows, _, _| {
            Box::pin(async move {
                samples::upsert_station_samples_on(conn, rows, Some(&STATION_SAMPLES_GUARD)).await
            })
        },
        options: Options::default(),
        rows: PhantomData,
    }
}

// ---------------------------------------------------------------------------
// station-full-coverage-samples/1 → station_full_coverage_samples, on
// resolved_at.

static STATION_FULL_COVERAGE_GUARD: LazyLock<String> =
    LazyLock::new(|| guard("station_full_coverage_samples", "resolved_at"));

/// The changed-rows-only guard (plan 3a.9): against
/// `GREATEST(resolved_at, the feed's observed time)`, the readers'
/// derivation.
static STATION_FULL_COVERAGE_DERIVED_GUARD: LazyLock<String> = LazyLock::new(|| {
    derived_guard(
        "station_full_coverage_samples",
        "resolved_at",
        sources::STATION_FULL_COVERAGE_SAMPLES,
    )
});

/// `station-full-coverage-samples/1`; with
/// [`Options::changed_rows_only`], an unchanged row is not written (see the
/// module docs).
pub fn station_full_coverage_samples(options: Options) -> Snapshot<StationFullCoverageSample> {
    Snapshot {
        source: sources::STATION_FULL_COVERAGE_SAMPLES,
        observe: |row, observed| row.resolved_at = observed.observed_at(Some(row.resolved_at)),
        validate: no_validation,
        write: |conn, rows, _, options| {
            Box::pin(async move {
                let writes = if options.changed_rows_only {
                    RowWrites::ChangedOnly(&STATION_FULL_COVERAGE_DERIVED_GUARD)
                } else {
                    RowWrites::EveryRow(Some(&STATION_FULL_COVERAGE_GUARD))
                };
                samples::upsert_station_full_coverage_samples_on(conn, rows, writes).await
            })
        },
        options,
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
        write: |conn, rows, produced_at, _| {
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
            })
        },
        options: Options::default(),
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
        write: |conn, rows, _, _| {
            Box::pin(async move {
                full_coverage_window::upsert_full_coverage_window_stats_on(
                    conn,
                    rows,
                    Some(&FULL_COVERAGE_WINDOW_GUARD),
                )
                .await
            })
        },
        options: Options::default(),
        rows: PhantomData,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_snapshot_schema_is_a_stream_schema() {
        for (schema, _) in handlers(Options::default()) {
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
        assert!(
            STATION_FULL_COVERAGE_DERIVED_GUARD
                .contains(samples::STATION_FULL_COVERAGE_RESOLVED_AT_SQL)
        );
    }
}
