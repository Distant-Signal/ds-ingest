//! The handler registry (spec §10, plan 3a.3): `schema name/version →`
//! [`SchemaHandler`].
//!
//! A handler is a thin wrapper over the `ds-store` function the api's
//! `/private` route calls today. It is given the writer's open transaction
//! (the `ingest_dedup` row is already in it) and the entry's [`Observed`]
//! clock, and it:
//!
//! - decodes the payload (`entry.envelope.payload_as()`); a payload that
//!   does not decode is [`HandlerError::Poison`];
//! - writes, guarding each upsert with [`crate::observed::guard`] on the
//!   row's observed time;
//! - isolates rows the database refuses for a data error with
//!   [`apply_rows`], returning them as [`Applied::PartiallyRejected`] (they
//!   go to the dead-letter stream, the rest commit).
//!
//! [`check`](SchemaHandler::check) is the shadow mode's half: decode and
//! validate, write nothing.
//!
//! Lookup ([`Registry::lookup`]): an unknown schema *name* is poison; a known
//! name with an unknown *version* is [`HandlerError::UnsupportedSchema`] (the
//! producer is newer: the entry stays pending and alerts, spec §13.3).
//!
//! **The product handlers** ([`registry`], plan 3a.6) are the four snapshot
//! schemas of `ds:ingest:station-samples` and `ds:ingest:full-coverage`, in
//! [`snapshots`]; 3c adds `TfL`, tocs and the island of Ireland. The writer
//! refuses to start a stream whose schemas have no handler
//! (`stream::StreamModes::uncovered`), so a stream turned on early cannot
//! dead-letter everything as unknown.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::bail;
use ingest_stream::{HandlerError, SchemaId, StreamEntry};
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::PgConnection;

use crate::observed::Observed;

pub mod snapshots;

/// A boxed, sendable future borrowing for `'a`.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What a handler wrote.
#[derive(Debug)]
pub enum Applied {
    /// Every row (or the entry as a whole).
    All,
    /// Every row except `rejected` (a JSON payload in the entry's schema),
    /// which the database refused for a data error.
    PartiallyRejected {
        reason: String,
        rejected: Box<RawValue>,
    },
}

/// One schema's handler. See the module docs.
pub trait SchemaHandler: Send + Sync {
    /// Decodes and validates the payload; writes nothing (shadow mode).
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError>;

    /// Applies the entry inside the writer's transaction `conn`. A
    /// [`HandlerError`] rolls the whole entry back (the dedup row with it).
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>>;
}

/// The handlers by schema name, then version.
#[derive(Default)]
pub struct Registry {
    by_name: BTreeMap<String, BTreeMap<u32, Arc<dyn SchemaHandler>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `handler` for `schema`. A schema registered twice is a bug.
    pub fn register(
        &mut self,
        schema: &SchemaId,
        handler: impl SchemaHandler + 'static,
    ) -> anyhow::Result<()> {
        self.register_arc(schema, Arc::new(handler))
    }

    /// [`Registry::register`] for a handler already behind an `Arc`.
    pub fn register_arc(
        &mut self,
        schema: &SchemaId,
        handler: Arc<dyn SchemaHandler>,
    ) -> anyhow::Result<()> {
        let versions = self.by_name.entry(schema.name().to_owned()).or_default();
        if versions.contains_key(&schema.version()) {
            bail!("ingest handler for {schema} registered twice");
        }
        versions.insert(schema.version(), handler);
        Ok(())
    }

    /// The handler for `schema`: unknown name → poison, unknown version →
    /// unsupported (left pending).
    pub fn lookup(&self, schema: &SchemaId) -> Result<&dyn SchemaHandler, HandlerError> {
        let Some(versions) = self.by_name.get(schema.name()) else {
            return Err(HandlerError::Poison(format!(
                "unknown ingest schema name {:?}",
                schema.name()
            )));
        };
        versions
            .get(&schema.version())
            .map(AsRef::as_ref)
            .ok_or_else(|| {
                HandlerError::UnsupportedSchema(format!(
                    "{schema}: this writer knows versions {:?}",
                    versions.keys().collect::<Vec<_>>()
                ))
            })
    }

    /// Whether any version of schema `name` has a handler.
    pub fn knows(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }

    /// Every registered `name/version`.
    pub fn schemas(&self) -> Vec<String> {
        self.by_name
            .iter()
            .flat_map(|(name, versions)| versions.keys().map(move |v| format!("{name}/{v}")))
            .collect()
    }
}

/// The writer's registry: every product schema it applies. Plan 3a.6:
/// `station-samples/1` and the three full-coverage schemas; 3c adds
/// `tfl-line-status/1`, `tocs/1` and the island-of-Ireland schemas.
pub fn registry() -> Registry {
    let mut registry = Registry::new();
    for (schema, handler) in snapshots::handlers() {
        if let Err(err) = registry.register_arc(&schema, handler) {
            // A schema listed twice in `snapshots::handlers`: a bug the
            // unit test `the_registry_has_the_four_snapshot_schemas` catches.
            tracing::error!(error = %err, "ingest handler registry");
        }
    }
    registry
}

/// Maps a database error to the runtime's outcome (spec §7.3):
///
/// - SQLSTATE class 22 (data exception) or 23 (integrity constraint
///   violation) is the entry's fault: [`HandlerError::Poison`];
/// - everything else is [`HandlerError::Transient`]: connection (08),
///   serialization and deadlock (40), resources (53), operator intervention
///   such as a statement or lock timeout (57), a pool timeout, I/O, and also
///   a writer bug or a missing grant (42), which no amount of dead-lettering
///   fixes. It stays pending and the oldest-pending-age alert fires.
pub fn classify(err: &sqlx::Error) -> HandlerError {
    if is_data_error(err) {
        HandlerError::Poison(format!("data error: {err}"))
    } else {
        HandlerError::Transient(err.to_string())
    }
}

/// SQLSTATE class 22 or 23.
pub fn is_data_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db) => db
            .code()
            .is_some_and(|code| code.starts_with("22") || code.starts_with("23")),
        _ => false,
    }
}

/// Decodes the entry's payload as `T`; a payload that does not decode is
/// poison.
pub fn decode<T: serde::de::DeserializeOwned>(entry: &StreamEntry) -> Result<T, HandlerError> {
    entry.envelope.payload_as().map_err(|err| {
        HandlerError::Poison(format!(
            "{} payload does not decode: {err}",
            entry.envelope.schema
        ))
    })
}

/// The rows [`apply_rows`] wrote and the ones it refused, with the
/// database's message for each.
#[derive(Debug)]
pub struct RowOutcome<T> {
    pub applied: usize,
    pub rejected: Vec<(T, String)>,
}

impl<T: Serialize> RowOutcome<T> {
    /// [`Applied::All`] when nothing was refused, else
    /// [`Applied::PartiallyRejected`] with the refused rows serialized by
    /// `payload` (the entry's body shape) and their messages as the reason.
    pub fn into_applied<P: Serialize>(
        self,
        payload: impl FnOnce(Vec<T>) -> P,
    ) -> Result<Applied, HandlerError> {
        if self.rejected.is_empty() {
            return Ok(Applied::All);
        }
        let count = self.rejected.len();
        let (rows, errors): (Vec<T>, Vec<String>) = self.rejected.into_iter().unzip();
        let mut reason = format!("{count} row(s) refused: {}", errors.join("; "));
        // A dead-letter field, not a log line: keep it bounded.
        if reason.len() > 2000 {
            let mut end = 2000;
            while !reason.is_char_boundary(end) {
                end -= 1;
            }
            reason.truncate(end);
            reason.push('…');
        }
        let rejected = serde_json::value::to_raw_value(&payload(rows))
            .map_err(|err| HandlerError::Transient(format!("serializing rejected rows: {err}")))?;
        Ok(Applied::PartiallyRejected { reason, rejected })
    }
}

/// Per-row isolation (spec §7.3): writes each row with `write` inside a
/// savepoint of the open transaction `conn`. A row the database refuses for
/// a data error (class 22/23) is rolled back to its savepoint and collected;
/// any other error aborts the entry ([`classify`]).
///
/// A savepoint per row is a round trip each; a handler with a batched
/// statement tries the batch first (in its own savepoint) and falls back to
/// this only on a data error.
pub async fn apply_rows<T, F>(
    conn: &mut PgConnection,
    rows: Vec<T>,
    mut write: F,
) -> Result<RowOutcome<T>, HandlerError>
where
    T: Send + Sync,
    F: for<'c> FnMut(&'c mut PgConnection, &'c T) -> BoxFuture<'c, sqlx::Result<()>>,
{
    let mut outcome = RowOutcome {
        applied: 0,
        rejected: Vec::new(),
    };
    for row in rows {
        execute(conn, "SAVEPOINT ingest_row").await?;
        match write(&mut *conn, &row).await {
            Ok(()) => {
                execute(conn, "RELEASE SAVEPOINT ingest_row").await?;
                outcome.applied += 1;
            }
            Err(err) if is_data_error(&err) => {
                execute(conn, "ROLLBACK TO SAVEPOINT ingest_row").await?;
                execute(conn, "RELEASE SAVEPOINT ingest_row").await?;
                outcome.rejected.push((row, err.to_string()));
            }
            Err(err) => return Err(classify(&err)),
        }
    }
    Ok(outcome)
}

/// [`apply_rows`] for a handler with a batched statement: `write` runs once
/// for all `rows` inside a savepoint; only if the database refuses that for
/// a data error (class 22/23) is it rolled back and retried a row at a time
/// (`write` with a one-row slice), so the refused rows are isolated and the
/// rest commit. Any other error aborts the entry ([`classify`]).
pub async fn apply_batch<T, F>(
    conn: &mut PgConnection,
    rows: Vec<T>,
    mut write: F,
) -> Result<RowOutcome<T>, HandlerError>
where
    T: Send + Sync,
    F: for<'c> FnMut(&'c mut PgConnection, &'c [T]) -> BoxFuture<'c, sqlx::Result<()>>,
{
    execute(conn, "SAVEPOINT ingest_batch").await?;
    match write(&mut *conn, &rows).await {
        Ok(()) => {
            execute(conn, "RELEASE SAVEPOINT ingest_batch").await?;
            Ok(RowOutcome {
                applied: rows.len(),
                rejected: Vec::new(),
            })
        }
        Err(err) if is_data_error(&err) => {
            execute(conn, "ROLLBACK TO SAVEPOINT ingest_batch").await?;
            execute(conn, "RELEASE SAVEPOINT ingest_batch").await?;
            tracing::warn!(error = %err, rows = rows.len(), "a batch was refused for a data error; isolating its rows");
            apply_rows(conn, rows, |conn, row| {
                write(conn, std::slice::from_ref(row))
            })
            .await
        }
        Err(err) => Err(classify(&err)),
    }
}

/// [`classify`] for an `anyhow` error from a `ds-store` function: its
/// `sqlx` cause decides; any other error is [`HandlerError::Transient`].
pub fn classify_anyhow(err: &anyhow::Error) -> HandlerError {
    match err.downcast_ref::<sqlx::Error>() {
        Some(err) => classify(err),
        None => HandlerError::Transient(format!("{err:#}")),
    }
}

async fn execute(conn: &mut PgConnection, sql: &'static str) -> Result<(), HandlerError> {
    sqlx::query(sql)
        .execute(&mut *conn)
        .await
        .map(|_| ())
        .map_err(|err| classify(&err))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Nop;

    impl SchemaHandler for Nop {
        fn check(&self, _: &StreamEntry) -> Result<(), HandlerError> {
            Ok(())
        }

        fn apply<'a>(
            &'a self,
            _: &'a mut PgConnection,
            _: &'a StreamEntry,
            _: &'a Observed,
        ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
            Box::pin(async { Ok(Applied::All) })
        }
    }

    fn schema(name: &str, version: u32) -> SchemaId {
        SchemaId::new(name, version).unwrap()
    }

    #[test]
    fn lookup_tells_an_unknown_name_from_an_unknown_version() {
        let mut registry = Registry::new();
        registry.register(&schema("test-rows", 1), Nop).unwrap();
        assert!(registry.lookup(&schema("test-rows", 1)).is_ok());
        assert!(matches!(
            registry.lookup(&schema("test-rows", 2)),
            Err(HandlerError::UnsupportedSchema(_))
        ));
        assert!(matches!(
            registry.lookup(&schema("other", 1)),
            Err(HandlerError::Poison(_))
        ));
        assert!(registry.knows("test-rows"));
        assert_eq!(registry.schemas(), ["test-rows/1"]);
    }

    #[test]
    fn a_schema_registered_twice_is_refused() {
        let mut registry = Registry::new();
        registry.register(&schema("test-rows", 1), Nop).unwrap();
        assert!(registry.register(&schema("test-rows", 1), Nop).is_err());
        registry.register(&schema("test-rows", 2), Nop).unwrap();
    }

    #[test]
    fn the_registry_has_the_four_snapshot_schemas() {
        assert_eq!(
            registry().schemas(),
            [
                "full-coverage-stats/1",
                "full-coverage-window-stats/1",
                "station-full-coverage-samples/1",
                "station-samples/1",
            ]
        );
    }

    #[test]
    fn non_database_errors_are_transient() {
        assert!(matches!(
            classify(&sqlx::Error::PoolTimedOut),
            HandlerError::Transient(_)
        ));
        assert!(!is_data_error(&sqlx::Error::RowNotFound));
    }

    #[test]
    fn rejected_rows_become_a_bounded_payload() {
        let outcome = RowOutcome {
            applied: 1,
            rejected: vec![(7_u32, "x".repeat(3000))],
        };
        let Applied::PartiallyRejected { reason, rejected } =
            outcome.into_applied(|rows| rows).unwrap()
        else {
            panic!("expected PartiallyRejected");
        };
        assert_eq!(rejected.get(), "[7]");
        assert!(reason.starts_with("1 row(s) refused: xxx"));
        assert!(reason.chars().count() <= 2001);

        let none: RowOutcome<u32> = RowOutcome {
            applied: 2,
            rejected: vec![],
        };
        assert!(matches!(none.into_applied(|rows| rows), Ok(Applied::All)));
    }
}
