//! The `observed_at` guard helpers (spec §7.4 and §7.8, decision D13; plan
//! 3a.3). Every snapshot handler (3a.6, 3c) uses them:
//!
//! - **The observed time** of a row is its own time where it has one
//!   (`polled_at`, `resolved_at`, `computed_at`), else the envelope's
//!   `produced_at` (the fetch time, kept across retries, so a late apply
//!   still stamps the time the data was true): [`Observed::observed_at`].
//! - **The clamp.** Every observed time is clamped to the writer's
//!   `now() + 2 min`, and each clamp is counted in
//!   `ingest_stream_observed_at_clamped_total{stream,schema}`, so a producer
//!   whose clock ran ahead cannot stamp rows in the future.
//! - **The guard.** The upsert's `WHERE` is [`guard`]: the incoming row wins
//!   when it is at least as new, when the stored row has no time yet
//!   (`NULL`, from before the column existed, counts as older), or when the
//!   stored row is stamped more than 2 minutes in the future. That last arm
//!   heals a row a skewed clock wrote before the clamp existed: the next
//!   snapshot overwrites it instead of every real one being refused until
//!   the clock catches up.
//!
//! No lower bound: the guard and the stream caps cover old data.

use chrono::{DateTime, TimeDelta, Utc};
use ingest_stream::StreamEntry;

/// How far ahead of the writer's clock an observed time may be.
pub const MAX_AHEAD: TimeDelta = TimeDelta::minutes(2);

/// [`MAX_AHEAD`] as SQL, for the guard's healing arm.
pub const MAX_AHEAD_SQL: &str = "interval '2 min'";

/// `time` clamped to `now + MAX_AHEAD`, and whether it was clamped.
pub fn clamp(time: DateTime<Utc>, now: DateTime<Utc>) -> (DateTime<Utc>, bool) {
    let limit = now + MAX_AHEAD;
    if time > limit {
        (limit, true)
    } else {
        (time, false)
    }
}

/// The observed-time context of one stream entry: its stream, schema and
/// `produced_at`, and the writer's clock when the entry was handled.
#[derive(Clone, Debug)]
pub struct Observed {
    stream: String,
    schema: String,
    produced_at: DateTime<Utc>,
    now: DateTime<Utc>,
}

impl Observed {
    /// For `entry`, at the writer's current time.
    pub fn for_entry(entry: &StreamEntry) -> Self {
        Self::at(
            &entry.stream,
            &entry.envelope.schema.to_string(),
            entry.envelope.produced_at,
            Utc::now(),
        )
    }

    /// For an entry of `stream`/`schema` produced at `produced_at`, with the
    /// writer's clock at `now`.
    pub fn at(stream: &str, schema: &str, produced_at: DateTime<Utc>, now: DateTime<Utc>) -> Self {
        Self {
            stream: stream.to_owned(),
            schema: schema.to_owned(),
            produced_at,
            now,
        }
    }

    /// The writer's clock this context clamps against.
    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }

    /// The envelope's `produced_at`, clamped (and counted): the observed
    /// time of a row with no time of its own (`source_updated_at`, `TfL`'s
    /// `computed_at`, freshness).
    pub fn produced_at(&self) -> DateTime<Utc> {
        self.observed_at(None)
    }

    /// The row's own time where it has one, else `produced_at`; clamped to
    /// the writer's `now() + 2 min`, each clamp counted.
    pub fn observed_at(&self, row_time: Option<DateTime<Utc>>) -> DateTime<Utc> {
        let (time, clamped) = clamp(row_time.unwrap_or(self.produced_at), self.now);
        if clamped {
            ingest_stream::metrics::observed_at_clamped(&self.stream, &self.schema, 1);
            tracing::debug!(
                stream = %self.stream,
                schema = %self.schema,
                observed_at = %row_time.unwrap_or(self.produced_at),
                clamped_to = %time,
                "observed time ahead of the writer's clock; clamped"
            );
        }
        time
    }
}

/// The ordering guard for an upsert into `table` on its observed-time
/// `column`, for `ON CONFLICT … DO UPDATE SET … WHERE <guard>`:
///
/// ```text
/// (table.column IS NULL OR EXCLUDED.column >= table.column
///  OR table.column > now() + interval '2 min')
/// ```
///
/// `table` is the conflict target's name (or alias) and `column` the time
/// column; both are SQL identifiers fixed in the handler's code, never
/// data.
///
/// # Panics
///
/// If either is not a plain lowercase identifier: a programming error.
pub fn guard(table: &str, column: &str) -> String {
    assert!(
        is_identifier(table),
        "guard: bad table identifier {table:?}"
    );
    assert!(
        is_identifier(column),
        "guard: bad column identifier {column:?}"
    );
    guard_against(&format!("{table}.{column}"), column)
}

/// [`guard`] against the derived time of a changed-rows-only table (plan
/// 3a.9, spec §7.8): `GREATEST(table.column, the feed's observed time)`
/// (`ds_store::samples::feed_observed_at_sql` for `source`). An unchanged
/// row the writer skipped keeps its own older time, so comparing against
/// the row's time alone would let an older snapshot, redelivered after that
/// skipped newer one, overwrite it. The feed's time is read in the entry's
/// transaction before this entry records its own, so the entry being
/// applied compares against the previous snapshot. The same healing arm
/// applies to the derived time.
///
/// # Panics
///
/// As [`guard`], or if `source` is not a [`ds_store::samples::sources`]
/// shaped name.
pub fn derived_guard(table: &str, column: &str, source: &str) -> String {
    assert!(
        is_identifier(table),
        "guard: bad table identifier {table:?}"
    );
    assert!(
        is_identifier(column),
        "guard: bad column identifier {column:?}"
    );
    guard_against(
        &ds_store::samples::feed_observed_at_sql(&format!("{table}.{column}"), source),
        column,
    )
}

/// `stored IS NULL OR EXCLUDED.column >= stored OR stored > now() + 2 min`.
fn guard_against(stored: &str, column: &str) -> String {
    format!(
        "({stored} IS NULL OR EXCLUDED.{column} >= {stored} \
         OR {stored} > now() + {MAX_AHEAD_SQL})"
    )
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().to_utc()
    }

    fn clamped_count(rendered: &str, stream: &str) -> u64 {
        let prefix = format!(
            "distant_signal_ingest_stream_observed_at_clamped_total{{stream=\"{stream}\",schema=\"test/1\"}} "
        );
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(&prefix)?.trim().parse().ok())
            .unwrap_or(0)
    }

    /// Plan 3a.3: a time 5 min ahead is clamped and counted, one 1 min ahead
    /// is not.
    #[test]
    fn a_time_five_minutes_ahead_is_clamped_and_counted_one_minute_is_not() {
        let now = at("2026-10-07T12:00:00Z");
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let (ahead_5, ahead_1, own_time) = metrics::with_local_recorder(&recorder, || {
            let observed = Observed::at("s", "test/1", now + TimeDelta::minutes(5), now);
            let ahead_5 = observed.produced_at();
            let ahead_1 =
                Observed::at("s", "test/1", now + TimeDelta::minutes(1), now).observed_at(None);
            // The row's own time wins over produced_at, and is clamped too.
            let own_time = observed.observed_at(Some(now - TimeDelta::minutes(3)));
            (ahead_5, ahead_1, own_time)
        });
        assert_eq!(ahead_5, now + TimeDelta::minutes(2));
        assert_eq!(ahead_1, now + TimeDelta::minutes(1));
        assert_eq!(own_time, now - TimeDelta::minutes(3));
        assert_eq!(clamped_count(&handle.render(), "s"), 1);

        let more = metrics::with_local_recorder(&recorder, || {
            Observed::at("s", "test/1", now, now).observed_at(Some(now + TimeDelta::hours(1)))
        });
        assert_eq!(more, now + TimeDelta::minutes(2));
        assert_eq!(clamped_count(&handle.render(), "s"), 2);
    }

    #[test]
    fn clamp_is_inclusive_at_the_limit() {
        let now = at("2026-10-07T12:00:00Z");
        assert_eq!(clamp(now + MAX_AHEAD, now), (now + MAX_AHEAD, false));
        assert_eq!(
            clamp(now + MAX_AHEAD + TimeDelta::milliseconds(1), now),
            (now + MAX_AHEAD, true)
        );
        let old = now - TimeDelta::days(3);
        assert_eq!(clamp(old, now), (old, false));
    }

    #[test]
    fn the_guard_fragment() {
        assert_eq!(
            guard("station_samples", "polled_at"),
            "(station_samples.polled_at IS NULL OR EXCLUDED.polled_at >= station_samples.polled_at \
             OR station_samples.polled_at > now() + interval '2 min')"
        );
    }

    #[test]
    fn the_derived_guard_compares_against_the_greatest_of_row_and_feed_time() {
        let derived = "GREATEST(t.c, (SELECT fetched_at FROM ingest_freshness WHERE source = 'f'))";
        assert_eq!(
            derived_guard("t", "c", "f"),
            format!(
                "({derived} IS NULL OR EXCLUDED.c >= {derived} \
                 OR {derived} > now() + interval '2 min')"
            )
        );
    }

    #[test]
    #[should_panic(expected = "bad column identifier")]
    fn the_guard_refuses_anything_but_an_identifier() {
        let _ = guard("t", "x; DROP TABLE t");
    }
}
