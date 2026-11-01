//! The writer's read-only gauges (spec §14.1), exported by every replica
//! (no loop lock: they only read):
//!
//! - `distant_signal_ingest_freshness_timestamp_seconds{source}`: each
//!   `ingest_freshness` row's `fetched_at` as Unix time, read every
//!   [`FRESHNESS_INTERVAL`] by [`freshness_loop`]. The chart's
//!   `DistantSignalIngestSourceStale` compares it with a per-source age,
//!   whichever path (the api's routes, a direct writer, a stream handler)
//!   recorded the row. A source with no row has no series.
//! - `distant_signal_ingest_stream_mode_info{stream, mode}`: 1 for each
//!   stream's `INGEST_WRITER_STREAMS` mode (`off` included), so a
//!   dashboard can show which streams this writer applies.
//! - schedule-reference's alert inputs from Postgres (`CronJob` phase 4,
//!   DS-241; docs/scheduled-jobs.md), read with the freshness rows by
//!   [`export_schedule_reference`]: a `CronJob` run of schedule-reference
//!   exits before Prometheus could scrape it, so its alerts read these.
//!   - `distant_signal_schedule_reference_recorded_signal_timestamp_seconds{kind, product}`:
//!     each `schedule_reference_signals` row's `recorded_at` (`seeded`,
//!     `rejected`, `staged_mismatch`, `settled`);
//!   - `distant_signal_schedule_reference_recorded_published_delivery_timestamp_seconds`:
//!     the delivery time (its directory name) of the newest
//!     `schedule_reference_publishes` row, as the in-pod
//!     `..._last_published_delivery_timestamp_seconds` gauge reports it.

use std::time::Duration;

use chrono::{DateTime, Utc};
use common::metrics::metric_name;
use sqlx::PgPool;

use crate::stream::{STREAMS, StreamModes};

/// `ingest_freshness_timestamp_seconds{source}`.
pub const FRESHNESS_METRIC: &str = "ingest_freshness_timestamp_seconds";
/// `ingest_stream_mode_info{stream, mode}`.
pub const STREAM_MODE_INFO: &str = "ingest_stream_mode_info";
/// `schedule_reference_recorded_signal_timestamp_seconds{kind, product}`.
pub const SCHEDULE_REFERENCE_SIGNAL_METRIC: &str =
    "schedule_reference_recorded_signal_timestamp_seconds";
/// `schedule_reference_recorded_published_delivery_timestamp_seconds`.
pub const SCHEDULE_REFERENCE_PUBLISHED_METRIC: &str =
    "schedule_reference_recorded_published_delivery_timestamp_seconds";
/// How often [`freshness_loop`] reads `ingest_freshness` (about a dozen
/// rows) and schedule-reference's rows (a few dozen).
pub const FRESHNESS_INTERVAL: Duration = Duration::from_secs(60);

/// Sets `ingest_stream_mode_info` for every stream the writer knows.
pub fn export_stream_modes(modes: &StreamModes) {
    for spec in STREAMS {
        common::metrics::set_info(
            STREAM_MODE_INFO,
            &[
                ("stream", spec.stream),
                ("mode", &modes.mode(spec.name).to_string()),
            ],
        );
    }
}

/// Reads every `ingest_freshness` row and sets its gauge; the number of
/// rows read.
pub async fn export_freshness(pool: &PgPool) -> sqlx::Result<usize> {
    let rows: Vec<(String, DateTime<Utc>)> =
        sqlx::query_as("SELECT source, fetched_at FROM ingest_freshness")
            .fetch_all(pool)
            .await?;
    for (source, fetched_at) in &rows {
        metrics::gauge!(metric_name(FRESHNESS_METRIC), "source" => source.clone())
            .set(unix_seconds(*fetched_at));
    }
    Ok(rows.len())
}

/// Reads schedule-reference's signal rows and its newest completed publish
/// and sets their gauges; the number of signal rows read. No completed
/// publish (or an unparseable delivery name) leaves that gauge unset, as
/// the in-pod one was before its first publish.
pub async fn export_schedule_reference(pool: &PgPool) -> anyhow::Result<usize> {
    use ds_store::schedule::reference_signals;
    let rows = reference_signals::all(pool).await?;
    for row in &rows {
        metrics::gauge!(
            metric_name(SCHEDULE_REFERENCE_SIGNAL_METRIC),
            "kind" => row.kind.clone(),
            "product" => row.product.clone()
        )
        .set(unix_seconds(row.recorded_at));
    }
    let delivery = ds_store::schedule::last_completed_schedule_reference_publish(pool).await?;
    if let Some(at) = delivery
        .as_deref()
        .and_then(reference_signals::delivery_timestamp)
    {
        metrics::gauge!(metric_name(SCHEDULE_REFERENCE_PUBLISHED_METRIC)).set(seconds_f64(at));
    }
    Ok(rows.len())
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Unix seconds stay exact in an f64 for millions of years"
)]
const fn seconds_f64(at: i64) -> f64 {
    at as f64
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Unix milliseconds stay exact in an f64 until the year 287,396"
)]
fn unix_seconds(at: DateTime<Utc>) -> f64 {
    at.timestamp_millis() as f64 / 1000.0
}

/// [`export_freshness`] every [`FRESHNESS_INTERVAL`], forever. A failed
/// read is logged and leaves the gauges as they were (they then age, which
/// is what the alert should see if the database is gone).
pub async fn freshness_loop(pool: PgPool) {
    let mut interval = tokio::time::interval(FRESHNESS_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        if let Err(err) = export_freshness(&pool).await {
            tracing::warn!(error = ?err, "could not read ingest_freshness for its gauges; will retry");
        }
        if let Err(err) = export_schedule_reference(&pool).await {
            tracing::warn!(
                error = ?err,
                "could not read schedule-reference's signals for their gauges; will retry"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    #[test]
    fn every_stream_has_a_mode_info_series() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let modes: StreamModes = "tfl:apply,station-samples:apply".parse().unwrap();
        metrics::with_local_recorder(&recorder, || export_stream_modes(&modes));
        let rendered = handle.render();
        for series in [
            r#"distant_signal_ingest_stream_mode_info{stream="ds:ingest:tfl",mode="apply"} 1"#,
            r#"distant_signal_ingest_stream_mode_info{stream="ds:ingest:station-samples",mode="apply"} 1"#,
            r#"distant_signal_ingest_stream_mode_info{stream="ds:ingest:reference",mode="off"} 1"#,
        ] {
            assert!(
                rendered.contains(series),
                "{series} missing from {rendered}"
            );
        }
    }

    #[test]
    fn unix_seconds_keeps_the_milliseconds() {
        let at = DateTime::parse_from_rfc3339("2026-10-08T12:00:00.250Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!((unix_seconds(at) - 1_791_460_800.25).abs() < 1e-6);
    }

    /// Database-gated: `DATABASE_URL`, a migrated database. The fixture row
    /// is written and deleted through `MIGRATION_DATABASE_URL` (the owner,
    /// under scripts/test-postgres-roles.py) when set: the narrow writer
    /// role may not delete from `ingest_freshness`, and may write only its
    /// own sources (security review L4), but reads every row.
    #[tokio::test]
    #[ignore = "needs DATABASE_URL (a migrated database)"]
    async fn the_freshness_gauge_follows_the_table() {
        let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        let admin = match std::env::var("MIGRATION_DATABASE_URL") {
            Ok(url) => PgPool::connect(&url).await.unwrap(),
            Err(_) => pool.clone(),
        };
        let source = format!("telemetry-test-{}", std::process::id());
        sqlx::query(
            "INSERT INTO ingest_freshness (source, fetched_at) VALUES ($1, '2026-10-08T12:00:00Z') \
             ON CONFLICT (source) DO UPDATE SET fetched_at = EXCLUDED.fetched_at",
        )
        .bind(&source)
        .execute(&admin)
        .await
        .unwrap();
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let read = {
            let _guard = metrics::set_default_local_recorder(&recorder);
            export_freshness(&pool).await.unwrap()
        };
        sqlx::query("DELETE FROM ingest_freshness WHERE source = $1")
            .bind(&source)
            .execute(&admin)
            .await
            .unwrap();
        assert!(read >= 1);
        let series = format!(
            r#"distant_signal_ingest_freshness_timestamp_seconds{{source="{source}"}} 1791460800"#
        );
        let rendered = handle.render();
        assert!(
            rendered.contains(&series),
            "{series} missing from {rendered}"
        );
    }

    /// Database-gated, like the freshness test: schedule-reference's signal
    /// rows and its newest completed publish become the alerts' gauges.
    /// Fixtures go in (and out) through `MIGRATION_DATABASE_URL` when set:
    /// the narrow writer role only reads these tables.
    #[tokio::test]
    #[ignore = "needs DATABASE_URL (a migrated database)"]
    async fn the_schedule_reference_gauges_follow_the_tables() {
        use ds_store::schedule::reference_signals::{self, Signal};
        let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        let admin = match std::env::var("MIGRATION_DATABASE_URL") {
            Ok(url) => PgPool::connect(&url).await.unwrap(),
            Err(_) => pool.clone(),
        };
        let product = format!("telemetry-test-{}", std::process::id());
        // Newer than any real row, so it is the newest completed publish.
        let delivery = "29991231T235959Z";
        reference_signals::record(&admin, Signal::Rejected, &product, Some(delivery))
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO schedule_reference_publishes (delivery, completed_at) \
             VALUES ($1, '2999-12-31T23:59:59Z') ON CONFLICT (delivery) DO NOTHING",
        )
        .bind(delivery)
        .execute(&admin)
        .await
        .unwrap();
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let read = {
            let _guard = metrics::set_default_local_recorder(&recorder);
            export_schedule_reference(&pool).await
        };
        sqlx::query("DELETE FROM schedule_reference_signals WHERE product = $1")
            .bind(&product)
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query("DELETE FROM schedule_reference_publishes WHERE delivery = $1")
            .bind(delivery)
            .execute(&admin)
            .await
            .unwrap();
        assert!(read.unwrap() >= 1);
        let rendered = handle.render();
        let signal = format!(
            r#"distant_signal_schedule_reference_recorded_signal_timestamp_seconds{{kind="rejected",product="{product}"}}"#
        );
        assert!(
            rendered.contains(&signal),
            "{signal} missing from {rendered}"
        );
        let published = "distant_signal_schedule_reference_recorded_published_delivery_timestamp_seconds 32503679999";
        assert!(
            rendered.contains(published),
            "{published} missing from {rendered}"
        );
    }
}
