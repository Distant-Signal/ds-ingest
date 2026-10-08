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

use std::time::Duration;

use chrono::{DateTime, Utc};
use common::metrics::metric_name;
use sqlx::PgPool;

use crate::stream::{STREAMS, StreamModes};

/// `ingest_freshness_timestamp_seconds{source}`.
pub const FRESHNESS_METRIC: &str = "ingest_freshness_timestamp_seconds";
/// `ingest_stream_mode_info{stream, mode}`.
pub const STREAM_MODE_INFO: &str = "ingest_stream_mode_info";
/// How often [`freshness_loop`] reads `ingest_freshness` (about a dozen
/// rows).
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
        let modes: StreamModes = "tfl:apply,station-samples:shadow".parse().unwrap();
        metrics::with_local_recorder(&recorder, || export_stream_modes(&modes));
        let rendered = handle.render();
        for series in [
            r#"distant_signal_ingest_stream_mode_info{stream="ds:ingest:tfl",mode="apply"} 1"#,
            r#"distant_signal_ingest_stream_mode_info{stream="ds:ingest:station-samples",mode="shadow"} 1"#,
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
}
