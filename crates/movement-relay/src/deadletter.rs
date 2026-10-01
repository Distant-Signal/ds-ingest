//! Age limit on `movement-events-deadletter` (D5, 2026-09-30).
//!
//! The dead-letter stream holds raw TRUST payloads that the three consumer
//! groups set aside (`movement_feed::redis_stream`). Its 10,000-record cap
//! is never trimmed by count, but the TRUST 1-day retention safeguard
//! (`crates/aggregator/src/config.rs`) means a record must not outlive a
//! day either. movement-relay's lag loop therefore runs
//! `XTRIM movement-events-deadletter MINID <now - max age>` every tick,
//! counts what it removed, and reports the oldest record's age so an alert
//! fires while there is still time to re-inject it
//! (`docs/movement-events-deadletter.md`).
//!
//! The age limit is configurable (`--deadletter-max-age-secs`) but can
//! never exceed [`MAX_DEADLETTER_AGE_SECS`].

use std::time::Duration;

/// movement-feed's shared dead-letter stream (`movement_feed::redis_stream`
/// names it `<stream>-deadletter`; this crate does not depend on
/// movement-feed).
pub(crate) const DEADLETTER_STREAM: &str = "movement-events-deadletter";

/// Hard upper bound on `--deadletter-max-age-secs`: 24 hours, the TRUST
/// 1-day retention safeguard. clap refuses anything longer, and so does
/// the chart.
pub(crate) const MAX_DEADLETTER_AGE_SECS: u64 = 24 * 60 * 60;

/// Lower bound on `--deadletter-max-age-secs`: an hour. Anything shorter
/// leaves no time to inspect or re-inject a record and is almost
/// certainly a typo (minutes entered as seconds).
pub(crate) const MIN_DEADLETTER_AGE_SECS: u64 = 60 * 60;

/// The `MINID` threshold for `now_ms` and `max_age`: every entry whose id
/// is below it (added more than `max_age` ago, by the stream-id clock) is
/// trimmed. Entry ids are `<ms>-<seq>`, so `<cutoff>-0` keeps everything
/// added at or after the cutoff millisecond.
#[expect(
    clippy::cast_possible_truncation,
    reason = "these durations are seconds to hours, far below u64::MAX milliseconds"
)]
pub(crate) fn min_id(now_ms: u64, max_age: Duration) -> String {
    format!("{}-0", now_ms.saturating_sub(max_age.as_millis() as u64))
}

/// Age in whole seconds of the entry with stream id `id` at `now_ms`
/// (0 for an id from the future, as after a clock step). `None` for an id
/// that is not `<ms>-<seq>`.
pub(crate) fn age_secs(id: &str, now_ms: u64) -> Option<u64> {
    let ms: u64 = id.split_once('-')?.0.parse().ok()?;
    Some(now_ms.saturating_sub(ms) / 1000)
}

/// `XTRIM <stream> MINID <min_id>` -- exact, not `~`, so no record older
/// than the limit survives (the stream holds at most 10,000 records, so an
/// exact trim is cheap). Returns how many were removed; 0 for a stream
/// that does not exist. `XTRIM` is not `denyoom`, so it runs at maxmemory.
pub(crate) async fn trim_older_than<C: redis::aio::ConnectionLike + Send>(
    conn: &mut C,
    stream: &str,
    min_id: &str,
) -> anyhow::Result<u64> {
    Ok(redis::cmd("XTRIM")
        .arg(stream)
        .arg("MINID")
        .arg(min_id)
        .query_async(conn)
        .await?)
}

/// The id of the stream's oldest entry, `None` when it is empty or does
/// not exist. `XRANGE - + COUNT 1` also returns that entry's fields; only
/// the id is kept.
pub(crate) async fn oldest_id<C: redis::aio::ConnectionLike + Send>(
    conn: &mut C,
    stream: &str,
) -> anyhow::Result<Option<String>> {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(stream)
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg(1)
        .query_async(conn)
        .await?;
    Ok(reply.ids.into_iter().next().map(|entry| entry.id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cutoff_is_max_age_before_now() {
        assert_eq!(
            min_id(1_790_000_000_000, Duration::from_secs(86_400)),
            "1789913600000-0"
        );
    }

    #[test]
    fn a_cutoff_before_the_epoch_saturates_at_zero() {
        assert_eq!(min_id(1_000, Duration::from_secs(86_400)), "0-0");
    }

    #[test]
    fn age_is_read_from_the_id_millisecond_part() {
        assert_eq!(age_secs("1790000000000-3", 1_790_000_072_500), Some(72));
        assert_eq!(age_secs("1790000000000-0", 1_789_999_999_000), Some(0));
        assert_eq!(age_secs("garbage", 1), None);
    }

    #[test]
    fn the_hard_maximum_is_one_day() {
        assert_eq!(MAX_DEADLETTER_AGE_SECS, 86_400);
        const { assert!(MIN_DEADLETTER_AGE_SECS < MAX_DEADLETTER_AGE_SECS) };
    }

    /// `#[ignore]`d: against a real Redis/valkey (`REDIS_URL`, default the
    /// local one), on uniquely named streams.
    mod redis_tests {
        use super::super::*;

        fn redis_url() -> String {
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
        }

        async fn conn() -> common::redis_conn::RedisConn {
            let client = redis::Client::open(redis_url()).unwrap();
            common::redis_conn::connect(&client).await.unwrap()
        }

        fn unique_stream(name: &str) -> String {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("relay-dl-test-{name}-{nanos}")
        }

        async fn xadd_at(conn: &mut common::redis_conn::RedisConn, stream: &str, id: &str) {
            let _: String = redis::cmd("XADD")
                .arg(stream)
                .arg(id)
                .arg("payload")
                .arg("x")
                .query_async(conn)
                .await
                .unwrap();
        }

        /// Records older than the limit go; the rest, and the stream,
        /// stay. An explicit id stands in for "added N hours ago".
        #[tokio::test]
        #[ignore = "requires a live Redis/valkey at REDIS_URL"]
        async fn only_records_older_than_the_limit_are_trimmed() {
            let stream = unique_stream("trim");
            let mut conn = conn().await;
            let now_ms: u64 = 1_790_000_000_000;
            let hour = 3_600_000;
            for hours_ago in [30, 25, 23, 1] {
                xadd_at(
                    &mut conn,
                    &stream,
                    &format!("{}-0", now_ms - hours_ago * hour),
                )
                .await;
            }

            let cutoff = min_id(now_ms, Duration::from_secs(MAX_DEADLETTER_AGE_SECS));
            let trimmed = trim_older_than(&mut conn, &stream, &cutoff).await.unwrap();

            assert_eq!(trimmed, 2);
            let oldest = oldest_id(&mut conn, &stream).await.unwrap().unwrap();
            assert_eq!(oldest, format!("{}-0", now_ms - 23 * hour));
            assert_eq!(age_secs(&oldest, now_ms), Some(23 * 3600));
            // Idempotent: nothing more to trim.
            assert_eq!(
                trim_older_than(&mut conn, &stream, &cutoff).await.unwrap(),
                0
            );
            let _: i64 = redis::cmd("DEL")
                .arg(&stream)
                .query_async(&mut conn)
                .await
                .unwrap();
        }

        /// A dead-letter stream that was never written (the normal state)
        /// trims nothing and has no oldest record; neither call creates it.
        #[tokio::test]
        #[ignore = "requires a live Redis/valkey at REDIS_URL"]
        async fn a_missing_stream_trims_nothing_and_is_not_created() {
            let stream = unique_stream("missing");
            let mut conn = conn().await;

            assert_eq!(trim_older_than(&mut conn, &stream, "1-0").await.unwrap(), 0);
            assert_eq!(oldest_id(&mut conn, &stream).await.unwrap(), None);
            let exists: bool = redis::cmd("EXISTS")
                .arg(&stream)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert!(!exists);
        }
    }
}
