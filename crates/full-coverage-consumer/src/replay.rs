//! Startup replay: rebuilding the current rail day's in-memory state from
//! `movement-events` before consuming as the group again.
//!
//! **Why** (2026-09-27 full-coverage lag review): this consumer XACKs each
//! batch as soon as it is dispatched into in-memory state, so an ACKed
//! entry is never redelivered, and nothing used to restore that state on
//! start. Every restart (about 200 on rail day 2026-09-26: OOM kills,
//! rollouts, node reboots) therefore wiped the day's correlation so far,
//! and `stats::build_line_row` counted every train seen before the restart
//! as cancelled.
//!
//! **How**: a group-less `XRANGE` (nothing delivered, claimed or ACKed)
//! from the start of the current rail day (02:00 Europe/London, as a
//! stream id `<ms>-0`) up to and including the group's `last-delivered-id`
//! -- exactly the entries this group has already been handed. Entries still
//! in the group's pending-entries list are skipped: the group redelivers
//! them next (startup PEL replay / `XAUTOCLAIM`), and they must not be
//! applied twice. Then normal group consumption resumes from where the
//! group was.
//!
//! With `MOVEMENT_STREAM_MAXLEN` at 1,048,576 (about 24 h of traffic) a
//! whole rail day is normally retained; replaying ~1M entries takes about
//! 4 minutes at the measured ~4.5k entries/s. When the day's first entries
//! HAVE been trimmed (or the backend cannot replay at all -- Kafka), the
//! day is marked partial instead: see [`crate::day::DayState::partial_reason`].

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use movement_feed::MovementFeed;
use movement_feed::redis_stream::{
    RangePage, RedisStreamMovementFeed, StreamPositions, stream_id_less_than,
};

use crate::day::{DayState, Lookups, PartialReason};
use crate::population_reload::SharedPopulation;

/// Entries per `XRANGE` round trip. Bounded so the replay never holds more
/// than one page of payloads at a time (~1 MB at 1000 entries).
pub const REPLAY_PAGE_SIZE: usize = 1000;

/// What a startup replay needs from a stream. Implemented by the real Redis
/// reader and by `ActiveFeed` (which has nothing to offer under Kafka).
#[async_trait]
pub trait ReplaySource: Send {
    /// `None`: this backend cannot replay (there is no group-less read).
    async fn positions(&mut self) -> anyhow::Result<Option<StreamPositions>>;
    async fn pending_ids(&mut self) -> anyhow::Result<HashSet<String>>;
    async fn read_range(
        &mut self,
        start: &str,
        end: &str,
        count: usize,
    ) -> anyhow::Result<RangePage>;
}

#[async_trait]
impl ReplaySource for RedisStreamMovementFeed {
    async fn positions(&mut self) -> anyhow::Result<Option<StreamPositions>> {
        Ok(Some(self.stream_positions().await?))
    }
    async fn pending_ids(&mut self) -> anyhow::Result<HashSet<String>> {
        self.group_pending_ids().await
    }
    async fn read_range(
        &mut self,
        start: &str,
        end: &str,
        count: usize,
    ) -> anyhow::Result<RangePage> {
        RedisStreamMovementFeed::read_range(self, start, end, count).await
    }
}

#[async_trait]
impl<K: MovementFeed> ReplaySource for movement_feed::ActiveFeed<K> {
    async fn positions(&mut self) -> anyhow::Result<Option<StreamPositions>> {
        match self.redis_stream() {
            Some(feed) => ReplaySource::positions(feed).await,
            None => Ok(None),
        }
    }
    async fn pending_ids(&mut self) -> anyhow::Result<HashSet<String>> {
        match self.redis_stream() {
            Some(feed) => ReplaySource::pending_ids(feed).await,
            None => Ok(HashSet::new()),
        }
    }
    async fn read_range(
        &mut self,
        start: &str,
        end: &str,
        count: usize,
    ) -> anyhow::Result<RangePage> {
        match self.redis_stream() {
            Some(feed) => ReplaySource::read_range(feed, start, end, count).await,
            None => Ok(RangePage::default()),
        }
    }
}

/// What to replay, decided purely from a [`StreamPositions`] snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPlan {
    /// `<rail day start ms>-0`, inclusive.
    pub start_id: String,
    /// The group's `last-delivered-id`, inclusive; `None` when the group
    /// has not yet been handed anything from this rail day (nothing to
    /// replay -- live consumption covers the whole day).
    pub end_id: Option<String>,
    /// Some of the day's entries are no longer in the stream.
    pub day_start_trimmed: bool,
}

pub fn plan_replay(positions: &StreamPositions, day_start_ms: i64) -> ReplayPlan {
    let start_id = format!("{day_start_ms}-0");
    let end_id = positions
        .group_last_delivered_id
        .clone()
        .filter(|last| !stream_id_less_than(last, &start_id));
    let day_start_trimmed = day_start_trimmed(positions, &start_id);
    ReplayPlan {
        start_id,
        end_id,
        day_start_trimmed,
    }
}

/// Whether entries at or after `start_id` may have left the stream.
///
/// - An `XDEL` at or after the day start (`max-deleted-entry-id`) -- exact.
/// - Nothing ever removed (`entries-added == length`) -- exactly not.
/// - Otherwise trimming removed the oldest entries, all older than the
///   first retained one: if that is AFTER the day start, the day's first
///   entries are presumed among them. Conservative -- it cannot tell a day
///   whose first event simply came late (never the case for a live TRUST
///   feed, which is never quiet for long) -- and the cost of a false
///   "partial" is only a row that declines to claim completeness.
///
/// Redis does NOT advance `max-deleted-entry-id` for `MAXLEN` trimming
/// (checked against valkey 9), which is why the trim case needs the
/// first-entry comparison.
fn day_start_trimmed(positions: &StreamPositions, start_id: &str) -> bool {
    if positions
        .stream_max_deleted_entry_id
        .as_deref()
        .is_some_and(|max| max != "0-0" && !stream_id_less_than(max, start_id))
    {
        return true;
    }
    if positions.entries_removed() == Some(0) {
        return false;
    }
    match positions.stream_first_entry_id.as_deref() {
        Some(first) => stream_id_less_than(start_id, first),
        // Empty, yet something was removed: if anything was ever added on or
        // after the day start, it is gone.
        None => positions
            .stream_last_generated_id
            .as_deref()
            .is_some_and(|last| !stream_id_less_than(last, start_id)),
    }
}

/// Retries `$op` until it succeeds: 1 s, doubling to 30 s, beating
/// `$progress` (the process is alive; a restart would only repeat this).
/// A macro rather than a closure-taking fn because `$op` borrows the
/// source mutably on every attempt.
macro_rules! retry {
    ($what:expr, $progress:expr, $op:expr) => {{
        let mut backoff = Duration::from_secs(1);
        loop {
            match $op.await {
                Ok(value) => break value,
                Err(err) => {
                    tracing::error!(error = ?err, retry_in_secs = backoff.as_secs(), "startup replay: failed to {}; retrying", $what);
                    metrics::counter!(
                        common::metrics::metric_name("full_coverage_consumer_errors_total"),
                        "operation" => "startup_replay"
                    )
                    .increment(1);
                    $progress.beat();
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }};
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayReport {
    /// Stream entries dispatched into the day state.
    pub entries: u64,
    /// Entries skipped because the group will redeliver them.
    pub skipped_pending: u64,
    pub parse_errors: u64,
}

/// Replays the current rail day into `day` -- see this module's doc. Sets
/// `day.partial_reason` when the replay cannot be complete. Redis errors
/// are retried with backoff (the replay resumes where it stopped): nothing
/// can be consumed while Redis is unreachable anyway.
pub async fn run_startup_replay<S: ReplaySource + ?Sized>(
    source: &mut S,
    day: &mut DayState,
    lookups: &Lookups,
    population: &SharedPopulation,
    progress: &health_http::Progress,
    page_size: usize,
) -> ReplayReport {
    let mut report = ReplayReport::default();
    let day_start = crate::stats::rail_day_start(day.service_date);

    let Some(positions) = retry!("read stream positions", progress, source.positions()) else {
        tracing::warn!(
            service_date = %day.service_date,
            "this movement-feed backend cannot replay the rail day; marking it partial"
        );
        day.partial_reason = Some(PartialReason::ReplayUnsupported);
        return report;
    };

    let plan = plan_replay(&positions, day_start.timestamp_millis());
    if plan.day_start_trimmed {
        tracing::error!(
            service_date = %day.service_date,
            day_start = %plan.start_id,
            first_entry = ?positions.stream_first_entry_id,
            "the start of the current rail day has already been trimmed from movement-events; \
             replaying what is left and marking the day partial"
        );
        day.partial_reason = Some(PartialReason::DayStartTrimmed);
    }
    let Some(end_id) = plan.end_id else {
        tracing::info!(
            service_date = %day.service_date,
            last_delivered = ?positions.group_last_delivered_id,
            "the group has not yet read anything from this rail day; nothing to replay"
        );
        return report;
    };

    let pending = retry!(
        "list the group's pending entries",
        progress,
        source.pending_ids()
    );
    tracing::info!(
        service_date = %day.service_date,
        from = %plan.start_id,
        to = %end_id,
        pending = pending.len(),
        "replaying the current rail day from movement-events before consuming"
    );

    let mut start = plan.start_id.clone();
    loop {
        let page = retry!(
            "read a replay page",
            progress,
            source.read_range(&start, &end_id, page_size)
        );
        let Some(last_id) = page.last_id else {
            break;
        };
        let population = population.load();
        for (id, payload) in &page.entries {
            if pending.contains(id) {
                report.skipped_pending += 1;
                continue;
            }
            if let Err(err) = day.dispatch_payload(payload, lookups, &population) {
                // Already dead-lettered (or about to be, if still pending)
                // by the group path when it was first delivered.
                tracing::debug!(error = ?err, %id, "skipping unparseable payload during replay");
                report.parse_errors += 1;
                continue;
            }
            report.entries += 1;
        }
        metrics::counter!(common::metrics::metric_name(
            "full_coverage_consumer_startup_replay_entries_total"
        ))
        .increment(page.entries.len() as u64);
        progress.beat();
        if last_id == end_id {
            break;
        }
        start = format!("({last_id}");
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn positions(
        first: Option<&str>,
        last_delivered: &str,
        added: u64,
        length: u64,
    ) -> StreamPositions {
        StreamPositions {
            group_last_delivered_id: Some(last_delivered.to_string()),
            stream_first_entry_id: first.map(str::to_string),
            stream_length: length,
            stream_entries_added: Some(added),
            stream_last_generated_id: Some("9999-0".to_string()),
            stream_max_deleted_entry_id: Some("0-0".to_string()),
            ..StreamPositions::default()
        }
    }

    #[test]
    fn the_whole_day_retained_replays_to_the_group_position() {
        let plan = plan_replay(&positions(Some("500-0"), "2000-3", 5000, 4000), 1000);
        assert_eq!(plan.start_id, "1000-0");
        assert_eq!(plan.end_id.as_deref(), Some("2000-3"));
        assert!(!plan.day_start_trimmed);
    }

    #[test]
    fn a_first_entry_after_the_day_start_with_trimming_means_partial() {
        let plan = plan_replay(&positions(Some("1500-0"), "2000-0", 5000, 1000), 1000);
        assert!(plan.day_start_trimmed);
        assert_eq!(
            plan.end_id.as_deref(),
            Some("2000-0"),
            "what is left is still replayed"
        );
    }

    #[test]
    fn a_stream_that_never_lost_anything_is_not_partial_even_if_it_starts_late() {
        let plan = plan_replay(&positions(Some("1500-0"), "2000-0", 10, 10), 1000);
        assert!(!plan.day_start_trimmed);
    }

    #[test]
    fn a_group_that_has_read_nothing_from_today_replays_nothing() {
        let plan = plan_replay(&positions(Some("500-0"), "999-9", 5000, 4000), 1000);
        assert_eq!(plan.end_id, None);
        assert!(!plan.day_start_trimmed);
    }

    #[test]
    fn an_xdel_inside_the_day_means_partial() {
        let mut p = positions(Some("500-0"), "2000-0", 5000, 4000);
        p.stream_max_deleted_entry_id = Some("1200-0".to_string());
        assert!(plan_replay(&p, 1000).day_start_trimmed);
    }

    #[test]
    fn unknown_removal_counters_are_treated_conservatively() {
        let mut p = positions(Some("1500-0"), "2000-0", 0, 10);
        p.stream_entries_added = None;
        assert!(plan_replay(&p, 1000).day_start_trimmed);
        p.stream_first_entry_id = Some("900-0".to_string());
        assert!(!plan_replay(&p, 1000).day_start_trimmed);
    }
}
