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
//! **Lookback** (2026-09-27, windowed stats design section 4.3.1): the
//! replay starts [`LOOKBACK`] before the rail day, and from that segment
//! applies only Activations for the day being rebuilt. TRUST activates a
//! train about an hour before it departs (up to ~8.5 h measured), so the
//! day's first trains are activated before it starts, and a replay from
//! the day start alone could never match them. A lookback segment already
//! trimmed from the stream is not what makes a day partial: the day start
//! is.
//!
//! With `MOVEMENT_STREAM_MAXLEN` at 1,048,576 (about 24 h of traffic) a
//! whole rail day is normally retained; replaying ~1M entries takes about
//! 4 minutes at the measured ~4.5k entries/s. When the day's first entries
//! HAVE been trimmed, the day is marked partial instead: see [`crate::day::DayState::partial_reason`].

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use movement_feed::redis_stream::{
    RangePage, RedisStreamMovementFeed, StreamPositions, stream_id_less_than,
};

use crate::day::{DayState, Lookups, PartialReason};
use crate::population_reload::SharedPopulation;

/// Entries per `XRANGE` round trip. Bounded so the replay never holds more
/// than one page of payloads at a time (~1 MB at 1000 entries).
pub const REPLAY_PAGE_SIZE: usize = 1000;

/// How far before the rail-day start the replay begins -- see this
/// module's doc. Measured: an Activation precedes the train's first call on
/// a line by at most 247 min at p99 and 513 min at the maximum.
pub const LOOKBACK: chrono::Duration = chrono::Duration::hours(6);

/// What a startup replay needs from a stream. Implemented by the real Redis
/// reader and by `ActiveFeed` (which delegates to it).
#[async_trait]
pub trait ReplaySource: Send {
    async fn positions(&mut self) -> anyhow::Result<StreamPositions>;
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
    async fn positions(&mut self) -> anyhow::Result<StreamPositions> {
        self.stream_positions().await
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
impl ReplaySource for movement_feed::ActiveFeed {
    async fn positions(&mut self) -> anyhow::Result<StreamPositions> {
        ReplaySource::positions(self.redis_stream()).await
    }
    async fn pending_ids(&mut self) -> anyhow::Result<HashSet<String>> {
        ReplaySource::pending_ids(self.redis_stream()).await
    }
    async fn read_range(
        &mut self,
        start: &str,
        end: &str,
        count: usize,
    ) -> anyhow::Result<RangePage> {
        ReplaySource::read_range(self.redis_stream(), start, end, count).await
    }
}

/// What to replay, decided purely from a [`StreamPositions`] snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPlan {
    /// `<lookback start ms>-0`, inclusive -- where the replay begins.
    pub start_id: String,
    /// `<rail day start ms>-0`: entries before it are the lookback segment.
    pub day_start_id: String,
    /// The group's `last-delivered-id`, inclusive; `None` when the group
    /// has not yet been handed anything from this rail day (nothing to
    /// replay -- live consumption covers the whole day).
    pub end_id: Option<String>,
    /// Some of the day's entries are no longer in the stream.
    pub day_start_trimmed: bool,
}

pub fn plan_replay(
    positions: &StreamPositions,
    day_start_ms: i64,
    lookback_start_ms: i64,
) -> ReplayPlan {
    let start_id = format!("{}-0", lookback_start_ms.min(day_start_ms));
    let day_start_id = format!("{day_start_ms}-0");
    let end_id = positions
        .group_last_delivered_id
        .clone()
        .filter(|last| !stream_id_less_than(last, &start_id));
    let day_start_trimmed = day_start_trimmed(positions, &day_start_id);
    ReplayPlan {
        start_id,
        day_start_id,
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

/// The instant a stream entry id (`<ms>-<seq>`) was generated at.
pub fn stream_id_time(id: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let millis: i64 = id.split('-').next()?.parse().ok()?;
    chrono::DateTime::from_timestamp_millis(millis)
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
    /// Of `entries`, how many came from the lookback segment (Activations
    /// only).
    pub lookback_entries: u64,
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

    let positions = retry!("read stream positions", progress, source.positions());

    let plan = plan_replay(
        &positions,
        day_start.timestamp_millis(),
        (day_start - LOOKBACK).timestamp_millis(),
    );
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
    // Everything from the lookback start on is replayed -- unless the
    // stream has already lost some of it, in which case only from its
    // first retained entry.
    let lookback_start = day_start - LOOKBACK;
    day.observed_from = if day_start_trimmed(&positions, &plan.start_id) {
        positions
            .stream_first_entry_id
            .as_deref()
            .and_then(stream_id_time)
            .map_or_else(chrono::Utc::now, |first| first.max(lookback_start))
    } else {
        lookback_start
    };
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
            let lookback = stream_id_less_than(id, &plan.day_start_id);
            // The entry's own time: when the relay received it.
            let received_at = stream_id_time(id).unwrap_or_else(chrono::Utc::now);
            let dispatched = if lookback {
                day.dispatch_lookback_payload(payload, lookups, received_at)
            } else {
                day.dispatch_payload(payload, lookups, &population, received_at)
            };
            if let Err(err) = dispatched {
                // Already dead-lettered (or about to be, if still pending)
                // by the group path when it was first delivered.
                tracing::debug!(error = ?err, %id, "skipping unparseable payload during replay");
                report.parse_errors += 1;
                continue;
            }
            report.entries += 1;
            if lookback {
                report.lookback_entries += 1;
            }
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
        let plan = plan_replay(&positions(Some("500-0"), "2000-3", 5000, 4000), 1000, 1000);
        assert_eq!(plan.start_id, "1000-0");
        assert_eq!(plan.end_id.as_deref(), Some("2000-3"));
        assert!(!plan.day_start_trimmed);
    }

    #[test]
    fn a_first_entry_after_the_day_start_with_trimming_means_partial() {
        let plan = plan_replay(&positions(Some("1500-0"), "2000-0", 5000, 1000), 1000, 1000);
        assert!(plan.day_start_trimmed);
        assert_eq!(
            plan.end_id.as_deref(),
            Some("2000-0"),
            "what is left is still replayed"
        );
    }

    /// The replay starts at the lookback, but "partial" still keys on the
    /// day start: a trimmed lookback costs nothing but early activations.
    #[test]
    fn the_replay_starts_at_the_lookback_but_partial_keys_on_the_day_start() {
        let plan = plan_replay(&positions(Some("700-0"), "2000-0", 5000, 4000), 1000, 400);
        assert_eq!(plan.start_id, "400-0");
        assert_eq!(plan.day_start_id, "1000-0");
        assert!(!plan.day_start_trimmed);
        let plan = plan_replay(&positions(Some("500-0"), "900-0", 5000, 4000), 1000, 400);
        assert_eq!(
            plan.end_id.as_deref(),
            Some("900-0"),
            "a group still inside the lookback has lookback to replay"
        );
    }

    struct FakeSource {
        positions: StreamPositions,
        entries: Vec<(String, String)>,
    }

    #[async_trait]
    impl ReplaySource for FakeSource {
        async fn positions(&mut self) -> anyhow::Result<StreamPositions> {
            Ok(self.positions.clone())
        }
        async fn pending_ids(&mut self) -> anyhow::Result<HashSet<String>> {
            Ok(HashSet::new())
        }
        async fn read_range(
            &mut self,
            start: &str,
            end: &str,
            count: usize,
        ) -> anyhow::Result<RangePage> {
            let (exclusive, start) = match start.strip_prefix('(') {
                Some(s) => (true, s),
                None => (false, start),
            };
            let entries: Vec<(String, String)> = self
                .entries
                .iter()
                .filter(|(id, _)| {
                    let after = if exclusive {
                        stream_id_less_than(start, id)
                    } else {
                        !stream_id_less_than(id, start)
                    };
                    after && !stream_id_less_than(end, id)
                })
                .take(count)
                .cloned()
                .collect();
            let last_id = entries.last().map(|(id, _)| id.clone());
            Ok(RangePage { entries, last_id })
        }
    }

    /// The lookback segment applies this day's Activations, and nothing
    /// else: a Movement from before the day start is not replayed.
    #[tokio::test]
    async fn the_lookback_segment_applies_activations_but_not_movements() {
        let service_date: chrono::NaiveDate = "2026-09-27".parse().unwrap();
        let day_start = crate::stats::rail_day_start(service_date).timestamp_millis();
        let before = day_start - 30 * 60 * 1000;
        let after = day_start + 30 * 60 * 1000;
        let activation = r#"{"header":{"msg_type":"0001"},"body":{"train_id":"722N71MW27","train_uid":"C11052","toc_id":"SW"}}"#;
        let movement = r#"{"header":{"msg_type":"0003"},"body":{"train_id":"722N71MW27","event_type":"DEPARTURE","loc_stanox":"87212","variation_status":"ON TIME"}}"#;
        let mut source = FakeSource {
            positions: StreamPositions {
                group_last_delivered_id: Some(format!("{after}-1")),
                stream_first_entry_id: Some(format!("{}-0", before - 1)),
                stream_length: 3,
                stream_entries_added: Some(3),
                ..StreamPositions::default()
            },
            entries: vec![
                (format!("{before}-0"), activation.to_string()),
                (format!("{before}-1"), movement.to_string()),
                (format!("{after}-0"), activation.to_string()),
            ],
        };
        let mut day = DayState::new(service_date);
        let population: SharedPopulation = std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(
            crate::population::Population::default(),
        ));
        let report = run_startup_replay(
            &mut source,
            &mut day,
            &Lookups::default(),
            &population,
            &health_http::Progress::new(Duration::from_secs(60)),
            2,
        )
        .await;
        assert_eq!(report.entries, 3);
        assert_eq!(report.lookback_entries, 2);
        assert_eq!(day.partial_reason, None);
        assert_eq!(
            day.observed_from,
            crate::stats::rail_day_start(service_date) - LOOKBACK,
            "nothing lost: observed from the lookback start"
        );
        assert_eq!(
            day.correlation
                .pending_activations
                .get("722N71MW27")
                .map(String::as_str),
            Some("C11052")
        );
    }

    /// A trimmed lookback (the day start still retained): observed only
    /// from the first retained entry, and the day is not partial.
    #[tokio::test]
    async fn a_trimmed_lookback_sets_observed_from_to_the_first_entry() {
        let service_date: chrono::NaiveDate = "2026-09-27".parse().unwrap();
        let day_start = crate::stats::rail_day_start(service_date).timestamp_millis();
        let first = day_start - 60 * 60 * 1000;
        let mut source = FakeSource {
            positions: StreamPositions {
                group_last_delivered_id: Some(format!("{}-0", day_start + 1000)),
                stream_first_entry_id: Some(format!("{first}-0")),
                stream_length: 10,
                stream_entries_added: Some(500),
                ..StreamPositions::default()
            },
            entries: vec![],
        };
        let mut day = DayState::new(service_date);
        let population: SharedPopulation = std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(
            crate::population::Population::default(),
        ));
        run_startup_replay(
            &mut source,
            &mut day,
            &Lookups::default(),
            &population,
            &health_http::Progress::new(Duration::from_secs(60)),
            10,
        )
        .await;
        assert_eq!(day.partial_reason, None);
        assert_eq!(
            day.observed_from,
            stream_id_time(&format!("{first}-0")).unwrap()
        );
    }

    #[test]
    fn a_stream_that_never_lost_anything_is_not_partial_even_if_it_starts_late() {
        let plan = plan_replay(&positions(Some("1500-0"), "2000-0", 10, 10), 1000, 1000);
        assert!(!plan.day_start_trimmed);
    }

    #[test]
    fn a_group_that_has_read_nothing_from_today_replays_nothing() {
        let plan = plan_replay(&positions(Some("500-0"), "999-9", 5000, 4000), 1000, 1000);
        assert_eq!(plan.end_id, None);
        assert!(!plan.day_start_trimmed);
    }

    #[test]
    fn an_xdel_inside_the_day_means_partial() {
        let mut p = positions(Some("500-0"), "2000-0", 5000, 4000);
        p.stream_max_deleted_entry_id = Some("1200-0".to_string());
        assert!(plan_replay(&p, 1000, 1000).day_start_trimmed);
    }

    #[test]
    fn unknown_removal_counters_are_treated_conservatively() {
        let mut p = positions(Some("1500-0"), "2000-0", 0, 10);
        p.stream_entries_added = None;
        assert!(plan_replay(&p, 1000, 1000).day_start_trimmed);
        p.stream_first_entry_id = Some("900-0".to_string());
        assert!(!plan_replay(&p, 1000, 1000).day_start_trimmed);
    }
}
