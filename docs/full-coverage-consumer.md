# `full-coverage-consumer`: restarts, partial days and metrics

`full-coverage-consumer` correlates every TRUST movement against the full
scheduled population of each shadow line and writes one
`full_coverage_line_stats` row per line per rail day. The aggregator merges
those rows into a line's status only for full-coverage-enabled lines: a line
whose TOML sets `full_coverage_enabled = true`, or every line when
`FULL_COVERAGE_ENABLED_DEFAULT` is true (binary default `false`; the chart's
`aggregator.fullCoverageEnabledDefault` and `api.fullCoverageEnabledDefault`
default to `true`). Otherwise its output is shadow-only.

Code: `crates/full-coverage-consumer/src/{main,replay,population_reload,day,stats}.rs`.

## What happens on start

The consumer does not read from its consumer group until it can correlate
correctly. It starts in three steps:

1. **Load the stanox/crs crosswalk.** A failed load is retried from 1 s,
   doubling up to 30 s. The shadow line set is derived from the crosswalk.
2. **Wait for the first population load.** The population is loaded in a
   background task. A failed cycle is retried from 1 s, doubling up to
   `failed_reload_retry_delay(POPULATION_RELOAD_SECS)` (15 s at the default
   300 s). Consumption starts only after a cycle in which every line's
   population for the current rail day loaded. If `api` refuses every request,
   the consumer waits indefinitely.
   - If some lines still fail after `POPULATION_INITIAL_WAIT_SECS` (default
     600) while other lines load, consumption starts anyway. The failing lines
     are then partial for that rail day (see below).
3. **Replay the current rail day.** The consumer runs a group-less `XRANGE` on
   `movement-events`, from the start of the rail day (02:00 Europe/London)
   up to the group's `last-delivered-id`. This rebuilds the day's in-memory
   state. Entries still in the group's pending list are skipped, because the
   group delivers those again next. When the replay finishes, the consumer
   switches to normal group reads.

Before this change, every restart lost the day's state. Batches are
acknowledged as soon as they are in memory, so the group never delivers them
again. Every train seen before the restart then counted as cancelled. About
200 restarts on rail day 2026-09-26 left that day's final row covering about
20 minutes.

**Timing and memory.** A replay of 1,000,001 synthetic entries (a full day)
took 8.6 s. Measured on 2026-09-27 with a production-sized population (243
lines; about 171k uid memberships per date, the largest line having 3,357
entries, 21 MB of JSON), the startup peak RSS was **381,492 KB (about
373 MiB)**:

- The population phase peaks at about 190 MB.
- The replay then builds the full day's state.

For comparison, the previous binary reached 304,984 KB (about 298 MiB) after
consuming the same day through the group.

`/healthz` keeps beating throughout startup. A long wait for `api` is
therefore not restarted by the liveness probe; `full_coverage_consumer_startup_complete`
reports it instead.

## Partial days

A row with `partial = true` is not clean signal. The consumer did not see
every event of that line's rail day, so:

- scheduled trains with no observed event are **left out** of the row, not
  counted as cancelled;
- the row never reads `availability = 'available'`, even after the day closes.

A day is partial when the process started during that day and could not replay
the day's start, or when a line's population was still missing as consumption
began.

| Reason | When |
| --- | --- |
| `day_start_trimmed` | The day's first entries are no longer in `movement-events`. The stream's first retained entry is later than 02:00 local, and entries have been trimmed. A day whose first event simply came late looks the same, so this rule is conservative. An `XDEL` inside the day also counts. |
| `replay_unsupported` | The Kafka backend. It has no group-less read. |
| Missing population | That line only: its population still failed after `POPULATION_INITIAL_WAIT_SECS`. |

A day entered through the in-process rail-day rollover is never partial.

Rows written before 2026-09-27 were all marked `partial` by migration
`20260927050000`. They came from a consumer that rebuilt nothing on restart.

## `full_coverage_line_stats` history

The table is keyed by `(line_id, service_date)`, one row per line per rail
day, so earlier days can be audited. The aggregator deletes rows older than
`FULL_COVERAGE_LINE_STATS_RETENTION_DAYS` (default 90). To see one line's
history, run `cargo run -p api --bin compare_full_coverage -- --line-id <id>` or query the
table:

```sql
SELECT service_date, availability, partial, total, cancelled
FROM full_coverage_line_stats WHERE line_id = 'swr-waterloo-reading'
ORDER BY service_date DESC;
```

An upsert skips a row whose values have not changed. `updated_at` is
therefore the time the row last changed, not the time it was last posted.

## Windowed stats (2026-09-27)

Design: `docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md`
(read its "Decisions (2026-09-27)" section first). Everything below is **off
by default**; with the defaults the consumer and aggregator behave exactly as
before.

| Setting | Where | Default | Effect |
| --- | --- | --- | --- |
| `FULL_COVERAGE_WINDOWED_STATS` (`fullCoverageConsumer.windowedStats.enabled`) | consumer | `false` | `true`: per-train state, `recent`/`day_to_date` window POSTs every minute, `stats_version` 2 rows. |
| `FULL_COVERAGE_RECENT_WINDOW_MINUTES` / `FULL_COVERAGE_GRACE_MINUTES` | consumer | 60 / 10 | `recent` covers trains due in `(now-10-60, now-10]`. |
| `FULL_COVERAGE_ACTIVATIONS_MIN` / `FULL_COVERAGE_FEED_STALE_SECS` | consumer | 20 / 300 | Feed health: fewer Activations in the last hour, or a newest event older than this, marks the write `feed_stale` (no severity, no presumed cancellations). |
| `FULL_COVERAGE_WINDOW_MODE` (`aggregator.fullCoverageWindow.mode`) | aggregator | `off` | `shadow`: record a verdict per line in `full_coverage_window_verdicts`, change nothing. `enforce`: also escalate allow-listed lines. |
| `FULL_COVERAGE_WINDOW_ENFORCE_LINES` | aggregator | empty | Lines `enforce` may change (comma list, or `*`). Empty: nothing is enforced until lines are named. |
| `FULL_COVERAGE_WINDOW_MIN_ESCALATION_RANK` | aggregator | 4 | Only Severe Delays / Part Suspended are enforced. Minor Delays / Reduced Service are recorded as `would_escalate_to` with `below_min_rank`. |
| `FULL_COVERAGE_WINDOW_STATS_RETENTION_DAYS` | aggregator | 14 | Prunes both window tables, in every mode. |
| `full_coverage_delay_threshold_minutes` / `full_coverage_min_sample_size` / `full_coverage_min_affected` | `Defaults`, per line via `severity_overrides` | 3 / 6 / 3 | Delayed = 3+ minutes late at the train's first report on the line; a window needs 6 evaluable trains and a tier 3 affected trains. |

**Rollout.** Deploy `schedule-reference` and `api` first (the population then
carries `operator_atoc`/`train_status`, and the route and migrations exist),
then turn on the consumer flag, then `shadow` for at least 7 days including a
weekend, then `enforce` with about five pilot lines of different volumes.
Rollback at any stage: `FULL_COVERAGE_WINDOW_MODE=off`.

**Evidence.** `compare_full_coverage --windows --all-lines --days 7 [--csv DIR]`
reports bucket health, the would-escalate log (enforced tier vs lower tiers),
the comparison with LDBWS, the closed-day audit rows, the aggregator's own
verdicts, and per-line volume with suggested pilot lines.

**Expect more "delayed" trains than the design measured.** It measured at 5
minutes; the threshold is 3.

**Version skew.** An older `api` answers the window POST with 404: counted in
`full_coverage_consumer_errors_total{operation="post_window_stats"}` and
logged at most every 10 minutes. A population from an older
`schedule-reference` reads `relevance = 'stops_only'`: no presumed
cancellations for that line and date.

**Metrics** (windowed): `full_coverage_consumer_window_rows_posted_total`,
`full_coverage_consumer_window_feed_stale` (gauge),
`full_coverage_consumer_window_presumed_cancelled` and
`full_coverage_consumer_pending_trains` (gauges over the last write's
`recent` windows), `full_coverage_consumer_parked_messages` (0002/0005/0006
waiting for their Activation), `full_coverage_consumer_unattributed_total{msg_type}`
(given up at the rollover); aggregator
`aggregator_full_coverage_window_verdicts_total{verdict}`,
`aggregator_full_coverage_window_escalations_total{severity,mode,below_min_rank}`,
`aggregator_full_coverage_window_stats_pruned_total`.

**Fixes in the legacy row (all modes).** A late train's delay now comes
from `timetable_variation` (`delayed` was always 0); a train cancelled before
it moved is counted cancelled; the next day's Activations survive the
rollover and a restart (the replay starts 6 h before the rail day and applies
only that day's Activations from the lookback); rail-replacement buses and
ships are left out of the population once `schedule-reference` publishes
`train_status`.

## Metrics

Every name has the usual `distant_signal_` prefix.

| Metric | Type | Meaning / what to alert on |
| --- | --- | --- |
| `full_coverage_consumer_startup_complete` | gauge 0/1 | 1 once the replay has finished and group reads have started. If it stays 0 for more than about 15 min, the consumer is stuck waiting for `api` (population or stanox/crs) or for Redis. |
| `full_coverage_consumer_population_loaded` | gauge 0/1 | 1 once the first population load has been accepted. |
| `full_coverage_consumer_population_last_success_timestamp_seconds` | gauge | Unix time of the last reload cycle in which every fetch succeeded. Alert when `time() -` this exceeds a few `POPULATION_RELOAD_SECS`. |
| `full_coverage_consumer_population_reload_duration_seconds` | histogram | Duration of one full reload cycle. This runs off the consume path. |
| `full_coverage_consumer_population_uids` | gauge | uid memberships held (today and tomorrow). Use it for memory sizing. |
| `full_coverage_consumer_startup_replay_seconds` | gauge | Duration of the last startup replay. |
| `full_coverage_consumer_startup_replay_entries_total` | counter | Stream entries read by startup replays. |
| `full_coverage_consumer_day_partial` | gauge 0/1 | 1 while the current rail day is partial as a whole. Alert or annotate: that day's stats are not clean signal. |
| `full_coverage_consumer_lines_partial` | gauge | Lines whose current-day row is partial (the whole day, or a missing population). |
| `full_coverage_consumer_stream_gap_detected_total` | counter | Incremented on every gap check (every `REDIS_GAP_CHECK_SECS`) that finds loss. Created at 0 on start, so a missing series means the target is not being scraped. It does not mean there was no gap. Alert on `increase(...[1h]) > 0`. |
| `full_coverage_consumer_errors_total{operation}` | counter | Includes `operation="startup_replay"`, a Redis error during the replay, which is retried. |
| `aggregator_full_coverage_line_stats_pruned_total` | counter | Rows removed by the aggregator's retention pass. |

**Gap check.** Implemented in `movement_feed::redis_stream::detect_gap`. A gap
is reported only when events were lost:

- Unread entries were trimmed. This needs `entries-added - entries-read >
  length` while the group's read position is behind the first retained entry.
  The old check also fired when the first retained entry was simply the
  group's next unread entry.
- An entry that was delivered but not yet acknowledged was trimmed. The group's
  pending list then starts before the first retained entry.

Redis's `entries-read` drifts by a few hundred across AOF reloads. The
position condition keeps that drift from raising an alarm. The same exact rule
now also applies to `trust_consumer_stream_gap_detected_total` and
`trust_backlog_consumer_stream_gap_detected_total`, since those consumers call
the same function.
