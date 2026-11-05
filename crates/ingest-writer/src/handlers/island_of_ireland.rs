//! The island-of-Ireland schemas (streams `ds:ingest:ioi-gtfs` and
//! `ds:ingest:ioi-nir`: stations and lines; `ds:ingest:ioi-live`: samples;
//! plan 3c.1, decision D8; the pollers are disabled by default):
//!
//! | Schema | Body (today's api route body) | Writer |
//! |---|---|---|
//! | `ioi-stations/1` | `Vec<IslandOfIrelandStation>` | `upsert_stations_observed` |
//! | `ioi-lines/1` | `Vec<IslandOfIrelandLineDefinition>` | `upsert_lines_observed` |
//! | `ioi-station-samples/1` | `Vec<IslandOfIrelandStationSample>` | `upsert_station_samples_observed`, then `record_train_history` and `prune_train_history` |
//! | `ioi-live-stations/1` | `Vec<IslandOfIrelandLiveStation>` | `upsert_live_stations_observed` |
//!
//! all in `ds_store::samples::island_of_ireland`. After a Republic of
//! Ireland stations snapshot or a live station list, the same transaction
//! re-matches the catalogue to the live codes (`refresh_live_codes`,
//! [`refresh_live_codes`]), so `island_of_ireland_stations.live_code`
//! follows whichever side changed; the match is logged and exported as
//! `distant_signal_island_of_ireland_live_codes{outcome}`. A line's
//! `replaces` deletes the directional rows a merged corridor replaces
//! (in `upsert_lines_observed`). Observed times (D13):
//! stations and lines have none of their own, so a changed row's
//! `fetched_at` and the freshness marker are the envelope's `produced_at`;
//! a station sample's is its own `polled_at`, clamped to the writer's
//! `now() + 2 min` ([`Observed::observed_at`]). Each upsert guards on that
//! time, so an older snapshot never overwrites a newer one.
//!
//! Rows are counted as the snapshot handlers count them; an unchanged or
//! older row is `skipped`.
//!
//! **Network pinning** (security review, 2026-10-08): `poller-irish-rail-gtfs`
//! and `poller-nir-stations` produce the same two schemas on their own
//! streams, so each stream's catalogue must be its own network's:
//! `ds:ingest:ioi-gtfs` only `republic-of-ireland` rows, `ds:ingest:ioi-nir`
//! only `northern-ireland` ones ([`stream_network`]). An entry with any row
//! of the other network is poison ([`NETWORK_NOT_FOR_STREAM`]), checked
//! before anything is written; and the upsert never moves
//! an existing row to the other network (`ds_store`'s guard), so a poller
//! cannot take over the other network's row by its id either.

use common::island_of_ireland::live_codes::{LiveCodeOutcome, LiveCodeReport, Unmatched};
use common::island_of_ireland::{
    IslandOfIrelandLineDefinition, IslandOfIrelandLiveStation, IslandOfIrelandNetwork,
    IslandOfIrelandStation, IslandOfIrelandStationSample,
};
use ds_store::samples::island_of_ireland as store;
use ingest_stream::{HandlerError, StreamEntry, streams};
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, classify_anyhow, count_apply, decode};
use crate::observed::Observed;

/// The dead-letter reason prefix of an entry carrying a row of a network
/// its stream may not write.
pub const NETWORK_NOT_FOR_STREAM: &str = "network_not_for_stream";

/// The one network a catalogue stream's rows may carry: the network whose
/// feed its poller reads (`IslandOfIrelandNetwork`: "which feed do we
/// source this from").
pub fn stream_network(stream: &str) -> Option<IslandOfIrelandNetwork> {
    match stream {
        streams::IOI_GTFS => Some(IslandOfIrelandNetwork::RepublicOfIreland),
        streams::IOI_NIR => Some(IslandOfIrelandNetwork::NorthernIreland),
        _ => None,
    }
}

/// Poison unless every row's network is `entry`'s stream's
/// ([`stream_network`]).
fn pin_network<T>(
    entry: &StreamEntry,
    rows: &[T],
    network: impl Fn(&T) -> IslandOfIrelandNetwork,
    id: impl Fn(&T) -> &str,
) -> Result<IslandOfIrelandNetwork, HandlerError> {
    let Some(expected) = stream_network(&entry.stream) else {
        return Err(HandlerError::Poison(format!(
            "{NETWORK_NOT_FOR_STREAM}: {} carries no island-of-Ireland catalogue",
            entry.stream
        )));
    };
    if let Some(row) = rows.iter().find(|row| network(row) != expected) {
        return Err(HandlerError::Poison(format!(
            "{NETWORK_NOT_FOR_STREAM}: {} carries only {} rows; {:?} is {}",
            entry.stream,
            store::network_wire(expected),
            id(row),
            store::network_wire(network(row)),
        )));
    }
    Ok(expected)
}

fn pin_stations(
    entry: &StreamEntry,
    stations: &[IslandOfIrelandStation],
) -> Result<IslandOfIrelandNetwork, HandlerError> {
    pin_network(entry, stations, |s| s.network, |s| s.id.as_str())
}

fn pin_lines(
    entry: &StreamEntry,
    lines: &[IslandOfIrelandLineDefinition],
) -> Result<IslandOfIrelandNetwork, HandlerError> {
    pin_network(entry, lines, |l| l.network, |l| l.id.as_str())
}

/// `ioi-stations/1`.
pub struct Stations;

impl SchemaHandler for Stations {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let stations: Vec<IslandOfIrelandStation> = decode(entry)?;
            let network = pin_stations(entry, &stations)?;
            let written = store::upsert_stations_observed(conn, &stations, observed.produced_at())
                .await
                .map_err(|err| classify_anyhow(&err))?;
            if network == IslandOfIrelandNetwork::RepublicOfIreland {
                refresh_live_codes(conn).await?;
            }
            count_apply(entry, stations.len(), written);
            Ok(Applied::All)
        })
    }
}

/// `ioi-lines/1`.
pub struct Lines;

impl SchemaHandler for Lines {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let lines: Vec<IslandOfIrelandLineDefinition> = decode(entry)?;
            pin_lines(entry, &lines)?;
            let written = store::upsert_lines_observed(conn, &lines, observed.produced_at())
                .await
                .map_err(|err| classify_anyhow(&err))?;
            count_apply(entry, lines.len(), written);
            Ok(Applied::All)
        })
    }
}

/// [`StationSamples`]' default history window: 14 days, what the
/// aggregator keeps of an untracked GB train and its movements.
pub const DEFAULT_TRAIN_HISTORY_RETENTION_DAYS: u32 = 14;

/// Departures the board history left out (no `Traindate`, or a train code
/// the history refuses), by the counter's only label, `reason`.
pub const TRAIN_HISTORY_SKIPPED_METRIC: &str = "island_of_ireland_history_skipped_departures_total";

/// Departures whose `Status` is not one `train_state` knows. They are
/// stored as sent and read as unknown; a rise means the feed has a word we
/// should learn (design §6.2). No label: the text is the feed's.
pub const UNRECOGNISED_STATUS_METRIC: &str = "island_of_ireland_unrecognised_status_total";

/// History rows pruned past the retention window.
pub const TRAIN_HISTORY_PRUNED_METRIC: &str = "island_of_ireland_history_pruned_rows_total";

/// `ioi-station-samples/1`: the latest board per station, then, in the same
/// transaction, the board history by train (`record_train_history`) and
/// its prune: rows whose `train_date` is more than `history_retention_days`
/// before Dublin's today, at most `TRAIN_HISTORY_PRUNE_BATCH` per table per
/// entry (distant-signal's
/// docs/superpowers/specs/2026-11-05-irish-rail-train-tracking-design.md
/// §4). The prune runs where the history is written, so a writer whose
/// live poller is off neither writes nor prunes anything.
pub struct StationSamples {
    pub history_retention_days: u32,
}

impl SchemaHandler for StationSamples {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let mut samples: Vec<IslandOfIrelandStationSample> = decode(entry)?;
            for sample in &mut samples {
                sample.polled_at = observed.observed_at(Some(sample.polled_at));
            }
            let written = store::upsert_station_samples_observed(conn, &samples)
                .await
                .map_err(|err| classify_anyhow(&err))?;
            let history = store::record_train_history(conn, &samples)
                .await
                .map_err(|err| classify_anyhow(&err))?;
            // ds-core v0.4's prune takes a separate cutoff for tracked
            // trains; until the writer tracks Irish trains they share the
            // one window, as before.
            let cutoff = history_cutoff(chrono::Utc::now(), self.history_retention_days);
            let pruned =
                store::prune_train_history(conn, cutoff, cutoff, store::TRAIN_HISTORY_PRUNE_BATCH)
                    .await
                    .map_err(|err| classify_anyhow(&err))?;
            record_history(&samples, history, pruned);
            count_apply(entry, samples.len(), written);
            Ok(Applied::All)
        })
    }
}

/// The first `train_date` the history keeps: `retention_days` before
/// Dublin's date at `now` (Irish trains' service dates are Dublin's).
pub fn history_cutoff(
    now: chrono::DateTime<chrono::Utc>,
    retention_days: u32,
) -> chrono::NaiveDate {
    let today = now.with_timezone(&chrono_tz::Europe::Dublin).date_naive();
    today - chrono::Days::new(u64::from(retention_days))
}

/// The history's counters for one entry.
fn record_history(
    samples: &[IslandOfIrelandStationSample],
    history: store::TrainHistoryWrite,
    pruned: u64,
) {
    if history.skipped > 0 {
        metrics::counter!(
            ds_runtime::metrics::metric_name(TRAIN_HISTORY_SKIPPED_METRIC),
            "reason" => "no_train_date_or_bad_code"
        )
        .increment(history.skipped);
    }
    let unrecognised = samples
        .iter()
        .flat_map(|s| &s.departures)
        .filter(|d| !common::island_of_ireland::train_state::is_known_status(&d.status))
        .count();
    if unrecognised > 0 {
        metrics::counter!(ds_runtime::metrics::metric_name(UNRECOGNISED_STATUS_METRIC))
            .increment(u64::try_from(unrecognised).unwrap_or(u64::MAX));
    }
    if pruned > 0 {
        metrics::counter!(ds_runtime::metrics::metric_name(
            TRAIN_HISTORY_PRUNED_METRIC
        ))
        .increment(pruned);
    }
    tracing::debug!(
        trains = history.trains,
        calls = history.calls,
        skipped = history.skipped,
        pruned,
        "island-of-Ireland board history recorded"
    );
}

/// `ioi-live-stations/1`: Iarnród Éireann's live station list, from
/// `ds:ingest:ioi-live` only (the stream's schema list).
pub struct LiveStations;

impl SchemaHandler for LiveStations {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let stations: Vec<IslandOfIrelandLiveStation> = decode(entry)?;
            let written =
                store::upsert_live_stations_observed(conn, &stations, observed.produced_at())
                    .await
                    .map_err(|err| classify_anyhow(&err))?;
            refresh_live_codes(conn).await?;
            count_apply(entry, stations.len(), written);
            Ok(Applied::All)
        })
    }
}

/// The live-code metric: catalogue stations by match outcome.
pub const LIVE_CODES_METRIC: &str = "island_of_ireland_live_codes";

/// Re-matches the catalogue to the live codes in the caller's transaction
/// and reports the result: the gauge, and one log line naming every
/// station left unmatched (each needs an override in
/// `common::island_of_ireland::live_codes::OVERRIDES`, or has no board).
async fn refresh_live_codes(conn: &mut PgConnection) -> Result<(), HandlerError> {
    let (report, changed) = store::refresh_live_codes(conn)
        .await
        .map_err(|err| classify_anyhow(&err))?;
    if let Some(report) = report {
        record_live_codes(&report, changed);
    }
    Ok(())
}

/// The gauge and log line for one refresh.
fn record_live_codes(report: &LiveCodeReport, changed: u64) {
    for (outcome, count) in outcome_counts(report) {
        metrics::gauge!(
            ds_runtime::metrics::metric_name(LIVE_CODES_METRIC),
            "outcome" => outcome
        )
        .set(f64::from(u32::try_from(count).unwrap_or(u32::MAX)));
    }
    let unmatched = describe_unmatched(report);
    if unmatched.is_empty() {
        tracing::info!(
            changed,
            "island-of-Ireland live codes refreshed; every station matched"
        );
    } else {
        tracing::warn!(
            changed,
            unmatched = %unmatched.join(" "),
            "island-of-Ireland stations without a live code; add an override once confirmed"
        );
    }
}

/// Every outcome label with its station count (zero included, so a
/// fixed station drops its series to 0 rather than leaving it stale).
fn outcome_counts(report: &LiveCodeReport) -> Vec<(&'static str, usize)> {
    use common::island_of_ireland::live_codes::MatchMethod;
    let mut counts: Vec<(&'static str, usize)> = [
        MatchMethod::Override,
        MatchMethod::Name,
        MatchMethod::NamePrefix,
        MatchMethod::Coordinates,
    ]
    .iter()
    .map(|m| (m.label(), 0))
    .chain([("no_board", 0), ("unmatched", 0), ("ambiguous", 0)])
    .collect();
    for (_, outcome) in &report.outcomes {
        let label = match outcome {
            LiveCodeOutcome::Matched { method, .. } => method.label(),
            LiveCodeOutcome::NoBoard => "no_board",
            LiveCodeOutcome::Unmatched(Unmatched::Ambiguous(_)) => "ambiguous",
            LiveCodeOutcome::Unmatched(_) => "unmatched",
        };
        if let Some(entry) = counts.iter_mut().find(|(l, _)| *l == label) {
            entry.1 += 1;
        }
    }
    counts
}

/// `id(why)` for each unmatched station, sorted.
fn describe_unmatched(report: &LiveCodeReport) -> Vec<String> {
    let mut out: Vec<String> = report
        .unmatched()
        .map(|(id, why)| match why {
            Unmatched::NoCandidate => format!("{id}(no-candidate)"),
            Unmatched::Ambiguous(codes) => format!("{id}(ambiguous:{})", codes.join("|")),
            Unmatched::OverrideCodeMissing(code) => format!("{id}(override-code-missing:{code})"),
        })
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {

    /// Dublin's date, not UTC's: at 23:30 UTC in summer it is already
    /// tomorrow in Dublin.
    #[test]
    fn the_history_cutoff_counts_back_from_dublins_date() {
        let summer_night: chrono::DateTime<Utc> = "2026-07-01T23:30:00Z".parse().unwrap();
        assert_eq!(
            history_cutoff(summer_night, 14),
            chrono::NaiveDate::from_ymd_opt(2026, 6, 18).unwrap()
        );
        let winter_night: chrono::DateTime<Utc> = "2026-12-01T23:30:00Z".parse().unwrap();
        assert_eq!(
            history_cutoff(winter_night, 14),
            chrono::NaiveDate::from_ymd_opt(2026, 11, 17).unwrap()
        );
    }

    use chrono::Utc;
    use ingest_stream::{Envelope, SchemaId};

    use super::*;

    fn station(id: &str, network: IslandOfIrelandNetwork) -> IslandOfIrelandStation {
        IslandOfIrelandStation {
            id: id.into(),
            name: id.into(),
            network,
            latitude: None,
            longitude: None,
            raw_name: None,
        }
    }

    fn line(id: &str, network: IslandOfIrelandNetwork) -> IslandOfIrelandLineDefinition {
        IslandOfIrelandLineDefinition {
            id: id.into(),
            name: id.into(),
            network,
            stations: vec![],
            short_name: None,
            agency: None,
            color: None,
            text_color: None,
            replaces: vec![],
        }
    }

    #[test]
    fn the_live_code_report_counts_every_outcome_and_names_the_unmatched() {
        use common::island_of_ireland::live_codes::MatchMethod;
        let report = LiveCodeReport {
            outcomes: vec![
                (
                    "A".into(),
                    LiveCodeOutcome::Matched {
                        code: "AAA".into(),
                        method: MatchMethod::Name,
                    },
                ),
                (
                    "B".into(),
                    LiveCodeOutcome::Unmatched(Unmatched::NoCandidate),
                ),
                (
                    "C".into(),
                    LiveCodeOutcome::Unmatched(Unmatched::Ambiguous(vec!["X".into(), "Y".into()])),
                ),
                ("D".into(), LiveCodeOutcome::NoBoard),
            ],
        };
        let counts = outcome_counts(&report);
        let count = |label: &str| counts.iter().find(|(l, _)| *l == label).unwrap().1;
        assert_eq!(count("name"), 1);
        assert_eq!(count("override"), 0);
        assert_eq!(count("unmatched"), 1);
        assert_eq!(count("ambiguous"), 1);
        assert_eq!(count("no_board"), 1);
        assert_eq!(
            describe_unmatched(&report),
            ["B(no-candidate)", "C(ambiguous:X|Y)"]
        );
    }

    fn entry<T: serde::Serialize>(stream: &str, schema: &str, body: &T) -> StreamEntry {
        StreamEntry {
            stream: stream.to_owned(),
            id: "1-0".to_owned(),
            envelope: Envelope::new(
                SchemaId::new(schema, 1).unwrap(),
                "test/pod",
                format!("{schema}:b:1/1"),
                Utc::now(),
                body,
            )
            .unwrap(),
        }
    }

    /// What `apply` checks before it writes: the payload decodes and its
    /// rows are of the stream's network.
    fn check_stations(entry: &StreamEntry) -> Result<(), HandlerError> {
        pin_stations(entry, &decode::<Vec<IslandOfIrelandStation>>(entry)?).map(drop)
    }

    fn check_lines(entry: &StreamEntry) -> Result<(), HandlerError> {
        pin_lines(entry, &decode::<Vec<IslandOfIrelandLineDefinition>>(entry)?).map(drop)
    }

    fn poisoned(result: Result<(), HandlerError>) -> bool {
        matches!(result, Err(HandlerError::Poison(reason)) if reason.starts_with(NETWORK_NOT_FOR_STREAM))
    }

    #[test]
    fn each_catalogue_stream_is_pinned_to_its_network() {
        use IslandOfIrelandNetwork::{NorthernIreland as Ni, RepublicOfIreland as Roi};
        assert_eq!(stream_network(streams::IOI_GTFS), Some(Roi));
        assert_eq!(stream_network(streams::IOI_NIR), Some(Ni));
        assert_eq!(stream_network(streams::IOI_LIVE), None);
        for (stream, own, other) in [(streams::IOI_GTFS, Roi, Ni), (streams::IOI_NIR, Ni, Roi)] {
            let ok = entry(stream, "ioi-stations", &[station("a", own)]);
            assert!(check_stations(&ok).is_ok(), "{stream}");
            let mixed = entry(
                stream,
                "ioi-stations",
                &[station("a", own), station("b", other)],
            );
            assert!(poisoned(check_stations(&mixed)), "{stream}");
            let ok = entry(stream, "ioi-lines", &[line("l", own)]);
            assert!(check_lines(&ok).is_ok(), "{stream}");
            let foreign = entry(stream, "ioi-lines", &[line("l", other)]);
            assert!(poisoned(check_lines(&foreign)), "{stream}");
        }
        // A catalogue entry on any other stream is poison too.
        let elsewhere = entry(streams::IOI_LIVE, "ioi-stations", &[station("a", Roi)]);
        assert!(poisoned(check_stations(&elsewhere)));
    }
}
