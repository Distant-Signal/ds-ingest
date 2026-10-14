//! The transit networks' schemas (transit spec §9; plan 1.6), one stream
//! per network (`ds:ingest:transit-<network>`, off by default):
//!
//! | Schema | Body | Writer |
//! |---|---|---|
//! | `transit-timetable/1` | [`TimetablePart`] | `ds_store::transit::stage_part` |
//! | `transit-timetable-commit/1` | [`TimetableCommit`] | `ds_store::transit::commit` |
//! | `transit-situations/1` | [`TransitSituations`] | `ds_store::transit::apply_situations` |
//!
//! The situations handler also writes the network's `line_status` rows
//! (`source = 'transit'`, ids `<network>:<line>`; spec §10.3), which the
//! writer's row policy admits (db-grants.yaml, plan 4.1).
//!
//! **Network pinning** (spec §9.1): each stream carries only its own
//! network ([`stream_network`]). An entry whose body names another network
//! is poison ([`NETWORK_NOT_FOR_STREAM`]) before anything is written, so a
//! compromised producer cannot write another network's rows. Row-level
//! security cannot tell networks apart (it works per role, and one writer
//! role applies every network).
//!
//! **Ids** are checked here too ([`check_part`]), so a bad one is a clear
//! poison reason rather than a CHECK violation.
//!
//! **Outcomes.** A part of a version already active (a re-sent unchanged
//! feed) writes nothing and counts its rows as skipped. A commit that
//! activates, is unchanged or is older is applied; one whose parts are
//! missing ([`INCOMPLETE_FEED_VERSION`]) or whose rows do not hang
//! together ([`INCONSISTENT_FEED_VERSION`]) is poison, and the active
//! version keeps serving until the next day's feed.

use common::transit::{
    MAX_FEED_VERSION_LEN, MAX_PICKUP_DROP_OFF, NETWORKS, NetworkId, TimetableCommit, TimetablePart,
    TimetableRows, TransitSituations, is_id, is_stop_id,
};
use ds_store::transit::{
    CommitOutcome, PartIndex, SituationsOutcome, StageOutcome, apply_situations, commit, stage_part,
};
use ingest_stream::{HandlerError, StreamEntry};
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, classify_anyhow, count_apply, decode};
use crate::observed::Observed;

/// The dead-letter reason prefix of an entry for another network than its
/// stream's.
pub const NETWORK_NOT_FOR_STREAM: &str = "network_not_for_stream";
/// The prefix of a commit whose staged parts differ from its manifest.
pub const INCOMPLETE_FEED_VERSION: &str = "incomplete_feed_version";
/// The prefix of a commit whose staged rows do not hang together.
pub const INCONSISTENT_FEED_VERSION: &str = "inconsistent_feed_version";
/// The prefix of a part with an id outside its grammar.
pub const INVALID_TRANSIT_ROW: &str = "invalid_transit_row";

/// The one network `stream` carries: `ds:ingest:transit-<network>` for a
/// network compiled into this build (`common::transit::NETWORKS`).
pub fn stream_network(stream: &str) -> Option<NetworkId> {
    let network = stream.strip_prefix("ds:ingest:transit-")?;
    NETWORKS
        .contains(&network)
        .then(|| NetworkId::parse(network).ok())
        .flatten()
}

/// Poison unless `network` is `entry`'s stream's.
fn pin_network(entry: &StreamEntry, network: &NetworkId) -> Result<(), HandlerError> {
    match stream_network(&entry.stream) {
        Some(expected) if &expected == network => Ok(()),
        Some(expected) => Err(HandlerError::Poison(format!(
            "{NETWORK_NOT_FOR_STREAM}: {} carries only {expected}; this entry is {network}",
            entry.stream
        ))),
        None => Err(HandlerError::Poison(format!(
            "{NETWORK_NOT_FOR_STREAM}: {} carries no transit network",
            entry.stream
        ))),
    }
}

fn invalid(what: &str, value: &str) -> HandlerError {
    HandlerError::Poison(format!("{INVALID_TRANSIT_ROW}: {what} {value:?}"))
}

fn check_feed_version(version: &str) -> Result<(), HandlerError> {
    if version.is_empty() || version.len() > MAX_FEED_VERSION_LEN {
        return Err(invalid("feed_version", version));
    }
    Ok(())
}

fn check_line(id: Option<&str>) -> Result<(), HandlerError> {
    match id {
        Some(id) if !is_id(id) => Err(invalid("line id", id)),
        _ => Ok(()),
    }
}

fn check_stop(id: &str) -> Result<(), HandlerError> {
    if is_stop_id(id) {
        Ok(())
    } else {
        Err(invalid("stop id", id))
    }
}

/// The ids of a part's rows follow their grammar (`common::transit`).
pub fn check_part(part: &TimetablePart) -> Result<(), HandlerError> {
    check_feed_version(&part.feed_version)?;
    match &part.rows {
        TimetableRows::Network(rows) if rows.len() != 1 => Err(HandlerError::Poison(format!(
            "{INVALID_TRANSIT_ROW}: a network part has {} rows, not 1",
            rows.len()
        ))),
        TimetableRows::Network(_) | TimetableRows::ServiceDays(_) => Ok(()),
        TimetableRows::Lines(rows) => rows.iter().try_for_each(|line| {
            check_line(Some(&line.line_id))?;
            line.directions
                .iter()
                .flat_map(|d| &d.stations)
                .try_for_each(|s| check_stop(s))
        }),
        TimetableRows::Stops(rows) => rows.iter().try_for_each(|stop| {
            check_stop(&stop.stop_id)?;
            stop.parent_stop_id.as_deref().map_or(Ok(()), check_stop)
        }),
        TimetableRows::Patterns(rows) => rows.iter().try_for_each(|pattern| {
            check_line(pattern.line_id.as_deref())?;
            pattern.stops.iter().try_for_each(|call| {
                check_stop(&call.stop_id)?;
                if call.pickup > MAX_PICKUP_DROP_OFF || call.drop_off > MAX_PICKUP_DROP_OFF {
                    return Err(invalid(
                        "pickup or drop-off in pattern",
                        &pattern.pattern_id,
                    ));
                }
                Ok(())
            })
        }),
        TimetableRows::Trips(rows) => rows
            .iter()
            .try_for_each(|trip| check_line(trip.line_id.as_deref())),
        TimetableRows::RailLinks(rows) => {
            rows.iter().try_for_each(|link| check_stop(&link.stop_id))
        }
    }
}

/// The part's number within its kind, from the envelope's batch part.
fn part_index(entry: &StreamEntry) -> Result<PartIndex, HandlerError> {
    let batch = entry.envelope.batch.as_ref().ok_or_else(|| {
        HandlerError::Poison(format!(
            "{INVALID_TRANSIT_ROW}: a transit-timetable part needs its batch part"
        ))
    })?;
    if batch.part == 0 || batch.part > batch.parts {
        return Err(HandlerError::Poison(format!(
            "{INVALID_TRANSIT_ROW}: part {} of {}",
            batch.part, batch.parts
        )));
    }
    Ok(PartIndex {
        part: batch.part,
        parts: batch.parts,
    })
}

/// `transit-timetable/1`.
pub struct Timetable;

impl SchemaHandler for Timetable {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let part: TimetablePart = decode(entry)?;
            pin_network(entry, &part.network)?;
            check_part(&part)?;
            let index = part_index(entry)?;
            let outcome = stage_part(conn, &part, index, observed.produced_at())
                .await
                .map_err(|err| classify_anyhow(&err))?;
            let rows = part.rows.len();
            let written = match outcome {
                StageOutcome::Staged { rows } => u64::try_from(rows).unwrap_or(u64::MAX),
                StageOutcome::NotStaging => 0,
            };
            count_apply(entry, rows, written);
            Ok(Applied::All)
        })
    }
}

/// `transit-timetable-commit/1`.
pub struct TimetableCommitHandler;

impl SchemaHandler for TimetableCommitHandler {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let manifest: TimetableCommit = decode(entry)?;
            pin_network(entry, &manifest.network)?;
            check_feed_version(&manifest.feed_version)?;
            let outcome = commit(conn, &manifest, observed.produced_at())
                .await
                .map_err(|err| classify_anyhow(&err))?;
            let network = manifest.network.as_str();
            let version = manifest.feed_version.as_str();
            match outcome {
                CommitOutcome::Activated => {
                    tracing::info!(
                        network,
                        feed_version = version,
                        "transit timetable activated"
                    );
                    count_apply(entry, 1, 1);
                }
                CommitOutcome::Unchanged | CommitOutcome::Older => {
                    tracing::info!(
                        network,
                        feed_version = version,
                        ?outcome,
                        "transit timetable commit changed nothing"
                    );
                    count_apply(entry, 1, 0);
                }
                CommitOutcome::Incomplete(why) => {
                    return Err(HandlerError::Poison(format!(
                        "{INCOMPLETE_FEED_VERSION}: {network} {version}: {why}"
                    )));
                }
                CommitOutcome::Inconsistent(why) => {
                    return Err(HandlerError::Poison(format!(
                        "{INCONSISTENT_FEED_VERSION}: {network} {version}: {why}"
                    )));
                }
            }
            Ok(Applied::All)
        })
    }
}

/// The longest `SituationNumber` the table accepts.
pub const MAX_SITUATION_ID_LEN: usize = 128;

/// A snapshot's ids follow their grammar: each situation's number, and every
/// line and stop id it and its consequences name.
pub fn check_situations(snapshot: &TransitSituations) -> Result<(), HandlerError> {
    snapshot.situations.iter().try_for_each(|situation| {
        let id = &situation.situation_id;
        if id.is_empty() || id.len() > MAX_SITUATION_ID_LEN {
            return Err(invalid("situation id", id));
        }
        let lines = situation
            .line_ids
            .iter()
            .chain(situation.consequences.iter().flat_map(|c| &c.line_ids));
        let stops = situation
            .stop_ids
            .iter()
            .chain(situation.consequences.iter().flat_map(|c| &c.stop_ids));
        lines.into_iter().try_for_each(|l| check_line(Some(l)))?;
        stops.into_iter().try_for_each(|s| check_stop(s))
    })
}

/// `transit-situations/1`: the network's whole situation set, and the line
/// statuses derived from it (spec §9.3, §10.3). An older snapshot than the
/// newest applied changes nothing and is counted as skipped.
pub struct Situations;

impl SchemaHandler for Situations {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let snapshot: TransitSituations = decode(entry)?;
            pin_network(entry, &snapshot.network)?;
            check_situations(&snapshot)?;
            let outcome = apply_situations(conn, &snapshot, observed.produced_at())
                .await
                .map_err(|err| classify_anyhow(&err))?;
            let rows = snapshot.situations.len();
            match outcome {
                SituationsOutcome::Applied(applied) => {
                    tracing::debug!(
                        network = snapshot.network.as_str(),
                        situations = applied.situations,
                        ended = applied.ended,
                        lines = applied.lines,
                        history = applied.history,
                        pruned = applied.pruned,
                        "applied transit situations"
                    );
                    count_apply(entry, rows, applied.situations);
                }
                SituationsOutcome::Older => {
                    tracing::info!(
                        network = snapshot.network.as_str(),
                        "transit situations older than the newest applied; nothing written"
                    );
                    count_apply(entry, rows, 0);
                }
            }
            Ok(Applied::All)
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use common::transit::{Direction, StopKind, TransitPattern, TransitPatternStop, TransitStop};
    use ingest_stream::{Envelope, SchemaId, streams};

    use super::*;

    fn part(network: &str, rows: TimetableRows) -> TimetablePart {
        TimetablePart {
            network: NetworkId::parse(network).unwrap(),
            feed_version: "v1".to_owned(),
            rows,
        }
    }

    fn entry(stream: &str, body: &TimetablePart) -> StreamEntry {
        StreamEntry {
            stream: stream.to_owned(),
            id: "1-0".to_owned(),
            envelope: Envelope::new(
                SchemaId::new("transit-timetable", 1).unwrap(),
                "test/pod",
                "transit-timetable:b/stops:1/1",
                Utc::now(),
                body,
            )
            .unwrap(),
        }
    }

    fn stop(id: &str) -> TransitStop {
        TransitStop {
            stop_id: id.to_owned(),
            kind: StopKind::Station,
            parent_stop_id: None,
            name: id.to_owned(),
            indicator: None,
            lat: None,
            lon: None,
            wheelchair: None,
        }
    }

    fn poisoned(result: Result<(), HandlerError>, reason: &str) -> bool {
        matches!(result, Err(HandlerError::Poison(got)) if got.starts_with(reason))
    }

    #[test]
    fn each_transit_stream_is_pinned_to_its_network() {
        assert_eq!(
            stream_network(streams::TRANSIT_METROLINK),
            Some(NetworkId::parse("metrolink").unwrap())
        );
        assert_eq!(stream_network("ds:ingest:transit-net"), None);
        assert_eq!(stream_network(streams::TFL), None);
        let metrolink = part("metrolink", TimetableRows::Stops(vec![stop("9400ZZMAPIC")]));
        let other = part("net", TimetableRows::Stops(vec![stop("9400ZZMAPIC")]));
        assert!(
            pin_network(
                &entry(streams::TRANSIT_METROLINK, &metrolink),
                &metrolink.network
            )
            .is_ok()
        );
        assert!(poisoned(
            pin_network(&entry(streams::TRANSIT_METROLINK, &other), &other.network),
            NETWORK_NOT_FOR_STREAM
        ));
        assert!(poisoned(
            pin_network(&entry(streams::TFL, &metrolink), &metrolink.network),
            NETWORK_NOT_FOR_STREAM
        ));
    }

    #[test]
    fn ids_outside_their_grammar_are_poison() {
        let ok = part(
            "metrolink",
            TimetableRows::Stops(vec![stop("9400ZZMAPIC1")]),
        );
        assert!(check_part(&ok).is_ok());
        let lower = part("metrolink", TimetableRows::Stops(vec![stop("9400zzmapic")]));
        assert!(poisoned(check_part(&lower), INVALID_TRANSIT_ROW));
        let pattern = |line: &str, pickup: u8| {
            part(
                "metrolink",
                TimetableRows::Patterns(vec![TransitPattern {
                    pattern_id: "p".to_owned(),
                    line_id: Some(line.to_owned()),
                    direction: Direction::Outbound,
                    headsign: None,
                    stops: vec![TransitPatternStop {
                        stop_id: "9400ZZMAPIC1".to_owned(),
                        arr_offset_s: 0,
                        dep_offset_s: 0,
                        pickup,
                        drop_off: 0,
                    }],
                }]),
            )
        };
        assert!(check_part(&pattern("blue", 0)).is_ok());
        assert!(poisoned(
            check_part(&pattern("Blue", 0)),
            INVALID_TRANSIT_ROW
        ));
        assert!(poisoned(
            check_part(&pattern("blue", 4)),
            INVALID_TRANSIT_ROW
        ));
        let mut long = ok.clone();
        long.feed_version = "v".repeat(MAX_FEED_VERSION_LEN + 1);
        assert!(poisoned(check_part(&long), INVALID_TRANSIT_ROW));
        let two_networks = part("metrolink", TimetableRows::Network(vec![]));
        assert!(poisoned(check_part(&two_networks), INVALID_TRANSIT_ROW));
    }

    #[test]
    fn a_part_needs_its_batch_part() {
        let body = part("metrolink", TimetableRows::Stops(vec![]));
        let bare = entry(streams::TRANSIT_METROLINK, &body);
        assert!(matches!(part_index(&bare), Err(HandlerError::Poison(_))));
        let mut numbered = bare.clone();
        numbered.envelope = numbered.envelope.with_batch(ingest_stream::BatchPart {
            batch: "b/stops".to_owned(),
            part: 2,
            parts: 3,
        });
        assert_eq!(
            part_index(&numbered).unwrap(),
            PartIndex { part: 2, parts: 3 }
        );
        numbered.envelope.batch.as_mut().unwrap().part = 4;
        assert!(matches!(
            part_index(&numbered),
            Err(HandlerError::Poison(_))
        ));
    }

    #[test]
    fn situation_ids_outside_their_grammar_are_poison() {
        let fixture: TransitSituations = serde_json::from_str(include_str!(
            "../../../common/tests/fixtures/transit/situations.json"
        ))
        .unwrap();
        assert!(check_situations(&fixture).is_ok());
        let mut long = fixture.clone();
        long.situations[0].situation_id = "x".repeat(MAX_SITUATION_ID_LEN + 1);
        assert!(poisoned(check_situations(&long), INVALID_TRANSIT_ROW));
        let mut line = fixture.clone();
        line.situations[1].consequences[0].line_ids = vec!["Blue".to_owned()];
        assert!(poisoned(check_situations(&line), INVALID_TRANSIT_ROW));
        let mut stop = fixture;
        stop.situations[0].stop_ids = vec!["9400zzmahea".to_owned()];
        assert!(poisoned(check_situations(&stop), INVALID_TRANSIT_ROW));
    }
}
