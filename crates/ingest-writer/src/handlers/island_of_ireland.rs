//! The island-of-Ireland schemas (streams `ds:ingest:ioi-gtfs` and
//! `ds:ingest:ioi-nir`: stations and lines; `ds:ingest:ioi-live`: samples;
//! plan 3c.1, decision D8; the pollers are disabled by default):
//!
//! | Schema | Body (today's api route body) | Writer |
//! |---|---|---|
//! | `ioi-stations/1` | `Vec<IslandOfIrelandStation>` | `upsert_stations_observed` |
//! | `ioi-lines/1` | `Vec<IslandOfIrelandLineDefinition>` | `upsert_lines_observed` |
//! | `ioi-station-samples/1` | `Vec<IslandOfIrelandStationSample>` | `upsert_station_samples_observed` |
//!
//! all in `ds_store::samples::island_of_ireland`. Observed times (D13):
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
//! of the other network is poison ([`NETWORK_NOT_FOR_STREAM`]), checked in
//! shadow and apply before anything is written; and the upsert never moves
//! an existing row to the other network (`ds_store`'s guard), so a poller
//! cannot take over the other network's row by its id either.

use common::island_of_ireland::{
    IslandOfIrelandLineDefinition, IslandOfIrelandNetwork, IslandOfIrelandStation,
    IslandOfIrelandStationSample,
};
use ds_store::samples::island_of_ireland as store;
use ingest_stream::{HandlerError, StreamEntry, streams};
use sqlx::PgConnection;

use super::{
    Applied, BoxFuture, SchemaHandler, classify_anyhow, count_apply, count_shadow, decode,
};
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
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        let stations: Vec<IslandOfIrelandStation> = decode(entry)?;
        pin_stations(entry, &stations)?;
        count_shadow(entry, stations.len());
        Ok(())
    }

    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let stations: Vec<IslandOfIrelandStation> = decode(entry)?;
            pin_stations(entry, &stations)?;
            let written = store::upsert_stations_observed(conn, &stations, observed.produced_at())
                .await
                .map_err(|err| classify_anyhow(&err))?;
            count_apply(entry, stations.len(), written);
            Ok(Applied::All)
        })
    }
}

/// `ioi-lines/1`.
pub struct Lines;

impl SchemaHandler for Lines {
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        let lines: Vec<IslandOfIrelandLineDefinition> = decode(entry)?;
        pin_lines(entry, &lines)?;
        count_shadow(entry, lines.len());
        Ok(())
    }

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

/// `ioi-station-samples/1`.
pub struct StationSamples;

impl SchemaHandler for StationSamples {
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        let samples: Vec<IslandOfIrelandStationSample> = decode(entry)?;
        count_shadow(entry, samples.len());
        Ok(())
    }

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
            count_apply(entry, samples.len(), written);
            Ok(Applied::All)
        })
    }
}

#[cfg(test)]
mod tests {
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
        }
    }

    fn line(id: &str, network: IslandOfIrelandNetwork) -> IslandOfIrelandLineDefinition {
        IslandOfIrelandLineDefinition {
            id: id.into(),
            name: id.into(),
            network,
            stations: vec![],
        }
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
            assert!(Stations.check(&ok).is_ok(), "{stream}");
            let mixed = entry(
                stream,
                "ioi-stations",
                &[station("a", own), station("b", other)],
            );
            assert!(poisoned(Stations.check(&mixed)), "{stream}");
            let ok = entry(stream, "ioi-lines", &[line("l", own)]);
            assert!(Lines.check(&ok).is_ok(), "{stream}");
            let foreign = entry(stream, "ioi-lines", &[line("l", other)]);
            assert!(poisoned(Lines.check(&foreign)), "{stream}");
        }
        // A catalogue entry on any other stream is poison too.
        let elsewhere = entry(streams::IOI_LIVE, "ioi-stations", &[station("a", Roi)]);
        assert!(poisoned(Stations.check(&elsewhere)));
    }
}
