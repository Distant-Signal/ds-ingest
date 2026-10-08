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

use common::island_of_ireland::{
    IslandOfIrelandLineDefinition, IslandOfIrelandStation, IslandOfIrelandStationSample,
};
use ds_store::samples::island_of_ireland as store;
use ingest_stream::{HandlerError, StreamEntry};
use sqlx::PgConnection;

use super::{
    Applied, BoxFuture, SchemaHandler, classify_anyhow, count_apply, count_shadow, decode,
};
use crate::observed::Observed;

/// `ioi-stations/1`.
pub struct Stations;

impl SchemaHandler for Stations {
    fn check(&self, entry: &StreamEntry) -> Result<(), HandlerError> {
        let stations: Vec<IslandOfIrelandStation> = decode(entry)?;
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
