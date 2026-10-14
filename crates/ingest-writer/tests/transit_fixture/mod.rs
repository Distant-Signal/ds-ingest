//! Shared by `transit_store.rs` and `transit_handlers.rs`: a small
//! Metrolink feed (one line, two stations with a platform each, one
//! pattern, `trips` trips in two parts, today's service, one rail link)
//! and the database handles.
//!
//! The feeds are network `metrolink`, the one transit network this build
//! knows: under the writer's own role, its `ingest_freshness` row policy
//! admits only `transit-metrolink-*` sources. The tests clean every
//! metrolink row up through `MIGRATION_DATABASE_URL` (the owner, under
//! scripts/test-postgres-roles.py), since the writer may not delete
//! `transit_networks` or freshness rows. So run them on a test database
//! only, with `--test-threads=1`.

use std::collections::BTreeMap;

use chrono::{DateTime, Days, Utc};
use common::transit::{
    Direction, Horizon, NetworkId, RailLinkKind, StopKind, TimetableCommit, TimetableCounts,
    TimetablePart, TimetablePartKind, TimetableRows, TimetableSource, TransitAttribution,
    TransitLine, TransitLineDirection, TransitNetworkRow, TransitPattern, TransitPatternStop,
    TransitRailLink, TransitServiceDays, TransitStop, TransitTrip, service_date,
};
use ds_store::transit::PartIndex;
use sqlx::PgPool;

pub(crate) const NETWORK: &str = "metrolink";

pub(crate) fn network() -> NetworkId {
    NetworkId::parse(NETWORK).unwrap()
}

/// Now, at the envelope's millisecond precision.
pub(crate) fn now_millis() -> DateTime<Utc> {
    DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap()
}

/// One feed version: its parts in order (with their part numbers) and its
/// commit.
pub(crate) struct Feed {
    pub(crate) parts: Vec<(TimetablePart, PartIndex)>,
    pub(crate) commit: TimetableCommit,
}

impl Feed {
    /// Version `version` with `trips` trips (split over two parts), whose
    /// service runs yesterday and today (London) as of `at`. `sha` is the
    /// content hash (64 hex characters are built from it).
    #[expect(clippy::too_many_lines, reason = "one literal feed, part by part")]
    pub(crate) fn new(version: &str, sha: char, trips: usize, at: DateTime<Utc>) -> Self {
        let today = service_date(at);
        let part = |rows: TimetableRows| TimetablePart {
            network: network(),
            feed_version: version.to_owned(),
            rows,
        };
        let one = PartIndex { part: 1, parts: 1 };
        let stations = ["9400ZZMAPIC", "9400ZZMAECC"];
        let mut stops = Vec::new();
        for station in stations {
            stops.push(TransitStop {
                stop_id: station.to_owned(),
                kind: StopKind::Station,
                parent_stop_id: None,
                name: station.to_owned(),
                indicator: None,
                lat: Some(53.47),
                lon: Some(-2.23),
                wheelchair: Some(true),
            });
            stops.push(TransitStop {
                stop_id: format!("{station}1"),
                kind: StopKind::Platform,
                parent_stop_id: Some(station.to_owned()),
                name: station.to_owned(),
                indicator: Some("To Eccles".to_owned()),
                lat: None,
                lon: None,
                wheelchair: None,
            });
        }
        let trips: Vec<TransitTrip> = (0..trips)
            .map(|i| TransitTrip {
                trip_id: format!("T{i}"),
                line_id: Some("blue".to_owned()),
                pattern_id: "p1".to_owned(),
                service_id: "s1".to_owned(),
                start_s: 18_000 + i32::try_from(i).unwrap() * 720,
                headsign: Some("Eccles".to_owned()),
                replacement_bus: false,
            })
            .collect();
        let half = trips.len().div_ceil(2);
        let (first, second) = trips.split_at(half);
        let parts = vec![
            (
                part(TimetableRows::Network(vec![TransitNetworkRow {
                    name: "Manchester Metrolink".to_owned(),
                    mode: "tram".to_owned(),
                    timezone: "Europe/London".to_owned(),
                    attribution: TransitAttribution {
                        licence: "OGL-UK-3.0".to_owned(),
                        text: "Contains public sector information.".to_owned(),
                    },
                }])),
                one,
            ),
            (
                part(TimetableRows::Lines(vec![TransitLine {
                    line_id: "blue".to_owned(),
                    name: "Blue Line".to_owned(),
                    colour: Some("#0072bc".to_owned()),
                    sort_order: 1,
                    directions: vec![TransitLineDirection {
                        direction: Direction::Outbound,
                        headsign: Some("Eccles".to_owned()),
                        stations: stations.map(str::to_owned).to_vec(),
                    }],
                }])),
                one,
            ),
            (part(TimetableRows::Stops(stops)), one),
            (
                part(TimetableRows::Patterns(vec![TransitPattern {
                    pattern_id: "p1".to_owned(),
                    line_id: Some("blue".to_owned()),
                    direction: Direction::Outbound,
                    headsign: Some("Eccles".to_owned()),
                    stops: vec![
                        TransitPatternStop {
                            stop_id: "9400ZZMAPIC1".to_owned(),
                            arr_offset_s: 0,
                            dep_offset_s: 0,
                            pickup: 0,
                            drop_off: 1,
                        },
                        TransitPatternStop {
                            stop_id: "9400ZZMAECC1".to_owned(),
                            arr_offset_s: 1800,
                            dep_offset_s: 1800,
                            pickup: 1,
                            drop_off: 0,
                        },
                    ],
                }])),
                one,
            ),
            (
                part(TimetableRows::Trips(first.to_vec())),
                PartIndex { part: 1, parts: 2 },
            ),
            (
                part(TimetableRows::Trips(second.to_vec())),
                PartIndex { part: 2, parts: 2 },
            ),
            (
                part(TimetableRows::ServiceDays(vec![TransitServiceDays {
                    service_id: "s1".to_owned(),
                    dates: vec![today.checked_sub_days(Days::new(1)).unwrap(), today],
                }])),
                one,
            ),
            (
                part(TimetableRows::RailLinks(vec![TransitRailLink {
                    stop_id: "9400ZZMAPIC".to_owned(),
                    crs: "MAN".to_owned(),
                    tiploc: "MNCRPIC".to_owned(),
                    kind: RailLinkKind::Interchange,
                    note: None,
                }])),
                one,
            ),
        ];
        let mut counts: BTreeMap<TimetablePartKind, u32> = BTreeMap::new();
        for (p, _) in &parts {
            *counts.entry(p.rows.kind()).or_default() += 1;
        }
        let commit = TimetableCommit {
            network: network(),
            feed_version: version.to_owned(),
            content_sha256: sha.to_string().repeat(64),
            parts: counts,
            counts: TimetableCounts {
                lines: 1,
                stops: 4,
                patterns: 1,
                trips: u32::try_from(trips.len()).unwrap(),
                service_days: 2,
            },
            horizon: Horizon {
                from: today.checked_sub_days(Days::new(1)).unwrap(),
                to: today,
            },
            source: TimetableSource {
                url_kind: "bods-gtfs".to_owned(),
                feed_version: Some(version.to_owned()),
                published_at: None,
                refresh: None,
            },
        };
        Self { parts, commit }
    }
}

pub(crate) struct Db {
    pub(crate) pool: PgPool,
    /// Cleanup as the owner (`MIGRATION_DATABASE_URL`) when set.
    pub(crate) admin: PgPool,
}

impl Db {
    pub(crate) async fn new() -> Self {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect");
        let admin = match std::env::var("MIGRATION_DATABASE_URL") {
            Ok(admin_url) => sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&admin_url)
                .await
                .expect("connect MIGRATION_DATABASE_URL"),
            Err(_) => pool.clone(),
        };
        let db = Self { pool, admin };
        db.cleanup().await;
        db
    }

    /// Deletes every metrolink row (and the test dedup keys).
    pub(crate) async fn cleanup(&self) {
        for sql in [
            "DELETE FROM transit_feed_versions WHERE network = $1",
            "DELETE FROM transit_networks WHERE network = $1",
            "DELETE FROM transit_rail_links WHERE network = $1",
            "DELETE FROM ingest_freshness WHERE source = 'transit-' || $1 || '-timetable'",
            "DELETE FROM ingest_freshness WHERE source = 'transit-' || $1 || '-situations'",
            "DELETE FROM transit_situations WHERE network = $1",
            "DELETE FROM line_status_history WHERE starts_with(line_id, $1 || ':')",
            "DELETE FROM line_status WHERE starts_with(line_id, $1 || ':')",
            "DELETE FROM ingest_dedup WHERE key LIKE '%transit-test-%' AND $1 <> ''",
        ] {
            sqlx::query(sql)
                .bind(NETWORK)
                .execute(&self.admin)
                .await
                .unwrap_or_else(|err| panic!("{sql}: {err}"));
        }
    }

    /// `(active_feed_version, activated_at)` of the network.
    pub(crate) async fn active(&self) -> Option<(Option<String>, Option<DateTime<Utc>>)> {
        sqlx::query_as(
            "SELECT active_feed_version, activated_at FROM transit_networks WHERE network = $1",
        )
        .bind(NETWORK)
        .fetch_optional(&self.pool)
        .await
        .unwrap()
    }

    /// Every version of the network and its state, by version.
    pub(crate) async fn versions(&self) -> Vec<(String, String)> {
        sqlx::query_as(
            "SELECT feed_version, state FROM transit_feed_versions WHERE network = $1 ORDER BY 1",
        )
        .bind(NETWORK)
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    /// The trips stored under `version`.
    pub(crate) async fn trips(&self, version: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM transit_trips WHERE network = $1 AND feed_version = $2",
        )
        .bind(NETWORK)
        .bind(version)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    pub(crate) async fn freshness(&self) -> Option<DateTime<Utc>> {
        sqlx::query_scalar(
            "SELECT fetched_at FROM ingest_freshness WHERE source = 'transit-metrolink-timetable'",
        )
        .fetch_optional(&self.pool)
        .await
        .unwrap()
    }
}
