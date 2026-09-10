//! Decision 2b's in-memory population map and Decision 2c's reverse
//! tiploc->line index, built from `schedule_query::LinePopulationEntry`
//! rows fetched via `GET /private/schedule-line-population`.

use std::collections::HashMap;

use schedule_query::{CallingPoint, LinePopulationEntry};

#[derive(Debug, Clone, Default)]
pub struct Population {
    /// line_id -> service_date -> uid -> calling points
    by_line: HashMap<String, HashMap<chrono::NaiveDate, HashMap<String, Vec<CallingPoint>>>>,
}

impl Population {
    pub fn insert(
        &mut self,
        line_id: &str,
        service_date: chrono::NaiveDate,
        entries: Vec<LinePopulationEntry>,
    ) {
        let by_uid: HashMap<String, Vec<CallingPoint>> = entries
            .into_iter()
            .map(|e| (e.uid, e.calling_points))
            .collect();
        self.by_line
            .entry(line_id.to_string())
            .or_default()
            .insert(service_date, by_uid);
    }

    /// Every UID this line's population contains for `service_date`,
    /// empty if nothing has been published yet (Decision 2e's Pending
    /// case, upstream of the rail-day gate).
    pub fn uids_for(&self, line_id: &str, service_date: chrono::NaiveDate) -> Vec<&str> {
        self.by_line
            .get(line_id)
            .and_then(|by_date| by_date.get(&service_date))
            .map(|by_uid| by_uid.keys().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// Unused by `main.rs`'s loop today -- `stats::synthesize_departure`
    /// doesn't consult a UID's own calling points yet (Decision 2g's
    /// PASS-to-skipped mapping is unresolved, per that module's own doc
    /// comment), but this accessor is real, tested API surface for that
    /// future pass, not speculative.
    #[allow(dead_code)]
    pub fn calling_points(
        &self,
        line_id: &str,
        service_date: chrono::NaiveDate,
        uid: &str,
    ) -> Option<&[CallingPoint]> {
        self.by_line
            .get(line_id)?
            .get(&service_date)?
            .get(uid)
            .map(Vec::as_slice)
    }
}

/// Real, CIF-derived CRS -> TIPLOC(s), inverted from a live
/// `stanox_crs`/`common::StanoxCrsRecord` snapshot -- mirrors
/// `schedule-reference::crs_to_tiploc_map` exactly (same shape, same
/// reasoning: a CRS can resolve to more than one real TIPLOC, e.g.
/// multiple STANOX rows sharing a CRS for different platforms/areas of
/// one physical location).
fn crs_to_tiploc_map(records: &[common::StanoxCrsRecord]) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for record in records {
        map.entry(record.crs.to_uppercase())
            .or_default()
            .push(record.tiploc.clone());
    }
    map
}

/// Decision 2c's reverse index: tiploc -> every shadow-computed line whose
/// catalogue includes a station resolving to it. Rebuilt every
/// `stanox_crs` reload cycle (not just once from the static catalogue --
/// see `main.rs`'s own reload step) from `stanox_crs_records`, the same
/// real, CIF-derived data `stanox_tiploc::StanoxTable` is built from.
///
/// As of the 2026-09-09 tiploc-schedule-matching-gap fix, this no longer
/// gates on the `lines/*.toml` `Station.tiploc` field at all: that field
/// is hand-curated, optional, and mostly absent (~83% of catalogued CRS
/// codes have no TOML `tiploc` set), so gating on it silently excluded
/// most real stations from ever being indexed -- this was this codebase's
/// fourth independent copy of the same bug already fixed in
/// `api::data::schedule_matching::crs_to_line_ids`,
/// `schedule-reference::lines_to_publish`/`line_tiplocs`, and
/// `trust-backlog-consumer::crs_index::build_crs_index`. Each station's
/// real TIPLOC(s) are now resolved from `crs_to_tiploc_map` via its CRS
/// (always present, unlike the TOML `tiploc` field) instead.
pub fn build_tiploc_index(
    lines: &[common::LineDefinition],
    stanox_crs_records: &[common::StanoxCrsRecord],
) -> HashMap<String, Vec<String>> {
    let crs_to_tiploc = crs_to_tiploc_map(stanox_crs_records);
    let mut index: HashMap<String, Vec<String>> = HashMap::new();
    for line in lines {
        for station in &line.stations {
            let Some(tiplocs) = crs_to_tiploc.get(&station.crs.to_uppercase()) else {
                continue;
            };
            for tiploc in tiplocs {
                let ids = index.entry(tiploc.clone()).or_default();
                if !ids.contains(&line.id) {
                    ids.push(line.id.clone());
                }
            }
        }
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_calling_point(tiploc: &str) -> CallingPoint {
        CallingPoint {
            tiploc: tiploc.to_string(),
            kind: schedule_query::CallingPointKind::Origin,
            booked_arrival: None,
            booked_departure: None,
            is_half_minute_arrival: false,
            is_half_minute_departure: false,
            day_offset: 0,
        }
    }

    #[test]
    fn insert_then_uids_for_returns_the_inserted_uids() {
        let mut population = Population::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        population.insert(
            "waterloo-reading",
            date,
            vec![LinePopulationEntry {
                uid: "C11052".to_string(),
                calling_points: vec![fixture_calling_point("WATRLMN")],
            }],
        );
        assert_eq!(
            population.uids_for("waterloo-reading", date),
            vec!["C11052"]
        );
    }

    #[test]
    fn uids_for_an_unpublished_line_or_date_is_empty_not_a_panic() {
        let population = Population::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        assert!(population.uids_for("nonexistent", date).is_empty());
    }

    #[test]
    fn calling_points_round_trips_the_inserted_entry() {
        let mut population = Population::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let cp = fixture_calling_point("WATRLMN");
        population.insert(
            "waterloo-reading",
            date,
            vec![LinePopulationEntry {
                uid: "C11052".to_string(),
                calling_points: vec![cp.clone()],
            }],
        );
        assert_eq!(
            population.calling_points("waterloo-reading", date, "C11052"),
            Some(&[cp][..])
        );
    }

    /// Stations are built with `tiploc: None` throughout -- the exact
    /// scenario the 2026-09-09 fix covers: `build_tiploc_index` must
    /// resolve real TIPLOCs from `stanox_crs_records` via each station's
    /// CRS, never from this TOML field.
    fn fixture_line(id: &str, crs_codes: &[&str]) -> common::LineDefinition {
        common::LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "rail".to_string(),
            category: "national-rail".to_string(),
            operators: vec![],
            stations: crs_codes
                .iter()
                .map(|c| common::Station {
                    crs: c.to_string(),
                    tiploc: None,
                    role: "minor".to_string(),
                    segment: None,
                })
                .collect(),
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: std::collections::HashMap::new(),
            exclusive_segments: vec![],
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
        }
    }

    fn fixture_stanox_crs_record(crs: &str, tiploc: &str) -> common::StanoxCrsRecord {
        common::StanoxCrsRecord {
            stanox: format!("STANOX-{tiploc}"),
            crs: crs.to_string(),
            tiploc: tiploc.to_string(),
            station_name: format!("{crs} STATION"),
            source_sequence: 1,
        }
    }

    #[test]
    fn build_tiploc_index_maps_a_shared_crs_to_both_lines() {
        let lines = vec![
            fixture_line("line-a", &["SHR", "ZZA"]),
            fixture_line("line-b", &["SHR", "ZZB"]),
        ];
        let records = vec![
            fixture_stanox_crs_record("SHR", "SHARED"),
            fixture_stanox_crs_record("ZZA", "ONLY_A"),
            fixture_stanox_crs_record("ZZB", "ONLY_B"),
        ];
        let index = build_tiploc_index(&lines, &records);
        let mut shared = index.get("SHARED").cloned().unwrap_or_default();
        shared.sort();
        assert_eq!(shared, vec!["line-a".to_string(), "line-b".to_string()]);
        assert_eq!(index.get("ONLY_A"), Some(&vec!["line-a".to_string()]));
        assert_eq!(index.get("ONLY_B"), Some(&vec!["line-b".to_string()]));
    }

    /// The actual regression test for the tiploc-schedule-matching-gap bug
    /// (2026-09-09), this crate's own fourth site: a station whose
    /// `lines/*.toml` entry carries no `tiploc` at all (the ~83%-of-CRS-
    /// codes common case) must still be indexed, because its real TIPLOC
    /// now comes from the CIF-derived `stanox_crs` snapshot via its CRS,
    /// not from the TOML field. Before this fix, `build_tiploc_index`
    /// looked only at `station.tiploc.is_some()`, so this exact station
    /// would have been silently absent from the index -- any real live
    /// TRUST Movement reported at it would never match this line's
    /// correlation/coverage metrics.
    #[test]
    fn build_tiploc_index_includes_a_station_with_no_toml_tiploc_via_real_cif_data() {
        let lines = vec![fixture_line("line-a", &["ZNT"])];
        let records = vec![fixture_stanox_crs_record("ZNT", "ZNOTIPLOC")];
        let index = build_tiploc_index(&lines, &records);
        assert_eq!(index.get("ZNOTIPLOC"), Some(&vec!["line-a".to_string()]));
    }

    #[test]
    fn build_tiploc_index_ignores_a_station_with_no_matching_stanox_crs_record() {
        let lines = vec![fixture_line("line-a", &["ZZZ"])];
        let index = build_tiploc_index(&lines, &[]);
        assert!(index.is_empty());
    }

    #[test]
    fn crs_to_tiploc_map_inverts_records_uppercasing_the_crs_key() {
        let records = vec![
            common::StanoxCrsRecord {
                stanox: "S1".to_string(),
                crs: "znt".to_string(),
                tiploc: "ZNOTIPLOC".to_string(),
                station_name: "TEST STATION".to_string(),
                source_sequence: 1,
            },
            common::StanoxCrsRecord {
                stanox: "S2".to_string(),
                crs: "ZNT".to_string(),
                tiploc: "ZNOTIPLOC2".to_string(),
                station_name: "TEST STATION".to_string(),
                source_sequence: 1,
            },
        ];
        let map = crs_to_tiploc_map(&records);
        let mut tiplocs = map.get("ZNT").cloned().unwrap_or_default();
        tiplocs.sort_unstable();
        assert_eq!(
            tiplocs,
            vec!["ZNOTIPLOC".to_string(), "ZNOTIPLOC2".to_string()]
        );
    }
}
