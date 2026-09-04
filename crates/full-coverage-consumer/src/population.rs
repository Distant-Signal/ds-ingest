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

/// Decision 2c's reverse index: tiploc -> every shadow-computed line whose
/// `lines/*.toml` `Station.tiploc` includes it. Built once per population
/// reload from the static catalogue (not from the population data itself
/// -- a line's STATION list, not its scheduled services, defines which
/// TIPLOCs are "its own").
pub fn build_tiploc_index(lines: &[common::LineDefinition]) -> HashMap<String, Vec<String>> {
    let mut index: HashMap<String, Vec<String>> = HashMap::new();
    for line in lines {
        for station in &line.stations {
            if let Some(tiploc) = &station.tiploc {
                index
                    .entry(tiploc.clone())
                    .or_default()
                    .push(line.id.clone());
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

    fn fixture_line(id: &str, tiplocs: &[&str]) -> common::LineDefinition {
        common::LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "rail".to_string(),
            category: "national-rail".to_string(),
            operators: vec![],
            stations: tiplocs
                .iter()
                .map(|t| common::Station {
                    crs: "ZZZ".to_string(),
                    tiploc: Some(t.to_string()),
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

    #[test]
    fn build_tiploc_index_maps_a_shared_tiploc_to_both_lines() {
        let lines = vec![
            fixture_line("line-a", &["SHARED", "ONLY_A"]),
            fixture_line("line-b", &["SHARED", "ONLY_B"]),
        ];
        let index = build_tiploc_index(&lines);
        let mut shared = index.get("SHARED").cloned().unwrap_or_default();
        shared.sort();
        assert_eq!(shared, vec!["line-a".to_string(), "line-b".to_string()]);
        assert_eq!(index.get("ONLY_A"), Some(&vec!["line-a".to_string()]));
        assert_eq!(index.get("ONLY_B"), Some(&vec!["line-b".to_string()]));
    }
}
