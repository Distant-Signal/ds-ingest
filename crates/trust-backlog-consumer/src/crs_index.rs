//! An independently-built `HashSet<CRS>` of every catalogued-line CRS
//! with at least one TIPLOC-bearing station -- this consumer's own scope
//! boundary (Decision 2 of
//! docs/superpowers/specs/2026-09-05-trust-event-backlog-design.md).
//!
//! Deliberately NOT reused from, or dependent on,
//! docs/superpowers/plans/2026-09-05-schedule-first-train-tracking-plan.md's
//! own `crs_to_line_ids` (a private field inside `api`'s own process
//! memory, unreachable from a separate binary crate regardless of merge
//! order) -- see
//! docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md's own
//! "Dependency on the schedule-first plan" section for the full
//! reasoning. This module is this codebase's THIRD independent
//! implementation of the same "line has >=1 TIPLOC-bearing station"
//! predicate, alongside `full-coverage-consumer::population::build_tiploc_index`
//! and `api::data::schedule_matching::crs_to_line_ids` (both now merged
//! on `main`) -- a named, accepted repeat (see that plan's own
//! Non-goals), not deduplicated here.
//!
//! A plain `HashSet<String>` (not a `HashMap<String, Vec<line_id>>` like
//! the other two): this consumer only ever needs a yes/no "is this CRS
//! in scope" answer (Decision 2's own scoping rule), never "which line,"
//! so the smaller, simpler type is used rather than carrying unused
//! line-id data through every Movement this consumer processes.

use std::collections::HashSet;

use common::LineDefinition;

pub fn build_crs_index(lines: &[LineDefinition]) -> HashSet<String> {
    let mut index = HashSet::new();
    for line in lines {
        for station in &line.stations {
            if station.tiploc.is_some() {
                index.insert(station.crs.to_uppercase());
            }
        }
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(id: &str, stations: Vec<(&str, Option<&str>)>) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "rail".to_string(),
            category: "national-rail".to_string(),
            operators: vec![],
            stations: stations
                .into_iter()
                .map(|(crs, tiploc)| common::Station {
                    crs: crs.to_string(),
                    tiploc: tiploc.map(str::to_string),
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
    fn a_tiploc_bearing_station_is_indexed_uppercased() {
        let lines = vec![line("wcml", vec![("eus", Some("EUSTON"))])];
        let index = build_crs_index(&lines);
        assert!(index.contains("EUS"));
    }

    #[test]
    fn a_station_with_no_tiploc_is_not_indexed() {
        let lines = vec![line("wcml", vec![("ZZZ", None)])];
        let index = build_crs_index(&lines);
        assert!(!index.contains("ZZZ"));
    }

    #[test]
    fn two_lines_sharing_a_crs_index_it_once() {
        let lines = vec![
            line("line-a", vec![("EUS", Some("EUSTON"))]),
            line("line-b", vec![("EUS", Some("EUSTON"))]),
        ];
        let index = build_crs_index(&lines);
        assert_eq!(index.len(), 1);
    }
}
