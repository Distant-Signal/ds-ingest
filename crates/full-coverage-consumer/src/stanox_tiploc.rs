//! STANOX -> TIPLOC and STANOX -> CRS, from the same live
//! `/private/stanox-crs` feed `trust-consumer` already reloads --
//! extended (unlike `trust-consumer::stanox_crs::StanoxCrsTable`, which
//! drops `tiploc`) to keep BOTH fields, since this consumer needs both:
//! TIPLOC for line-population membership (Decision 2c), CRS for
//! station-level grouping (Decision 2h). See
//! docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md's
//! "Current relevant state" section, the `common::StanoxCrsRecord`
//! finding.

use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct StanoxTable {
    stanox_to_tiploc: HashMap<String, String>,
    stanox_to_crs: HashMap<String, String>,
}

impl StanoxTable {
    pub fn from_records(records: &[common::StanoxCrsRecord]) -> Self {
        let mut stanox_to_tiploc = HashMap::new();
        let mut stanox_to_crs = HashMap::new();
        for r in records {
            stanox_to_tiploc.insert(r.stanox.clone(), r.tiploc.clone());
            stanox_to_crs.insert(r.stanox.clone(), r.crs.clone());
        }
        Self {
            stanox_to_tiploc,
            stanox_to_crs,
        }
    }

    pub fn tiploc(&self, stanox: &str) -> Option<&str> {
        self.stanox_to_tiploc.get(stanox).map(String::as_str)
    }

    pub fn crs(&self, stanox: &str) -> Option<&str> {
        self.stanox_to_crs.get(stanox).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_records() -> Vec<common::StanoxCrsRecord> {
        vec![
            common::StanoxCrsRecord {
                stanox: "87212".to_string(),
                crs: "WAT".to_string(),
                tiploc: "WATRLMN".to_string(),
                station_name: "LONDON WATERLOO".to_string(),
                source_sequence: 1,
            },
            common::StanoxCrsRecord {
                stanox: "87701".to_string(),
                crs: "WOK".to_string(),
                tiploc: "WOKINGM".to_string(),
                station_name: "WOKING".to_string(),
                source_sequence: 1,
            },
        ]
    }

    #[test]
    fn both_lookups_succeed_for_known_stanox_values() {
        let table = StanoxTable::from_records(&fixture_records());
        assert_eq!(table.tiploc("87212"), Some("WATRLMN"));
        assert_eq!(table.crs("87212"), Some("WAT"));
        assert_eq!(table.tiploc("87701"), Some("WOKINGM"));
        assert_eq!(table.crs("87701"), Some("WOK"));
    }

    #[test]
    fn an_unknown_stanox_returns_none_for_both_lookups() {
        let table = StanoxTable::from_records(&fixture_records());
        assert_eq!(table.tiploc("00000"), None);
        assert_eq!(table.crs("00000"), None);
    }
}
