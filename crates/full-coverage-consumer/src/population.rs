//! Decision 2b's in-memory population map and Decision 2c's reverse
//! tiploc->line index, built from `schedule_query::LinePopulationEntry`
//! rows fetched via `GET /private/schedule-line-population`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[cfg(test)]
use schedule_query::LinePopulationEntry;

/// One `schedule-line-population` wire entry, reduced to the only field
/// this crate reads. Deserializing into this instead of
/// `schedule_query::LinePopulationEntry` skips each entry's
/// `calling_points` array without building it (serde ignores unknown
/// fields): the largest production line (3,357 entries, ~22 calling points
/// each, 21 MB of JSON on 2026-09-27) used to be materialized in full --
/// every `CallingPoint` of every entry -- just to take the uids and drop
/// the rest. Now a line costs its body text plus its uids.
#[derive(Debug, serde::Deserialize)]
pub struct UidOnly {
    pub uid: String,
}

#[derive(Debug, Clone, Default)]
pub struct Population {
    /// line_id -> service_date -> uid set.
    ///
    /// Deliberately just the UID, not the `Vec<CallingPoint>` each wire
    /// entry (`schedule_query::LinePopulationEntry`) also carries -- see
    /// `insert`'s own doc comment for why retaining it was a real
    /// production memory-pressure bug (found during the 2026-09-26
    /// crash-loop investigation), not a deliberate design choice.
    ///
    /// Each set is behind an `Arc` (2026-09-27) so the background reloader
    /// can build the next snapshot sharing every unchanged set with the one
    /// it replaces (a `304` just carries the old `Arc` over) instead of
    /// deep-copying the population on every cycle -- see
    /// `population_reload`.
    by_line: HashMap<String, HashMap<chrono::NaiveDate, Arc<HashSet<String>>>>,
    /// `(line_id, service_date)` -> the `ETag` `api` sent with the
    /// population currently held in `by_line` for that key, if it sent one.
    ///
    /// Sent back as `If-None-Match` on the next reload so an unchanged
    /// population comes back as a bodyless `304` instead of a full
    /// re-download (2026-09-26: every line, today and tomorrow, every 300s,
    /// was ~10 GB per cycle of JSON through `api`, and part of what kept
    /// `api` OOM-killing). Only ever set together with the uid set it
    /// describes, and dropped with it, so a validator is never sent for a
    /// population this process does not actually hold. An `api` that
    /// predates `ETag` support sends none, so nothing is stored and every
    /// reload is a plain unconditional GET, exactly as before.
    etags: HashMap<(String, chrono::NaiveDate), String>,
}

impl Population {
    /// Inserts `entries` for `(line_id, service_date)`, keeping only each
    /// entry's `uid` -- **not** its `calling_points`.
    ///
    /// This used to retain the whole `Vec<CallingPoint>` per UID per line
    /// per date, straight off the wire. That was pure waste: `uids_for`,
    /// the only accessor this crate's dispatch loop
    /// (`correlate::apply_movement`) or its stats path (`main::write_stats`)
    /// ever calls, only needs UID membership, and the one accessor that DID
    /// return calling points (`calling_points`, removed by this fix) had
    /// exactly one caller in the entire repo: its own round-trip test --
    /// confirmed by grepping every crate for `.calling_points(` during the
    /// 2026-09-26 investigation into this consumer OOM-killing against its
    /// 1Gi limit.
    ///
    /// Worse, the waste was multiplied by the line catalogue: `schedules_touching`
    /// (`schedule-reference`'s producer side) is run independently per
    /// catalogued line, and each run pulls in EVERY schedule touching ANY
    /// of that line's own stations, complete with that schedule's FULL
    /// national calling-point list -- not just the calling points at that
    /// line's own stations. With 244 `lines/*.toml` files as of this fix
    /// (up from the 109 a 2026-09-11 doc comment elsewhere in this crate
    /// still quotes -- the catalogue has more than doubled since numbers
    /// like that were last checked), a great many real services call at
    /// stations belonging to several catalogued lines at once, so the same
    /// schedule's calling-point list was being deserialized and retained
    /// once per line it touched, for both today's and tomorrow's date,
    /// every `population_reload_secs` cycle (300s by default). A
    /// `CallingPoint` is not small either: `tiploc`/`activity` `String`s
    /// plus four `Option<NaiveTime>` fields per entry, times roughly a
    /// dozen calling points on a typical schedule -- multiple orders of
    /// magnitude heavier per UID than the bare UID `String` this crate
    /// actually needs.
    ///
    /// This is exactly the "footprint scales with the line catalogue" risk
    /// `charts/distant-signal/values.yaml`'s `fullCoverageConsumer.resources`
    /// comment already named when its 1Gi limit was set (2026-09-25) --
    /// except the data driving that footprint was never actually read at
    /// runtime, so the fix is to stop retaining it, not to raise the limit.
    ///
    /// Test-only since 2026-09-26: the reload calls
    /// [`Population::insert_uids`].
    #[cfg(test)]
    pub fn insert(
        &mut self,
        line_id: &str,
        service_date: chrono::NaiveDate,
        entries: Vec<LinePopulationEntry>,
    ) {
        self.insert_with_etag(line_id, service_date, entries, None);
    }

    /// [`Population::insert`], also recording the `ETag` the population
    /// arrived with. Test-only: see [`Population::insert_uids`] (`None` when `api` sent none, which clears any previous
    /// one for this key -- the new data is no longer described by it).
    #[cfg(test)]
    pub fn insert_with_etag(
        &mut self,
        line_id: &str,
        service_date: chrono::NaiveDate,
        entries: Vec<LinePopulationEntry>,
        etag: Option<String>,
    ) {
        let uids: HashSet<String> = entries.into_iter().map(|e| e.uid).collect();
        self.insert_uids(line_id, service_date, uids, etag);
    }

    /// Stores `(line_id, service_date)`'s uid set and the `ETag` it arrived
    /// with (`None` clears any previous one -- the new data is no longer
    /// described by it). What the reload calls, with uids parsed straight
    /// off the wire by [`UidOnly`].
    pub fn insert_uids(
        &mut self,
        line_id: &str,
        service_date: chrono::NaiveDate,
        uids: HashSet<String>,
        etag: Option<String>,
    ) {
        self.by_line
            .entry(line_id.to_string())
            .or_default()
            .insert(service_date, Arc::new(uids));
        let key = (line_id.to_string(), service_date);
        match etag {
            Some(etag) => {
                self.etags.insert(key, etag);
            }
            None => {
                self.etags.remove(&key);
            }
        }
    }

    /// The `ETag` of the population currently held for
    /// `(line_id, service_date)`, to send as `If-None-Match` -- `None` when
    /// nothing is held or `api` sent no `ETag` with it.
    pub fn etag_for(&self, line_id: &str, service_date: chrono::NaiveDate) -> Option<&str> {
        self.etags
            .get(&(line_id.to_string(), service_date))
            .map(String::as_str)
    }

    /// Drops every stored date strictly older than `service_date`, and any
    /// line left with no dates at all.
    ///
    /// Without this, nothing ever removed a past date: `insert` is called
    /// for today AND tomorrow on every reload cycle (300s by default), so a
    /// long-lived process accumulated one full per-line UID set per rail
    /// day forever -- data no longer read by anything, since `uids_for` is
    /// only ever asked about the current `service_date`. Called at each
    /// rail-day rollover and at the end of each reload, so the resident set
    /// stays at today+tomorrow.
    ///
    /// Test-only since 2026-09-27: `population_reload::reload_cycle` builds
    /// each snapshot from scratch with only today's and tomorrow's dates, so
    /// an older date is dropped by construction.
    #[cfg(test)]
    pub fn retain_from(&mut self, service_date: chrono::NaiveDate) {
        self.by_line.retain(|_line_id, by_date| {
            by_date.retain(|date, _| *date >= service_date);
            !by_date.is_empty()
        });
        self.etags.retain(|(_, date), _| *date >= service_date);
    }

    /// Carries `(line_id, service_date)`'s population -- its uid set and
    /// `ETag` -- over from `previous` unchanged, sharing the set rather than
    /// copying it. Used for a `304` and for a fetch that failed (keep the
    /// previous snapshot). A no-op when `previous` holds nothing for it.
    pub fn carry_over(
        &mut self,
        previous: &Population,
        line_id: &str,
        service_date: chrono::NaiveDate,
    ) {
        let Some(uids) = previous
            .by_line
            .get(line_id)
            .and_then(|by_date| by_date.get(&service_date))
        else {
            return;
        };
        self.by_line
            .entry(line_id.to_string())
            .or_default()
            .insert(service_date, Arc::clone(uids));
        let key = (line_id.to_string(), service_date);
        if let Some(etag) = previous.etags.get(&key) {
            self.etags.insert(key, etag.clone());
        }
    }

    /// Whether a population (possibly empty) is held for
    /// `(line_id, service_date)`.
    #[cfg(test)]
    pub fn has(&self, line_id: &str, service_date: chrono::NaiveDate) -> bool {
        self.by_line
            .get(line_id)
            .is_some_and(|by_date| by_date.contains_key(&service_date))
    }

    /// Whether `uid` is in `line_id`'s population for `service_date` --
    /// the per-Movement membership test. A hash lookup: the previous
    /// `uids_for(..).contains(..)` collected the line's whole uid set into
    /// a `Vec` for every candidate line of every Movement, which is what a
    /// startup replay of a full rail day (~1M entries) would otherwise pay.
    pub fn contains(&self, line_id: &str, service_date: chrono::NaiveDate, uid: &str) -> bool {
        self.by_line
            .get(line_id)
            .and_then(|by_date| by_date.get(&service_date))
            .is_some_and(|uids| uids.contains(uid))
    }

    /// Total uids held across every line and date -- for the
    /// `population_uids` gauge and memory sizing.
    pub fn total_uids(&self) -> usize {
        self.by_line
            .values()
            .flat_map(|by_date| by_date.values())
            .map(|uids| uids.len())
            .sum()
    }

    /// Every UID this line's population contains for `service_date`,
    /// empty if nothing has been published yet (Decision 2e's Pending
    /// case, upstream of the rail-day gate).
    pub fn uids_for(&self, line_id: &str, service_date: chrono::NaiveDate) -> Vec<&str> {
        self.by_line
            .get(line_id)
            .and_then(|by_date| by_date.get(&service_date))
            .map(|uids| uids.iter().map(String::as_str).collect())
            .unwrap_or_default()
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

    fn fixture_calling_point(tiploc: &str) -> schedule_query::CallingPoint {
        schedule_query::CallingPoint {
            tiploc: tiploc.into(),
            kind: schedule_query::CallingPointKind::Origin,
            booked_arrival: None,
            booked_departure: None,
            is_half_minute_arrival: false,
            is_half_minute_departure: false,
            day_offset: 0,
            activity: Default::default(),
            public_arrival: None,
            public_departure: None,
            platform: None,
        }
    }

    /// The reload parses the real wire shape (full calling points, as
    /// `api` serves them) into uids alone; `null` (nothing published) still
    /// parses as `None`.
    #[test]
    fn uid_only_parses_the_real_wire_shape_and_skips_calling_points() {
        let body = r#"[{"uid": "W45448", "calling_points": [{"kind": "Origin", "tiploc": "THBDGS ",
            "activity": "TB", "platform": "1", "day_offset": 0, "booked_arrival": null,
            "public_arrival": null, "booked_departure": "17:28:00", "public_departure": "17:28:00",
            "is_half_minute_arrival": false, "is_half_minute_departure": false}]},
            {"uid": "W45449", "calling_points": []}]"#;
        let parsed: Option<Vec<UidOnly>> = serde_json::from_str(body).unwrap();
        let uids: Vec<String> = parsed.unwrap().into_iter().map(|e| e.uid).collect();
        assert_eq!(uids, vec!["W45448", "W45449"]);
        assert!(
            serde_json::from_str::<Option<Vec<UidOnly>>>("null")
                .unwrap()
                .is_none()
        );
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

    /// The conditional-reload bookkeeping: an `ETag` is held exactly as
    /// long as the population it describes -- replaced or cleared by the
    /// next insert for the same key, and pruned with its date.
    #[test]
    fn etags_track_the_population_they_describe() {
        let mut population = Population::default();
        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let tomorrow = today + chrono::Duration::days(1);
        let entry = |uid: &str| LinePopulationEntry {
            uid: uid.to_string(),
            calling_points: vec![fixture_calling_point("WATRLMN")],
        };

        assert_eq!(population.etag_for("waterloo-reading", today), None);

        population.insert_with_etag(
            "waterloo-reading",
            today,
            vec![entry("C11052")],
            Some("\"slp-1\"".to_string()),
        );
        population.insert_with_etag(
            "waterloo-reading",
            tomorrow,
            vec![entry("C11053")],
            Some("\"slp-2\"".to_string()),
        );
        assert_eq!(
            population.etag_for("waterloo-reading", today),
            Some("\"slp-1\"")
        );
        assert_eq!(population.etag_for("other-line", today), None);

        // An api that sends no ETag (one predating conditional GET) clears
        // the stale validator rather than leaving it describing old data.
        population.insert_with_etag("waterloo-reading", today, vec![entry("C99999")], None);
        assert_eq!(population.etag_for("waterloo-reading", today), None);
        assert_eq!(
            population.uids_for("waterloo-reading", today),
            vec!["C99999"]
        );

        population.retain_from(tomorrow);
        assert_eq!(population.etag_for("waterloo-reading", today), None);
        assert_eq!(
            population.etag_for("waterloo-reading", tomorrow),
            Some("\"slp-2\"")
        );
    }

    #[test]
    fn uids_for_an_unpublished_line_or_date_is_empty_not_a_panic() {
        let population = Population::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        assert!(population.uids_for("nonexistent", date).is_empty());
    }

    /// Regression test for the 2026-09-26 crash-loop investigation's actual
    /// finding: a UID's `calling_points` must never end up resident in
    /// `Population`, no matter how large -- only its membership in the
    /// line/date's UID set. This is the fix for
    /// `charts/distant-signal/values.yaml`'s `fullCoverageConsumer` 1Gi
    /// limit being hit: this crate used to retain the FULL calling-point
    /// list (potentially dozens of entries, each carrying several `String`/
    /// `Option<NaiveTime>` fields) for every UID, once per catalogued line
    /// it touched (244 `lines/*.toml` files, many sharing stations), for
    /// both today's and tomorrow's date -- all of it dead weight, since
    /// nothing in this crate ever read it back out (`uids_for` is the only
    /// accessor any caller uses).
    ///
    /// Reaches into the private `by_line` field (this test module is a
    /// child of `population`'s own module, so it may) specifically to
    /// assert on the STORAGE TYPE, not just behavior: `HashSet<String>`
    /// cannot hold a `Vec<CallingPoint>` even by accident, which is the
    /// load-bearing guarantee here, not merely "the test happens to pass
    /// today."
    #[test]
    fn insert_discards_calling_points_keeping_only_uid_membership() {
        let mut population = Population::default();
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        // A deliberately oversized calling-point list -- if any of it were
        // retained, this test's real point (the type itself makes that
        // impossible) would be moot, but the size still documents the scale
        // of the waste a real long schedule represents.
        let heavy_calling_points: Vec<schedule_query::CallingPoint> = (0..50)
            .map(|i| fixture_calling_point(&format!("TPL{i}")))
            .collect();
        population.insert(
            "waterloo-reading",
            date,
            vec![LinePopulationEntry {
                uid: "C11052".to_string(),
                calling_points: heavy_calling_points,
            }],
        );

        assert_eq!(
            population.uids_for("waterloo-reading", date),
            vec!["C11052"],
            "membership must still work"
        );

        let uids = population
            .by_line
            .get("waterloo-reading")
            .and_then(|by_date| by_date.get(&date))
            .expect("just inserted");
        assert_eq!(uids.len(), 1);
        assert!(uids.contains("C11052"));
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
            change_time_minutes: None,
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
                change_time_minutes: None,
            },
            common::StanoxCrsRecord {
                stanox: "S2".to_string(),
                crs: "ZNT".to_string(),
                tiploc: "ZNOTIPLOC2".to_string(),
                station_name: "TEST STATION".to_string(),
                source_sequence: 1,
                change_time_minutes: None,
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
