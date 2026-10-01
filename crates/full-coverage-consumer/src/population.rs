//! Decision 2b's in-memory population map and Decision 2c's reverse
//! tiploc->line index, built from `schedule_query::LinePopulationEntry`
//! rows fetched via `GET /private/schedule-line-population`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[cfg(test)]
use schedule_query::LinePopulationEntry;

/// Whether a line's population for a date could apply the full relevance
/// filter (windowed stats design section 4.1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Relevance {
    /// The population carried `operator_atoc`/`train_status`: buses and
    /// ships are out, and a train must be run by one of the line's
    /// operators and call at two of its stations.
    Full,
    /// An older `schedule-reference` published neither field: only the
    /// two-stations rule applies, and buses cannot be told apart, so
    /// presumed cancellation is off for the line and date.
    #[default]
    StopsOnly,
}

impl Relevance {
    pub fn as_str(self) -> &'static str {
        match self {
            Relevance::Full => "full",
            Relevance::StopsOnly => "stops_only",
        }
    }
}

/// One relevant train of a line, reduced to what the windowed stats need:
/// all times are UTC minutes since the Unix epoch. No calling points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineTrain {
    pub uid: Box<str>,
    /// First booked call at a station of the line (departure, else
    /// arrival): when the train is "due" on it.
    pub due_min: u32,
    /// Last booked call at a station of the line.
    pub last_due_min: u32,
    /// The schedule's origin departure.
    pub origin_dep_min: u32,
}

/// One line's population for one service date.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LinePop {
    /// Every UID except buses and ships -- movement matching and the
    /// legacy (flag-off) row, unchanged semantics.
    pub uids: HashSet<String>,
    /// The relevant trains (section 4.1), sorted by `due_min`. Empty
    /// unless the line's geometry was known when the population was parsed
    /// (only with `FULL_COVERAGE_WINDOWED_STATS=true`).
    pub trains: Vec<LineTrain>,
    pub relevance: Relevance,
    /// [`LineGeometry::hash`] the trains were reduced with (0: none). A
    /// reload re-fetches unconditionally when the line's geometry has
    /// changed since, rather than accepting a `304` for trains reduced
    /// against the old one.
    pub geometry_hash: u64,
    /// Buses and ships left out.
    pub buses_excluded: u32,
}

/// What a line's population is reduced against: its stations' TIPLOCs
/// (resolved through the same `stanox_crs` crosswalk as
/// [`build_tiploc_index`]) and its operators.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LineGeometry {
    /// Bare TIPLOC -> CRS, for the line's stations.
    pub crs_by_tiploc: HashMap<String, String>,
    pub operators: HashSet<String>,
    /// Stable fingerprint of the two fields above.
    pub hash: u64,
}

impl LineGeometry {
    pub fn new(crs_by_tiploc: HashMap<String, String>, operators: HashSet<String>) -> Self {
        use std::hash::{Hash, Hasher};
        let mut pairs: Vec<(&String, &String)> = crs_by_tiploc.iter().collect();
        pairs.sort();
        let mut ops: Vec<&String> = operators.iter().collect();
        ops.sort();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        pairs.hash(&mut hasher);
        ops.hash(&mut hasher);
        // Never 0, which means "no geometry".
        let hash = hasher.finish().max(1);
        Self {
            crs_by_tiploc,
            operators,
            hash,
        }
    }
}

/// line_id -> its geometry, rebuilt on every stanox/crs reload.
pub fn build_line_geometry(
    lines: &[common::LineDefinition],
    stanox_crs_records: &[common::StanoxCrsRecord],
) -> HashMap<String, Arc<LineGeometry>> {
    let crs_to_tiploc = crs_to_tiploc_map(stanox_crs_records);
    lines
        .iter()
        .map(|line| {
            let mut crs_by_tiploc = HashMap::new();
            for station in &line.stations {
                let crs = station.crs.to_uppercase();
                for tiploc in crs_to_tiploc.get(&crs).into_iter().flatten() {
                    crs_by_tiploc.insert(
                        schedule_query::normalize_tiploc(tiploc).to_string(),
                        crs.clone(),
                    );
                }
            }
            let operators = line.operators.iter().cloned().collect();
            (
                line.id.clone(),
                Arc::new(LineGeometry::new(crs_by_tiploc, operators)),
            )
        })
        .collect()
}

#[derive(serde::Deserialize)]
struct CallingPointLite {
    tiploc: String,
    #[serde(default)]
    booked_arrival: Option<chrono::NaiveTime>,
    #[serde(default)]
    booked_departure: Option<chrono::NaiveTime>,
    #[serde(default)]
    day_offset: u8,
}

/// One `schedule-line-population` wire entry, reduced on the spot -- see
/// [`parse_line_population`]. `calling_points` is only deserialized when a
/// geometry is given (serde skips it otherwise).
#[derive(serde::Deserialize)]
struct EntryLite {
    uid: String,
    #[serde(default)]
    calling_points: Vec<CallingPointLite>,
    #[serde(default)]
    operator_atoc: Option<String>,
    #[serde(default)]
    train_status: Option<char>,
}

#[derive(serde::Deserialize)]
struct EntryUidOnly {
    uid: String,
    #[serde(default)]
    train_status: Option<char>,
}

struct Candidate {
    train: LineTrain,
    operator_ok: bool,
}

struct LinePopBuilder<'a> {
    geometry: Option<&'a LineGeometry>,
    date: chrono::NaiveDate,
    pop: LinePop,
    candidates: Vec<Candidate>,
    saw_schedule_facts: bool,
}

fn utc_minutes(date: chrono::NaiveDate, day_offset: u8, time: chrono::NaiveTime) -> u32 {
    let date = date + chrono::Duration::days(i64::from(day_offset));
    let instant = common::rail_day::london_to_utc(date, time);
    u32::try_from(instant.timestamp().div_euclid(60)).unwrap_or(0)
}

impl<'a> LinePopBuilder<'a> {
    fn new(geometry: Option<&'a LineGeometry>, date: chrono::NaiveDate) -> Self {
        Self {
            geometry,
            date,
            pop: LinePop {
                geometry_hash: geometry.map_or(0, |g| g.hash),
                ..LinePop::default()
            },
            candidates: Vec::new(),
            saw_schedule_facts: false,
        }
    }

    /// Returns `false` for a bus or ship, which is left out entirely.
    fn admit(&mut self, uid: &str, train_status: Option<char>, has_operator: bool) -> bool {
        self.saw_schedule_facts |= train_status.is_some() || has_operator;
        if schedule_query::is_bus_or_ship(train_status) {
            self.pop.buses_excluded += 1;
            return false;
        }
        self.pop.uids.insert(uid.to_string());
        true
    }

    fn add(&mut self, entry: EntryLite) {
        if !self.admit(
            &entry.uid,
            entry.train_status,
            entry.operator_atoc.is_some(),
        ) {
            return;
        }
        let Some(geometry) = self.geometry else {
            return;
        };
        let mut first: Option<u32> = None;
        let mut last: Option<u32> = None;
        let mut stations: Vec<&str> = Vec::new();
        for cp in &entry.calling_points {
            let tiploc = schedule_query::normalize_tiploc(&cp.tiploc);
            let Some(crs) = geometry.crs_by_tiploc.get(tiploc) else {
                continue;
            };
            let Some(time) = cp.booked_departure.or(cp.booked_arrival) else {
                continue; // a pass, not a call
            };
            // `time` is the departure when there is one: a stop dwelling
            // across midnight departs a day after its stored (arrival)
            // day_offset (R-043).
            let day_offset = schedule_query::records::departure_day_offset(
                cp.booked_arrival,
                cp.booked_departure,
                cp.day_offset,
            );
            let minutes = utc_minutes(self.date, day_offset, time);
            first.get_or_insert(minutes);
            last = Some(minutes);
            if !stations.contains(&crs.as_str()) {
                stations.push(crs);
            }
        }
        let (Some(due_min), Some(last_due_min)) = (first, last) else {
            return;
        };
        if stations.len() < 2 {
            return;
        }
        let origin_dep_min = entry
            .calling_points
            .first()
            .and_then(|cp| {
                cp.booked_departure
                    .or(cp.booked_arrival)
                    .map(|t| utc_minutes(self.date, cp.day_offset, t))
            })
            .unwrap_or(due_min);
        let operator_ok = geometry.operators.is_empty()
            || entry
                .operator_atoc
                .as_deref()
                .is_some_and(|op| geometry.operators.contains(op));
        self.candidates.push(Candidate {
            train: LineTrain {
                uid: entry.uid.into_boxed_str(),
                due_min,
                last_due_min,
                origin_dep_min,
            },
            operator_ok,
        });
    }

    fn finish(mut self) -> LinePop {
        self.pop.relevance = if self.saw_schedule_facts {
            Relevance::Full
        } else {
            Relevance::StopsOnly
        };
        let full = self.pop.relevance == Relevance::Full;
        self.pop.trains = self
            .candidates
            .into_iter()
            .filter(|c| !full || c.operator_ok)
            .map(|c| c.train)
            .collect();
        self.pop.trains.sort_by_key(|t| t.due_min);
        self.pop.trains.shrink_to_fit();
        self.pop
    }
}

struct PopulationSeed<'a> {
    geometry: Option<&'a LineGeometry>,
    date: chrono::NaiveDate,
}

impl<'de> serde::de::DeserializeSeed<'de> for PopulationSeed<'_> {
    type Value = Option<LinePop>;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_option(self)
    }
}

impl<'de> serde::de::Visitor<'de> for PopulationSeed<'_> {
    type Value = Option<LinePop>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("null or an array of line-population entries")
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_some<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(self)
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut builder = LinePopBuilder::new(self.geometry, self.date);
        if self.geometry.is_some() {
            while let Some(entry) = seq.next_element::<EntryLite>()? {
                builder.add(entry);
            }
        } else {
            // No geometry (the windowed flag off): read uids and status only,
            // skipping every calling point -- the pre-2026-09-27 cost.
            while let Some(entry) = seq.next_element::<EntryUidOnly>()? {
                builder.admit(&entry.uid, entry.train_status, false);
            }
        }
        Ok(Some(builder.finish()))
    }
}

/// Parses one `schedule-line-population` body straight into a [`LinePop`],
/// one entry at a time: each entry's calling points are reduced to a
/// [`LineTrain`] (or nothing) and dropped as soon as they are read, so a
/// line never costs more than its body text plus its reduced trains
/// (2026-09-26/27 memory fixes). `Ok(None)` for a `null` body (nothing
/// published yet).
///
/// Rail-replacement buses (`B`/`5`) and ships (`S`/`4`) are left out
/// entirely: TRUST never reports them, so they read as cancellations all
/// day (882 of the 891 population UIDs with no TRUST message at all on
/// 2026-09-26). A population published without `train_status` (an older
/// `schedule-reference`) keeps every entry, and reads `StopsOnly`.
pub fn parse_line_population(
    body: &str,
    geometry: Option<&LineGeometry>,
    date: chrono::NaiveDate,
) -> serde_json::Result<Option<LinePop>> {
    use serde::de::DeserializeSeed;
    let mut deserializer = serde_json::Deserializer::from_str(body);
    let pop = PopulationSeed { geometry, date }.deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(pop)
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
    by_line: HashMap<String, HashMap<chrono::NaiveDate, Arc<LinePop>>>,
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

    /// Stores `(line_id, service_date)`'s uid set alone (no trains).
    #[cfg(test)]
    pub fn insert_uids(
        &mut self,
        line_id: &str,
        service_date: chrono::NaiveDate,
        uids: HashSet<String>,
        etag: Option<String>,
    ) {
        self.insert_line_pop(
            line_id,
            service_date,
            LinePop {
                uids,
                ..LinePop::default()
            },
            etag,
        );
    }

    /// Stores `(line_id, service_date)`'s population and the `ETag` it
    /// arrived with (`None` clears any previous one -- the new data is no
    /// longer described by it). What the reload calls, with the population
    /// parsed straight off the wire by [`parse_line_population`].
    pub fn insert_line_pop(
        &mut self,
        line_id: &str,
        service_date: chrono::NaiveDate,
        pop: LinePop,
        etag: Option<String>,
    ) {
        self.by_line
            .entry(line_id.to_string())
            .or_default()
            .insert(service_date, Arc::new(pop));
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

    /// [`Population::etag_for`], but only while the held population was
    /// reduced against `geometry_hash` -- after a line's stations or
    /// operators change, its population must be downloaded and reduced
    /// again, not revalidated.
    pub fn etag_if_current(
        &self,
        line_id: &str,
        service_date: chrono::NaiveDate,
        geometry_hash: u64,
    ) -> Option<&str> {
        let held = self.line_pop(line_id, service_date)?;
        if held.geometry_hash != geometry_hash {
            return None;
        }
        self.etag_for(line_id, service_date)
    }

    /// `(line_id, service_date)`'s population, if held.
    pub fn line_pop(
        &self,
        line_id: &str,
        service_date: chrono::NaiveDate,
    ) -> Option<&Arc<LinePop>> {
        self.by_line
            .get(line_id)
            .and_then(|by_date| by_date.get(&service_date))
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
        let Some(pop) = previous.line_pop(line_id, service_date) else {
            return;
        };
        self.by_line
            .entry(line_id.to_string())
            .or_default()
            .insert(service_date, Arc::clone(pop));
        let key = (line_id.to_string(), service_date);
        if let Some(etag) = previous.etags.get(&key) {
            self.etags.insert(key, etag.clone());
        }
    }

    /// Whether a population (possibly empty) is held for
    /// `(line_id, service_date)`.
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
            .is_some_and(|pop| pop.uids.contains(uid))
    }

    /// Every line whose `service_date` population contains `uid`. A scan of
    /// the ~250 lines' hash sets, paid once per Cancellation, not per
    /// Movement.
    pub fn lines_containing(&self, service_date: chrono::NaiveDate, uid: &str) -> Vec<&str> {
        self.by_line
            .iter()
            .filter(|(_, by_date)| {
                by_date
                    .get(&service_date)
                    .is_some_and(|pop| pop.uids.contains(uid))
            })
            .map(|(line_id, _)| line_id.as_str())
            .collect()
    }

    /// Total uids held across every line and date -- for the
    /// `population_uids` gauge and memory sizing.
    pub fn total_uids(&self) -> usize {
        self.by_line
            .values()
            .flat_map(|by_date| by_date.values())
            .map(|pop| pop.uids.len())
            .sum()
    }

    /// Every UID this line's population contains for `service_date`,
    /// empty if nothing has been published yet (Decision 2e's Pending
    /// case, upstream of the rail-day gate).
    pub fn uids_for(&self, line_id: &str, service_date: chrono::NaiveDate) -> Vec<&str> {
        self.by_line
            .get(line_id)
            .and_then(|by_date| by_date.get(&service_date))
            .map(|pop| pop.uids.iter().map(String::as_str).collect())
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
    /// `api` serves them) into uids alone when no geometry is given;
    /// `null` (nothing published) still parses as `None`.
    #[test]
    fn the_real_wire_shape_parses_into_uids_without_a_geometry() {
        let body = r#"[{"uid": "W45448", "calling_points": [{"kind": "Origin", "tiploc": "THBDGS ",
            "activity": "TB", "platform": "1", "day_offset": 0, "booked_arrival": null,
            "public_arrival": null, "booked_departure": "17:28:00", "public_departure": "17:28:00",
            "is_half_minute_arrival": false, "is_half_minute_departure": false}]},
            {"uid": "W45449", "calling_points": []}]"#;
        let date: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let pop = parse_line_population(body, None, date).unwrap().unwrap();
        let mut uids: Vec<&str> = pop.uids.iter().map(String::as_str).collect();
        uids.sort_unstable();
        assert_eq!(uids, vec!["W45448", "W45449"]);
        assert!(pop.trains.is_empty());
        assert_eq!(pop.relevance, Relevance::StopsOnly);
        assert!(parse_line_population("null", None, date).unwrap().is_none());
        assert!(parse_line_population("[1]", None, date).is_err());
    }

    // --- 2026-09-27: windowed stats, due times and relevance ---

    fn geometry(stations: &[(&str, &str)], operators: &[&str]) -> LineGeometry {
        LineGeometry::new(
            stations
                .iter()
                .map(|(t, c)| (t.to_string(), c.to_string()))
                .collect(),
            operators.iter().map(|o| o.to_string()).collect(),
        )
    }

    fn cp(tiploc: &str, arr: Option<&str>, dep: Option<&str>, day_offset: u8) -> serde_json::Value {
        serde_json::json!({"kind": "Intermediate", "tiploc": format!("{tiploc:<7}"),
            "booked_arrival": arr, "booked_departure": dep, "day_offset": day_offset,
            "is_half_minute_arrival": false, "is_half_minute_departure": false})
    }

    fn entry(
        uid: &str,
        op: Option<&str>,
        status: Option<&str>,
        cps: Vec<serde_json::Value>,
    ) -> serde_json::Value {
        let mut e = serde_json::json!({"uid": uid, "calling_points": cps});
        if let Some(op) = op {
            e["operator_atoc"] = op.into();
        }
        if let Some(status) = status {
            e["train_status"] = status.into();
        }
        e
    }

    fn parse(entries: Vec<serde_json::Value>, g: &LineGeometry, date: &str) -> LinePop {
        let body = serde_json::Value::Array(entries).to_string();
        parse_line_population(&body, Some(g), date.parse().unwrap())
            .unwrap()
            .unwrap()
    }

    fn minutes(instant: &str) -> u32 {
        (instant
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap()
            .timestamp()
            / 60) as u32
    }

    /// Due is the first call at a line station -- its departure, or its
    /// arrival when it has none -- in UTC (BST here); last_due the last;
    /// origin the schedule's first calling point.
    #[test]
    fn due_is_the_first_line_call_and_times_are_utc() {
        let g = geometry(&[("LLJ", "LLJ"), ("BFF", "BFF")], &["AW"]);
        let pop = parse(
            vec![entry(
                "C1",
                Some("AW"),
                Some("P"),
                vec![
                    cp("CREWE", None, Some("07:00:00"), 0),
                    cp("LLJ", Some("08:10:00"), Some("08:12:00"), 0),
                    cp("BFF", Some("09:05:00"), None, 0),
                ],
            )],
            &g,
            "2026-07-15",
        );
        assert_eq!(pop.relevance, Relevance::Full);
        assert_eq!(
            pop.trains,
            vec![LineTrain {
                uid: "C1".into(),
                due_min: minutes("2026-07-15T07:12:00Z"),
                last_due_min: minutes("2026-07-15T08:05:00Z"),
                origin_dep_min: minutes("2026-07-15T06:00:00Z"),
            }]
        );

        // GMT, and after-midnight calls (day_offset 1).
        let pop = parse(
            vec![entry(
                "C2",
                Some("AW"),
                Some("1"),
                vec![
                    cp("CREWE", None, Some("23:30:00"), 0),
                    cp("LLJ", Some("00:40:00"), Some("00:41:00"), 1),
                    cp("BFF", Some("01:20:00"), None, 1),
                ],
            )],
            &g,
            "2026-01-15",
        );
        assert_eq!(pop.trains[0].due_min, minutes("2026-01-16T00:41:00Z"));
        assert_eq!(pop.trains[0].last_due_min, minutes("2026-01-16T01:20:00Z"));
        assert_eq!(
            pop.trains[0].origin_dep_min,
            minutes("2026-01-15T23:30:00Z")
        );

        // Arrival only at the first line station.
        let pop = parse(
            vec![entry(
                "C3",
                Some("AW"),
                Some("P"),
                vec![
                    cp("LLJ", Some("10:00:00"), None, 0),
                    cp("BFF", Some("10:30:00"), None, 0),
                ],
            )],
            &g,
            "2026-01-15",
        );
        assert_eq!(pop.trains[0].due_min, minutes("2026-01-15T10:00:00Z"));
    }

    #[test]
    fn buses_other_operators_and_one_station_trains_are_not_relevant() {
        let g = geometry(&[("LLJ", "LLJ"), ("BFF", "BFF")], &["AW"]);
        let calls = || {
            vec![
                cp("LLJ", None, Some("08:00:00"), 0),
                cp("BFF", Some("09:00:00"), None, 0),
            ]
        };
        let pop = parse(
            vec![
                entry("TRAIN", Some("AW"), Some("P"), calls()),
                entry("BUS", Some("AW"), Some("5"), calls()),
                entry("OTHEROP", Some("XC"), Some("P"), calls()),
                entry(
                    "ONESTN",
                    Some("AW"),
                    Some("P"),
                    vec![
                        cp("LLJ", None, Some("08:00:00"), 0),
                        cp("CREWE", Some("09:00:00"), None, 0),
                    ],
                ),
                entry(
                    "PASSES",
                    Some("AW"),
                    Some("P"),
                    vec![
                        cp("LLJ", None, Some("08:00:00"), 0),
                        cp("BFF", None, None, 0),
                    ],
                ),
            ],
            &g,
            "2026-07-15",
        );
        assert_eq!(
            pop.trains.iter().map(|t| &*t.uid).collect::<Vec<_>>(),
            vec!["TRAIN"]
        );
        assert!(
            !pop.uids.contains("BUS"),
            "a bus is not in the population at all"
        );
        assert!(
            pop.uids.contains("OTHEROP"),
            "matching still sees every train"
        );
        assert_eq!(pop.buses_excluded, 1);
    }

    /// An older schedule-reference: no operator or status anywhere.
    #[test]
    fn a_population_without_schedule_facts_is_stops_only() {
        let g = geometry(&[("LLJ", "LLJ"), ("BFF", "BFF")], &["AW"]);
        let pop = parse(
            vec![entry(
                "C1",
                None,
                None,
                vec![
                    cp("LLJ", None, Some("08:00:00"), 0),
                    cp("BFF", Some("09:00:00"), None, 0),
                ],
            )],
            &g,
            "2026-07-15",
        );
        assert_eq!(pop.relevance, Relevance::StopsOnly);
        assert_eq!(pop.trains.len(), 1, "the two-stations rule still applies");
    }

    /// Memory regression: a line the size of the largest production one
    /// (3,357 entries x 22 calls) reduces to fixed-size trains with no
    /// calling points retained -- checked by type size, not allocator.
    #[test]
    fn a_large_line_body_reduces_to_no_calling_points() {
        let stations: Vec<(String, String)> = (0..22)
            .map(|i| (format!("T{i:05}"), format!("C{i:02}")))
            .collect();
        let g = LineGeometry::new(
            stations.iter().cloned().collect(),
            ["LM".to_string()].into_iter().collect(),
        );
        let entries: Vec<serde_json::Value> = (0..3357)
            .map(|n| {
                entry(
                    &format!("U{n:05}"),
                    Some("LM"),
                    Some("P"),
                    (0..22)
                        .map(|i| {
                            let t = format!("{:02}:{:02}:00", 6 + i / 2, (i % 2) * 30);
                            cp(&stations[i].0, Some(&t), Some(&t), 0)
                        })
                        .collect(),
                )
            })
            .collect();
        let pop = parse(entries, &g, "2026-09-27");
        assert_eq!(pop.trains.len(), 3357);
        assert!(std::mem::size_of::<LineTrain>() <= 32);
        let resident = pop.trains.capacity() * std::mem::size_of::<LineTrain>()
            + pop.trains.iter().map(|t| t.uid.len()).sum::<usize>();
        assert!(resident < 3357 * 40, "{resident} bytes");
    }

    #[test]
    fn line_geometry_is_resolved_through_stanox_crs_and_fingerprinted() {
        let lines = vec![fixture_line("line-a", &["SHR", "ZZA"])];
        let records = vec![
            fixture_stanox_crs_record("SHR", "SHARED"),
            fixture_stanox_crs_record("ZZA", "ONLY_A"),
        ];
        let geometry = build_line_geometry(&lines, &records);
        let g = &geometry["line-a"];
        assert_eq!(
            g.crs_by_tiploc.get("SHARED").map(String::as_str),
            Some("SHR")
        );
        assert_ne!(g.hash, 0);
        let again = build_line_geometry(&lines, &records);
        assert_eq!(again["line-a"].hash, g.hash);
        let fewer = build_line_geometry(&lines, &records[..1]);
        assert_ne!(fewer["line-a"].hash, g.hash);
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
                operator_atoc: None,
                train_status: None,
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
            operator_atoc: None,
            train_status: None,
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
                operator_atoc: None,
                train_status: None,
            }],
        );

        assert_eq!(
            population.uids_for("waterloo-reading", date),
            vec!["C11052"],
            "membership must still work"
        );

        let pop = population
            .by_line
            .get("waterloo-reading")
            .and_then(|by_date| by_date.get(&date))
            .expect("just inserted");
        assert_eq!(pop.uids.len(), 1);
        assert!(pop.uids.contains("C11052"));
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
