//! STANOX->CRS translation for the TRUST Train Movements feed.
//!
//! TRUST movement messages (`0003`) carry a `loc_stanox` -- a plain,
//! zero-padded 5-digit numeric location code. Everything else in this
//! codebase that identifies a station (`common::StationReference`, a pin's
//! `pin_origin_crs`) uses the 3-letter National Rail CRS code instead
//! (`"EUS"`), so `process.rs` needs a STANOX->CRS table to bridge the two.
//! This module loads that table at startup and exposes the translation.
//!
//! # Where the data lives
//!
//! Two tiers, in order:
//!
//! 1. **Startup / fallback**: `reference-data/stanox-crs.csv`, loaded once
//!    at process startup via `StanoxCrsTable::from_file` -- the
//!    `--stanox-crs-file`/`STANOX_CRS_FILE` flag in `config.rs`, unchanged
//!    since before the live table existed. This remains the checked-in
//!    default for local dev and any environment without the schedule-feed
//!    pipeline deployed, and the value this crate falls back to (and keeps
//!    indefinitely) if the live table below is ever empty, unreachable, or
//!    a fresh environment has never had `crates/schedule-reference`
//!    successfully run.
//! 2. **Live reload**: once `crates/schedule-reference` (a sibling
//!    container to `schedule-ingest`, see
//!    docs/superpowers/specs/2026-09-01-schedule-ingest-stanox-crs-table-design.md)
//!    has successfully parsed a CIF SCHEDULE delivery, this crate's main
//!    loop periodically (`--stanox-crs-reload-secs`) `GET`s
//!    `/private/stanox-crs` and swaps a fresh `StanoxCrsTable::from_records`
//!    into a shared `std::sync::RwLock` cell (see `main.rs`'s second
//!    reload block, alongside its existing tracked-trains reload). A
//!    failed or empty reload never clears the currently-loaded table --
//!    see `process::apply_stanox_crs_reload`.
//!
//! **Full provenance for the CSV specifically** -- exactly how it was
//! extracted, the record format's byte offsets, and the documented
//! exclusion policy for ambiguous STANOX values -- lives in
//! `reference-data/stanox-crs.md`. The live table applies the identical
//! exclusion policy, reimplemented as real code in
//! `crates/schedule-reference/src/parser.rs`.
//!
//! A lookup miss (`None`) is the honest "we don't know" case, not an
//! error: it covers both genuinely non-passenger locations (signals,
//! junctions, sidings, depots have no CRS to begin with) and this
//! snapshot's small set of deliberately-excluded ambiguous STANOX values
//! (see `reference-data/stanox-crs.md`). The caller (`process.rs`) already
//! falls back to the raw STANOX for display (see
//! `journey::apply_movement`), and a pin simply won't match on that event
//! -- exactly as if the location were untranslatable, which today it
//! always was.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, bail};

/// One row of `stanox-crs.csv`, i.e. one STANOX's resolved CRS.
///
/// Deliberately a plain struct rather than a positional tuple: `parse`
/// looks up each field by its column *name* in the header, so adding a
/// future column (e.g. `tiploc`) means adding one more named lookup and
/// one more field here -- the parsing shape itself doesn't change.
struct StanoxCrsRecord {
    stanox: String,
    crs: String,
}

/// The loaded STANOX->CRS table, built once at startup from
/// `reference-data/stanox-crs.csv` (see this module's doc comment).
///
/// Holds owned `String`s rather than `&'static str`s, since the data is no
/// longer a compile-time literal -- same choice `common::LineDefinition`
/// makes for its own fields. `stanox_to_crs` is called once per Movement
/// message, not in a hot loop, so the small extra allocation per lookup
/// this implies is not worth avoiding via `Box::leak`.
#[derive(Debug, Clone, Default)]
pub struct StanoxCrsTable {
    by_stanox: HashMap<String, String>,
}

impl StanoxCrsTable {
    /// Loads and parses `path` (see `parse` for the file format). Fails
    /// loudly -- returns `Err`, never a silently-empty table -- if the file
    /// is missing or malformed, matching this codebase's "config load
    /// fails fast at startup" posture (see `common::LineDefinition::from_file`
    /// and `crates/aggregator/src/config.rs`'s `parse_lines`).
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading STANOX->CRS table at {}", path.display()))?;
        Self::parse(&text)
            .with_context(|| format!("parsing STANOX->CRS table at {}", path.display()))
    }

    /// Parses a headered, comma-delimited STANOX->CRS table.
    ///
    /// The header line names columns; every row is parsed by looking up
    /// `"stanox"` and `"crs"` by column *name* in that header, not by
    /// fixed position. This is what keeps the format genuinely extensible:
    /// a future `tiploc` (or other code type) column can be inserted
    /// anywhere in the header, in any order, without this parser changing
    /// shape -- it would just gain one more named lookup alongside these
    /// two.
    ///
    /// A hand-rolled split on `,` is deliberate, not a missing dependency:
    /// STANOX and CRS values are short, fixed-shape alphanumeric codes with
    /// no embedded commas/quoting to worry about, so pulling in the `csv`
    /// crate (not used anywhere else in this workspace) for this would be
    /// more machinery than the data needs.
    fn parse(text: &str) -> anyhow::Result<Self> {
        let mut lines = text.lines();
        let header = lines
            .next()
            .context("STANOX->CRS table is empty (missing header line)")?;
        let columns: Vec<&str> = header.split(',').map(str::trim).collect();

        let column_index = |name: &str| -> anyhow::Result<usize> {
            columns.iter().position(|&c| c == name).with_context(|| {
                format!("STANOX->CRS table header is missing required column {name:?}: {header:?}")
            })
        };
        let stanox_col = column_index("stanox")?;
        let crs_col = column_index("crs")?;

        let mut by_stanox = HashMap::new();
        for (offset, line) in lines.enumerate() {
            let line_no = offset + 2; // +1 for the header, +1 for 1-indexing
            if line.trim().is_empty() {
                continue;
            }

            let fields: Vec<&str> = line.split(',').collect();
            let record = StanoxCrsRecord {
                stanox: field_at(&fields, stanox_col, line_no, "stanox")?
                    .trim()
                    .to_string(),
                crs: field_at(&fields, crs_col, line_no, "crs")?
                    .trim()
                    .to_string(),
            };

            if record.stanox.is_empty() {
                bail!("STANOX->CRS table row {line_no} has an empty stanox: {line:?}");
            }
            if record.crs.is_empty() {
                bail!("STANOX->CRS table row {line_no} has an empty crs: {line:?}");
            }
            if let Some(previous) = by_stanox.insert(record.stanox.clone(), record.crs) {
                bail!(
                    "STANOX->CRS table row {line_no} duplicates stanox {:?} (previously {previous:?})",
                    record.stanox
                );
            }
        }

        Ok(Self { by_stanox })
    }

    /// Builds a table directly from `api`'s `GET /private/stanox-crs`
    /// response rows -- the live-reload counterpart to `from_file`/`parse`.
    /// `tiploc`/`station_name`/`source_sequence` are not needed for
    /// lookup and are dropped here; only `stanox`/`crs` matter to
    /// `stanox_to_crs`.
    pub fn from_records(records: Vec<common::StanoxCrsRecord>) -> Self {
        let by_stanox = records.into_iter().map(|r| (r.stanox, r.crs)).collect();
        Self { by_stanox }
    }

    /// Translates a TRUST Movement's `loc_stanox` to a National Rail CRS
    /// code. Returns `None` when the STANOX isn't in the table -- see this
    /// module's doc comment for what that honestly means.
    ///
    /// Input is trimmed before lookup, and short-padded with leading zeros
    /// if needed, so a caller that hasn't zero-padded a short STANOX (e.g.
    /// `"2071"` instead of `"02071"`) still resolves; TRUST's real feed
    /// sends fixed 5-digit zero-padded strings, so this is defensive
    /// rather than load-bearing.
    pub fn stanox_to_crs(&self, stanox: &str) -> Option<String> {
        let trimmed = stanox.trim();
        if let Some(crs) = self.by_stanox.get(trimmed) {
            return Some(crs.clone());
        }
        if !trimmed.is_empty() && trimmed.len() < 5 && trimmed.chars().all(|c| c.is_ascii_digit()) {
            let padded = format!("{trimmed:0>5}");
            return self.by_stanox.get(padded.as_str()).cloned();
        }
        None
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.by_stanox.len()
    }
}

/// Looks up `fields[index]`, turning a too-short row into a named parse
/// error instead of an `unwrap` panic on genuinely malformed input.
fn field_at<'a>(
    fields: &[&'a str],
    index: usize,
    line_no: usize,
    column: &str,
) -> anyhow::Result<&'a str> {
    fields.get(index).copied().with_context(|| {
        format!("STANOX->CRS table row {line_no} is missing its {column:?} column")
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// The real, checked-in data file -- not a fixture copy -- mirroring
    /// how `crates/aggregator`/`crates/api`'s own tests load the real
    /// `lines/` directory directly (e.g. `crates/aggregator/src/segments.rs`'s
    /// `load_all_lines`), rather than a synthetic stand-in.
    fn load_real_table() -> StanoxCrsTable {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../reference-data/stanox-crs.csv");
        StanoxCrsTable::from_file(&path).expect("reference-data/stanox-crs.csv should parse")
    }

    /// Real `TI` lines from `timetable_full.zip`'s `RJTTF942MCA.txt`
    /// (2026-08-28 extract), quoted verbatim (byte-for-byte, confirmed via
    /// direct extraction, not retyped from a summary) rather than only
    /// exercised through the generated data file, so this test would catch
    /// a mistake in either the extraction script or the file's contents,
    /// not just re-assert whatever the file already says.
    const REAL_EUSTON: &str =
        "TIEUSTON 00144400NLONDON EUSTON             724102893EUSLONDON EUSTON           ";
    const REAL_KINGS_CROSS: &str =
        "TIKNGX   00612100QLONDON KINGS CROSS        543112901KGXLONDON KINGS X          ";
    const REAL_ABERDEEN: &str =
        "TIABRDEEN00897600GABERDEEN                  020712800ABDABERDEEN                ";
    const REAL_GLASGOW_CENTRAL: &str =
        "TIGLGC   04981300TGLASGOW CENTRAL           072572857GLCGLASGOW CENTRAL         ";
    /// The STANOX-sharing case `reference-data/stanox-crs.md` documents:
    /// `87201` is shared by London Victoria's real passenger CRS (`VIC`,
    /// this line)...
    const REAL_VICTORIA: &str =
        "TIVICTRIA00542600PLONDON VICTORIA           87201   0VICLONDON VICTORIA         ";
    /// ...and a real non-passenger `XVR` TIPLOC at the very same STANOX.
    const REAL_VICTORIA_CARRIAGE_ROAD: &str =
        "TIVICTRCR48542662MVICTORIA CARRIAGE ROAD    87201   0XVR                        ";

    fn decode(real_ti_line: &str) -> (&str, &str) {
        (real_ti_line[44..49].trim(), real_ti_line[53..56].trim())
    }

    #[test]
    fn decodes_stanox_and_crs_at_the_verified_real_column_offsets() {
        assert_eq!(decode(REAL_EUSTON), ("72410", "EUS"));
        assert_eq!(decode(REAL_KINGS_CROSS), ("54311", "KGX"));
        assert_eq!(decode(REAL_ABERDEEN), ("02071", "ABD"));
        assert_eq!(decode(REAL_GLASGOW_CENTRAL), ("07257", "GLC"));
        assert_eq!(decode(REAL_VICTORIA), ("87201", "VIC"));
        assert_eq!(decode(REAL_VICTORIA_CARRIAGE_ROAD), ("87201", "XVR"));
    }

    #[test]
    fn translates_real_known_stations() {
        let table = load_real_table();
        assert_eq!(
            table.stanox_to_crs("72410"),
            Some("EUS".to_string()),
            "Euston"
        );
        assert_eq!(
            table.stanox_to_crs("54311"),
            Some("KGX".to_string()),
            "King's Cross"
        );
        assert_eq!(
            table.stanox_to_crs("02071"),
            Some("ABD".to_string()),
            "Aberdeen"
        );
        assert_eq!(
            table.stanox_to_crs("07257"),
            Some("GLC".to_string()),
            "Glasgow Central"
        );
        assert_eq!(
            table.stanox_to_crs("81700"),
            Some("BRI".to_string()),
            "Bristol Temple Meads"
        );
        assert_eq!(
            table.stanox_to_crs("17132"),
            Some("LDS".to_string()),
            "Leeds"
        );
    }

    #[test]
    fn from_records_builds_a_table_usable_by_stanox_to_crs() {
        let records = vec![common::StanoxCrsRecord {
            stanox: "72410".to_string(),
            crs: "EUS".to_string(),
            tiploc: "EUSTON".to_string(),
            station_name: "LONDON EUSTON".to_string(),
            source_sequence: 942,
        }];
        let table = StanoxCrsTable::from_records(records);
        assert_eq!(table.stanox_to_crs("72410"), Some("EUS".to_string()));
        assert_eq!(table.stanox_to_crs("99999"), None);
    }

    #[test]
    fn a_shared_stanox_resolves_to_the_real_passenger_crs_not_the_pseudo_code() {
        // 87201 is genuinely shared by TIPLOC VICTRIA (CRS VIC, a real
        // bookable station) and TIPLOC VICTRCR (CRS XVR, a non-passenger
        // pseudo-code) in the real data -- see reference-data/stanox-crs.md
        // and the two REAL_VICTORIA* fixtures above.
        let table = load_real_table();
        assert_eq!(table.stanox_to_crs("87201"), Some("VIC".to_string()));
    }

    #[test]
    fn an_unknown_stanox_translates_to_none_not_a_panic() {
        let table = load_real_table();
        assert_eq!(table.stanox_to_crs("00000"), None);
        assert_eq!(table.stanox_to_crs("99999"), None);
        assert_eq!(table.stanox_to_crs("not-a-stanox"), None);
        assert_eq!(table.stanox_to_crs(""), None);
    }

    #[test]
    fn an_ambiguous_excluded_stanox_translates_to_none() {
        // 89428 real-world maps to two distinct real (non-X) CRS codes
        // across two TIPLOCs (an Ashford-area station cluster) with no
        // principled way to pick one -- deliberately excluded from the
        // data file rather than guessed at. See reference-data/stanox-crs.md.
        let table = load_real_table();
        assert_eq!(table.stanox_to_crs("89428"), None);
    }

    #[test]
    fn a_short_unpadded_stanox_still_resolves() {
        let table = load_real_table();
        assert_eq!(table.stanox_to_crs("2071"), Some("ABD".to_string()));
    }

    #[test]
    fn the_real_data_file_has_3124_entries() {
        assert_eq!(load_real_table().len(), 3124);
    }

    #[test]
    fn every_real_entry_is_a_five_digit_stanox_and_a_three_letter_crs() {
        let table = load_real_table();
        for (stanox, crs) in &table.by_stanox {
            assert_eq!(stanox.len(), 5, "STANOX must be 5 digits: {stanox}");
            assert!(
                stanox.chars().all(|c| c.is_ascii_digit()),
                "non-digit STANOX: {stanox}"
            );
            assert_eq!(crs.len(), 3, "CRS must be 3 letters: {crs}");
            assert!(
                crs.chars().all(|c| c.is_ascii_uppercase()),
                "non-uppercase CRS: {crs}"
            );
        }
    }

    #[test]
    fn parse_rejects_a_header_missing_the_crs_column() {
        let err = StanoxCrsTable::parse("stanox\n72410\n").unwrap_err();
        assert!(
            format!("{err:#}").contains("crs"),
            "error should name the missing column: {err:#}"
        );
    }

    #[test]
    fn parse_rejects_a_duplicate_stanox() {
        let err = StanoxCrsTable::parse("stanox,crs\n72410,EUS\n72410,EUS\n").unwrap_err();
        assert!(
            format!("{err:#}").contains("duplicates"),
            "error should mention the duplicate: {err:#}"
        );
    }

    #[test]
    fn parse_rejects_an_empty_crs() {
        let err = StanoxCrsTable::parse("stanox,crs\n72410,\n").unwrap_err();
        assert!(
            format!("{err:#}").contains("empty crs"),
            "error should mention the empty crs: {err:#}"
        );
    }

    #[test]
    fn parse_skips_blank_lines() {
        let table = StanoxCrsTable::parse("stanox,crs\n72410,EUS\n\n54311,KGX\n").unwrap();
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn parse_tolerates_columns_in_a_different_order() {
        // Named-column lookup, not positional -- so a header with `crs`
        // before `stanox` still works, which is the property that makes
        // adding a future column order-independent too.
        let table = StanoxCrsTable::parse("crs,stanox\nEUS,72410\n").unwrap();
        assert_eq!(table.stanox_to_crs("72410"), Some("EUS".to_string()));
    }
}
