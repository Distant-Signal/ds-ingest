# `stanox-crs.csv` provenance

`stanox-crs.csv` maps a TRUST Train Movements STANOX (`loc_stanox`) to its
National Rail CRS code, for `crates/trust-consumer/src/stanox_crs.rs`'s
loader. This file is the full extraction methodology and exclusion policy
the data was generated under; the Rust module's doc comment only summarises
and points here.

## Why this mapping is needed

TRUST movement messages (`0003`) carry a `loc_stanox` -- a plain,
zero-padded 5-digit numeric location code (confirmed real, e.g. Euston's
STANOX is `"72410"` -- see
docs/superpowers/specs/2026-08-29-trust-schedule-delay-validation-findings.md's
final section). Everything else in this codebase that identifies a station
(`common::StationReference`, a pin's `pin_origin_crs`) uses the 3-letter
National Rail CRS code instead (`"EUS"`), so `process.rs` needs a
STANOX->CRS table to bridge the two.

## Where the data comes from

This crate ships no live CIF feed connection (that's a separate, larger,
unbuilt ingestion pipeline -- see
docs/superpowers/specs/2026-08-30-schedule-feed-ingress-design.md). Instead,
`stanox-crs.csv` is a **generated, checked-in snapshot**, extracted once
from a real CIF full-timetable extract already available in this repo's
example payload: `timetable_full.zip` (not committed to git -- 73MB
compressed/~711MB uncompressed -- kept out-of-repo; regenerate from a fresh
extract using the recipe below if it's ever refreshed).

The task that produced this table was originally pointed at
`RJTTF942MSN.txt` (the CIF "Master Station Names" file's `'A'` station
records), on the assumption that its two trailing 5-digit numeric fields
were a STANOX (and/or an Ordnance Survey Easting/Northing pair). That
assumption did **not** survive verification against this repo's own real
data: searching the raw MSN line for Euston (streamed via
`unzip -p timetable_full.zip RJTTF942MSN.txt`) for the substring `"72410"`
-- Euston's STANOX, confirmed independently by this codebase's own
validation findings doc -- finds nothing. Byte-for-byte, Euston's real
`'A'` record is:

```text
A    LONDON EUSTON                 3EUSTON EUS   EUS15295 6182715
```

whatever `15295`/`61827`/`15` are, none of them is `72410`, so MSN's `'A'`
record is not this table's source (its trailing fields are left
unidentified here rather than guessed at).

What **does** carry a verified STANOX, at a fixed, cross-checked column
offset, is the `TI` (TIPLOC Insert) record type inside the same zip's main
CIF schedule extract, `RJTTF942MCA.txt`. Euston's real `TI` record (80
bytes, confirmed via direct byte-offset inspection of the file --
`unzip -p timetable_full.zip RJTTF942MCA.txt | grep '^TIEUSTON'`):

```text
TIEUSTON 00144400NLONDON EUSTON             724102893EUSLONDON EUSTON
```

contains the literal substring `72410` immediately followed by `2893EUS` --
STANOX, an unidentified 4-digit field, then the CRS code. Column offsets
(0-indexed, verified against ~12,000 real `TI` lines, every one exactly 80
bytes, boundaries stable regardless of TIPLOC length or content):

| Bytes    | Field                                                    |
|----------|-----------------------------------------------------------|
| `0..2`   | Record type, always `"TI"`                               |
| `2..9`   | TIPLOC code (7, space-padded)                            |
| `9..17`  | Unidentified 8-char field (not needed here)              |
| `17..18` | Unidentified 1-char field (not needed here)              |
| `18..44` | TPS description / station name (26, space-padded)       |
| `44..49` | **STANOX code (5 digits, zero-padded)**                  |
| `49..53` | Unidentified 4-char field (not needed here)              |
| `53..56` | **CRS code (3 letters, blank if this TIPLOC has none)**  |
| `56..80` | Fuller description (24, space-padded)                    |

`wiki.openraildata.com` (the canonical citation for this record format)
returned HTTP 403 to every fetch attempt made while building this table
(direct fetch, `action=raw`, and a text-proxy fetch were all blocked at the
edge -- not a page-specific block, the bare domain root 403'd too), so the
layout above is verified against the real file's own bytes instead, per
this task's own fallback allowance: cross-checked against ~12,000 real `TI`
lines (every one exactly 80 bytes, field boundaries constant regardless of
TIPLOC length or content) and spot-verified against six independently-known
real stations (Euston/EUS, King's Cross/KGX, Aberdeen/ABD, Glasgow
Central/GLC, Bristol Temple Meads/BRI, Leeds/LDS) plus the STANOX-sharing
case documented below.

## Extraction and exclusion policy

Of 12,085 real `TI` records in the 2026-08-28 extract:
- 941 carry no STANOX (blank or the `"00000"` sentinel) -- excluded.
- 8,510 carry no CRS (signals, junctions, sidings, and other non-passenger
  locations genuinely have no booking-office code) -- excluded; this is
  expected and correct, not a data-quality problem.
- Of the 3,129 distinct STANOX values left with at least one CRS, most map
  to exactly one CRS. **14 STANOX values map to more than one**, because
  one physical STANOX area covers multiple TIPLOCs that each carry their
  own CRS (e.g. a station's below-ground/high-level split, or a passenger
  platform sharing a STANOX with an adjacent sidings/depot TIPLOC). Real
  example: STANOX `87201` covers both TIPLOC `VICTRIA` (CRS `VIC`, London
  Victoria's real passenger code) and TIPLOC `VICTRCR` (CRS `XVR`, a
  non-passenger pseudo-code -- `X`-prefixed CRS codes are Network Rail's
  convention for non-bookable locations). For these, this table prefers the
  sole non-`X`-prefixed candidate, since a user's pin is created against a
  genuine bookable CRS, never a pseudo-code. That resolves 9 of the 14
  cleanly. The remaining 5 have either two genuine non-`X` candidates or
  two `X`-prefixed ones (STANOX `89428`, `87981`, `89530`, `86935`,
  `52215`) -- with no principled way to prefer one over the other, these
  are **excluded entirely** rather than guessed at; a lookup miss for one
  of these five is treated exactly like any other unmapped STANOX (see
  `stanox_crs`'s doc comment). This leaves 3,124 confidently-resolved rows
  in `stanox-crs.csv`.

## File format

`stanox-crs.csv` is a plain headered, comma-delimited file. The first line
names its columns (`stanox,crs`); every following line is one row, parsed
by column *name* (not position) -- see `stanox_crs::StanoxCrsTable::parse`.
This is deliberate: a future code type (e.g. TIPLOC) can be added as a new
named column (`stanox,crs,tiploc`) without changing the parser's shape,
and without every existing row needing to carry a value for it if the
loader treats a missing/empty cell as absent for that column. Rows are
sorted by `stanox` for a reviewable diff.

## Regenerating this table

1. `unzip -p timetable_full.zip RJTTF942MCA.txt | grep '^TI' > ti.txt` (do
   not extract the whole 711MB archive to disk).
2. For each 80-byte line, take `stanox = line[44..49].trim()` and
   `crs = line[53..56].trim()`, skipping any where `stanox` is empty or
   `"00000"`, or `crs` is empty.
3. Group by `stanox`; where more than one distinct `crs` maps to the same
   `stanox`, apply the exclusion policy above.
4. Regenerate `stanox-crs.csv`: write the header `stanox,crs`, then one
   `stanox,crs` line per resolved entry, sorted by `stanox`.
