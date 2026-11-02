# Reference data for the TRUST consumers: sources and terms

The code in this repository is licensed AGPL-3.0-or-later (`LICENSE`).
Some files here are derived from upstream data, which stays under its own
terms. This file lists each file's source and terms (repo split spec §6,
"Data is separate from code"). The site credits the same sources on
`/attribution` (`frontend/components/OpenDataAttribution.tsx`). The other
reference data files, and their terms, are listed in `reference-data/README.md`.

| File | Source | Terms |
|---|---|---|
| `stanox-crs.csv` | The CIF timetable (Master Station Names and schedule `TI` records). Provenance: `stanox-crs.md`. | Rail Settlement Plan timetable data licence ("Source: RSP"). |

`trust-consumer` and `trust-backlog-consumer` both read `stanox-crs.csv`
(their images copy it to `/app/reference-data/stanox-crs.csv`).
`stanox-crs.md` is this repository's own documentation (AGPL-3.0-or-later).
