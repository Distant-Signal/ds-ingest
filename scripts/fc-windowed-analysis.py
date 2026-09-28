#!/usr/bin/env python3
# ruff: noqa: T201  # a report generator: printing to stdout is its whole output
r"""Read-only analysis behind the full-coverage windowed-stats design.

The design is
docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md.

Works on CSV exports of production tables; it never connects to anything.
The exports were taken with plain SELECTs (\copy ... TO STDOUT), e.g.

  K='kubectl --context mine-bringer-ts -n distant-signal exec -i'
  K="$K distant-signal-postgres-0 -- psql -U distant_signal -d distant_signal -c"
  $K "\copy (SELECT train_id, train_uid, service_date, msg_type, event_type, crs,
               planned_timestamp, actual_timestamp, variation_status,
               delay_minutes, received_at
             FROM trust_event_backlog WHERE service_date >= '2026-09-25')
             TO STDOUT WITH CSV HEADER" > teb.csv
  $K "\copy (SELECT service_date, uid, seq, tiploc, kind, booked_arrival,
               booked_departure, day_offset
             FROM schedule_calling_points_full
             WHERE service_date IN ('2026-09-26','2026-09-27'))
             TO STDOUT WITH CSV HEADER" > scp.csv
  $K "\copy (SELECT line_id, service_date, e->>'uid' FROM schedule_line_population,
             jsonb_array_elements(population) e
             WHERE service_date IN ('2026-09-26','2026-09-27'))
             TO STDOUT WITH CSV HEADER" > pop.csv
  $K "\copy (SELECT stanox, crs, tiploc FROM stanox_crs)
             TO STDOUT WITH CSV HEADER" > stanox.csv
  $K "\copy (SELECT DISTINCT ON (service_date, train_uid) service_date, train_uid,
               operator_atoc, headcode
             FROM schedule_destination_departures
             WHERE service_date IN ('2026-09-26','2026-09-27')
             ORDER BY service_date, train_uid) TO STDOUT WITH CSV HEADER" > ops.csv
  $K "\copy (SELECT line_id, half_hour_start, sample_cycles, total, delayed, cancelled
             FROM line_status_half_hourly_stats
             WHERE half_hour_start >= '2026-09-26 01:00Z'
             AND half_hour_start < '2026-09-27 01:00Z')
             TO STDOUT WITH CSV HEADER" > ldbws_hh.csv

Optional: FC_BS=<file of CIF "BS" records> (grep '^BS' RJTTF971MCA.txt from the
schedulefeed pod's /data/schedule-feed) excludes bus/ship schedules (Train
Status B/5/S/4) from every population, which is what the design recommends.
FC_RELEVANCE picks the line-relevance filter (default op_calls2).

Usage: fc-windowed-analysis.py <export-dir> <lines-dir> [section ...]
Sections: relevance, volume, outcomes, lag, simulate, simulate2, perline, escalate,
ldbws (default: relevance volume outcomes lag simulate ldbws). `simulate` infers
lateness from silence (the rejected variant, design §3.5); simulate2/perline/escalate
implement the design's §4.3.2 rules (overdue_margin = infinity, pending excluded).

Also used (ad hoc, same exports): hist.csv / hist0.csv from line_status_history
(statuses exploded: line_id, computed_at, severity, data_quality, sample_stats) for
the `escalate` section.

The service date analysed is 2026-09-26 (BST, so London local = UTC+1).
trust_event_backlog keeps ~24 h by received_at; the export began at 03:36Z,
so trains due before ANALYSIS_START are excluded (their activations may
have been pruned already).
"""

import bisect
import csv
import datetime as dt
import math
import os
import statistics
import sys
import tomllib
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

DATE = "2026-09-26"
UTC_OFFSET_H = 1  # BST
UTC = dt.UTC
ANALYSIS_START = dt.datetime(2026, 9, 26, 6, 0, tzinfo=UTC)
ANALYSIS_END = dt.datetime(2026, 9, 27, 0, 30, tzinfo=UTC)
FINAL_NOW = dt.datetime(2026, 9, 27, 2, 3, tzinfo=UTC)
DELAY_THRESHOLD = 5  # common::Defaults::delay_threshold_minutes
RELEVANCE = os.environ.get("FC_RELEVANCE", "op_calls2")
ARGC_MIN = 3  # program, export-dir, lines-dir
RELEVANCE_MODES = ("all", "calls1", "calls2", "op", "op_calls1", "op_calls2")
DEFAULT_SECTIONS = frozenset(
    {"relevance", "volume", "outcomes", "lag", "simulate", "ldbws"}
)
STEP = dt.timedelta(minutes=15)
NO_OVERDUE = 10**6  # overdue_margin "infinity": never infer lateness from silence

# TRUST message types.
MSG_ACTIVATION = "0001"
MSG_CANCELLATION = "0002"
MSG_MOVEMENT = "0003"
MSG_REINSTATEMENT = "0005"

# Local-time bands: band i covers hours [edges[i-1], edges[i]).
BAND_EDGES = (6, 7, 10, 16, 19, 23)
BAND_NAMES = (
    "night 23-06L",
    "early 06-07L",
    "peak 07-10L",
    "offpeak 10-16L",
    "peak 16-19L",
    "evening 19-23L",
    "night 23-06L",
)
DAY_START_H = 7  # the "daytime" and "07-22L" filters start here (local)
DAY_END_H = 19
EVENING_END_H = 22

# Severity rules (design §4.3.2): cancelled / late share of a window.
CANCEL_PART_SUSPENDED = 0.60
CANCEL_REDUCED = 0.25
LATE_SEVERE = 0.5
LATE_MINOR = 0.25
MIN_WINDOW_TRAINS = 6  # the `simulate` section's fixed minimum window size

MIN_RUNNING_PER_LINE = 20  # perline: lines with fewer running trains skipped
MIN_LDBWS_SERVICES = 50  # perline: lines compared against LDBWS
MIN_OPERATOR_UIDS = 200  # outcomes: operators in the silent-rate table
MIN_LINE_WINDOWS = 20  # simulate2: lines in the not-Good share table
LAG_THRESHOLD_S = 300
SHORT_LEAD_MIN = (-5, -10)

# line_status_history Severity discriminant -> rank (0 = best).
SEVERITY_RANK = {
    10: 0,
    0: 1,
    12: 1,
    13: 1,
    22: 1,
    4: 2,
    5: 2,
    7: 3,
    9: 3,
    14: 3,
    20: 3,
    1: 4,
    2: 4,
    3: 4,
    6: 4,
    8: 4,
    11: 4,
    21: 4,
    23: 4,
}
PART_SUSPENDED_RANK = 4
REDUCED_RANK = 3

# CIF BS record layout (0-based slices).
BS_UID = slice(3, 9)
BS_FROM = slice(9, 15)
BS_TO = slice(15, 21)
BS_DAYS_START = 21
BS_STATUS = 29
BS_STP = 79
STP_RANK = {"C": 0, "N": 1, "O": 2, "P": 3}
BUS_SHIP_STATUSES = "B5S4"

type Row = dict[str, str]


@dataclass(frozen=True)
class Line:
    """The parts of one lines/*.toml the analysis uses."""

    operators: set[str]
    crs: set[str]
    dest_filter: set[str]
    headcodes: list[str]
    sample_stations: list[str]


class Call(NamedTuple):
    """One schedule calling point (t_utc: booked time, None if unbooked)."""

    seq: int
    tiploc: str
    kind: str
    t_utc: dt.datetime | None
    crs: str | None


class Movement(NamedTuple):
    """One TRUST 0003 movement."""

    received: dt.datetime
    planned: dt.datetime | None
    actual: dt.datetime | None
    crs: str
    event_type: str


@dataclass
class Train:
    """Everything received for one (uid, TRUST train_id)."""

    act: dt.datetime | None = None
    canx: list[tuple[dt.datetime, dt.datetime | None]] = field(default_factory=list)
    reinst: list[dt.datetime] = field(default_factory=list)
    mov: list[Movement] = field(default_factory=list)


@dataclass(frozen=True)
class Silence:
    """How outcome_at treats activated trains that have not reached the line."""

    overdue_margin: float = 0
    pending_excluded: bool = False


INFER_FROM_SILENCE = Silence()
NO_SILENCE_INFERENCE = Silence(NO_OVERDUE, pending_excluded=True)


@dataclass
class Data:
    """The loaded exports."""

    export_dir: Path
    lines: dict[str, Line]
    sched: dict[str, list[Call]]
    pop: dict[str, set[str]]
    ops: dict[str, tuple[str, str]]
    by_uid: dict[str, list[Train]]
    due: dict[str, list[tuple[dt.datetime, str]]] = field(default_factory=dict)

    def trains(self, uid: str) -> list[Train]:
        """TRUST trains received for a schedule uid."""
        return self.by_uid.get(uid, [])

    def operator(self, uid: str, default: str = "") -> str:
        """Return the schedule's operator ATOC code."""
        return self.ops.get(uid, (default, ""))[0]


def ts(s: str) -> dt.datetime | None:
    """Parse a Postgres timestamptz CSV value; empty means NULL."""
    if not s:
        return None
    s = s.replace("+00", "+00:00") if s.endswith("+00") else s
    return dt.datetime.fromisoformat(s)


def req_ts(s: str) -> dt.datetime:
    """Parse a NOT NULL Postgres timestamptz CSV value."""
    t = ts(s)
    if t is None:
        msg = "unexpected empty timestamp"
        raise ValueError(msg)
    return t


def pct[T: (int, float)](values: Sequence[T], p: float) -> T | None:
    """Nearest-rank percentile, or None for no values."""
    if not values:
        return None
    v = sorted(values)
    k = min(len(v) - 1, max(0, round(p / 100 * (len(v) - 1))))
    return v[k]


def share_at_least(values: Sequence[float], k: float) -> float:
    """Share of values >= k."""
    return sum(1 for x in values if x >= k) / len(values)


def read_csv(path: Path) -> list[Row]:
    """All rows of a CSV export with a header."""
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def time_band(local_h: int) -> str:
    """Name of the local-time band an hour falls in."""
    return BAND_NAMES[bisect.bisect_right(BAND_EDGES, local_h)]


def local_hour(t: dt.datetime) -> int:
    """London local hour of a UTC time on the analysed day."""
    return (t.hour + UTC_OFFSET_H) % 24


def load_lines(lines_dir: Path) -> dict[str, Line]:
    """Every lines/*.toml, by line id."""
    lines = {}
    for path in lines_dir.glob("*.toml"):
        with path.open("rb") as f:
            d = tomllib.load(f)
        lines[d["id"]] = Line(
            operators=set(d.get("operators", [])),
            crs={s["crs"].upper() for s in d.get("stations", [])},
            dest_filter=set(d.get("destination_crs_filter", [])),
            headcodes=d.get("headcode_prefixes", []),
            sample_stations=d.get("sample_stations", []),
        )
    return lines


def load_schedules(export_dir: Path) -> dict[str, list[Call]]:
    """Load calling points per uid on DATE, in sequence order."""
    tiploc_crs = {}
    for r in read_csv(export_dir / "stanox.csv"):
        tiploc_crs[r["tiploc"].strip()] = r["crs"].upper()
    base = dt.datetime.fromisoformat(DATE).replace(tzinfo=UTC)
    sched: defaultdict[str, list[Call]] = defaultdict(list)
    for r in read_csv(export_dir / "scp.csv"):
        if r["service_date"] != DATE:
            continue
        t = r["booked_departure"] or r["booked_arrival"]
        tu = None
        if t:
            hh, mm, ss = (int(x) for x in t.split(":"))
            tu = base + dt.timedelta(
                days=int(r["day_offset"]),
                hours=hh - UTC_OFFSET_H,
                minutes=mm,
                seconds=ss,
            )
        tip = r["tiploc"].strip()
        sched[r["uid"]].append(
            Call(int(r["seq"]), tip, r["kind"], tu, tiploc_crs.get(tip))
        )
    for v in sched.values():
        v.sort()
    return sched


def load_population(export_dir: Path) -> dict[str, set[str]]:
    """Line population uids on DATE, by line id."""
    pop: defaultdict[str, set[str]] = defaultdict(set)
    with (export_dir / "pop.csv").open(encoding="utf-8") as f:
        for r in csv.reader(f):
            if r[1] == DATE:
                pop[r[0]].add(r[2])
    return pop


def add_message(tr: Train, r: Row) -> None:
    """Fold one trust_event_backlog row into its train."""
    m = r["msg_type"]
    if m == MSG_ACTIVATION:
        rec = req_ts(r["received_at"])
        tr.act = rec if tr.act is None else min(tr.act, rec)
    elif m == MSG_CANCELLATION:
        tr.canx.append((req_ts(r["received_at"]), ts(r["actual_timestamp"])))
    elif m == MSG_REINSTATEMENT:
        tr.reinst.append(req_ts(r["received_at"]))
    elif m == MSG_MOVEMENT:
        tr.mov.append(
            Movement(
                req_ts(r["received_at"]),
                ts(r["planned_timestamp"]),
                ts(r["actual_timestamp"]),
                r["crs"],
                r["event_type"],
            )
        )


def load_trust(export_dir: Path) -> dict[str, list[Train]]:
    """TRUST messages received for DATE, grouped per uid and train_id."""
    tid_uid: dict[str, str] = {}
    trains: defaultdict[tuple[str, str], Train] = defaultdict(Train)
    rows = read_csv(export_dir / "teb.csv")
    for r in rows:
        if r["train_uid"]:
            tid_uid.setdefault(r["train_id"], r["train_uid"])
    for r in rows:
        if r["service_date"] != DATE:
            continue
        uid = r["train_uid"] or tid_uid.get(r["train_id"])
        if not uid:
            continue
        add_message(trains[uid, r["train_id"]], r)
    by_uid: defaultdict[str, list[Train]] = defaultdict(list)
    for (uid, _), tr in trains.items():
        tr.mov.sort(key=lambda x: x.received)
        by_uid[uid].append(tr)
    return by_uid


def bus_ship_uids(bs: Path) -> set[str]:
    """Uids whose DATE schedule (by STP precedence) is a bus or ship."""
    d = dt.date.fromisoformat(DATE)
    best: dict[str, tuple[int, str]] = {}
    with bs.open(encoding="utf-8") as f:
        for line in f:
            try:
                valid_from = (
                    dt.datetime.strptime(line[BS_FROM], "%y%m%d")
                    .replace(tzinfo=UTC)
                    .date()
                )
                valid_to = (
                    dt.datetime.strptime(line[BS_TO], "%y%m%d")
                    .replace(tzinfo=UTC)
                    .date()
                )
            except ValueError:
                continue
            if (
                not (valid_from <= d <= valid_to)
                or line[BS_DAYS_START + d.weekday()] != "1"
            ):
                continue
            rank = STP_RANK.get(line[BS_STP], 9)
            uid = line[BS_UID]
            if uid not in best or rank < best[uid][0]:
                best[uid] = (rank, line[BS_STATUS])
    return {u for u, (_, status) in best.items() if status in BUS_SHIP_STATUSES}


def load(export_dir: Path, lines_dir: Path) -> Data:
    """Load every export the sections use (FC_BS applied)."""
    lines = load_lines(lines_dir)
    sched = load_schedules(export_dir)
    pop = load_population(export_dir)
    ops = {}
    for r in read_csv(export_dir / "ops.csv"):
        if r["service_date"] == DATE:
            ops[r["train_uid"]] = (r["operator_atoc"], r["headcode"])
    by_uid = load_trust(export_dir)
    bs = os.environ.get("FC_BS")
    if bs:
        buses = bus_ship_uids(Path(bs))
        removed = 0
        for lid in pop:
            before = len(pop[lid])
            pop[lid] -= buses
            removed += before - len(pop[lid])
        print(
            f"FC_BS: excluded {len(buses)} bus/ship uids "
            f"({removed} line-population entries)"
        )
    return Data(export_dir, lines, sched, pop, ops, by_uid)


def line_calls(
    line: Line, sched_uid: Iterable[Call]
) -> list[tuple[dt.datetime, str | None]]:
    """Return the schedule's booked calls at the line's stations."""
    return [
        (c.t_utc, c.crs) for c in sched_uid if c.t_utc is not None and c.crs in line.crs
    ]


def relevant(line: Line, uid: str, data: Data, mode: str) -> bool:
    """Whether a population uid counts for the line under a relevance mode."""
    calls = line_calls(line, data.sched.get(uid, []))
    on_operator = data.operator(uid) in line.operators
    distinct_stations = len({c for _, c in calls})
    match mode:
        case "all":
            return True
        case "calls1":
            return len(calls) >= 1
        case "op":
            return on_operator
        case "op_calls1":
            return on_operator and len(calls) >= 1
        case "calls2":
            return distinct_stations >= 2  # noqa: PLR2004  # the mode's name
        case "op_calls2":
            return on_operator and distinct_stations >= 2  # noqa: PLR2004  # ditto
        case _:
            raise ValueError(mode)


def due_trains(
    data: Data, mode: str = RELEVANCE
) -> dict[str, list[tuple[dt.datetime, str]]]:
    """Map line -> [(due_utc, uid)] of relevant trains; due = first call on it."""
    out = {}
    for lid, line in data.lines.items():
        lst = []
        for uid in data.pop.get(lid, ()):
            if not relevant(line, uid, data, mode):
                continue
            calls = line_calls(line, data.sched.get(uid, []))
            if not calls:
                continue
            lst.append((min(t for t, _ in calls), uid))
        lst.sort()
        out[lid] = lst
    return out


def minutes(delta: dt.timedelta) -> float:
    """Convert a timedelta to minutes."""
    return delta.total_seconds() / 60


def outcome_at(
    trs: Sequence[Train],
    due: dt.datetime,
    line: Line,
    now: dt.datetime,
    silence: Silence = INFER_FROM_SILENCE,
) -> tuple[str, float | None]:
    """Classify one due train using only what had been RECEIVED by `now`.

    Returns (class, delay) where class is one of
    ran / late / overdue / cancelled_explicit / presumed_cancelled /
    unknown_activated (or pending when silence.pending_excluded).
    """
    activated = any(t.act and t.act <= now for t in trs)
    movs = [m for t in trs for m in t.mov if m.received <= now]
    canx = [c for t in trs for c in t.canx if c[0] <= now]
    reinst = [r for t in trs for r in t.reinst if r <= now]
    # reached the line: a movement at one of the line's stations, or planned
    # at/after the due time
    reached = [
        m for m in movs if (m.crs in line.crs) or (m.planned and m.planned >= due)
    ]
    if reached:
        first = min(reached, key=lambda m: m.planned or m.received)
        delay = (
            minutes(first.actual - first.planned)
            if (first.planned and first.actual)
            else 0
        )
        return ("late" if delay >= DELAY_THRESHOLD else "ran"), delay
    if canx:
        last_canx = max(c[0] for c in canx)
        if not any(r >= last_canx for r in reinst):
            return "cancelled_explicit", None
    if not activated and not movs:
        return "presumed_cancelled", None
    last_delay: float = 0
    if movs:
        m = max(movs, key=lambda m: m.received)
        if m.planned and m.actual:
            last_delay = minutes(m.actual - m.planned)
    overdue = minutes(now - due) - silence.overdue_margin
    est = max(last_delay, overdue)
    if est >= DELAY_THRESHOLD:
        return "overdue", est
    return ("pending" if silence.pending_excluded else "unknown_activated"), est


def windows(w: int, g: int) -> Iterator[tuple[dt.datetime, dt.datetime, dt.datetime]]:
    """(t, lo, hi): every 15 min, trains due in (lo, hi] = (t-w-g, t-g]."""
    t = ANALYSIS_START + dt.timedelta(minutes=w + g)
    while t < ANALYSIS_END:
        yield t, t - dt.timedelta(minutes=w + g), t - dt.timedelta(minutes=g)
        t += STEP


def severity(tot: int, canc: int, late: int, min_k: int = 0) -> str:
    """Window severity (design §4.3.2); each rule also needs >= min_k trains."""
    cr, lr = canc / tot, late / tot
    if cr >= CANCEL_PART_SUSPENDED and canc >= min_k:
        return "PartSuspended"
    if cr >= CANCEL_REDUCED and canc >= min_k:
        return "Reduced"
    if lr >= LATE_SEVERE and late >= min_k:
        return "Severe"
    if lr >= LATE_MINOR and late >= min_k:
        return "Minor"
    return "Good"


def escalation(tot: int, canc: int, late: int, min_k: int) -> tuple[int, str] | None:
    """(rank, name) the window would raise the line to, or None."""
    cr, lr = canc / tot, late / tot
    if cr >= CANCEL_PART_SUSPENDED and canc >= min_k:
        return PART_SUSPENDED_RANK, "PartSuspended"
    if lr >= LATE_SEVERE and late >= min_k:
        return PART_SUSPENDED_RANK, "Severe"
    if cr >= CANCEL_REDUCED and canc >= min_k:
        return REDUCED_RANK, "Reduced"
    if lr >= LATE_MINOR and late >= min_k:
        return REDUCED_RANK, "Minor"
    return None


def is_cancelled(cls: str) -> bool:
    """Whether an outcome class counts as cancelled."""
    return cls in {"cancelled_explicit", "presumed_cancelled"}


def is_late(cls: str) -> bool:
    """Whether an outcome class counts as late."""
    return cls in {"late", "overdue"}


def truth_breakdown(conf: Counter[tuple[str, str]], cls: str) -> tuple[int, str]:
    """(n, "truth=share, ...") for evaluations classified `cls` at t."""
    row = {k[1]: v for k, v in conf.items() if k[0] == cls}
    n = sum(row.values())
    text = (
        ", ".join(
            f"{k}={v / n:.1%}" for k, v in sorted(row.items(), key=lambda x: -x[1])
        )
        if n
        else ""
    )
    return n, text


def section_relevance(data: Data) -> None:
    """Per-line day population under each relevance filter."""
    print("\n== relevance: per-line day population under each filter ==")
    pop, lines = data.pop, data.lines
    for mode in RELEVANCE_MODES:
        sizes = [
            sum(1 for u in pop[lid] if relevant(lines[lid], u, data, mode))
            for lid in pop
            if lid in lines
        ]
        print(
            f"{mode:10s} total={sum(sizes):7d} "
            f"median/line={statistics.median(sizes):6.0f} "
            f"p10={pct(sizes, 10)} p90={pct(sizes, 90)} max={max(sizes)} "
            f"zero_lines={sum(1 for s in sizes if s == 0)}"
        )
    missing = sum(1 for lid in pop for u in pop[lid] if u not in data.sched)
    print(f"population uids with no schedule_calling_points_full row: {missing}")
    for lid in ["tfw-conwy-valley", "cross-country", "overground-windrush"]:
        if lid in pop:
            print(
                lid,
                {
                    m: sum(1 for u in pop[lid] if relevant(lines[lid], u, data, m))
                    for m in RELEVANCE_MODES
                },
            )


def volume_window(
    due: dict[str, list[tuple[dt.datetime, str]]], w: int
) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    """Trains due in the w minutes before each 15-min step of the day.

    Returns the counts per time band, and per line in daytime.
    """
    vals: defaultdict[str, list[int]] = defaultdict(list)
    per_line_min: defaultdict[str, list[int]] = defaultdict(list)
    t = dt.datetime(2026, 9, 26, 1, 0, tzinfo=UTC)
    while t < dt.datetime(2026, 9, 27, 1, 0, tzinfo=UTC):
        lh = local_hour(t)
        for lid, lst in due.items():
            times = [d for d, _ in lst]
            n = bisect.bisect_right(times, t) - bisect.bisect_right(
                times, t - dt.timedelta(minutes=w)
            )
            vals[time_band(lh)].append(n)
            if DAY_START_H <= lh < DAY_END_H:
                per_line_min[lid].append(n)
        t += STEP
    return vals, per_line_min


def section_volume(data: Data) -> None:
    """Relevant trains due per line per window."""
    due = data.due
    print(
        f"\n== volume: relevant ({RELEVANCE}) trains due per line per 60-min window =="
    )
    per_line_med: dict[str, float] = {}
    for w in (30, 60, 90):
        vals, per_line_min = volume_window(due, w)
        print(f"-- W={w} min: per (line, 15-min step) counts, by local time band")
        for b, v in vals.items():
            print(
                f"   {b:16s} p10={pct(v, 10)} p25={pct(v, 25)} p50={pct(v, 50)} "
                f"p75={pct(v, 75)} p90={pct(v, 90)} "
                f"share>=4={share_at_least(v, 4):.2f} "
                f"share>=6={share_at_least(v, 6):.2f} "
                f"share>=8={share_at_least(v, 8):.2f}"
            )
        if w == 60:  # noqa: PLR2004  # the headline window size
            per_line_med = {
                lid: statistics.median(v) for lid, v in per_line_min.items() if v
            }
    meds = sorted(per_line_med.values())
    print(
        f"per-line median trains due per 60 min, 07-19 local: p10={pct(meds, 10)} "
        f"p25={pct(meds, 25)} p50={pct(meds, 50)} p75={pct(meds, 75)} "
        f"p90={pct(meds, 90)} max={meds[-1]}"
    )
    for mn in (3, 4, 6, 8):
        print(
            f"   lines whose daytime median >= {mn}: "
            f"{sum(1 for m in meds if m >= mn)}/{len(meds)}"
        )
    daytot = sorted(len(v) for v in due.values())
    print(
        f"relevant trains per line per day: p10={pct(daytot, 10)} "
        f"p50={pct(daytot, 50)} p90={pct(daytot, 90)} "
        f"max={daytot[-1]} zero={sum(1 for x in daytot if x == 0)}"
    )
    print("conwy per-day:", len(due.get("tfw-conwy-valley", [])))


def message_key(trs: Sequence[Train]) -> str:
    """Which message kinds a uid got: A(ctivation) M(ovement) C(ancel) R(einstate)."""
    return (
        ("A" if any(t.act for t in trs) else "-")
        + ("M" if any(t.mov for t in trs) else "-")
        + ("C" if any(t.canx for t in trs) else "-")
        + ("R" if any(t.reinst for t in trs) else "-")
    )


def section_outcomes(data: Data) -> None:
    """End-of-day outcome classes of the relevant trains."""
    print("\n== outcomes (end-of-day truth, relevant trains due 06:00Z-00:30Z) ==")
    c: Counter[str] = Counter()
    seen: set[str] = set()
    silent_uids = set()
    for lid, lst in data.due.items():
        for d, uid in lst:
            if not (ANALYSIS_START <= d < ANALYSIS_END):
                continue
            cls, _ = outcome_at(data.trains(uid), d, data.lines[lid], FINAL_NOW)
            c["line_train:" + cls] += 1
            if uid in seen:
                continue
            seen.add(uid)
            key = message_key(data.trains(uid))
            c["uid:" + key] += 1
            if key[:3] == "---":
                silent_uids.add(uid)
    tot_lt = sum(v for k, v in c.items() if k.startswith("line_train:"))
    for k, v in sorted(c.items()):
        den = tot_lt if k.startswith("line_train:") else len(seen)
        print(f"   {k:28s} {v:7d}  {v / den:6.2%}")
    print(
        f"   distinct uids={len(seen)}  silent (no 0001/0002/0003)={len(silent_uids)}"
    )
    # silent uids: operator breakdown and in-schedule check
    opc = Counter(data.operator(u, "?") for u in silent_uids)
    print("   silent by operator (top 12):", opc.most_common(12))
    allc = Counter(data.operator(u, "?") for u in seen)
    print(
        "   silent rate by operator (>=200 uids):",
        sorted(
            (
                (o, round(opc[o] / n, 3), n)
                for o, n in allc.items()
                if n >= MIN_OPERATOR_UIDS
            ),
            key=lambda x: -x[1],
        )[:15],
    )
    # explicit cancellations with and without movements
    ex_nomov = sum(1 for u in seen for t in data.trains(u) if t.canx and not t.mov)
    ex_mov = sum(1 for u in seen for t in data.trains(u) if t.canx and t.mov)
    print(
        f"   train_ids with 0002: no 0003 at all={ex_nomov}, "
        f"with 0003 (en-route/partial)={ex_mov}"
    )


def section_lag(data: Data) -> None:
    """Receive lag of movements; lead of activations and cancellations."""
    print("\n== lag ==")
    lag = [
        (m.received - m.actual).total_seconds()
        for trs in data.by_uid.values()
        for t in trs
        for m in t.mov
        if m.actual and ANALYSIS_START <= m.actual < ANALYSIS_END
    ]
    print(
        f"0003 received - actual (s): n={len(lag)} p50={pct(lag, 50)} "
        f"p90={pct(lag, 90)} p99={pct(lag, 99)} p99.9={pct(lag, 99.9)} "
        f"share>300s={share_above(lag, LAG_THRESHOLD_S):.4f}"
    )
    lead: list[float] = []
    canx_lead: list[float] = []
    for lst in data.due.values():
        for d, uid in lst:
            if not (ANALYSIS_START + dt.timedelta(hours=2) <= d < ANALYSIS_END):
                continue
            for t in data.trains(uid):
                if t.act:
                    lead.append(minutes(d - t.act))
                canx_lead.extend(minutes(d - rec) for rec, _ in t.canx)
    print(
        f"activation lead before line due (min): n={len(lead)} p1={pct(lead, 1)} "
        f"p5={pct(lead, 5)} p50={pct(lead, 50)} p95={pct(lead, 95)} "
        f"share<0={share_below(lead, 0):.4f} "
        f"share<-5={share_below(lead, SHORT_LEAD_MIN[0]):.4f} "
        f"share<-10={share_below(lead, SHORT_LEAD_MIN[1]):.4f}"
    )
    after_due = sum(1 for x in canx_lead if x < 0) / max(1, len(canx_lead))
    print(
        f"0002 received before line due (min, +ve = before): n={len(canx_lead)} "
        f"p5={pct(canx_lead, 5)} p25={pct(canx_lead, 25)} "
        f"p50={pct(canx_lead, 50)} p75={pct(canx_lead, 75)} "
        f"share_after_due={after_due:.3f}"
    )


def share_above(values: Sequence[float], k: float) -> float:
    """Share of values > k."""
    return sum(1 for x in values if x > k) / len(values)


def share_below(values: Sequence[float], k: float) -> float:
    """Share of values < k."""
    return sum(1 for x in values if x < k) / len(values)


@dataclass
class SimulateStats:
    """Accumulators for one (W, grace) run of the `simulate` section."""

    conf: Counter[tuple[str, str]] = field(default_factory=Counter)
    sev: Counter[str] = field(default_factory=Counter)
    below: Counter[int] = field(default_factory=Counter)
    n_windows: int = 0


def simulate_window(
    data: Data,
    lid: str,
    window: tuple[dt.datetime, dt.datetime, dt.datetime],
    st: SimulateStats,
) -> None:
    """Classify one line-window at t and fold it into the stats."""
    t, lo, hi = window
    tot = canc = late = pres = 0
    for d, uid in data.due[lid]:
        if not (lo < d <= hi):
            continue
        trs = data.trains(uid)
        cls, _ = outcome_at(trs, d, data.lines[lid], t)
        truth, _ = outcome_at(trs, d, data.lines[lid], FINAL_NOW)
        st.conf[cls, truth] += 1
        tot += 1
        canc += is_cancelled(cls)
        pres += cls == "presumed_cancelled"
        late += is_late(cls)
    st.n_windows += 1
    for mn in (3, 4, 6, 8):
        if tot < mn:
            st.below[mn] += 1
    if tot >= MIN_WINDOW_TRAINS:
        st.sev[severity(tot, canc, late)] += 1
        if pres and (canc - pres) / tot < CANCEL_REDUCED <= canc / tot:
            st.sev["cancel_sev_needs_presumed"] += 1


def section_simulate(data: Data) -> None:
    """Windowed classification at t vs end-of-day truth, inferring from silence."""
    print(
        "\n== simulate: windowed classification with info received by t, "
        "vs end-of-day truth =="
    )
    for w, g in [(60, 5), (60, 10), (60, 15), (30, 10), (90, 10)]:
        st = SimulateStats()
        for window in windows(w, g):
            for lid in data.due:
                simulate_window(data, lid, window, st)
        tot = sum(st.conf.values())
        print(
            f"-- W={w} grace={g}: line-train evaluations={tot}, "
            f"line-windows={st.n_windows}"
        )
        for mn in (3, 4, 6, 8):
            print(f"   windows below min {mn}: {st.below[mn] / st.n_windows:.2%}")
        print("   severity (min 6):", dict(st.sev))
        for cls in [
            "ran",
            "late",
            "overdue",
            "unknown_activated",
            "cancelled_explicit",
            "presumed_cancelled",
        ]:
            n, text = truth_breakdown(st.conf, cls)
            if n:
                print(
                    f"   at-t {cls:20s} n={n:7d} ({n / tot:6.2%}) "
                    f"-> end-of-day truth: {text}"
                )


# simulate2 severity gates: name -> (min trains, min trains per rule).
SIMULATE2_GATES = {
    "n>=6": (6, 0),
    "n>=6,k>=3": (6, 3),
    "n>=8,k>=3": (8, 3),
    "n>=4,k>=3": (4, 3),
}
PER_LINE_GATE = "n>=6,k>=3"


@dataclass
class Simulate2Stats:
    """Accumulators for one silence policy in the `simulate2` section."""

    conf: Counter[tuple[str, str]] = field(default_factory=Counter)
    sev: defaultdict[str, Counter[str]] = field(
        default_factory=lambda: defaultdict(Counter)
    )
    per_line: defaultdict[str, Counter[str]] = field(
        default_factory=lambda: defaultdict(Counter)
    )


def simulate2_window(
    data: Data,
    lid: str,
    window: tuple[dt.datetime, dt.datetime, dt.datetime],
    silence: Silence,
    st: Simulate2Stats,
) -> None:
    """Classify one line-window at t and fold it into the stats."""
    t, lo, hi = window
    tot = canc = late = 0
    for d, uid in data.due[lid]:
        if not (lo < d <= hi):
            continue
        trs = data.trains(uid)
        cls, _ = outcome_at(trs, d, data.lines[lid], t, silence)
        truth, _ = outcome_at(trs, d, data.lines[lid], FINAL_NOW)
        st.conf[cls, truth] += 1
        if cls == "pending":
            continue
        tot += 1
        canc += is_cancelled(cls)
        late += is_late(cls)
    lh = local_hour(t)
    for gate, (mn, mk) in SIMULATE2_GATES.items():
        if tot < mn:
            continue
        sv = severity(tot, canc, late, mk)
        st.sev[gate][sv] += 1
        if gate == PER_LINE_GATE and DAY_START_H <= lh < EVENING_END_H:
            st.per_line[lid][sv] += 1


def print_simulate2(st: Simulate2Stats) -> None:
    """Report one silence policy of the `simulate2` section."""
    conf = st.conf
    tot = sum(conf.values())
    for cls in ["late", "overdue", "unknown_activated", "pending"]:
        n, text = truth_breakdown(conf, cls)
        if n:
            print(f"   at-t {cls:18s} n={n:7d} ({n / tot:6.2%}) -> truth: {text}")
    late_truth = sum(v for k, v in conf.items() if k[1] == "late")
    late_caught = sum(v for k, v in conf.items() if k[1] == "late" and is_late(k[0]))
    print(f"   truth-late recall at t: {late_caught / late_truth:.1%}")
    for gate, c in st.sev.items():
        n = sum(c.values())
        print(
            f"   severity {gate:10s} windows={n:6d} "
            + " ".join(f"{k}={v / n:.2%}" for k, v in sorted(c.items()))
        )
    shares = sorted(
        ((1 - c["Good"] / sum(c.values())), lid)
        for lid, c in st.per_line.items()
        if sum(c.values()) >= MIN_LINE_WINDOWS
    )
    xs = [x for x, _ in shares]
    top5 = [(lid, round(x, 2)) for x, lid in shares[-5:]]
    print(
        f"   lines (n>=6,k>=3, 07-22L): share of windows not Good: "
        f"p50={pct(xs, 50):.2f} p90={pct(xs, 90):.2f} max={shares[-1][0]:.2f}; "
        f"top5={top5}"
    )


def section_simulate2(data: Data) -> None:
    """Silence policies and severity gates (W=60, grace=10)."""
    print(
        "\n== simulate2: handling of activated-but-not-yet-reached trains; "
        "severity gates (W=60, grace=10) =="
    )
    for margin, excl in [(0, False), (15, True), (30, True), (NO_OVERDUE, True)]:
        silence = Silence(margin, pending_excluded=excl)
        st = Simulate2Stats()
        for window in windows(60, 10):
            for lid in data.due:
                simulate2_window(data, lid, window, silence, st)
        print(f"-- overdue_margin={margin} pending_excluded={excl}")
        print_simulate2(st)


def ldbws_by_line(export_dir: Path) -> defaultdict[str, Counter[str]]:
    """LDBWS day totals per line."""
    ld: defaultdict[str, Counter[str]] = defaultdict(Counter)
    for r in read_csv(export_dir / "ldbws_hh.csv"):
        ld[r["line_id"]]["total"] += int(r["total"])
        ld[r["line_id"]]["delayed"] += int(r["delayed"])
        ld[r["line_id"]]["cancelled"] += int(r["cancelled"])
    return ld


def corr(x: Sequence[float], y: Sequence[float]) -> float:
    """Pearson correlation."""
    mx, my = statistics.mean(x), statistics.mean(y)
    sx = math.sqrt(sum((a - mx) ** 2 for a in x))
    sy = math.sqrt(sum((b - my) ** 2 for b in y))
    return sum((a - mx) * (b - my) for a, b in zip(x, y, strict=True)) / (sx * sy)


class PerLineRow(NamedTuple):
    """One line's day rates, full-coverage vs LDBWS."""

    lid: str
    fc_n: int
    obs: float
    fc_late: float
    fc_canx: float
    ldbws_n: int
    ldbws_late: float
    ldbws_canx: float


def section_perline(data: Data) -> None:
    """Per-line observability and day rates vs LDBWS."""
    print(
        "\n== perline: observability (activated, not cancelled, ever reached "
        "the line) and day rates vs LDBWS =="
    )
    ld = ldbws_by_line(data.export_dir)
    obs: list[tuple[float, str]] = []
    rows: list[PerLineRow] = []
    for lid, lst in data.due.items():
        c: Counter[str] = Counter()
        for d, uid in lst:
            if not (ANALYSIS_START <= d < ANALYSIS_END):
                continue
            cls, _ = outcome_at(
                data.trains(uid), d, data.lines[lid], FINAL_NOW, NO_SILENCE_INFERENCE
            )
            c[cls] += 1
        running = c["ran"] + c["late"] + c["overdue"] + c["pending"]
        if running < MIN_RUNNING_PER_LINE:
            continue
        o = (c["ran"] + c["late"]) / running
        obs.append((o, lid))
        tot = sum(c.values())
        ld_tot = max(1, ld[lid]["total"])
        rows.append(
            PerLineRow(
                lid,
                tot,
                o,
                c["late"] / max(1, c["ran"] + c["late"]),
                (c["cancelled_explicit"] + c["presumed_cancelled"]) / tot,
                ld[lid]["total"],
                ld[lid]["delayed"] / ld_tot,
                ld[lid]["cancelled"] / ld_tot,
            )
        )
    obs.sort()
    ov = [o for o, _ in obs]
    print(
        f"lines={len(obs)} observability p5={pct(ov, 5):.3f} p10={pct(ov, 10):.3f} "
        f"p25={pct(ov, 25):.3f} p50={pct(ov, 50):.3f}"
    )
    for th in (0.8, 0.9, 0.95):
        print(f"   lines with observability < {th}: {sum(1 for o in ov if o < th)}")
    print("   worst 10:", [(lid, round(o, 3)) for o, lid in obs[:10]])
    both = [r for r in rows if r.ldbws_n >= MIN_LDBWS_SERVICES]
    fc_l = [r.fc_late for r in both]
    ld_l = [r.ldbws_late for r in both]
    fc_c = [r.fc_canx for r in both]
    ld_c = [r.ldbws_canx for r in both]
    print(
        f"   lines with >=50 LDBWS services: {len(both)}; "
        f"late-rate FC p50={pct(fc_l, 50):.3f} LDBWS p50={pct(ld_l, 50):.3f} "
        f"corr={corr(fc_l, ld_l):.2f}; "
        f"cancel-rate FC p50={pct(fc_c, 50):.3f} LDBWS p50={pct(ld_c, 50):.3f} "
        f"corr={corr(fc_c, ld_c):.2f}"
    )
    for r in sorted(both, key=lambda r: -r.fc_late)[:8]:
        print(
            f"   {r.lid:32s} fc_n={r.fc_n:4d} obs={r.obs:.2f} "
            f"fc_late={r.fc_late:.3f} fc_canx={r.fc_canx:.3f} "
            f"ldbws_n={r.ldbws_n:4d} ldbws_late={r.ldbws_late:.3f} "
            f"ldbws_canx={r.ldbws_canx:.3f}"
        )


type Timeline = list[tuple[dt.datetime, int, set[str]]]


def load_timelines(export_dir: Path) -> dict[str, Timeline]:
    """line_status_history per line: [(computed_at, worst rank, provenances)].

    hist0.csv is the last row before the day; hist*.csv columns are
    line_id, computed_at, sev (Severity discriminant), dq, ss.
    """
    snaps: defaultdict[str, defaultdict[dt.datetime, list[tuple[int, str]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for fn in ("hist0.csv", "hist.csv"):
        for r in read_csv(export_dir / fn):
            snaps[r["line_id"]][req_ts(r["computed_at"])].append(
                (SEVERITY_RANK.get(int(r["sev"]), 0), r["dq"])
            )
    return {
        lid: sorted((t, max(x[0] for x in v), {x[1] for x in v}) for t, v in d.items())
        for lid, d in snaps.items()
    }


def current(
    timelines: dict[str, Timeline], lid: str, t: dt.datetime
) -> tuple[int, set[str]]:
    """Return the line's live (rank, provenances) at t."""
    tl = timelines.get(lid)
    if not tl:
        return 0, {"none"}
    i = bisect.bisect_right([x[0] for x in tl], t) - 1
    return (tl[i][1], tl[i][2]) if i >= 0 else (0, {"none"})


# escalate gates: name -> (min trains, min trains per rule, count presumed).
ESCALATE_GATES = {
    "n>=6,k>=3": (6, 3, True),
    "n>=6,k>=3,explicit-only": (6, 3, False),
    "n>=8,k>=3": (8, 3, True),
    "n>=6,k>=2": (6, 2, True),
}


def escalate_counts(
    data: Data,
    lid: str,
    window: tuple[dt.datetime, dt.datetime, dt.datetime],
    *,
    pres_ok: bool,
) -> tuple[int, int, int]:
    """(total, cancelled, late) of a line-window, pending (and maybe presumed) out."""
    t, lo, hi = window
    tot = canc = late = 0
    for d, uid in data.due[lid]:
        if not (lo < d <= hi):
            continue
        cls, _ = outcome_at(
            data.trains(uid), d, data.lines[lid], t, NO_SILENCE_INFERENCE
        )
        if cls == "pending" or (cls == "presumed_cancelled" and not pres_ok):
            continue
        tot += 1
        canc += is_cancelled(cls)
        late += is_late(cls)
    return tot, canc, late


def section_escalate(data: Data) -> None:
    """How often the recent window would escalate the live severity."""
    timelines = load_timelines(data.export_dir)
    print(
        "\n== escalate: how often the recent window would escalate the live "
        "severity (W=60, grace=10, no silence-based lateness) =="
    )
    for gate, (mn, mk, pres_ok) in ESCALATE_GATES.items():
        esc: Counter[str] = Counter()
        evals = 0
        line_hours: set[tuple[str, dt.datetime]] = set()
        by_dq: Counter[str] = Counter()
        for window in windows(60, 10):
            t = window[0]
            for lid in data.due:
                tot, canc, late = escalate_counts(data, lid, window, pres_ok=pres_ok)
                if tot < mn:
                    continue
                evals += 1
                raised = escalation(tot, canc, late, mk)
                if raised is None:
                    continue
                fr, name = raised
                cur, dqs = current(timelines, lid, t)
                if fr > cur:
                    esc[f"{name} over rank {cur}"] += 1
                    line_hours.add((lid, t.replace(minute=0)))
                    by_dq["/".join(sorted(dqs))] += 1
        n = sum(esc.values())
        print(
            f"-- {gate}: evaluable line-windows={evals}, escalations={n} "
            f"({n / max(1, evals):.2%}), distinct line-hours={len(line_hours)}, "
            f"lines={len({lid for lid, _ in line_hours})}"
        )
        print("   ", dict(esc.most_common()))
        print("    current provenance when escalated:", dict(by_dq.most_common(6)))


def section_ldbws(data: Data) -> None:
    """LDBWS half-hourly deduped stats on the same day."""
    print(
        "\n== LDBWS half-hourly deduped stats on the same day (per line-half-hour) =="
    )
    tot: Counter[str] = Counter()
    rows = read_csv(data.export_dir / "ldbws_hh.csv")
    per = [int(r["total"]) for r in rows]
    print(
        f"LDBWS distinct services per line-half-hour: n={len(per)} "
        f"p50={pct(per, 50)} p90={pct(per, 90)}"
    )
    for r in rows:
        tot["total"] += int(r["total"])
        tot["delayed"] += int(r["delayed"])
        tot["cancelled"] += int(r["cancelled"])
    print(
        "LDBWS day totals:",
        dict(tot),
        f"cancel_rate={tot['cancelled'] / tot['total']:.3%} "
        f"delay_rate={tot['delayed'] / tot['total']:.3%}",
    )


def main(argv: Sequence[str]) -> int:
    """Run the requested sections; returns the exit status."""
    if len(argv) > 1 and argv[1] in {"-h", "--help"}:
        print(__doc__)
        return 0
    if len(argv) < ARGC_MIN:
        print(__doc__, file=sys.stderr)
        return 2
    sections = set(argv[3:]) or DEFAULT_SECTIONS
    data = load(Path(argv[1]), Path(argv[2]))
    print(
        f"lines={len(data.lines)} populated_lines={len(data.pop)} "
        f"sched_uids={len(data.sched)} trust_uids={len(data.by_uid)}"
    )

    if "relevance" in sections:
        section_relevance(data)
    data.due = due_trains(data)
    for name, run in (
        ("volume", section_volume),
        ("outcomes", section_outcomes),
        ("lag", section_lag),
        ("simulate", section_simulate),
        ("simulate2", section_simulate2),
        ("perline", section_perline),
        ("escalate", section_escalate),
        ("ldbws", section_ldbws),
    ):
        if name in sections:
            run(data)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
