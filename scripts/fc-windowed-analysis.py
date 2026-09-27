#!/usr/bin/env python3
"""Read-only analysis behind
docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md.

Works on CSV exports of production tables; it never connects to anything.
The exports were taken with plain SELECTs (\\copy ... TO STDOUT), e.g.

  K='kubectl --context mine-bringer-ts -n distant-signal exec -i distant-signal-postgres-0 -- psql -U distant_signal -d distant_signal -c'
  $K "\\copy (SELECT train_id, train_uid, service_date, msg_type, event_type, crs, planned_timestamp,
               actual_timestamp, variation_status, delay_minutes, received_at
             FROM trust_event_backlog WHERE service_date >= '2026-09-25') TO STDOUT WITH CSV HEADER" > teb.csv
  $K "\\copy (SELECT service_date, uid, seq, tiploc, kind, booked_arrival, booked_departure, day_offset
             FROM schedule_calling_points_full WHERE service_date IN ('2026-09-26','2026-09-27'))
             TO STDOUT WITH CSV HEADER" > scp.csv
  $K "\\copy (SELECT line_id, service_date, e->>'uid' FROM schedule_line_population,
             jsonb_array_elements(population) e WHERE service_date IN ('2026-09-26','2026-09-27'))
             TO STDOUT WITH CSV HEADER" > pop.csv
  $K "\\copy (SELECT stanox, crs, tiploc FROM stanox_crs) TO STDOUT WITH CSV HEADER" > stanox.csv
  $K "\\copy (SELECT DISTINCT ON (service_date, train_uid) service_date, train_uid, operator_atoc, headcode
             FROM schedule_destination_departures WHERE service_date IN ('2026-09-26','2026-09-27')
             ORDER BY service_date, train_uid) TO STDOUT WITH CSV HEADER" > ops.csv
  $K "\\copy (SELECT line_id, half_hour_start, sample_cycles, total, delayed, cancelled
             FROM line_status_half_hourly_stats WHERE half_hour_start >= '2026-09-26 01:00Z'
             AND half_hour_start < '2026-09-27 01:00Z') TO STDOUT WITH CSV HEADER" > ldbws_hh.csv

Optional: FC_BS=<file of CIF "BS" records> (grep '^BS' RJTTF971MCA.txt from the
schedulefeed pod's /data/schedule-feed) excludes bus/ship schedules (Train
Status B/5/S/4) from every population, which is what the design recommends.
FC_RELEVANCE picks the line-relevance filter (default op_calls2).

Usage: fc-windowed-analysis.py <export-dir> <lines-dir> [section ...]
Sections: relevance, volume, outcomes, lag, simulate, simulate2, perline, escalate, ldbws
(default: relevance volume outcomes lag simulate ldbws). `simulate` infers lateness from
silence (the rejected variant, design §3.5); simulate2/perline/escalate implement the
design's §4.3.2 rules (overdue_margin = infinity, pending excluded).

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
import glob
import os
import statistics
import sys
import tomllib
from collections import Counter, defaultdict

DATE = "2026-09-26"
UTC_OFFSET_H = 1  # BST
ANALYSIS_START = dt.datetime(2026, 9, 26, 6, 0, tzinfo=dt.timezone.utc)
ANALYSIS_END = dt.datetime(2026, 9, 27, 0, 30, tzinfo=dt.timezone.utc)
DELAY_THRESHOLD = 5  # common::Defaults::delay_threshold_minutes
UTC = dt.timezone.utc


def ts(s):
    if not s:
        return None
    s = s.replace("+00", "+00:00") if s.endswith("+00") else s
    return dt.datetime.fromisoformat(s)


def pct(values, p):
    if not values:
        return None
    v = sorted(values)
    k = min(len(v) - 1, max(0, int(round(p / 100 * (len(v) - 1)))))
    return v[k]


def load(export_dir, lines_dir):
    lines = {}
    for path in glob.glob(os.path.join(lines_dir, "*.toml")):
        with open(path, "rb") as f:
            d = tomllib.load(f)
        lines[d["id"]] = {
            "operators": set(d.get("operators", [])),
            "crs": {s["crs"].upper() for s in d.get("stations", [])},
            "dest_filter": set(d.get("destination_crs_filter", [])),
            "headcodes": d.get("headcode_prefixes", []),
            "sample_stations": d.get("sample_stations", []),
        }
    tiploc_crs = {}
    crs_tiplocs = defaultdict(set)
    for r in csv.DictReader(open(os.path.join(export_dir, "stanox.csv"))):
        tiploc_crs[r["tiploc"].strip()] = r["crs"].upper()
        crs_tiplocs[r["crs"].upper()].add(r["tiploc"].strip())
    base = dt.datetime.fromisoformat(DATE).replace(tzinfo=UTC)
    sched = defaultdict(list)  # uid -> [(seq, tiploc, kind, t_utc, crs)]
    for r in csv.DictReader(open(os.path.join(export_dir, "scp.csv"))):
        if r["service_date"] != DATE:
            continue
        t = r["booked_departure"] or r["booked_arrival"]
        tu = None
        if t:
            hh, mm, ss = (int(x) for x in t.split(":"))
            tu = base + dt.timedelta(days=int(r["day_offset"]), hours=hh - UTC_OFFSET_H, minutes=mm, seconds=ss)
        tip = r["tiploc"].strip()
        sched[r["uid"]].append((int(r["seq"]), tip, r["kind"], tu, tiploc_crs.get(tip)))
    for v in sched.values():
        v.sort()
    pop = defaultdict(set)
    for r in csv.reader(open(os.path.join(export_dir, "pop.csv"))):
        if r[1] == DATE:
            pop[r[0]].add(r[2])
    ops = {}
    for r in csv.DictReader(open(os.path.join(export_dir, "ops.csv"))):
        if r["service_date"] == DATE:
            ops[r["train_uid"]] = (r["operator_atoc"], r["headcode"])
    # TRUST
    tid_uid = {}
    trains = defaultdict(lambda: {"act": None, "canx": [], "reinst": [], "mov": []})
    rows = list(csv.DictReader(open(os.path.join(export_dir, "teb.csv"))))
    for r in rows:
        if r["train_uid"]:
            tid_uid.setdefault(r["train_id"], r["train_uid"])
    for r in rows:
        if r["service_date"] != DATE:
            continue
        uid = r["train_uid"] or tid_uid.get(r["train_id"])
        if not uid:
            continue
        tr = trains[(uid, r["train_id"])]
        rec = ts(r["received_at"])
        m = r["msg_type"]
        if m == "0001":
            tr["act"] = rec if tr["act"] is None else min(tr["act"], rec)
        elif m == "0002":
            tr["canx"].append((rec, ts(r["actual_timestamp"])))
        elif m == "0005":
            tr["reinst"].append(rec)
        elif m == "0003":
            tr["mov"].append((rec, ts(r["planned_timestamp"]), ts(r["actual_timestamp"]), r["crs"], r["event_type"]))
    by_uid = defaultdict(list)
    for (uid, tid), tr in trains.items():
        tr["mov"].sort(key=lambda x: x[0])
        by_uid[uid].append(tr)
    bs = os.environ.get("FC_BS")
    if bs:
        d = dt.date.fromisoformat(DATE)
        best = {}
        for line in open(bs):
            try:
                f = dt.datetime.strptime(line[9:15], "%y%m%d").date()
                t = dt.datetime.strptime(line[15:21], "%y%m%d").date()
            except ValueError:
                continue
            if not (f <= d <= t) or line[21 + d.weekday()] != "1":
                continue
            rank = {"C": 0, "N": 1, "O": 2, "P": 3}.get(line[79], 9)
            if line[3:9] not in best or rank < best[line[3:9]][0]:
                best[line[3:9]] = (rank, line[29])
        buses = {u for u, (_, status) in best.items() if status in "B5S4"}
        removed = 0
        for lid in pop:
            before = len(pop[lid])
            pop[lid] -= buses
            removed += before - len(pop[lid])
        print(f"FC_BS: excluded {len(buses)} bus/ship uids ({removed} line-population entries)")
    return lines, crs_tiplocs, sched, pop, ops, by_uid


def line_calls(line, sched_uid):
    """Calling points of this schedule at the line's stations that carry a booked time."""
    return [(t, crs) for (_, _, _, t, crs) in sched_uid if t is not None and crs in line["crs"]]


def relevant(line, uid, sched, ops, mode):
    calls = line_calls(line, sched.get(uid, []))
    op = ops.get(uid, ("", ""))[0]
    if mode == "all":
        return True
    if mode == "calls1":
        return len(calls) >= 1
    if mode == "op":
        return op in line["operators"]
    if mode == "op_calls1":
        return op in line["operators"] and len(calls) >= 1
    if mode == "calls2":
        return len({c for _, c in calls}) >= 2
    if mode == "op_calls2":
        return op in line["operators"] and len({c for _, c in calls}) >= 2
    raise ValueError(mode)


def due_trains(lines, sched, pop, ops, mode=os.environ.get("FC_RELEVANCE", "op_calls2")):
    """line -> [(due_utc, uid, line_crs_set)] for relevant trains; due = first call on the line."""
    out = {}
    for lid, line in lines.items():
        lst = []
        for uid in pop.get(lid, ()):
            if not relevant(line, uid, sched, ops, mode):
                continue
            calls = line_calls(line, sched.get(uid, []))
            if not calls:
                continue
            lst.append((min(t for t, _ in calls), uid))
        lst.sort()
        out[lid] = lst
    return out


def outcome_at(uid, due, line, by_uid, now, overdue_margin=0, pending_excluded=False):
    """Classify one due train using only what had been RECEIVED by `now`.

    Returns (class, delay) where class is one of
    ran / late / overdue / cancelled_explicit / presumed_cancelled / unknown_activated.
    """
    trs = by_uid.get(uid, [])
    activated = any(t["act"] and t["act"] <= now for t in trs)
    movs = [m for t in trs for m in t["mov"] if m[0] <= now]
    canx = [c for t in trs for c in t["canx"] if c[0] <= now]
    reinst = [r for t in trs for r in t["reinst"] if r <= now]
    # reached the line: a movement at one of the line's stations, or planned at/after the due time
    reached = [m for m in movs if (m[3] in line["crs"]) or (m[1] and m[1] >= due)]
    if reached:
        first = min(reached, key=lambda m: m[1] or m[0])
        delay = ((first[2] - first[1]).total_seconds() / 60) if (first[1] and first[2]) else 0
        return ("late" if delay >= DELAY_THRESHOLD else "ran"), delay
    if canx:
        last_canx = max(c[0] for c in canx)
        if not any(r >= last_canx for r in reinst):
            return "cancelled_explicit", None
    if not activated and not movs:
        return "presumed_cancelled", None
    last_delay = 0
    if movs:
        m = max(movs, key=lambda m: m[0])
        if m[1] and m[2]:
            last_delay = (m[2] - m[1]).total_seconds() / 60
    overdue = (now - due).total_seconds() / 60 - overdue_margin
    est = max(last_delay, overdue)
    if est >= DELAY_THRESHOLD:
        return "overdue", est
    return ("pending" if pending_excluded else "unknown_activated"), est


def main():
    export_dir, lines_dir = sys.argv[1], sys.argv[2]
    sections = set(sys.argv[3:]) or {"relevance", "volume", "outcomes", "lag", "simulate", "ldbws"}
    lines, crs_tiplocs, sched, pop, ops, by_uid = load(export_dir, lines_dir)
    print(f"lines={len(lines)} populated_lines={len(pop)} sched_uids={len(sched)} trust_uids={len(by_uid)}")

    if "relevance" in sections:
        print("\n== relevance: per-line day population under each filter ==")
        for mode in ["all", "calls1", "calls2", "op", "op_calls1", "op_calls2"]:
            sizes = [sum(1 for u in pop[l] if relevant(lines[l], u, sched, ops, mode)) for l in pop if l in lines]
            print(f"{mode:10s} total={sum(sizes):7d} median/line={statistics.median(sizes):6.0f} "
                  f"p10={pct(sizes,10)} p90={pct(sizes,90)} max={max(sizes)} zero_lines={sum(1 for s in sizes if s==0)}")
        missing = sum(1 for l in pop for u in pop[l] if u not in sched)
        print(f"population uids with no schedule_calling_points_full row: {missing}")
        for l in ["tfw-conwy-valley", "cross-country", "overground-windrush"]:
            if l in pop:
                print(l, {m: sum(1 for u in pop[l] if relevant(lines[l], u, sched, ops, m))
                          for m in ["all", "calls1", "calls2", "op", "op_calls1", "op_calls2"]})

    due = due_trains(lines, sched, pop, ops)

    if "volume" in sections:
        print(f"\n== volume: relevant ({os.environ.get('FC_RELEVANCE', 'op_calls2')}) trains due per line per 60-min window ==")
        buckets = {"night 23-06L": [], "early 06-07L": [], "peak 07-10L": [], "offpeak 10-16L": [],
                   "peak 16-19L": [], "evening 19-23L": []}

        def bucket(local_h):
            if local_h >= 23 or local_h < 6:
                return "night 23-06L"
            if local_h < 7:
                return "early 06-07L"
            if local_h < 10:
                return "peak 07-10L"
            if local_h < 16:
                return "offpeak 10-16L"
            if local_h < 19:
                return "peak 16-19L"
            return "evening 19-23L"
        per_line_med = {}
        per_line_day = {}
        for W in (30, 60, 90):
            vals = defaultdict(list)
            per_line_min = defaultdict(list)
            t = dt.datetime(2026, 9, 26, 1, 0, tzinfo=UTC)
            while t < dt.datetime(2026, 9, 27, 1, 0, tzinfo=UTC):
                lh = (t.hour + UTC_OFFSET_H) % 24
                for lid, lst in due.items():
                    times = [d for d, _ in lst]
                    n = bisect.bisect_right(times, t) - bisect.bisect_right(times, t - dt.timedelta(minutes=W))
                    vals[bucket(lh)].append(n)
                    if 7 <= lh < 19:
                        per_line_min[lid].append(n)
                t += dt.timedelta(minutes=15)
            print(f"-- W={W} min: per (line, 15-min step) counts, by local time band")
            for b, v in vals.items():
                print(f"   {b:16s} p10={pct(v,10)} p25={pct(v,25)} p50={pct(v,50)} p75={pct(v,75)} p90={pct(v,90)} "
                      f"share>=4={sum(1 for x in v if x>=4)/len(v):.2f} share>=6={sum(1 for x in v if x>=6)/len(v):.2f} "
                      f"share>=8={sum(1 for x in v if x>=8)/len(v):.2f}")
            if W == 60:
                per_line_med = {l: statistics.median(v) for l, v in per_line_min.items() if v}
        meds = sorted(per_line_med.values())
        print(f"per-line median trains due per 60 min, 07-19 local: p10={pct(meds,10)} p25={pct(meds,25)} "
              f"p50={pct(meds,50)} p75={pct(meds,75)} p90={pct(meds,90)} max={meds[-1]}")
        for mn in (3, 4, 6, 8):
            print(f"   lines whose daytime median >= {mn}: {sum(1 for m in meds if m >= mn)}/{len(meds)}")
        daytot = sorted(len(v) for v in due.values())
        print(f"relevant trains per line per day: p10={pct(daytot,10)} p50={pct(daytot,50)} p90={pct(daytot,90)} "
              f"max={daytot[-1]} zero={sum(1 for x in daytot if x==0)}")
        print("conwy per-day:", len(due.get("tfw-conwy-valley", [])))

    final_now = dt.datetime(2026, 9, 27, 2, 3, tzinfo=UTC)
    if "outcomes" in sections:
        print("\n== outcomes (end-of-day truth, relevant trains due 06:00Z-00:30Z) ==")
        c = Counter()
        seen = set()
        silent_uids = set()
        for lid, lst in due.items():
            for d, uid in lst:
                if not (ANALYSIS_START <= d < ANALYSIS_END):
                    continue
                cls, _ = outcome_at(uid, d, lines[lid], by_uid, final_now)
                c["line_train:" + cls] += 1
                if uid in seen:
                    continue
                seen.add(uid)
                trs = by_uid.get(uid, [])
                act = any(t["act"] for t in trs)
                mov = any(t["mov"] for t in trs)
                can = any(t["canx"] for t in trs)
                rein = any(t["reinst"] for t in trs)
                key = ("A" if act else "-") + ("M" if mov else "-") + ("C" if can else "-") + ("R" if rein else "-")
                c["uid:" + key] += 1
                if not act and not mov and not can:
                    silent_uids.add(uid)
        tot_lt = sum(v for k, v in c.items() if k.startswith("line_train:"))
        for k, v in sorted(c.items()):
            den = tot_lt if k.startswith("line_train:") else len(seen)
            print(f"   {k:28s} {v:7d}  {v/den:6.2%}")
        print(f"   distinct uids={len(seen)}  silent (no 0001/0002/0003)={len(silent_uids)}")
        # silent uids: operator breakdown and in-schedule check
        opc = Counter(ops.get(u, ("?", ""))[0] for u in silent_uids)
        print("   silent by operator (top 12):", opc.most_common(12))
        allc = Counter(ops.get(u, ("?", ""))[0] for u in seen)
        print("   silent rate by operator (>=200 uids):",
              sorted(((o, round(opc[o] / n, 3), n) for o, n in allc.items() if n >= 200), key=lambda x: -x[1])[:15])
        # explicit cancellations with and without movements
        ex_nomov = sum(1 for u in seen for t in by_uid.get(u, []) if t["canx"] and not t["mov"])
        ex_mov = sum(1 for u in seen for t in by_uid.get(u, []) if t["canx"] and t["mov"])
        print(f"   train_ids with 0002: no 0003 at all={ex_nomov}, with 0003 (en-route/partial)={ex_mov}")

    if "lag" in sections:
        print("\n== lag ==")
        lag = []
        for trs in by_uid.values():
            for t in trs:
                for (rec, pl, ac, crs, ev) in t["mov"]:
                    if ac and ANALYSIS_START <= ac < ANALYSIS_END:
                        lag.append((rec - ac).total_seconds())
        print(f"0003 received - actual (s): n={len(lag)} p50={pct(lag,50)} p90={pct(lag,90)} p99={pct(lag,99)} "
              f"p99.9={pct(lag,99.9)} share>300s={sum(1 for x in lag if x>300)/len(lag):.4f}")
        lead, canx_lead, first_ev = [], [], []
        for lid, lst in due.items():
            for d, uid in lst:
                if not (ANALYSIS_START + dt.timedelta(hours=2) <= d < ANALYSIS_END):
                    continue
                for t in by_uid.get(uid, []):
                    if t["act"]:
                        lead.append((d - t["act"]).total_seconds() / 60)
                    for rec, _ in t["canx"]:
                        canx_lead.append((d - rec).total_seconds() / 60)
        print(f"activation lead before line due (min): n={len(lead)} p1={pct(lead,1)} p5={pct(lead,5)} "
              f"p50={pct(lead,50)} p95={pct(lead,95)} share<0={sum(1 for x in lead if x<0)/len(lead):.4f} "
              f"share<-5={sum(1 for x in lead if x<-5)/len(lead):.4f} share<-10={sum(1 for x in lead if x<-10)/len(lead):.4f}")
        print(f"0002 received before line due (min, +ve = before): n={len(canx_lead)} p5={pct(canx_lead,5)} "
              f"p25={pct(canx_lead,25)} p50={pct(canx_lead,50)} p75={pct(canx_lead,75)} "
              f"share_after_due={sum(1 for x in canx_lead if x<0)/max(1,len(canx_lead)):.3f}")

    if "simulate" in sections:
        print("\n== simulate: windowed classification with info received by t, vs end-of-day truth ==")
        for W, G in [(60, 5), (60, 10), (60, 15), (30, 10), (90, 10)]:
            conf = Counter()
            sev = Counter()
            n_windows = 0
            below = Counter()
            t = ANALYSIS_START + dt.timedelta(minutes=W + G)
            while t < ANALYSIS_END:
                lo, hi = t - dt.timedelta(minutes=W + G), t - dt.timedelta(minutes=G)
                for lid, lst in due.items():
                    tot = canc = late = pres = 0
                    for d, uid in lst:
                        if not (lo < d <= hi):
                            continue
                        cls, _ = outcome_at(uid, d, lines[lid], by_uid, t)
                        truth, _ = outcome_at(uid, d, lines[lid], by_uid, final_now)
                        conf[(cls, truth)] += 1
                        tot += 1
                        if cls in ("cancelled_explicit", "presumed_cancelled"):
                            canc += 1
                        if cls == "presumed_cancelled":
                            pres += 1
                        if cls in ("late", "overdue"):
                            late += 1
                    n_windows += 1
                    for mn in (3, 4, 6, 8):
                        if tot < mn:
                            below[mn] += 1
                    if tot >= 6:
                        cr, lr = canc / tot, late / tot
                        s = ("PartSuspended" if cr >= 0.60 else "Reduced" if cr >= 0.25 else
                             "Severe" if lr >= 0.5 else "Minor" if lr >= 0.25 else "Good")
                        sev[s] += 1
                        if pres and (canc - pres) / tot < 0.25 <= cr:
                            sev["cancel_sev_needs_presumed"] += 1
                t += dt.timedelta(minutes=15)
            tot = sum(conf.values())
            print(f"-- W={W} grace={G}: line-train evaluations={tot}, line-windows={n_windows}")
            for mn in (3, 4, 6, 8):
                print(f"   windows below min {mn}: {below[mn]/n_windows:.2%}")
            print("   severity (min 6):", dict(sev))
            for cls in ["ran", "late", "overdue", "unknown_activated", "cancelled_explicit", "presumed_cancelled"]:
                row = {k[1]: v for k, v in conf.items() if k[0] == cls}
                n = sum(row.values())
                if n:
                    print(f"   at-t {cls:20s} n={n:7d} ({n/tot:6.2%}) -> end-of-day truth: "
                          + ", ".join(f"{k}={v/n:.1%}" for k, v in sorted(row.items(), key=lambda x: -x[1])))

    if "simulate2" in sections:
        print("\n== simulate2: handling of activated-but-not-yet-reached trains; severity gates (W=60, grace=10) ==")
        W, G = 60, 10
        for margin, excl in [(0, False), (15, True), (30, True), (10**6, True)]:
            conf = Counter()
            sev = defaultdict(Counter)
            per_line = defaultdict(Counter)
            t = ANALYSIS_START + dt.timedelta(minutes=W + G)
            while t < ANALYSIS_END:
                lo, hi = t - dt.timedelta(minutes=W + G), t - dt.timedelta(minutes=G)
                lh = (t.hour + UTC_OFFSET_H) % 24
                for lid, lst in due.items():
                    tot = canc = late = pres = 0
                    for d, uid in lst:
                        if not (lo < d <= hi):
                            continue
                        cls, _ = outcome_at(uid, d, lines[lid], by_uid, t, margin, excl)
                        truth, _ = outcome_at(uid, d, lines[lid], by_uid, final_now)
                        conf[(cls, truth)] += 1
                        if cls == "pending":
                            continue
                        tot += 1
                        canc += cls in ("cancelled_explicit", "presumed_cancelled")
                        late += cls in ("late", "overdue")
                    for gate, (mn, mk) in {"n>=6": (6, 0), "n>=6,k>=3": (6, 3), "n>=8,k>=3": (8, 3), "n>=4,k>=3": (4, 3)}.items():
                        if tot < mn:
                            continue
                        cr, lr = canc / tot, late / tot
                        if cr >= 0.60 and canc >= mk:
                            sv = "PartSuspended"
                        elif cr >= 0.25 and canc >= mk:
                            sv = "Reduced"
                        elif lr >= 0.5 and late >= mk:
                            sv = "Severe"
                        elif lr >= 0.25 and late >= mk:
                            sv = "Minor"
                        else:
                            sv = "Good"
                        sev[gate][sv] += 1
                        if gate == "n>=6,k>=3" and 7 <= lh < 22:
                            per_line[lid][sv] += 1
                t += dt.timedelta(minutes=15)
            tot = sum(conf.values())
            print(f"-- overdue_margin={margin} pending_excluded={excl}")
            for cls in ["late", "overdue", "unknown_activated", "pending"]:
                row = {k[1]: v for k, v in conf.items() if k[0] == cls}
                n = sum(row.values())
                if n:
                    print(f"   at-t {cls:18s} n={n:7d} ({n/tot:6.2%}) -> truth: "
                          + ", ".join(f"{k}={v/n:.1%}" for k, v in sorted(row.items(), key=lambda x: -x[1])))
            late_truth = sum(v for k, v in conf.items() if k[1] == "late")
            late_caught = sum(v for k, v in conf.items() if k[1] == "late" and k[0] in ("late", "overdue"))
            print(f"   truth-late recall at t: {late_caught/late_truth:.1%}")
            for gate, c in sev.items():
                n = sum(c.values())
                print(f"   severity {gate:10s} windows={n:6d} " + " ".join(f"{k}={v/n:.2%}" for k, v in sorted(c.items())))
            shares = sorted(((1 - c["Good"] / sum(c.values())), lid) for lid, c in per_line.items() if sum(c.values()) >= 20)
            print(f"   lines (n>=6,k>=3, 07-22L): share of windows not Good: p50={pct([x for x, _ in shares],50):.2f} "
                  f"p90={pct([x for x, _ in shares],90):.2f} max={shares[-1][0]:.2f}; top5={[(l, round(x,2)) for x, l in shares[-5:]]}")

    if "perline" in sections:
        print("\n== perline: observability (activated, not cancelled, ever reached the line) and day rates vs LDBWS ==")
        ld = defaultdict(Counter)
        for r in csv.DictReader(open(os.path.join(export_dir, "ldbws_hh.csv"))):
            ld[r["line_id"]]["total"] += int(r["total"])
            ld[r["line_id"]]["delayed"] += int(r["delayed"])
            ld[r["line_id"]]["cancelled"] += int(r["cancelled"])
        obs, rows = [], []
        for lid, lst in due.items():
            c = Counter()
            for d, uid in lst:
                if not (ANALYSIS_START <= d < ANALYSIS_END):
                    continue
                cls, _ = outcome_at(uid, d, lines[lid], by_uid, final_now, 10**6, True)
                c[cls] += 1
            running = c["ran"] + c["late"] + c["overdue"] + c["pending"]
            if running < 20:
                continue
            o = (c["ran"] + c["late"]) / running
            obs.append((o, lid))
            tot = sum(c.values())
            rows.append((lid, tot, o, c["late"] / max(1, c["ran"] + c["late"]),
                         (c["cancelled_explicit"] + c["presumed_cancelled"]) / tot,
                         ld[lid]["total"], ld[lid]["delayed"] / max(1, ld[lid]["total"]),
                         ld[lid]["cancelled"] / max(1, ld[lid]["total"])))
        obs.sort()
        ov = [o for o, _ in obs]
        print(f"lines={len(obs)} observability p5={pct(ov,5):.3f} p10={pct(ov,10):.3f} p25={pct(ov,25):.3f} p50={pct(ov,50):.3f}")
        for th in (0.8, 0.9, 0.95):
            print(f"   lines with observability < {th}: {sum(1 for o in ov if o < th)}")
        print("   worst 10:", [(l, round(o, 3)) for o, l in obs[:10]])
        both = [r for r in rows if r[5] >= 50]
        fc_l = [r[3] for r in both]; ld_l = [r[6] for r in both]
        fc_c = [r[4] for r in both]; ld_c = [r[7] for r in both]
        def corr(x, y):
            mx, my = statistics.mean(x), statistics.mean(y)
            sx = sum((a - mx) ** 2 for a in x) ** .5; sy = sum((b - my) ** 2 for b in y) ** .5
            return sum((a - mx) * (b - my) for a, b in zip(x, y)) / (sx * sy)
        print(f"   lines with >=50 LDBWS services: {len(both)}; late-rate FC p50={pct(fc_l,50):.3f} LDBWS p50={pct(ld_l,50):.3f} "
              f"corr={corr(fc_l, ld_l):.2f}; cancel-rate FC p50={pct(fc_c,50):.3f} LDBWS p50={pct(ld_c,50):.3f} corr={corr(fc_c, ld_c):.2f}")
        for r in sorted(both, key=lambda r: -r[3])[:8]:
            print(f"   {r[0]:32s} fc_n={r[1]:4d} obs={r[2]:.2f} fc_late={r[3]:.3f} fc_canx={r[4]:.3f} ldbws_n={r[5]:4d} ldbws_late={r[6]:.3f} ldbws_canx={r[7]:.3f}")

    if "escalate" in sections:
        # line_status_history timeline (whole statuses array per change); hist0.csv = last row before the day
        #   hist*.csv: line_id, computed_at, sev (Severity discriminant), dq, ss
        RANK = {10: 0, 0: 1, 12: 1, 13: 1, 22: 1, 4: 2, 5: 2, 7: 3, 9: 3, 14: 3, 20: 3,
                1: 4, 2: 4, 3: 4, 6: 4, 8: 4, 11: 4, 21: 4, 23: 4}
        snaps = defaultdict(lambda: defaultdict(list))
        for fn in ("hist0.csv", "hist.csv"):
            for r in csv.DictReader(open(os.path.join(export_dir, fn))):
                snaps[r["line_id"]][ts(r["computed_at"])].append((RANK.get(int(r["sev"]), 0), r["dq"]))
        timeline = {lid: sorted((t, max(x[0] for x in v), {x[1] for x in v}) for t, v in d.items())
                    for lid, d in snaps.items()}

        def current(lid, t):
            tl = timeline.get(lid)
            if not tl:
                return 0, {"none"}
            i = bisect.bisect_right([x[0] for x in tl], t) - 1
            return (tl[i][1], tl[i][2]) if i >= 0 else (0, {"none"})
        print("\n== escalate: how often the recent window would escalate the live severity (W=60, grace=10, "
              "no silence-based lateness) ==")
        W, G = 60, 10
        for gate, (mn, mk, pres_ok) in {"n>=6,k>=3": (6, 3, True), "n>=6,k>=3,explicit-only": (6, 3, False),
                                         "n>=8,k>=3": (8, 3, True), "n>=6,k>=2": (6, 2, True)}.items():
            esc = Counter()
            evals = 0
            line_hours = set()
            by_dq = Counter()
            t = ANALYSIS_START + dt.timedelta(minutes=W + G)
            while t < ANALYSIS_END:
                lo, hi = t - dt.timedelta(minutes=W + G), t - dt.timedelta(minutes=G)
                for lid, lst in due.items():
                    tot = canc = late = 0
                    for d, uid in lst:
                        if not (lo < d <= hi):
                            continue
                        cls, _ = outcome_at(uid, d, lines[lid], by_uid, t, 10**6, True)
                        if cls == "pending" or (cls == "presumed_cancelled" and not pres_ok):
                            continue
                        tot += 1
                        canc += cls in ("cancelled_explicit", "presumed_cancelled")
                        late += cls in ("late", "overdue")
                    if tot < mn:
                        continue
                    evals += 1
                    cr, lr = canc / tot, late / tot
                    if cr >= 0.60 and canc >= mk:
                        fr, name = 4, "PartSuspended"
                    elif lr >= 0.5 and late >= mk:
                        fr, name = 4, "Severe"
                    elif cr >= 0.25 and canc >= mk:
                        fr, name = 3, "Reduced"
                    elif lr >= 0.25 and late >= mk:
                        fr, name = 3, "Minor"
                    else:
                        continue
                    cur, dqs = current(lid, t)
                    if fr > cur:
                        esc[f"{name} over rank {cur}"] += 1
                        line_hours.add((lid, t.replace(minute=0)))
                        by_dq["/".join(sorted(dqs))] += 1
                t += dt.timedelta(minutes=15)
            n = sum(esc.values())
            print(f"-- {gate}: evaluable line-windows={evals}, escalations={n} ({n/max(1,evals):.2%}), "
                  f"distinct line-hours={len(line_hours)}, lines={len({l for l, _ in line_hours})}")
            print("   ", dict(esc.most_common()))
            print("    current provenance when escalated:", dict(by_dq.most_common(6)))

    if "ldbws" in sections:
        print("\n== LDBWS half-hourly deduped stats on the same day (per line-half-hour) ==")
        tot = Counter()
        rows = list(csv.DictReader(open(os.path.join(export_dir, "ldbws_hh.csv"))))
        per = [int(r["total"]) for r in rows]
        print(f"LDBWS distinct services per line-half-hour: n={len(per)} p50={pct(per,50)} p90={pct(per,90)}")
        for r in rows:
            tot["total"] += int(r["total"])
            tot["delayed"] += int(r["delayed"])
            tot["cancelled"] += int(r["cancelled"])
        print("LDBWS day totals:", dict(tot), f"cancel_rate={tot['cancelled']/tot['total']:.3%} delay_rate={tot['delayed']/tot['total']:.3%}")


if __name__ == "__main__":
    main()
