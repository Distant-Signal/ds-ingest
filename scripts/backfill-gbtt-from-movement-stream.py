#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI that writes SQL to stdout and a summary to stderr
r"""Backfill gbtt_timestamp from the retained movement-events stream.

One-off repair for the 2026-10-07 bug: trust-backlog-consumer, the primary
writer of `train_movement_events`, dropped TRUST's `gbtt_timestamp`, so
every stored row has a NULL public time. `trust_event_backlog` never kept
it and `raw_body` is `{}`, so the only surviving copy is the Redis
`movement-events` stream (capped, roughly the last 28 hours). Rows older
than that cannot be backfilled.

This script does not touch Redis or Postgres itself. It reads the
`redis-cli --raw XRANGE` output of the stream (taken read-only, by the
operator) and writes a psql script:

  # PAGE the dump. Never run a single `XRANGE movement-events - +`: on
  # 2026-10-07 that one command (~1M entries) grew Redis's client output
  # buffer past the container's 3Gi limit and OOM-killed Redis.
  start=-; : > stream.txt
  while :; do
    page=$(kubectl ... exec deploy/distant-signal-redis -- \
        redis-cli --raw XRANGE movement-events "$start" + COUNT 10000)
    [ -z "$page" ] && break
    printf '%s\n' "$page" >> stream.txt
    start="($(printf '%s\n' "$page" | grep -E '^[0-9]+-[0-9]+$' | tail -n 1)"
  done
  uv run scripts/backfill-gbtt-from-movement-stream.py stream.txt > dry.sql
  kubectl ... exec -i distant-signal-postgres-0 -- \
      psql -U distant_signal -d distant_signal -v ON_ERROR_STOP=1 < dry.sql

The default output is a DRY RUN: it loads the candidates into a temporary
table inside a transaction, reports how many rows would change, and rolls
back. `--apply` instead writes chunked UPDATEs, each committed on its own,
so a run can be interrupted and resumed.

How a stream entry is matched to a row: by the same `dedup_key` the
consumers compute (`trust_schema::dedup::dedup_key`, SHA-256 of train_id,
msg_type, event_type, loc_stanox, the raw planned_timestamp and the raw
planned timestamp's UTC date, NUL-separated), joined through `trains`
(train_id) so each lookup uses the `(trains_id, dedup_key)` unique index
rather than scanning the table. The stored value is the raw GBTT time moved
by the same correction the consumer applied to that row's planned time,
recovered as `stored planned_timestamp - raw planned_timestamp`, so this
script never re-derives the local-as-UTC correction itself. Only an offset
of 0 or +/-1 hour is trusted; anything else is skipped and counted.

Rows written by the backlog-match or uid-less REPLAY paths use different
dedup keys (no loc_stanox, a service_date date) and are not matched; they
keep NULL. `trust_event_backlog` rows (unique `dedup_key`) are backfilled
too, so a later replay carries the public time.

Idempotent (`WHERE gbtt_timestamp IS NULL`) and bounded (`--chunk-size`
keys per committed UPDATE, `--max-entries` cap on the input). Rows the
fixed consumer has already written with a public time are left alone.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

CALLING_EVENT_TYPES = frozenset({"ARRIVAL", "DEPARTURE"})
DEFAULT_CHUNK_SIZE = 5000


@dataclass(frozen=True)
class Candidate:
    """One stream Movement that carried a public time."""

    dedup_key: str
    train_id: str
    planned_raw: dt.datetime
    gbtt_raw: dt.datetime


def dedup_key(  # noqa: PLR0913, PLR0917  # mirrors trust_schema::dedup::dedup_key's signature
    train_id: str,
    msg_type: str,
    event_type: str | None,
    loc_stanox: str | None,
    planned_timestamp: str | None,
    event_date: dt.date,
) -> str:
    """Return `trust_schema::dedup::dedup_key` for these fields."""
    hasher = hashlib.sha256()
    for field in (
        train_id,
        msg_type,
        event_type or "",
        loc_stanox or "",
        planned_timestamp or "",
        event_date.isoformat(),
    ):
        hasher.update(field.encode())
        hasher.update(b"\0")
    return hasher.hexdigest()


def epoch_millis(raw: object) -> dt.datetime | None:
    """Parse a TRUST epoch-millis string as its raw UTC instant."""
    if not isinstance(raw, str):
        return None
    try:
        millis = int(raw.strip())
    except ValueError:
        return None
    if millis <= 0:
        return None
    return dt.datetime.fromtimestamp(millis / 1000, tz=dt.UTC)


def movement_body(payload: object) -> dict[str, object] | None:
    """Return a TRUST 0003 (Movement) envelope's body, else None."""
    if not isinstance(payload, dict):
        return None
    header = payload.get("header")
    body = payload.get("body")
    if not isinstance(header, dict) or not isinstance(body, dict):
        return None
    if header.get("msg_type") != "0003":
        return None
    return body


def candidate_from_payload(payload: object) -> Candidate | None:
    """Return the backfill candidate a stream payload carries, if any.

    Only a calling Movement (ARRIVAL/DEPARTURE) with a parseable planned
    and GBTT time can be matched and has something to backfill.
    """
    body = movement_body(payload)
    if body is None:
        return None
    train_id = body.get("train_id")
    event_type = body.get("event_type")
    planned_raw_str = body.get("planned_timestamp")
    loc_stanox = body.get("loc_stanox")
    if not isinstance(train_id, str) or event_type not in CALLING_EVENT_TYPES:
        return None
    if loc_stanox is not None and not isinstance(loc_stanox, str):
        return None
    planned = epoch_millis(planned_raw_str)
    gbtt = epoch_millis(body.get("gbtt_timestamp"))
    if planned is None or gbtt is None or not isinstance(planned_raw_str, str):
        return None
    # `trust_schema::dedup::event_date` for a Movement: the planned
    # timestamp's raw UTC date (planned parsed, so it is the first choice).
    key = dedup_key(
        train_id,
        "0003",
        str(event_type),
        loc_stanox,
        planned_raw_str,
        planned.date(),
    )
    return Candidate(key, train_id, planned, gbtt)


def read_candidates(
    lines: Iterable[str], max_entries: int | None
) -> Iterator[Candidate]:
    """Yield the candidates in `redis-cli --raw XRANGE` output.

    Every field value sits on its own line; a payload is the line holding a
    JSON object. Entry ids and the `msg_type`/`payload` field names are
    skipped. A key seen twice (a redelivered message) is yielded once.
    """
    seen: set[str] = set()
    payloads = 0
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        payloads += 1
        if max_entries is not None and payloads > max_entries:
            return
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        candidate = candidate_from_payload(payload)
        if candidate is None or candidate.dedup_key in seen:
            continue
        seen.add(candidate.dedup_key)
        yield candidate


def copy_value(value: str) -> str:
    """Escape a value for COPY's text format (no tabs/newlines expected)."""
    return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


# The offset the consumer's correction applied to this row's planned time.
OFFSET = "(m.planned_timestamp - b.planned_raw)"
TRUSTED_OFFSET = f"{OFFSET} IN (interval '0', interval '1 hour', interval '-1 hour')"
BACKLOG_OFFSET = "(e.planned_timestamp - b.planned_raw)"
BACKLOG_TRUSTED_OFFSET = (
    f"{BACKLOG_OFFSET} IN (interval '0', interval '1 hour', interval '-1 hour')"
)
MOVEMENT_JOIN = (
    "FROM gbtt_backfill b "
    "JOIN trains t ON t.train_id = b.train_id AND t.train_id IS NOT NULL "
    "JOIN train_movement_events m "
    "ON m.trains_id = t.id AND m.dedup_key = b.dedup_key AND m.msg_type = '0003'"
)


def write_sql(
    out: TextIO,
    candidates: list[Candidate],
    *,
    apply: bool,
    chunk_size: int,
) -> None:
    """Write the dry-run or apply psql script for `candidates`."""
    out.write("\\set ON_ERROR_STOP on\n")
    out.write("SET lock_timeout = '5s';\nSET statement_timeout = '5min';\n")
    if not apply:
        out.write("BEGIN;\n")
    out.write(
        "CREATE TEMP TABLE gbtt_backfill (\n"
        "    dedup_key TEXT PRIMARY KEY,\n"
        "    train_id TEXT NOT NULL,\n"
        "    planned_raw TIMESTAMPTZ NOT NULL,\n"
        "    gbtt_raw TIMESTAMPTZ NOT NULL,\n"
        "    chunk INT NOT NULL\n"
        ");\n"
        "COPY gbtt_backfill (dedup_key, train_id, planned_raw, gbtt_raw, chunk)\n"
        "FROM stdin;\n",
    )
    for index, candidate in enumerate(candidates):
        fields = (
            candidate.dedup_key,
            copy_value(candidate.train_id),
            candidate.planned_raw.isoformat(),
            candidate.gbtt_raw.isoformat(),
            str(index // chunk_size),
        )
        out.write("\t".join(fields) + "\n")
    out.write("\\.\n")
    out.write("CREATE INDEX ON gbtt_backfill (chunk);\nANALYZE gbtt_backfill;\n")
    # Every interpolated piece below is a module constant or an int: no
    # input from the stream is ever spliced into SQL (it goes through COPY).
    out.write(
        "SELECT\n"  # noqa: S608  # constants only, see above
        "    (SELECT count(*) FROM gbtt_backfill) AS stream_candidates,\n"
        "    count(*) AS matched_movement_rows,\n"
        "    count(*) FILTER (WHERE m.gbtt_timestamp IS NOT NULL) AS already_set,\n"
        f"    count(*) FILTER (WHERE m.gbtt_timestamp IS NULL AND {TRUSTED_OFFSET})"
        " AS would_update,\n"
        "    count(*) FILTER (WHERE m.gbtt_timestamp IS NULL AND NOT "
        f"{TRUSTED_OFFSET}) AS skipped_untrusted_offset,\n"
        "    min(m.received_at) AS oldest_matched,\n"
        "    max(m.received_at) AS newest_matched\n"
        f"{MOVEMENT_JOIN};\n",
    )
    out.write(
        "SELECT count(*) AS matched_backlog_rows,\n"
        "    count(*) FILTER (WHERE e.gbtt_timestamp IS NULL AND "
        f"{BACKLOG_TRUSTED_OFFSET}) AS backlog_would_update\n"
        "FROM gbtt_backfill b JOIN trust_event_backlog e ON e.dedup_key = b.dedup_key "
        "AND e.msg_type = '0003';\n",
    )
    if not apply:
        out.write("ROLLBACK;\n")
        return
    chunks = (len(candidates) + chunk_size - 1) // chunk_size
    out.writelines(
        (
            f"\\echo chunk {chunk + 1}/{chunks}\n"
            "UPDATE train_movement_events m\n"
            f"SET gbtt_timestamp = b.gbtt_raw + {OFFSET}\n"
            "FROM gbtt_backfill b JOIN trains t ON t.train_id = b.train_id "
            "AND t.train_id IS NOT NULL\n"
            "WHERE m.trains_id = t.id AND m.dedup_key = b.dedup_key "
            "AND m.msg_type = '0003'\n"
            f"  AND b.chunk = {chunk} AND m.gbtt_timestamp IS NULL\n"
            f"  AND {TRUSTED_OFFSET};\n"
            "UPDATE trust_event_backlog e\n"
            f"SET gbtt_timestamp = b.gbtt_raw + {BACKLOG_OFFSET}\n"
            "FROM gbtt_backfill b\n"
            "WHERE e.dedup_key = b.dedup_key AND e.msg_type = '0003'\n"
            f"  AND b.chunk = {chunk} AND e.gbtt_timestamp IS NULL\n"
            f"  AND {BACKLOG_TRUSTED_OFFSET};\n"
        )
        for chunk in range(chunks)
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0] if __doc__ else None,
    )
    parser.add_argument(
        "stream_dump",
        type=Path,
        help=(
            "concatenated paged `redis-cli --raw XRANGE movement-events "
            "<start> + COUNT 10000` output ('-' for stdin); see the module "
            "docstring, and never dump the stream with one XRANGE"
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the committing UPDATEs instead of the dry-run report",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"keys per committed UPDATE (default {DEFAULT_CHUNK_SIZE})",
    )
    parser.add_argument(
        "--max-entries",
        type=int,
        default=None,
        help="read at most this many stream payloads",
    )
    args = parser.parse_args(argv)
    if args.chunk_size < 1:
        parser.error("--chunk-size must be at least 1")
    return args


def main(argv: list[str]) -> int:
    """Write the psql script for the stream dump named in `argv`."""
    args = parse_args(argv)
    stream_dump: Path = args.stream_dump
    if str(stream_dump) == "-":
        candidates = list(read_candidates(sys.stdin, args.max_entries))
    else:
        with stream_dump.open(encoding="utf-8") as lines:
            candidates = list(read_candidates(lines, args.max_entries))
    write_sql(sys.stdout, candidates, apply=args.apply, chunk_size=args.chunk_size)
    mode = "APPLY" if args.apply else "dry run"
    print(f"{mode}: {len(candidates)} stream candidates", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
