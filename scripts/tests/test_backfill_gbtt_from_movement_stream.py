"""Tests for scripts/backfill-gbtt-from-movement-stream.py (no database).

uv run python -m unittest discover -s scripts/tests
"""

import datetime as dt
import importlib.util
import io
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType

SCRIPT = (
    Path(__file__).resolve().parent.parent / "backfill-gbtt-from-movement-stream.py"
)


def load_script() -> ModuleType:
    """Import the hyphenated script as a module."""
    spec = importlib.util.spec_from_file_location("backfill_gbtt", SCRIPT)
    if spec is None or spec.loader is None:
        raise ImportError(SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["backfill_gbtt"] = module
    spec.loader.exec_module(module)
    return module


backfill = load_script()

# The pinned key `trust_schema::dedup::tests::the_key_matches_the_pinned_vector`
# asserts for the same inputs.
PINNED_KEY = "d4a64ed43d9d200e6956cd39f6c1a9390f92b77a5cc2c721b6459992b1b79735"


def payload(**body: str) -> str:
    """Return one stream payload line: a 0003 Movement with `body` merged in."""
    fields = {
        "train_id": "221832406",
        "event_type": "DEPARTURE",
        "loc_stanox": "87212",
        "planned_timestamp": "1787945520000",
        "actual_timestamp": "1787945580000",
        "gbtt_timestamp": "1787945460000",
    }
    fields.update(body)
    return json.dumps({"body": fields, "header": {"msg_type": "0003"}})


def stream(*payloads: str) -> list[str]:
    """Return `redis-cli --raw XRANGE` lines holding `payloads`."""
    lines: list[str] = []
    for index, raw in enumerate(payloads):
        lines += [
            f"1791356487969-{index}\n",
            "payload\n",
            raw + "\n",
            "msg_type\n",
            "0003\n",
        ]
    return lines


class DedupKeyTest(unittest.TestCase):
    """The Python key must equal the Rust one."""

    def test_matches_the_rust_pinned_vector(self) -> None:
        """Same inputs, same SHA-256 as trust_schema::dedup::dedup_key."""
        key = backfill.dedup_key(
            "221832406",
            "0003",
            "DEPARTURE",
            "87212",
            "1787945520000",
            dt.date(2026, 8, 28),
        )
        self.assertEqual(key, PINNED_KEY)


class ReadCandidatesTest(unittest.TestCase):
    """Parsing the stream dump."""

    def test_a_calling_movement_with_a_public_time_is_a_candidate(self) -> None:
        """Key, train id and both raw instants come off the payload."""
        (candidate,) = backfill.read_candidates(stream(payload()), None)
        self.assertEqual(candidate.dedup_key, PINNED_KEY)
        self.assertEqual(candidate.train_id, "221832406")
        self.assertEqual(
            candidate.planned_raw, dt.datetime(2026, 8, 28, 19, 32, tzinfo=dt.UTC)
        )
        self.assertEqual(
            candidate.gbtt_raw, dt.datetime(2026, 8, 28, 19, 31, tzinfo=dt.UTC)
        )

    def test_nothing_to_backfill_is_skipped(self) -> None:
        """A pass, an empty or absent GBTT, no planned time, another type."""
        no_gbtt = json.loads(payload())
        del no_gbtt["body"]["gbtt_timestamp"]
        activation = json.loads(payload())
        activation["header"]["msg_type"] = "0001"
        lines = stream(
            payload(event_type="PASS"),
            payload(gbtt_timestamp=""),
            payload(planned_timestamp=""),
            json.dumps(no_gbtt),
            json.dumps(activation),
            "{not json",
        )
        self.assertEqual(list(backfill.read_candidates(lines, None)), [])

    def test_a_redelivery_is_read_once_and_max_entries_bounds_the_input(self) -> None:
        """Duplicate keys collapse; `max_entries` stops reading early."""
        lines = stream(
            payload(), payload(), payload(train_id="2"), payload(train_id="3")
        )
        self.assertEqual(len(list(backfill.read_candidates(lines, None))), 3)
        self.assertEqual(len(list(backfill.read_candidates(lines, 3))), 2)


class WriteSqlTest(unittest.TestCase):
    """The generated psql script."""

    def render(self, *, apply: bool, chunk_size: int = 2) -> str:
        """Render the script for three candidates."""
        lines = stream(payload(), payload(train_id="2"), payload(train_id="3"))
        candidates = list(backfill.read_candidates(lines, None))
        out = io.StringIO()
        backfill.write_sql(out, candidates, apply=apply, chunk_size=chunk_size)
        return out.getvalue()

    def test_the_default_is_a_dry_run_that_rolls_back(self) -> None:
        """No UPDATE, and the whole thing runs in a rolled-back transaction."""
        sql = self.render(apply=False)
        self.assertNotIn("UPDATE", sql)
        self.assertTrue(sql.rstrip().endswith("ROLLBACK;"))
        self.assertIn("BEGIN;", sql)
        self.assertIn("would_update", sql)

    def test_apply_updates_in_bounded_idempotent_chunks(self) -> None:
        """One UPDATE pair per chunk, each only touching NULLs."""
        sql = self.render(apply=True)
        self.assertNotIn("ROLLBACK", sql)
        self.assertEqual(sql.count("UPDATE train_movement_events"), 2)
        self.assertEqual(sql.count("UPDATE trust_event_backlog"), 2)
        self.assertEqual(sql.count("m.gbtt_timestamp IS NULL\n"), 2)
        self.assertIn("b.chunk = 1", sql)
        self.assertIn("interval '-1 hour'", sql)

    def test_copy_rows_are_tab_separated_and_chunked(self) -> None:
        """Each candidate is one COPY row carrying its chunk number."""
        sql = self.render(apply=False)
        copy = sql.split("FROM stdin;\n", 1)[1].split("\\.\n", 1)[0]
        rows = [row.split("\t") for row in copy.splitlines()]
        self.assertEqual([row[4] for row in rows], ["0", "0", "1"])
        self.assertEqual(rows[0][0], PINNED_KEY)


if __name__ == "__main__":
    unittest.main()
