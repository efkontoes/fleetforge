"""Trino reads the Silver tables Spark wrote (state left by test_slice.py's last test).

No Spark here: expected values come from the pure reference implementation, so this
phase fits in memory next to Trino. Run with `make -f lakehouse.mk lake-test-trino`.
"""

from __future__ import annotations

from collections import Counter

import pytest

from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse.silver import archive_fingerprint, build, build_id

from .test_slice import reference_silver

pytestmark = [pytest.mark.lakehouse, pytest.mark.lakehouse_trino]


def _q(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def _expected(cur, runs):
    """Reference build for the Bronze snapshot that Silver recorded."""
    ref, prepared = reference_silver(runs)
    snap = _q(
        cur,
        """SELECT summary['fleetforge.silver.bronze-snapshot-id']
                      FROM silver."telemetry_events$snapshots" ORDER BY committed_at DESC LIMIT 1""",
    )
    archive_fp = archive_fingerprint(fx.schema_archive())
    return build(prepared, build_id=build_id(int(snap[0][0]), ref.fingerprint(), archive_fp))


def test_current_version_matches_the_reference(trino_cursor):
    want = _expected(trino_cursor, (1, 2))
    [(events, current, build_ids)] = _q(
        trino_cursor,
        """
        SELECT count(*), count_if(is_current), array_agg(DISTINCT silver_build_id)
        FROM silver.telemetry_events""",
    )
    assert (events, current) == (len(want.events), sum(e["is_current"] for e in want.events))
    assert build_ids == [want.build_id]
    rejected = dict(
        _q(
            trino_cursor,
            """
        SELECT reason_code, count(*) FROM silver.telemetry_record_outcomes
        WHERE outcome = 'REJECTED' GROUP BY 1""",
        )
    )
    assert rejected == dict(
        Counter(o["reason_code"] for o in want.outcomes if o["outcome"] == "REJECTED")
    )


def test_previous_version_is_readable_by_snapshot(trino_cursor):
    # One snapshot per published build. (Not filtered by operation: Iceberg reports the
    # first overwrite of an empty table as an append, since it deleted nothing.)
    snaps = _q(
        trino_cursor,
        """SELECT snapshot_id FROM silver."telemetry_events$snapshots"
                                WHERE summary['fleetforge.silver.build-id'] IS NOT NULL
                                ORDER BY committed_at""",
    )
    assert len(snaps) >= 2
    [(old,)] = _q(
        trino_cursor,
        f"SELECT count(*) FROM silver.telemetry_events FOR VERSION AS OF {snaps[-2][0]}",
    )
    _, prepared_run1 = reference_silver((1,))
    assert old == len(build(prepared_run1, build_id="x").events)


def test_quarantine_joins_back_to_the_exact_source_bytes(trino_cursor):
    rows = _q(
        trino_cursor,
        """
        SELECT o.reason_code, b.value_framing, length(b.record_value)
        FROM silver.telemetry_record_outcomes o
        JOIN bronze.kafka_records b
          ON b.kafka_topic_id = o.kafka_topic_id AND b.kafka_partition = o.kafka_partition
         AND b.kafka_offset = o.kafka_offset
        WHERE o.outcome = 'REJECTED' AND o.reason_code = 'MALFORMED_PAYLOAD'""",
    )
    framings = Counter(r[1] for r in rows)
    assert framings == Counter({"CONFLUENT": 2, "NONE": 1, "TOMBSTONE": 1})


def test_types_are_what_the_spec_documents(trino_cursor):
    cols = dict((r[0], r[1]) for r in _q(trino_cursor, "DESCRIBE silver.telemetry_events"))
    assert cols["observed_at"] == "timestamp(6) with time zone"
    assert cols["quality_flags"] == "array(varchar)"
    assert cols["boot_number"] == "bigint"
