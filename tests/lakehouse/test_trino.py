"""Trino reads the Silver tables Spark wrote (state left by test_slice.py's last test).

No Spark here: expected values come from the pure reference implementation, so this
phase fits in memory next to Trino. Run with `make -f lakehouse.mk lake-test-trino`.
"""

from __future__ import annotations

from collections import Counter

import pytest

from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse import gold
from fleetforge.lakehouse.silver import TELEMETRY_TOPIC, build

from .test_slice import reference_silver

pytestmark = [pytest.mark.lakehouse, pytest.mark.lakehouse_trino]


def _q(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def _builds(cur):
    return _q(cur, "SELECT build_id, mode, events_snapshot_id, outcomes_snapshot_id "
                   "FROM silver.build_log ORDER BY committed_at")  # fmt: skip


def test_current_version_matches_the_reference(trino_cursor):
    _, prepared = reference_silver((1, 2))
    want = build(prepared, build_id="reference")
    [(events, current)] = _q(trino_cursor, "SELECT count(*), count_if(is_current) "
                                           "FROM silver.telemetry_events")  # fmt: skip
    assert (events, current) == (len(want.events), sum(e["is_current"] for e in want.events))
    by_outcome = dict(_q(trino_cursor, "SELECT outcome || '/' || coalesce(reason_code, '-'), "
                                       "count(*) FROM silver.telemetry_record_outcomes GROUP BY 1"))  # fmt: skip
    assert by_outcome == dict(Counter(f"{o['outcome']}/{o['reason_code'] or '-'}"
                                      for o in want.outcomes))  # fmt: skip


def test_previous_build_is_readable_by_its_logged_snapshot(trino_cursor):
    builds = _builds(trino_cursor)
    assert [b[1] for b in builds] == ["FULL", "INCREMENTAL"]
    [(old,)] = _q(trino_cursor, "SELECT count(*) FROM silver.telemetry_events "
                                f"FOR VERSION AS OF {builds[0][2]}")  # fmt: skip
    _, prepared_run1 = reference_silver((1,))
    assert old == len(build(prepared_run1, build_id="x").events)
    [(pinned,)] = _q(trino_cursor, "SELECT count(*) FROM silver.telemetry_record_outcomes "
                                   f"FOR VERSION AS OF {builds[-1][3]}")  # fmt: skip
    assert pinned == len([s for s in fx.scenario() if s.expect is not None])


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
    # The unknown schema id is UNRESOLVED now, not malformed (ADR 0016).
    assert framings == Counter({"CONFLUENT": 1, "NONE": 1, "TOMBSTONE": 1})
    [(unresolved,)] = _q(trino_cursor, "SELECT count(*) FROM silver.telemetry_record_outcomes "
                                       "WHERE outcome = 'UNRESOLVED'")  # fmt: skip
    assert unresolved == 1


def test_types_are_what_the_spec_documents(trino_cursor):
    cols = dict((r[0], r[1]) for r in _q(trino_cursor, "DESCRIBE silver.telemetry_events"))
    assert cols["observed_at"] == "timestamp(6) with time zone"
    assert cols["quality_flags"] == "array(varchar)"
    assert cols["boot_number"] == "bigint"


def _dicts(cur, sql):
    cur.execute(sql)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def _same(got, want, key):
    """Rows equal by key; float sums may differ in the last bits between engines."""
    got, want = {key(r): r for r in got}, {key(r): r for r in want}
    assert got.keys() == want.keys()
    for k, w in want.items():
        for col, value in w.items():
            if isinstance(value, float):
                assert got[k][col] == pytest.approx(value, rel=1e-12), (k, col)
            else:
                assert got[k][col] == value, (k, col)


def test_dbt_gold_equals_the_reference(trino_cursor):
    _, prepared = reference_silver((1, 2))
    want = build(prepared, build_id="reference")
    [(build_id,)] = _q(trino_cursor, "SELECT build_id FROM silver.build_log "
                                     "ORDER BY committed_at DESC LIMIT 1")  # fmt: skip
    bronze = fx.bronze_rows(fx.scenario(), (1, 2))
    daily = gold.daily_telemetry(want.events, want.outcomes, build_id, gold.key_robots(bronze))
    _same(_dicts(trino_cursor, "SELECT * FROM gold.robot_daily_telemetry"), daily,
          lambda r: (r["observed_date"], r["robot_id"]))  # fmt: skip

    topic_of = {tid: topic for topic, tid in fx.TOPIC_IDS.items()}
    quality = gold.pipeline_quality(want.outcomes, topic_of, build_id)
    _same(_dicts(trino_cursor, "SELECT * FROM gold.pipeline_daily_quality"), quality,
          lambda r: (r["ingest_date"], r["source_topic"], r["outcome"], r["reason_code"]))  # fmt: skip

    telemetry = [r for r in bronze if r["source_topic"] == TELEMETRY_TOPIC]
    rates = gold.pipeline_rates(telemetry, want.outcomes, build_id)
    _same(_dicts(trino_cursor, "SELECT * FROM gold.pipeline_daily_rates"), rates,
          lambda r: (r["ingest_date"], r["source_topic"]))  # fmt: skip


def test_the_scenario_release_matches_the_reference(trino_cursor):
    _, prepared = reference_silver((1, 2))
    want = build(prepared, build_id="reference")
    [release] = _dicts(trino_cursor, "SELECT * FROM gold.training_dataset_releases "
                                     "WHERE release_id = 'scenario-1'")  # fmt: skip
    rows = gold.observations(want.events, "scenario-1", "seed-1", release["silver_build_id"])
    assert release["rows_total"] == len(rows)
    assert release["content_sha256"] == gold.content_hash(rows)
    [(stored,)] = _q(trino_cursor, "SELECT count(*) FROM gold.robot_state_observations "
                                   "FOR VERSION AS OF 'release-scenario-1'")  # fmt: skip
    assert stored == len(rows)
