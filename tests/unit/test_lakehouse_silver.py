"""Silver row and set stages against the deterministic scenario (no Spark)."""

from __future__ import annotations

import json
import math
import random

import pytest

from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse.bronze import coordinates
from fleetforge.lakehouse.reference import FIRMWARE_TOPIC, ROBOT_EVENTS_TOPIC, build_reference
from fleetforge.lakehouse.silver import (
    EVENT_COLUMNS,
    OUTCOME_COLUMNS,
    TELEMETRY_TOPIC,
    SchemaArchiveIncomplete,
    build,
    build_id,
    content_fingerprint,
    parse_writer_schemas,
    prepare,
    wrap_angle,
)

SCENARIO = fx.scenario()
WRITERS = parse_writer_schemas(fx.schema_archive())


def _silver(runs=(1, 2), shuffle_seed=None):
    rows = fx.bronze_rows(SCENARIO, runs)
    ref = build_reference([r for r in rows if r["source_topic"] == FIRMWARE_TOPIC],
                          [r for r in rows if r["source_topic"] == ROBOT_EVENTS_TOPIC])  # fmt: skip
    tel = [r for r in rows if r["source_topic"] == TELEMETRY_TOPIC]
    if shuffle_seed is not None:
        random.Random(shuffle_seed).shuffle(tel)
    prepared = [prepare(r, WRITERS, ref) for r in tel]
    return tel, build(prepared, build_id=build_id(1, ref.fingerprint()))


def _expected():
    return {
        (fx.TOPIC_IDS[TELEMETRY_TOPIC], s.record.partition, s.record.offset): s.expect
        for s in SCENARIO
        if s.expect is not None
    }


def test_every_record_gets_exactly_the_outcome_the_scenario_states():
    tel, silver = _silver()
    expected = _expected()
    assert len(silver.outcomes) == len(tel) == len(expected)  # reconciliation: none lost
    got = {coordinates(o): o for o in silver.outcomes}
    assert set(got) == set(expected)
    for coords, exp in expected.items():
        o = got[coords]
        assert (o["outcome"], o["reason_code"]) == (exp.outcome, exp.reason), (coords, exp.note)
        assert (o["superseded_by"] is not None) == exp.superseded, (coords, exp.note)


def test_accepted_events_carry_ordering_flags_and_field_repairs():
    _, silver = _silver()
    by_coords = {coordinates(e): e for e in silver.events}
    for coords, exp in _expected().items():
        if exp.outcome != "ACCEPTED":
            assert coords not in by_coords
            continue
        e = by_coords[coords]
        assert e["arrived_out_of_order"] is exp.out_of_order, (coords, exp.note)
        assert tuple(e["quality_flags"]) == exp.flags, (coords, exp.note)
        assert e["is_current"] is (not exp.superseded)


def test_rows_match_the_table_specifications_exactly():
    _, silver = _silver()
    assert all(tuple(e) == EVENT_COLUMNS for e in silver.events)
    assert all(tuple(o) == OUTCOME_COLUMNS for o in silver.outcomes)
    assert len({e["event_id"] for e in silver.events}) == len(silver.events)  # identity


def test_firmware_units_are_converted_exactly():
    _, silver = _silver()
    b0 = next(e for e in silver.events if e["event_id"] == fx.uid(f"{fx.BOOT_B1}:0"))
    assert (b0["battery_pct"], b0["x_m"], b0["y_m"]) == (50.0, 1.234, -0.25)
    assert (b0["source_battery_unit"], b0["source_position_unit"]) == ("FRACTION", "MM")
    a0 = next(e for e in silver.events if e["event_id"] == fx.uid(f"{fx.BOOT_A1}:0"))
    assert (a0["battery_pct"], a0["x_m"], a0["source_position_unit"]) == (80.0, 1.0, "M")


def test_out_of_range_is_judged_after_conversion():
    tel, silver = _silver()
    b3 = next(o for o in silver.outcomes if o["event_id"] == fx.uid(f"{fx.BOOT_B1}:3"))
    assert b3["reason_code"] == "BATTERY_OUT_OF_RANGE"
    assert "120" in json.loads(b3["errors"])[0]["message"]


def test_corrections_supersede_and_the_latest_correction_wins():
    _, silver = _silver()
    s7 = fx.uid(f"{fx.BOOT_A1}:7")
    c1, c2 = fx.correction_id(s7, 1), fx.correction_id(s7, 2)
    events = {e["event_id"]: e for e in silver.events}
    assert s7 not in events  # the original was rejected (battery 150) ...
    original = next(o for o in silver.outcomes if o["event_id"] == s7)
    assert (original["outcome"], original["superseded_by"]) == ("REJECTED", c2)  # ... and resolved
    assert (events[c1]["is_current"], events[c1]["superseded_by"]) == (False, c2)
    assert (events[c2]["is_current"], events[c2]["battery_pct"]) == (True, 76.0)
    current = [e for e in silver.events if e["is_current"]]
    slots = [(e["robot_id"], e["boot_number"], e["sequence_number"]) for e in current]
    assert len(slots) == len(set(slots))  # one current version per ordering slot


def test_duplicates_are_counted_on_the_accepted_event():
    _, silver = _silver()
    events = {e["event_id"]: e for e in silver.events}
    assert events[fx.uid(f"{fx.BOOT_A1}:1")]["duplicate_deliveries"] == 1
    assert events[fx.uid(f"{fx.BOOT_A1}:0")]["duplicate_deliveries"] == 1  # cross-run replay
    dup = [o for o in silver.outcomes if o["outcome"] == "DUPLICATE"]
    assert {o["canonical_event_id"] for o in dup} == {fx.uid(f"{fx.BOOT_A1}:{s}") for s in (0, 1)}


def test_retry_eligibility_follows_the_milestone_2_taxonomy():
    _, silver = _silver()
    retry = {o["reason_code"] for o in silver.outcomes if o["retry_eligible"]}
    assert retry == {"UNKNOWN_ROBOT", "UNKNOWN_FIRMWARE"}
    rejected = [o for o in silver.outcomes if o["outcome"] == "REJECTED"]
    assert all(o["error_category"] == "PERMANENT_DATA" and o["errors"] for o in rejected)
    assert all(json.loads(o["errors"]) for o in rejected)  # valid, non-empty JSON


def test_result_does_not_depend_on_input_order():
    _, a = _silver()
    for seed in range(5):
        _, b = _silver(shuffle_seed=seed)
        assert (b.events, b.outcomes) == (a.events, a.outcomes)


def test_rebuild_is_deterministic_and_bit_identical():
    _, a = _silver()
    _, b = _silver()
    assert a.build_id == b.build_id
    assert repr(a.events) == repr(b.events) and repr(a.outcomes) == repr(b.outcomes)


def test_late_batch_adds_rows_without_rewriting_earlier_decisions():
    """Run 2 arrives a day later. Run-1 records keep their outcome; only corrections
    may change them (superseded_by), which is the point of a correction."""
    _, first = _silver(runs=(1,))
    _, both = _silver(runs=(1, 2))
    later = {coordinates(o): o for o in both.outcomes}
    changed = []
    for o in first.outcomes:
        n = later[coordinates(o)]
        assert (n["outcome"], n["reason_code"]) == (o["outcome"], o["reason_code"])
        if n["superseded_by"] != o["superseded_by"]:
            changed.append(n["event_id"])
    assert changed == [fx.uid(f"{fx.BOOT_A1}:7")]
    assert len(both.events) == len(first.events) + 5  # boot 2 x2, late seq 11, 2 corrections


def test_missing_schema_archive_entry_aborts_instead_of_quarantining():
    rows = fx.bronze_rows(SCENARIO, (1,))
    ref = build_reference([], [])
    framed = next(r for r in rows if r["value_schema_id"] == fx.SCHEMA_V2)
    with pytest.raises(SchemaArchiveIncomplete):
        prepare(framed, {fx.SCHEMA_V1: WRITERS[fx.SCHEMA_V1]}, ref)


def test_fingerprint_ignores_delivery_metadata_and_schema_revision():
    rec = fx.telemetry(fx.ROBOT_A, fx.BOOT_A1, 1, 3)
    later = {**rec, "producer": {"producer_id": "other", "produced_at": fx.T0}}
    v2 = {**rec, "heading_rad": None}
    assert content_fingerprint(rec) == content_fingerprint(later) == content_fingerprint(v2)
    assert content_fingerprint(rec) != content_fingerprint({**rec, "battery_pct": 79.0})
    no_sensors = {**rec, "sensors": None}
    all_null = {**rec, "sensors": {"motor_temp_c": None, "lidar_ok": None}}
    assert content_fingerprint(no_sensors) == content_fingerprint(all_null)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(0.0, 0.0), (math.pi, -math.pi), (-math.pi, -math.pi), (3 * math.pi / 2, -math.pi / 2),
     (-7.0, -7.0 + 2 * math.pi), (1e6, None)],
)  # fmt: skip
def test_wrap_angle_lands_in_half_open_interval(raw, expected):
    w = wrap_angle(raw)
    assert -math.pi <= w < math.pi
    if expected is not None:
        assert w == pytest.approx(expected, abs=1e-12)
    assert math.cos(w) == pytest.approx(math.cos(raw), abs=1e-9)
