"""Gold reference rules: completeness, distance, finality, rates, observations and hashing."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from fleetforge.lakehouse import gold

T0 = datetime(2026, 3, 1, 23, 59, 50, tzinfo=UTC)


def event(seq, at, *, robot="r1", boot="b1", boot_number=1, x=0.0, y=0.0, **extra):
    row = {
        "event_id": f"{robot}-{boot}-{seq}", "robot_id": robot, "boot_id": boot,
        "boot_number": boot_number, "sequence_number": seq, "observed_at": at,
        "robot_model": "AMR-100", "firmware_version": "1.0.0", "battery_pct": 50.0,
        "x_m": x, "y_m": y, "heading_rad": None, "motor_temp_c": None, "lidar_ok": True,
        "quality_flags": [], "is_current": True, "correction_of": None,
        "arrived_out_of_order": False, "duplicate_deliveries": 0,
        "source_topic": "telemetry", "kafka_topic_id": "t", "kafka_partition": 0,
        "kafka_offset": seq,
    }  # fmt: skip
    return {**row, **extra}


def outcome(robot="r1", kind="ACCEPTED", retry=False, ingested=T0, reason=None):
    return {"robot_id": robot, "outcome": kind, "retry_eligible": retry, "event_id": None,
            "bronze_ingested_at": ingested, "kafka_topic_id": "t", "kafka_partition": 0,
            "kafka_offset": 99, "reason_code": reason}  # fmt: skip


def by_day(rows):
    return {(r["observed_date"], r["robot_id"]): r for r in rows}


def test_a_gap_across_midnight_is_counted_on_the_later_day():
    events = [event(0, T0, x=0), event(1, T0 + timedelta(seconds=5), x=3, y=4),
              event(4, T0 + timedelta(seconds=20), x=6, y=8)]  # fmt: skip
    days = by_day(gold.daily_telemetry(events, [outcome()], "b"))
    first, second = days[(T0.date(), "r1")], days[(T0.date() + timedelta(days=1), "r1")]
    assert (first["observed_positions"], first["missing_sequences"]) == (2, 0)
    assert (second["observed_positions"], second["missing_sequences"]) == (1, 2)
    assert second["expected_events"] == 3
    assert first["distance_m"] == 5.0  # the step over the gap is not distance
    assert second["distance_m"] == 0.0


def test_distance_skips_a_clock_regression():
    events = [event(0, T0, x=0), event(1, T0 - timedelta(seconds=1), x=10),
              event(2, T0 + timedelta(seconds=1), x=13)]  # fmt: skip
    (row,) = gold.daily_telemetry(events, [outcome()], "b")
    assert row["distance_m"] == 3.0  # 0 -> 1 goes back in time, so only 1 -> 2 counts


def test_non_current_events_are_left_out():
    events = [event(0, T0), event(1, T0, is_current=False)]
    assert gold.daily_telemetry(events, [outcome()], "b")[0]["events"] == 1


def test_finality_needs_the_window_and_no_open_retries():
    day = datetime(2026, 3, 1, 12, tzinfo=UTC)
    events = [event(0, day), event(0, day, robot="r2", boot="b2")]
    late = day + timedelta(days=20)
    rows = by_day(gold.daily_telemetry(
        events, [outcome(ingested=late), outcome("r2", "REJECTED", True, late)], "b"))  # fmt: skip
    assert rows[(day.date(), "r1")]["is_final"] is True
    assert rows[(day.date(), "r2")]["is_final"] is False  # r2 still has a retry-eligible record
    recent = by_day(gold.daily_telemetry(events, [outcome(ingested=day)], "b"))
    assert not any(r["is_final"] for r in recent.values())
    unresolved = [outcome(ingested=late), outcome(None, "UNRESOLVED", True, late)]
    robotless = by_day(gold.daily_telemetry(events, unresolved, "b"))
    assert not any(r["is_final"] for r in robotless.values())
    keyed = by_day(gold.daily_telemetry(events, unresolved, "b", {("t", 0, 99): "r2"}))
    assert keyed[(day.date(), "r1")]["is_final"] and not keyed[(day.date(), "r2")]["is_final"]


def test_rates_keep_unresolved_out_of_the_denominator():
    bronze = [{"kafka_topic_id": "t", "source_topic": "telemetry", "ingested_at": T0}] * 4
    outs = [outcome(kind="ACCEPTED"), outcome(kind="DUPLICATE"), outcome(kind="REJECTED"),
            outcome(kind="UNRESOLVED", retry=True)]  # fmt: skip
    (row,) = gold.pipeline_rates(bronze, outs, "b")
    assert row["bronze_records"] == row["classified"] + row["unresolved"] == 4
    assert row["invalid_rate"] == row["duplicate_rate"] == pytest.approx(1 / 3)
    (empty,) = gold.pipeline_rates(bronze[:1], [outcome(kind="UNRESOLVED", retry=True)], "b")
    assert empty["classified"] == 0 and empty["invalid_rate"] is None


def test_observations_differentiate_only_valid_steps():
    at = [T0 + timedelta(seconds=s) for s in (0, 2, 4, 4)]
    events = [event(5, at[0], x=0), event(6, at[1], x=4), event(8, at[2], x=5),
              event(9, at[3], x=6)]  # fmt: skip
    rows = {r["sequence_number"]: r for r in gold.observations(events, "rel", "seed", "b")}
    assert rows[5]["quality_flags"] == ["NO_PREVIOUS_STEP"] and rows[5]["dt_prev_s"] is None
    assert (rows[6]["dt_prev_s"], rows[6]["vx_mps"], rows[6]["vy_mps"]) == (2.0, 2.0, 0.0)
    assert rows[8]["quality_flags"] == ["SEQUENCE_GAP"] and rows[8]["vx_mps"] is None
    assert rows[9]["quality_flags"] == ["NON_POSITIVE_DT"] and rows[9]["dt_prev_s"] is None
    assert [rows[s]["step_index"] for s in (5, 6, 8, 9)] == [0, 1, 3, 4]


def test_splits_never_share_an_episode_or_a_robot():
    events = [event(s, T0 + timedelta(seconds=s), robot=f"r{r}", boot=f"r{r}-b{b}")
              for r in range(8) for b in range(5) for s in range(3)]  # fmt: skip
    rows = gold.observations(events, "rel", "seed-1", "b")
    for key, split in (("episode_id", "split"), ("robot_id", "split_robot")):
        seen = {}
        for r in rows:
            assert seen.setdefault(r[key], r[split]) == r[split]
    assert {r["split"] for r in rows} == {"train", "validation", "test"}


def test_split_is_a_pure_function_of_key_and_seed():
    assert gold.split_of("boot-1", "s") == gold.split_of("boot-1", "s")
    counts = {name: 0 for name, _ in gold.SPLITS}
    for i in range(4000):
        counts[gold.split_of(f"boot-{i}", "s")] += 1
    assert 0.66 < counts["train"] / 4000 < 0.74


def test_content_hash_ignores_input_order_but_not_content():
    events = [event(s, T0 + timedelta(seconds=s), x=s * 0.1) for s in range(20)]
    rows = gold.observations(events, "rel", "seed", "b")
    shuffled = rows[:]
    random.Random(1).shuffle(shuffled)
    assert gold.content_hash(rows) == gold.content_hash(shuffled)
    changed = [dict(r) for r in rows]
    changed[3]["battery_pct"] = 50.000000001
    assert gold.content_hash(changed) != gold.content_hash(rows)


def test_naive_and_aware_utc_times_hash_alike(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    import time

    time.tzset()
    aware = gold.row_line(gold.observations([event(0, T0)], "rel", "s", "b")[0])
    naive = gold.row_line(
        gold.observations([event(0, T0.replace(tzinfo=None))], "rel", "s", "b")[0]
    )
    assert aware == naive
