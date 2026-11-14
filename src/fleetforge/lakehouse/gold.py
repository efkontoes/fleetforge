"""Gold in plain Python: the reference that dbt models and the Spark release job must match."""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from fleetforge.lakehouse.bronze import coordinates
from fleetforge.lakehouse.tables import GOLD_ROBOT_STATE_OBSERVATIONS

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
CORRECTION_WINDOW = timedelta(days=14)
FEATURE_SCHEMA_VERSION = "robot-state/1"
SPLITS = (("train", 0.70), ("validation", 0.85), ("test", 1.0))
OBSERVATION_COLUMNS = GOLD_ROBOT_STATE_OBSERVATIONS.column_names


def _micros(ts: datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.astimezone()  # PySpark hands back naive local times
    return (ts - EPOCH) // timedelta(microseconds=1)


def _seconds(later: datetime, earlier: datetime) -> float:
    return (_micros(later) - _micros(earlier)) / 1e6


def open_retry_robots(
    outcomes: Iterable[Mapping[str, Any]], key_robot: Mapping[tuple, str] | None = None
) -> tuple[set[str], bool]:
    """Robots with records a later build may still accept, and whether one names no robot.

    A record whose payload could not be read is attributed by its Kafka key (`key_robot`).
    """
    robots, robotless = set(), False
    for o in outcomes:
        if o["outcome"] in ("REJECTED", "UNRESOLVED") and o["retry_eligible"]:
            robot = o["robot_id"] or (key_robot or {}).get(coordinates(o))
            if robot:
                robots.add(robot)
            else:
                robotless = True
    return robots, robotless


def key_robots(bronze: Iterable[Mapping[str, Any]]) -> dict[tuple, str]:
    """Bronze coordinates -> robot named by the Kafka key (UTF-8, as Trino's from_utf8)."""
    return {coordinates(b): b["record_key"].decode("utf-8", "replace")
            for b in bronze if b["record_key"]}  # fmt: skip


def _steps(events: list[Mapping[str, Any]]) -> Iterator[tuple[Mapping, Mapping | None]]:
    """Each event with the previous observed position of its boot (None for the first)."""
    by_boot = defaultdict(list)
    for e in events:
        by_boot[(e["robot_id"], e["boot_number"])].append(e)
    for rows in by_boot.values():
        rows.sort(key=lambda e: e["sequence_number"])
        prev = None
        for e in rows:
            yield e, prev
            prev = e


def daily_telemetry(events: Iterable[Mapping[str, Any]], outcomes: Iterable[Mapping[str, Any]],
                    build_id: str, key_robot: Mapping[tuple, str] | None = None,
                    ) -> list[dict[str, Any]]:  # fmt: skip
    """gold.robot_daily_telemetry from current Silver events (see tables.py for each column)."""
    outcomes = list(outcomes)
    current = [e for e in events if e["is_current"]]
    newest = max((o["bronze_ingested_at"] for o in outcomes), default=None)
    open_robots, robotless_open = open_retry_robots(outcomes, key_robot)
    final_before = (newest - CORRECTION_WINDOW).date() if newest else None

    days: dict[tuple[date, str], dict[str, Any]] = {}
    for e, prev in _steps(current):
        key = (e["observed_at"].date(), e["robot_id"])
        d = days.setdefault(key, {"events": [], "missing": 0, "distance": 0.0})
        d["events"].append(e)
        if prev is None:
            continue
        d["missing"] += e["sequence_number"] - prev["sequence_number"] - 1
        consecutive = e["sequence_number"] == prev["sequence_number"] + 1
        if consecutive and e["observed_at"] >= prev["observed_at"]:
            d["distance"] += math.hypot(e["x_m"] - prev["x_m"], e["y_m"] - prev["y_m"])

    rows = []
    for (day, robot), d in sorted(days.items()):
        es = d["events"]
        positions = len({(e["boot_number"], e["sequence_number"]) for e in es})
        temps = [e["motor_temp_c"] for e in es if e["motor_temp_c"] is not None]
        batteries = [e["battery_pct"] for e in es]
        final = (final_before is not None and day <= final_before
                 and robot not in open_robots and not robotless_open)  # fmt: skip
        rows.append({
            "observed_date": day, "robot_id": robot,
            "robot_model": max(e["robot_model"] for e in es),
            "events": len(es), "boots": len({e["boot_id"] for e in es}),
            "observed_positions": positions, "expected_events": positions + d["missing"],
            "missing_sequences": d["missing"],
            "out_of_order_events": sum(1 for e in es if e["arrived_out_of_order"]),
            "duplicate_deliveries": sum(e["duplicate_deliveries"] for e in es),
            "corrected_events": sum(1 for e in es if e["correction_of"]),
            "battery_pct_min": min(batteries), "battery_pct_avg": sum(batteries) / len(batteries),
            "battery_pct_max": max(batteries), "distance_m": d["distance"],
            "motor_temp_c_max": max(temps) if temps else None,
            "lidar_fault_events": sum(1 for e in es if e["lidar_ok"] is False),
            "first_observed_at": min(e["observed_at"] for e in es),
            "last_observed_at": max(e["observed_at"] for e in es),
            "is_final": final, "silver_build_id": build_id,
        })  # fmt: skip
    return rows


def pipeline_quality(outcomes: Iterable[Mapping[str, Any]], topic_of: Mapping[str, str],
                     build_id: str) -> list[dict[str, Any]]:  # fmt: skip
    """gold.pipeline_daily_quality: outcome counts per ingestion day, topic and reason."""
    groups: dict[tuple, list[Mapping]] = defaultdict(list)
    for o in outcomes:
        key = (o["bronze_ingested_at"].date(), topic_of[o["kafka_topic_id"]], o["outcome"],
               o["reason_code"] or "NONE", o["retry_eligible"])  # fmt: skip
        groups[key].append(o)
    return [
        {"ingest_date": k[0], "source_topic": k[1], "outcome": k[2], "reason_code": k[3],
         "retry_eligible": k[4], "records": len(rows),
         "distinct_events": len({o["event_id"] for o in rows if o["event_id"]}),
         "distinct_robots": len({o["robot_id"] for o in rows if o["robot_id"]}),
         "silver_build_id": build_id}
        for k, rows in sorted(groups.items())
    ]  # fmt: skip


def pipeline_rates(bronze: Iterable[Mapping[str, Any]], outcomes: Iterable[Mapping[str, Any]],
                   build_id: str) -> list[dict[str, Any]]:  # fmt: skip
    """gold.pipeline_daily_rates: N_bronze = classified + unresolved, rates over classified."""
    counts: dict[tuple, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    topic_of = {}
    for b in bronze:
        topic_of[b["kafka_topic_id"]] = b["source_topic"]
        counts[(b["ingested_at"].date(), b["source_topic"])]["bronze_records"] += 1
    for o in outcomes:
        counts[(o["bronze_ingested_at"].date(), topic_of[o["kafka_topic_id"]])][o["outcome"]] += 1
    rows = []
    for (day, topic), c in sorted(counts.items()):
        classified = c["ACCEPTED"] + c["DUPLICATE"] + c["REJECTED"]
        rows.append({
            "ingest_date": day, "source_topic": topic, "bronze_records": c["bronze_records"],
            "classified": classified, "accepted": c["ACCEPTED"], "duplicates": c["DUPLICATE"],
            "rejected": c["REJECTED"], "unresolved": c["UNRESOLVED"],
            "invalid_rate": c["REJECTED"] / classified if classified else None,
            "duplicate_rate": c["DUPLICATE"] / classified if classified else None,
            "silver_build_id": build_id,
        })  # fmt: skip
    return rows


def split_of(key: str, seed: str) -> str:
    """Hash split: SHA-256 of "seed:key", first 8 hex digits as a fraction of 2**32."""
    fraction = int(hashlib.sha256(f"{seed}:{key}".encode()).hexdigest()[:8], 16) / 2**32
    return next(name for name, upper in SPLITS if fraction < upper)


def observations(events: Iterable[Mapping[str, Any]], release_id: str, seed: str,
                 build_id: str) -> list[dict[str, Any]]:  # fmt: skip
    """gold.robot_state_observations for current events; derivatives only over valid steps."""
    current = [e for e in events if e["is_current"]]
    first_seq: dict[str, int] = {}
    for e in current:
        first_seq[e["boot_id"]] = min(first_seq.get(e["boot_id"], e["sequence_number"]),
                                      e["sequence_number"])  # fmt: skip
    rows = []
    for e, prev in _episode_steps(current):
        flags = set(e["quality_flags"])
        dt = vx = vy = None
        if prev is None:
            flags.add("NO_PREVIOUS_STEP")
        elif e["sequence_number"] != prev["sequence_number"] + 1:
            flags.add("SEQUENCE_GAP")
        elif _seconds(e["observed_at"], prev["observed_at"]) <= 0:
            flags.add("NON_POSITIVE_DT")
        else:
            dt = _seconds(e["observed_at"], prev["observed_at"])
            vx, vy = (e["x_m"] - prev["x_m"]) / dt, (e["y_m"] - prev["y_m"]) / dt
        rows.append({
            "release_id": release_id, "observation_id": e["event_id"], "robot_id": e["robot_id"],
            "robot_model": e["robot_model"], "firmware_version": e["firmware_version"],
            "episode_id": e["boot_id"], "boot_number": e["boot_number"],
            "sequence_number": e["sequence_number"],
            "step_index": e["sequence_number"] - first_seq[e["boot_id"]],
            "observed_at": e["observed_at"], "battery_pct": e["battery_pct"], "x_m": e["x_m"],
            "y_m": e["y_m"], "heading_rad": e["heading_rad"], "motor_temp_c": e["motor_temp_c"],
            "lidar_ok": e["lidar_ok"], "dt_prev_s": dt, "vx_mps": vx, "vy_mps": vy,
            "split": split_of(e["boot_id"], seed), "split_robot": split_of(e["robot_id"], seed),
            "quality_flags": sorted(flags), "source_topic": e["source_topic"],
            "kafka_topic_id": e["kafka_topic_id"],
            "kafka_partition": e["kafka_partition"], "kafka_offset": e["kafka_offset"],
            "silver_build_id": build_id,
        })  # fmt: skip
    return rows


def _episode_steps(events: list[Mapping[str, Any]]) -> Iterator[tuple[Mapping, Mapping | None]]:
    by_episode = defaultdict(list)
    for e in events:
        by_episode[e["boot_id"]].append(e)
    for rows in by_episode.values():
        rows.sort(key=lambda e: e["sequence_number"])
        yield from zip(rows, [None, *rows[:-1]], strict=True)


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        return _micros(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, list | tuple):
        return [_canonical(v) for v in value]
    return value


def row_line(row: Mapping[str, Any]) -> str:
    """One observation as canonical JSON in column order (floats by repr, times in micros)."""
    return json.dumps([_canonical(row[c]) for c in OBSERVATION_COLUMNS], separators=(",", ":"))


def content_hash(rows: Iterable[Mapping[str, Any]], presorted: bool = False) -> str:
    """SHA-256 over the canonical lines of rows ordered by observation_id."""
    ordered = rows if presorted else sorted(rows, key=lambda r: r["observation_id"])
    h = hashlib.sha256()
    for row in ordered:
        h.update(row_line(row).encode() + b"\n")
    return h.hexdigest()
