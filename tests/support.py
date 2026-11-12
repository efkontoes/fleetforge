"""Helpers shared by Milestone 2 tests: turn a simulated stream into Kafka-like
records without needing a broker (the broker-backed tests live in tests/stack)."""

from __future__ import annotations

import zlib
from collections import defaultdict
from uuid import UUID

from confluent_kafka.schema_registry._sync.mock_schema_registry_client import (
    MockSchemaRegistryClient,
)
from sqlalchemy import Engine, text

from fleetforge.contracts.api import RobotRegistration
from fleetforge.kafka.codec import TelemetryDecoder, TelemetryEncoder
from fleetforge.kafka.schemas import register
from fleetforge.projector.processor import SourceRecord
from fleetforge.services import robots
from fleetforge.simulator.fleet import Item, RegisterBoot, RegisterRobot, Telemetry
from fleetforge.simulator.kafka_publisher import kafka_value

TOPIC = "telemetry.raw.v1"
PARTITIONS = 6


def mock_codec(name: str = "unit") -> tuple[TelemetryEncoder, TelemetryDecoder]:
    client = MockSchemaRegistryClient({"url": f"mock://{name}"})
    register(client, f"{TOPIC}-value", 1, set_rule=False)
    return TelemetryEncoder(client, TOPIC), TelemetryDecoder(client, TOPIC)


def partition_of(robot_id: str) -> int:
    return zlib.crc32(robot_id.encode()) % PARTITIONS


def register_and_encode(
    db: Engine, items: list[Item], encoder: TelemetryEncoder
) -> list[SourceRecord]:
    """Registers robots/boots through the real service layer, encodes telemetry the way
    the Kafka publisher does, and assigns per-partition offsets in stream order."""
    boot_numbers: dict[str, int] = {}
    firmware: dict[str, str] = {}
    offsets: dict[int, int] = defaultdict(int)
    out = []
    for it in items:
        if isinstance(it, RegisterRobot):
            robots.register_robot(db, RobotRegistration(robot_id=it.robot_id,
                                  robot_model=it.robot_model,
                                  firmware_version=it.firmware_version))  # fmt: skip
            firmware[it.robot_id] = it.firmware_version
        elif isinstance(it, RegisterBoot):
            boot_numbers[it.boot_id], _ = robots.register_boot(
                db, UUID(it.robot_id), UUID(it.boot_id)
            )
        elif isinstance(it, Telemetry):
            p = partition_of(it.robot_id)
            value = kafka_value(it, encoder, boot_numbers, firmware, "test-producer")
            out.append(SourceRecord(TOPIC, p, offsets[p], value, 1_700_000_000_000))
            offsets[p] += 1
    return out


def interleave(records: list[SourceRecord]) -> list[SourceRecord]:
    """A consumer sees partitions interleaved in arbitrary chunks; per-partition order
    is preserved. Deterministic round-robin in chunks of 7 to exercise mixing."""
    by_part: dict[int, list[SourceRecord]] = defaultdict(list)
    for r in records:
        by_part[r.partition].append(r)
    out, i = [], 0
    while True:
        chunks = [by_part[p][i : i + 7] for p in sorted(by_part)]
        if not any(chunks):
            return out
        for c in chunks:
            out.extend(c)
        i += 7


def batches(records: list, size: int) -> list[list]:
    return [records[i : i + size] for i in range(0, len(records), size)]


def db_state(db: Engine, robot_ids: set[str] | None = None) -> dict[UUID, dict]:
    with db.connect() as c:
        rows = c.execute(text("""
            SELECT robot_id, event_id, boot_id, sequence_number, battery_pct, x_m, y_m,
                   state_version FROM fleet.robot_latest_state
        """)).mappings()  # fmt: skip
        return {r["robot_id"]: dict(r) for r in rows
                if robot_ids is None or str(r["robot_id"]) in robot_ids}  # fmt: skip


def db_rejection_rows(db: Engine) -> set[tuple[str, str]]:
    with db.connect() as c:
        rows = c.execute(text("SELECT dedup_key, reason_code FROM fleet.telemetry_rejections"))
        return {(r.dedup_key, r.reason_code) for r in rows}


def assert_state_matches_oracle(state: dict, exp) -> None:
    assert set(state) == set(exp.latest), "robots with state differ from oracle"
    for robot_id, payload in exp.latest.items():
        row = state[robot_id]
        assert str(row["event_id"]) == payload["event_id"], robot_id
        assert row["sequence_number"] == payload["sequence_number"]
        assert (row["battery_pct"], row["x_m"], row["y_m"]) == (
            payload["battery_pct"], payload["x_m"], payload["y_m"])  # fmt: skip
        assert row["state_version"] == exp.state_versions[robot_id], robot_id


def robot_rejection_rows(db: Engine, items: list[Item]) -> set[tuple[str, str]]:
    """Kafka-sourced audit rows belonging to one simulated fleet (shared stack DB)."""
    from fleetforge.domain.telemetry import dedup_key, identity_hints, payload_for_audit

    keys = set()
    for it in items:
        if isinstance(it, Telemetry):
            eid, _ = identity_hints(payload_for_audit(it.body()))
            keys.add(dedup_key(eid, it.body()))
    with db.connect() as c:
        rows = c.execute(
            text(
                "SELECT dedup_key, reason_code FROM fleet.telemetry_rejections"
                " WHERE source = 'kafka' AND dedup_key = ANY(:k)"
            ),
            {"k": list(keys)},
        )
        return {(r.dedup_key, r.reason_code) for r in rows}
