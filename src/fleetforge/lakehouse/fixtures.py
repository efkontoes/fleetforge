"""A deterministic, Kafka-compatible scenario for the first vertical slice.

Records are what a Kafka client would return: coordinates, key, value bytes in the
Confluent wire format (Avro encoded with the real Milestone 2 schemas), headers.
Each telemetry record states the outcome Silver must produce, so tests check
against the scenario, not against the implementation they are testing.

Two ingestion runs: run 2 happens a day later and brings a reboot, a late event
from the previous boot, two corrections and a cross-run redelivery.

Synthetic reference data, labelled as such: firmware contract AMR-300
3.0.0-fixture uses FRACTION/MM units. Every contract seeded by Milestone 2 uses
PERCENT/M, so without it the unit conversion would never be exercised (gap G6).
"""

from __future__ import annotations

import io
import json
import math
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import fastavro

from fleetforge.kafka.schemas import schema_str
from fleetforge.lakehouse.bronze import KafkaRecord, bronze_row
from fleetforge.lakehouse.reference import FIRMWARE_TOPIC, ROBOT_EVENTS_TOPIC
from fleetforge.lakehouse.silver import TELEMETRY_TOPIC

NS = uuid.UUID("1b7c9a52-3f0e-4d6a-9a77-2f5f8f6f0a01")
CORRECTION_NS = uuid.UUID("5a0c1d8e-5c4b-4b58-9a4e-0d3b0f5f7a11")  # = projector/replay.py

TOPIC_IDS = {
    TELEMETRY_TOPIC: "TlmFixtureTopicId00001",
    FIRMWARE_TOPIC: "FwFixtureTopicId000001",
    ROBOT_EVENTS_TOPIC: "RbFixtureTopicId000001",
}
SCHEMA_V1, SCHEMA_V2, SCHEMA_UNKNOWN = 1, 2, 99

T0 = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
RUNS = {
    1: ("run-1", datetime(2026, 11, 1, 8, 5, tzinfo=UTC)),
    2: ("run-2", datetime(2026, 11, 2, 9, 0, tzinfo=UTC)),
}


def uid(name: str) -> str:
    return str(uuid.uuid5(NS, name))


ROBOT_A, ROBOT_B, ROBOT_X = uid("robot-a"), uid("robot-b"), uid("robot-x")
BOOT_A1, BOOT_A2, BOOT_B1, BOOT_X1 = uid("a-boot-1"), uid("a-boot-2"), uid("b-boot-1"), uid("x-1")


def correction_id(original: str, attempt: int) -> str:
    """Same derivation as `fleet-replay correct` (uuid5 of '<original>:<attempt>')."""
    return str(uuid.uuid5(CORRECTION_NS, f"{original}:{attempt}"))


_PARSED = {
    SCHEMA_V1: fastavro.parse_schema(json.loads(schema_str(1))),
    SCHEMA_V2: fastavro.parse_schema(json.loads(schema_str(2))),
}


def framed(record: dict[str, Any], schema_id: int) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, _PARSED[schema_id], record)
    return b"\x00" + schema_id.to_bytes(4, "big") + buf.getvalue()


def telemetry(robot: str, boot: str, boot_number: int, seq: int, *, battery: float = 80.0,
              x: float = 1.0, y: float = 2.0, event_id: str | None = None,
              observed_at: datetime | None = None, firmware: str = "1.1.0",
              heading: float | None = None, motor_temp: float | None = 40.0,
              correction_of: str | None = None, schema_version: int = 1,
              produced_at: datetime | None = None) -> dict[str, Any]:  # fmt: skip
    base = T0 + timedelta(minutes=10 * (boot_number - 1))
    rec = {
        "schema_version": schema_version,
        "event_id": event_id or uid(f"{boot}:{seq}"),
        "robot_id": robot,
        "boot_id": boot,
        "boot_number": boot_number,
        "sequence_number": seq,
        "observed_at": observed_at or base + timedelta(milliseconds=100 * seq),
        "firmware_version": firmware,
        "battery_pct": battery,
        "position": {"x_m": x, "y_m": y},
        "sensors": {"motor_temp_c": motor_temp, "lidar_ok": True},
        "correction_of": correction_of,
        "producer": {"producer_id": "fleet-sim",
                     "produced_at": produced_at or T0 + timedelta(seconds=seq)},
    }  # fmt: skip
    if heading is not None:
        rec["heading_rad"] = heading
    return rec


@dataclass(frozen=True)
class Expect:
    outcome: str
    reason: str | None = None
    note: str = ""
    out_of_order: bool = False
    flags: tuple[str, ...] = ()
    superseded: bool = False


@dataclass(frozen=True)
class ScenarioRecord:
    run: int
    record: KafkaRecord
    expect: Expect | None  # None for reference-data topics


def _tel(run: int, partition: int, offset: int, value: bytes | None, key: str,
         expect: Expect) -> ScenarioRecord:  # fmt: skip
    ts = int((RUNS[run][1] - timedelta(seconds=30)).timestamp() * 1000) + offset
    rec = KafkaRecord(TELEMETRY_TOPIC, partition, offset, key.encode(), value, ts, 0)
    return ScenarioRecord(run, rec, expect)


def scenario() -> list[ScenarioRecord]:
    A, B, X = ROBOT_A, ROBOT_B, ROBOT_X
    s7 = uid(f"{BOOT_A1}:7")
    out: list[ScenarioRecord] = []
    ok = Expect("ACCEPTED")

    def a(seq: int, **kw: Any) -> bytes:
        return framed(telemetry(A, BOOT_A1, 1, seq, **kw), SCHEMA_V1)

    def b(seq: int, **kw: Any) -> bytes:
        return framed(telemetry(B, BOOT_B1, 1, seq, firmware="3.0.0-fixture", **kw), SCHEMA_V2)

    legacy_json = json.dumps({  # Milestone 1 JSON from a schema-bypassing producer
        "schema_version": 1, "event_id": uid("legacy"), "robot_id": A, "boot_id": BOOT_A1,
        "sequence_number": 99, "observed_at": T0.isoformat(), "x_m": 1.0, "y_m": 2.0,
    }).encode()  # fmt: skip
    p0 = [
        (a(0), ok),
        (a(1), ok),
        (a(2), ok),
        (a(1, produced_at=T0 + timedelta(minutes=1)),
         Expect("DUPLICATE", note="redelivery, re-encoded later: same content")),
        (a(3), ok),
        (a(5), ok),
        (a(4), Expect("ACCEPTED", note="arrives after seq 5", out_of_order=True)),
        (a(6, battery=math.nan), Expect("REJECTED", "INVALID_FIELD", "NaN battery")),
        (a(7, battery=150.0), Expect("REJECTED", "BATTERY_OUT_OF_RANGE", "corrected in run 2",
                                     superseded=True)),
        (legacy_json, Expect("REJECTED", "MALFORMED_PAYLOAD", "legacy JSON producer")),
        (a(8)[:-12], Expect("REJECTED", "MALFORMED_PAYLOAD", "truncated Avro body")),
        (b"\x00" + SCHEMA_UNKNOWN.to_bytes(4, "big") + b"\x02\x04",
         Expect("UNRESOLVED", "UNRESOLVED_SCHEMA_ID", "schema id not in the registry yet")),
        (a(3, event_id=uid("rogue-3")),
         Expect("REJECTED", "SEQUENCE_CONFLICT", "slot (1, 3) already held")),
        (a(2, battery=33.0),
         Expect("REJECTED", "EVENT_ID_CONFLICT", "event_id of seq 2, other content")),
        (a(8, firmware="9.9.9"), Expect("REJECTED", "UNKNOWN_FIRMWARE", "retry eligible")),
        (a(9, observed_at=RUNS[1][1] + timedelta(hours=1)),
         Expect("REJECTED", "OBSERVED_AT_IN_FUTURE", "1 h ahead of ingestion")),
        (a(10, schema_version=2), Expect("REJECTED", "UNSUPPORTED_SCHEMA_VERSION")),
    ]  # fmt: skip
    for off, (value, exp) in enumerate(p0):
        out.append(_tel(1, 0, off, value, A, exp))

    p1 = [
        (b(0, battery=0.5, x=1234.0, y=-250.0, heading=0.5), Expect("ACCEPTED",
         note="FRACTION/MM -> 50 %, 1.234 m, -0.25 m")),
        (b(1, battery=0.5, heading=7.0), Expect("ACCEPTED", flags=("HEADING_WRAPPED",))),
        (b(2, battery=0.5, heading=math.nan, motor_temp=math.inf),
         Expect("ACCEPTED", flags=("HEADING_NOT_FINITE", "MOTOR_TEMP_NOT_FINITE"))),
        (b(3, battery=1.2), Expect("REJECTED", "BATTERY_OUT_OF_RANGE", "1.2 FRACTION = 120 %")),
        (framed(telemetry(X, BOOT_X1, 1, 0), SCHEMA_V1),
         Expect("REJECTED", "UNKNOWN_ROBOT", "never registered; retry eligible")),
        (None, Expect("REJECTED", "MALFORMED_PAYLOAD", "tombstone on a telemetry topic")),
    ]  # fmt: skip
    for off, (value, exp) in enumerate(p1):
        out.append(_tel(1, 1, off, value, B if off < 4 else X, exp))

    c1, c2 = correction_id(s7, 1), correction_id(s7, 2)
    run2 = [
        (framed(telemetry(A, BOOT_A2, 2, 0), SCHEMA_V1), ok),
        (framed(telemetry(A, BOOT_A2, 2, 1), SCHEMA_V1), ok),
        (a(11), Expect("ACCEPTED", note="late: boot 1 event after boot 2", out_of_order=True)),
        (a(7, battery=75.0, event_id=c1, correction_of=s7),
         Expect("ACCEPTED", note="first correction, superseded by the second",
                out_of_order=True, superseded=True)),
        (a(0, produced_at=T0 + timedelta(days=1)),
         Expect("DUPLICATE", note="cross-run redelivery after a producer restart")),
        (a(7, battery=76.0, event_id=c2, correction_of=s7),
         Expect("ACCEPTED", note="second correction wins", out_of_order=True)),
    ]  # fmt: skip
    for i, (value, exp) in enumerate(run2):
        out.append(_tel(2, 0, len(p0) + i, value, A, exp))
    return out + reference_records()


def _firmware(offset: int, op: str, model: str, version: str, battery: str = "PERCENT",
              position: str = "M") -> ScenarioRecord:  # fmt: skip
    image = {"model_id": model, "version": version, "battery_unit": battery,
             "position_unit": position, "released_at": "2026-01-01T00:00:00.000000Z"}  # fmt: skip
    env = {
        "before": image if op == "d" else None,
        "after": None if op == "d" else image,
        "source": {"connector": "postgresql", "name": "fleetforge", "db": "fleetforge",
                   "schema": "fleet", "table": "firmware_versions", "lsn": 1000 + offset},
        "op": op,
        "ts_ms": int(T0.timestamp() * 1000) - 3_600_000 + offset,
    }  # fmt: skip
    key = json.dumps({"model_id": model, "version": version}).encode()
    rec = KafkaRecord(FIRMWARE_TOPIC, 0, offset, key, json.dumps(env).encode(), env["ts_ms"], 0)
    return ScenarioRecord(1, rec, None)


def _robot(offset: int, robot: str, model: str, firmware: str) -> ScenarioRecord:
    payload = {
        "robot_id": robot,
        "robot_model": model,
        "firmware_version": firmware,
        "lifecycle_status": "ACTIVE",
        "registered_at": "2026-10-31T12:00:00+00:00",
    }
    headers = (("eventType", b"RobotRegistered"), ("outboxCreatedAt", b"1761912000000000"))
    rec = KafkaRecord(ROBOT_EVENTS_TOPIC, 0, offset, robot.encode(),
                      json.dumps(payload).encode(), None, 0, headers)  # fmt: skip
    return ScenarioRecord(1, rec, None)


def reference_records() -> list[ScenarioRecord]:
    fw = [
        _firmware(0, "r", "AMR-100", "1.0.0"),
        _firmware(1, "r", "AMR-100", "1.1.0"),
        _firmware(2, "r", "AMR-200", "2.0.0"),
        _firmware(3, "c", "AMR-300", "3.0.0-fixture", "FRACTION", "MM"),  # synthetic
        _firmware(4, "c", "TMP-1", "0.0.1"),
        _firmware(5, "d", "TMP-1", "0.0.1"),
    ]
    tomb = KafkaRecord(FIRMWARE_TOPIC, 0, 6, b'{"model_id":"TMP-1","version":"0.0.1"}', None)
    return [
        *fw,
        ScenarioRecord(1, tomb, None),
        _robot(0, ROBOT_A, "AMR-100", "1.1.0"),
        _robot(1, ROBOT_B, "AMR-300", "3.0.0-fixture"),
    ]


def bronze_rows(records: list[ScenarioRecord], runs: tuple[int, ...] = (1, 2)) -> list[dict]:
    out = []
    for s in records:
        if s.run in runs:
            run_id, at = RUNS[s.run]
            out.append(bronze_row(s.record, topic_id=TOPIC_IDS[s.record.topic], run_id=run_id,
                                  ingested_at=at))  # fmt: skip
    return out


def schema_archive() -> list[dict[str, Any]]:
    """bronze.registry_schemas rows for the scenario (ids as a fresh registry assigns)."""
    rows = [
        {"schema_id": sid, "status": "FOUND", "schema_type": "AVRO", "schema_text": schema_str(rev)}
        for sid, rev in ((SCHEMA_V1, 1), (SCHEMA_V2, 2))
    ]
    rows.append({"schema_id": SCHEMA_UNKNOWN, "status": "NOT_FOUND", "schema_type": None,
                 "schema_text": None})  # fmt: skip
    return rows


# --------------------------------------------------------------------------- random corpus


@dataclass
class Corpus:
    """Kafka-compatible records split into ingestion runs, plus the schema archive per run."""

    batches: list[list[KafkaRecord]]
    archives: list[list[dict[str, Any]]]


UNKNOWN_THEN_FOUND = 77  # schema id the registry only learns in a later run


def random_corpus(seed: int, robots: int = 6, ticks: int = 30, runs: int = 4) -> Corpus:
    """Simulator traffic plus the edge cases incremental Silver must handle."""
    import random
    import zlib

    from fleetforge.kafka.codec import avro_record_from_payload
    from fleetforge.simulator.fleet import (
        FaultConfig,
        RegisterBoot,
        RegisterRobot,
        SimConfig,
        Telemetry,
        generate,
    )

    rng = random.Random(seed)
    faults = FaultConfig(duplicate_rate=0.06, reorder_rate=0.08, invalid_battery_rate=0.04,
                         missing_field_rate=0.02, reboot_rate=0.02)  # fmt: skip
    items = generate(SimConfig(seed=seed, robots=robots, ticks=ticks, faults=faults))
    firmware, boot_numbers, boots_per_robot, registrations = {}, {}, {}, []
    for it in items:
        if isinstance(it, RegisterRobot):
            firmware[it.robot_id] = it.firmware_version
            registrations.append(it)
        elif isinstance(it, RegisterBoot):
            boots_per_robot[it.robot_id] = boots_per_robot.get(it.robot_id, 0) + 1
            boot_numbers[it.boot_id] = boots_per_robot[it.robot_id]
    v2_robots = {r.robot_id for r in registrations if rng.random() < 0.3}

    stream: list[tuple[str, bytes]] = []  # (robot_id, value) in delivery order
    valid: list[tuple[int, dict]] = []  # (stream index, Avro record) of decodable events
    legacy: list[tuple[int, Telemetry]] = []  # JSON records a later correction can fix
    for it in (i for i in items if isinstance(i, Telemetry)):
        if it.fault == "missing_field":
            stream.append((it.robot_id, it.body()))
            if "event_id" in it.payload:
                legacy.append((len(stream) - 1, it))
            continue
        rec = avro_record_from_payload(it.payload, boot_number=boot_numbers[it.payload["boot_id"]],
                                       firmware_version=firmware[it.robot_id], producer_id="sim",
                                       produced_at=T0 + timedelta(seconds=len(stream)))  # fmt: skip
        schema = SCHEMA_V1
        if it.robot_id in v2_robots:
            rec["heading_rad"], schema = rng.uniform(-4.0, 4.0), SCHEMA_V2
        stream.append((it.robot_id, framed(rec, schema)))
        valid.append((len(stream) - 1, rec))

    def insert_after(index: int, robot: str, value: bytes) -> None:
        stream.insert(rng.randint(index + 1, len(stream)), (robot, value))

    for index, rec in rng.sample(valid, 3):  # same event_id, other content
        insert_after(index, rec["robot_id"], framed({**rec, "battery_pct": 1.0}, SCHEMA_V1))
    for index, rec in rng.sample(valid, 3):  # same slot, other event_id
        insert_after(index, rec["robot_id"], framed({**rec, "event_id": uid(f"slot-{index}")},
                                                    SCHEMA_V1))  # fmt: skip
    for index, rec in rng.sample(valid, 4):  # corrections, one target corrected twice
        attempts = 2 if rng.random() < 0.5 else 1
        for attempt in range(1, attempts + 1):
            target = str(rec["event_id"])
            fix = {**rec, "event_id": correction_id(target, attempt), "correction_of": target,
                   "battery_pct": 42.0 + attempt}  # fmt: skip
            insert_after(index, rec["robot_id"], framed(fix, SCHEMA_V1))
    for (index, rec), (_, other) in zip(rng.sample(valid, 2), rng.sample(valid, 2), strict=True):
        if other["robot_id"] != rec["robot_id"]:  # another robot reuses this event_id
            insert_after(index, other["robot_id"],
                         framed({**other, "event_id": rec["event_id"]}, SCHEMA_V1))  # fmt: skip
    for index, it in legacy[:3]:  # fix a rejected legacy record, robot_id possibly missing
        rec = next(r for _, r in valid if r["robot_id"] == it.robot_id)
        fix = {**rec, "event_id": correction_id(it.payload["event_id"], 1),
               "correction_of": it.payload["event_id"], "battery_pct": 50.0}  # fmt: skip
        insert_after(index, it.robot_id, framed(fix, SCHEMA_V1))
    for index, rec in rng.sample(valid, 3):  # framed with a schema id the registry lacks
        body = framed(rec, SCHEMA_V1)
        stream[index] = (
            rec["robot_id"],
            b"\x00" + UNKNOWN_THEN_FOUND.to_bytes(4, "big") + body[5:],
        )

    cuts = sorted(rng.sample(range(1, len(stream)), runs - 1))
    bounds = list(zip([0, *cuts], [*cuts, len(stream)], strict=True))
    offsets: dict[int, int] = {}
    batches: list[list[KafkaRecord]] = []
    for lo, hi in bounds:
        batch = []
        for robot, value in stream[lo:hi]:
            part = zlib.crc32(robot.encode()) % 3
            offset = offsets.get(part, 0)
            offsets[part] = offset + 1
            batch.append(KafkaRecord(TELEMETRY_TOPIC, part, offset, robot.encode(), value, None, 0))
        batches.append(batch)

    late = rng.choice(registrations)  # registered only in the last run
    batches[0] = [s.record for s in reference_records() if s.record.topic == FIRMWARE_TOPIC
                  ] + batches[0]  # fmt: skip
    robot_offset = 0
    for reg in registrations:
        if reg is not late:
            batches[0].append(_robot(robot_offset, reg.robot_id, reg.robot_model,
                                     reg.firmware_version).record)  # fmt: skip
            robot_offset += 1
    batches[-1].append(_robot(robot_offset, late.robot_id, late.robot_model,
                              late.firmware_version).record)  # fmt: skip
    if runs > 2:  # a firmware unit change rewrites history of every AMR-200 robot
        batches[runs // 2].append(_firmware(7, "u", "AMR-200", "2.0.0", "FRACTION", "MM").record)

    found_from = rng.randrange(1, runs) if runs > 1 else runs
    archives = []
    for run in range(runs):
        archive = schema_archive()
        status = "FOUND" if run >= found_from else "NOT_FOUND"
        text = schema_str(1) if status == "FOUND" else None
        archive.append({"schema_id": UNKNOWN_THEN_FOUND, "status": status, "schema_type": None,
                        "schema_text": text})  # fmt: skip
        archives.append(archive)
    return Corpus(batches, archives)


def corpus_bronze(corpus: Corpus) -> list[list[dict]]:
    """Bronze rows of each run, with run times late enough to pass the skew check."""
    start = datetime(2026, 1, 2, tzinfo=UTC)
    return [
        [
            bronze_row(
                r,
                topic_id=TOPIC_IDS[r.topic],
                run_id=f"run-{n}",
                ingested_at=start + timedelta(hours=n),
            )
            for r in batch
        ]  # fmt: skip
        for n, batch in enumerate(corpus.batches)
    ]
