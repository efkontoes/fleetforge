"""Reference data for Silver, rebuilt from Bronze copies of two Milestone 2 streams.

* fleetforge.fleet.firmware_versions: Debezium table CDC of fleet.firmware_versions
  (JsonConverter, schemas disabled, REPLICA IDENTITY FULL, tombstones after
  deletes, compacted topic). Key = {"model_id", "version"}; value = the Debezium
  envelope {"before", "after", "source", "op", "ts_ms", ...}.
* fleet.robot-events.v1: the outbox EventRouter's expanded JSON payload of
  RobotRegistered, with the event type in the ``eventType`` header. The payload
  carries robot_id, robot_model, firmware_version, lifecycle_status, registered_at.

Telemetry events carry firmware_version but no robot model, and firmware contracts
are keyed by (model_id, version), so the robot model must come from the outbox
(contract gap G1).

Semantics, chosen for determinism: Silver applies the *latest* contract state in
the pinned Bronze snapshot to all history. A firmware contract describes how that
firmware emits values; it does not change over time, so an UPDATE of a contract row
is a correction of reference data, and the rebuild re-applies it everywhere.
Malformed reference records are not silently skipped: they become anomalies, which
the job reports and the fingerprint covers.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fleetforge.lakehouse.bronze import arrival_key

FIRMWARE_TOPIC = "fleetforge.fleet.firmware_versions"
ROBOT_EVENTS_TOPIC = "fleet.robot-events.v1"

# Canonical units: percent and metres. Multiply battery, divide position. Division
# by an exact integer is correctly rounded (9 / 1000 == 0.009); multiplying by the
# inexact constant 0.001 is not (9 * 0.001 == 0.009000000000000001).
BATTERY_MULTIPLIER = {"PERCENT": 1, "FRACTION": 100}
POSITION_DIVISOR = {"M": 1, "MM": 1000}


@dataclass(frozen=True)
class Source:
    source_topic: str
    kafka_topic_id: str
    kafka_partition: int
    kafka_offset: int


@dataclass(frozen=True)
class FirmwareContract:
    model_id: str
    firmware_version: str
    battery_unit: str
    position_unit: str
    cdc_op: str
    source: Source


@dataclass(frozen=True)
class RobotRegistration:
    robot_id: str
    robot_model: str
    registered_firmware_version: str
    registered_at: str | None
    source: Source


@dataclass(frozen=True)
class Anomaly:
    source: Source
    message: str


@dataclass(frozen=True)
class Reference:
    firmware: Mapping[tuple[str, str], FirmwareContract]
    robots: Mapping[str, RobotRegistration]
    anomalies: tuple[Anomaly, ...] = ()

    def robot_model(self, robot_id: str) -> str | None:
        reg = self.robots.get(robot_id)
        return reg.robot_model if reg else None

    def fingerprint(self) -> str:
        """Hash of everything Silver uses, so a reference change changes the build id."""
        body = {
            "firmware": sorted(
                [c.model_id, c.firmware_version, c.battery_unit, c.position_unit]
                for c in self.firmware.values()
            ),
            "robots": sorted([r.robot_id, r.robot_model] for r in self.robots.values()),
            "anomalies": sorted(
                [
                    a.source.kafka_topic_id,
                    a.source.kafka_partition,
                    a.source.kafka_offset,
                    a.message,
                ]
                for a in self.anomalies
            ),  # fmt: skip
        }
        return hashlib.sha256(json.dumps(body, separators=(",", ":")).encode()).hexdigest()


def _source(row: Mapping[str, Any]) -> Source:
    return Source(row["source_topic"], row["kafka_topic_id"], row["kafka_partition"],
                  row["kafka_offset"])  # fmt: skip


def _json(raw: bytes | None) -> Any:
    value = json.loads(raw.decode("utf-8"))
    # A converter with schemas enabled wraps the value: {"schema": ..., "payload": ...}.
    if isinstance(value, dict) and set(value) == {"schema", "payload"}:
        value = value["payload"]
    return value


def build_reference(
    firmware_rows: Iterable[Mapping[str, Any]], robot_event_rows: Iterable[Mapping[str, Any]]
) -> Reference:
    """Fold Bronze rows (any order; sorted here by arrival) into reference state."""
    anomalies: list[Anomaly] = []
    firmware: dict[tuple[str, str], FirmwareContract] = {}
    for row in sorted(firmware_rows, key=arrival_key):
        src = _source(row)
        if row["record_value"] is None:
            continue  # tombstone: follows a delete event, which already removed the key
        try:
            env = _json(row["record_value"])
            op = env["op"]
            if op in ("c", "r", "u"):
                image = env["after"]
                key = (str(image["model_id"]), str(image["version"]))
                battery, position = str(image["battery_unit"]), str(image["position_unit"])
                if battery not in BATTERY_MULTIPLIER or position not in POSITION_DIVISOR:
                    raise ValueError(f"unsupported units {battery}/{position}")
                firmware[key] = FirmwareContract(*key, battery, position, op, src)
            elif op == "d":
                image = env["before"]
                firmware.pop((str(image["model_id"]), str(image["version"])), None)
            else:
                raise ValueError(f"unexpected Debezium op {op!r}")
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            anomalies.append(Anomaly(src, f"firmware CDC: {type(exc).__name__}: {exc}"))

    robots: dict[str, RobotRegistration] = {}
    for row in sorted(robot_event_rows, key=arrival_key):
        src = _source(row)
        headers = {h["key"]: h["value"] for h in (row.get("record_headers") or [])}
        event_type = headers.get("eventType")
        if event_type is None:
            anomalies.append(Anomaly(src, "robot event without eventType header"))
            continue
        if event_type != b"RobotRegistered":
            continue  # other robot event types carry no registry facts Silver uses
        try:
            p = _json(row["record_value"])
            reg = RobotRegistration(
                str(p["robot_id"]).lower(),
                str(p["robot_model"]),
                str(p["firmware_version"]),
                p.get("registered_at"),
                src,
            )
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            anomalies.append(Anomaly(src, f"RobotRegistered: {type(exc).__name__}: {exc}"))
            continue
        held = robots.get(reg.robot_id)
        if held is None:
            robots[reg.robot_id] = reg  # first registration wins; the model is immutable
        elif held.robot_model != reg.robot_model:
            anomalies.append(
                Anomaly(
                    src,
                    f"robot {reg.robot_id} re-registered as "
                    f"{reg.robot_model}, held {held.robot_model}",
                )
            )
    return Reference(firmware, robots, tuple(anomalies))
