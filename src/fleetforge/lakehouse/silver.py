"""Silver: validated, canonical telemetry, as a pure function of its inputs.

    Silver = F(Bronze snapshot, reference data, TRANSFORM_VERSION)

F has two stages, kept separate because they scale differently on Spark:

Row stage, ``prepare()``: one Bronze row in, one ``Prepared`` out. It decodes the
Avro body with the archived writer schema, applies the Milestone 2 contract rules
unchanged (``observation_from_record``, ``check_semantics``), resolves the firmware
contract, converts units and repairs optional fields. On Spark this exact function
runs inside ``mapInPandas``.

Set stage, ``build()``: needs other rows. It resolves duplicates, event-id
conflicts, sequence conflicts, corrections and out-of-order arrival. On Spark it is
window functions (``jobs/silver_build.py``); this Python version is the reference
implementation the Spark one is tested against.

Determinism rules:
  * no wall clock. The future-timestamp check compares observed_at with the
    record's Bronze ``ingested_at``, which is stored, not with "now".
  * "first" always means first in Bronze arrival order (``bronze.arrival_key``).
  * ordering is (boot_number, sequence_number). observed_at is the robot's clock
    and is never used to order events.
  * every float conversion is exact or correctly rounded (reference.py).
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import fastavro

from fleetforge.domain.errors import REASONS
from fleetforge.domain.ingest import Observation, observation_from_record
from fleetforge.domain.telemetry import (
    ReasonCode,
    Rejection,
    check_semantics,
    identity_hints,
    payload_for_audit,
)
from fleetforge.kafka.codec import json_safe
from fleetforge.lakehouse.bronze import CONFLUENT, arrival_key
from fleetforge.lakehouse.reference import BATTERY_MULTIPLIER, POSITION_DIVISOR, Reference
from fleetforge.lakehouse.tables import SILVER_TELEMETRY_EVENTS, SILVER_TELEMETRY_RECORD_OUTCOMES

TRANSFORM_VERSION = "silver-telemetry/1"  # bump on ANY rule change below
TELEMETRY_TOPIC = "telemetry.raw.v1"
# Milestone 2's default (fleetforge.config max_clock_skew_s). Fixed here, not read
# from the environment: it is part of the rule set, so changing it is a new version.
MAX_CLOCK_SKEW = timedelta(seconds=300)
TAU = 2 * math.pi


class Outcome(StrEnum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    REJECTED = "REJECTED"


class SilverReason(StrEnum):
    """Reasons that exist only at the set level of Silver (Milestone 2 cannot see
    them: its latest-state table does not remember every event)."""

    EVENT_ID_CONFLICT = "EVENT_ID_CONFLICT"  # same event_id, different content


class Flag(StrEnum):
    HEADING_NOT_FINITE = "HEADING_NOT_FINITE"  # source value NaN/Inf: set to NULL
    HEADING_WRAPPED = "HEADING_WRAPPED"  # source value outside [-pi, pi): wrapped
    MOTOR_TEMP_NOT_FINITE = "MOTOR_TEMP_NOT_FINITE"  # source value NaN/Inf: set to NULL
    CORRECTION_TARGET_UNKNOWN = "CORRECTION_TARGET_UNKNOWN"  # correction_of never seen


def reason_category(code: str) -> tuple[str, bool]:
    """(error_category, retry_eligible), from Milestone 2's taxonomy where it applies."""
    if code == SilverReason.EVENT_ID_CONFLICT:
        return "PERMANENT_DATA", False
    category, retry = REASONS[ReasonCode(code)]
    return category.value, retry


class SchemaArchiveIncomplete(RuntimeError):
    """A schema id in Bronze is neither archived nor known to be absent. Recoverable:
    archive it (Bronze job) and rerun. Never a quarantine: the record may be fine."""


# --------------------------------------------------------------------------- row stage


@dataclass(frozen=True)
class Canonical:
    event_id: str
    robot_id: str
    boot_id: str
    boot_number: int
    sequence_number: int
    observed_at: datetime
    robot_model: str
    firmware_version: str
    battery_pct: float
    x_m: float
    y_m: float
    heading_rad: float | None
    motor_temp_c: float | None
    lidar_ok: bool | None
    source_battery_unit: str
    source_position_unit: str
    quality_flags: tuple[str, ...]
    schema_version: int
    writer_schema_id: int
    producer_id: str
    produced_at: datetime
    correction_of: str | None
    fingerprint: str


@dataclass(frozen=True)
class Prepared:
    row: Mapping[str, Any]  # the Bronze row (coordinates, ingested_at, ...)
    event: Canonical | None = None
    reason: str | None = None
    errors: str | None = None  # JSON array
    event_id: str | None = None  # identity hints, also for rejected records
    robot_id: str | None = None

    @property
    def arrival(self) -> tuple:
        return arrival_key(self.row)


def parse_writer_schemas(archive: Iterable[Mapping[str, Any]]) -> dict[int, Any]:
    """bronze.registry_schemas rows -> {schema_id: parsed schema | None (NOT_FOUND)}."""
    out: dict[int, Any] = {}
    for r in archive:
        sid = int(r["schema_id"])
        if r["status"] == "FOUND":
            out[sid] = fastavro.parse_schema(json.loads(r["schema_text"]))
        else:
            out[sid] = None
    return out


def _errors_json(errors: Iterable[Mapping[str, Any]]) -> str:
    return json.dumps(list(errors), sort_keys=True, separators=(",", ":"), default=str)


def _reject(row, code, errors, event_id=None, robot_id=None) -> Prepared:
    return Prepared(row, None, str(code), _errors_json(errors), event_id, robot_id)


def wrap_angle(rad: float) -> float:
    """Map any finite angle into [-pi, pi). Wrapping changes representation, not
    direction; it does not assume a zero direction or rotation sense."""
    w = math.fmod(rad + math.pi, TAU)
    if w < 0:
        w += TAU
    out = w - math.pi
    return -math.pi if out >= math.pi else out


def _micros(dt: datetime) -> int:
    return (dt - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def content_fingerprint(rec: Mapping[str, Any]) -> str:
    """Hash of an event's business content, excluding delivery metadata (producer id
    and time, writer schema): a retry re-encoded later, or by a newer schema
    revision, is the same event. A v1 record (no heading) equals a v2 record with
    heading NULL. Absent sensors equal all-NULL sensors."""
    sensors = rec.get("sensors") or {}

    def f(v: Any) -> str | None:
        return None if v is None else repr(float(v))

    body = {
        "schema_version": rec["schema_version"],
        "event_id": str(rec["event_id"]).lower(),
        "robot_id": str(rec["robot_id"]).lower(),
        "boot_id": str(rec["boot_id"]).lower(),
        "boot_number": rec["boot_number"],
        "sequence_number": rec["sequence_number"],
        "observed_at_us": _micros(rec["observed_at"]),
        "firmware_version": rec["firmware_version"],
        "battery_pct": f(rec["battery_pct"]),
        "x_m": f(rec["position"]["x_m"]),
        "y_m": f(rec["position"]["y_m"]),
        "heading_rad": f(rec.get("heading_rad")),
        "motor_temp_c": f(sensors.get("motor_temp_c")),
        "lidar_ok": sensors.get("lidar_ok"),
        "correction_of": str(rec["correction_of"]).lower() if rec.get("correction_of") else None,
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def prepare(row: Mapping[str, Any], writers: Mapping[int, Any], reference: Reference) -> Prepared:
    """One Bronze telemetry row -> canonical event or rejection (with a reason)."""
    value = row["record_value"]
    if row["value_framing"] != CONFLUENT:
        payload = payload_for_audit(bytes(value or b""))
        eid, rid = identity_hints(payload)
        return _reject(row, ReasonCode.MALFORMED_PAYLOAD,
                       [{"code": "MALFORMED_PAYLOAD", "field": None,
                         "message": f"not Confluent-framed Avro ({row['value_framing']})"}],
                       _s(eid), _s(rid))  # fmt: skip
    sid = int(row["value_schema_id"])
    if sid not in writers:
        raise SchemaArchiveIncomplete(f"schema id {sid} is not in bronze.registry_schemas")
    writer = writers[sid]
    if writer is None:
        return _reject(
            row,
            ReasonCode.MALFORMED_PAYLOAD,
            [
                {
                    "code": "MALFORMED_PAYLOAD",
                    "field": None,
                    "message": f"schema id {sid} does not exist in the registry",
                }
            ],
        )
    try:
        rec = fastavro.schemaless_reader(io.BytesIO(bytes(value)[5:]), writer, None)
        if not isinstance(rec, dict):
            raise TypeError("decoded value is not a record")
    except Exception as exc:  # truncated body, bad union index, bad UTF-8, ...
        return _reject(row, ReasonCode.MALFORMED_PAYLOAD,
                       [{"code": "MALFORMED_PAYLOAD", "field": None,
                         "message": f"{type(exc).__name__}: {exc}"}])  # fmt: skip

    obs = observation_from_record(rec)  # Milestone 2 contract rules, unchanged
    if isinstance(obs, Rejection):
        eid, rid = identity_hints(json_safe(rec))
        return _reject(row, obs.reason_code, obs.errors, _s(eid), _s(rid))
    eid, rid = str(obs.event_id), str(obs.robot_id)

    model = reference.robot_model(rid)
    if model is None:
        return _reject(row, ReasonCode.UNKNOWN_ROBOT,
                       [{"code": "UNKNOWN_ROBOT", "field": "robot_id",
                         "message": "no RobotRegistered event in Bronze for this robot"}],
                       eid, rid)  # fmt: skip
    contract = reference.firmware.get((model, obs.firmware_version))
    if contract is None:
        return _reject(row, ReasonCode.UNKNOWN_FIRMWARE,
                       [{"code": "UNKNOWN_FIRMWARE", "field": "firmware_version",
                         "message": f"no firmware contract for {model} {obs.firmware_version}"}],
                       eid, rid)  # fmt: skip

    battery = obs.battery_pct * BATTERY_MULTIPLIER[contract.battery_unit]
    divisor = POSITION_DIVISOR[contract.position_unit]
    x_m, y_m = obs.x_m / divisor, obs.y_m / divisor
    converted: Observation = replace(obs, battery_pct=battery, x_m=x_m, y_m=y_m)
    sem = check_semantics(converted, row["ingested_at"], MAX_CLOCK_SKEW)  # type: ignore[arg-type]
    if sem is not None:
        return _reject(row, sem.reason_code, sem.errors, eid, rid)

    flags: list[str] = []
    heading = rec.get("heading_rad")  # absent from writer schema revision 1
    if heading is not None:
        if not math.isfinite(heading):
            heading = None
            flags.append(Flag.HEADING_NOT_FINITE)
        else:
            wrapped = wrap_angle(heading)
            if wrapped != heading:
                flags.append(Flag.HEADING_WRAPPED)
            heading = wrapped
    sensors = rec.get("sensors") or {}
    motor = sensors.get("motor_temp_c")
    if motor is not None and not math.isfinite(motor):
        motor = None
        flags.append(Flag.MOTOR_TEMP_NOT_FINITE)
    producer = rec["producer"]
    event = Canonical(
        event_id=eid,
        robot_id=rid,
        boot_id=str(obs.boot_id),
        boot_number=obs.boot_number,
        sequence_number=obs.sequence_number,
        observed_at=obs.observed_at,
        robot_model=model,
        firmware_version=obs.firmware_version,
        battery_pct=battery,
        x_m=x_m,
        y_m=y_m,
        heading_rad=heading,
        motor_temp_c=motor,
        lidar_ok=sensors.get("lidar_ok"),
        source_battery_unit=contract.battery_unit,
        source_position_unit=contract.position_unit,
        quality_flags=tuple(sorted(flags)),
        schema_version=obs.schema_version,
        writer_schema_id=sid,
        producer_id=str(producer["producer_id"]),
        produced_at=producer["produced_at"],
        correction_of=str(obs.correction_of) if obs.correction_of else None,
        fingerprint=content_fingerprint(rec),
    )
    return Prepared(row, event, None, None, eid, rid)


def _s(v: Any) -> str | None:
    return None if v is None else str(v)


# --------------------------------------------------------------------------- set stage


def build_id(bronze_snapshot_id: int, reference_fingerprint: str) -> str:
    """Equal inputs and rules -> equal id -> identical rows. A rebuild with the same
    id is a no-op, and any input change yields a new, distinguishable build."""
    material = f"{bronze_snapshot_id}|{reference_fingerprint}|{TRANSFORM_VERSION}"
    return hashlib.sha256(material.encode()).hexdigest()[:24]


# Set-level rejection details. One template per reason renders both the Python
# string and the Spark SQL expression, so the two cannot drift apart.
SEQUENCE_CONFLICT_TEMPLATE = (
    '[{"code":"SEQUENCE_CONFLICT","field":null,"held_event_id":"{held_event_id}",'
    '"message":"another event already holds this (robot, boot_number, sequence_number)"}]'
)
EVENT_ID_CONFLICT_TEMPLATE = (
    '[{"code":"EVENT_ID_CONFLICT","field":null,"held_kafka_offset":{held_kafka_offset},'
    '"held_kafka_partition":{held_kafka_partition},'
    '"message":"same event_id as an earlier record with different content"}]'
)


def _fill(template: str, values: Mapping[str, Any]) -> str:
    for k, v in values.items():
        template = template.replace("{" + k + "}", str(v))
    return template


def template_sql(template: str, columns: Mapping[str, str]) -> str:
    """concat('<literal>', CAST(<expr> AS STRING), ...) rendering of a template, with
    each {placeholder} replaced by a Spark SQL expression. Literal parts may contain
    JSON braces but no single quote or backslash (checked), so need no escaping."""
    pattern = "(" + "|".join(re.escape("{" + k + "}") for k in columns) + ")"
    out = []
    for part in re.split(pattern, template):
        if part.startswith("{") and part[1:-1] in columns:
            out.append(f"CAST({columns[part[1:-1]]} AS STRING)")
        elif part:
            if "'" in part or "\\" in part:
                raise ValueError(f"template literal needs escaping: {part!r}")
            out.append(f"'{part}'")
    return "concat(" + ", ".join(out) + ")"


def sequence_conflict_errors(held_event_id: str) -> str:
    return _fill(SEQUENCE_CONFLICT_TEMPLATE, {"held_event_id": held_event_id})


def event_id_conflict_errors(held_offset: int, held_partition: int) -> str:
    values = {"held_kafka_offset": held_offset, "held_kafka_partition": held_partition}
    return _fill(EVENT_ID_CONFLICT_TEMPLATE, values)


# reason_code -> (error_category, retry_eligible) for every reason Silver can emit.
REASON_TABLE: dict[str, tuple[str, bool]] = {
    code: reason_category(code)
    for code in [
        *(c.value for c in ReasonCode if c in REASONS),
        SilverReason.EVENT_ID_CONFLICT.value,
    ]
}


@dataclass
class SilverBuild:
    build_id: str
    events: list[dict[str, Any]]
    outcomes: list[dict[str, Any]]

    def counts(self) -> Counter:
        return Counter((o["outcome"], o["reason_code"]) for o in self.outcomes)


def build(prepared: Iterable[Prepared], *, build_id: str) -> SilverBuild:
    """Set stage. Input order does not matter: everything is decided in arrival order.

    1. Rejected rows from the row stage keep their reason.
    2. Valid rows grouped by event_id: the first is the representative; a later copy
       with the same content fingerprint is a DUPLICATE, with different content an
       EVENT_ID_CONFLICT (a producer bug; the first copy stays).
    3. Representatives that are not corrections grouped by (robot_id, boot_number,
       sequence_number): the first holds the slot, later ones are SEQUENCE_CONFLICT,
       as in Milestone 2. Corrections share their target's slot by design and are
       exempt.
    4. Corrections: per target, the latest-arriving correction wins. The target (an
       accepted event or a rejected record) and the losing corrections get
       superseded_by = winner.
    5. arrived_out_of_order: an accepted event of the same robot with a greater
       ordering key arrived earlier.
    """
    rows = sorted(prepared, key=lambda p: p.arrival)
    outcome: dict[tuple, dict[str, Any]] = {}

    def record(p: Prepared, kind: Outcome, reason: str | None, errors: str | None,
               canonical: str | None) -> None:  # fmt: skip
        category, retry = reason_category(reason) if reason else (None, False)
        r = p.row
        outcome[p.arrival] = {
            "source_topic": r["source_topic"],
            "kafka_topic_id": r["kafka_topic_id"],
            "kafka_partition": r["kafka_partition"],
            "kafka_offset": r["kafka_offset"],
            "outcome": kind.value,
            "reason_code": reason,
            "error_category": category,
            "retry_eligible": retry,
            "errors": errors,
            "event_id": p.event_id,
            "robot_id": p.robot_id,
            "canonical_event_id": canonical,
            "superseded_by": None,
            "writer_schema_id": r["value_schema_id"],
            "bronze_ingested_at": r["ingested_at"],
            "silver_build_id": build_id,
            "transform_version": TRANSFORM_VERSION,
        }

    valid: list[Prepared] = []
    for p in rows:  # 1
        if p.event is None:
            record(p, Outcome.REJECTED, p.reason, p.errors, None)
        else:
            valid.append(p)

    first: dict[str, Prepared] = {}  # 2
    reps: list[Prepared] = []
    duplicates: Counter = Counter()
    for p in valid:
        rep = first.get(p.event.event_id)
        if rep is None:
            first[p.event.event_id] = p
            reps.append(p)
        elif p.event.fingerprint == rep.event.fingerprint:
            duplicates[p.event.event_id] += 1
            record(p, Outcome.DUPLICATE, None, None, p.event.event_id)
        else:
            err = event_id_conflict_errors(rep.row["kafka_offset"], rep.row["kafka_partition"])
            record(p, Outcome.REJECTED, SilverReason.EVENT_ID_CONFLICT.value, err, None)

    holder: dict[tuple[str, int, int], str] = {}  # 3
    accepted: list[Prepared] = []
    for p in reps:
        e = p.event
        if e.correction_of is not None:
            accepted.append(p)
            continue
        slot = (e.robot_id, e.boot_number, e.sequence_number)
        if slot in holder:
            err = sequence_conflict_errors(holder[slot])
            record(p, Outcome.REJECTED, ReasonCode.SEQUENCE_CONFLICT.value, err, None)
        else:
            holder[slot] = e.event_id
            accepted.append(p)

    by_target: dict[str, list[str]] = defaultdict(list)  # 4 (arrival order)
    for p in accepted:
        if p.event.correction_of is not None:
            by_target[p.event.correction_of].append(p.event.event_id)
    superseded: dict[str, str] = {}
    for target, corrections in by_target.items():
        superseded[target] = corrections[-1]
    for _, corrections in by_target.items():
        for loser in corrections[:-1]:
            superseded.setdefault(loser, corrections[-1])
    seen_ids = {p.event_id for p in rows if p.event_id}

    best: dict[str, tuple[int, int]] = {}  # 5: running max ordering key per robot
    events: list[dict[str, Any]] = []
    for p in accepted:
        e, r = p.event, p.row
        key = (e.boot_number, e.sequence_number)
        prior = best.get(e.robot_id)
        late = prior is not None and prior > key
        if prior is None or key > prior:
            best[e.robot_id] = key
        flags = list(e.quality_flags)
        if e.correction_of is not None and e.correction_of not in seen_ids:
            flags.append(Flag.CORRECTION_TARGET_UNKNOWN)
        record(p, Outcome.ACCEPTED, None, None, e.event_id)
        events.append({
            "event_id": e.event_id,
            "robot_id": e.robot_id,
            "boot_id": e.boot_id,
            "boot_number": e.boot_number,
            "sequence_number": e.sequence_number,
            "observed_at": e.observed_at,
            "robot_model": e.robot_model,
            "firmware_version": e.firmware_version,
            "battery_pct": e.battery_pct,
            "x_m": e.x_m,
            "y_m": e.y_m,
            "heading_rad": e.heading_rad,
            "motor_temp_c": e.motor_temp_c,
            "lidar_ok": e.lidar_ok,
            "source_battery_unit": e.source_battery_unit,
            "source_position_unit": e.source_position_unit,
            "quality_flags": sorted(str(f) for f in flags),
            "schema_version": e.schema_version,
            "writer_schema_id": e.writer_schema_id,
            "producer_id": e.producer_id,
            "produced_at": e.produced_at,
            "correction_of": e.correction_of,
            "superseded_by": superseded.get(e.event_id),
            "is_current": e.event_id not in superseded,
            "arrived_out_of_order": late,
            "duplicate_deliveries": duplicates[e.event_id],
            "source_topic": r["source_topic"],
            "kafka_topic_id": r["kafka_topic_id"],
            "kafka_partition": r["kafka_partition"],
            "kafka_offset": r["kafka_offset"],
            "kafka_timestamp": r["kafka_timestamp"],
            "bronze_ingested_at": r["ingested_at"],
            "silver_build_id": build_id,
            "transform_version": TRANSFORM_VERSION,
        })  # fmt: skip

    for o in outcome.values():  # supersession is visible on every record of the target
        if o["event_id"] in superseded:
            o["superseded_by"] = superseded[o["event_id"]]
    outcomes = [outcome[p.arrival] for p in rows]
    return SilverBuild(build_id, events, outcomes)


EVENT_COLUMNS = SILVER_TELEMETRY_EVENTS.column_names
OUTCOME_COLUMNS = SILVER_TELEMETRY_RECORD_OUTCOMES.column_names

# ------------------------------------------------------------------ row stage on Spark

# The row stage's output as flat columns: what mapInPandas returns and the Spark set
# stage consumes. Bronze lineage first, then the decision, then the canonical event.
PREPARED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("source_topic", "STRING"),
    ("kafka_topic_id", "STRING"),
    ("kafka_partition", "INT"),
    ("kafka_offset", "BIGINT"),
    ("kafka_timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
    ("value_schema_id", "BIGINT"),
    ("status", "STRING"),  # VALID | INVALID
    ("reason_code", "STRING"),
    ("errors", "STRING"),
    ("event_id", "STRING"),
    ("robot_id", "STRING"),
    *((f, t) for f, t in (
        ("boot_id", "STRING"), ("boot_number", "BIGINT"), ("sequence_number", "BIGINT"),
        ("observed_at", "TIMESTAMP"), ("robot_model", "STRING"), ("firmware_version", "STRING"),
        ("battery_pct", "DOUBLE"), ("x_m", "DOUBLE"), ("y_m", "DOUBLE"),
        ("heading_rad", "DOUBLE"), ("motor_temp_c", "DOUBLE"), ("lidar_ok", "BOOLEAN"),
        ("source_battery_unit", "STRING"), ("source_position_unit", "STRING"),
        ("quality_flags", "ARRAY<STRING>"), ("schema_version", "INT"),
        ("writer_schema_id", "BIGINT"), ("producer_id", "STRING"), ("produced_at", "TIMESTAMP"),
        ("correction_of", "STRING"), ("fingerprint", "STRING"),
    )),
)  # fmt: skip
PREPARED_SCHEMA = ", ".join(f"{n} {t}" for n, t in PREPARED_COLUMNS)
_CANONICAL_FIELDS = [n for n, _ in PREPARED_COLUMNS[12:]]


def prepared_row(p: Prepared) -> dict[str, Any]:
    """Flatten a Prepared for Spark. Keys are exactly PREPARED_COLUMNS."""
    r = p.row
    out: dict[str, Any] = {
        "source_topic": r["source_topic"],
        "kafka_topic_id": r["kafka_topic_id"],
        "kafka_partition": r["kafka_partition"],
        "kafka_offset": r["kafka_offset"],
        "kafka_timestamp": r["kafka_timestamp"],
        "ingested_at": r["ingested_at"],
        "value_schema_id": r["value_schema_id"],
        "status": "VALID" if p.event is not None else "INVALID",
        "reason_code": p.reason,
        "errors": p.errors,
        "event_id": p.event_id,
        "robot_id": p.robot_id,
    }
    e = p.event
    for name in _CANONICAL_FIELDS:
        value = getattr(e, name) if e is not None else None
        out[name] = list(value) if name == "quality_flags" and value is not None else value
    return out
