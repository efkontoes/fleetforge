"""Transport-neutral observation and the batch ordering planner. Pure: no I/O.

The batch planner is what makes batching *correct*. A batch can hold several
observations for one robot (seq 5, 7, 6, 7-again). Writing only the batch maximum
gives the right final row, but the per-event outcomes and the state_version would
then differ from one-event-at-a-time processing. The planner replays the batch in
delivery order against the state held at the start of the batch, so every event
gets exactly the decision it would have received alone, and the database receives
one write per robot: the winner, plus how many events were applied before it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fleetforge.domain.telemetry import (
    Decision,
    OrderingKey,
    ReasonCode,
    Rejection,
    StoredPosition,
    classify,
)

REQUIRED = (
    "schema_version",
    "event_id",
    "robot_id",
    "boot_id",
    "boot_number",
    "sequence_number",
    "observed_at",
    "firmware_version",
    "battery_pct",
    "position",
)


@dataclass(frozen=True)
class Observation:
    event_id: UUID
    robot_id: UUID
    boot_id: UUID
    boot_number: int
    sequence_number: int
    observed_at: datetime
    battery_pct: float
    x_m: float
    y_m: float
    schema_version: int
    firmware_version: str
    correction_of: UUID | None = None
    produced_at: datetime | None = None


def _err(code: ReasonCode, field_name: str | None, message: str) -> dict[str, Any]:
    return {"code": code.value, "field": field_name, "message": message}


def observation_from_record(rec: dict[str, Any]) -> Observation | Rejection:
    """Contract validation of a decoded Avro record (decoding already guaranteed the
    *writer's* types; this checks the fields *we* require are present and sane)."""
    missing = [f for f in REQUIRED if rec.get(f) is None]
    if missing:
        return Rejection(
            ReasonCode.MISSING_FIELD,
            [_err(ReasonCode.MISSING_FIELD, f, "required field missing") for f in missing],
        )
    if rec["schema_version"] != 1:
        return Rejection(
            ReasonCode.UNSUPPORTED_SCHEMA_VERSION,
            [_err(ReasonCode.UNSUPPORTED_SCHEMA_VERSION, "schema_version", "expected 1")],
        )
    errors = []
    try:
        pos = rec["position"]
        values = {
            "battery_pct": float(rec["battery_pct"]),
            "x_m": float(pos["x_m"]),
            "y_m": float(pos["y_m"]),
        }
        ids = {k: _uuid(rec[k]) for k in ("event_id", "robot_id", "boot_id")}
        observed_at = _aware(rec["observed_at"])
    except (KeyError, TypeError, ValueError) as exc:
        return Rejection(ReasonCode.INVALID_FIELD, [_err(ReasonCode.INVALID_FIELD, None, str(exc))])
    for name, v in values.items():
        if not math.isfinite(v):  # Avro doubles can carry NaN/Infinity
            errors.append(_err(ReasonCode.INVALID_FIELD, name, f"{name} is not finite"))
    if rec["sequence_number"] < 0 or rec["boot_number"] < 1:
        errors.append(_err(ReasonCode.INVALID_FIELD, "sequence_number", "out of range"))
    if errors:
        return Rejection(ReasonCode.INVALID_FIELD, errors)
    producer = rec.get("producer") or {}
    return Observation(
        **ids,
        boot_number=int(rec["boot_number"]),
        sequence_number=int(rec["sequence_number"]),
        observed_at=observed_at,
        **values,
        schema_version=int(rec["schema_version"]),
        firmware_version=str(rec["firmware_version"]),
        correction_of=_uuid(rec["correction_of"]) if rec.get("correction_of") else None,
        produced_at=_aware(producer["produced_at"]) if producer.get("produced_at") else None,
    )


def _uuid(v: Any) -> UUID:
    return v if isinstance(v, UUID) else UUID(str(v))


def _aware(v: Any) -> datetime:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)
    raise TypeError(f"expected timestamp, got {type(v).__name__}")


# --------------------------------------------------------------------------- planning


@dataclass(frozen=True)
class Candidate:
    """An observation that passed structural, semantic and referential checks."""

    index: int  # position in the batch (delivery order)
    obs: Observation


@dataclass(frozen=True)
class Planned:
    index: int
    decision: Decision
    held: StoredPosition | None  # what the event was compared against


@dataclass
class Winner:
    obs: Observation
    applied: int  # how many events of this batch were APPLIED for this robot


@dataclass
class Plan:
    decisions: list[Planned] = field(default_factory=list)
    winners: dict[UUID, Winner] = field(default_factory=dict)


def plan_batch(candidates: list[Candidate], stored: dict[UUID, StoredPosition]) -> Plan:
    """Sequential-equivalent decisions for a batch.

    Precondition: candidates are in delivery order. For Kafka that is offset order
    within a partition; all events of one robot share a partition (key = robot_id),
    so per-robot order is the producer's order.
    """
    plan = Plan()
    current = dict(stored)
    for c in candidates:
        rid = c.obs.robot_id
        key = OrderingKey(c.obs.boot_number, c.obs.sequence_number)
        held = current.get(rid)
        decision = classify(key, c.obs.event_id, held)
        if decision is Decision.APPLY:
            current[rid] = StoredPosition(key, c.obs.event_id)
            prev = plan.winners.get(rid)
            plan.winners[rid] = Winner(c.obs, (prev.applied if prev else 0) + 1)
        plan.decisions.append(Planned(c.index, decision, held))
    return plan
