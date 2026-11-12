"""Telemetry validation and ordering rules. Pure functions: no I/O, no database.

Three layers of validity (spec section 6):
  1. structural  - can we decode it as a v1 event?            -> parse_event()
  2. semantic    - is it physically/logically plausible?       -> check_semantics()
  3. referential - do we know this robot and boot session?     -> needs the DB (services layer)
Ordering (is it newer than what we hold?) is decided by classify(), which is the
Python mirror of the conditional upsert in fleetforge.db.repository.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, NamedTuple
from uuid import UUID

from pydantic import ValidationError

from fleetforge.contracts.telemetry import TelemetryEventV1


class ReasonCode(StrEnum):
    MALFORMED_JSON = "MALFORMED_JSON"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_FIELD = "INVALID_FIELD"
    UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
    BATTERY_OUT_OF_RANGE = "BATTERY_OUT_OF_RANGE"
    OBSERVED_AT_IN_FUTURE = "OBSERVED_AT_IN_FUTURE"
    UNKNOWN_ROBOT = "UNKNOWN_ROBOT"
    UNKNOWN_BOOT = "UNKNOWN_BOOT"
    STALE_SEQUENCE = "STALE_SEQUENCE"
    SEQUENCE_CONFLICT = "SEQUENCE_CONFLICT"


# When one payload has several problems, the primary reason is the most fundamental one.
_STRUCTURAL_PRECEDENCE = [
    ReasonCode.MALFORMED_JSON,
    ReasonCode.UNSUPPORTED_SCHEMA_VERSION,
    ReasonCode.MISSING_FIELD,
    ReasonCode.INVALID_FIELD,
]


@dataclass(frozen=True)
class Rejection:
    reason_code: ReasonCode
    errors: list[dict[str, Any]] = field(default_factory=list)


def _error(code: ReasonCode, field_name: str | None, message: str) -> dict[str, Any]:
    return {"code": code.value, "field": field_name, "message": message}


# --------------------------------------------------------------------------- structural


def parse_event(body: bytes) -> TelemetryEventV1 | Rejection:
    try:
        return TelemetryEventV1.model_validate_json(body)
    except ValidationError as exc:
        errors = []
        for err in exc.errors(include_url=False, include_input=False):
            loc = ".".join(str(p) for p in err["loc"]) or None
            if err["type"] == "json_invalid":
                code = ReasonCode.MALFORMED_JSON
            elif err["type"] == "missing":
                code = ReasonCode.MISSING_FIELD
            elif loc == "schema_version":
                code = ReasonCode.UNSUPPORTED_SCHEMA_VERSION
            else:
                code = ReasonCode.INVALID_FIELD
            errors.append(_error(code, loc, err["msg"]))
        codes = {e["code"] for e in errors}
        primary = next(c for c in _STRUCTURAL_PRECEDENCE if c.value in codes)
        return Rejection(primary, errors)


# --------------------------------------------------------------------------- semantic


def check_semantics(
    event: TelemetryEventV1, received_at: datetime, max_clock_skew: timedelta
) -> Rejection | None:
    errors = []
    if not 0.0 <= event.battery_pct <= 100.0:
        errors.append(
            _error(
                ReasonCode.BATTERY_OUT_OF_RANGE,
                "battery_pct",
                f"battery_pct={event.battery_pct} outside [0, 100]",
            )
        )
    if event.observed_at > received_at + max_clock_skew:
        errors.append(
            _error(
                ReasonCode.OBSERVED_AT_IN_FUTURE,
                "observed_at",
                f"observed_at is {(event.observed_at - received_at).total_seconds():.1f}s "
                f"ahead of server time (tolerance {max_clock_skew.total_seconds():.0f}s)",
            )
        )
    if not errors:
        return None
    return Rejection(ReasonCode(errors[0]["code"]), errors)


# --------------------------------------------------------------------------- ordering


class OrderingKey(NamedTuple):
    """Lexicographic: any event from a later boot is newer than every event of an earlier boot.

    Python compares NamedTuples element by element, exactly like PostgreSQL's row
    comparison (boot_number, sequence_number) > (..., ...) in the upsert.
    """

    boot_number: int
    sequence_number: int


@dataclass(frozen=True)
class StoredPosition:
    key: OrderingKey
    event_id: UUID


class Decision(StrEnum):
    APPLY = "APPLY"
    DUPLICATE = "DUPLICATE"  # same position, same event: an idempotent retry
    STALE = "STALE"  # older than what we already hold
    CONFLICT = "CONFLICT"  # same position, different event: a producer bug


def classify(incoming: OrderingKey, event_id: UUID, stored: StoredPosition | None) -> Decision:
    if stored is None or incoming > stored.key:
        return Decision.APPLY
    if incoming == stored.key:
        return Decision.DUPLICATE if event_id == stored.event_id else Decision.CONFLICT
    return Decision.STALE


# --------------------------------------------------------------------------- audit helpers


def payload_for_audit(body: bytes) -> Any:
    """Best-effort JSON value of the raw body, safe to store in a JSONB column."""
    text = body.decode("utf-8", errors="replace")
    try:
        # NaN/Infinity are kept as strings: PostgreSQL JSONB cannot represent them.
        value = json.loads(text, parse_constant=lambda c: c)
    except ValueError:
        value = text
    # JSONB cannot store U+0000.
    encoded = json.dumps(value).replace("\\u0000", "\\ufffd")
    return json.loads(encoded)


def identity_hints(payload: Any) -> tuple[UUID | None, UUID | None]:
    """Extract event_id / robot_id from an invalid payload when they are parseable."""

    def as_uuid(value: Any) -> UUID | None:
        try:
            return UUID(str(value)) if value is not None else None
        except ValueError:
            return None

    if not isinstance(payload, dict):
        return None, None
    return as_uuid(payload.get("event_id")), as_uuid(payload.get("robot_id"))


def dedup_key(event_id: UUID | None, body: bytes) -> str:
    """Rejection audit identity: event_id if we have one, else a hash of the exact bytes."""
    if event_id is not None:
        return str(event_id)
    return "sha256:" + hashlib.sha256(body).hexdigest()
