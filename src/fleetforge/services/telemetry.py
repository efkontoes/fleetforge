"""Telemetry ingestion: validate, then conditionally apply to the latest-state read model."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, Engine

from fleetforge.contracts.api import TelemetryOutcome
from fleetforge.contracts.telemetry import TelemetryEventV1
from fleetforge.db import repository as repo
from fleetforge.domain.telemetry import (
    Decision,
    OrderingKey,
    ReasonCode,
    Rejection,
    check_semantics,
    classify,
    dedup_key,
    identity_hints,
    parse_event,
    payload_for_audit,
)
from fleetforge.observability import TELEMETRY_EVENTS

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestResult:
    outcome: TelemetryOutcome
    event_id: UUID | None = None
    reason_code: ReasonCode | None = None
    errors: list[dict[str, Any]] = field(default_factory=list)
    state_version: int | None = None


def ingest(
    engine: Engine, body: bytes, received_at: datetime, max_clock_skew: timedelta
) -> IngestResult:
    result = _ingest(engine, body, received_at, max_clock_skew)
    TELEMETRY_EVENTS.labels(result.outcome.value, result.reason_code or "").inc()
    if result.reason_code is not None:
        log.info(
            "telemetry not applied",
            extra={
                "outcome": result.outcome.value,
                "reason_code": result.reason_code.value,
                "event_id": result.event_id,
            },
        )
    return result


def _ingest(
    engine: Engine, body: bytes, received_at: datetime, max_clock_skew: timedelta
) -> IngestResult:
    # 1. Structural validity: no database needed.
    parsed = parse_event(body)
    if isinstance(parsed, Rejection):
        payload = payload_for_audit(body)
        event_id, robot_id = identity_hints(payload)
        with engine.begin() as conn:
            _record(conn, parsed, body, event_id, robot_id, received_at)
        return _rejected(parsed, event_id)

    event = parsed

    # 2. Semantic validity: no database needed.
    if (rejection := check_semantics(event, received_at, max_clock_skew)) is not None:
        with engine.begin() as conn:
            _record(conn, rejection, body, event.event_id, event.robot_id, received_at)
        return _rejected(rejection, event.event_id)

    # 3. Referential validity + ordering: one transaction.
    with engine.begin() as conn:
        robot_exists, boot_number = repo.resolve_boot(conn, event.robot_id, event.boot_id)
        if not robot_exists or boot_number is None:
            code = ReasonCode.UNKNOWN_ROBOT if not robot_exists else ReasonCode.UNKNOWN_BOOT
            rejection = Rejection(
                code,
                [{"code": code.value, "field": None, "message": _unknown_message(code, event)}],
            )
            _record(conn, rejection, body, event.event_id, event.robot_id, received_at)
            return _rejected(rejection, event.event_id)

        state_version = repo.apply_telemetry(conn, event, boot_number, received_at)
        if state_version is not None:
            return IngestResult(
                TelemetryOutcome.APPLIED, event.event_id, state_version=state_version
            )

        # Not applied. The decision is already final and correct; this read only
        # explains *why*, for the response and the audit trail. If a newer event
        # committed between the upsert and this read, a true duplicate can be
        # labelled STALE. That affects the label only, never the stored state.
        stored = repo.get_stored_position(conn, event.robot_id)
        incoming = OrderingKey(boot_number, event.sequence_number)
        decision = classify(incoming, event.event_id, stored)

        if decision is Decision.DUPLICATE:
            return IngestResult(TelemetryOutcome.DUPLICATE, event.event_id)
        if decision is Decision.APPLY:  # pragma: no cover - would mean the upsert is broken
            raise RuntimeError(f"upsert declined event {event.event_id} that classify() accepts")

        code, outcome = (
            (ReasonCode.STALE_SEQUENCE, TelemetryOutcome.STALE)
            if decision is Decision.STALE
            else (ReasonCode.SEQUENCE_CONFLICT, TelemetryOutcome.CONFLICT)
        )
        held = {
            "boot_number": stored.key.boot_number,
            "sequence_number": stored.key.sequence_number,
        }
        rejection = Rejection(
            code,
            [
                {
                    "code": code.value,
                    "field": "sequence_number",
                    "message": (
                        f"incoming (boot {incoming.boot_number}, seq {incoming.sequence_number}) "
                        f"vs held (boot {held['boot_number']}, seq {held['sequence_number']})"
                    ),
                    "held": held,
                    "held_event_id": str(stored.event_id),
                }
            ],
        )
        _record(conn, rejection, body, event.event_id, event.robot_id, received_at)
        return IngestResult(outcome, event.event_id, code, rejection.errors)


def _unknown_message(code: ReasonCode, event: TelemetryEventV1) -> str:
    if code is ReasonCode.UNKNOWN_ROBOT:
        return f"robot {event.robot_id} is not registered"
    return f"boot {event.boot_id} is not registered for robot {event.robot_id}"


def _rejected(rejection: Rejection, event_id: UUID | None) -> IngestResult:
    return IngestResult(
        TelemetryOutcome.REJECTED, event_id, rejection.reason_code, rejection.errors
    )


def _record(
    conn: Connection,
    rejection: Rejection,
    body: bytes,
    event_id: UUID | None,
    robot_id: UUID | None,
    received_at: datetime,
) -> None:
    repo.record_rejection(
        conn,
        dedup_key=dedup_key(event_id, body),
        event_id=event_id,
        robot_id=robot_id,
        reason_code=rejection.reason_code.value,
        errors=rejection.errors,
        payload=payload_for_audit(body),
        received_at=received_at,
    )
