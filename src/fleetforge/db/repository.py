"""All SQL for the operational schema.

Functions take an open Connection and never commit: the caller (services layer)
owns the transaction boundary, so several of these calls can be made atomic.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, RowMapping, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import bindparam

from fleetforge.contracts.telemetry import TelemetryEventV1
from fleetforge.domain.errors import category_of, initial_status, retry_eligible
from fleetforge.domain.telemetry import OrderingKey, ReasonCode, StoredPosition

# ------------------------------------------------------------------ robots & boots


def insert_robot(conn: Connection, robot_id: UUID, model: str, firmware: str) -> RowMapping | None:
    """Insert if absent. Returns the new row, or None if the robot already existed."""
    return (
        conn.execute(
            text("""
                INSERT INTO fleet.robots (robot_id, robot_model, firmware_version)
                VALUES (:robot_id, :model, :firmware)
                ON CONFLICT (robot_id) DO NOTHING
                RETURNING robot_id, robot_model, firmware_version, lifecycle_status, registered_at
            """),
            {"robot_id": robot_id, "model": model, "firmware": firmware},
        )
        .mappings()
        .first()
    )


def get_robot(conn: Connection, robot_id: UUID, *, lock: bool = False) -> RowMapping | None:
    sql = """
        SELECT robot_id, robot_model, firmware_version, lifecycle_status, registered_at
        FROM fleet.robots WHERE robot_id = :robot_id
    """
    if lock:
        # NO KEY UPDATE (not plain UPDATE): it serializes boot registrations for this robot
        # but does not conflict with the KEY SHARE locks that foreign-key checks take, so
        # telemetry upserts and mission inserts for the same robot are not blocked.
        sql += " FOR NO KEY UPDATE"
    return conn.execute(text(sql), {"robot_id": robot_id}).mappings().first()


def firmware_exists(conn: Connection, model: str, firmware: str) -> bool:
    return (
        conn.execute(
            text("SELECT 1 FROM fleet.firmware_versions WHERE model_id = :m AND version = :v"),
            {"m": model, "v": firmware},
        ).first()
        is not None
    )


def get_boot(conn: Connection, boot_id: UUID) -> RowMapping | None:
    return (
        conn.execute(
            text("SELECT robot_id, boot_id, boot_number FROM fleet.robot_boots WHERE boot_id = :b"),
            {"b": boot_id},
        )
        .mappings()
        .first()
    )


def insert_next_boot(conn: Connection, robot_id: UUID, boot_id: UUID) -> int:
    """Assign boot_number = previous + 1. Caller must hold the robot row lock."""
    return conn.execute(
        text("""
            INSERT INTO fleet.robot_boots (boot_id, robot_id, boot_number)
            SELECT :boot_id, :robot_id, COALESCE(MAX(boot_number), 0) + 1
            FROM fleet.robot_boots WHERE robot_id = :robot_id
            RETURNING boot_number
        """),
        {"boot_id": boot_id, "robot_id": robot_id},
    ).scalar_one()


def resolve_boot(conn: Connection, robot_id: UUID, boot_id: UUID) -> tuple[bool, int | None]:
    """(robot_exists, boot_number-or-None) in one round trip."""
    row = conn.execute(
        text("""
            SELECT b.boot_number
            FROM fleet.robots r
            LEFT JOIN fleet.robot_boots b ON b.robot_id = r.robot_id AND b.boot_id = :boot_id
            WHERE r.robot_id = :robot_id
        """),
        {"robot_id": robot_id, "boot_id": boot_id},
    ).first()
    if row is None:
        return False, None
    return True, row.boot_number


# ------------------------------------------------------------------ latest state

# The heart of Milestone 1. One statement both decides and writes:
#  * no row yet          -> INSERT
#  * row exists          -> UPDATE only if the incoming (boot, seq) is strictly newer
# On conflict PostgreSQL locks the existing row and evaluates WHERE against the latest
# *committed* version, so a concurrent writer cannot slip in between check and write.
_APPLY_TELEMETRY = text("""
    INSERT INTO fleet.robot_latest_state AS s (
        robot_id, boot_id, boot_number, sequence_number, event_id, schema_version,
        observed_at, received_at, battery_pct, x_m, y_m, state_version, updated_at
    ) VALUES (
        :robot_id, :boot_id, :boot_number, :sequence_number, :event_id, :schema_version,
        :observed_at, :received_at, :battery_pct, :x_m, :y_m, 1, now()
    )
    ON CONFLICT (robot_id) DO UPDATE SET
        boot_id         = EXCLUDED.boot_id,
        boot_number     = EXCLUDED.boot_number,
        sequence_number = EXCLUDED.sequence_number,
        event_id        = EXCLUDED.event_id,
        schema_version  = EXCLUDED.schema_version,
        observed_at     = EXCLUDED.observed_at,
        received_at     = EXCLUDED.received_at,
        battery_pct     = EXCLUDED.battery_pct,
        x_m             = EXCLUDED.x_m,
        y_m             = EXCLUDED.y_m,
        state_version   = s.state_version + 1,
        updated_at      = now()
    WHERE (EXCLUDED.boot_number, EXCLUDED.sequence_number)
        > (s.boot_number, s.sequence_number)
    RETURNING state_version
""")


def apply_telemetry(
    conn: Connection, event: TelemetryEventV1, boot_number: int, received_at: datetime
) -> int | None:
    """Returns the new state_version, or None if the event was not newer."""
    return conn.execute(
        _APPLY_TELEMETRY,
        {
            "robot_id": event.robot_id,
            "boot_id": event.boot_id,
            "boot_number": boot_number,
            "sequence_number": event.sequence_number,
            "event_id": event.event_id,
            "schema_version": event.schema_version,
            "observed_at": event.observed_at,
            "received_at": received_at,
            "battery_pct": event.battery_pct,
            "x_m": event.x_m,
            "y_m": event.y_m,
        },
    ).scalar_one_or_none()


def get_stored_position(conn: Connection, robot_id: UUID) -> StoredPosition | None:
    row = conn.execute(
        text("""
            SELECT boot_number, sequence_number, event_id
            FROM fleet.robot_latest_state WHERE robot_id = :robot_id
        """),
        {"robot_id": robot_id},
    ).first()
    if row is None:
        return None
    return StoredPosition(OrderingKey(row.boot_number, row.sequence_number), row.event_id)


def get_robot_state(conn: Connection, robot_id: UUID) -> RowMapping | None:
    return (
        conn.execute(
            text("""
                SELECT s.robot_id, s.boot_id, s.boot_number, s.sequence_number, s.event_id,
                       s.observed_at, s.received_at, s.battery_pct, s.x_m, s.y_m,
                       s.state_version,
                       m.mission_id AS active_mission_id, m.status AS active_mission_status,
                       EXTRACT(EPOCH FROM (now() - s.received_at))::float8 AS age_s
                FROM fleet.robot_latest_state s
                LEFT JOIN fleet.missions m
                       ON m.robot_id = s.robot_id AND m.status IN ('ASSIGNED', 'RUNNING')
                WHERE s.robot_id = :robot_id
            """),
            {"robot_id": robot_id},
        )
        .mappings()
        .first()
    )


# ------------------------------------------------------------------ rejections

_INSERT_REJECTION = text("""
    INSERT INTO fleet.telemetry_rejections
        (dedup_key, event_id, robot_id, reason_code, errors, payload, received_at,
         error_category, retry_eligible, status, source, source_topic, source_partition,
         source_offset, consumer_version, payload_bytes)
    VALUES (:dedup_key, :event_id, :robot_id, :reason_code, :errors, :payload, :received_at,
            :error_category, :retry_eligible, :status, :source, :source_topic,
            :source_partition, :source_offset, :consumer_version, :payload_bytes)
    ON CONFLICT (dedup_key, reason_code) DO NOTHING
""").bindparams(bindparam("errors", type_=JSONB), bindparam("payload", type_=JSONB))


def record_rejection(
    conn: Connection,
    *,
    dedup_key: str,
    event_id: UUID | None,
    robot_id: UUID | None,
    reason_code: str,
    errors: list[dict[str, Any]],
    payload: Any,
    received_at: datetime,
    source: str = "http",
    source_topic: str | None = None,
    source_partition: int | None = None,
    source_offset: int | None = None,
    consumer_version: str | None = None,
    payload_bytes: bytes | None = None,
) -> bool:
    """Idempotent: returns False if this (event, reason) was already recorded.
    Category, retry eligibility and initial status follow the error taxonomy."""
    code = ReasonCode(reason_code)
    result = conn.execute(
        _INSERT_REJECTION,
        {
            "error_category": category_of(code).value,
            "retry_eligible": retry_eligible(code),
            "status": initial_status(code),
            "source": source,
            "source_topic": source_topic,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "consumer_version": consumer_version,
            "payload_bytes": payload_bytes,
            "dedup_key": dedup_key,
            "event_id": event_id,
            "robot_id": robot_id,
            "reason_code": reason_code,
            "errors": errors,
            "payload": payload,
            "received_at": received_at,
        },
    )
    return result.rowcount == 1


# ------------------------------------------------------------------ missions & outbox

_MISSION_COLUMNS = "mission_id, robot_id, status, version, created_at, updated_at"


def insert_mission(
    conn: Connection, mission_id: UUID, robot_id: UUID, status: str
) -> RowMapping | None:
    return (
        conn.execute(
            text(f"""
                INSERT INTO fleet.missions (mission_id, robot_id, status)
                VALUES (:mission_id, :robot_id, :status)
                ON CONFLICT (mission_id) DO NOTHING
                RETURNING {_MISSION_COLUMNS}
            """),
            {"mission_id": mission_id, "robot_id": robot_id, "status": status},
        )
        .mappings()
        .first()
    )


def get_mission(conn: Connection, mission_id: UUID) -> RowMapping | None:
    return (
        conn.execute(
            text(f"SELECT {_MISSION_COLUMNS} FROM fleet.missions WHERE mission_id = :id"),
            {"id": mission_id},
        )
        .mappings()
        .first()
    )


def update_mission_status(
    conn: Connection, mission_id: UUID, to_status: str, expected_version: int
) -> RowMapping | None:
    """Optimistic concurrency: succeeds only if nobody changed the mission since the
    caller read it. Returns the updated row, or None on version mismatch."""
    return (
        conn.execute(
            text(f"""
                UPDATE fleet.missions
                SET status = :to_status, version = version + 1, updated_at = now()
                WHERE mission_id = :mission_id AND version = :expected_version
                RETURNING {_MISSION_COLUMNS}
            """),
            {
                "mission_id": mission_id,
                "to_status": to_status,
                "expected_version": expected_version,
            },
        )
        .mappings()
        .first()
    )


def insert_mission_event(
    conn: Connection,
    *,
    event_id: UUID,
    mission_id: UUID,
    from_status: str | None,
    to_status: str,
    mission_version: int,
) -> None:
    conn.execute(
        text("""
            INSERT INTO fleet.mission_events
                (event_id, mission_id, from_status, to_status, mission_version)
            VALUES (:event_id, :mission_id, :from_status, :to_status, :mission_version)
        """),
        {
            "event_id": event_id,
            "mission_id": mission_id,
            "from_status": from_status,
            "to_status": to_status,
            "mission_version": mission_version,
        },
    )


def get_mission_event(conn: Connection, event_id: UUID) -> RowMapping | None:
    return (
        conn.execute(
            text("""
                SELECT event_id, mission_id, from_status, to_status, mission_version
                FROM fleet.mission_events WHERE event_id = :id
            """),
            {"id": event_id},
        )
        .mappings()
        .first()
    )


_INSERT_OUTBOX = text("""
    INSERT INTO fleet.outbox_events (event_id, aggregate_type, aggregate_id, event_type, payload)
    VALUES (:event_id, :aggregate_type, :aggregate_id, :event_type, :payload)
""").bindparams(bindparam("payload", type_=JSONB))


def insert_outbox_event(
    conn: Connection,
    *,
    event_id: UUID,
    aggregate_type: str,
    aggregate_id: UUID,
    event_type: str,
    payload: dict[str, Any],
) -> None:
    conn.execute(
        _INSERT_OUTBOX,
        {
            "event_id": event_id,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "payload": payload,
        },
    )
