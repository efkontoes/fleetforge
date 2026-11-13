"""Set-based SQL for batched telemetry projection.

Every statement here is ONE round trip regardless of batch size: rows travel as
PostgreSQL arrays and are expanded server-side with unnest(). They run on the
psycopg connection underneath the caller's SQLAlchemy transaction.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Connection

from fleetforge.domain.errors import (
    OffsetLedgerError,
    category_of,
    initial_status,
    retry_eligible,
)
from fleetforge.domain.ingest import Observation
from fleetforge.domain.telemetry import OrderingKey, ReasonCode, StoredPosition


def _cur(conn: Connection):
    return conn.connection.driver_connection.cursor()


# ------------------------------------------------------------------ offset ledger


@dataclass(frozen=True)
class LedgerPosition:
    next_offset: int
    kafka_topic_id: str | None  # Kafka's topic UUID when the row was written


def lock_ledger(
    conn: Connection,
    group: str,
    parts: Iterable[tuple[str, int]],
    topic_ids: dict[str, str] | None = None,
) -> dict[tuple[str, int], int]:
    """Current next_offset per (topic, partition), row-locked until commit.

    The lock fences a zombie consumer. A row written for another topic UUID (the
    topic was recreated) raises OffsetLedgerError.
    """
    parts = sorted(set(parts))
    topic_ids = topic_ids or {}
    topics, partitions = [t for t, _ in parts], [p for _, p in parts]
    ids = [topic_ids.get(t) for t in topics]
    with _cur(conn) as cur:
        # FOR UPDATE only locks rows that exist. Seed missing rows first: a concurrent
        # seeder of the same key blocks on the unique index until we commit, so even
        # the very first batch of a partition is fenced.
        cur.execute(
            """
            INSERT INTO fleet.consumer_offsets
                (consumer_group, topic, partition, next_offset, kafka_topic_id)
            SELECT %s, t, p, 0, i FROM unnest(%s::text[], %s::int[], %s::text[]) AS u(t, p, i)
            ON CONFLICT DO NOTHING
            """,
            (group, topics, partitions, ids),
        )
        cur.execute(
            """
            SELECT topic, partition, next_offset, kafka_topic_id FROM fleet.consumer_offsets
            WHERE consumer_group = %s
              AND (topic, partition) IN (SELECT * FROM unnest(%s::text[], %s::int[]))
            ORDER BY topic, partition
            FOR UPDATE
            """,
            (group, topics, partitions),
        )
        out = {}
        for t, p, o, tid in cur.fetchall():
            expected = topic_ids.get(t)
            if expected and tid and tid != expected:
                raise OffsetLedgerError(
                    f"ledger row {group}/{t}/{p} belongs to topic id {tid}, but the topic now"
                    f" has id {expected}: it was deleted and recreated. Refusing to apply"
                    " old offsets to a new log; see `fleet-replay ledger-reset`."
                )
            out[(t, p)] = o
        return out


def advance_ledger(
    conn: Connection,
    group: str,
    next_offsets: dict[tuple[str, int], int],
    topic_ids: dict[str, str] | None = None,
) -> None:
    if not next_offsets:
        return
    items = sorted(next_offsets.items())
    topic_ids = topic_ids or {}
    with _cur(conn) as cur:
        cur.execute(
            """
            INSERT INTO fleet.consumer_offsets AS c
                (consumer_group, topic, partition, next_offset, kafka_topic_id)
            SELECT %s, t, p, o, i
            FROM unnest(%s::text[], %s::int[], %s::int8[], %s::text[]) AS u(t, p, o, i)
            ON CONFLICT (consumer_group, topic, partition) DO UPDATE
            SET next_offset = GREATEST(c.next_offset, EXCLUDED.next_offset),
                kafka_topic_id = COALESCE(c.kafka_topic_id, EXCLUDED.kafka_topic_id),
                updated_at = now()
            """,
            (
                group,
                [k[0] for k, _ in items],
                [k[1] for k, _ in items],
                [o for _, o in items],
                [topic_ids.get(k[0]) for k, _ in items],
            ),
        )


def read_ledger(conn: Connection, group: str, topic: str) -> dict[int, LedgerPosition]:
    with _cur(conn) as cur:
        cur.execute(
            "SELECT partition, next_offset, kafka_topic_id FROM fleet.consumer_offsets"
            " WHERE consumer_group = %s AND topic = %s",
            (group, topic),
        )
        return {p: LedgerPosition(o, tid) for p, o, tid in cur.fetchall()}


# ------------------------------------------------------------------ registry lookups


def lookup_boots(conn: Connection, keys: list[tuple[UUID, UUID]]) -> dict:
    """(robot_id, boot_id) -> boot_number for registered boots."""
    with _cur(conn) as cur:
        cur.execute(
            """
            SELECT b.robot_id, b.boot_id, b.boot_number FROM fleet.robot_boots b
            JOIN unnest(%s::uuid[], %s::uuid[]) AS k(r, bt) ON b.robot_id = k.r AND b.boot_id = k.bt
            """,
            ([k[0] for k in keys], [k[1] for k in keys]),
        )
        return {(r, b): n for r, b, n in cur.fetchall()}


def lookup_robots(conn: Connection, robot_ids: list[UUID]) -> dict[UUID, str]:
    with _cur(conn) as cur:
        cur.execute(
            "SELECT robot_id, robot_model FROM fleet.robots WHERE robot_id = ANY(%s::uuid[])",
            (robot_ids,),
        )
        return dict(cur.fetchall())


def load_firmware(conn: Connection) -> set[tuple[str, str]]:
    with _cur(conn) as cur:
        cur.execute("SELECT model_id, version FROM fleet.firmware_versions")
        return {(m, v) for m, v in cur.fetchall()}


# ------------------------------------------------------------------ latest state


def lock_states(conn: Connection, robot_ids: list[UUID]) -> dict[UUID, StoredPosition]:
    """Existing state rows for these robots, locked in a deterministic order (by
    robot_id) so two writers locking overlapping sets cannot deadlock."""
    with _cur(conn) as cur:
        cur.execute(
            """
            SELECT robot_id, boot_number, sequence_number, event_id
            FROM fleet.robot_latest_state WHERE robot_id = ANY(%s::uuid[])
            ORDER BY robot_id FOR UPDATE
            """,
            (sorted(robot_ids),),
        )
        return {r: StoredPosition(OrderingKey(b, s), e) for r, b, s, e in cur.fetchall()}


@dataclass(frozen=True)
class StateRow:
    obs: Observation
    applied: int  # added to state_version


_UPSERT_COLUMNS = (
    "robot_id, boot_id, boot_number, sequence_number, event_id, schema_version,"
    " observed_at, received_at, battery_pct, x_m, y_m, state_version"
)
_UPSERT_TAIL = """
    ON CONFLICT (robot_id) DO UPDATE SET
        boot_id = EXCLUDED.boot_id, boot_number = EXCLUDED.boot_number,
        sequence_number = EXCLUDED.sequence_number, event_id = EXCLUDED.event_id,
        schema_version = EXCLUDED.schema_version, observed_at = EXCLUDED.observed_at,
        received_at = EXCLUDED.received_at, battery_pct = EXCLUDED.battery_pct,
        x_m = EXCLUDED.x_m, y_m = EXCLUDED.y_m,
        state_version = s.state_version + EXCLUDED.state_version, updated_at = now()
    -- Defense in depth: even though the plan was computed under row locks, a robot
    -- with no row at lock time could have been inserted concurrently (e.g. by the
    -- legacy HTTP path). The database still refuses to move state backwards.
    WHERE (EXCLUDED.boot_number, EXCLUDED.sequence_number) > (s.boot_number, s.sequence_number)
    RETURNING robot_id
"""


def _state_columns(rows: list[StateRow], received_at: datetime) -> list[list[Any]]:
    o = [r.obs for r in rows]
    return [
        [x.robot_id for x in o],
        [x.boot_id for x in o],
        [x.boot_number for x in o],
        [x.sequence_number for x in o],
        [x.event_id for x in o],
        [x.schema_version for x in o],
        [x.observed_at for x in o],
        [received_at] * len(o),
        [x.battery_pct for x in o],
        [x.x_m for x in o],
        [x.y_m for x in o],
        [r.applied for r in rows],
    ]


def upsert_states(conn: Connection, rows: list[StateRow], received_at: datetime) -> set[UUID]:
    """Multi-row conditional upsert via unnest(). Returns robot_ids actually written."""
    if not rows:
        return set()
    with _cur(conn) as cur:
        cur.execute(
            f"""
            INSERT INTO fleet.robot_latest_state AS s ({_UPSERT_COLUMNS})
            SELECT * FROM unnest(%s::uuid[], %s::uuid[], %s::int8[], %s::int8[], %s::uuid[],
                                 %s::int4[], %s::timestamptz[], %s::timestamptz[],
                                 %s::float8[], %s::float8[], %s::float8[], %s::int8[])
            {_UPSERT_TAIL}
            """,
            _state_columns(rows, received_at),
        )
        return {r[0] for r in cur.fetchall()}


def upsert_states_copy(conn: Connection, rows: list[StateRow], received_at: datetime) -> set[UUID]:
    """Same statement, but rows are streamed with COPY into a temp staging table first.
    Kept for the benchmark comparison with unnest()."""
    if not rows:
        return set()
    with _cur(conn) as cur:
        cur.execute(
            "CREATE TEMP TABLE IF NOT EXISTS stage_state"
            " (LIKE fleet.robot_latest_state INCLUDING DEFAULTS) ON COMMIT DELETE ROWS"
        )
        cols = _state_columns(rows, received_at)
        with cur.copy(f"COPY stage_state ({_UPSERT_COLUMNS}) FROM STDIN") as cp:
            for row in zip(*cols, strict=True):
                cp.write_row(row)
        cur.execute(
            f"""
            INSERT INTO fleet.robot_latest_state AS s ({_UPSERT_COLUMNS})
            SELECT {_UPSERT_COLUMNS} FROM stage_state
            {_UPSERT_TAIL}
            """
        )
        return {r[0] for r in cur.fetchall()}


# ------------------------------------------------------------------ rejections


@dataclass(frozen=True)
class RejectionRow:
    dedup_key: str
    event_id: UUID | None
    robot_id: UUID | None
    reason: ReasonCode
    errors: list[dict[str, Any]]
    payload: Any
    payload_bytes: bytes | None
    source: str
    topic: str | None
    partition: int | None
    offset: int | None


def insert_rejections(
    conn: Connection, rows: list[RejectionRow], received_at: datetime, consumer_version: str
) -> None:
    if not rows:
        return
    with _cur(conn) as cur:
        cur.execute(
            """
            INSERT INTO fleet.telemetry_rejections
                (dedup_key, event_id, robot_id, reason_code, errors, payload, payload_bytes,
                 received_at, source, source_topic, source_partition, source_offset,
                 error_category, retry_eligible, status, consumer_version)
            SELECT d, e, r, rc, er::jsonb, pl::jsonb, pb, %s, src, t, p, o, cat, ret, st, %s
            FROM unnest(%s::text[], %s::uuid[], %s::uuid[], %s::text[], %s::text[], %s::text[],
                        %s::bytea[], %s::text[], %s::text[], %s::int[], %s::int8[],
                        %s::text[], %s::bool[], %s::text[])
                 AS u(d, e, r, rc, er, pl, pb, src, t, p, o, cat, ret, st)
            ON CONFLICT (dedup_key, reason_code) DO NOTHING
            """,
            (
                received_at,
                consumer_version,
                [x.dedup_key for x in rows],
                [x.event_id for x in rows],
                [x.robot_id for x in rows],
                [x.reason.value for x in rows],
                [json.dumps(x.errors) for x in rows],
                [json.dumps(x.payload) for x in rows],
                [x.payload_bytes for x in rows],
                [x.source for x in rows],
                [x.topic for x in rows],
                [x.partition for x in rows],
                [x.offset for x in rows],
                [category_of(x.reason).value for x in rows],
                [retry_eligible(x.reason) for x in rows],
                [initial_status(x.reason) for x in rows],
            ),
        )
