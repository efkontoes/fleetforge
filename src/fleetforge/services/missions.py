"""Mission lifecycle with a transactional outbox.

Every state change commits three rows in ONE transaction:
    missions (UPDATE)  +  mission_events (audit)  +  outbox_events (to be published)
If anything fails before COMMIT, PostgreSQL discards all three. There is no state
in which the mission changed but no event exists, or an event exists for a change
that never happened. That is the whole point of the outbox: we never "dual-write"
to the database and a broker.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from psycopg import errors as pg_errors
from sqlalchemy import Connection, Engine, RowMapping
from sqlalchemy.exc import IntegrityError

from fleetforge.contracts.api import MissionCreate, MissionStatus, MissionTransition
from fleetforge.db import repository as repo
from fleetforge.domain.missions import INITIAL_STATUS, InvalidTransition, check_transition
from fleetforge.observability import MISSION_TRANSITIONS
from fleetforge.services.errors import Conflict, NotFound, Unprocessable


@dataclass(frozen=True)
class TransitionResult:
    mission_id: UUID
    transition_id: UUID
    from_status: MissionStatus | None
    to_status: MissionStatus
    version: int
    replayed: bool  # True when this was a retry of an already-committed transition


def create_mission(engine: Engine, req: MissionCreate) -> tuple[RowMapping, bool]:
    """Idempotent on mission_id. Returns (mission, created)."""
    with engine.begin() as conn:
        if repo.get_robot(conn, req.robot_id) is None:
            raise Unprocessable("UNKNOWN_ROBOT", f"robot {req.robot_id} is not registered")
        row = repo.insert_mission(conn, req.mission_id, req.robot_id, INITIAL_STATUS.value)
        if row is None:
            existing = repo.get_mission(conn, req.mission_id)
            if existing.robot_id != req.robot_id:
                raise Conflict("MISSION_ID_CONFLICT", "mission_id exists for a different robot")
            return existing, False
        _append_event(conn, row, from_status=None, event_id=uuid4(), event_type="MissionCreated")
        return row, True


def get_mission(engine: Engine, mission_id: UUID) -> RowMapping:
    with engine.connect() as conn:
        row = repo.get_mission(conn, mission_id)
    if row is None:
        raise NotFound("UNKNOWN_MISSION", f"mission {mission_id} does not exist")
    return row


def transition_mission(
    engine: Engine, mission_id: UUID, req: MissionTransition
) -> TransitionResult:
    with engine.begin() as conn:
        if (replay := _replay(conn, mission_id, req)) is not None:
            return replay

        mission = repo.get_mission(conn, mission_id)
        if mission is None:
            raise NotFound("UNKNOWN_MISSION", f"mission {mission_id} does not exist")
        if mission.version != req.expected_version:
            return _replay_or_conflict(conn, mission_id, req)
        from_status = MissionStatus(mission.status)
        try:
            check_transition(from_status, req.to_status)
        except InvalidTransition as exc:
            raise Unprocessable("INVALID_TRANSITION", str(exc)) from exc

        try:
            updated = repo.update_mission_status(
                conn, mission_id, req.to_status.value, req.expected_version
            )
        except IntegrityError as exc:
            if _violated(exc, "uq_missions_one_active_per_robot"):
                raise Conflict(
                    "ROBOT_BUSY", f"robot {mission.robot_id} already has an active mission"
                ) from exc
            raise

        if updated is None:
            # Lost the race between our read and our UPDATE.
            return _replay_or_conflict(conn, mission_id, req)

        _append_event(
            conn,
            updated,
            from_status=from_status,
            event_id=req.transition_id,
            event_type="MissionStatusChanged",
        )

    MISSION_TRANSITIONS.labels(req.to_status.value).inc()
    return TransitionResult(
        mission_id, req.transition_id, from_status, req.to_status, updated.version, replayed=False
    )


def _append_event(
    conn: Connection,
    mission: RowMapping,
    *,
    from_status: MissionStatus | None,
    event_id: UUID,
    event_type: str,
) -> None:
    """Audit row + outbox row, inside the caller's transaction."""
    repo.insert_mission_event(
        conn,
        event_id=event_id,
        mission_id=mission.mission_id,
        from_status=from_status.value if from_status else None,
        to_status=mission.status,
        mission_version=mission.version,
    )
    repo.insert_outbox_event(
        conn,
        event_id=event_id,
        aggregate_type="mission",
        aggregate_id=mission.mission_id,
        event_type=event_type,
        payload={
            "event_id": str(event_id),
            "mission_id": str(mission.mission_id),
            "robot_id": str(mission.robot_id),
            "from_status": from_status.value if from_status else None,
            "to_status": mission.status,
            # Consumers use version to discard stale or duplicated deliveries.
            "version": mission.version,
            "occurred_at": mission.updated_at.isoformat(),
        },
    )


def _replay(conn: Connection, mission_id: UUID, req: MissionTransition) -> TransitionResult | None:
    prior = repo.get_mission_event(conn, req.transition_id)
    if prior is None:
        return None
    if prior.mission_id != mission_id or prior.to_status != req.to_status.value:
        raise Conflict(
            "IDEMPOTENCY_KEY_REUSED",
            "transition_id was already used for a different transition",
        )
    return TransitionResult(
        mission_id,
        req.transition_id,
        MissionStatus(prior.from_status) if prior.from_status else None,
        MissionStatus(prior.to_status),
        prior.mission_version,
        replayed=True,
    )


def _replay_or_conflict(
    conn: Connection, mission_id: UUID, req: MissionTransition
) -> TransitionResult:
    """The mission is not at expected_version. Before calling that a conflict, check
    whether it moved because a concurrent retry of THIS request committed after our
    first idempotency check. (READ COMMITTED: each statement sees the latest commits.)
    Without this re-check, a retry racing its original gets a spurious 409 although
    the transition it asked for did happen. Found by
    test_concurrent_retries_of_one_transition_commit_once."""
    if (replay := _replay(conn, mission_id, req)) is not None:
        return replay
    current = repo.get_mission(conn, mission_id)
    raise _version_conflict(current.version, req.expected_version)


def _version_conflict(current: int, expected: int) -> Conflict:
    return Conflict(
        "VERSION_CONFLICT",
        f"mission is at version {current}, request expected {expected}",
        current_version=current,
    )


def _violated(exc: IntegrityError, constraint: str) -> bool:
    orig = exc.orig
    return isinstance(orig, pg_errors.UniqueViolation) and orig.diag.constraint_name == constraint
