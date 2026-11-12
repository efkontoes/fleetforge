from uuid import UUID, uuid4

from sqlalchemy import Engine, RowMapping

from fleetforge.contracts.api import RobotRegistration
from fleetforge.db import repository as repo
from fleetforge.services.errors import Conflict, NotFound, Unprocessable


def register_robot(engine: Engine, req: RobotRegistration) -> tuple[RowMapping, bool]:
    """Idempotent on robot_id. Returns (robot, created)."""
    with engine.begin() as conn:
        if not repo.firmware_exists(conn, req.robot_model, req.firmware_version):
            raise Unprocessable(
                "UNKNOWN_FIRMWARE",
                f"no firmware contract for {req.robot_model} {req.firmware_version}",
            )
        row = repo.insert_robot(conn, req.robot_id, req.robot_model, req.firmware_version)
        if row is None:
            existing = repo.get_robot(conn, req.robot_id)
            same = (existing.robot_model, existing.firmware_version) == (
                req.robot_model,
                req.firmware_version,
            )
            if not same:
                raise Conflict(
                    "ROBOT_ID_CONFLICT", "robot_id already registered with other attributes"
                )
            return existing, False
        repo.insert_outbox_event(
            conn,
            event_id=uuid4(),
            aggregate_type="robot",
            aggregate_id=row.robot_id,
            event_type="RobotRegistered",
            payload={
                "robot_id": str(row.robot_id),
                "robot_model": row.robot_model,
                "firmware_version": row.firmware_version,
                "lifecycle_status": row.lifecycle_status,
                "registered_at": row.registered_at.isoformat(),
            },
        )
        return row, True


def register_boot(engine: Engine, robot_id: UUID, boot_id: UUID) -> tuple[int, bool]:
    """Assign the next boot_number for this robot. Idempotent on boot_id.

    Without the robot row lock, two concurrent registrations could both read
    MAX(boot_number)=3 and both try to insert 4; the unique constraint would turn
    one of them into an error instead of serializing them.
    """
    with engine.begin() as conn:
        if repo.get_robot(conn, robot_id, lock=True) is None:
            raise NotFound("UNKNOWN_ROBOT", f"robot {robot_id} is not registered")
        existing = repo.get_boot(conn, boot_id)
        if existing is not None:
            if existing.robot_id != robot_id:
                raise Conflict("BOOT_ID_CONFLICT", "boot_id is registered to a different robot")
            return existing.boot_number, False
        return repo.insert_next_boot(conn, robot_id, boot_id), True
