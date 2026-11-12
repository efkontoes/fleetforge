"""Acceptance scenario 6: a mission assignment fails midway; nothing commits.

The failure is injected *after* the mission UPDATE has executed inside the
transaction (we verify that from within the transaction) and *before* the outbox
row is written. If the code had committed the UPDATE separately (the dual-write
mistake), the mission would now be ASSIGNED with no event announcing it.
"""

from uuid import uuid4

import pytest
from sqlalchemy import text

from fleetforge.contracts.api import (
    MissionCreate,
    MissionStatus,
    MissionTransition,
    RobotRegistration,
)
from fleetforge.db import repository as repo
from fleetforge.services import missions, robots


class InjectedFailure(RuntimeError):
    pass


def snapshot(db, mission_id):
    with db.connect() as c:
        return {
            "mission": dict(
                c.execute(
                    text("SELECT status, version FROM fleet.missions WHERE mission_id = :m"),
                    {"m": mission_id},
                )
                .mappings()
                .one()
            ),
            "mission_events": c.execute(text("SELECT count(*) FROM fleet.mission_events")).scalar(),
            "outbox": c.execute(text("SELECT count(*) FROM fleet.outbox_events")).scalar(),
        }


@pytest.mark.parametrize("fail_at", ["insert_mission_event", "insert_outbox_event"])
def test_failure_midway_through_assignment_commits_nothing(db, monkeypatch, fail_at):
    robot_id, mission_id = uuid4(), uuid4()
    robots.register_robot(
        db, RobotRegistration(robot_id=robot_id, robot_model="AMR-100", firmware_version="1.0.0")
    )
    missions.create_mission(db, MissionCreate(mission_id=mission_id, robot_id=robot_id))
    before = snapshot(db, mission_id)
    assert before["mission"] == {"status": "PENDING", "version": 0}

    seen_inside_tx = {}

    def explode(conn, **kwargs):
        # Prove we really are "midway": the UPDATE is visible inside this transaction.
        seen_inside_tx["status"] = conn.execute(
            text("SELECT status FROM fleet.missions WHERE mission_id = :m"), {"m": mission_id}
        ).scalar_one()
        raise InjectedFailure(f"injected failure in {fail_at}")

    monkeypatch.setattr(repo, fail_at, explode)
    req = MissionTransition(
        transition_id=uuid4(), to_status=MissionStatus.ASSIGNED, expected_version=0
    )

    with pytest.raises(InjectedFailure):
        missions.transition_mission(db, mission_id, req)

    assert seen_inside_tx["status"] == "ASSIGNED"  # the change existed, uncommitted
    assert snapshot(db, mission_id) == before  # ...and was rolled back with everything else

    # Recovery: the client retries the same request after the fault is gone.
    monkeypatch.undo()
    result = missions.transition_mission(db, mission_id, req)
    assert (result.version, result.replayed) == (1, False)
    after = snapshot(db, mission_id)
    assert after["mission"] == {"status": "ASSIGNED", "version": 1}
    assert after["outbox"] == before["outbox"] + 1
