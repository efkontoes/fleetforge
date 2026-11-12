"""Mission lifecycle, idempotency keys, and outbox consistency through the API."""

from uuid import uuid4

from sqlalchemy import text


def outbox(db, aggregate_type="mission"):
    with db.connect() as c:
        return [
            dict(r)
            for r in c.execute(
                text(
                    "SELECT * FROM fleet.outbox_events"
                    " WHERE aggregate_type = :t ORDER BY created_at"
                ),
                {"t": aggregate_type},
            ).mappings()
        ]


def mission_events(db, mission_id):
    with db.connect() as c:
        return c.execute(
            text(
                "SELECT event_id, from_status, to_status, mission_version"
                " FROM fleet.mission_events WHERE mission_id = :m ORDER BY mission_version"
            ),
            {"m": mission_id},
        ).all()


def create(client, robot_id, mission_id=None):
    mission_id = mission_id or uuid4()
    r = client.post("/v1/missions", json={"mission_id": str(mission_id), "robot_id": str(robot_id)})
    return mission_id, r


def transition(client, mission_id, to_status, expected_version, transition_id=None):
    transition_id = transition_id or uuid4()
    r = client.post(
        f"/v1/missions/{mission_id}/transitions",
        json={
            "transition_id": str(transition_id),
            "to_status": to_status,
            "expected_version": expected_version,
        },
    )
    return transition_id, r


def test_full_lifecycle_writes_one_audit_row_and_one_outbox_row_per_version(fleet, client, db):
    robot = fleet.robot()
    mission_id, r = create(client, robot)
    assert r.status_code == 201
    assert r.json()["status"] == "PENDING" and r.json()["version"] == 0

    ids = []
    for version, status in enumerate(["ASSIGNED", "RUNNING", "COMPLETED"]):
        tid, r = transition(client, mission_id, status, version)
        assert r.status_code == 200, r.text
        assert r.json()["version"] == version + 1
        ids.append(tid)

    events = mission_events(db, mission_id)
    assert [(e.from_status, e.to_status, e.mission_version) for e in events] == [
        (None, "PENDING", 0),
        ("PENDING", "ASSIGNED", 1),
        ("ASSIGNED", "RUNNING", 2),
        ("RUNNING", "COMPLETED", 3),
    ]
    rows = outbox(db)
    assert [o["event_type"] for o in rows] == ["MissionCreated"] + ["MissionStatusChanged"] * 3
    # The outbox event IS the audit event: same id, so consumers can deduplicate.
    assert [o["event_id"] for o in rows] == [e.event_id for e in events]
    assert [o["event_id"] for o in rows[1:]] == ids
    assert rows[-1]["payload"]["version"] == 3
    assert rows[-1]["payload"]["to_status"] == "COMPLETED"


def test_create_is_idempotent_on_mission_id(fleet, client, db):
    robot = fleet.robot()
    mission_id, first = create(client, robot)
    _, retry = create(client, robot, mission_id)
    assert (first.status_code, retry.status_code) == (201, 200)
    assert len(outbox(db)) == 1

    _, other = create(client, fleet.robot(), mission_id)
    assert other.status_code == 409


def test_retried_transition_is_replayed_not_reapplied(fleet, client, db):
    robot = fleet.robot()
    mission_id, _ = create(client, robot)
    tid, first = transition(client, mission_id, "ASSIGNED", 0)
    _, retry = transition(client, mission_id, "ASSIGNED", 0, transition_id=tid)

    assert first.json()["replayed"] is False
    assert retry.status_code == 200
    assert retry.json()["replayed"] is True
    assert retry.json()["version"] == 1
    assert len(mission_events(db, mission_id)) == 2  # created + one assignment


def test_stale_expected_version_is_rejected(fleet, client):
    mission_id, _ = create(client, fleet.robot())
    transition(client, mission_id, "ASSIGNED", 0)
    _, r = transition(client, mission_id, "CANCELLED", 0)  # someone else already moved it
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "VERSION_CONFLICT"
    assert r.json()["error"]["current_version"] == 1


def test_invalid_transition_is_rejected_without_side_effects(fleet, client, db):
    mission_id, _ = create(client, fleet.robot())
    _, r = transition(client, mission_id, "COMPLETED", 0)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "INVALID_TRANSITION"
    assert len(outbox(db)) == 1


def test_robot_busy_is_enforced_by_the_database(fleet, client, db):
    robot = fleet.robot()
    m1, _ = create(client, robot)
    m2, _ = create(client, robot)
    transition(client, m1, "ASSIGNED", 0)

    _, r = transition(client, m2, "ASSIGNED", 0)

    assert r.status_code == 409
    assert r.json()["error"]["code"] == "ROBOT_BUSY"
    assert client.get(f"/v1/missions/{m2}").json()["status"] == "PENDING"
    assert len(mission_events(db, m2)) == 1  # nothing from the failed attempt
    # Once m1 finishes, m2 can be assigned.
    transition(client, m1, "CANCELLED", 1)
    assert transition(client, m2, "ASSIGNED", 0)[1].status_code == 200


def test_idempotency_key_reuse_for_different_transition_is_rejected(fleet, client):
    mission_id, _ = create(client, fleet.robot())
    tid, _ = transition(client, mission_id, "ASSIGNED", 0)
    _, r = transition(client, mission_id, "RUNNING", 1, transition_id=tid)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"


def test_robot_state_shows_active_mission(fleet, client):
    robot = fleet.robot()
    boot = fleet.boot(robot)
    fleet.send(fleet.event(robot, boot, 1))
    mission_id, _ = create(client, robot)
    assert fleet.state(robot)["active_mission_id"] is None
    transition(client, mission_id, "ASSIGNED", 0)
    state = fleet.state(robot)
    assert state["active_mission_id"] == str(mission_id)
    assert state["active_mission_status"] == "ASSIGNED"


def test_robot_registration_emits_outbox_event_once(fleet, client, db):
    robot_id = str(uuid4())
    body = {"robot_id": robot_id, "robot_model": "AMR-200", "firmware_version": "2.0.0"}
    client.post("/v1/robots", json=body)
    client.post("/v1/robots", json=body)
    [event] = outbox(db, "robot")
    assert event["event_type"] == "RobotRegistered"
    assert event["payload"]["robot_id"] == robot_id
