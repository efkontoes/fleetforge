"""Milestone 1 acceptance scenarios 1, 2, 3 and 5, plus boot-session rules,
through the real HTTP API into a real PostgreSQL."""

import json
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError


def rejections(db, **where):
    clause = " AND ".join(f"{k} = :{k}" for k in where) or "true"
    with db.connect() as conn:
        return [
            dict(r)
            for r in conn.execute(
                text(
                    f"SELECT * FROM fleet.telemetry_rejections WHERE {clause} ORDER BY rejection_id"
                ),
                where,
            ).mappings()
        ]


# ------------------------------------------------------------- acceptance 1: normal


def test_normal_operation_latest_observation_wins(fleet):
    robot = fleet.robot()
    boot = fleet.boot(robot)
    for seq in (100, 101, 102):
        r = fleet.send(fleet.event(robot, boot, seq))
        assert r.status_code == 200
        assert r.json()["outcome"] == "APPLIED"

    state = fleet.state(robot)
    assert state["sequence_number"] == 102
    assert state["x_m"] == 102.0
    assert state["state_version"] == 3
    assert state["is_fresh"] is True


# ------------------------------------------------------------- acceptance 2: duplicate


def test_duplicate_delivery_creates_no_logical_update(fleet, db):
    robot = fleet.robot()
    boot = fleet.boot(robot)
    fleet.send(fleet.event(robot, boot, 100))
    ev102 = fleet.event(robot, boot, 102)
    assert fleet.send(ev102).json()["outcome"] == "APPLIED"
    before = fleet.state(robot)

    again = fleet.send(ev102)

    assert again.status_code == 200  # success, so a retrying producer stops retrying
    assert again.json()["outcome"] == "DUPLICATE"
    after = fleet.state(robot)
    assert after["state_version"] == before["state_version"]  # no new logical version
    assert after == {**before, "age_s": after["age_s"], "is_fresh": after["is_fresh"]}
    assert rejections(db) == []  # a duplicate is not a rejection


# ------------------------------------------------------------- acceptance 3: stale


def test_out_of_order_delivery_leaves_state_unchanged(fleet, db):
    robot = fleet.robot()
    boot = fleet.boot(robot)
    fleet.send(fleet.event(robot, boot, 100))
    fleet.send(fleet.event(robot, boot, 102))
    ev101 = fleet.event(robot, boot, 101)

    r = fleet.send(ev101)

    assert r.status_code == 200
    body = r.json()
    assert body["outcome"] == "STALE"
    assert body["reason_code"] == "STALE_SEQUENCE"
    assert body["errors"][0]["held"] == {"boot_number": 1, "sequence_number": 102}
    assert fleet.state(robot)["sequence_number"] == 102

    [audit] = rejections(db, reason_code="STALE_SEQUENCE")
    assert str(audit["event_id"]) == ev101["event_id"]
    assert audit["payload"] == ev101  # original payload preserved verbatim


def test_same_sequence_different_event_is_a_conflict(fleet):
    robot = fleet.robot()
    boot = fleet.boot(robot)
    fleet.send(fleet.event(robot, boot, 102))

    r = fleet.send(fleet.event(robot, boot, 102))  # new event_id, same position

    assert r.status_code == 409
    assert r.json()["reason_code"] == "SEQUENCE_CONFLICT"


# ------------------------------------------------------------- acceptance 5: invalid


def test_battery_150_rejected_with_machine_readable_reason(fleet, db):
    robot = fleet.robot()
    boot = fleet.boot(robot)
    ev = fleet.event(robot, boot, 100, battery_pct=150.0)

    r = fleet.send(ev)

    assert r.status_code == 422
    assert r.json() == {
        "outcome": "REJECTED",
        "event_id": ev["event_id"],
        "reason_code": "BATTERY_OUT_OF_RANGE",
        "errors": [
            {
                "code": "BATTERY_OUT_OF_RANGE",
                "field": "battery_pct",
                "message": "battery_pct=150.0 outside [0, 100]",
            }
        ],
        "state_version": None,
    }
    assert fleet.c.get(f"/v1/robots/{robot}/state").status_code == 404  # nothing applied
    [audit] = rejections(db)
    assert audit["reason_code"] == "BATTERY_OUT_OF_RANGE"
    assert audit["payload"] == ev


def test_missing_field_rejected_and_audited_with_identity(fleet, db):
    robot = fleet.robot()
    boot = fleet.boot(robot)
    ev = fleet.event(robot, boot, 100)
    del ev["battery_pct"]

    r = fleet.send(ev)

    assert r.status_code == 422
    assert r.json()["reason_code"] == "MISSING_FIELD"
    [audit] = rejections(db)
    # Even an invalid event is traceable to its robot when the identity fields parse.
    assert str(audit["robot_id"]) == str(robot)
    assert str(audit["event_id"]) == ev["event_id"]


def test_malformed_body_is_audited_as_text(fleet, db):
    r = fleet.c.post(
        "/v1/telemetry", content=b"{not json", headers={"content-type": "application/json"}
    )
    assert r.status_code == 422
    assert r.json()["reason_code"] == "MALFORMED_JSON"
    [audit] = rejections(db)
    assert audit["payload"] == "{not json"
    assert audit["dedup_key"].startswith("sha256:")


def test_rejection_audit_is_idempotent_under_retries(fleet, db):
    robot = fleet.robot()
    boot = fleet.boot(robot)
    ev = fleet.event(robot, boot, 100, battery_pct=-5.0)
    for _ in range(3):
        assert fleet.send(ev).status_code == 422
    garbage = b'{"schema_version": 1'
    for _ in range(3):
        fleet.c.post("/v1/telemetry", content=garbage)
    assert len(rejections(db)) == 2


def test_database_constraint_backs_up_application_validation(fleet, db):
    """Requirement 4: even if a code path skipped domain validation, the DB refuses."""
    robot = fleet.robot()
    boot = fleet.boot(robot)
    fleet.send(fleet.event(robot, boot, 1))
    with (
        pytest.raises(IntegrityError, match="robot_latest_state_battery_pct_check"),
        db.begin() as conn,
    ):
        conn.execute(
            text("UPDATE fleet.robot_latest_state SET battery_pct = 150 WHERE robot_id = :r"),
            {"r": robot},
        )


# ------------------------------------------------------------- referential + boots


def test_unknown_robot_and_unknown_boot(fleet):
    r = fleet.send(fleet.event(uuid4(), uuid4(), 1))
    assert (r.status_code, r.json()["reason_code"]) == (422, "UNKNOWN_ROBOT")

    robot = fleet.robot()
    r = fleet.send(fleet.event(robot, uuid4(), 1))
    assert (r.status_code, r.json()["reason_code"]) == (422, "UNKNOWN_BOOT")


def test_new_boot_supersedes_old_boot_despite_sequence_reset(fleet):
    robot = fleet.robot()
    boot1 = fleet.boot(robot)
    fleet.send(fleet.event(robot, boot1, 90_000))
    boot2 = fleet.boot(robot)

    assert fleet.send(fleet.event(robot, boot2, 0)).json()["outcome"] == "APPLIED"
    state = fleet.state(robot)
    assert (state["boot_number"], state["sequence_number"]) == (2, 0)

    # A delayed message from the previous boot is stale, however high its sequence.
    late = fleet.send(fleet.event(robot, boot1, 90_001))
    assert late.json()["outcome"] == "STALE"
    assert fleet.state(robot)["boot_id"] == str(boot2)


def test_boot_registration_is_idempotent_and_ordered(fleet):
    robot = fleet.robot()
    boot_id = uuid4()
    first = fleet.c.post(f"/v1/robots/{robot}/boots", json={"boot_id": str(boot_id)})
    retry = fleet.c.post(f"/v1/robots/{robot}/boots", json={"boot_id": str(boot_id)})
    assert (first.status_code, retry.status_code) == (201, 200)
    assert first.json()["boot_number"] == retry.json()["boot_number"] == 1

    other_robot = fleet.robot()
    stolen = fleet.c.post(f"/v1/robots/{other_robot}/boots", json={"boot_id": str(boot_id)})
    assert stolen.status_code == 409


def test_robot_registration_idempotency_and_firmware_contract(fleet, client):
    robot_id = str(uuid4())
    body = {"robot_id": robot_id, "robot_model": "AMR-100", "firmware_version": "1.0.0"}
    assert client.post("/v1/robots", json=body).status_code == 201
    assert client.post("/v1/robots", json=body).status_code == 200
    changed = {**body, "firmware_version": "1.1.0"}
    assert client.post("/v1/robots", json=changed).status_code == 409
    unknown_fw = {**body, "robot_id": str(uuid4()), "firmware_version": "9.9.9"}
    r = client.post("/v1/robots", json=unknown_fw)
    assert (r.status_code, r.json()["error"]["code"]) == (422, "UNKNOWN_FIRMWARE")


def test_rejection_payload_matches_bytes_sent(fleet, db):
    robot = fleet.robot()
    boot = fleet.boot(robot)
    ev = fleet.event(robot, boot, 5, battery_pct=101.5)
    fleet.c.post("/v1/telemetry", content=json.dumps(ev).encode())
    [audit] = rejections(db)
    assert audit["payload"] == ev
