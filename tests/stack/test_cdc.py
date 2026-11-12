"""Debezium CDC on the real stack: outbox events, table-level CDC, recovery."""

from __future__ import annotations

import json
import time
import uuid

import httpx
import pytest
from confluent_kafka import Consumer
from sqlalchemy import text

from fleetforge.projector.missions import apply_events

from .conftest import API, BOOTSTRAP, CONNECT, compose, wait_until

MISSION_TOPIC = "fleet.mission-events.v1"
REFERENCE_TOPIC = "fleetforge.fleet.firmware_versions"


def api() -> httpx.Client:
    return httpx.Client(base_url=API, timeout=15)


def new_robot(c: httpx.Client) -> str:
    rid = str(uuid.uuid4())
    c.post("/v1/robots", json={"robot_id": rid, "robot_model": "AMR-100",
                               "firmware_version": "1.0.0"}).raise_for_status()  # fmt: skip
    return rid


def lifecycle(c: httpx.Client, robot: str, statuses=("ASSIGNED", "RUNNING", "COMPLETED")) -> str:
    mid = str(uuid.uuid4())
    c.post("/v1/missions", json={"mission_id": mid, "robot_id": robot}).raise_for_status()
    for v, s in enumerate(statuses):
        c.post(f"/v1/missions/{mid}/transitions", json={"transition_id": str(uuid.uuid4()),
               "to_status": s, "expected_version": v}).raise_for_status()  # fmt: skip
    return mid


def read_topic(topic: str, key: str | None, want: int, timeout: float = 60) -> list:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"probe-{uuid.uuid4()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})  # fmt: skip
    c.subscribe([topic])
    out, deadline = [], time.monotonic() + timeout
    try:
        while time.monotonic() < deadline and len(out) < want:
            m = c.poll(0.5)
            if m is None or m.error():
                continue
            if key is None or (m.key() and key in m.key().decode()):
                out.append(m)
    finally:
        c.close()
    return out


def downstream(db, mid):
    with db.connect() as c:
        return (
            c.execute(
                text(
                    "SELECT status, version, events_seen FROM downstream.mission_status"
                    " WHERE mission_id = :m"
                ),
                {"m": mid},
            )
            .mappings()
            .first()
        )


def connector_running(name: str) -> bool:
    try:
        s = httpx.get(f"{CONNECT}/connectors/{name}/status", timeout=3).json()
        return s["connector"]["state"] == "RUNNING" and all(
            t["state"] == "RUNNING" for t in s["tasks"]
        )
    except Exception:
        return False


def test_mission_lifecycle_flows_through_outbox_cdc_to_independent_consumer(stack_db):
    c = api()
    robot = new_robot(c)
    mid = lifecycle(c, robot)

    msgs = read_topic(MISSION_TOPIC, mid, 4)
    events = [(dict(m.headers())["eventType"].decode(), json.loads(m.value())) for m in msgs]
    assert [(t, e["to_status"], e["version"]) for t, e in events] == [
        ("MissionCreated", "PENDING", 0),
        ("MissionStatusChanged", "ASSIGNED", 1),
        ("MissionStatusChanged", "RUNNING", 2),
        ("MissionStatusChanged", "COMPLETED", 3),
    ]
    assert len({m.partition() for m in msgs}) == 1  # keyed by mission id: one ordered stream
    # Each Kafka message is a committed outbox row, same id.
    with stack_db.connect() as conn:
        ids = {
            str(r.event_id)
            for r in conn.execute(
                text("SELECT event_id FROM fleet.outbox_events WHERE aggregate_id = :m"), {"m": mid}
            )
        }
    assert {e["event_id"] for _, e in events} == ids

    wait_until(lambda: (downstream(stack_db, mid) or {}).get("version") == 3, 30,
               msg="mission consumer applied all versions")  # fmt: skip
    assert dict(downstream(stack_db, mid)) == {
        "status": "COMPLETED",
        "version": 3,
        "events_seen": 4,
    }


def test_rolled_back_change_never_reaches_kafka(stack_db):
    """A transition rejected by the database (robot busy) leaves no outbox row and
    therefore no CDC event: only committed transactions are captured."""
    c = api()
    robot = new_robot(c)
    m1 = lifecycle(c, robot, ("ASSIGNED",))
    m2 = str(uuid.uuid4())
    c.post("/v1/missions", json={"mission_id": m2, "robot_id": robot}).raise_for_status()
    r = c.post(f"/v1/missions/{m2}/transitions", json={"transition_id": str(uuid.uuid4()),
               "to_status": "ASSIGNED", "expected_version": 0})  # fmt: skip
    assert r.status_code == 409
    msgs = read_topic(MISSION_TOPIC, m2, 2, timeout=10)
    assert [json.loads(m.value())["to_status"] for m in msgs] == ["PENDING"]
    assert m1


def test_table_level_cdc_insert_update_delete(stack_db):
    version = f"9.{uuid.uuid4().int % 10**6}"
    with stack_db.begin() as c:
        c.execute(
            text(
                "INSERT INTO fleet.firmware_versions (model_id, version, battery_unit,"
                " position_unit) VALUES ('CDC-TEST', :v, 'PERCENT', 'M')"
            ),
            {"v": version},
        )
    with stack_db.begin() as c:
        c.execute(
            text(
                "UPDATE fleet.firmware_versions SET position_unit = 'MM'"
                " WHERE model_id = 'CDC-TEST' AND version = :v"
            ),
            {"v": version},
        )
    with stack_db.begin() as c:
        c.execute(
            text(
                "DELETE FROM fleet.firmware_versions WHERE model_id = 'CDC-TEST' AND version = :v"
            ),
            {"v": version},
        )

    msgs = read_topic(REFERENCE_TOPIC, version, 4)
    ops = [json.loads(m.value())["op"] if m.value() else "tombstone" for m in msgs]
    assert ops == ["c", "u", "d", "tombstone"]
    upd = json.loads(msgs[1].value())
    assert (upd["before"]["position_unit"], upd["after"]["position_unit"]) == ("M", "MM")
    dele = json.loads(msgs[2].value())
    assert dele["after"] is None and dele["before"]["version"] == version  # REPLICA IDENTITY FULL


def test_outbox_cleanup_is_invisible_to_cdc(stack_db):
    """Retention deletes on the outbox must not reach consumers: the outbox publication
    publishes INSERTs only."""
    c = api()
    mid = lifecycle(c, new_robot(c), ("ASSIGNED",))
    assert len(read_topic(MISSION_TOPIC, mid, 2)) == 2
    with stack_db.begin() as conn:
        n = conn.execute(text("DELETE FROM fleet.outbox_events WHERE aggregate_id = :m"),
                         {"m": mid}).rowcount  # fmt: skip
    assert n == 2
    # A later business event for another mission proves the connector is still streaming.
    probe = lifecycle(c, new_robot(c), ())
    assert len(read_topic(MISSION_TOPIC, probe, 1)) == 1
    assert len(read_topic(MISSION_TOPIC, mid, 3, timeout=5)) == 2  # no delete event appeared


def test_repeated_delivery_is_absorbed_by_version_guard(stack_db):
    c = api()
    mid = lifecycle(c, new_robot(c))
    msgs = read_topic(MISSION_TOPIC, mid, 4)
    wait_until(lambda: (downstream(stack_db, mid) or {}).get("version") == 3, 30)
    raw = [(m.topic(), m.partition(), m.offset(), m.value()) for m in msgs]
    # Deliver everything again, twice, out of order, under a group with no ledger history.
    group = f"redelivery-{uuid.uuid4().hex[:8]}"
    out = apply_events(stack_db, list(reversed(raw)) + raw, group)
    assert out["applied"] == 0 and out["ignored"] == 8
    assert dict(downstream(stack_db, mid)) == {
        "status": "COMPLETED",
        "version": 3,
        "events_seen": 4,
    }


@pytest.mark.disruptive
def test_debezium_restart_resumes_from_committed_offset(stack_db):
    c = api()
    compose("stop", "connect")
    try:
        mid = lifecycle(c, new_robot(c))  # committed while CDC is down: only in the WAL
        with stack_db.connect() as conn:
            retained = conn.execute(
                text(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM pg_replication_slots"
                    " WHERE slot_name = 'fleetforge_outbox'"
                )
            ).scalar_one()
        assert retained > 0  # the slot holds the WAL for us
    finally:
        compose("start", "connect")
    started = time.monotonic()
    wait_until(lambda: connector_running("fleetforge-outbox"), 120, msg="connector running")
    msgs = read_topic(MISSION_TOPIC, mid, 4, timeout=90)
    print(f"\nconnector back and caught up in {time.monotonic() - started:.1f}s")
    assert [json.loads(m.value())["version"] for m in msgs] == [0, 1, 2, 3]
    wait_until(lambda: (downstream(stack_db, mid) or {}).get("version") == 3, 60)


@pytest.mark.disruptive
def test_postgres_restart_cdc_recovers(stack_db):
    compose("restart", "postgres")
    wait_until(lambda: _db_ok(stack_db), 60, msg="postgres back")
    c = api()
    wait_until(lambda: _api_ready(c), 60, msg="api ready")
    mid = lifecycle(c, new_robot(c))
    msgs = read_topic(MISSION_TOPIC, mid, 4, timeout=120)
    assert [json.loads(m.value())["version"] for m in msgs] == [0, 1, 2, 3]
    assert connector_running("fleetforge-outbox")


def _db_ok(db) -> bool:
    try:
        with db.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def _api_ready(c) -> bool:
    try:
        return c.get("/readyz").status_code == 200
    except Exception:
        return False
