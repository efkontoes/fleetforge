"""Least privilege, tested: each role can do its job and nothing outside it.
Uses SET ROLE from the test superuser, so privilege checks apply exactly as for
a login with that role."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

ALLOWED = {
    "fleetforge_app": [
        "SELECT count(*) FROM fleet.missions",
        "INSERT INTO fleet.robots (robot_id, robot_model, firmware_version)"
        " VALUES (gen_random_uuid(), 'AMR-100', '1.0.0')",
        "SELECT 1 FROM fleet.robots FOR NO KEY UPDATE",  # boot registration lock (0006)
    ],
    "fleetforge_projector": [
        "SELECT count(*) FROM fleet.robot_latest_state",
        "INSERT INTO fleet.consumer_offsets VALUES ('g', 'telemetry.raw.v1', 0, 1)",
        "SELECT count(*) FROM fleet.telemetry_quarantine",
    ],
    "fleetforge_mission_consumer": [
        "SELECT count(*) FROM downstream.mission_status",
        "INSERT INTO fleet.consumer_offsets VALUES ('g', 'fleet.mission-events.v1', 0, 1)",
        "INSERT INTO downstream.mission_event_quarantine (source_topic, source_partition,"
        " source_offset, error) VALUES ('t', 0, 0, 'x')",
    ],
    "fleetforge_cdc": ["SELECT count(*) FROM fleet.outbox_events"],
    "fleetforge_readonly": [
        "SELECT count(*) FROM fleet.robot_latest_state",
        "SELECT count(*) FROM downstream.mission_status",
        "SELECT count(*) FROM pg_replication_slots",
    ],
}

DENIED = {
    "fleetforge_app": [
        "UPDATE fleet.consumer_offsets SET next_offset = 0",  # projector's ledger
        "DELETE FROM fleet.outbox_events",  # outbox is append-only for the API
        "UPDATE fleet.robots SET robot_model = 'X'",  # only lifecycle_status is updatable
    ],
    "fleetforge_projector": [
        "UPDATE fleet.missions SET status = 'FAILED'",  # missions belong to the API
        "INSERT INTO fleet.outbox_events (event_id, aggregate_type, aggregate_id, event_type,"
        " payload) VALUES (gen_random_uuid(), 'x', gen_random_uuid(), 'x', '{}')",
        "DELETE FROM fleet.robot_latest_state",
        "SELECT count(*) FROM fleet.missions",  # unused grant revoked in 0008
    ],
    "fleetforge_mission_consumer": ["SELECT count(*) FROM fleet.robot_latest_state"],
    "fleetforge_cdc": [
        "SELECT count(*) FROM fleet.robots",
        "INSERT INTO fleet.outbox_events (event_id, aggregate_type, aggregate_id, event_type,"
        " payload) VALUES (gen_random_uuid(), 'x', gen_random_uuid(), 'x', '{}')",
    ],
    "fleetforge_readonly": [
        "INSERT INTO fleet.consumer_offsets VALUES ('g', 't', 0, 1)",
        "UPDATE fleet.robot_latest_state SET battery_pct = 1",
    ],
}


def _as(db, role, sql):
    with db.connect() as c:
        tx = c.begin()
        try:
            c.execute(text(f"SET LOCAL ROLE {role}"))
            c.execute(text(sql))
        finally:
            tx.rollback()


@pytest.mark.parametrize(("role", "sql"), [(r, s) for r, ss in ALLOWED.items() for s in ss])
def test_role_can_do_its_job(db, role, sql):
    _as(db, role, sql)


@pytest.mark.parametrize(("role", "sql"), [(r, s) for r, ss in DENIED.items() for s in ss])
def test_role_cannot_exceed_its_job(db, role, sql):
    with pytest.raises(ProgrammingError, match="permission denied"):
        _as(db, role, sql)


def test_only_cdc_role_can_replicate_and_roles_have_connection_limits(db):
    with db.connect() as c:
        sql = text(
            "SELECT rolname, rolreplication, rolconnlimit, rolsuper"
            " FROM pg_roles WHERE rolname LIKE 'fleetforge\\_%'"
        )
        rows = c.execute(sql).all()
    by = {r.rolname: r for r in rows}
    assert {n for n, r in by.items() if r.rolreplication} == {"fleetforge_cdc"}
    assert all(r.rolconnlimit > 0 and not r.rolsuper for r in by.values())


@pytest.mark.parametrize(("role", "foreign_topic"), [
    ("fleetforge_projector", "fleet.mission-events.v1"),
    ("fleetforge_mission_consumer", "telemetry.raw.v1"),
])  # fmt: skip
def test_consumers_cannot_touch_each_others_offset_ledger(db, role, foreign_topic):
    """Row-level security (0008): a compromised or buggy consumer cannot move another
    consumer's durable position, and cannot even see it."""
    with db.begin() as c:
        c.execute(text("INSERT INTO fleet.consumer_offsets VALUES ('other', :t, 0, 500)"),
                  {"t": foreign_topic})  # fmt: skip
    with db.connect() as c:
        tx = c.begin()
        c.execute(text(f"SET LOCAL ROLE {role}"))
        assert c.execute(text("SELECT count(*) FROM fleet.consumer_offsets")).scalar_one() == 0
        upd = c.execute(text("UPDATE fleet.consumer_offsets SET next_offset = 0"))
        assert upd.rowcount == 0
        tx.rollback()
    with pytest.raises(ProgrammingError, match="row-level security"):
        _as(db, role, f"INSERT INTO fleet.consumer_offsets VALUES ('x', '{foreign_topic}', 0, 1)")
    with db.connect() as c:
        assert c.execute(text("SELECT next_offset FROM fleet.consumer_offsets WHERE"
                              " consumer_group = 'other'")).scalar_one() == 500  # fmt: skip
