"""End-to-end: a fault-heavy simulated fleet through the real API into PostgreSQL,
checked against the oracle, robot by robot and outcome by outcome."""

from uuid import UUID

from sqlalchemy import text

from fleetforge.simulator.fleet import FaultConfig, SimConfig, generate
from fleetforge.simulator.oracle import expected
from fleetforge.simulator.sender import send

CFG = SimConfig(
    seed=2026,
    robots=15,
    ticks=60,
    faults=FaultConfig(
        duplicate_rate=0.08,
        reorder_rate=0.08,
        invalid_battery_rate=0.03,
        missing_field_rate=0.03,
        reboot_rate=0.02,
        max_delay_ticks=3,
    ),
)


def db_snapshot(db):
    with db.connect() as c:
        state = {
            r.robot_id: r
            for r in c.execute(
                text("""
                    SELECT robot_id, event_id, boot_id, sequence_number, battery_pct, x_m, y_m,
                           state_version
                    FROM fleet.robot_latest_state
                """)
            )
        }
        rejections = {
            (r.dedup_key, r.reason_code)
            for r in c.execute(
                text("SELECT dedup_key, reason_code FROM fleet.telemetry_rejections")
            )
        }
    return state, rejections


def run_once(client, db):
    items = generate(CFG)
    report = send(items, lambda: _NonClosing(client), workers=1)
    return items, report, db_snapshot(db)


class _NonClosing:
    """Lets the sender use the shared TestClient without closing it."""

    def __init__(self, client):
        self.client = client

    def __enter__(self):
        return self.client

    def __exit__(self, *exc):
        return False


def test_fault_injected_stream_matches_oracle(client, db):
    items, report, (state, rejections) = run_once(client, db)
    exp = expected(items)

    # Sanity: the scenario really exercised every path.
    kinds = {o for o, _ in exp.outcomes}
    assert {"APPLIED", "DUPLICATE", "STALE", "REJECTED"} <= kinds

    assert report.outcomes == exp.outcomes
    assert rejections == exp.rejection_rows
    assert set(state) == set(exp.latest)
    for robot_id, payload in exp.latest.items():
        row = state[robot_id]
        assert str(row.event_id) == payload["event_id"]
        assert str(row.boot_id) == payload["boot_id"]
        assert row.sequence_number == payload["sequence_number"]
        assert (row.battery_pct, row.x_m, row.y_m) == (
            payload["battery_pct"],
            payload["x_m"],
            payload["y_m"],
        )
        assert row.state_version == exp.state_versions[robot_id]


def test_same_seed_twice_produces_identical_database_state(client, db, engine):
    _, report1, (state1, rej1) = run_once(client, db)
    with engine.begin() as c:
        c.execute(
            text(
                "TRUNCATE fleet.robot_latest_state, fleet.telemetry_rejections, "
                "fleet.outbox_events, fleet.robot_boots, fleet.robots CASCADE"
            )
        )
    _, report2, (state2, rej2) = run_once(client, db)

    def logical(state):  # everything except server-side timestamps
        return {UUID(str(k)): tuple(v) for k, v in state.items()}

    assert report1.outcomes == report2.outcomes
    assert logical(state1) == logical(state2)
    assert rej1 == rej2


def test_partitioned_concurrent_delivery_matches_oracle(settings, db):
    """Same check with 6 workers, robots partitioned by robot_id across them."""
    from fastapi.testclient import TestClient

    from fleetforge.api.app import create_app

    with TestClient(create_app(settings, engine=db)) as tc:
        items = generate(CFG)
        report = send(items, lambda: _NonClosing(tc), workers=6)
    assert report.outcomes == expected(items).outcomes
