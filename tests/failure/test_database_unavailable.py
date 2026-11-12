"""Database unavailable: the API must say so (503 + Retry-After), never fake success,
and must recover by itself when connections are restored."""

from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import NullPool, create_engine, text

from fleetforge.api.app import create_app
from fleetforge.config import Settings
from fleetforge.db.engine import make_engine


def test_unreachable_database_returns_503_with_retry_after():
    # Port 1 on localhost: connection refused immediately.
    settings = Settings(
        database_url="postgresql+psycopg://x:y@127.0.0.1:1/none", db_pool_timeout_s=1
    )
    engine = make_engine(settings)
    with TestClient(create_app(settings, engine=engine)) as client:
        ev = {
            "schema_version": 1,
            "event_id": str(uuid4()),
            "robot_id": str(uuid4()),
            "boot_id": str(uuid4()),
            "sequence_number": 1,
            "observed_at": "2026-11-12T10:00:00Z",
            "battery_pct": 50.0,
            "x_m": 0.0,
            "y_m": 0.0,
        }
        for method, path, body in [
            ("post", "/v1/telemetry", ev),
            ("get", f"/v1/robots/{uuid4()}/state", None),
            ("get", "/readyz", None),
        ]:
            r = getattr(client, method)(path, json=body) if body else getattr(client, method)(path)
            assert r.status_code == 503, (path, r.text)
            assert r.headers["Retry-After"] == "1"
            assert r.json()["error"]["code"] == "DATABASE_UNAVAILABLE"
        assert client.get("/healthz").status_code == 200  # process is alive, just not ready
    engine.dispose()


def test_pool_recovers_after_all_connections_are_killed(fleet, settings):
    """Simulates a database restart: every pooled connection is terminated server-side."""
    robot = fleet.robot()
    boot = fleet.boot(robot)
    assert fleet.send(fleet.event(robot, boot, 1)).status_code == 200

    # The killer uses its own unpooled connection, outside the application's pool.
    outsider = create_engine(settings.database_url, poolclass=NullPool)
    with outsider.connect() as c:
        killed = c.execute(
            text("""
                SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
                WHERE datname = current_database() AND pid <> pg_backend_pid()
                  AND backend_type = 'client backend'
            """)
        ).scalar_one()
    assert killed >= 1

    # pool_pre_ping detects the dead connections and replaces them transparently.
    r = fleet.send(fleet.event(robot, boot, 2))
    assert r.status_code == 200
    assert fleet.state(robot)["sequence_number"] == 2


def test_retry_after_lost_response_is_safe(fleet):
    """The classic ambiguity: the server committed but the client never saw the
    response, so it retries. Idempotency turns the retry into a no-op."""
    robot = fleet.robot()
    boot = fleet.boot(robot)
    ev = fleet.event(robot, boot, 7)
    fleet.send(ev)  # response "lost"
    retry = fleet.send(ev)
    assert (retry.status_code, retry.json()["outcome"]) == (200, "DUPLICATE")
    assert fleet.state(robot)["state_version"] == 1


def test_fresh_engine_against_real_db_reports_ready(settings):
    engine = make_engine(settings)
    with TestClient(create_app(settings, engine=engine)) as client:
        assert client.get("/readyz").status_code == 200
    engine.dispose()
