"""Shared fixtures.

Integration tests run against a real PostgreSQL 17:
  * if FLEETFORGE_TEST_DATABASE_URL is set, that database is used (CI service container);
  * otherwise Testcontainers starts a throwaway postgres:17 container for the session.
Migrations run once per session; every test starts from empty tables.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

from fleetforge.api.app import create_app
from fleetforge.config import Settings
from fleetforge.db.engine import make_engine

# Pinned by digest so a tag update cannot change what the tests run against.
POSTGRES_IMAGE = (
    "postgres:17@sha256:ff87138949ad9a46f32ff23bd6852b52ce024a13dffe9b4087be98292cdd3b4f"
)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TABLES = (
    "fleet.robot_latest_state, fleet.telemetry_rejections, fleet.outbox_events, "
    "fleet.mission_events, fleet.missions, fleet.robot_boots, fleet.robots, "
    "fleet.consumer_offsets, fleet.replay_runs, downstream.mission_status"
)


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    if url := os.environ.get("FLEETFORGE_TEST_DATABASE_URL"):
        yield url
        return
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as pg:
        yield pg.get_connection_url()


@pytest.fixture(scope="session")
def settings(database_url: str) -> Settings:
    return Settings(database_url=database_url, db_pool_size=20, db_max_overflow=0)


@pytest.fixture(scope="session")
def migrated(settings: Settings) -> None:
    cfg = Config(os.path.join(ROOT, "alembic.ini"))
    cfg.attributes["database_url"] = settings.database_url
    command.upgrade(cfg, "head")


@pytest.fixture(scope="session")
def engine(settings: Settings, migrated: None) -> Iterator[Engine]:
    eng = make_engine(settings)
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine: Engine) -> Engine:
    """Empty tables (reference data such as firmware contracts is kept)."""
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE"))
    return engine


@pytest.fixture
def client(db: Engine, settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings, engine=db)) as c:
        yield c


# ------------------------------------------------------------------ helpers


class Fleet:
    """Small helper so tests read as scenarios rather than HTTP plumbing."""

    def __init__(self, client: TestClient):
        self.c = client

    def robot(self, model: str = "AMR-100", firmware: str = "1.0.0") -> UUID:
        robot_id = uuid4()
        r = self.c.post(
            "/v1/robots",
            json={"robot_id": str(robot_id), "robot_model": model, "firmware_version": firmware},
        )
        assert r.status_code == 201, r.text
        return robot_id

    def boot(self, robot_id: UUID) -> UUID:
        boot_id = uuid4()
        r = self.c.post(f"/v1/robots/{robot_id}/boots", json={"boot_id": str(boot_id)})
        assert r.status_code == 201, r.text
        return boot_id

    @staticmethod
    def event(robot_id: UUID, boot_id: UUID, seq: int, **overrides) -> dict:
        """A valid v1 event. battery and position are derived from seq so each
        observation has distinct, checkable values."""
        ev = {
            "schema_version": 1,
            "event_id": str(uuid4()),
            "robot_id": str(robot_id),
            "boot_id": str(boot_id),
            "sequence_number": seq,
            "observed_at": datetime.now(UTC).isoformat(),
            "battery_pct": 100.0 - (seq % 100) * 0.5,
            "x_m": float(seq),
            "y_m": 0.0,
        }
        ev.update(overrides)
        return ev

    def send(self, ev: dict):
        return self.c.post("/v1/telemetry", json=ev)

    def state(self, robot_id: UUID) -> dict:
        r = self.c.get(f"/v1/robots/{robot_id}/state")
        assert r.status_code == 200, r.text
        return r.json()


@pytest.fixture
def fleet(client: TestClient) -> Fleet:
    return Fleet(client)
