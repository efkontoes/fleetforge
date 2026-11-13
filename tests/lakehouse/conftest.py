"""Lakehouse stack tests: Spark + Iceberg (JDBC catalog) + Garage (S3), and Trino.

Deferred: they need Java, the Spark image and the lakehouse stack, which must not
run beside the Milestone 2 stack on the development machine. Run them with
`make -f lakehouse.mk lake-test` (docs/architecture/milestone-3.md, "Deferred
execution"). Without FLEETFORGE_LAKEHOUSE=1 every test here is skipped.

Safety: tests drop and recreate tables, so they refuse any catalog database whose
name does not end in `_test`.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.tables import ALL_TABLES, CATALOG

ENABLED = os.environ.get("FLEETFORGE_LAKEHOUSE") == "1"


def pytest_collection_modifyitems(config, items):
    if ENABLED:
        return
    skip = pytest.mark.skip(reason="lakehouse stack tests need FLEETFORGE_LAKEHOUSE=1")
    for item in items:
        if "tests/lakehouse/" in str(item.fspath):
            item.add_marker(skip)


@pytest.fixture(scope="session")
def lake_settings() -> LakehouseSettings:
    s = LakehouseSettings()
    if not s.catalog_jdbc_url.rstrip("/").endswith("_test"):
        pytest.exit(f"refusing to run destructive tests against {s.catalog_jdbc_url}", 2)
    return s


@pytest.fixture(scope="session")
def spark(lake_settings):
    from fleetforge.lakehouse.jobs.common import session

    s = session("fleetforge-lakehouse-tests", lake_settings)
    yield s
    s.stop()


@pytest.fixture
def clean_lake(spark) -> Iterator[None]:
    """Every test starts from empty implemented tables (PURGE deletes their files)."""
    for t in ALL_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{t.qualified} PURGE")
    yield


@pytest.fixture(scope="session")
def trino_cursor():
    """Trino on the lakehouse network. Polls until the coordinator answers queries
    (it reports healthy before its catalogs are loaded)."""
    import time

    import trino

    conn = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"), port=8080,
                               user="lakehouse-tests", catalog="lake_test")  # fmt: skip
    cur = conn.cursor()
    deadline = time.monotonic() + 180
    while True:
        try:
            cur.execute("SELECT count(*) FROM information_schema.schemata")
            cur.fetchall()
            return cur
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(3)
