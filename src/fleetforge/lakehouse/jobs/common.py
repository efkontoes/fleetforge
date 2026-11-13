"""Spark session, DDL, snapshot metadata and the single-writer lease."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from fleetforge.lakehouse import versions
from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.tables import CATALOG, Table

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = logging.getLogger("fleetforge.lakehouse")


def spark_conf(s: LakehouseSettings) -> dict[str, str]:
    """Everything the session needs, as plain strings (unit-testable without Java)."""
    cat = f"spark.sql.catalog.{CATALOG}"
    conf = {
        "spark.master": s.spark_master,
        "spark.driver.memory": s.driver_memory,  # only effective before the JVM starts
        "spark.sql.shuffle.partitions": str(s.shuffle_partitions),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"{cat}.uri": s.catalog_jdbc_url,
        f"{cat}.jdbc.user": s.catalog_user,
        f"{cat}.jdbc.password": s.catalog_password,
        f"{cat}.warehouse": s.warehouse,
        f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{cat}.s3.endpoint": s.s3_endpoint,
        f"{cat}.s3.path-style-access": "true",
        f"{cat}.s3.access-key-id": s.s3_access_key,
        f"{cat}.s3.secret-access-key": s.s3_secret_key,
        f"{cat}.client.region": s.s3_region,
    }
    if s.jars_dir:
        jars = sorted(str(p) for p in Path(s.jars_dir).glob("*.jar"))
        conf["spark.jars"] = ",".join(jars)
    else:
        conf["spark.jars.packages"] = ",".join(versions.SPARK_PACKAGES)
    return conf


def session(app_name: str, settings: LakehouseSettings | None = None) -> SparkSession:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for k, v in spark_conf(settings or LakehouseSettings()).items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def ensure_tables(spark: SparkSession, tables: Iterable[Table]) -> None:
    for t in tables:
        for stmt in t.create_statements(CATALOG):
            spark.sql(stmt)


def fq(table: Table) -> str:
    return f"{CATALOG}.{table.qualified}"


def current_snapshot_id(spark: SparkSession, table: Table) -> int | None:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {fq(table)}.history WHERE is_current_ancestor "
        "ORDER BY made_current_at DESC LIMIT 1"
    ).collect()
    return rows[0][0] if rows else None


def snapshots(spark: SparkSession, table: Table) -> list[tuple[int, int | None, dict[str, str]]]:
    rows = spark.sql(f"SELECT snapshot_id, parent_id, summary FROM {fq(table)}.snapshots").collect()
    return [(r[0], r[1], dict(r[2] or {})) for r in rows]


def snapshot_summary(spark: SparkSession, table: Table, snapshot_id: int | None) -> dict[str, str]:
    for sid, _, summary in snapshots(spark, table):
        if sid == snapshot_id:
            return summary
    return {}


class LeaseUnavailable(RuntimeError):
    """Another writer holds the lease. Not an error of this run: exit and let the
    scheduler try later. Never wait-and-retry inside the job (two queued writers
    would then run back to back on stale plans)."""


def lease_key(name: str) -> int:
    return int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], "big", signed=True)


@contextmanager
def single_writer_lease(dsn: str, name: str) -> Iterator[None]:
    """Session-level PostgreSQL advisory lock in the catalog database.

    Released automatically if this process dies (its connection closes). Limit: if
    the connection drops while the job keeps running, the lock is gone too; the
    post-commit parent check (bronze job) detects a writer that slipped in."""
    import psycopg

    key = lease_key(name)
    with psycopg.connect(dsn, autocommit=True) as conn:
        if not conn.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]:
            raise LeaseUnavailable(f"another writer holds the lease for {name}")
        try:
            yield
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (key,))
