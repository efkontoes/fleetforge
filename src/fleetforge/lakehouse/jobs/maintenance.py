"""Table maintenance: expire snapshots (release tags survive), find orphans, report storage.

python -m fleetforge.lakehouse.jobs.maintenance [--delete-orphans] [--compact]
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from fleetforge.lakehouse.jobs import exits
from fleetforge.lakehouse.jobs.common import ensure_tables, fq, session, single_writer_lease
from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.tables import ALL_TABLES, CATALOG, OPS_TABLE_STORAGE, Table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("fleetforge.lakehouse.maintenance")
LEASE = "lake-maintenance"
SMALL_FILE = 8 * 1024 * 1024


def _exists(spark: SparkSession, table: Table) -> bool:
    return spark.catalog.tableExists(fq(table))


def _ts(at: datetime) -> str:
    return f"TIMESTAMP '{at.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def measure(spark: SparkSession, table: Table) -> dict[str, int]:
    files = spark.sql(
        f"SELECT count(*), coalesce(sum(file_size_in_bytes), 0), coalesce(sum(record_count), 0), "
        f"count_if(file_size_in_bytes < {SMALL_FILE}) FROM {fq(table)}.data_files"
    ).collect()[0]
    snapshots = spark.sql(f"SELECT count(*) FROM {fq(table)}.snapshots").collect()[0][0]
    tags = spark.sql(f"SELECT count(*) FROM {fq(table)}.refs WHERE type = 'TAG'").collect()[0][0]
    return {"data_files": files[0], "data_bytes": files[1], "records": files[2],
            "small_files": files[3], "snapshots": snapshots, "tags": tags}  # fmt: skip


def maintain(
    spark: SparkSession,
    *,
    now: datetime | None = None,
    keep_days: int = 7,
    retain_last: int = 10,
    orphan_age: timedelta = timedelta(hours=72),
    delete_orphans: bool = False,
    compact: bool = False,
) -> list[dict[str, Any]]:
    """Maintain every existing lakehouse table and record one ops.table_storage row each."""
    now = now or datetime.now(UTC)
    run_id = uuid.uuid4().hex
    ensure_tables(spark, [OPS_TABLE_STORAGE])
    rows = []
    for table in ALL_TABLES:
        if table is OPS_TABLE_STORAGE or not _exists(spark, table):
            continue
        name = fq(table)
        before = measure(spark, table)["snapshots"]
        # Tagged snapshots are references, so expiry never removes a release's inputs.
        spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{name}', "
                  f"older_than => {_ts(now - timedelta(days=keep_days))}, "
                  f"retain_last => {retain_last})").collect()  # fmt: skip
        orphans = spark.sql(
            f"CALL {CATALOG}.system.remove_orphan_files(table => '{name}', "
            f"older_than => {_ts(now - orphan_age)}, "
            f"dry_run => {str(not delete_orphans).lower()}, "
            "prefix_listing => true)"
        ).collect()  # S3FileIO lists, not Hadoop
        if compact:
            spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{name}')").collect()
        stats = measure(spark, table)
        expired = before - stats["snapshots"]
        rows.append({"run_id": run_id, "table_name": table.qualified, **stats,
                     "orphan_files": len(orphans),
                     "expired_snapshots": expired, "measured_at": now})  # fmt: skip
    if rows:
        spark.createDataFrame(rows, OPS_TABLE_STORAGE.spark_schema()).writeTo(
            fq(OPS_TABLE_STORAGE)
        ).append()
    return rows


def main(argv: list[str] | None = None) -> list[dict[str, Any]]:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="fleet-lake-maintenance")
    ap.add_argument("--keep-days", type=int, default=7, help="expire untagged snapshots older")
    ap.add_argument("--retain-last", type=int, default=10)
    ap.add_argument("--delete-orphans", action="store_true", help="delete, not just report")
    ap.add_argument("--compact", action="store_true", help="rewrite small data files")
    args = ap.parse_args(argv)
    lake = LakehouseSettings()
    with single_writer_lease(lake.catalog_dsn, LEASE):
        rows = maintain(session("fleetforge-maintenance", lake), keep_days=args.keep_days,
                        retain_last=args.retain_last, delete_orphans=args.delete_orphans,
                        compact=args.compact)  # fmt: skip
    return rows


if __name__ == "__main__":
    exits.run(main)
