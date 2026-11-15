"""Freshness SLO, measured: how long after Kafka a record became queryable in Silver.

A record is available when the first Silver build covering its Bronze ingestion commits.
python -m fleetforge.lakehouse.jobs.freshness [--hours 24] [--target-minutes 15]
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from fleetforge.lakehouse.jobs import exits
from fleetforge.lakehouse.jobs.common import fq, session
from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.tables import BRONZE_KAFKA_RECORDS, SILVER_BUILD_LOG

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("fleetforge.lakehouse.freshness")


def availability(
    builds: list[tuple[datetime, datetime]], ingested: list[datetime], now: datetime
) -> dict[datetime, datetime]:
    """Per Bronze ingestion time, the commit of the first build covering it (else now)."""
    ordered = sorted(builds)  # (committed_at, bronze_max_ingested_at)
    out = {}
    for at in ingested:
        out[at] = next((c for c, covered in ordered if covered >= at), now)
    return out


def measure(
    spark: SparkSession,
    topic: str,
    hours: float = 24,
    target_minutes: float = 15,
    now: datetime | None = None,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    now = now or datetime.now(UTC)
    if not spark.catalog.tableExists(fq(SILVER_BUILD_LOG)):
        return {"records": 0, "topic": topic, "measured_at": now, "slo_met": None}
    since = now - timedelta(hours=hours)
    builds = [
        (r[0], r[1])
        for r in spark.table(fq(SILVER_BUILD_LOG))
        .select("committed_at", "bronze_max_ingested_at")
        .collect()
    ]
    recent = (F.col("source_topic") == topic) & (F.col("kafka_timestamp") >= F.lit(since))
    bronze = spark.table(fq(BRONZE_KAFKA_RECORDS)).where(recent)
    runs = [r[0] for r in bronze.select("ingested_at").distinct().collect()]
    naive_now = now.astimezone().replace(tzinfo=None)  # PySpark collects naive local times
    ready = availability(builds, runs, naive_now)
    ready_df = spark.createDataFrame(
        [(k, v) for k, v in ready.items()], "ingested_at TIMESTAMP, available_at TIMESTAMP"
    )
    lat = bronze.join(ready_df, "ingested_at").select(
        ((F.unix_micros("available_at") - F.unix_micros("kafka_timestamp")) / 1e6).alias("s")
    )
    target_s = target_minutes * 60
    row = lat.agg(
        F.count("s"),
        F.percentile("s", 0.5),
        F.percentile("s", 0.95),
        F.percentile("s", 0.99),
        F.max("s"),
        F.avg((F.col("s") <= target_s).cast("double")),
    ).collect()[0]
    keys = ("records", "p50_s", "p95_s", "p99_s", "max_s", "within_target")
    report: dict[str, Any] = dict(zip(keys, row, strict=True))
    report.update(
        topic=topic,
        window_hours=hours,
        target_minutes=target_minutes,
        measured_at=now,
        slo="95% of records within the target",
    )
    report["slo_met"] = (report["within_target"] or 0) >= 0.95
    return report


def main(argv: list[str] | None = None) -> dict[str, Any]:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="fleet-lake-freshness")
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--target-minutes", type=float, default=15)
    args = ap.parse_args(argv)
    lake = LakehouseSettings()
    report = measure(
        session("fleetforge-freshness", lake), lake.telemetry_topic, args.hours, args.target_minutes
    )
    return report


if __name__ == "__main__":
    exits.run(main)
