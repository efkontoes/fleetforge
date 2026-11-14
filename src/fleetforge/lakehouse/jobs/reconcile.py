"""Reconciliation: Kafka -> Bronze -> Silver -> Gold counts, recorded in ops.reconciliation_runs.

python -m fleetforge.lakehouse.jobs.reconcile [--kafka-bootstrap host:port]
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fleetforge.lakehouse.jobs import exits
from fleetforge.lakehouse.jobs.common import current_snapshot_id, ensure_tables, fq, session
from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.silver import TELEMETRY_TOPIC
from fleetforge.lakehouse.tables import (
    BRONZE_KAFKA_RECORDS,
    GOLD_PIPELINE_DAILY_QUALITY,
    GOLD_ROBOT_DAILY_TELEMETRY,
    GOLD_ROBOT_STATE_OBSERVATIONS,
    GOLD_TRAINING_DATASET_RELEASES,
    OPS_RECONCILIATION_RUNS,
    SILVER_BUILD_LOG,
    SILVER_TELEMETRY_EVENTS,
    SILVER_TELEMETRY_RECORD_OUTCOMES,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("fleetforge.lakehouse.reconcile")
COORDS = "kafka_topic_id, kafka_partition, kafka_offset"


class Checks:
    """Collects check results; each check is expected == actual unless stated otherwise."""

    def __init__(self, build_id: str | None) -> None:
        self.run_id, self.build_id, self.rows = uuid.uuid4().hex, build_id, []

    def add(self, name: str, expected: Any, actual: Any, detail: str = "",
            passed: bool | None = None) -> None:  # fmt: skip
        ok = expected == actual if passed is None else passed
        self.rows.append({
            "run_id": self.run_id, "check_name": name, "passed": bool(ok),
            "expected": json.dumps(expected, default=str),
            "actual": json.dumps(actual, default=str), "detail": detail or None,
            "silver_build_id": self.build_id, "checked_at": datetime.now(UTC),
        })  # fmt: skip


def _one(spark: SparkSession, sql: str) -> Any:
    return spark.sql(sql).collect()[0]


def _at(table, snapshot) -> str:
    return f"{fq(table)} VERSION AS OF {int(snapshot)}"


def check_kafka(spark: SparkSession, checks: Checks, bootstrap: str, topic: str) -> None:
    """Count Kafka records up to Bronze's checkpoint with an independent read, per partition."""
    from fleetforge.lakehouse.jobs import bronze_ingest as bj

    sql = (f"SELECT kafka_topic_id, kafka_partition, count(*), max(kafka_offset) "
           f"FROM {fq(BRONZE_KAFKA_RECORDS)} WHERE source_topic = '{topic}' "
           "GROUP BY 1, 2")  # fmt: skip
    bronze = {(r[0], r[1]): (r[2], r[3]) for r in spark.sql(sql).collect()}
    tid, _ = bj.kafka_metadata(bootstrap, topic)
    ends = {p: last + 1 for (t, p), (_, last) in bronze.items() if t == tid}
    if not ends:
        checks.add(f"kafka_vs_bronze:{topic}", "bronze rows", 0, "no Bronze rows for this topic")
        return
    offsets = json.dumps({topic: {str(p): e for p, e in ends.items()}})
    kafka = (spark.read.format("kafka").option("kafka.bootstrap.servers", bootstrap)
             .option("subscribe", topic).option("startingOffsets", "earliest")
             .option("endingOffsets", offsets).load()
             .groupBy("partition").count().collect())  # fmt: skip
    got = {r[0]: r[1] for r in kafka}
    want = {p: bronze[(tid, p)][0] for p in ends}
    checks.add(f"kafka_vs_bronze:{topic}", want, {p: got.get(p, 0) for p in want},
               "Kafka records below Bronze's checkpoint, per partition")  # fmt: skip


def reconcile(spark: SparkSession, kafka_bootstrap: str | None = None,
              telemetry_topic: str = TELEMETRY_TOPIC) -> list[dict[str, Any]]:  # fmt: skip
    if not spark.catalog.tableExists(fq(SILVER_BUILD_LOG)):
        return []  # no Silver build yet: nothing to reconcile
    builds = spark.table(fq(SILVER_BUILD_LOG)).orderBy("committed_at", ascending=False).limit(1)
    found = builds.collect()
    if not found:
        return []
    b = found[0].asDict()
    checks = Checks(b["build_id"])
    bronze, outcomes = _at(BRONZE_KAFKA_RECORDS, b["bronze_snapshot_id"]), _at(
        SILVER_TELEMETRY_RECORD_OUTCOMES, b["outcomes_snapshot_id"])  # fmt: skip
    events = _at(SILVER_TELEMETRY_EVENTS, b["events_snapshot_id"])

    n, keys = _one(spark, f"SELECT count(*), count(DISTINCT {COORDS}) FROM {bronze}")
    checks.add("bronze_coordinates_unique", n, keys, "one row per (topic id, partition, offset)")

    n_bronze = _one(spark, f"SELECT count(*) FROM {bronze} "
                           f"WHERE source_topic = '{telemetry_topic}'")[0]  # fmt: skip
    classes = {r[0]: r[1] for r in spark.sql(
        f"SELECT outcome, count(*) FROM {outcomes} GROUP BY 1").collect()}  # fmt: skip
    checks.add("bronze_equals_outcomes", n_bronze, sum(classes.values()),
               f"accepted + duplicate + rejected + unresolved = {classes}")  # fmt: skip
    n_out, out_keys = _one(spark, f"SELECT count(*), count(DISTINCT {COORDS}) FROM {outcomes}")
    checks.add("outcomes_one_per_record", n_out, out_keys)

    for table, key in ((SILVER_TELEMETRY_EVENTS, "events_snapshot_id"),
                       (SILVER_TELEMETRY_RECORD_OUTCOMES, "outcomes_snapshot_id")):  # fmt: skip
        checks.add(f"unchanged_since_build:{table.name}", b[key],
                   current_snapshot_id(spark, table), "written only by Silver builds")  # fmt: skip

    n_events, ids = _one(spark, f"SELECT count(*), count(DISTINCT event_id) FROM {events}")
    checks.add("events_equal_accepted", classes.get("ACCEPTED", 0), n_events)
    checks.add("event_ids_unique", n_events, ids)

    gold_checks: dict[str, Callable[[], None]] = {
        GOLD_PIPELINE_DAILY_QUALITY.qualified: lambda: _gold_quality(spark, checks, b, n_out),
        GOLD_ROBOT_DAILY_TELEMETRY.qualified: lambda: _gold_daily(spark, checks, b, events),
        GOLD_TRAINING_DATASET_RELEASES.qualified: lambda: _releases(spark, checks),
    }
    for name, check in gold_checks.items():
        if spark.catalog.tableExists(f"lake.{name}"):
            check()
    if kafka_bootstrap:
        check_kafka(spark, checks, kafka_bootstrap, telemetry_topic)

    ensure_tables(spark, [OPS_RECONCILIATION_RUNS])
    spark.createDataFrame(checks.rows, OPS_RECONCILIATION_RUNS.spark_schema()).writeTo(
        fq(OPS_RECONCILIATION_RUNS)
    ).append()
    return checks.rows


def _gold_quality(spark: SparkSession, checks: Checks, b: dict, n_outcomes: int) -> None:
    builds, records = _one(spark, f"SELECT collect_set(silver_build_id), sum(records) "
                                  f"FROM {fq(GOLD_PIPELINE_DAILY_QUALITY)}")  # fmt: skip
    checks.add("gold_quality_is_current", [b["build_id"]], sorted(builds))
    checks.add("gold_quality_equals_outcomes", n_outcomes, records)


def _gold_daily(spark: SparkSession, checks: Checks, b: dict, events: str) -> None:
    gold = fq(GOLD_ROBOT_DAILY_TELEMETRY)
    diff = _one(spark, f"""
        WITH s AS (SELECT to_date(observed_at) AS d, robot_id, count(*) AS n FROM {events}
                   WHERE is_current GROUP BY 1, 2),
             g AS (SELECT observed_date AS d, robot_id, events AS n, is_final, silver_build_id
                   FROM {gold})
        SELECT count(*) FROM s FULL OUTER JOIN g ON s.d = g.d AND s.robot_id = g.robot_id
        WHERE g.robot_id IS NULL OR (g.silver_build_id = '{b["build_id"]}'
              AND (s.robot_id IS NULL OR s.n <> g.n)) OR (g.silver_build_id <> '{b["build_id"]}'
              AND NOT g.is_final)""")[0]  # fmt: skip
    checks.add("gold_daily_equals_silver", 0, diff, "robot-days that disagree, final days aside")


def _releases(spark: SparkSession, checks: Checks) -> None:
    releases = spark.table(fq(GOLD_TRAINING_DATASET_RELEASES)).collect()
    for r in releases:
        stored = _one(spark, f"SELECT count(*) FROM {fq(GOLD_ROBOT_STATE_OBSERVATIONS)} "
                             f"VERSION AS OF {r['observations_snapshot_id']} "
                             f"WHERE release_id = '{r['release_id']}'")[0]  # fmt: skip
        checks.add(f"release_rows:{r['release_id']}", r["rows_total"], stored)


def main(argv: list[str] | None = None) -> tuple[int, list[dict[str, Any]]]:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="fleet-lake-reconcile")
    ap.add_argument("--kafka-bootstrap", help="also count Kafka records independently")
    args = ap.parse_args(argv)
    lake = LakehouseSettings()
    rows = reconcile(session("fleetforge-reconcile", lake), args.kafka_bootstrap,
                     lake.telemetry_topic)  # fmt: skip
    return (0 if all(r["passed"] for r in rows) else exits.PERMANENT), rows


if __name__ == "__main__":
    exits.run(main)
