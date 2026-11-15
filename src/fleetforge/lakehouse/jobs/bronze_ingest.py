"""Bronze ingestion: Kafka (bounded batch) -> lake.bronze.kafka_records.

    python -m fleetforge.lakehouse.jobs.bronze_ingest [--topic T ...]
        [--max-records-per-partition N] [--accept-gaps] [--accept-new-incarnation T]

One run, in order:
  1. take the single-writer lease (catalog database advisory lock);
  2. read the checkpoint (newest snapshot carrying one, merged with the data);
  3. ask Kafka for topic ids and watermarks; plan ranges (fail closed: bronze.py);
  4. read exactly those ranges with Spark's Kafka *batch* source;
  5. verify what was read; verify the topic id did not change meanwhile;
  6. anti-join against Bronze rows in the same ranges (insert-if-absent);
  7. append, with checkpoint, ranges and run id in the same Iceberg commit;
  8. verify the new snapshot's parent is the one planned against;
  9. archive registry schemas for ids first seen (separate, idempotent commit).

A crash before step 7 leaves orphan data files and no commit: the next run re-plans
the same ranges. A crash after it leaves a committed checkpoint. An unknown commit
outcome (CommitStateUnknownException) is resolved the same way, by re-reading the
table, because the checkpoint lives in the table itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fleetforge.config import get_settings
from fleetforge.lakehouse import ops
from fleetforge.lakehouse.bronze import (
    BRONZE_COLUMNS,
    SPARK_FRAMING_SQL,
    BronzePositionError,
    OffsetRange,
    TopicPlan,
    Watermarks,
    checkpoints_from_data,
    latest_checkpoint,
    merge_checkpoints,
    plan_topic,
    range_predicate_sql,
    snapshot_properties,
    spark_offsets_json,
    verify_batch,
)
from fleetforge.lakehouse.jobs import exits
from fleetforge.lakehouse.jobs.common import (
    current_snapshot_id,
    ensure_tables,
    fq,
    session,
    single_writer_lease,
    snapshots,
)
from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.tables import BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("fleetforge.lakehouse.bronze")
LEASE = "lake.bronze.kafka_records"


@dataclass(frozen=True)
class SourceTopic:
    contiguous: bool  # every offset is a record: no transactions, no compaction
    avro: bool  # Confluent-framed Avro values whose schemas are archived


def sources(lake: LakehouseSettings) -> dict[str, SourceTopic]:
    return {
        lake.telemetry_topic: SourceTopic(contiguous=True, avro=True),  # processor.py, ADR 0011
        lake.robot_events_topic: SourceTopic(contiguous=False, avro=False),  # Kafka Connect
        lake.mission_events_topic: SourceTopic(contiguous=False, avro=False),
        lake.firmware_topic: SourceTopic(contiguous=False, avro=False),  # compacted
    }


# --------------------------------------------------------------------------- Kafka metadata


def kafka_metadata(bootstrap: str, topic: str) -> tuple[str, dict[int, Watermarks]]:
    """(topic id, watermarks per partition), uncached, from the brokers."""
    from confluent_kafka import Consumer, TopicPartition

    from fleetforge.kafka.offsets import topic_id

    tid = topic_id(bootstrap, topic)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"lakehouse-meta-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        parts = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        wms = {}
        for p in sorted(parts):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10,
                                                       cached=False)  # fmt: skip
            wms[p] = Watermarks(low, high)
    finally:
        consumer.close()
    return tid, wms


# --------------------------------------------------------------------------- Spark stages


def read_kafka(spark: SparkSession, bootstrap: str, ranges: list[OffsetRange]) -> DataFrame:
    start, end = spark_offsets_json(ranges)
    assign: dict[str, list[int]] = {}
    for r in ranges:
        assign.setdefault(r.topic, []).append(r.partition)
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("assign", json.dumps(assign, sort_keys=True))
        .option("startingOffsets", start)
        .option("endingOffsets", end)
        .option("failOnDataLoss", "true")  # a missing offset fails the read
        .option("includeHeaders", "true")
        .load()
    )


def to_bronze(kafka_df: DataFrame, topic_ids: dict[str, str], run_id: str,
              ingested_at: datetime) -> DataFrame:  # fmt: skip
    """Kafka source columns -> Bronze columns, in JVM expressions only (no Python
    UDF on the ingestion path). Must equal bronze.bronze_row(): tests/lakehouse."""
    from pyspark.sql import functions as F

    ids = F.create_map(*[F.lit(x) for kv in sorted(topic_ids.items()) for x in kv])
    cols = {
        "source_topic": F.col("topic"),
        "kafka_topic_id": ids[F.col("topic")],
        "kafka_partition": F.col("partition"),
        "kafka_offset": F.col("offset"),
        "kafka_timestamp": F.expr(SPARK_FRAMING_SQL["kafka_timestamp"]),
        "kafka_timestamp_type": F.expr(SPARK_FRAMING_SQL["kafka_timestamp_type"]),
        "record_key": F.col("key"),
        "record_value": F.col("value"),
        "record_headers": F.col("headers"),
        "value_sha256": F.expr(SPARK_FRAMING_SQL["value_sha256"]),
        "value_framing": F.expr(SPARK_FRAMING_SQL["value_framing"]),
        "value_schema_id": F.expr(SPARK_FRAMING_SQL["value_schema_id"]),
        "ingestion_run_id": F.lit(run_id),
        "ingested_at": F.lit(ingested_at).cast("timestamp"),
    }
    return kafka_df.select(*[cols[c].alias(c) for c in BRONZE_COLUMNS])


def observed_ranges(df: DataFrame) -> dict[tuple[str, int], tuple[int, int, int]]:
    from pyspark.sql import functions as F

    rows = df.groupBy("source_topic", "kafka_partition").agg(
        F.count("*"), F.min("kafka_offset"), F.max("kafka_offset")
    ).collect()  # fmt: skip
    return {(r[0], r[1]): (r[2], r[3], r[4]) for r in rows}


def append_bronze(spark: SparkSession, rows: DataFrame, ranges: list[OffsetRange],
                  properties: dict[str, str],
                  fault: Callable[[str], None] = lambda stage: None) -> int:  # fmt: skip
    """Insert-if-absent append with the checkpoint in the same commit. Returns the
    number of new rows. Shared by the Kafka path and the fixture-driven tests."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    target = fq(BRONZE_KAFKA_RECORDS)
    identity = list(BRONZE_KAFKA_RECORDS.identity)
    existing = spark.table(target).where(range_predicate_sql(ranges)).select(*identity)
    # One row per identity within the batch. Not dropDuplicates(): on Spark 4.1.4 its
    # aggregate plus Iceberg's write projection fails CollapseProject plan validation.
    first = Window.partitionBy(*identity).orderBy("ingested_at")
    new = (
        rows.join(existing, identity, "left_anti")
        .withColumn("_copy", F.row_number().over(first))
        .where("_copy = 1")
        .drop("_copy")
    )
    count = new.count()
    writer = new.writeTo(target)
    for k, v in properties.items():
        writer = writer.option(f"snapshot-property.{k}", v)
    fault("before_commit")  # tests: a crash after reading and planning, before the commit
    # Also when count == 0: the commit still records the checkpoint (otherwise a fully
    # replayed range would be planned again). Whether Iceberg commits an empty append
    # is checked by tests/lakehouse; merge_checkpoints() covers it if it does not.
    writer.append()
    return count


def read_checkpoints(spark: SparkSession, topics: Iterable[str]) -> dict:
    from pyspark.sql import functions as F

    target = BRONZE_KAFKA_RECORDS
    committed = latest_checkpoint(snapshots(spark, target), current_snapshot_id(spark, target))
    topics = sorted(topics)
    data_rows = (
        spark.table(fq(target)).where(F.col("source_topic").isin(topics))
        .groupBy("source_topic", "kafka_topic_id", "kafka_partition")
        .agg(F.max("kafka_offset"), F.max("ingested_at")).collect()
    )  # fmt: skip
    derived = checkpoints_from_data([tuple(r) for r in data_rows])
    return merge_checkpoints(committed, derived)


def archive_schemas(
    spark: SparkSession, run_id: str, registry_url: str, topics: list[str] | None = None
) -> int:
    """Archive every schema id referenced by Bronze Avro topics and not yet archived.
    Raises RegistryUnavailable (retry later) rather than guessing."""
    from pyspark.sql import functions as F

    from fleetforge.kafka.codec import RegistryUnavailable, schema_not_found
    from fleetforge.kafka.schemas import registry_client

    avro_topics = topics or [t for t, s in sources(LakehouseSettings()).items() if s.avro]
    wanted = (
        spark.table(fq(BRONZE_KAFKA_RECORDS))
        .where(F.col("source_topic").isin(avro_topics) & F.col("value_schema_id").isNotNull())
        .groupBy("value_schema_id").agg(F.min("source_topic").alias("topic"))
    )  # fmt: skip
    # Only FOUND is final. A NOT_FOUND id is asked again on every run: a restarting
    # registry can answer 404 for an id it holds (observed in Milestone 2's CI).
    # The archive is a few rows: read it into Python (a join with a table created in the same
    # session trips Spark 4.1's "computeStats called before pushdown" planner bug).
    archived = spark.table(fq(BRONZE_REGISTRY_SCHEMAS)).select("schema_id", "status").collect()
    found = {r[0] for r in archived if r[1] == "FOUND"}
    known_absent = {r[0] for r in archived if r[1] == "NOT_FOUND"}
    missing = [r for r in wanted.collect() if int(r[0]) not in found]
    if not missing:
        return 0
    client = registry_client(registry_url)
    now = datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for r in missing:
        sid, topic = int(r[0]), r[1]
        base = {"schema_id": sid, "registry_url": registry_url, "first_seen_topic": topic,
                "archived_at": now, "ingestion_run_id": run_id}  # fmt: skip
        try:
            reg = client.get_schema(sid)
        except Exception as exc:
            # Only an explicit 404 is recorded, and only as "not found yet" (ADR 0016).
            if not schema_not_found(exc):
                raise RegistryUnavailable(f"schema {sid}: {exc}") from exc
            if sid in known_absent:
                continue  # still absent; already recorded
            rows.append({**base, "status": "NOT_FOUND", "schema_type": None,
                         "schema_text": None, "schema_sha256": None})  # fmt: skip
            continue
        text = reg.schema_str
        rows.append({**base, "status": "FOUND", "schema_type": reg.schema_type or "AVRO",
                     "schema_text": text,
                     "schema_sha256": hashlib.sha256(text.encode()).hexdigest()})  # fmt: skip
    if not rows:
        return 0
    ordered = [{c: row[c] for c in BRONZE_REGISTRY_SCHEMAS.column_names} for row in rows]
    df = spark.createDataFrame(ordered, schema=BRONZE_REGISTRY_SCHEMAS.spark_schema())
    df.writeTo(fq(BRONZE_REGISTRY_SCHEMAS)).append()
    return len(rows)


def verify_parent(spark: SparkSession, planned_on: int | None) -> None:
    """The commit must sit directly on the snapshot the plan was made against.
    Otherwise another writer committed in between: duplicates are still prevented
    only if it wrote other ranges. Escalate; never ignore."""
    sid = current_snapshot_id(spark, BRONZE_KAFKA_RECORDS)
    parent = {s: p for s, p, _ in snapshots(spark, BRONZE_KAFKA_RECORDS)}.get(sid)
    if sid != planned_on and parent != planned_on:
        raise BronzePositionError(
            f"concurrent Bronze writer: committed on {parent}, planned on {planned_on}"
        )


# --------------------------------------------------------------------------- entry point


def run(args: argparse.Namespace) -> dict[str, Any]:
    from pyspark import StorageLevel

    lake = LakehouseSettings()
    m2 = get_settings()
    run_id = f"bronze-{uuid.uuid4()}"
    ingested_at = datetime.now(UTC)  # constant for the whole run (arrival order)
    known = sources(lake)
    topics = args.topic or list(known)
    if unknown := set(topics) - set(known):
        raise SystemExit(f"unknown topics {sorted(unknown)}; configured: {sorted(known)}")
    with single_writer_lease(lake.catalog_dsn, LEASE):
        spark = session("fleetforge-bronze-ingest", lake)
        ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
        planned_on = current_snapshot_id(spark, BRONZE_KAFKA_RECORDS)
        previous = read_checkpoints(spark, topics)
        plans: list[TopicPlan] = []
        for topic in topics:
            tid, wms = kafka_metadata(m2.kafka_bootstrap, topic)
            start = None if args.replay else previous.get(topic)  # replay: from the earliest offset
            plans.append(plan_topic(
                topic, tid, start, wms,
                max_records_per_partition=args.max_records_per_partition,
                accept_gaps=args.accept_gaps,
                accept_new_incarnation=topic in (args.accept_new_incarnation or []),
            ))  # fmt: skip
        ranges = [r for p in plans for r in p.ranges]
        report: dict[str, Any] = {"run_id": run_id, "ranges": len(ranges), "new_rows": 0,
                                  "notes": [n for p in plans for n in p.notes]}  # fmt: skip
        if ranges:
            kafka_df = read_kafka(spark, m2.kafka_bootstrap, ranges)
            topic_ids = {p.topic: p.topic_id for p in plans}
            rows = to_bronze(kafka_df, topic_ids, run_id, ingested_at)
            rows = rows.persist(StorageLevel.DISK_ONLY)  # read Kafka once: verify, then write
            observed = observed_ranges(rows)
            for p in plans:
                mine = {k: v for k, v in observed.items() if k[0] == p.topic}
                verify_batch(p.ranges, mine, contiguous=known[p.topic].contiguous)
            for p in plans:
                if p.ranges and kafka_metadata(m2.kafka_bootstrap, p.topic)[0] != p.topic_id:
                    raise BronzePositionError(f"{p.topic} was recreated during the read")
            props = snapshot_properties(run_id, plans, previous)
            report["new_rows"] = append_bronze(spark, rows, ranges, props)
            verify_parent(spark, planned_on)
            rows.unpersist()
        report["schemas_archived"] = archive_schemas(spark, run_id, m2.schema_registry_url)
        report["snapshot_id"] = current_snapshot_id(spark, BRONZE_KAFKA_RECORDS)
        report["lineage"] = {
            "inputs": [ops.dataset(f"kafka://{m2.kafka_bootstrap}", t) for t in topics],
            "outputs": [ops.table(BRONZE_KAFKA_RECORDS.qualified, report["snapshot_id"])],
        }
        return report


def main(argv: list[str] | None = None) -> dict[str, Any]:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="fleet-lake-bronze")
    ap.add_argument("--topic", action="append", help="default: every configured source topic")
    ap.add_argument("--max-records-per-partition", type=int)
    ap.add_argument(
        "--accept-gaps",
        action="store_true",
        help="record retention gaps and continue (audited in the snapshot)",
    )
    ap.add_argument("--replay", action="store_true",
                    help="re-read from the earliest offset, skipping rows Bronze has")  # fmt: skip
    ap.add_argument("--accept-new-incarnation", action="append", metavar="TOPIC",
                    help="treat a recreated topic as new (audited in the snapshot)")  # fmt: skip
    return run(ap.parse_args(argv))


if __name__ == "__main__":
    exits.run(main)
