"""Silver build: one pinned Bronze snapshot -> lake.silver.* (full deterministic rebuild).

    python -m fleetforge.lakehouse.jobs.silver_build

Slice 1 rebuilds Silver completely from a pinned Bronze snapshot and replaces each
Silver table in one atomic Iceberg commit (overwrite of all rows). That is the
simplest way to make Silver a pure function of its inputs: the same Bronze snapshot,
reference data and rules always give the same build id and the same rows, and a
rebuild with an unchanged build id is skipped. Incremental processing is slice 2;
its acceptance test is equality with this full rebuild.

Execution plan:
  driver:  pin the Bronze snapshot; load archived schemas; fail if any schema id in
           Bronze is not archived (recoverable, never a quarantine); rebuild the
           reference data from the firmware CDC and robot outbox rows (small).
  row:     mapInPandas runs silver.prepare() on every telemetry row (narrow: no shuffle).
  set:     window functions over event_id, the ordering slot, correction targets and
           robot (each a shuffle), mirroring silver.build() step by step.
  write:   reference tables, outcomes, then events. Iceberg has no multi-table
           commit here, so readers pair tables by silver_build_id (see the spec).
"""

from __future__ import annotations

import json
import logging
import math
import sys
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fleetforge.lakehouse.bronze import CONFLUENT
from fleetforge.lakehouse.jobs.common import (
    current_snapshot_id,
    ensure_tables,
    fq,
    session,
    single_writer_lease,
    snapshot_summary,
)
from fleetforge.lakehouse.reference import (
    FIRMWARE_TOPIC,
    ROBOT_EVENTS_TOPIC,
    Reference,
    build_reference,
)
from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.silver import (
    EVENT_ID_CONFLICT_TEMPLATE,
    PREPARED_COLUMNS,
    PREPARED_SCHEMA,
    REASON_TABLE,
    SEQUENCE_CONFLICT_TEMPLATE,
    TELEMETRY_TOPIC,
    TRANSFORM_VERSION,
    Flag,
    SchemaArchiveIncomplete,
    SilverReason,
    build_id,
    parse_writer_schemas,
    prepare,
    prepared_row,
    template_sql,
)
from fleetforge.lakehouse.tables import (
    BRONZE_KAFKA_RECORDS,
    BRONZE_REGISTRY_SCHEMAS,
    SILVER_FIRMWARE_CONTRACTS,
    SILVER_ROBOT_REGISTRY,
    SILVER_TELEMETRY_EVENTS,
    SILVER_TELEMETRY_RECORD_OUTCOMES,
    Table,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("fleetforge.lakehouse.silver")
LEASE = "lake.silver"
BUILD_ID_PROPERTY = "fleetforge.silver.build-id"
ARRIVAL = ("ingested_at", "kafka_topic_id", "kafka_partition", "kafka_offset")
ROW_STAGE_INPUT = ("source_topic", "kafka_topic_id", "kafka_partition", "kafka_offset",
                   "kafka_timestamp", "record_value", "value_framing", "value_schema_id",
                   "ingested_at")  # fmt: skip
SILVER_TABLES = (SILVER_FIRMWARE_CONTRACTS, SILVER_ROBOT_REGISTRY,
                 SILVER_TELEMETRY_RECORD_OUTCOMES, SILVER_TELEMETRY_EVENTS)  # fmt: skip


# --------------------------------------------------------------------------- row stage


def _py(value: Any, spark_type: str) -> Any:
    """pandas cell -> plain Python value. Arrow hands nullable integers over as floats
    with NaN, and timestamps as tz-naive values in the session time zone (UTC)."""
    import pandas as pd

    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NaT:
        return None
    if spark_type == "TIMESTAMP":
        ts = pd.Timestamp(value)
        return (
            ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        ).to_pydatetime()
    if spark_type in ("INT", "BIGINT"):
        return int(value)
    if spark_type == "BINARY":
        return bytes(value)
    return value


_INPUT_TYPES = {c: BRONZE_KAFKA_RECORDS.column(c).type for c in ROW_STAGE_INPUT}
_PANDAS_DTYPES = {"INT": "Int32", "BIGINT": "Int64", "DOUBLE": "float64", "BOOLEAN": "boolean"}


def to_pandas(rows: list[dict[str, Any]]) -> pd.DataFrame:
    import pandas as pd

    data = {}
    for name, spark_type in PREPARED_COLUMNS:
        values = [r[name] for r in rows]
        if spark_type == "TIMESTAMP":
            data[name] = pd.to_datetime(pd.Series(values, dtype="object"), utc=True)
        elif spark_type in _PANDAS_DTYPES:
            data[name] = pd.array(values, dtype=_PANDAS_DTYPES[spark_type])
        else:
            data[name] = pd.Series(values, dtype="object")
    return pd.DataFrame(data, columns=[n for n, _ in PREPARED_COLUMNS])


def row_stage(
    archive_rows: list[dict[str, Any]], reference: Reference
) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
    """mapInPandas function. The closure (archive, reference) is shipped to executors;
    writer schemas are parsed once per task, not per row."""

    def fn(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        writers = parse_writer_schemas(archive_rows)
        for pdf in batches:
            out = []
            for rec in pdf.to_dict("records"):
                row = {c: _py(rec[c], t) for c, t in _INPUT_TYPES.items()}
                out.append(prepared_row(prepare(row, writers, reference)))
            yield to_pandas(out)

    return fn


# --------------------------------------------------------------------------- set stage


def set_stage(p: DataFrame, bid: str) -> tuple[DataFrame, DataFrame]:
    """Mirror of silver.build(), step for step (numbers match its docstring)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    asc = [F.col(c).asc() for c in ARRIVAL]
    desc = [F.col(c).desc() for c in ARRIVAL]
    invalid = p.where(F.col("status") == "INVALID")  # 1
    valid = p.where(F.col("status") == "VALID")

    w_eid = Window.partitionBy("event_id").orderBy(*asc)  # 2
    v = (
        valid.withColumn("_eid_rank", F.row_number().over(w_eid))
        .withColumn("_rep_fp", F.first("fingerprint").over(w_eid))
        .withColumn("_rep_partition", F.first("kafka_partition").over(w_eid))
        .withColumn("_rep_offset", F.first("kafka_offset").over(w_eid))
    )
    reps = v.where("_eid_rank = 1")
    dups = v.where("_eid_rank > 1 AND fingerprint = _rep_fp")
    eid_conflicts = v.where("_eid_rank > 1 AND fingerprint <> _rep_fp")

    w_slot = Window.partitionBy("robot_id", "boot_number", "sequence_number").orderBy(*asc)  # 3
    originals = (
        reps.where(F.col("correction_of").isNull())
        .withColumn("_slot_rank", F.row_number().over(w_slot))
        .withColumn("_holder", F.first("event_id").over(w_slot))
    )
    seq_conflicts = originals.where("_slot_rank > 1")
    accepted = originals.where("_slot_rank = 1").unionByName(
        reps.where(F.col("correction_of").isNotNull()), allowMissingColumns=True
    )

    w_target = Window.partitionBy("correction_of").orderBy(*desc)  # 4
    c = (
        accepted.where(F.col("correction_of").isNotNull())
        .withColumn("_r", F.row_number().over(w_target))
        .withColumn("_winner", F.first("event_id").over(w_target))
    )
    by_target = c.where("_r = 1").select(
        F.col("correction_of").alias("_sid"), F.col("event_id").alias("_w_target")
    )
    by_sibling = c.where("_r > 1").select(
        F.col("event_id").alias("_sid"), F.col("_winner").alias("_w_sibling")
    )
    superseded = by_target.join(by_sibling, "_sid", "full_outer").select(
        F.col("_sid").alias("event_id"),
        F.coalesce("_w_target", "_w_sibling").alias("superseded_by"),
    )
    seen = (
        p.where(F.col("event_id").isNotNull())
        .select(F.col("event_id").alias("correction_of"))
        .distinct()
        .withColumn("_target_seen", F.lit(True))
    )

    key = F.struct("boot_number", "sequence_number")  # 5
    w_robot = (
        Window.partitionBy("robot_id").orderBy(*asc).rowsBetween(Window.unboundedPreceding, -1)
    )
    dup_counts = dups.groupBy("event_id").agg(F.count("*").cast("int").alias("_dups"))
    target_unknown = F.col("correction_of").isNotNull() & F.col("_target_seen").isNull()
    flag = F.array(F.lit(str(Flag.CORRECTION_TARGET_UNKNOWN)))
    events = (
        accepted.withColumn("_prior", F.max(key).over(w_robot))
        .join(seen, "correction_of", "left")
        .join(superseded, "event_id", "left")
        .join(dup_counts, "event_id", "left")
        .withColumn("arrived_out_of_order", F.coalesce(F.col("_prior") > key, F.lit(False)))
        .withColumn(
            "quality_flags",
            F.array_sort(
                F.when(target_unknown, F.concat("quality_flags", flag)).otherwise(
                    F.col("quality_flags")
                )
            ),
        )  # fmt: skip
        .withColumn("is_current", F.col("superseded_by").isNull())
        .withColumn("duplicate_deliveries", F.coalesce("_dups", F.lit(0)))
        .withColumn("bronze_ingested_at", F.col("ingested_at"))
        .withColumn("silver_build_id", F.lit(bid))
        .withColumn("transform_version", F.lit(TRANSFORM_VERSION))
    )

    def outcome(df: DataFrame, kind: str, reason, errors, canonical) -> DataFrame:
        return df.select(
            "source_topic", "kafka_topic_id", "kafka_partition", "kafka_offset",
            F.lit(kind).alias("outcome"), reason.alias("reason_code"), errors.alias("errors"),
            "event_id", "robot_id", canonical.alias("canonical_event_id"),
            F.col("value_schema_id").alias("writer_schema_id"),
            F.col("ingested_at").alias("bronze_ingested_at"),
        )  # fmt: skip

    null = F.lit(None).cast("string")
    eid_err = F.expr(template_sql(EVENT_ID_CONFLICT_TEMPLATE, {
        "held_kafka_offset": "_rep_offset", "held_kafka_partition": "_rep_partition"}))  # fmt: skip
    seq_err = F.expr(template_sql(SEQUENCE_CONFLICT_TEMPLATE, {"held_event_id": "_holder"}))
    categories = F.create_map(*[F.lit(x) for k, (c, _) in REASON_TABLE.items() for x in (k, c)])
    retry = F.create_map(*[F.lit(x) for k, (_, r) in REASON_TABLE.items() for x in (k, r)])
    outcomes = (
        outcome(invalid, "REJECTED", F.col("reason_code"), F.col("errors"), null)
        .unionByName(outcome(dups, "DUPLICATE", null, null, F.col("event_id")))
        .unionByName(outcome(eid_conflicts, "REJECTED",
                             F.lit(SilverReason.EVENT_ID_CONFLICT.value), eid_err, null))
        .unionByName(outcome(seq_conflicts, "REJECTED", F.lit("SEQUENCE_CONFLICT"), seq_err, null))
        .unionByName(outcome(accepted, "ACCEPTED", null, null, F.col("event_id")))
        .join(superseded, "event_id", "left")
        .withColumn("error_category", categories[F.col("reason_code")])
        .withColumn("retry_eligible", F.coalesce(retry[F.col("reason_code")], F.lit(False)))
        .withColumn("silver_build_id", F.lit(bid))
        .withColumn("transform_version", F.lit(TRANSFORM_VERSION))
    )  # fmt: skip
    return conform(events, SILVER_TELEMETRY_EVENTS), conform(
        outcomes, SILVER_TELEMETRY_RECORD_OUTCOMES
    )


def conform(df: DataFrame, table: Table) -> DataFrame:
    from pyspark.sql import functions as F

    return df.select(*[F.col(c.name).cast(c.type).alias(c.name) for c in table.columns])


# --------------------------------------------------------------------------- driver


def reference_rows(ref: Reference, bid: str) -> tuple[list[dict], list[dict]]:
    def src(s) -> dict[str, Any]:
        return {"source_topic": s.source_topic, "kafka_topic_id": s.kafka_topic_id,
                "kafka_partition": s.kafka_partition, "kafka_offset": s.kafka_offset}  # fmt: skip

    fw = [{"model_id": c.model_id, "firmware_version": c.firmware_version,
           "battery_unit": c.battery_unit, "position_unit": c.position_unit, "cdc_op": c.cdc_op,
           **src(c.source), "silver_build_id": bid} for c in ref.firmware.values()]  # fmt: skip
    robots = [{"robot_id": r.robot_id, "robot_model": r.robot_model,
               "registered_firmware_version": r.registered_firmware_version,
               "registered_at": r.registered_at, **src(r.source), "silver_build_id": bid}
              for r in ref.robots.values()]  # fmt: skip
    return fw, robots


def write_replace(spark: SparkSession, df: DataFrame, table: Table, props: dict[str, str]) -> None:
    from pyspark.sql import functions as F

    writer = df.writeTo(fq(table))
    for k, v in props.items():
        writer = writer.option(f"snapshot-property.{k}", v)
    writer.overwrite(F.lit(True))  # all rows replaced, one atomic snapshot


def build_silver(
    spark: SparkSession,
    bronze_snapshot: int | None = None,
    fault: Callable[[str], None] = lambda stage: None,
    telemetry_topic: str = TELEMETRY_TOPIC,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    ensure_tables(spark, SILVER_TABLES)
    snap = bronze_snapshot or current_snapshot_id(spark, BRONZE_KAFKA_RECORDS)
    if snap is None:
        return {"status": "empty-bronze"}
    bronze = spark.sql(f"SELECT * FROM {fq(BRONZE_KAFKA_RECORDS)} VERSION AS OF {int(snap)}")
    archive = [r.asDict() for r in spark.table(fq(BRONZE_REGISTRY_SCHEMAS)).collect()]
    telemetry = bronze.where(F.col("source_topic") == telemetry_topic)
    ids = {r[0] for r in telemetry.where(F.col("value_framing") == CONFLUENT)
           .select("value_schema_id").distinct().collect()}  # fmt: skip
    missing = sorted(ids - {int(a["schema_id"]) for a in archive})
    if missing:
        raise SchemaArchiveIncomplete(f"schema ids {missing} are not archived; run Bronze first")

    def small(topic: str) -> list[dict[str, Any]]:
        return [
            r.asDict(recursive=True) for r in bronze.where(F.col("source_topic") == topic).collect()
        ]

    ref = build_reference(small(FIRMWARE_TOPIC), small(ROBOT_EVENTS_TOPIC))
    bid = build_id(int(snap), ref.fingerprint())
    published = snapshot_summary(spark, SILVER_TELEMETRY_EVENTS,
                                 current_snapshot_id(spark, SILVER_TELEMETRY_EVENTS))  # fmt: skip
    if published.get(BUILD_ID_PROPERTY) == bid:
        return {"status": "unchanged", "build_id": bid}

    prepared = telemetry.select(*ROW_STAGE_INPUT).mapInPandas(
        row_stage(archive, ref), PREPARED_SCHEMA
    )
    events, outcomes = set_stage(prepared, bid)
    props = {BUILD_ID_PROPERTY: bid, "fleetforge.silver.bronze-snapshot-id": str(snap),
             "fleetforge.silver.reference-fingerprint": ref.fingerprint(),
             "fleetforge.silver.transform-version": TRANSFORM_VERSION,
             "fleetforge.silver.reference-anomalies": json.dumps(
                 [[a.source.kafka_offset, a.message] for a in ref.anomalies])}  # fmt: skip
    fw, robots = reference_rows(ref, bid)
    for rows, table in ((fw, SILVER_FIRMWARE_CONTRACTS), (robots, SILVER_ROBOT_REGISTRY)):
        write_replace(spark, spark.createDataFrame(rows, table.spark_schema()), table, props)
    write_replace(spark, outcomes, SILVER_TELEMETRY_RECORD_OUTCOMES, props)
    fault("before_events")  # tests: a crash between the two table commits
    write_replace(spark, events, SILVER_TELEMETRY_EVENTS, props)  # last: publishes the build
    return {"status": "built", "build_id": bid, "bronze_snapshot": snap,
            "reference_anomalies": len(ref.anomalies),
            "finished_at": datetime.now(UTC).isoformat()}  # fmt: skip


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    lake = LakehouseSettings()
    with single_writer_lease(lake.catalog_dsn, LEASE):
        report = build_silver(session("fleetforge-silver-build", lake))
    print(json.dumps(report, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
