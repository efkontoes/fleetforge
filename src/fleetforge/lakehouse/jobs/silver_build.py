"""Silver build: full or incremental, from a pinned Bronze snapshot (ADR 0014, ADR 0017).

python -m fleetforge.lakehouse.jobs.silver_build [--full]
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import sys
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fleetforge.lakehouse.bronze import CONFLUENT
from fleetforge.lakehouse.incremental import reference_changes, schema_changes
from fleetforge.lakehouse.jobs.common import (
    current_snapshot_id,
    ensure_tables,
    fq,
    session,
    single_writer_lease,
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
    archive_fingerprint,
    build_id,
    parse_writer_schemas,
    prepare,
    prepared_row,
    template_sql,
)
from fleetforge.lakehouse.tables import (
    BRONZE_KAFKA_RECORDS,
    BRONZE_REGISTRY_SCHEMAS,
    SILVER_BUILD_LOG,
    SILVER_FIRMWARE_CONTRACTS,
    SILVER_ROBOT_REGISTRY,
    SILVER_TELEMETRY_EVENTS,
    SILVER_TELEMETRY_PREPARED,
    SILVER_TELEMETRY_RECORD_OUTCOMES,
    Table,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("fleetforge.lakehouse.silver")
LEASE = "lake.silver"
ARRIVAL = ("ingested_at", "kafka_topic_id", "kafka_partition", "kafka_offset")
ROW_STAGE_INPUT = ("source_topic", "kafka_topic_id", "kafka_partition", "kafka_offset",
                   "kafka_timestamp", "record_value", "value_framing", "value_schema_id",
                   "ingested_at")  # fmt: skip
SILVER_TABLES = (SILVER_FIRMWARE_CONTRACTS, SILVER_ROBOT_REGISTRY, SILVER_TELEMETRY_PREPARED,
                 SILVER_TELEMETRY_RECORD_OUTCOMES, SILVER_TELEMETRY_EVENTS,
                 SILVER_BUILD_LOG)  # fmt: skip


# --------------------------------------------------------------------------- row stage


def _py(value: Any, spark_type: str) -> Any:
    """Convert a pandas cell back to a plain value (Arrow sends NaN ints and naive UTC times)."""
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
    """mapInPandas function; writer schemas are parsed once per task."""

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
    """Spark mirror of silver.build(), step for step."""
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

    def outcome(df: DataFrame, kind, reason, errors, canonical) -> DataFrame:
        kind = F.lit(kind) if isinstance(kind, str) else kind
        return df.select(
            "source_topic", "kafka_topic_id", "kafka_partition", "kafka_offset",
            kind.alias("outcome"), reason.alias("reason_code"), errors.alias("errors"),
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
    unresolved = categories[F.col("reason_code")] == F.lit("UNRESOLVED")
    invalid_kind = F.when(unresolved, "UNRESOLVED").otherwise("REJECTED")
    outcomes = (
        outcome(invalid, invalid_kind, F.col("reason_code"), F.col("errors"), null)
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


COORDS = ("kafka_topic_id", "kafka_partition", "kafka_offset")


def write_replace(spark: SparkSession, df: DataFrame, table: Table, props: dict[str, str]) -> None:
    from pyspark.sql import functions as F

    writer = df.writeTo(fq(table))
    for k, v in props.items():
        writer = writer.option(f"snapshot-property.{k}", v)
    writer.overwrite(F.lit(True))  # all rows replaced in one atomic snapshot


def merge_by_coords(spark: SparkSession, df: DataFrame, table: Table) -> None:
    """Upsert rows keyed by Kafka coordinates; `df` must not read a table (4.1 cannot plan it)."""
    view = f"_src_{table.name}"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{c} = s.{c}" for c in COORDS)
    spark.sql(f"MERGE INTO {fq(table)} t USING {view} s ON {on} "
              "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")  # fmt: skip


class ConcurrentWrite(RuntimeError):
    """Another writer committed to a Silver table while this build ran."""


def replace_robots(spark: SparkSession, df: DataFrame, robots: list[str], base: int | None) -> None:
    """Swap the events of `robots` in one row-level commit, refusing if someone wrote meanwhile."""
    if current_snapshot_id(spark, SILVER_TELEMETRY_EVENTS) != base:
        raise ConcurrentWrite("silver.telemetry_events changed during the build")
    df.createOrReplaceTempView("_src_events")
    listed = ", ".join("'" + r.replace("'", "''") + "'" for r in robots)
    spark.sql(f"MERGE INTO {fq(SILVER_TELEMETRY_EVENTS)} t USING _src_events s "
              "ON t.event_id = s.event_id "
              "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * "
              f"WHEN NOT MATCHED BY SOURCE AND t.robot_id IN ({listed}) THEN DELETE")  # fmt: skip


def link_keys(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    ids = df.select(F.col("event_id").alias("k"))
    return ids.union(df.select(F.col("correction_of").alias("k"))).where("k IS NOT NULL")


def scope_of(prepared: DataFrame, changed: DataFrame) -> tuple[list[str], DataFrame]:
    """Spark mirror of incremental.closure(): robots to recompute and the rows they need."""
    from pyspark.sql import functions as F

    def robots_of(df: DataFrame) -> set[str]:
        return {
            r[0] for r in df.where("robot_id IS NOT NULL").select("robot_id").distinct().collect()
        }

    robots = robots_of(changed)
    while True:
        mine = prepared.where(F.col("robot_id").isin(sorted(robots)))
        keys = link_keys(changed).union(link_keys(mine)).distinct()
        linked = prepared.join(keys, prepared.event_id == keys.k, "left_semi").union(
            prepared.join(keys, prepared.correction_of == keys.k, "left_semi"))  # fmt: skip
        grown = robots | robots_of(linked)
        if grown == robots:
            break
        robots = grown
    robotless = prepared.where("robot_id IS NULL")
    by_key = robotless.join(keys, robotless.event_id == keys.k, "left_semi")
    by_coords = robotless.join(changed.select(*COORDS), list(COORDS), "left_semi")
    scope = mine.unionByName(by_key).unionByName(by_coords.join(by_key, list(COORDS), "left_anti"))
    return sorted(robots), scope


def reference_at(bronze: DataFrame, topics: dict[str, str]) -> Reference:
    from pyspark.sql import functions as F

    def rows(topic: str) -> list[dict[str, Any]]:
        return [r.asDict(recursive=True)
                for r in bronze.where(F.col("source_topic") == topic).collect()]  # fmt: skip

    return build_reference(rows(topics["firmware"]), rows(topics["robots"]))


def archive_at(spark: SparkSession, snapshot: int | None) -> list[dict[str, Any]]:
    if snapshot is None:
        return []
    sql = f"SELECT * FROM {fq(BRONZE_REGISTRY_SCHEMAS)} VERSION AS OF {int(snapshot)}"
    return [r.asDict() for r in spark.sql(sql).collect()]


def last_build(spark: SparkSession) -> dict[str, Any] | None:
    rows = spark.sql(f"SELECT * FROM {fq(SILVER_BUILD_LOG)} ORDER BY committed_at DESC LIMIT 1")
    found = rows.collect()
    return found[0].asDict() if found else None


def incremental_build_id(base: str, snap: int, ref_fp: str, archive_fp: str) -> str:
    material = f"{base}|{snap}|{ref_fp}|{archive_fp}|{TRANSFORM_VERSION}"
    return hashlib.sha256(material.encode()).hexdigest()[:24]


def build_silver(
    spark: SparkSession,
    *,
    full: bool = False,
    bronze_snapshot: int | None = None,
    fault: Callable[[str], None] = lambda stage: None,
    telemetry_topic: str = TELEMETRY_TOPIC,
    reference_topics: dict[str, str] | None = None,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    topics = reference_topics or {"firmware": FIRMWARE_TOPIC, "robots": ROBOT_EVENTS_TOPIC}
    started = datetime.now(UTC)
    ensure_tables(spark, SILVER_TABLES)
    snap = bronze_snapshot or current_snapshot_id(spark, BRONZE_KAFKA_RECORDS)
    if snap is None:
        return {"status": "empty-bronze"}
    bronze = spark.sql(f"SELECT * FROM {fq(BRONZE_KAFKA_RECORDS)} VERSION AS OF {int(snap)}")
    telemetry = bronze.where(F.col("source_topic") == telemetry_topic)
    archive_snap = current_snapshot_id(spark, BRONZE_REGISTRY_SCHEMAS)
    archive = archive_at(spark, archive_snap)
    ref = reference_at(bronze, topics)
    ref_fp, archive_fp = ref.fingerprint(), archive_fingerprint(archive)
    base = None if full else last_build(spark)
    if base is not None and base["transform_version"] != TRANSFORM_VERSION:
        base = None  # new rules: everything is recomputed
    inputs = (snap, ref_fp, archive_fp)
    if base is not None and inputs == (base["bronze_snapshot_id"], base["reference_fingerprint"],
                                       base["archive_fingerprint"]):  # fmt: skip
        return {"status": "unchanged", "build_id": base["build_id"]}

    if base is None:
        mode, bid, reprepared = "FULL", build_id(int(snap), ref_fp, archive_fp), 0
        source, new_rows = telemetry, telemetry
    else:
        mode = "INCREMENTAL"
        bid = incremental_build_id(base["build_id"], snap, ref_fp, archive_fp)
        old_bronze = spark.sql(f"SELECT * FROM {fq(BRONZE_KAFKA_RECORDS)} "
                               f"VERSION AS OF {int(base['bronze_snapshot_id'])}")  # fmt: skip
        robots = sorted(reference_changes(reference_at(old_bronze, topics), ref))
        ids = sorted(schema_changes(archive_at(spark, base["archive_snapshot_id"]), archive))
        prepared_table = spark.table(fq(SILVER_TELEMETRY_PREPARED))
        affected = F.col("robot_id").isin(robots) | F.col("value_schema_id").isin(ids)
        redo_keys = prepared_table.where(affected).select(*COORDS)
        is_new = F.col("ingested_at") > F.lit(base["bronze_max_ingested_at"])
        new_rows = telemetry.where(is_new)
        redo = telemetry.where(~is_new).join(redo_keys, list(COORDS), "left_semi")
        reprepared = redo.count()
        source = new_rows.unionByName(redo)

    framed = source.where(F.col("value_framing") == CONFLUENT).select("value_schema_id")
    missing = sorted(
        {r[0] for r in framed.distinct().collect()} - {int(a["schema_id"]) for a in archive}
    )
    if missing:
        raise SchemaArchiveIncomplete(f"schema ids {missing} are not archived; run Bronze first")

    changed = source.select(*ROW_STAGE_INPUT).mapInPandas(row_stage(archive, ref), PREPARED_SCHEMA)
    changed = changed.localCheckpoint()  # computed once, and later writes cannot change it
    new_count = new_rows.count()
    if mode == "FULL":
        write_replace(
            spark, conform(changed, SILVER_TELEMETRY_PREPARED), SILVER_TELEMETRY_PREPARED, {}
        )
        robots, scope = None, changed
    else:
        merge_by_coords(
            spark, conform(changed, SILVER_TELEMETRY_PREPARED), SILVER_TELEMETRY_PREPARED
        )
        fault("after_prepared")
        robots, scope = scope_of(spark.table(fq(SILVER_TELEMETRY_PREPARED)), changed)
    scope = scope.localCheckpoint()
    scope_rows = scope.count()
    events, outcomes = set_stage(scope, bid)
    events_base = current_snapshot_id(spark, SILVER_TELEMETRY_EVENTS)
    if mode == "FULL":
        write_replace(spark, outcomes, SILVER_TELEMETRY_RECORD_OUTCOMES, {})
        fault("before_events")
        write_replace(spark, events, SILVER_TELEMETRY_EVENTS, {})
        scope_robots = scope.where("robot_id IS NOT NULL").select("robot_id").distinct().count()
    else:
        merge_by_coords(spark, outcomes, SILVER_TELEMETRY_RECORD_OUTCOMES)
        fault("before_events")
        if robots:
            replace_robots(spark, events, robots, events_base)
        scope_robots = len(robots)
    fw, regs = reference_rows(ref, bid)
    for rows, table in ((fw, SILVER_FIRMWARE_CONTRACTS), (regs, SILVER_ROBOT_REGISTRY)):
        write_replace(spark, spark.createDataFrame(rows, table.spark_schema()), table, {})
    fault("before_build_log")
    max_ingested = telemetry.agg(F.max("ingested_at")).collect()[0][0]
    entry = {
        "build_id": bid, "mode": mode, "base_build_id": base["build_id"] if base else None,
        "transform_version": TRANSFORM_VERSION, "bronze_snapshot_id": int(snap),
        "bronze_max_ingested_at": max_ingested, "reference_fingerprint": ref_fp,
        "archive_snapshot_id": archive_snap, "archive_fingerprint": archive_fp,
        "prepared_snapshot_id": current_snapshot_id(spark, SILVER_TELEMETRY_PREPARED),
        "outcomes_snapshot_id": current_snapshot_id(spark, SILVER_TELEMETRY_RECORD_OUTCOMES),
        "events_snapshot_id": current_snapshot_id(spark, SILVER_TELEMETRY_EVENTS),
        "new_bronze_rows": new_count, "reprepared_rows": reprepared,
        "scope_robots": scope_robots, "scope_rows": scope_rows,
        "telemetry_rows": telemetry.count(), "started_at": started,
        "committed_at": datetime.now(UTC),
    }  # fmt: skip
    row = spark.createDataFrame([entry], SILVER_BUILD_LOG.spark_schema())
    row.writeTo(fq(SILVER_BUILD_LOG)).append()
    pin_base(spark, int(snap), archive_snap)
    return {"status": "built", **{k: v for k, v in entry.items() if k != "started_at"}}


def pin_base(spark: SparkSession, bronze_snap: int, archive_snap: int | None) -> None:
    """Tag the inputs the next incremental build will read, so snapshot expiry keeps them."""
    pins = [(BRONZE_KAFKA_RECORDS, bronze_snap), (BRONZE_REGISTRY_SCHEMAS, archive_snap)]
    for table, snap in pins:
        if snap is not None:
            spark.sql(f"ALTER TABLE {fq(table)} CREATE OR REPLACE TAG `{BASE_TAG}` "
                      f"AS OF VERSION {int(snap)}")  # fmt: skip


BASE_TAG = "silver-base"
COMMIT_CONFLICTS = ("CommitFailedException", "ValidationException", "ConcurrentWrite")


def build_with_retry(spark: SparkSession, attempts: int = 3, **kwargs: Any) -> dict[str, Any]:
    """Retry a build that lost an optimistic commit race; reruns are idempotent."""
    for attempt in range(1, attempts + 1):
        try:
            return build_silver(spark, **kwargs)
        except Exception as exc:
            text = f"{type(exc).__name__}: {exc}"
            if attempt == attempts or not any(c in text for c in COMMIT_CONFLICTS):
                raise
            log.warning("silver commit conflict, retrying", extra={"attempt": attempt})
    raise AssertionError("unreachable")


def main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="fleet-lake-silver")
    ap.add_argument("--full", action="store_true", help="rebuild everything from Bronze")
    ap.add_argument("--bronze-snapshot", type=int, help="build from this Bronze snapshot")
    args = ap.parse_args(argv)
    lake = LakehouseSettings()
    with single_writer_lease(lake.catalog_dsn, LEASE):
        report = build_with_retry(
            session("fleetforge-silver-build", lake), full=args.full,
            bronze_snapshot=args.bronze_snapshot, telemetry_topic=lake.telemetry_topic,
            reference_topics={"firmware": lake.firmware_topic, "robots": lake.robot_events_topic},
        )  # fmt: skip
    print(json.dumps(report, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
