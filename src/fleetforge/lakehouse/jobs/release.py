"""Training dataset releases: immutable, pinned to one Silver build, reproducible.

python -m fleetforge.lakehouse.jobs.release create --release-id r1 --seed s1
python -m fleetforge.lakehouse.jobs.release reproduce --release-id r1
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fleetforge.lakehouse import gold, ops
from fleetforge.lakehouse.jobs import exits
from fleetforge.lakehouse.jobs.common import (
    current_snapshot_id,
    ensure_tables,
    fq,
    session,
    single_writer_lease,
)
from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.silver import TRANSFORM_VERSION
from fleetforge.lakehouse.tables import (
    BRONZE_KAFKA_RECORDS,
    GOLD_ROBOT_STATE_OBSERVATIONS,
    GOLD_TRAINING_DATASET_RELEASES,
    SILVER_BUILD_LOG,
    SILVER_TELEMETRY_EVENTS,
    SILVER_TELEMETRY_RECORD_OUTCOMES,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("fleetforge.lakehouse.release")
LEASE = "gold-release"
RELEASE_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
SELECTION = {"events": "current versions only (is_current)", "exclusions": []}
KNOWN_LIMITATIONS = (
    "Synthetic fleet data. Episodes are boots until missions are specified (gap G7). "
    "Velocities are finite differences of reported positions, not measurements. "
    "No labels: the release is a candidate until learning-readiness checks pass."
)


class ReleaseError(RuntimeError):
    """A release cannot be created or reproduced as asked."""


def tag_name(release_id: str) -> str:
    return f"release-{release_id}"


def observations_df(spark: SparkSession, events_snapshot: int, release_id: str, seed: str,
                    build_id: str) -> DataFrame:  # fmt: skip
    """Spark mirror of gold.observations() over one pinned events snapshot."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    events = spark.sql(
        f"SELECT * FROM {fq(SILVER_TELEMETRY_EVENTS)} VERSION AS OF {int(events_snapshot)}"
    ).where("is_current")
    step = Window.partitionBy("boot_id").orderBy("sequence_number")
    episode = Window.partitionBy("boot_id")
    prev_seq = F.lag("sequence_number").over(step)
    dt = (F.unix_micros("observed_at") - F.unix_micros(F.lag("observed_at").over(step))) / 1e6
    flag = (F.when(prev_seq.isNull(), "NO_PREVIOUS_STEP")
            .when(F.col("sequence_number") != prev_seq + 1, "SEQUENCE_GAP")
            .when(dt <= 0, "NON_POSITIVE_DT"))  # fmt: skip
    valid_dt = F.when(flag.isNull(), dt)
    gold_flags = F.when(flag.isNotNull(), F.array(flag)).otherwise(F.array().cast("array<string>"))

    def split(key: str):
        bits = F.conv(F.substring(F.sha2(F.concat(F.lit(f"{seed}:"), F.col(key)), 256), 1, 8),
                      16, 10).cast("bigint")  # fmt: skip
        fraction = bits / float(2**32)
        (a, upper_a), (b, upper_b), (c, _) = gold.SPLITS
        return F.when(fraction < upper_a, a).when(fraction < upper_b, b).otherwise(c)

    df = events.select(
        F.lit(release_id).alias("release_id"),
        F.col("event_id").alias("observation_id"),
        "robot_id", "robot_model", "firmware_version",
        F.col("boot_id").alias("episode_id"),
        "boot_number", "sequence_number",
        (F.col("sequence_number") - F.min("sequence_number").over(episode)).alias("step_index"),
        "observed_at", "battery_pct", "x_m", "y_m", "heading_rad", "motor_temp_c", "lidar_ok",
        valid_dt.alias("dt_prev_s"),
        ((F.col("x_m") - F.lag("x_m").over(step)) / valid_dt).alias("vx_mps"),
        ((F.col("y_m") - F.lag("y_m").over(step)) / valid_dt).alias("vy_mps"),
        split("boot_id").alias("split"),
        split("robot_id").alias("split_robot"),
        F.array_sort(F.array_distinct(F.concat("quality_flags", gold_flags)))
        .alias("quality_flags"),
        "source_topic", "kafka_topic_id", "kafka_partition", "kafka_offset",
        F.lit(build_id).alias("silver_build_id"),
    )  # fmt: skip
    return df.select(*gold.OBSERVATION_COLUMNS)


def hash_rows(df: DataFrame) -> tuple[str, dict[str, int]]:
    """Content hash (gold.content_hash) and rows per split, streamed in observation_id order."""
    counts = {name: 0 for name, _ in gold.SPLITS}

    def rows():
        for r in df.orderBy("observation_id").toLocalIterator():
            counts[r["split"]] += 1
            yield r.asDict()

    digest = gold.content_hash(rows(), presorted=True)
    return digest, counts


def _release_row(spark: SparkSession, release_id: str) -> dict[str, Any] | None:
    found = (spark.table(fq(GOLD_TRAINING_DATASET_RELEASES))
             .where(f"release_id = '{release_id}'").collect())  # fmt: skip
    return found[0].asDict() if found else None


def _build(spark: SparkSession, build_id: str | None) -> dict[str, Any]:
    log_rows = spark.table(fq(SILVER_BUILD_LOG))
    if build_id:
        log_rows = log_rows.where(f"build_id = '{build_id}'")
    found = log_rows.orderBy("committed_at", ascending=False).limit(1).collect()
    if not found:
        raise ReleaseError(f"no published Silver build {build_id or ''}".strip())
    return found[0].asDict()


def create_release(spark: SparkSession, release_id: str, seed: str,
                   build_id: str | None = None) -> dict[str, Any]:  # fmt: skip
    """Write the observations of one Silver build, tag every input snapshot, then publish."""
    if not RELEASE_ID.match(release_id):
        raise ReleaseError(f"release id {release_id!r} must match {RELEASE_ID.pattern}")
    ensure_tables(spark, [GOLD_ROBOT_STATE_OBSERVATIONS, GOLD_TRAINING_DATASET_RELEASES])
    existing = _release_row(spark, release_id)
    if existing is not None:
        if existing["split_seed"] == seed and build_id in (None, existing["silver_build_id"]):
            return {"status": "exists", **existing}  # a retried task finds its own release
        raise ReleaseError(f"release {release_id} exists with other inputs; releases are immutable")
    build = _build(spark, build_id)
    observations = fq(GOLD_ROBOT_STATE_OBSERVATIONS)
    # Rows without a release row were left by a crash before publishing.
    spark.sql(f"DELETE FROM {observations} WHERE release_id = '{release_id}'")

    df = observations_df(spark, build["events_snapshot_id"], release_id, seed, build["build_id"])
    df.writeTo(observations).append()
    obs_snapshot = current_snapshot_id(spark, GOLD_ROBOT_STATE_OBSERVATIONS)
    stored = spark.sql(f"SELECT * FROM {observations} VERSION AS OF {obs_snapshot}")
    digest, counts = hash_rows(stored.where(f"release_id = '{release_id}'"))

    pins = ((GOLD_ROBOT_STATE_OBSERVATIONS, obs_snapshot),
            (SILVER_TELEMETRY_EVENTS, build["events_snapshot_id"]),
            (SILVER_TELEMETRY_RECORD_OUTCOMES, build["outcomes_snapshot_id"]),
            (BRONZE_KAFKA_RECORDS, build["bronze_snapshot_id"]))  # fmt: skip
    for table, snap in pins:
        spark.sql(f"ALTER TABLE {fq(table)} CREATE TAG IF NOT EXISTS `{tag_name(release_id)}` "
                  f"AS OF VERSION {int(snap)}")  # fmt: skip

    row = {
        "release_id": release_id, "created_at": datetime.now(UTC),
        "silver_build_id": build["build_id"],
        "silver_events_snapshot_id": build["events_snapshot_id"],
        "silver_outcomes_snapshot_id": build["outcomes_snapshot_id"],
        "bronze_snapshot_id": build["bronze_snapshot_id"],
        "observations_snapshot_id": obs_snapshot, "transform_version": TRANSFORM_VERSION,
        "feature_schema_version": gold.FEATURE_SCHEMA_VERSION, "split_seed": seed,
        "selection": json.dumps(SELECTION, sort_keys=True), "rows_train": counts["train"],
        "rows_validation": counts["validation"], "rows_test": counts["test"],
        "rows_total": sum(counts.values()), "content_sha256": digest, "readiness": "CANDIDATE",
        "known_limitations": KNOWN_LIMITATIONS,
    }  # fmt: skip
    spark.createDataFrame([row], GOLD_TRAINING_DATASET_RELEASES.spark_schema()).writeTo(
        fq(GOLD_TRAINING_DATASET_RELEASES)
    ).append()
    return {"status": "created", **row}


def reproduce_release(spark: SparkSession, release_id: str) -> dict[str, Any]:
    """Recompute a release from its pinned Silver snapshot and compare all three hashes."""
    release = _release_row(spark, release_id)
    if release is None:
        raise ReleaseError(f"no release {release_id}")
    if release["feature_schema_version"] != gold.FEATURE_SCHEMA_VERSION:
        raise ReleaseError(f"release {release_id} uses {release['feature_schema_version']}, "
                           f"this code computes {gold.FEATURE_SCHEMA_VERSION}")  # fmt: skip
    fresh = observations_df(spark, release["silver_events_snapshot_id"], release_id,
                            release["split_seed"], release["silver_build_id"])  # fmt: skip
    recomputed, _ = hash_rows(fresh)
    stored_df = spark.sql(f"SELECT * FROM {fq(GOLD_ROBOT_STATE_OBSERVATIONS)} "
                          f"VERSION AS OF '{tag_name(release_id)}'")  # fmt: skip
    stored, _ = hash_rows(stored_df.where(f"release_id = '{release_id}'"))
    recorded = release["content_sha256"]
    return {"release_id": release_id, "recorded": recorded, "stored": stored,
            "recomputed": recomputed, "match": recorded == stored == recomputed}  # fmt: skip


def main(argv: list[str] | None = None) -> tuple[int, dict[str, Any]]:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="fleet-lake-release")
    ap.add_argument("action", choices=["create", "reproduce"])
    ap.add_argument("--release-id", required=True)
    ap.add_argument("--seed", help="split seed (create)")
    ap.add_argument("--silver-build", help="Silver build id (default: the newest)")
    args = ap.parse_args(argv)
    lake = LakehouseSettings()
    spark = session("fleetforge-release", lake)
    if args.action == "create":
        if not args.seed:
            ap.error("create needs --seed")
        with single_writer_lease(lake.catalog_dsn, LEASE):
            report = create_release(spark, args.release_id, args.seed, args.silver_build or None)
    else:
        report = reproduce_release(spark, args.release_id)
    if report.get("status") == "created":
        report["lineage"] = {
            "inputs": [ops.table(SILVER_TELEMETRY_EVENTS.qualified,
                                 report["silver_events_snapshot_id"])],
            "outputs": [ops.table(GOLD_ROBOT_STATE_OBSERVATIONS.qualified,
                                  report["observations_snapshot_id"])],
        }  # fmt: skip
    return (0 if report.get("match", True) else exits.PERMANENT), report


if __name__ == "__main__":
    exits.run(main)
