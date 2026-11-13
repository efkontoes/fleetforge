"""The pure parts of the Spark jobs, and consistency of every version pin.

The jobs themselves need Java and run in tests/lakehouse (deferred). What can be
checked without a JVM is checked here: configuration, SQL rendering, checkpoint
recovery, the row-stage contract and that all pins agree with versions.py."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse import versions
from fleetforge.lakehouse.bronze import (
    CHECKPOINT_PROPERTY,
    TopicCheckpoint,
    encode_checkpoints,
    latest_checkpoint,
    merge_checkpoints,
)
from fleetforge.lakehouse.jobs.common import lease_key, spark_conf
from fleetforge.lakehouse.reference import FIRMWARE_TOPIC, ROBOT_EVENTS_TOPIC, build_reference
from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.silver import (
    EVENT_ID_CONFLICT_TEMPLATE,
    PREPARED_COLUMNS,
    REASON_TABLE,
    SEQUENCE_CONFLICT_TEMPLATE,
    TELEMETRY_TOPIC,
    build,
    event_id_conflict_errors,
    parse_writer_schemas,
    prepare,
    prepared_row,
    sequence_conflict_errors,
    template_sql,
)
from fleetforge.lakehouse.tables import SILVER_TELEMETRY_EVENTS

ROOT = Path(__file__).resolve().parents[2]
T, ID = TELEMETRY_TOPIC, "TlmFixtureTopicId00001"


def test_spark_conf_wires_iceberg_jdbc_catalog_and_s3():
    s = LakehouseSettings(catalog_password="pw", s3_access_key="ak", s3_secret_key="sk")
    conf = spark_conf(s)
    assert conf["spark.sql.catalog.lake"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf["spark.sql.catalog.lake.catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"
    assert conf["spark.sql.catalog.lake.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf["spark.jars.packages"].split(",") == list(versions.SPARK_PACKAGES)


def test_baked_jars_replace_runtime_resolution(tmp_path):
    (tmp_path / "b.jar").touch()
    (tmp_path / "a.jar").touch()
    conf = spark_conf(LakehouseSettings(jars_dir=str(tmp_path)))
    assert "spark.jars.packages" not in conf
    assert conf["spark.jars"] == f"{tmp_path}/a.jar,{tmp_path}/b.jar"


def test_catalog_dsn_and_lease_key():
    s = LakehouseSettings(catalog_jdbc_url="jdbc:postgresql://db:6543/cat_test",
                          catalog_password="pw")  # fmt: skip
    assert s.catalog_dsn == "host=db port=6543 dbname=cat_test user=iceberg password=pw"
    assert lease_key("lake.silver") == lease_key("lake.silver") != lease_key("lake.bronze")
    assert -(2**63) <= lease_key("x") < 2**63


def _evaluate(sql: str, values: dict[str, object]) -> str:
    """Tiny evaluator for template_sql output: concat of literals and casts."""
    assert sql.startswith("concat(") and sql.endswith(")")
    out = []
    for lit, col in re.findall(r"'([^']*)'|CAST\((\w+) AS STRING\)", sql[7:-1]):
        out.append(lit if col == "" else str(values[col]))
    return "".join(out)


def test_spark_error_templates_render_exactly_the_python_strings():
    sql = template_sql(SEQUENCE_CONFLICT_TEMPLATE, {"held_event_id": "_holder"})
    assert _evaluate(sql, {"_holder": "abc"}) == sequence_conflict_errors("abc")
    sql = template_sql(
        EVENT_ID_CONFLICT_TEMPLATE,
        {"held_kafka_offset": "_rep_offset", "held_kafka_partition": "_rep_partition"},
    )
    assert _evaluate(sql, {"_rep_offset": 7, "_rep_partition": 1}) == event_id_conflict_errors(7, 1)
    with pytest.raises(ValueError):
        template_sql("it's {x}", {"x": "c"})


def test_checkpoint_survives_maintenance_snapshots_and_replays():
    cp = TopicCheckpoint(T, ID, {0: 17, 1: 6})
    history = [
        (1, None, {CHECKPOINT_PROPERTY: encode_checkpoints({T: TopicCheckpoint(T, ID, {0: 5})})}),
        (2, 1, {CHECKPOINT_PROPERTY: encode_checkpoints({T: cp})}),
        (3, 2, {"operation": "replace"}),  # compaction: no checkpoint
        (9, None, {CHECKPOINT_PROPERTY: "{}"}),  # not an ancestor of 3: ignored
    ]
    assert latest_checkpoint(history, 3) == {T: cp}
    assert latest_checkpoint(history, None) == {}
    data_ahead = {T: TopicCheckpoint(T, ID, {0: 20, 1: 6})}  # a replay committed nothing
    assert merge_checkpoints({T: cp}, data_ahead)[T].next_offsets == {0: 20, 1: 6}
    accepted_new = {T: TopicCheckpoint(T, "newIncarnation", {0: 0})}  # no rows yet
    assert merge_checkpoints(accepted_new, data_ahead) == accepted_new


def test_row_stage_output_matches_the_spark_schema_and_feeds_build():
    rows = fx.bronze_rows(fx.scenario())
    ref = build_reference([r for r in rows if r["source_topic"] == FIRMWARE_TOPIC],
                          [r for r in rows if r["source_topic"] == ROBOT_EVENTS_TOPIC])  # fmt: skip
    writers = parse_writer_schemas(fx.schema_archive())
    prepared = [prepare(r, writers, ref) for r in rows if r["source_topic"] == T]
    names = tuple(n for n, _ in PREPARED_COLUMNS)
    for p in prepared:
        flat = prepared_row(p)
        assert tuple(flat) == names
        assert (flat["status"] == "VALID") == (p.event is not None)
    emitted = {o["reason_code"] for o in build(prepared, build_id="x").outcomes} - {None}
    assert emitted <= set(REASON_TABLE)


def test_every_pin_agrees_with_versions_py():
    spark_df = (ROOT / "infrastructure/lakehouse/spark/Dockerfile").read_text()
    for pkg in versions.SPARK_PACKAGES:
        assert pkg in spark_df
    assert versions.JAVA_IMAGE in spark_df
    minio_df = (ROOT / "infrastructure/lakehouse/minio/Dockerfile").read_text()
    for pin in (versions.GO_IMAGE, versions.MINIO_COMMIT, versions.MC_COMMIT):
        assert pin in minio_df
    compose = (ROOT / "infrastructure/lakehouse/compose.yaml").read_text()
    assert f"trinodb/trino:{versions.TRINO}@sha256:" in compose
    assert f"fleetforge-minio:{versions.MINIO_RELEASE}" in compose
    extra = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"]
    assert sorted(extra["lakehouse"]) == sorted(
        f"{k}=={v}" for k, v in versions.PYTHON_PINS.items()
    )
    lock = (ROOT / "requirements-lakehouse.lock").read_text().splitlines()
    for k, v in versions.PYTHON_PINS.items():
        assert f"{k}=={v}" in lock
    ddl = "".join(SILVER_TELEMETRY_EVENTS.create_statements())
    assert f"'format-version' = '{versions.ICEBERG_FORMAT_VERSION}'" in ddl


def test_job_modules_import_without_pyspark():
    import fleetforge.lakehouse.jobs.bronze_ingest as b
    import fleetforge.lakehouse.jobs.silver_build as s

    assert b.SOURCES[T].contiguous and not b.SOURCES[FIRMWARE_TOPIC].contiguous
    assert s.ARRIVAL == ("ingested_at", "kafka_topic_id", "kafka_partition", "kafka_offset")
