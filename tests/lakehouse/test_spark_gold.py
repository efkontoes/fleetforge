"""Gold on Spark: dataset releases, maintenance and reconciliation, against the references.

The last test leaves the scenario lake (two Silver builds and a release) for the Trino
phase, where dbt builds the analytical Gold tables over it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath

import pytest

from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse import gold
from fleetforge.lakehouse.jobs import maintenance as mt
from fleetforge.lakehouse.jobs import reconcile as rc
from fleetforge.lakehouse.jobs import release as rel
from fleetforge.lakehouse.jobs import silver_build as sj
from fleetforge.lakehouse.jobs.common import current_snapshot_id, ensure_tables, fq, object_store
from fleetforge.lakehouse.tables import (
    BRONZE_KAFKA_RECORDS,
    BRONZE_REGISTRY_SCHEMAS,
    GOLD_ROBOT_STATE_OBSERVATIONS,
    GOLD_TRAINING_DATASET_RELEASES,
    SILVER_TELEMETRY_EVENTS,
    SILVER_TELEMETRY_RECORD_OUTCOMES,
)

from .test_slice import Crash, _archive, _corpus_runs, _ingest, _python_full

pytestmark = pytest.mark.lakehouse
LATER = datetime.now(UTC) + timedelta(days=30)


def _failed(rows):
    return [(r["check_name"], r["expected"], r["actual"]) for r in rows if not r["passed"]]


def test_a_release_equals_the_reference_and_survives_newer_builds_and_expiry(spark, clean_lake):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    corpus = fx.random_corpus(4, robots=4, ticks=20, runs=3)
    _corpus_runs(spark, corpus, [0, 1])
    made = rel.create_release(spark, "r1", "seed-a")
    events = _python_full(fx.corpus_bronze(corpus)[:2], corpus.archives[1]).events
    want = gold.observations(events, "r1", "seed-a", made["silver_build_id"])
    assert made["status"] == "created" and made["rows_total"] == len(want)
    assert made["content_sha256"] == gold.content_hash(want)

    _corpus_runs(spark, corpus, [2])  # Silver moves on
    assert rel.reproduce_release(spark, "r1")["match"]
    assert rel.create_release(spark, "r1", "seed-a")["status"] == "exists"
    with pytest.raises(rel.ReleaseError, match="immutable"):
        rel.create_release(spark, "r1", "seed-b")

    report = mt.maintain(spark, now=LATER, keep_days=0, retain_last=1)
    expired = {r["table_name"]: r["expired_snapshots"] for r in report}
    assert expired["silver.telemetry_events"] > 0
    assert rel.reproduce_release(spark, "r1")["match"]  # tags kept the release inputs
    _corpus_runs(spark, corpus, [2])  # the incremental base survived expiry too


def test_a_release_interrupted_before_publishing_is_redone(spark, clean_lake, monkeypatch):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    _corpus_runs(spark, fx.random_corpus(5, robots=3, ticks=15, runs=1), [0])

    def crash(df):
        raise Crash("after the observations were written")

    with monkeypatch.context() as m:
        m.setattr(rel, "hash_rows", crash)
        with pytest.raises(Crash):
            rel.create_release(spark, "r2", "s")
    assert spark.table(fq(GOLD_TRAINING_DATASET_RELEASES)).count() == 0
    made = rel.create_release(spark, "r2", "s")
    stored = spark.table(fq(GOLD_ROBOT_STATE_OBSERVATIONS)).where("release_id = 'r2'").count()
    assert stored == made["rows_total"] > 0


def test_maintenance_finds_and_removes_orphans_without_losing_rows(
    spark, clean_lake, lake_settings
):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    _corpus_runs(spark, fx.random_corpus(6, robots=3, ticks=15, runs=2), [0, 1])
    rows = spark.table(fq(SILVER_TELEMETRY_EVENTS)).count()
    data_file = spark.sql(f"SELECT file_path FROM {fq(SILVER_TELEMETRY_EVENTS)}.data_files LIMIT 1")
    orphan = str(
        PurePosixPath(data_file.collect()[0][0].removeprefix("s3://")).parent / "x.parquet"
    )
    store = object_store(lake_settings)
    with store.open_output_stream(orphan) as f:
        f.write(b"left behind by a crashed writer")
    found = mt.maintain(spark, now=LATER, orphan_age=timedelta(0))  # spark.testing allows it
    events = next(r for r in found if r["table_name"] == "silver.telemetry_events")
    assert events["orphan_files"] == 1
    mt.maintain(spark, now=LATER, orphan_age=timedelta(0), delete_orphans=True)
    assert store.get_file_info(orphan).type.name == "NotFound"
    assert spark.table(fq(SILVER_TELEMETRY_EVENTS)).count() == rows
    assert not _failed(rc.reconcile(spark))


def test_reconciliation_passes_and_catches_a_write_outside_a_build(spark, clean_lake):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    _corpus_runs(spark, fx.random_corpus(7, robots=3, ticks=15, runs=2), [0, 1])
    rows = rc.reconcile(spark)
    names = {r["check_name"] for r in rows}
    assert {"bronze_equals_outcomes", "events_equal_accepted", "event_ids_unique"} <= names
    assert not _failed(rows)
    spark.sql(f"DELETE FROM {fq(SILVER_TELEMETRY_RECORD_OUTCOMES)} WHERE kafka_offset = 0")
    failed = [name for name, _, _ in _failed(rc.reconcile(spark))]
    assert failed == ["unchanged_since_build:telemetry_record_outcomes"]


def test_late_batch_creates_a_new_version_and_the_old_one_stays_readable(spark, clean_lake):
    """Runs last on purpose: it leaves two Silver versions and a release for the Trino phase."""
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    _ingest(spark, 1)
    _archive(spark)
    v1 = sj.build_silver(spark)
    v1_snap = current_snapshot_id(spark, SILVER_TELEMETRY_EVENTS)
    v1_count = spark.table(fq(SILVER_TELEMETRY_EVENTS)).count()
    _ingest(spark, 2)
    v2 = sj.build_silver(spark)
    assert v2["build_id"] != v1["build_id"]
    assert spark.table(fq(SILVER_TELEMETRY_EVENTS)).count() == v1_count + 5
    old = spark.sql(f"SELECT count(*) FROM {fq(SILVER_TELEMETRY_EVENTS)} VERSION AS OF {v1_snap}")
    assert old.collect()[0][0] == v1_count
    assert rel.create_release(spark, "scenario-1", "seed-1")["rows_total"] > 0
    assert not _failed(rc.reconcile(spark))
