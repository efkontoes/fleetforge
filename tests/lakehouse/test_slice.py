"""First vertical slice on the real engines: fixture records -> Bronze (Iceberg) ->
Silver (Spark) -> Trino. Each Spark result is compared with the pure reference
implementation that the unit tests already pin down (differential testing)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse.bronze import (
    BRONZE_COLUMNS,
    CHECKPOINT_PROPERTY,
    OffsetRange,
    TopicCheckpoint,
    TopicPlan,
    bronze_row,
    decode_checkpoints,
    snapshot_properties,
)
from fleetforge.lakehouse.jobs import bronze_ingest as bj
from fleetforge.lakehouse.jobs import silver_build as sj
from fleetforge.lakehouse.jobs.common import (
    current_snapshot_id,
    ensure_tables,
    fq,
    snapshot_summary,
    snapshots,
)
from fleetforge.lakehouse.reference import FIRMWARE_TOPIC, ROBOT_EVENTS_TOPIC, build_reference
from fleetforge.lakehouse.silver import (
    EVENT_COLUMNS,
    OUTCOME_COLUMNS,
    TELEMETRY_TOPIC,
    archive_fingerprint,
    build,
    build_id,
    parse_writer_schemas,
    prepare,
)
from fleetforge.lakehouse.tables import (
    BRONZE_KAFKA_RECORDS,
    BRONZE_REGISTRY_SCHEMAS,
    SILVER_BUILD_LOG,
    SILVER_TELEMETRY_EVENTS,
    SILVER_TELEMETRY_RECORD_OUTCOMES,
)

pytestmark = pytest.mark.lakehouse
KAFKA_SOURCE_SCHEMA = (
    "topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP, timestampType INT, "
    "key BINARY, value BINARY, headers ARRAY<STRUCT<key: STRING, value: BINARY>>"
)


def _norm(v):
    """Collected Spark values -> what the pure code produces."""
    if isinstance(v, bytearray):
        return bytes(v)
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)  # session and container are UTC
    if isinstance(v, list):
        return [_norm(x) for x in v]
    if hasattr(v, "asDict"):
        return {k: _norm(x) for k, x in v.asDict().items()}
    return v


def _collect(df, columns):
    return [{c: _norm(r[c]) for c in columns} for r in df.collect()]


def _kafka_df(spark, records):
    rows = [
        (
            r.topic,
            r.partition,
            r.offset,
            datetime.fromtimestamp(r.timestamp_ms / 1000, UTC) if r.timestamp_ms else None,
            r.timestamp_type,
            r.key,
            r.value,
            [(k, v) for k, v in r.headers],
        )
        for r in records
    ]
    return spark.createDataFrame(rows, KAFKA_SOURCE_SCHEMA)


def _ingest(spark, run: int) -> int:
    """Append one scenario run through the job's own insert-if-absent path."""
    run_id, at = fx.RUNS[run]
    recs = [s.record for s in fx.scenario() if s.run == run]
    df = bj.to_bronze(_kafka_df(spark, recs), fx.TOPIC_IDS, run_id, at)
    ranges, plans = [], []
    for topic, tid in fx.TOPIC_IDS.items():
        parts: dict[int, list[int]] = {}
        for r in recs:
            if r.topic == topic:
                parts.setdefault(r.partition, []).append(r.offset)
        rs = tuple(OffsetRange(topic, tid, p, min(o), max(o) + 1) for p, o in sorted(parts.items()))
        ranges += rs
        nxt = {r.partition: r.end for r in rs}
        plans.append(TopicPlan(topic, tid, rs, TopicCheckpoint(topic, tid, nxt)))
    props = snapshot_properties(run_id, plans, {})
    return bj.append_bronze(spark, df, ranges, props)


def _ingest_reference(spark) -> int:
    """Only the firmware CDC and robot outbox records of run 1 (for Kafka-sourced tests)."""
    run_id, at = fx.RUNS[1]
    recs = [s.record for s in fx.scenario() if s.run == 1 and s.record.topic != TELEMETRY_TOPIC]
    df = bj.to_bronze(_kafka_df(spark, recs), fx.TOPIC_IDS, run_id, at)
    ranges = [OffsetRange(r.topic, fx.TOPIC_IDS[r.topic], r.partition, r.offset, r.offset + 1)
              for r in recs]  # fmt: skip
    return bj.append_bronze(spark, df, ranges, {})


def _archive(spark):
    rows = []
    for a in fx.schema_archive():
        rows.append({**a, "schema_sha256": None, "registry_url": "fixture",
                     "first_seen_topic": TELEMETRY_TOPIC, "archived_at": fx.RUNS[1][1],
                     "ingestion_run_id": "fixture"})  # fmt: skip
    df = spark.createDataFrame(rows, BRONZE_REGISTRY_SCHEMAS.spark_schema())
    df.writeTo(fq(BRONZE_REGISTRY_SCHEMAS)).append()


def reference_silver(runs):
    rows = fx.bronze_rows(fx.scenario(), runs)
    ref = build_reference([r for r in rows if r["source_topic"] == FIRMWARE_TOPIC],
                          [r for r in rows if r["source_topic"] == ROBOT_EVENTS_TOPIC])  # fmt: skip
    writers = parse_writer_schemas(fx.schema_archive())
    tel = [r for r in rows if r["source_topic"] == TELEMETRY_TOPIC]
    return ref, [prepare(r, writers, ref) for r in tel]


def _key(row):
    return (row["kafka_topic_id"], row["kafka_partition"], row["kafka_offset"])


def test_spark_bronze_columns_equal_the_python_reference(spark):
    recs = [s.record for s in fx.scenario() if s.run == 1]
    run_id, at = fx.RUNS[1]
    got = {_key(r): r for r in _collect(bj.to_bronze(_kafka_df(spark, recs), fx.TOPIC_IDS,
                                                     run_id, at), BRONZE_COLUMNS)}  # fmt: skip
    for rec in recs:
        want = bronze_row(rec, topic_id=fx.TOPIC_IDS[rec.topic], run_id=run_id, ingested_at=at)
        assert got[_key(want)] == want, rec


def test_bronze_append_is_insert_if_absent_and_carries_the_checkpoint(
    spark, clean_lake, record_property
):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    first = _ingest(spark, 1)
    assert first == len([s for s in fx.scenario() if s.run == 1])
    snap = current_snapshot_id(spark, BRONZE_KAFKA_RECORDS)
    cps = decode_checkpoints(
        snapshot_summary(spark, BRONZE_KAFKA_RECORDS, snap)[CHECKPOINT_PROPERTY]
    )
    assert cps[TELEMETRY_TOPIC].next_offsets == {0: 17, 1: 6}
    assert _ingest(spark, 1) == 0  # replay of the same ranges adds nothing
    assert spark.table(fq(BRONZE_KAFKA_RECORDS)).count() == first
    # Does Iceberg commit an empty append? Recorded, because the design depends on it
    # only through merge_checkpoints(), which covers both answers.
    record_property(
        "empty_append_created_snapshot", current_snapshot_id(spark, BRONZE_KAFKA_RECORDS) != snap
    )
    dupes = spark.sql(
        f"SELECT kafka_topic_id, kafka_partition, kafka_offset, count(*) c FROM "
        f"{fq(BRONZE_KAFKA_RECORDS)} GROUP BY 1, 2, 3 HAVING c > 1"
    ).count()
    assert dupes == 0


def test_spark_silver_equals_the_python_reference(spark, clean_lake):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    _ingest(spark, 1)
    _ingest(spark, 2)
    _archive(spark)
    snap = current_snapshot_id(spark, BRONZE_KAFKA_RECORDS)
    report = sj.build_silver(spark)
    assert report["status"] == "built"
    ref, prepared = reference_silver((1, 2))
    want = build(
        prepared,
        build_id=build_id(snap, ref.fingerprint(), archive_fingerprint(fx.schema_archive())),
    )
    assert report["build_id"] == want.build_id
    events = _collect(spark.table(fq(SILVER_TELEMETRY_EVENTS)), EVENT_COLUMNS)
    outcomes = _collect(spark.table(fq(SILVER_TELEMETRY_RECORD_OUTCOMES)), OUTCOME_COLUMNS)
    assert sorted(events, key=lambda e: e["event_id"]) == sorted(
        want.events, key=lambda e: e["event_id"]
    )
    assert sorted(outcomes, key=_key) == sorted(want.outcomes, key=_key)


def test_rebuild_with_unchanged_inputs_is_a_no_op(spark, clean_lake):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    _ingest(spark, 1)
    _archive(spark)
    first = sj.build_silver(spark)
    snap = current_snapshot_id(spark, SILVER_TELEMETRY_EVENTS)
    again = sj.build_silver(spark)
    assert again == {"status": "unchanged", "build_id": first["build_id"]}
    assert current_snapshot_id(spark, SILVER_TELEMETRY_EVENTS) == snap


def test_bronze_checkpoint_walks_past_maintenance_snapshots(spark, clean_lake):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS])
    _ingest(spark, 1)
    spark.sql(f"CALL lake.system.rewrite_data_files(table => '{BRONZE_KAFKA_RECORDS.qualified}', "
              "options => map('rewrite-all', 'true'))")  # fmt: skip
    cps = bj.read_checkpoints(spark, fx.TOPIC_IDS)
    assert cps[TELEMETRY_TOPIC].next_offsets == {0: 17, 1: 6}
    assert len(snapshots(spark, BRONZE_KAFKA_RECORDS)) >= 2


def test_spark_outcomes_match_every_scenario_expectation(spark, clean_lake):
    """Every Bronze telemetry record, checked against what the scenario says it must
    become (not against the reference implementation)."""
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    _ingest(spark, 1)
    _ingest(spark, 2)
    _archive(spark)
    sj.build_silver(spark)
    outcomes = {
        _key(o): o
        for o in _collect(spark.table(fq(SILVER_TELEMETRY_RECORD_OUTCOMES)), OUTCOME_COLUMNS)
    }
    events = {_key(e): e for e in _collect(spark.table(fq(SILVER_TELEMETRY_EVENTS)), EVENT_COLUMNS)}
    expected = {(fx.TOPIC_IDS[TELEMETRY_TOPIC], s.record.partition, s.record.offset): s.expect
                for s in fx.scenario() if s.expect is not None}  # fmt: skip
    assert set(outcomes) == set(expected)  # one outcome per Bronze record, none lost
    for key, exp in expected.items():
        o = outcomes[key]
        assert (o["outcome"], o["reason_code"]) == (exp.outcome, exp.reason), (key, exp.note)
        assert (o["superseded_by"] is not None) == exp.superseded, (key, exp.note)
        if exp.outcome == "ACCEPTED":
            e = events[key]
            assert e["arrived_out_of_order"] is exp.out_of_order, (key, exp.note)
            assert tuple(e["quality_flags"]) == exp.flags, (key, exp.note)
            assert e["is_current"] is (not exp.superseded), (key, exp.note)
        else:
            assert key not in events, (key, exp.note)


class Crash(RuntimeError):
    pass


def _crash_at(where):
    def fault(stage):
        if stage == where:
            raise Crash(stage)

    return fault


def test_bronze_crash_before_commit_restarts_without_loss_or_duplicates(spark, clean_lake):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS])
    before = current_snapshot_id(spark, BRONZE_KAFKA_RECORDS)
    original = bj.append_bronze
    try:
        bj.append_bronze = lambda *a, **k: original(*a, **k, fault=_crash_at("before_commit"))
        with pytest.raises(Crash):
            _ingest(spark, 1)
    finally:
        bj.append_bronze = original
    assert current_snapshot_id(spark, BRONZE_KAFKA_RECORDS) == before  # nothing visible
    assert spark.table(fq(BRONZE_KAFKA_RECORDS)).count() == 0
    expected = len([s for s in fx.scenario() if s.run == 1])
    assert _ingest(spark, 1) == expected  # the restart ingests everything once
    assert _ingest(spark, 1) == 0  # and a second restart adds nothing
    assert spark.table(fq(BRONZE_KAFKA_RECORDS)).count() == expected


def _append_bronze_rows(spark, rows) -> int:
    df = spark.createDataFrame(rows, BRONZE_KAFKA_RECORDS.spark_schema())
    ranges = [OffsetRange(r["source_topic"], r["kafka_topic_id"], r["kafka_partition"],
                          r["kafka_offset"], r["kafka_offset"] + 1) for r in rows]  # fmt: skip
    return bj.append_bronze(spark, df, ranges, {})


def _sync_archive(spark, archive) -> None:
    """Append archive rows the table does not hold yet, as the Bronze archiver does."""
    have = {(r[0], r[1]) for r in spark.table(fq(BRONZE_REGISTRY_SCHEMAS))
            .select("schema_id", "status").collect()}  # fmt: skip
    rows = [{**a, "schema_sha256": None, "registry_url": "fixture", "first_seen_topic":
             TELEMETRY_TOPIC, "archived_at": fx.RUNS[1][1], "ingestion_run_id": "fixture"}
            for a in archive if (a["schema_id"], a["status"]) not in have]  # fmt: skip
    if rows:
        df = spark.createDataFrame(rows, BRONZE_REGISTRY_SCHEMAS.spark_schema())
        df.writeTo(fq(BRONZE_REGISTRY_SCHEMAS)).append()


def _digest(rows, key):
    import hashlib
    import json

    lines = sorted((key(r), json.dumps({k: v for k, v in r.items() if k != "silver_build_id"},
                                       sort_keys=True, default=str)) for r in rows)  # fmt: skip
    return hashlib.sha256("\n".join(x for _, x in lines).encode()).hexdigest()


def _python_full(runs, archive):
    rows = [r for run in runs for r in run]
    ref = build_reference([r for r in rows if r["source_topic"] == FIRMWARE_TOPIC],
                          [r for r in rows if r["source_topic"] == ROBOT_EVENTS_TOPIC])  # fmt: skip
    writers = parse_writer_schemas(archive)
    tel = [r for r in rows if r["source_topic"] == TELEMETRY_TOPIC]
    return build([prepare(r, writers, ref) for r in tel], build_id="python")


def _assert_matches(spark, want):
    events = _collect(spark.table(fq(SILVER_TELEMETRY_EVENTS)), EVENT_COLUMNS)
    outcomes = _collect(spark.table(fq(SILVER_TELEMETRY_RECORD_OUTCOMES)), OUTCOME_COLUMNS)
    assert _digest(events, lambda e: e["event_id"]) == _digest(want.events, lambda e: e["event_id"])
    assert _digest(outcomes, _key) == _digest(want.outcomes, _key)


def _corpus_runs(spark, corpus, runs, fault=None):
    """Ingest corpus runs one by one and build Silver after each; return the build reports."""
    reports = []
    for n in runs:
        _append_bronze_rows(spark, fx.corpus_bronze(corpus)[n])
        _sync_archive(spark, corpus.archives[n])
        reports.append(sj.build_with_retry(spark, fault=fault or (lambda stage: None)))
    return reports


def test_spark_incremental_equals_full_on_a_random_corpus(spark, clean_lake, record_property):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    corpus = fx.random_corpus(2, robots=5, ticks=25, runs=3)
    reports = _corpus_runs(spark, corpus, range(3))
    assert [r["mode"] for r in reports] == ["FULL", "INCREMENTAL", "INCREMENTAL"]
    want = _python_full(fx.corpus_bronze(corpus), corpus.archives[-1])
    _assert_matches(spark, want)
    record_property("incremental_reports", [
        {k: r[k] for k in ("mode", "new_bronze_rows", "reprepared_rows", "scope_robots",
                           "scope_rows", "telemetry_rows")} for r in reports])  # fmt: skip
    rebuilt = sj.build_silver(spark, full=True)  # Spark full rebuild over the same Bronze
    assert rebuilt["mode"] == "FULL"
    _assert_matches(spark, want)


def test_spark_incremental_recovers_from_a_crash_after_the_prepared_merge(spark, clean_lake):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    corpus = fx.random_corpus(4, robots=4, ticks=20, runs=2)
    _corpus_runs(spark, corpus, [0])
    logged = spark.table(fq(SILVER_BUILD_LOG)).count()
    with pytest.raises(Crash):
        _corpus_runs(spark, corpus, [1], fault=_crash_at("after_prepared"))
    assert spark.table(fq(SILVER_BUILD_LOG)).count() == logged  # not published
    healed = sj.build_silver(spark)
    assert healed["mode"] == "INCREMENTAL"
    _assert_matches(spark, _python_full(fx.corpus_bronze(corpus), corpus.archives[1]))


def test_a_concurrent_events_writer_fails_validation_and_the_build_is_retried(spark, clean_lake):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    corpus = fx.random_corpus(6, robots=4, ticks=20, runs=2)
    _corpus_runs(spark, corpus, [0])
    injected = []

    def intruder(stage):  # a stale writer rewrites one event mid-build, once
        if stage == "before_events" and not injected:
            spark.sql(f"UPDATE {fq(SILVER_TELEMETRY_EVENTS)} SET duplicate_deliveries = 99 "
                      f"WHERE event_id = (SELECT min(event_id) FROM {fq(SILVER_TELEMETRY_EVENTS)})")  # fmt: skip
            injected.append(stage)

    original = sj.build_silver
    attempts = []
    sj.build_silver = lambda *a, **k: attempts.append(1) or original(*a, **k)
    try:
        report = _corpus_runs(spark, corpus, [1], fault=intruder)[0]
    finally:
        sj.build_silver = original
    assert injected and len(attempts) == 2 and report["mode"] == "INCREMENTAL"
    _assert_matches(spark, _python_full(fx.corpus_bronze(corpus), corpus.archives[1]))


def test_a_new_rule_version_forces_a_full_rebuild(spark, clean_lake, monkeypatch):
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    corpus = fx.random_corpus(8, robots=3, ticks=15, runs=1)
    _corpus_runs(spark, corpus, [0])
    monkeypatch.setattr(sj, "TRANSFORM_VERSION", "silver-telemetry/test-upgrade")
    report = sj.build_silver(spark)
    assert report["mode"] == "FULL" and report["transform_version"].endswith("test-upgrade")
    versions = {r[0] for r in spark.sql(
        f"SELECT DISTINCT transform_version FROM {fq(SILVER_TELEMETRY_EVENTS)}").collect()}  # fmt: skip
    assert versions == {"silver-telemetry/test-upgrade"}
