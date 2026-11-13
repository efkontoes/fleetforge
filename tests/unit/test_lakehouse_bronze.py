"""Bronze mapping, checkpoints and the fail-closed ingestion planner (no Spark)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse.bronze import (
    BRONZE_COLUMNS,
    BronzePositionError,
    BronzeReadError,
    KafkaRecord,
    OffsetRange,
    TopicCheckpoint,
    Watermarks,
    bronze_row,
    checkpoints_from_data,
    coordinates,
    decode_checkpoints,
    encode_checkpoints,
    framing,
    new_rows,
    plan_topic,
    range_predicate_sql,
    snapshot_properties,
    spark_offsets_json,
    verify_batch,
)

T = "telemetry.raw.v1"
ID = "TlmFixtureTopicId00001"
AT = datetime(2026, 11, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ("TOMBSTONE", None)),
        (b"", ("NONE", None)),
        (b"\x00\x00\x00\x01", ("NONE", None)),  # too short for the framing
        (b"\x00\x00\x00\x00\x07", ("CONFLUENT", 7)),  # framing alone, empty body
        (b"\x00\xff\xff\xff\xff\x02", ("CONFLUENT", 4294967295)),  # unsigned
        (b'{"event_id": 1}', ("NONE", None)),
    ],
)
def test_framing_is_read_from_bytes_only(value, expected):
    assert framing(value) == expected


def test_bronze_row_preserves_the_record_exactly():
    rec = KafkaRecord(T, 3, 41, b"robot", b"\x00\x00\x00\x00\x02abc", 1_761_984_000_123, 0,
                      (("h", b"v"), ("n", None)))  # fmt: skip
    row = bronze_row(rec, topic_id=ID, run_id="r1", ingested_at=AT)
    assert tuple(row) == BRONZE_COLUMNS
    assert (row["record_key"], row["record_value"]) == (rec.key, rec.value)
    assert row["record_headers"] == [{"key": "h", "value": b"v"}, {"key": "n", "value": None}]
    assert (row["value_framing"], row["value_schema_id"]) == ("CONFLUENT", 2)
    assert row["kafka_timestamp"] == datetime(2025, 11, 1, 8, 0, 0, 123000, tzinfo=UTC)
    assert row["kafka_timestamp_type"] == "CREATE_TIME"
    assert len(row["value_sha256"]) == 64
    assert coordinates(row) == (ID, 3, 41)


def test_replaying_a_batch_inserts_nothing():
    rows = fx.bronze_rows(fx.scenario(), (1,))
    first = new_rows([], rows)
    assert first == rows
    again = new_rows([coordinates(r) for r in first], fx.bronze_rows(fx.scenario(), (1,)))
    assert again == []
    assert new_rows([], rows + rows) == rows  # duplicates inside one batch, too


def test_checkpoint_round_trip_is_canonical():
    cps = {T: TopicCheckpoint(T, ID, {1: 5, 0: 10})}
    text = encode_checkpoints(cps)
    assert decode_checkpoints(text) == cps
    assert text == encode_checkpoints(decode_checkpoints(text))
    assert decode_checkpoints(None) == {}


def test_checkpoint_from_data_uses_the_latest_incarnation():
    later = datetime(2026, 11, 2, tzinfo=UTC)
    rows = [(T, "old", 0, 999, AT), (T, ID, 0, 9, later), (T, ID, 1, 4, later)]
    assert checkpoints_from_data(rows) == {T: TopicCheckpoint(T, ID, {0: 10, 1: 5})}


WM = {0: Watermarks(0, 100), 1: Watermarks(0, 50)}


def test_first_run_starts_at_the_earliest_retained_offset():
    plan = plan_topic(T, ID, None, {0: Watermarks(20, 100)})
    assert plan.ranges == (OffsetRange(T, ID, 0, 20, 100),)
    assert plan.next_checkpoint.next_offsets == {0: 100}
    assert any("predate ingestion" in n for n in plan.notes)


def test_normal_run_continues_from_the_checkpoint_and_caps_batches():
    cp = TopicCheckpoint(T, ID, {0: 90, 1: 50})
    plan = plan_topic(T, ID, cp, WM, max_records_per_partition=5)
    assert plan.ranges == (OffsetRange(T, ID, 0, 90, 95),)  # partition 1 is caught up
    assert plan.next_checkpoint.next_offsets == {0: 95, 1: 50}


def test_recreated_topic_is_refused_unless_accepted():
    cp = TopicCheckpoint(T, "previousIncarnation", {0: 90})
    with pytest.raises(BronzePositionError, match="recreated"):
        plan_topic(T, ID, cp, WM)
    plan = plan_topic(T, ID, cp, WM, accept_new_incarnation=True)
    assert [r.start for r in plan.ranges] == [0, 0]


def test_retention_gap_is_refused_unless_accepted_and_then_recorded():
    cp = TopicCheckpoint(T, ID, {0: 10, 1: 50})
    wm = {0: Watermarks(30, 100), 1: Watermarks(0, 50)}
    with pytest.raises(BronzePositionError, match=r"\[10, 30\) were deleted"):
        plan_topic(T, ID, cp, wm)
    plan = plan_topic(T, ID, cp, wm, accept_gaps=True)
    assert plan.accepted_gaps == ((0, 10, 30),)
    assert plan.ranges[0].start == 30
    props = snapshot_properties("run", [plan], {})
    assert "fleetforge.bronze.accepted-gaps" in props


def test_checkpoint_beyond_log_end_is_always_refused():
    with pytest.raises(BronzePositionError, match="beyond the log end"):
        plan_topic(T, ID, TopicCheckpoint(T, ID, {0: 101, 1: 0}), WM, accept_gaps=True)


def test_vanished_partition_is_refused_and_new_partition_starts_at_low():
    with pytest.raises(BronzePositionError, match="disappeared"):
        plan_topic(T, ID, TopicCheckpoint(T, ID, {0: 1, 1: 1, 2: 1}), WM)
    plan = plan_topic(T, ID, TopicCheckpoint(T, ID, {0: 100}), WM)
    assert plan.ranges == (OffsetRange(T, ID, 1, 0, 50),)


def test_verify_batch_requires_every_offset_on_contiguous_topics():
    r = [OffsetRange(T, ID, 0, 10, 20)]
    verify_batch(r, {(T, 0): (10, 10, 19)}, contiguous=True)
    with pytest.raises(BronzeReadError, match="read 9"):
        verify_batch(r, {(T, 0): (9, 10, 19)}, contiguous=True)
    verify_batch(r, {(T, 0): (9, 10, 19)}, contiguous=False)  # compacted topic: holes allowed
    with pytest.raises(BronzeReadError, match="outside"):
        verify_batch(r, {(T, 0): (1, 20, 20)}, contiguous=False)
    with pytest.raises(BronzeReadError, match="read 0"):
        verify_batch(r, {}, contiguous=True)
    with pytest.raises(BronzeReadError, match="unplanned"):
        verify_batch(r, {(T, 0): (10, 10, 19), (T, 1): (1, 0, 0)}, contiguous=True)


def test_snapshot_properties_copy_other_topics_forward():
    other = TopicCheckpoint("fleet.robot-events.v1", "RbFixtureTopicId000001", {0: 7})
    plan = plan_topic(T, ID, None, {0: Watermarks(0, 3)})
    props = snapshot_properties("run-9", [plan], {other.topic: other})
    cps = decode_checkpoints(props["fleetforge.bronze.checkpoint"])
    assert cps == {T: plan.next_checkpoint, other.topic: other}
    assert props["fleetforge.bronze.run-id"] == "run-9"


def test_range_predicate_and_offsets_json():
    ranges = [OffsetRange(T, ID, 0, 10, 20), OffsetRange(T, ID, 2, 0, 5)]
    sql = range_predicate_sql(ranges)
    assert sql.startswith("source_topic IN ('telemetry.raw.v1') AND (")
    assert "kafka_partition = 2 AND kafka_offset >= 0 AND kafka_offset < 5" in sql
    assert range_predicate_sql([]) == "FALSE"
    start, end = spark_offsets_json(ranges)
    assert (start, end) == ('{"telemetry.raw.v1": {"0": 10, "2": 0}}',
                            '{"telemetry.raw.v1": {"0": 20, "2": 5}}')  # fmt: skip


def test_topic_ids_from_librdkafka_are_accepted():
    # Seen from the Milestone 2 broker through confluent-kafka: standard base64.
    real = "0/lHs2ReR2i9LwLnxKhAcg"
    sql = range_predicate_sql([OffsetRange(T, real, 0, 0, 1)])
    assert f"kafka_topic_id = '{real}'" in sql
    assert plan_topic(T, real, None, {0: Watermarks(0, 1)}).topic_id == real


@pytest.mark.parametrize(
    ("topic", "topic_id"), [("t' OR '1'='1", ID), (T, "x' --"), ("", ID), (T, "a\\b")]
)
def test_identifiers_are_validated_before_they_reach_sql(topic, topic_id):
    with pytest.raises(ValueError):
        range_predicate_sql([OffsetRange(topic, topic_id, 0, 0, 1)])
