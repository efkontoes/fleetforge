"""Bronze from a real Kafka broker (Milestone 2's), through Spark's Kafka batch source.

Uses dedicated topics and registry subjects, created here (topics are deleted again);
nothing else in the Milestone 2 stack is touched. Run with
`make -f lakehouse.mk lake-test-kafka` with only Milestone 2's kafka and
schema-registry services up.
"""

from __future__ import annotations

import time
import uuid

import pytest

from fleetforge.config import get_settings
from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse.bronze import BronzePositionError, TopicCheckpoint, plan_topic
from fleetforge.lakehouse.jobs import bronze_ingest as bj
from fleetforge.lakehouse.jobs.common import ensure_tables, fq
from fleetforge.lakehouse.silver import TELEMETRY_TOPIC
from fleetforge.lakehouse.tables import BRONZE_KAFKA_RECORDS

pytestmark = [pytest.mark.lakehouse, pytest.mark.lakehouse_kafka]


@pytest.fixture
def topic():
    from confluent_kafka.admin import AdminClient, NewTopic

    name = f"lakehouse.test.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": get_settings().kafka_bootstrap})
    admin.create_topics([NewTopic(name, 2, 1)])[name].result(30)
    yield name, admin
    admin.delete_topics([name])[name].result(30)


def _produce(name: str) -> int:
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": get_settings().kafka_bootstrap, "enable.idempotence": True})
    recs = [s.record for s in fx.scenario() if s.record.topic == TELEMETRY_TOPIC]
    for r in recs:
        p.produce(name, key=r.key, value=r.value, partition=r.partition, headers=list(r.headers))
    assert p.flush(30) == 0
    return len(recs)


def test_ingest_replay_and_recreation_against_real_kafka(spark, clean_lake, topic):
    name, admin = topic
    produced = _produce(name)
    bootstrap = get_settings().kafka_bootstrap
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS])
    tid, wms = bj.kafka_metadata(bootstrap, name)
    plan = plan_topic(name, tid, None, wms)
    rows = bj.to_bronze(bj.read_kafka(spark, bootstrap, list(plan.ranges)), {name: tid}, "k1",
                        fx.RUNS[1][1])  # fmt: skip
    from fleetforge.lakehouse.bronze import snapshot_properties, verify_batch

    verify_batch(plan.ranges, bj.observed_ranges(rows), contiguous=True)
    assert (
        bj.append_bronze(spark, rows, list(plan.ranges), snapshot_properties("k1", [plan], {}))
        == produced
    )
    # Replaying the same ranges adds nothing.
    assert bj.append_bronze(spark, rows, list(plan.ranges), {}) == 0
    assert spark.table(fq(BRONZE_KAFKA_RECORDS)).count() == produced

    # Recreate the topic under the same name: the planner must refuse.
    from confluent_kafka.admin import NewTopic

    admin.delete_topics([name])[name].result(30)
    time.sleep(2)
    admin.create_topics([NewTopic(name, 2, 1)])[name].result(30)
    new_tid, new_wms = bj.kafka_metadata(bootstrap, name)
    assert new_tid != tid
    with pytest.raises(BronzePositionError, match="recreated"):
        plan_topic(
            name, new_tid, TopicCheckpoint(name, tid, plan.next_checkpoint.next_offsets), new_wms
        )


def _produce_records(name: str, records, remap: dict[int, int]) -> None:
    """Produce scenario records to `name`, re-framed with real registry ids."""
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": get_settings().kafka_bootstrap, "enable.idempotence": True})
    for r in records:
        value = r.value
        if value and len(value) >= 5 and value[0] == 0:
            sid = int.from_bytes(value[1:5], "big")
            value = b"\x00" + remap[sid].to_bytes(4, "big") + value[5:]
        p.produce(name, key=r.key, value=value, partition=r.partition, headers=list(r.headers))
    assert p.flush(30) == 0


def _drop_subject(client, subject: str) -> None:
    client.delete_subject(subject)
    client.delete_subject(subject, permanent=True)


def _ingest_from_kafka(spark, name: str, run: int) -> int:
    """One Bronze run from real Kafka, planned from the committed checkpoint."""
    from fleetforge.lakehouse.bronze import snapshot_properties, verify_batch

    bootstrap = get_settings().kafka_bootstrap
    previous = bj.read_checkpoints(spark, [name])
    tid, wms = bj.kafka_metadata(bootstrap, name)
    plan = plan_topic(name, tid, previous.get(name), wms)
    run_id, at = fx.RUNS[run]  # fixture times: the scenario's skew rule is relative to them
    rows = bj.to_bronze(bj.read_kafka(spark, bootstrap, list(plan.ranges)), {name: tid}, run_id, at)
    verify_batch(plan.ranges, bj.observed_ranges(rows), contiguous=True)
    return bj.append_bronze(
        spark, rows, list(plan.ranges), snapshot_properties(run_id, [plan], previous)
    )


def test_kafka_avro_with_real_registry_ids_to_silver(spark, clean_lake, topic, request):
    """Kafka bytes framed with ids the real registry assigned -> Bronze (two runs, the
    second planned from the committed checkpoint) -> schema archive fetched from the
    registry (including an authoritative 404) -> Silver, checked against the scenario."""
    from fleetforge.kafka.schemas import register, registry_client, subject_for
    from fleetforge.lakehouse.jobs import silver_build as sj
    from fleetforge.lakehouse.silver import EVENT_COLUMNS, OUTCOME_COLUMNS
    from fleetforge.lakehouse.tables import (
        BRONZE_REGISTRY_SCHEMAS,
        SILVER_TELEMETRY_EVENTS,
        SILVER_TELEMETRY_RECORD_OUTCOMES,
    )

    from .test_slice import _collect, _ingest_reference

    name, _ = topic
    url = get_settings().schema_registry_url
    client = registry_client(url)
    request.addfinalizer(lambda: _drop_subject(client, subject_for(name)))
    ids = {rev: register(client, subject_for(name), rev) for rev in (1, 2)}
    missing_id = 2**31 - 7  # never assigned by a fresh-ish registry
    remap = {fx.SCHEMA_V1: ids[1], fx.SCHEMA_V2: ids[2], fx.SCHEMA_UNKNOWN: missing_id}
    ensure_tables(spark, [BRONZE_KAFKA_RECORDS, BRONZE_REGISTRY_SCHEMAS])
    _ingest_reference(spark)
    scenario = [s for s in fx.scenario() if s.record.topic == TELEMETRY_TOPIC]
    for run in (1, 2):
        _produce_records(name, [s.record for s in scenario if s.run == run], remap)
        assert _ingest_from_kafka(spark, name, run) == len([s for s in scenario if s.run == run])

    assert bj.archive_schemas(spark, "kafka-test", url, topics=[name]) == 3
    archived = {
        r["schema_id"]: r["status"] for r in spark.table(fq(BRONZE_REGISTRY_SCHEMAS)).collect()
    }
    assert archived == {ids[1]: "FOUND", ids[2]: "FOUND", missing_id: "NOT_FOUND"}
    assert bj.archive_schemas(spark, "kafka-test", url, topics=[name]) == 0  # idempotent

    assert sj.build_silver(spark, telemetry_topic=name)["status"] == "built"
    outcomes = {(o["kafka_partition"], o["kafka_offset"]): o
                for o in _collect(spark.table(fq(SILVER_TELEMETRY_RECORD_OUTCOMES)), OUTCOME_COLUMNS)}  # fmt: skip
    events = {(e["kafka_partition"], e["kafka_offset"]): e
              for e in _collect(spark.table(fq(SILVER_TELEMETRY_EVENTS)), EVENT_COLUMNS)}  # fmt: skip
    assert len(outcomes) == len(scenario)
    for s in scenario:
        key, exp = (s.record.partition, s.record.offset), s.expect
        o = outcomes[key]
        assert (o["outcome"], o["reason_code"]) == (exp.outcome, exp.reason), (key, exp.note)
        if exp.outcome == "ACCEPTED":
            assert events[key]["writer_schema_id"] in (ids[1], ids[2])
            assert events[key]["arrived_out_of_order"] is exp.out_of_order, (key, exp.note)
