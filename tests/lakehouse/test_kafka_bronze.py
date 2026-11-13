"""Bronze from a real Kafka broker (Milestone 2's), through Spark's Kafka batch source.

Uses a dedicated topic, created and deleted here; nothing else in the Milestone 2
stack is touched. Run with `make -f lakehouse.mk lake-test-kafka` once the
Milestone 2 core profile (Kafka, schema registry) runs without its other services.
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
