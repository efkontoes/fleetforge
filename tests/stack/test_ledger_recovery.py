"""Offset-ledger recovery against a real broker: topic recreation, a ledger ahead of
the log, and retention deleting records that were never applied. In each case the
projector must refuse to start (nothing written) until an operator records a
decision with `fleet-replay ledger-reset`, which is audited."""

from __future__ import annotations

import time
import uuid

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import text

from fleetforge.domain.errors import OffsetLedgerError
from fleetforge.kafka.retry import retry_transient
from fleetforge.projector.main import Projector
from fleetforge.projector.replay import Replayer
from fleetforge.simulator.fleet import Telemetry

from .conftest import BOOTSTRAP, high_watermarks
from .test_projector_recovery import check, fleet


def ledger(db, group):
    with db.connect() as c:
        return {r[0]: (r[1], r[2]) for r in c.execute(text(
            "SELECT partition, next_offset, kafka_topic_id FROM fleet.consumer_offsets"
            " WHERE consumer_group = :g"), {"g": group})}  # fmt: skip


def recreate(topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    def delete():
        try:
            admin.delete_topics([topic])[topic].result(30)
        except KafkaException as exc:  # a retried request whose first attempt succeeded
            if exc.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise

    retry_transient(delete)
    for _ in range(60):  # deletion completes asynchronously on the controller
        try:
            admin.create_topics([NewTopic(topic, 6, 1)])[topic].result(30)
            return
        except Exception:
            time.sleep(1)
    raise AssertionError("could not recreate topic")


def reset_all(settings, db, group, topic, to, reason):
    r = Replayer(settings.model_copy(update={"telemetry_topic": topic}), db, operator="test")
    hw = high_watermarks(topic)
    return [r.ledger_reset(group, topic, p, to(p, hw), reason, dry_run=False) for p in hw]


def test_recreated_topic_is_refused_until_an_audited_reset(fresh_topic, stack_settings,
                                                           stack_db):  # fmt: skip
    group = f"recreate-{uuid.uuid4().hex[:8]}"
    seed = int(time.time()) % 10**6
    fleet(fresh_topic, seed + 1, robots=8, ticks=120)
    p = Projector(stack_settings, stack_db, topic=fresh_topic, group=group)
    p.run(max_idle_s=5)
    old_id = p.topic_id
    before = ledger(stack_db, group)
    assert before and all(tid == old_id for _, tid in before.values())

    recreate(fresh_topic)
    items = fleet(fresh_topic, seed + 2, robots=4, ticks=40)  # fewer records than the ledger
    robots = {i.robot_id for i in items if isinstance(i, Telemetry)}
    restarted = Projector(stack_settings, stack_db, topic=fresh_topic, group=group)
    assert restarted.topic_id != old_id
    with pytest.raises(OffsetLedgerError, match="recreated"):
        restarted.run(max_idle_s=5)
    assert ledger(stack_db, group) == before
    with stack_db.connect() as c:
        n = c.execute(text("SELECT count(*) FROM fleet.robot_latest_state"
                           " WHERE robot_id::text = ANY(:r)"), {"r": list(robots)}).scalar_one()  # fmt: skip
    assert n == 0  # nothing from the new log was skipped OR applied

    out = reset_all(stack_settings, stack_db, group, fresh_topic, lambda p, hw: 0,
                    "test: topic recreated")  # fmt: skip
    # Partitions the first fleet never wrote to have no ledger row ("before" is None).
    existed = [o for o in out if o["before"] is not None]
    assert existed and all(o["before"]["topic_id"] == old_id for o in existed)
    Projector(stack_settings, stack_db, topic=fresh_topic, group=group).run(max_idle_s=5)
    check(stack_db, items)
    with stack_db.connect() as c:
        audited = c.execute(text("SELECT count(*) FROM fleet.replay_runs WHERE mode ="
                                 " 'LEDGER_RESET' AND selector->>'group' = :g"),
                            {"g": group}).scalar_one()  # fmt: skip
    assert audited == 6


def test_legacy_ledger_ahead_of_the_log_is_refused(fresh_topic, stack_settings, stack_db):
    group = f"ahead-{uuid.uuid4().hex[:8]}"
    fleet(fresh_topic, int(time.time()) % 10**6 + 3, robots=4, ticks=40)
    Projector(stack_settings, stack_db, topic=fresh_topic, group=group).run(max_idle_s=5)
    with stack_db.begin() as c:  # a pre-0008 row: no topic id to compare
        c.execute(text("UPDATE fleet.consumer_offsets SET kafka_topic_id = NULL,"
                       " next_offset = next_offset + 1000 WHERE consumer_group = :g"),
                  {"g": group})  # fmt: skip
    with pytest.raises(OffsetLedgerError, match="beyond the end of the log"):
        Projector(stack_settings, stack_db, topic=fresh_topic, group=group).run(max_idle_s=5)


def test_records_deleted_before_being_applied_are_refused(fresh_topic, stack_settings,
                                                          stack_db):  # fmt: skip
    group = f"gap-{uuid.uuid4().hex[:8]}"
    seed = int(time.time()) % 10**6
    fleet(fresh_topic, seed + 4, robots=6, ticks=60)
    Projector(stack_settings, stack_db, topic=fresh_topic, group=group).run(max_idle_s=5)
    applied = ledger(stack_db, group)
    fleet(fresh_topic, seed + 5, robots=6, ticks=60)
    hw = high_watermarks(fresh_topic)
    # Only a partition with ledger history can show a gap (one without history may
    # legitimately start wherever the log begins); pick the one with most new records.
    candidates = [p for p in applied if hw[p] - applied[p][0] >= 3]
    assert candidates, "second fleet wrote too little to an already-applied partition"
    part = max(candidates, key=lambda p: hw[p] - applied[p][0])
    cut = applied[part][0] + 3  # retention removes 3 records we never applied
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    retry_transient(lambda: [f.result(30) for f in admin.delete_records(
        [TopicPartition(fresh_topic, part, cut)]).values()])  # fmt: skip

    with pytest.raises(OffsetLedgerError, match="deleted by retention"):
        Projector(stack_settings, stack_db, topic=fresh_topic, group=group).run(max_idle_s=5)
    assert ledger(stack_db, group) == applied

    r = Replayer(stack_settings.model_copy(update={"telemetry_topic": fresh_topic}), stack_db,
                 operator="test")  # fmt: skip
    with pytest.raises(SystemExit, match="outside the current log"):
        r.ledger_reset(group, fresh_topic, part, cut - 1, "too low", dry_run=False)
    dry = r.ledger_reset(group, fresh_topic, part, cut, "test: accept loss", dry_run=True)
    assert dry["dry_run"] and ledger(stack_db, group) == applied
    r.ledger_reset(group, fresh_topic, part, cut, "test: accept loss of 3 records",
                   dry_run=False)  # fmt: skip
    Projector(stack_settings, stack_db, topic=fresh_topic, group=group).run(max_idle_s=5)
    after = ledger(stack_db, group)
    assert all(after[p][0] == hw[p] for p in hw if hw[p] > 0)


def test_reset_to_zero_reprocesses_even_if_kafka_offsets_say_otherwise(fresh_topic,
                                                                       stack_settings,
                                                                       stack_db):  # fmt: skip
    """The ledger, not Kafka's advisory committed offset, decides. After an audited
    reset to 0 every record is offered again (and absorbed: DUPLICATE/SKIPPED-free)."""
    group = f"reset0-{uuid.uuid4().hex[:8]}"
    items = fleet(fresh_topic, int(time.time()) % 10**6 + 6, robots=4, ticks=40)
    Projector(stack_settings, stack_db, topic=fresh_topic, group=group).run(max_idle_s=5)
    reset_all(stack_settings, stack_db, group, fresh_topic, lambda p, hw: 0, "test: reprocess")
    again = Projector(stack_settings, stack_db, topic=fresh_topic, group=group)
    stats = again.run(max_idle_s=5)
    assert stats.records == sum(high_watermarks(fresh_topic).values())
    check(stack_db, items)  # reprocessing is idempotent: state unchanged
