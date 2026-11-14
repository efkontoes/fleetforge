"""Projector crash windows and infrastructure failures on the real broker/database."""

from __future__ import annotations

import threading
import time
import uuid

import pytest
from sqlalchemy import text
from support import assert_state_matches_oracle, db_state

from fleetforge.projector.main import Projector
from fleetforge.simulator.fleet import FaultConfig, SimConfig, Telemetry, generate
from fleetforge.simulator.kafka_publisher import HttpRegistrar, publish
from fleetforge.simulator.oracle import expected

from .conftest import API, BOOTSTRAP, REGISTRY, compose, ledger_caught_up, wait_until

FAULTS = FaultConfig(duplicate_rate=0.05, reorder_rate=0.05, invalid_battery_rate=0.02,
                     missing_field_rate=0.02, reboot_rate=0.01)  # fmt: skip


class Crash(Exception):
    pass


def fleet(topic, seed, robots=8, ticks=80):
    items = generate(SimConfig(seed=seed, robots=robots, ticks=ticks, faults=FAULTS))
    r = publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=topic,
                registrar=HttpRegistrar(API))  # fmt: skip
    assert not r.failed
    return items


def check(db, items):
    exp = expected(items, transport="kafka")
    robots = {i.robot_id for i in items if isinstance(i, Telemetry)}
    assert_state_matches_oracle(db_state(db, robots), exp)


def kafka_committed(group, topic):
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group})
    try:
        tps = [TopicPartition(topic, p) for p in range(6)]
        return {tp.partition: tp.offset for tp in c.committed(tps, timeout=10)}
    finally:
        c.close()


@pytest.mark.parametrize("seek_to_ledger", [True, False], ids=["resume-from-ledger",
                                                               "trust-kafka-offsets"])  # fmt: skip
def test_crash_after_db_commit_before_offset_commit(fresh_topic, stack_settings, stack_db,
                                                    seek_to_ledger):  # fmt: skip
    items = fleet(fresh_topic, int(time.time()) % 10**6 + 10)
    group = f"crash-{uuid.uuid4().hex[:8]}"
    settings = stack_settings.model_copy(update={"projector_batch_size": 200})

    def crash_after_first_batch(stage):
        if stage == "after_db_commit":
            raise Crash("killed between the DB commit and the Kafka offset commit")

    first = Projector(settings, stack_db, topic=fresh_topic, group=group,
                      fault_hook=crash_after_first_batch)  # fmt: skip
    with pytest.raises(Crash):
        first.run(max_idle_s=10)
    with stack_db.connect() as c:
        in_ledger = c.execute(text("SELECT coalesce(sum(next_offset), 0) FROM fleet.consumer_offsets"
                                   " WHERE consumer_group = :g"), {"g": group}).scalar_one()  # fmt: skip
    assert in_ledger > 0  # the batch is durably in PostgreSQL ...
    assert all(o < 0 for o in kafka_committed(group, fresh_topic).values())  # ... not in Kafka

    second = Projector(settings, stack_db, topic=fresh_topic, group=group,
                       seek_to_ledger=seek_to_ledger)  # fmt: skip
    skipped = []
    orig = second.processor.process

    def spy(records):
        r = orig(records)
        skipped.extend(x for x in r.results if x.outcome.value == "SKIPPED")
        return r

    second.processor.process = spy
    second.run(max_idle_s=5)
    if seek_to_ledger:
        assert skipped == []  # resumed exactly where the ledger says: nothing redelivered
    else:
        assert len(skipped) == in_ledger  # Kafka redelivered the batch; the ledger absorbed it
    check(stack_db, items)


def test_consumer_terminated_mid_stream_and_restarted(fresh_topic, stack_settings, stack_db):
    items = fleet(fresh_topic, int(time.time()) % 10**6 + 20, robots=10, ticks=150)
    group = f"term-{uuid.uuid4().hex[:8]}"
    settings = stack_settings.model_copy(update={"projector_batch_size": 50})
    p = Projector(settings, stack_db, topic=fresh_topic, group=group)
    t = threading.Thread(target=p.run, kwargs={"max_idle_s": 30}, daemon=True)
    t.start()
    try:
        wait_until(lambda: p.stats.batches >= 3, 60, 0.05, "a few batches")
    finally:
        p.stop.set()  # SIGTERM path: finish the current batch, close, exit
        t.join(30)
    assert 0 < p.stats.records < sum(isinstance(i, Telemetry) for i in items)

    Projector(settings, stack_db, topic=fresh_topic, group=group).run(max_idle_s=5)
    check(stack_db, items)


@pytest.mark.disruptive
def test_database_restart_while_kafka_buffers(fresh_topic, stack_settings, stack_db,
                                              record_property):  # fmt: skip
    """PostgreSQL goes away while telemetry keeps arriving. The projector must not
    quarantine anything or advance offsets; it pauses (lag grows), retries with
    backoff, and drains the backlog after recovery. Records recovery time and drain."""
    group = f"dbdown-{uuid.uuid4().hex[:8]}"
    settings = stack_settings.model_copy(update={"projector_max_retry_backoff_s": 2.0})
    p = Projector(settings, stack_db, topic=fresh_topic, group=group)
    t = threading.Thread(target=p.run, kwargs={"max_idle_s": 25}, daemon=True)
    t.start()

    items = generate(SimConfig(seed=int(time.time()) % 10**6 + 30, robots=10, ticks=150,
                               faults=FAULTS))  # fmt: skip
    registrations = [i for i in items if not isinstance(i, Telemetry)]
    reg = HttpRegistrar(API)  # registration needs the API, so do it before the outage
    boot_numbers = {}
    for it in registrations:
        if it.kind == "robot":
            reg.robot(it)
        else:
            boot_numbers[it.boot_id] = reg.boot(it)

    compose("stop", "postgres")
    down_at = time.monotonic()
    try:
        _publish_prepared(items, fresh_topic, boot_numbers)  # Kafka accepts while DB is down
        time.sleep(5)
        assert p.stats.retries > 0
        assert p.stats.records == 0
    finally:
        compose("start", "postgres")
    up_at = time.monotonic()
    wait_until(lambda: _db_ok(stack_db), 60, msg="postgres back")
    drained = wait_until(lambda: ledger_caught_up(stack_db, group, fresh_topic), 120,
                         msg="backlog drained")  # fmt: skip
    p.stop.set()
    t.join(30)
    print(
        f"\noutage {up_at - down_at:.1f}s, retries {p.stats.retries}, "
        f"backlog {p.stats.records} records drained {drained:.1f}s after restart"
    )
    import resource

    from .conftest import high_watermarks

    record_property("outage_s", round(up_at - down_at, 1))
    record_property("backlog_events", sum(high_watermarks(fresh_topic).values()))
    record_property("retries", p.stats.retries)
    record_property("drained_after_restart_s", round(drained, 1))
    record_property("test_process_max_rss_mb",
                    round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))  # fmt: skip
    check(stack_db, items)
    with stack_db.connect() as c:
        quarantined_infra = c.execute(text(
            "SELECT count(*) FROM fleet.telemetry_rejections WHERE source_topic = :t"
            " AND reason_code NOT IN ('BATTERY_OUT_OF_RANGE', 'MALFORMED_PAYLOAD',"
            " 'STALE_SEQUENCE')"), {"t": fresh_topic}).scalar_one()  # fmt: skip
    assert quarantined_infra == 0  # an outage is never mistaken for bad data


def _publish_prepared(items, topic, boot_numbers):
    from confluent_kafka import Producer

    from fleetforge.kafka.codec import TelemetryEncoder
    from fleetforge.kafka.schemas import registry_client
    from fleetforge.simulator.kafka_publisher import kafka_value

    enc = TelemetryEncoder(registry_client(REGISTRY), topic)
    firmware = {i.robot_id: i.firmware_version for i in items if i.kind == "robot"}
    prod = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    for it in items:
        if isinstance(it, Telemetry):
            prod.produce(topic, key=it.robot_id.encode(),
                         value=kafka_value(it, enc, boot_numbers, firmware, "outage-test"))  # fmt: skip
            prod.poll(0)
    assert prod.flush(60) == 0


def _db_ok(db) -> bool:
    try:
        with db.connect() as c:
            c.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@pytest.mark.disruptive
def test_registry_outage_pauses_projection_and_never_quarantines(fresh_topic, stack_settings,
                                                                 stack_db, record_property):  # fmt: skip
    """E5: a temporary schema-registry outage is an infrastructure failure, not bad
    data. A projector with a cold schema cache must retry (no commit, no quarantine),
    then project everything once the registry is back. Quarantine rows must equal the
    oracle's exactly: only the genuinely malformed records."""
    items = fleet(fresh_topic, int(time.time()) % 10**6 + 40, robots=6, ticks=60)
    group = f"regdown-{uuid.uuid4().hex[:8]}"
    settings = stack_settings.model_copy(update={"projector_max_retry_backoff_s": 2.0})
    compose("stop", "schema-registry")
    down_at = time.monotonic()
    p = Projector(settings, stack_db, topic=fresh_topic, group=group)
    t = threading.Thread(target=p.run, kwargs={"max_idle_s": 20}, daemon=True)
    t.start()
    try:
        wait_until(lambda: p.stats.retries >= 3, 90, 0.2, "retries while the registry is down")
        assert p.stats.records == 0
        with stack_db.connect() as c:
            rows = c.execute(text("SELECT count(*) FROM fleet.telemetry_rejections"
                                  " WHERE source_topic = :t"), {"t": fresh_topic}).scalar_one()  # fmt: skip
        assert rows == 0  # nothing quarantined during the outage
    finally:
        compose("start", "schema-registry")
    up_at = time.monotonic()
    drained = wait_until(lambda: ledger_caught_up(stack_db, group, fresh_topic), 240, 1,
                         "backlog projected after the registry returns")  # fmt: skip
    p.stop.set()
    t.join(30)
    check(stack_db, items)
    exp = expected(items, transport="kafka")
    with stack_db.connect() as c:
        got = {(r[0], r[1]) for r in c.execute(text(
            "SELECT dedup_key, reason_code FROM fleet.telemetry_rejections"
            " WHERE source_topic = :t"), {"t": fresh_topic})}  # fmt: skip
    assert got == exp.rejection_rows
    record_property("registry_outage_s", round(up_at - down_at, 1))
    record_property("retries", p.stats.retries)
    record_property("projected_after_registry_back_s", round(drained, 1))


@pytest.mark.disruptive
def test_registry_restart_never_turns_valid_events_into_malformed(fresh_topic, record_property):
    """ADR 0016. Decode a valid event continuously, with a cold decoder each time (as a
    starting projector does), while the real registry restarts three times. Before the
    fix, a connection reset from confluent-kafka's httpx2 client during the restart was
    classified as bad data (reproduced: 1 permanent DecodeError in 150 decodes over 6
    restarts). A valid event may only ever be retried, never judged malformed or parked."""
    import json
    from collections import Counter

    from fleetforge.kafka.codec import (
        DecodeError,
        RegistryUnavailable,
        SchemaUnresolved,
        TelemetryDecoder,
        TelemetryEncoder,
        avro_record_from_payload,
    )
    from fleetforge.kafka.schemas import registry_client

    tel = next(i for i in generate(SimConfig(seed=11, robots=1, ticks=2))
               if isinstance(i, Telemetry))  # fmt: skip
    rec = avro_record_from_payload(json.loads(tel.body()), boot_number=1,
                                   firmware_version="1.0.0", producer_id="restart-test")  # fmt: skip
    value = TelemetryEncoder(registry_client(REGISTRY), fresh_topic).encode(rec)
    outcomes: Counter = Counter()
    for _ in range(3):
        compose("restart", "schema-registry")
        deadline, ok_in_a_row = time.monotonic() + 120, 0
        while ok_in_a_row < 10 and time.monotonic() < deadline:
            try:
                TelemetryDecoder(registry_client(REGISTRY), fresh_topic).decode(value)
                outcomes["decoded"] += 1
                ok_in_a_row += 1
            except RegistryUnavailable:
                outcomes["held"] += 1
                ok_in_a_row = 0
            except (DecodeError, SchemaUnresolved) as exc:
                outcomes[f"WRONG {type(exc).__name__}: {exc}"] += 1
            time.sleep(0.03)
        assert ok_in_a_row == 10, "registry did not come back within 120 s"
    record_property("outcomes", dict(outcomes))
    assert outcomes["held"] > 0  # the outage window was really exercised
    assert set(outcomes) == {"decoded", "held"}, outcomes
