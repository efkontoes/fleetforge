"""Real broker, real registry, real projector process(es)."""

from __future__ import annotations

import threading
import time
import uuid
from collections import Counter

import pytest
from confluent_kafka import Producer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from sqlalchemy import text
from support import assert_state_matches_oracle, db_state, robot_rejection_rows

from fleetforge.kafka.codec import TelemetryEncoder, avro_record_from_payload
from fleetforge.kafka.schemas import is_compatible, register, registry_client, subject_for
from fleetforge.projector.main import Projector
from fleetforge.simulator.fleet import FaultConfig, SimConfig, Telemetry, generate
from fleetforge.simulator.kafka_publisher import HttpRegistrar, publish
from fleetforge.simulator.oracle import expected

from .conftest import API, BOOTSTRAP, REGISTRY, compose, ledger_caught_up, wait_until

FAULTS = FaultConfig(duplicate_rate=0.06, reorder_rate=0.06, invalid_battery_rate=0.03,
                     missing_field_rate=0.03, reboot_rate=0.01)  # fmt: skip


def run_projector(settings, db, topic, group, **kw):
    """Run an in-process projector on a dedicated topic until it has been idle for 4 s."""
    p = Projector(settings, db, topic=topic, group=group, **kw)
    return p, p.run(max_idle_s=4.0)


def test_compose_projector_processes_published_fleet_like_the_oracle(stack_db):
    """Simulator -> real Kafka -> the projector *container* -> PostgreSQL."""
    items = generate(SimConfig(seed=int(time.time()) % 10**6, robots=15, ticks=60, faults=FAULTS))
    report = publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic="telemetry.raw.v1",
                     registrar=HttpRegistrar(API))  # fmt: skip
    assert not report.failed and report.delivered == report.produced
    wait_until(lambda: ledger_caught_up(stack_db, "telemetry-projector", "telemetry.raw.v1"),
               120, msg="projector container to catch up")  # fmt: skip

    exp = expected(items, transport="kafka")
    robots = {i.robot_id for i in items if isinstance(i, Telemetry)}
    assert_state_matches_oracle(db_state(stack_db, robots), exp)
    assert robot_rejection_rows(stack_db, items) == exp.rejection_rows


def test_schema_evolution_on_the_real_registry(fresh_topic, stack_settings, stack_db):
    client = registry_client(REGISTRY)
    subject = subject_for(fresh_topic)
    # 1. v1 is registered (fixture). 2. v2 adds an optional field: BACKWARD compatible.
    assert is_compatible(client, subject, 2)
    v2_id = register(client, subject, 2)
    # 3. v3 changes a type: rejected by the registry itself (HTTP 409).
    assert not is_compatible(client, subject, 3)
    with pytest.raises(SchemaRegistryError) as exc:
        register(client, subject, 3)
    assert exc.value.http_status_code == 409

    # 4. A v2 producer and a malformed producer share a partition with valid v1 traffic.
    robot, boot = str(uuid.uuid4()), str(uuid.uuid4())
    reg = HttpRegistrar(API)
    from fleetforge.simulator.fleet import RegisterBoot, RegisterRobot

    reg.robot(RegisterRobot(robot, "AMR-100", "1.0.0"))
    boot_number = reg.boot(RegisterBoot(robot, boot, reboot=False))
    enc_v1 = TelemetryEncoder(client, fresh_topic, revision=1)
    enc_v2 = TelemetryEncoder(client, fresh_topic, revision=2)

    def rec(seq, **extra):
        p = {"schema_version": 1, "event_id": str(uuid.uuid4()), "robot_id": robot, "boot_id": boot,
             "sequence_number": seq, "observed_at": "2026-01-01T00:00:00+00:00",
             "battery_pct": 70.0, "x_m": float(seq), "y_m": 0.0}  # fmt: skip
        r = avro_record_from_payload(p, boot_number=boot_number, firmware_version="1.0.0",
                                     producer_id="evolution-test")  # fmt: skip
        r.update(extra)
        return r

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    # Unique garbage per run: undecodable records are deduplicated by a hash of their
    # bytes, so identical garbage from an earlier run would (correctly) not add a row.
    tag = uuid.uuid4().hex.encode()
    values = [enc_v1.encode(rec(1)), b"\x00\x00\x00\x99garbage-" + tag, b"not avro at all " + tag,
              enc_v2.encode(rec(2, heading_rad=1.57)), enc_v1.encode(rec(3))]  # fmt: skip
    for v in values:
        producer.produce(fresh_topic, key=robot.encode(), value=v)
    assert producer.flush(30) == 0
    assert int.from_bytes(values[3][1:5], "big") == v2_id  # really written with v2

    group = f"evo-{uuid.uuid4().hex[:8]}"
    p, stats = run_projector(stack_settings, stack_db, fresh_topic, group)
    assert stats.records == 5
    with stack_db.connect() as c:
        seq = c.execute(text("SELECT sequence_number FROM fleet.robot_latest_state"
                             " WHERE robot_id = :r"), {"r": robot}).scalar_one()  # fmt: skip
        bad = c.execute(text("SELECT count(*) FROM fleet.telemetry_rejections WHERE"
                             " source_topic = :t AND reason_code = 'MALFORMED_PAYLOAD'"),
                        {"t": fresh_topic}).scalar_one()  # fmt: skip
    assert seq == 3  # v2 event applied by a v1-era consumer; later v1 event applied too
    assert bad == 2  # both malformed records quarantined, partition kept moving


def test_producer_survives_broker_interruption(fresh_topic, stack_settings, stack_db):
    """Kafka is stopped mid-publish. The idempotent producer buffers and retries;
    every message is acknowledged exactly once; the projector result matches the oracle.
    Publishing is paced so that production provably overlaps the outage."""
    items = generate(SimConfig(seed=int(time.time()) % 10**6 + 1, robots=10, ticks=200,
                               faults=FAULTS))  # fmt: skip
    ack_times: list[float] = []
    outage = {}

    def interrupt():
        wait_until(lambda: len(ack_times) > 200, 60, 0.05, "first acknowledgements")
        outage["start"] = time.monotonic()
        compose("stop", "kafka")
        outage["stopped"] = time.monotonic()
        time.sleep(8)
        compose("start", "kafka")
        outage["end"] = time.monotonic()

    t = threading.Thread(target=interrupt)
    t.start()
    report = publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=fresh_topic,
                     registrar=HttpRegistrar(API), pace_eps=150,
                     on_delivery=lambda _i: ack_times.append(time.monotonic()))  # fmt: skip
    t.join()

    assert not report.failed
    assert report.delivered == report.produced == len(ack_times)
    # Proof of overlap: production continued through the outage, and messages produced
    # while the broker was down were acknowledged only after it came back.
    assert report.elapsed_s > outage["end"] - outage["start"]
    assert any(a > outage["end"] for a in ack_times)
    assert not any(outage["stopped"] < a < outage["end"] - 0.5 for a in ack_times)
    gap = max(b - a for a, b in zip(ack_times, ack_times[1:], strict=False))
    assert gap >= 8  # no acknowledgement for the whole outage

    wait_until(_broker_up, 60, msg="broker back")
    group = f"outage-{uuid.uuid4().hex[:8]}"
    run_projector(stack_settings, stack_db, fresh_topic, group)
    exp = expected(items, transport="kafka")
    robots = {i.robot_id for i in items if isinstance(i, Telemetry)}
    assert_state_matches_oracle(db_state(stack_db, robots), exp)


def test_producer_restart_resends_from_checkpoint_and_state_stays_correct(
    fresh_topic, stack_settings, stack_db
):
    """A producer 'crashes' (queue purged) and restarts from its last acknowledged
    checkpoint. Kafka cannot deduplicate across producer sessions, so some events
    arrive twice; the projector's idempotency absorbs them."""
    items = generate(
        SimConfig(seed=int(time.time()) % 10**6 + 2, robots=8, ticks=80, faults=FAULTS)
    )
    first = publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=fresh_topic,
                    registrar=HttpRegistrar(API), crash_after=300,
                    producer_conf={"linger.ms": 200})  # fmt: skip
    resume_from = first.checkpoint + 1
    second = publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=fresh_topic,
                     registrar=HttpRegistrar(API), start_index=resume_from)  # fmt: skip
    assert not second.failed

    group = f"restart-{uuid.uuid4().hex[:8]}"
    _, stats = run_projector(stack_settings, stack_db, fresh_topic, group)
    telemetry = sum(isinstance(i, Telemetry) for i in items)
    # Everything was delivered at least once; whatever was acked after the checkpoint
    # but before the crash was delivered twice.
    assert stats.records == first.delivered + second.delivered >= telemetry
    exp = expected(items, transport="kafka")
    robots = {i.robot_id for i in items if isinstance(i, Telemetry)}
    assert_state_matches_oracle(db_state(stack_db, robots), exp)


def _broker_up() -> bool:
    from confluent_kafka.admin import AdminClient

    try:
        AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=3)
        return True
    except Exception:
        return False


def test_outcome_mix_is_visible_in_projector_counts(fresh_topic, stack_settings, stack_db):
    items = generate(
        SimConfig(seed=int(time.time()) % 10**6 + 3, robots=6, ticks=50, faults=FAULTS)
    )
    publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=fresh_topic,
            registrar=HttpRegistrar(API))  # fmt: skip
    p = Projector(stack_settings, stack_db, topic=fresh_topic, group=f"mix-{uuid.uuid4().hex[:8]}")
    outcomes: Counter = Counter()
    orig = p.processor.process

    def counting(records):
        r = orig(records)
        outcomes.update(r.counts())
        return r

    p.processor.process = counting
    p.run(max_idle_s=4.0)
    assert outcomes == expected(items, transport="kafka").outcomes


def _paced_producer(items, topic, path, conf):
    """Child process: publish paced, appending each acknowledged index to `path`."""
    with open(path, "a", buffering=1) as f:
        publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=topic,
                registrar=HttpRegistrar(API), pace_eps=400, producer_conf=conf,
                on_delivery=lambda i: f.write(f"{i}\n"))  # fmt: skip


def test_sigkilled_producer_loses_only_unacknowledged_events(fresh_topic, stack_settings,
                                                             stack_db, tmp_path,
                                                             record_property):  # fmt: skip
    """E3: durability starts at the broker acknowledgement. A producer process killed
    with SIGKILL loses what was still in its in-memory queue; whatever it saw
    acknowledged is in Kafka. Resending from the last contiguous acknowledged index
    (the checkpoint a real device would persist) restores the oracle state."""
    import multiprocessing as mp
    import os
    import signal

    from .conftest import high_watermarks

    items = generate(SimConfig(seed=int(time.time()) % 10**6 + 7, robots=10, ticks=150,
                               faults=FAULTS))  # fmt: skip
    acks = tmp_path / "acks.txt"
    child = mp.get_context("fork").Process(target=_paced_producer, args=(
        items, fresh_topic, acks, {"linger.ms": 500}))  # fmt: skip
    child.start()
    wait_until(lambda: acks.exists() and len(acks.read_text().split()) > 300, 60, 0.05)
    os.kill(child.pid, signal.SIGKILL)
    child.join(10)
    acked = {int(x) for x in acks.read_text().split()}
    in_kafka = sum(high_watermarks(fresh_topic).values())
    telemetry_idx = [i for i, it in enumerate(items) if isinstance(it, Telemetry)]
    acked_telemetry = len(acked & set(telemetry_idx))
    assert acked_telemetry <= in_kafka < len(telemetry_idx)

    checkpoint = -1
    while checkpoint + 1 in acked or (checkpoint + 1 < len(items)
                                      and not isinstance(items[checkpoint + 1], Telemetry)):  # fmt: skip
        checkpoint += 1
    rest = publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=fresh_topic,
                   registrar=HttpRegistrar(API), start_index=checkpoint + 1)  # fmt: skip
    assert not rest.failed
    group = f"sigkill-{uuid.uuid4().hex[:8]}"
    run_projector(stack_settings, stack_db, fresh_topic, group)
    exp = expected(items, transport="kafka")
    robots = {i.robot_id for i in items if isinstance(i, Telemetry)}
    assert_state_matches_oracle(db_state(stack_db, robots), exp)
    record_property("acked_before_kill", acked_telemetry)
    record_property("in_kafka_before_resend", in_kafka)
    record_property("resent", rest.delivered)
    print(f"\nSIGKILL: {acked_telemetry} acked, {in_kafka} in Kafka; resent {rest.delivered}"
          f" from checkpoint {checkpoint + 1}; duplicates absorbed by idempotency")  # fmt: skip


@pytest.mark.disruptive
def test_outage_longer_than_message_timeout_is_reported_not_hidden(fresh_topic,
                                                                    stack_settings, stack_db,
                                                                    record_property):  # fmt: skip
    """E3: retries are bounded by message.timeout.ms. An outage longer than that turns
    queued messages into explicit delivery FAILURES (never silent loss), and the
    checkpoint tells the producer what to resend."""
    items = generate(SimConfig(seed=int(time.time()) % 10**6 + 8, robots=10, ticks=120,
                               faults=FAULTS))  # fmt: skip
    acks: list[int] = []

    def interrupt():
        wait_until(lambda: len(acks) > 100, 60, 0.05, "first acknowledgements")
        compose("stop", "kafka")
        time.sleep(10)
        compose("start", "kafka")

    t = threading.Thread(target=interrupt)
    t.start()
    first = publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=fresh_topic,
                    registrar=HttpRegistrar(API), pace_eps=150,
                    producer_conf={"message.timeout.ms": 3000}, on_delivery=acks.append)  # fmt: skip
    t.join()
    assert first.failed and first.delivered + len(first.failed) == first.produced
    wait_until(_broker_up, 60, msg="broker back")
    rest = publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=fresh_topic,
                   registrar=HttpRegistrar(API), start_index=first.checkpoint + 1)  # fmt: skip
    assert not rest.failed
    run_projector(stack_settings, stack_db, fresh_topic, f"timeout-{uuid.uuid4().hex[:8]}")
    exp = expected(items, transport="kafka")
    robots = {i.robot_id for i in items if isinstance(i, Telemetry)}
    assert_state_matches_oracle(db_state(stack_db, robots), exp)
    record_property("delivery_failures_reported", len(first.failed))
    record_property("resent_from_checkpoint", rest.delivered)
