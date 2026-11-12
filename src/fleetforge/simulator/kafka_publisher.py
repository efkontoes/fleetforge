"""Publish a simulated fleet stream to Kafka.

Delivery semantics, stated precisely:
  * acks=all + enable.idempotence=true: within ONE producer session, retries after a
    timeout or broker failover cannot create duplicates or reorder a partition.
  * Across producer sessions (process restart) Kafka cannot deduplicate: anything
    re-sent after a restart is a new message. That is fine because the consumer is
    idempotent on event_id / (boot, seq); we demonstrate it in the tests.
  * Bounded buffer: librdkafka queues at most `queue.buffering.max.messages`. When
    full, produce() raises BufferError and we block (poll) until space frees up:
    backpressure reaches the caller instead of memory growing without limit.
  * The buffer is in memory. If the process dies, unacknowledged messages are lost
    from the producer's point of view; the restart resumes from the last
    *acknowledged* checkpoint, which re-sends (never skips) events.
  * A message not acknowledged within message.timeout.ms is reported as FAILED and
    makes publish() raise. Nothing is dropped silently.

Kafka-mode fault mapping: Avro serialization enforces required fields at the source,
so the "missing_field" fault is published as the raw Milestone 1 JSON bytes, i.e. a
legacy/rogue producer that bypasses the schema. The projector must quarantine it
(MALFORMED_PAYLOAD) without stopping the partition.
"""

from __future__ import annotations

import os
import time
import zlib
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx
from confluent_kafka import KafkaError, Producer

from fleetforge.kafka.codec import TelemetryEncoder, avro_record_from_payload
from fleetforge.kafka.schemas import registry_client
from fleetforge.simulator.fleet import Item, RegisterBoot, RegisterRobot, Telemetry

DEFAULT_PRODUCER_CONF: dict[str, Any] = {
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "lz4",
    "linger.ms": 20,
    "batch.size": 262144,
    "queue.buffering.max.messages": 100_000,
    "queue.buffering.max.kbytes": 65_536,
    "message.timeout.ms": 300_000,  # ride out a broker outage of up to 5 minutes
    "request.timeout.ms": 30_000,
    "retry.backoff.ms": 100,
    "retry.backoff.max.ms": 2_000,
}


class Registrar(Protocol):
    def robot(self, item: RegisterRobot) -> None: ...
    def boot(self, item: RegisterBoot) -> int: ...


class HttpRegistrar:
    """Registration is owned by the Fleet API; robots call it before sending telemetry."""

    def __init__(self, api_url: str):
        self.client = httpx.Client(base_url=api_url, timeout=30.0)

    def robot(self, item: RegisterRobot) -> None:
        r = self.client.post("/v1/robots", json={"robot_id": item.robot_id,
                             "robot_model": item.robot_model,
                             "firmware_version": item.firmware_version})  # fmt: skip
        r.raise_for_status()

    def boot(self, item: RegisterBoot) -> int:
        r = self.client.post(f"/v1/robots/{item.robot_id}/boots", json={"boot_id": item.boot_id})
        r.raise_for_status()
        return r.json()["boot_number"]


class KnownRegistrar:
    """Registration already done (e.g. in bulk before a benchmark): only answers
    boot numbers, so a producer measurement contains no HTTP calls."""

    def __init__(self, boot_numbers: dict[str, int]):
        self.boot_numbers = boot_numbers

    def robot(self, item: RegisterRobot) -> None:
        return None

    def boot(self, item: RegisterBoot) -> int:
        return self.boot_numbers[item.boot_id]


def register_bulk(items: list[Item], api_url: str, threads: int = 8) -> dict[str, int]:
    """Register all robots/boots through the API, robots in parallel, each robot's
    boots in stream order (so server-assigned boot numbers follow the stream)."""
    from concurrent.futures import ThreadPoolExecutor

    by_robot: dict[str, list[Item]] = {}
    for it in items:
        if not isinstance(it, Telemetry):
            by_robot.setdefault(it.robot_id, []).append(it)

    def one(regs: list[Item]) -> dict[str, int]:
        reg, out = HttpRegistrar(api_url), {}
        for it in regs:
            if isinstance(it, RegisterRobot):
                reg.robot(it)
            else:
                out[it.boot_id] = reg.boot(it)
        return out

    numbers: dict[str, int] = {}
    with ThreadPoolExecutor(threads) as pool:
        for part in pool.map(one, by_robot.values()):
            numbers.update(part)
    return numbers


@dataclass
class PublishReport:
    produced: int = 0
    delivered: int = 0
    failed: list[str] = field(default_factory=list)
    ack_latencies_s: list[float] = field(default_factory=list)
    backpressure_waits: int = 0
    checkpoint: int = -1  # highest stream index such that it and all before it are acked
    elapsed_s: float = 0.0
    active_s: float = 0.0  # longest single producer's own publish window (no fork/pickle)
    cpu_s: float = 0.0
    faults: Counter = field(default_factory=Counter)


def sensors_for(payload: dict[str, Any]) -> dict[str, Any]:
    seq = payload.get("sequence_number", 0)
    return {"motor_temp_c": 35.0 + (seq % 40) * 0.25, "lidar_ok": seq % 97 != 0}


def kafka_value(
    item: Telemetry,
    encoder: TelemetryEncoder,
    boot_numbers: dict[str, int],
    firmware: dict[str, str],
    producer_id: str,
) -> bytes:
    if item.fault == "missing_field":
        return item.body()  # schema-bypassing legacy producer (see module docstring)
    p = item.payload
    rec = avro_record_from_payload(
        p,
        boot_number=boot_numbers[p["boot_id"]],
        firmware_version=firmware[item.robot_id],
        producer_id=producer_id,
        sensors=sensors_for(p),
    )
    return encoder.encode(rec)


def publish(
    items: list[Item],
    *,
    bootstrap: str,
    registry_url: str,
    topic: str,
    registrar: Registrar,
    producer_id: str = "fleet-sim",
    producer_conf: dict[str, Any] | None = None,
    start_index: int = 0,
    crash_after: int | None = None,
    on_delivery: Callable[[int], None] | None = None,
    pace_eps: float | None = None,
) -> PublishReport:
    """Publish items[start_index:]. `crash_after=n` simulates a process crash after n
    produce() calls: queued but unacknowledged messages are purged, not flushed."""
    conf = {**DEFAULT_PRODUCER_CONF, **(producer_conf or {}), "bootstrap.servers": bootstrap,
            "client.id": producer_id}  # fmt: skip
    producer = Producer(conf)
    encoder = TelemetryEncoder(registry_client(registry_url), topic)
    report = PublishReport()
    boot_numbers: dict[str, int] = {}
    firmware: dict[str, str] = {}
    acked: set[int] = set()
    started, cpu0 = time.perf_counter(), time.process_time()

    def make_cb(index: int, t0: float):
        def cb(err: KafkaError | None, _msg) -> None:
            if err is not None:
                report.failed.append(f"index {index}: {err.str()}")
                return
            report.delivered += 1
            report.ack_latencies_s.append(time.perf_counter() - t0)
            acked.add(index)
            if on_delivery:
                on_delivery(index)

        return cb

    for index in range(len(items)):
        item = items[index]
        if index < start_index and isinstance(item, Telemetry):
            continue  # already acknowledged before the restart
        if isinstance(item, RegisterRobot):  # registrations are idempotent: always replayed
            registrar.robot(item)
            firmware[item.robot_id] = item.firmware_version
            acked.add(index)
            continue
        if isinstance(item, RegisterBoot):
            boot_numbers[item.boot_id] = registrar.boot(item)
            acked.add(index)
            continue
        if crash_after is not None and report.produced >= crash_after:
            producer.purge()  # drop in-flight + queued: what a SIGKILL would do
            break
        if pace_eps:  # sustained-rate mode: never get ahead of the schedule
            ahead = report.produced / pace_eps - (time.perf_counter() - started)
            if ahead > 0:
                producer.poll(ahead)
        value = kafka_value(item, encoder, boot_numbers, firmware, producer_id)
        report.faults[item.fault or "none"] += 1
        while True:
            try:
                producer.produce(topic, key=item.robot_id.encode(), value=value,
                                 on_delivery=make_cb(index, time.perf_counter()))  # fmt: skip
                break
            except BufferError:
                report.backpressure_waits += 1
                producer.poll(0.05)  # serve delivery callbacks, wait for space
        report.produced += 1
        producer.poll(0)
    else:
        remaining = producer.flush(conf["message.timeout.ms"] / 1000 + 10)
        if remaining:
            report.failed.append(f"{remaining} messages still unacknowledged after flush")
    producer.poll(0)
    report.checkpoint = _contiguous(acked, start_index)
    report.elapsed_s = time.perf_counter() - started
    report.active_s = report.elapsed_s
    report.cpu_s = time.process_time() - cpu0
    return report


def _contiguous(acked: set[int], start: int) -> int:
    i = start - 1
    while i + 1 in acked:
        i += 1
    return i


# ------------------------------------------------------------------ multi-process


def _partition(items: list[Item], n: int) -> list[list[Item]]:
    parts: list[list[Item]] = [[] for _ in range(n)]
    for it in items:
        parts[zlib.crc32(it.robot_id.encode()) % n].append(it)
    return parts


def _worker(args: tuple) -> PublishReport:
    part, kw = args
    kw = dict(kw)
    api = kw.pop("api_url")
    known = kw.pop("boot_numbers", None)
    registrar = KnownRegistrar(known) if known is not None else HttpRegistrar(api)
    return publish(part, registrar=registrar, producer_id=f"fleet-sim-{os.getpid()}", **kw)


def publish_parallel(items: list[Item], processes: int, api_url: str, **kw: Any) -> PublishReport:
    """Robots are split across OS processes (no shared GIL); each robot's items stay
    in one process, in order, so per-robot ordering is preserved."""
    if processes <= 1:
        known = kw.pop("boot_numbers", None)
        reg = KnownRegistrar(known) if known is not None else HttpRegistrar(api_url)
        return publish(items, registrar=reg, **kw)
    started = time.perf_counter()
    total = PublishReport()
    with ProcessPoolExecutor(max_workers=processes) as pool:
        for r in pool.map(_worker, [(p, {**kw, "api_url": api_url})
                                     for p in _partition(items, processes)]):  # fmt: skip
            total.produced += r.produced
            total.delivered += r.delivered
            total.failed += r.failed
            total.ack_latencies_s += r.ack_latencies_s
            total.backpressure_waits += r.backpressure_waits
            total.cpu_s += r.cpu_s
            total.active_s = max(total.active_s, r.elapsed_s)
            total.faults.update(r.faults)
    total.elapsed_s = time.perf_counter() - started
    return total
