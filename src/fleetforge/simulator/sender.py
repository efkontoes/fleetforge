"""Deliver a simulated stream to the Fleet API.

Robots are partitioned across workers by robot_id, the way Kafka partitions by key:
each robot's items are sent by exactly one worker, in stream order. Robots are
independent, so outcomes are deterministic even with many workers.

Transient failures (503, connection errors, timeouts) are retried with bounded
exponential backoff. That is safe only because every endpoint is idempotent.
"""

from __future__ import annotations

import time
import zlib
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import httpx

from fleetforge.simulator.fleet import Item, RegisterBoot, RegisterRobot, Telemetry

TRANSIENT_STATUS = {502, 503, 504}


class DeliveryFailed(RuntimeError):
    pass


@dataclass
class SendReport:
    outcomes: Counter = field(default_factory=Counter)  # (outcome, reason_code | None)
    latencies_s: list[float] = field(default_factory=list)  # telemetry requests only
    retries: int = 0
    elapsed_s: float = 0.0


def _partition(robot_id: str, n: int) -> int:
    return zlib.crc32(robot_id.encode()) % n  # stable across processes, unlike hash()


def send(
    items: list[Item],
    client_factory: Callable[[], httpx.Client],
    workers: int = 1,
    max_attempts: int = 8,
    base_backoff_s: float = 0.05,
) -> SendReport:
    partitions: list[list[Item]] = [[] for _ in range(workers)]
    for item in items:
        partitions[_partition(item.robot_id, workers)].append(item)

    def run(part: list[Item]) -> SendReport:
        report = SendReport()
        with client_factory() as client:
            for item in part:
                _deliver(client, item, report, max_attempts, base_backoff_s)
        return report

    started = time.perf_counter()
    total = SendReport()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for r in pool.map(run, partitions):
            total.outcomes.update(r.outcomes)
            total.latencies_s.extend(r.latencies_s)
            total.retries += r.retries
    total.elapsed_s = time.perf_counter() - started
    return total


def _deliver(
    client: httpx.Client, item: Item, report: SendReport, max_attempts: int, backoff: float
) -> None:
    for attempt in range(max_attempts):
        try:
            t0 = time.perf_counter()
            resp = _request(client, item)
            dt = time.perf_counter() - t0
        except (httpx.TransportError, httpx.TimeoutException):
            resp = None
        if resp is not None and resp.status_code not in TRANSIENT_STATUS:
            break
        report.retries += 1
        time.sleep(min(backoff * 2**attempt, 2.0))
    else:
        raise DeliveryFailed(f"gave up on {item.kind} for robot {item.robot_id}")

    if isinstance(item, Telemetry):
        body = resp.json()
        report.outcomes[(body["outcome"], body.get("reason_code"))] += 1
        report.latencies_s.append(dt)
    elif resp.status_code >= 400:
        raise DeliveryFailed(f"{item.kind} registration failed: {resp.status_code} {resp.text}")


def _request(client: httpx.Client, item: Item) -> httpx.Response:
    if isinstance(item, RegisterRobot):
        return client.post(
            "/v1/robots",
            json={
                "robot_id": item.robot_id,
                "robot_model": item.robot_model,
                "firmware_version": item.firmware_version,
            },
        )
    if isinstance(item, RegisterBoot):
        return client.post(f"/v1/robots/{item.robot_id}/boots", json={"boot_id": item.boot_id})
    return client.post(
        "/v1/telemetry", content=item.body(), headers={"content-type": "application/json"}
    )
