"""fleet-sim: generate a deterministic fault-injected stream and/or send it to the API.

fleet-sim generate --seed 42 --robots 20 --ticks 100 --out stream.jsonl
fleet-sim send --in stream.jsonl --api http://localhost:8000 --workers 8
fleet-sim run --seed 42 --robots 20 --ticks 100 --api http://localhost:8000
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from collections import Counter
from pathlib import Path

import httpx

from fleetforge.simulator.fleet import (
    FaultConfig,
    SimConfig,
    Telemetry,
    from_jsonl,
    generate,
    to_jsonl,
)
from fleetforge.simulator.oracle import expected
from fleetforge.simulator.sender import send


def _add_gen_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--robots", type=int, default=10)
    p.add_argument("--ticks", type=int, default=100, help="observations per robot")
    p.add_argument("--hz", type=float, default=10.0)
    p.add_argument("--duplicate-rate", type=float, default=0.0)
    p.add_argument("--reorder-rate", type=float, default=0.0)
    p.add_argument("--invalid-battery-rate", type=float, default=0.0)
    p.add_argument("--missing-field-rate", type=float, default=0.0)
    p.add_argument("--reboot-rate", type=float, default=0.0)


def _add_send_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--workers", type=int, default=4)


def _config(a: argparse.Namespace) -> SimConfig:
    return SimConfig(
        seed=a.seed,
        robots=a.robots,
        ticks=a.ticks,
        hz=a.hz,
        faults=FaultConfig(
            duplicate_rate=a.duplicate_rate,
            reorder_rate=a.reorder_rate,
            invalid_battery_rate=a.invalid_battery_rate,
            missing_field_rate=a.missing_field_rate,
            reboot_rate=a.reboot_rate,
        ),
    )


def _summary(items) -> dict:
    data = to_jsonl(items)
    return {
        "items": len(items),
        "telemetry": sum(isinstance(i, Telemetry) for i in items),
        "injected_faults": dict(
            sorted(Counter(i.fault for i in items if isinstance(i, Telemetry) and i.fault).items())
        ),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def _send_and_check(items, a: argparse.Namespace) -> int:
    report = send(
        items,
        lambda: httpx.Client(base_url=a.api, timeout=10.0),
        workers=a.workers,
    )
    exp = expected(items)
    actual = {f"{o}/{r or '-'}": n for (o, r), n in sorted(report.outcomes.items(), key=str)}
    want = {f"{o}/{r or '-'}": n for (o, r), n in sorted(exp.outcomes.items(), key=str)}
    lat = sorted(report.latencies_s)
    out = {
        "sent_telemetry": len(lat),
        "elapsed_s": round(report.elapsed_s, 3),
        "throughput_eps": round(len(lat) / report.elapsed_s, 1) if report.elapsed_s else None,
        "latency_ms": {
            "p50": round(statistics.median(lat) * 1000, 2),
            "p95": round(lat[int(0.95 * (len(lat) - 1))] * 1000, 2),
            "p99": round(lat[int(0.99 * (len(lat) - 1))] * 1000, 2),
        }
        if lat
        else None,
        "retries": report.retries,
        "outcomes": actual,
        "matches_oracle": actual == want,
    }
    if actual != want:
        out["oracle_outcomes"] = want
    print(json.dumps(out, indent=2))
    return 0 if actual == want else 1


def _publish(items, a: argparse.Namespace) -> int:
    from fleetforge.simulator.kafka_publisher import publish_parallel

    report = publish_parallel(
        items,
        a.processes,
        a.api,
        bootstrap=a.bootstrap,
        registry_url=a.registry,
        topic=a.topic,
        producer_conf={
            "compression.type": a.compression,
            "linger.ms": a.linger_ms,
            "batch.size": a.batch_bytes,
        },  # fmt: skip
    )
    lat = sorted(report.ack_latencies_s)
    out = {
        **_summary(items),
        "produced": report.produced,
        "acknowledged": report.delivered,
        "failed": len(report.failed),
        "elapsed_s": round(report.elapsed_s, 3),
        "produce_rate_eps": round(report.produced / max(report.elapsed_s, 1e-9), 1),
        "producer_cpu_s": round(report.cpu_s, 2),
        "backpressure_waits": report.backpressure_waits,
        "ack_latency_ms": {
            "p50": round(statistics.median(lat) * 1000, 2),
            "p99": round(lat[int(0.99 * (len(lat) - 1))] * 1000, 2),
        }
        if lat
        else None,
    }  # fmt: skip
    print(json.dumps(out, indent=2))
    return 0 if not report.failed and report.delivered == report.produced else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fleet-sim", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="write a stream to a JSONL file")
    _add_gen_args(g)
    g.add_argument("--out", type=Path, required=True)

    s = sub.add_parser("send", help="send a JSONL stream to the API")
    s.add_argument("--in", dest="infile", type=Path, required=True)
    _add_send_args(s)

    r = sub.add_parser("run", help="generate and send")
    _add_gen_args(r)
    _add_send_args(r)

    k = sub.add_parser("publish", help="generate and publish telemetry to Kafka")
    _add_gen_args(k)
    k.add_argument("--api", default="http://localhost:8000", help="registration endpoint")
    k.add_argument("--bootstrap", default="localhost:29092")
    k.add_argument("--registry", default="http://localhost:8081/apis/ccompat/v7")
    k.add_argument("--topic", default="telemetry.raw.v1")
    k.add_argument("--processes", type=int, default=1, help="producer OS processes")
    k.add_argument(
        "--compression", default="lz4", choices=["none", "lz4", "zstd", "snappy", "gzip"]
    )
    k.add_argument("--linger-ms", type=int, default=20)
    k.add_argument("--batch-bytes", type=int, default=262144)

    a = parser.parse_args(argv)
    if a.cmd == "generate":
        items = generate(_config(a))
        a.out.write_bytes(to_jsonl(items))
        print(json.dumps(_summary(items), indent=2))
        return 0
    if a.cmd == "publish":
        return _publish(generate(_config(a)), a)
    if a.cmd == "send":
        return _send_and_check(from_jsonl(a.infile.read_bytes()), a)
    items = generate(_config(a))
    print(json.dumps(_summary(items), indent=2), file=sys.stderr)
    return _send_and_check(items, a)


if __name__ == "__main__":
    sys.exit(main())
