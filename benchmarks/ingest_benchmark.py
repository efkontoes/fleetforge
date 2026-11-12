"""Telemetry ingestion + state-query benchmark against a RUNNING Fleet API.

    docker compose --profile app up -d --build --wait
    .venv/bin/python benchmarks/ingest_benchmark.py [--robots 500 --ticks 20 --workers 1,4,8,16,32]

What is measured (client side, over real HTTP to the containerised API):
  * ingestion throughput (events/s) and p50/p95/p99 request latency, per worker count;
  * outcome counts (incl. rejections), cross-checked against the simulator oracle;
  * latest-state read latency (GET /v1/robots/{id}/state);
  * the PostgreSQL plan for the state query and the conditional upsert.
Registration traffic is sent before the timer starts. Each sweep step uses a fresh
seed, so it writes new robots instead of replaying old ones.

Results are written to benchmarks/results/<UTC timestamp>.json. Nothing is estimated:
if a number is in the report, it was measured in that run.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psycopg

from fleetforge.simulator.fleet import FaultConfig, SimConfig, Telemetry, generate
from fleetforge.simulator.oracle import expected
from fleetforge.simulator.sender import send

RESULTS = Path(__file__).parent / "results"


def pct(sorted_values: list[float], p: float) -> float:
    return sorted_values[min(len(sorted_values) - 1, int(p * (len(sorted_values) - 1)))]


def ms(v: float) -> float:
    return round(v * 1000, 2)


def ingest_step(api: str, seed: int, robots: int, ticks: int, workers: int, faults: FaultConfig):
    items = generate(SimConfig(seed=seed, robots=robots, ticks=ticks, faults=faults))
    # The stream starts with one robot + one boot registration per robot; those are sent
    # before the timer. Mid-stream reboots stay in order with the robot's telemetry.
    initial, rest = items[: 2 * robots], items[2 * robots :]
    assert not any(isinstance(i, Telemetry) for i in initial)

    def client():
        return httpx.Client(base_url=api, timeout=30.0)

    send(initial, client, workers=min(workers, 8))
    report = send(rest, client, workers=workers)
    lat = sorted(report.latencies_s)
    exp = expected(items)
    return {
        "workers": workers,
        "robots": robots,
        "telemetry_events": len(lat),
        "elapsed_s": round(report.elapsed_s, 3),
        "throughput_eps": round(len(lat) / report.elapsed_s, 1),
        "latency_ms": {
            "p50": ms(statistics.median(lat)),
            "p95": ms(pct(lat, 0.95)),
            "p99": ms(pct(lat, 0.99)),
            "max": ms(lat[-1]),
        },
        "retries": report.retries,
        "outcomes": {
            f"{o}/{r or '-'}": n for (o, r), n in sorted(report.outcomes.items(), key=str)
        },
        "rejected_total": sum(
            n for (o, _), n in report.outcomes.items() if o not in ("APPLIED", "DUPLICATE")
        ),
        "matches_oracle": report.outcomes == exp.outcomes,
        "robot_ids": sorted({i.robot_id for i in items if isinstance(i, Telemetry)}),
    }


def read_step(api: str, robot_ids: list[str], n: int = 2000):
    rng = random.Random(0)
    lat = []
    with httpx.Client(base_url=api, timeout=10.0) as c:
        for _ in range(100):
            c.get(f"/v1/robots/{rng.choice(robot_ids)}/state")
        for _ in range(n):
            t0 = time.perf_counter()
            r = c.get(f"/v1/robots/{rng.choice(robot_ids)}/state")
            lat.append(time.perf_counter() - t0)
            assert r.status_code == 200, r.text
    lat.sort()
    return {
        "requests": n,
        "latency_ms": {
            "p50": ms(statistics.median(lat)),
            "p95": ms(pct(lat, 0.95)),
            "p99": ms(pct(lat, 0.99)),
        },
    }


def db_facts(dsn: str, robot_id: str) -> dict:
    with psycopg.connect(dsn) as conn:
        facts = {
            "server_version": conn.execute("SHOW server_version").fetchone()[0],
            "synchronous_commit": conn.execute("SHOW synchronous_commit").fetchone()[0],
            "fsync": conn.execute("SHOW fsync").fetchone()[0],
            "rows": {
                t: conn.execute(f"SELECT count(*) FROM fleet.{t}").fetchone()[0]
                for t in ("robots", "robot_latest_state", "telemetry_rejections")
            },
        }
        plan = conn.execute(
            """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
               SELECT s.*, m.mission_id FROM fleet.robot_latest_state s
               LEFT JOIN fleet.missions m ON m.robot_id = s.robot_id
                                         AND m.status IN ('ASSIGNED','RUNNING')
               WHERE s.robot_id = %s""",
            (robot_id,),
        ).fetchone()[0][0]
        facts["state_query"] = {
            "execution_ms": plan["Execution Time"],
            "planning_ms": plan["Planning Time"],
            "plan": _plan_nodes(plan["Plan"]),
        }
    return facts


def _plan_nodes(node: dict) -> list[str]:
    label = node["Node Type"] + (f" on {node['Relation Name']}" if "Relation Name" in node else "")
    if "Index Name" in node:
        label += f" using {node['Index Name']}"
    return [label] + [n for child in node.get("Plans", []) for n in _plan_nodes(child)]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument(
        "--dsn",
        default=os.environ.get(
            "BENCH_DSN", "postgresql://fleetforge:fleetforge@localhost:5432/fleetforge"
        ),
    )
    ap.add_argument("--robots", type=int, default=500)
    ap.add_argument("--ticks", type=int, default=20)
    ap.add_argument("--workers", default="1,4,8,16,32")
    ap.add_argument("--seed", type=int, default=int(time.time()))
    ap.add_argument(
        "--api-desc",
        default="uvicorn in Docker, WEB_CONCURRENCY processes, sync handlers",
        help="free-text description of the API deployment, stored with the results",
    )
    args = ap.parse_args()

    faults = FaultConfig(
        duplicate_rate=0.02,
        reorder_rate=0.02,
        invalid_battery_rate=0.01,
        missing_field_rate=0.01,
        reboot_rate=0.002,
    )
    steps = []
    for k, w in enumerate(int(x) for x in args.workers.split(",")):
        step = ingest_step(args.api, args.seed * 100 + k, args.robots, args.ticks, w, faults)
        print(
            f"workers={w:>3}  {step['throughput_eps']:>8} ev/s  "
            f"p50={step['latency_ms']['p50']}ms p95={step['latency_ms']['p95']}ms "
            f"p99={step['latency_ms']['p99']}ms  rejected={step['rejected_total']}  "
            f"oracle={'ok' if step['matches_oracle'] else 'MISMATCH'}",
            flush=True,
        )
        steps.append(step)

    robot_ids = steps[-1].pop("robot_ids")
    for s in steps:
        s.pop("robot_ids", None)
    reads = read_step(args.api, robot_ids)
    print(f"state reads: p50={reads['latency_ms']['p50']}ms p95={reads['latency_ms']['p95']}ms")
    facts = db_facts(args.dsn, robot_ids[0])
    print(
        f"state query in PostgreSQL: {facts['state_query']['execution_ms']} ms  "
        f"plan={facts['state_query']['plan']}"
    )

    out = {
        "measured_at": datetime.now(UTC).isoformat(),
        "seed": args.seed,
        "environment": {
            "platform": platform.platform(),
            "cpus": os.cpu_count(),
            "mem_total_gib": round(
                os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1
            ),
            "api": args.api_desc,
            "client": "same host, httpx, robots partitioned by robot_id across worker threads",
        },
        "faults": faults.__dict__,
        "ingest": steps,
        "state_reads": reads,
        "database": facts,
    }
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    path.write_text(json.dumps(out, indent=2))
    print(f"written {path}")


if __name__ == "__main__":
    main()
