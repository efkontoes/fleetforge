"""Milestone 2 benchmark: HTTP vs Kafka, per-event vs batched vs set-based.

    docker compose --profile core up -d --wait       # API, Kafka, registry, PostgreSQL
    .venv/bin/python benchmarks/m2_benchmark.py http      --robots 200 --ticks 75
    .venv/bin/python benchmarks/m2_benchmark.py drain     --robots 200 --ticks 75 \
        --modes per_event:1 loop:500 set:1 set:10 set:50 set:100 set:250 set:500 set_copy:500
    .venv/bin/python benchmarks/m2_benchmark.py sustained --rates 2000 4000 5000 6000 \
        --mode set:500 --consumers 3 --seconds 30

Measurement boundaries (stated, never mixed):
  http       client request start -> HTTP response (DB committed). One event per request.
  produce    Kafka producer produce() -> broker acknowledgement (acks=all). Says nothing
             about PostgreSQL.
  drain      projector(s) start consuming a pre-loaded backlog -> last batch committed.
             Pure processing capacity: Kafka -> validation -> PostgreSQL.
  sustained  producer paced at a fixed rate while projectors run; per-event latency
             is Kafka CreateTime (producer clock) -> projector's DB commit returned.

Every run uses fresh robots (new seed), a dedicated topic and consumer group, and is
checked against the in-memory oracle. PostgreSQL transaction counts and WAL bytes
come from pg_stat_database / pg_current_wal_lsn() deltas over the measured window.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import platform
import statistics
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psycopg
from confluent_kafka.admin import AdminClient, NewTopic

from fleetforge.config import Settings
from fleetforge.kafka.schemas import register, registry_client, subject_for
from fleetforge.simulator.fleet import FaultConfig, SimConfig, Telemetry, generate
from fleetforge.simulator.kafka_publisher import publish_parallel, register_bulk
from fleetforge.simulator.oracle import expected
from fleetforge.simulator.sender import send

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "benchmarks" / "results"
API = "http://localhost:8000"
BOOTSTRAP = "localhost:29092"
REGISTRY = "http://localhost:8081/apis/ccompat/v7"
# Realistic, not adversarial: a few duplicates/reorders/invalid values.
FAULTS = FaultConfig(duplicate_rate=0.01, reorder_rate=0.01, invalid_battery_rate=0.005,
                     missing_field_rate=0.005, reboot_rate=0.0005)  # fmt: skip


# ------------------------------------------------------------------ environment / DB facts


def env_file() -> dict[str, str]:
    out = {}
    for line in (ROOT / ".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k] = v
    return out


def owner_dsn() -> str:
    e = env_file()
    return f"postgresql://{e.get('POSTGRES_USER', 'fleetforge')}:{e['POSTGRES_PASSWORD']}@localhost:5432/fleetforge"


def pg_counters(settle: bool = False) -> dict[str, float]:
    """pg_stat_database is flushed per backend at most every ~10 s while it idles
    (PostgreSQL 15+ shared-memory stats); `settle` waits so counts are complete.
    WAL position is exact at any time."""
    if settle:
        time.sleep(11)
    with psycopg.connect(owner_dsn()) as c:
        (xact,) = c.execute(
            "SELECT xact_commit FROM pg_stat_database WHERE datname = 'fleetforge'"
        ).fetchone()
        (wal,) = c.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')").fetchone()
    return {"xact_commit": float(xact), "wal_bytes": float(wal)}


def pg_delta(before: dict, after: dict, seconds: float, events: int) -> dict:
    tx = after["xact_commit"] - before["xact_commit"]
    wal = after["wal_bytes"] - before["wal_bytes"]
    return {"transactions": int(tx), "tx_per_s": round(tx / seconds, 1),
            "wal_bytes": int(wal), "wal_bytes_per_event": round(wal / max(events, 1), 1)}  # fmt: skip


def docker_stats() -> dict[str, str]:
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}} {{.MemUsage}}"],
                         capture_output=True, text=True).stdout  # fmt: skip
    return {line.split()[0]: " ".join(line.split()[1:]) for line in out.strip().splitlines()}


def environment() -> dict:
    meminfo = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())

    def ver(cmd):  # noqa: E306
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
        except Exception:
            return "?"

    return {
        "platform": platform.platform(),
        "cpu": ver(["sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"]).strip(),
        "vcpus": os.cpu_count(),
        "wsl_mem_total": meminfo["MemTotal"].strip(),
        "swap_total": meminfo["SwapTotal"].strip(),
        "storage": "WSL2 ext4 virtual disk (Docker named volumes)",
        "postgres": ver(["docker", "compose", "exec", "-T", "postgres", "postgres", "--version"]),
        "kafka_image": "apache/kafka:4.3.1 (KRaft, 1 broker, RF=1)",
        "python": platform.python_version(),
        "docker_limits": "postgres 768m, kafka 768m (heap 384m), registry 448m, api 512m",
    }


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * (len(xs) - 1)))] if xs else float("nan")


def lat_ms(xs: list[float]) -> dict:
    return {"p50": round(pct(xs, 0.5) * 1000, 2), "p95": round(pct(xs, 0.95) * 1000, 2),
            "p99": round(pct(xs, 0.99) * 1000, 2), "n": len(xs)}  # fmt: skip


def workload(seed: int, robots: int, ticks: int):
    return generate(SimConfig(seed=seed, robots=robots, ticks=ticks, faults=FAULTS))


def check_oracle(items, transport: str) -> bool:
    import sys

    sys.path.insert(0, str(ROOT / "tests"))
    from sqlalchemy import create_engine
    from support import assert_state_matches_oracle, db_state

    eng = create_engine(owner_dsn().replace("postgresql://", "postgresql+psycopg://"))
    try:
        robots = {i.robot_id for i in items if isinstance(i, Telemetry)}
        assert_state_matches_oracle(db_state(eng, robots), expected(items, transport=transport))
        return True
    except AssertionError as exc:
        print("ORACLE MISMATCH:", exc)
        return False
    finally:
        eng.dispose()


def save(name: str, data: dict) -> Path:
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"m2-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{name}.json"
    path.write_text(json.dumps(data, indent=2))
    print(f"-> {path.relative_to(ROOT)}")
    return path


# ------------------------------------------------------------------ A: HTTP baseline


def _http_worker(args):
    part, threads = args
    cpu0 = time.process_time()
    rep = send(part, lambda: httpx.Client(base_url=API, timeout=60), workers=threads)
    return rep.latencies_s, rep.outcomes, time.process_time() - cpu0, rep.retries


def run_http(a) -> dict:
    items = workload(a.seed, a.robots, a.ticks)
    telemetry = [i for i in items if isinstance(i, Telemetry)]
    register_bulk(items, API)  # registration is not part of the measured window
    import zlib

    parts = [[] for _ in range(a.processes)]
    for it in telemetry:
        parts[zlib.crc32(it.robot_id.encode()) % a.processes].append(it)
    before = pg_counters()
    t0 = time.perf_counter()
    with mp.get_context("fork").Pool(a.processes) as pool:
        out = pool.map(_http_worker, [(p, a.threads) for p in parts])
    elapsed = time.perf_counter() - t0
    after = pg_counters(settle=True)
    lats = [x for o in out for x in o[0]]
    client_cpu = sum(o[2] for o in out)
    res = {
        "architecture": "A: HTTP -> PostgreSQL, one event per transaction",
        "events": len(telemetry), "client_processes": a.processes, "threads_per_process": a.threads,
        "elapsed_s": round(elapsed, 2), "throughput_eps": round(len(telemetry) / elapsed, 1),
        "latency_ms": lat_ms(lats),
        "client_cpu_s": round(client_cpu, 1),
        "client_cpu_cores_avg": round(client_cpu / elapsed, 2),
        "retries": sum(o[3] for o in out),
        "postgres": pg_delta(before, after, elapsed, len(telemetry)),
        "matches_oracle": check_oracle(items, "http"),
        "containers": docker_stats(),
    }  # fmt: skip
    print(
        json.dumps(
            {
                k: res[k]
                for k in ("throughput_eps", "latency_ms", "client_cpu_cores_avg", "postgres")
            }
        )
    )
    return res


# ------------------------------------------------------------------ Kafka helpers


def new_topic(partitions: int = 6) -> str:
    name = f"bench.{uuid.uuid4().hex[:10]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})  # must outlive the request
    admin.create_topics([NewTopic(name, partitions, 1)])[name].result(30)
    register(registry_client(REGISTRY), subject_for(name), 1)
    return name


def drop_topic(name: str) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for f in admin.delete_topics([name]).values():
        f.result(30)  # keep the client alive until the request completes


def _projector_proc(topic, group, mode, batch, idle_s, q):
    from fleetforge.db.engine import make_engine
    from fleetforge.projector.main import Projector

    s = Settings(database_url=owner_dsn().replace("postgresql://", "postgresql+psycopg://"),
                 kafka_bootstrap=BOOTSTRAP, schema_registry_url=REGISTRY, projector_mode=mode,
                 projector_batch_size=batch, projector_flush_interval_s=0.05,
                 db_pool_size=2, db_max_overflow=0)  # fmt: skip
    eng = make_engine(s)
    p = Projector(s, eng, topic=topic, group=group)
    commits, e2e, sizes, window = [], [], [], [None, None]
    txs = 0
    orig = p.processor.process

    def spy(records):
        nonlocal txs
        start = time.time()
        r = orig(records)
        now = time.time()
        window[0] = window[0] or start
        window[1] = now
        commits.extend(r.commit_seconds)
        txs += r.transactions
        sizes.append(len(records))
        e2e.extend(now - x.timestamp_ms / 1000 for x in records if x.timestamp_ms)
        return r

    p.processor.process = spy
    cpu0 = time.process_time()
    stats = p.run(max_idle_s=idle_s)
    import resource

    q.put({"records": stats.records, "batches": stats.batches, "retries": stats.retries,
           "transactions": txs, "commit_s": commits, "e2e_s": e2e, "sizes": sizes,
           "window": window, "cpu_s": time.process_time() - cpu0,
           "max_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024})  # fmt: skip


def run_projectors(topic, mode, batch, consumers, idle_s):
    group = f"bench-{uuid.uuid4().hex[:8]}"
    q = mp.get_context("fork").Queue()
    procs = [mp.get_context("fork").Process(target=_projector_proc, args=(topic, group, mode, batch, idle_s, q))
             for _ in range(consumers)]  # fmt: skip
    for pr in procs:
        pr.start()
    return procs, q


def collect(procs, q):
    outs = [q.get() for _ in procs]
    for pr in procs:
        pr.join()
    return outs


# ------------------------------------------------------------------ B/C/D: backlog drain


def run_drain(a) -> dict:
    runs = []
    for k, spec in enumerate(a.modes):
        mode, batch = spec.split(":")
        items = workload(a.seed + k, a.robots, a.ticks)
        telemetry = sum(isinstance(i, Telemetry) for i in items)
        boot_numbers = register_bulk(items, API)
        topic = new_topic()
        before = pg_counters()
        # Consumers join and the group stabilizes BEFORE data arrives; otherwise each
        # late joiner triggers a stop-the-world rebalance inside the measured window
        # (observed: 3 consumers measured slower than 2).
        procs, q = run_projectors(topic, mode, int(batch), a.consumers, idle_s=12.0)
        time.sleep(8)
        prod = publish_parallel(items, a.producers, API, bootstrap=BOOTSTRAP, registry_url=REGISTRY,
                                topic=topic, boot_numbers=boot_numbers)  # fmt: skip
        assert not prod.failed and prod.delivered == telemetry
        time.sleep(1)
        stats_mid = docker_stats()
        outs = collect(procs, q)
        after = pg_counters()
        start = min(o["window"][0] for o in outs if o["window"][0])
        end = max(o["window"][1] for o in outs if o["window"][1])
        drain = end - start
        records = sum(o["records"] for o in outs)
        commits = [c for o in outs for c in o["commit_s"]]
        sizes = [s for o in outs for s in o["sizes"]]
        r = {
            "mode": mode, "batch_size": int(batch), "consumers": a.consumers, "events": telemetry,
            "produce": {"producer_processes": a.producers, "elapsed_s": round(prod.elapsed_s, 2),
                        "throughput_eps": round(telemetry / prod.elapsed_s, 1),
                        "ack_latency_ms": lat_ms(prod.ack_latencies_s),
                        "producer_cpu_s": round(prod.cpu_s, 2)},
            "drain": {"records": records, "seconds": round(drain, 2),
                      "throughput_eps": round(records / drain, 1),
                      "avg_batch": round(statistics.mean(sizes), 1),
                      "transactions": sum(o["transactions"] for o in outs),
                      "commit_latency_ms": lat_ms(commits),
                      "projector_cpu_s": round(sum(o["cpu_s"] for o in outs), 2),
                      "projector_cpu_cores_avg": round(sum(o["cpu_s"] for o in outs) / drain, 2),
                      "projector_max_rss_mb": round(max(o["max_rss_mb"] for o in outs), 1),
                      "retries": sum(o["retries"] for o in outs)},
            "postgres": pg_delta(before, after, drain, records),
            "containers_during_drain": stats_mid,
            "matches_oracle": check_oracle(items, "kafka"),
        }  # fmt: skip
        drop_topic(topic)
        print(
            f"{spec:>12}: produce {r['produce']['throughput_eps']:>8} ev/s | drain "
            f"{r['drain']['throughput_eps']:>8} ev/s, {r['drain']['transactions']:>6} tx, "
            f"commit p50 {r['drain']['commit_latency_ms']['p50']} ms, "
            f"WAL {r['postgres']['wal_bytes_per_event']} B/ev, oracle={r['matches_oracle']}",
            flush=True,
        )
        runs.append(r)
    return {"experiment": "backlog drain", "runs": runs}


# ------------------------------------------------------------------ sustained


def run_sustained(a) -> dict:
    mode, batch = a.mode.split(":")
    runs = []
    for k, rate in enumerate(a.rates):
        ticks = max(1, int(rate * a.seconds / a.robots))
        items = workload(a.seed + 100 + k, a.robots, ticks)
        telemetry = sum(isinstance(i, Telemetry) for i in items)
        boot_numbers = register_bulk(items, API)
        topic = new_topic()
        # Long idle timeout: forking producers with large workloads can take seconds.
        procs, q = run_projectors(topic, mode, int(batch), a.consumers, idle_s=25.0)
        time.sleep(4)  # consumers joined; warm-up excluded from producer schedule
        before = pg_counters()
        t0 = time.perf_counter()
        prod = publish_parallel(items, a.producers, API, bootstrap=BOOTSTRAP, registry_url=REGISTRY,
                                topic=topic, boot_numbers=boot_numbers,
                                pace_eps=rate / a.producers)  # fmt: skip
        produce_elapsed = time.perf_counter() - t0
        mid = docker_stats()
        outs = collect(procs, q)
        after = pg_counters(settle=True)
        e2e = [x for o in outs for x in o["e2e_s"]]
        end = max(o["window"][1] for o in outs if o["window"][1])
        last_lag_s = max(0.0, (end - time.time()) + (time.perf_counter() - t0) - produce_elapsed)
        warm = e2e[len(e2e) // 10 :]  # drop the first 10% (connection warm-up)
        r = {
            "target_rate_eps": rate, "events": telemetry, "mode": mode, "batch_size": int(batch),
            "consumers": a.consumers, "producer_processes": a.producers,
            "achieved_produce_eps": round(telemetry / prod.active_s, 1),
            "producer_active_s": round(prod.active_s, 2),
            "producer_ack_latency_ms": lat_ms(prod.ack_latencies_s),
            "processed": sum(o["records"] for o in outs),
            "event_to_state_ms": lat_ms(warm),
            "max_event_to_state_s": round(max(e2e), 2) if e2e else None,
            "projector_cpu_cores_avg": round(sum(o["cpu_s"] for o in outs) / prod.active_s, 2),
            "postgres": pg_delta(before, after, prod.active_s, telemetry),
            "containers": mid,
            "kept_up": pct(warm, 0.99) < 2.0,
            "matches_oracle": check_oracle(items, "kafka"),
            "tail_drain_s": round(last_lag_s, 2),
        }  # fmt: skip
        drop_topic(topic)
        print(
            f"rate {rate:>5}: achieved {r['achieved_produce_eps']} ev/s, e2e p50/p95/p99 "
            f"{r['event_to_state_ms']['p50']}/{r['event_to_state_ms']['p95']}/{r['event_to_state_ms']['p99']} ms, "
            f"projector cores {r['projector_cpu_cores_avg']}, oracle={r['matches_oracle']}",
            flush=True,
        )
        runs.append(r)
    return {"experiment": "sustained rate", "runs": runs}


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("http", "drain", "sustained"):
        p = sub.add_parser(name)
        p.add_argument("--robots", type=int, default=200)
        p.add_argument("--ticks", type=int, default=75)
        p.add_argument("--seed", type=int, default=int(time.time()) % 10**6)
        p.add_argument("--label", default="")
        if name == "http":
            p.add_argument("--processes", type=int, default=4)
            p.add_argument("--threads", type=int, default=8)
        else:
            p.add_argument("--producers", type=int, default=2)
            p.add_argument("--consumers", type=int, default=1)
        if name == "drain":
            p.add_argument("--modes", nargs="+", default=["set:500"])
        if name == "sustained":
            p.add_argument("--rates", nargs="+", type=int, default=[2000, 5000])
            p.add_argument("--mode", default="set:500")
            p.add_argument("--seconds", type=int, default=30)
    a = ap.parse_args()
    started = datetime.now(UTC).isoformat()
    fn = {"http": run_http, "drain": run_drain, "sustained": run_sustained}[a.cmd]
    data = {"started_at": started, "command": a.cmd, "args": vars(a), "environment": environment(),
            "result": fn(a)}  # fmt: skip
    save(f"{a.cmd}{'-' + a.label if a.label else ''}", data)


if __name__ == "__main__":
    main()
