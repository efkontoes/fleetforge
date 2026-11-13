"""Traced sustained-load benchmark (Milestone 2 audit).

    docker compose --profile full up -d --wait
    .venv/bin/python benchmarks/m2_traced.py --rate 5000 --seconds 300 --consumers 3 \
        --label sustained-5000

Why a second harness: m2_benchmark.py `sustained` measures one number (Kafka
CreateTime -> DB commit) and cannot say WHERE time goes. It also starts the clock
at produce(), so a producer that falls behind its schedule hides its own delay
(coordinated omission), and it pre-generates the workload in memory, which caps runs
at ~30 s on this machine. This harness:

  * streams each producer's fleet lazily (constant memory, runs of many minutes);
  * timestamps every sampled event on ONE clock, time.monotonic_ns(). On Linux,
    CLOCK_MONOTONIC is system-wide, so producer, projector and harness processes on
    this host share a timeline with ns resolution; no wall-clock or Kafka ms
    timestamps are mixed in;
  * samples deterministically: offsets with offset % trace_every == 0, recorded by
    the producer (delivery callback) and by the projector, then joined on
    (partition, offset);
  * records, per second, durable lag = sum(high watermarks) - sum(ledger next_offset),
    i.e. events in Kafka that are not yet durably applied in PostgreSQL.

Segments per event (all ns, one clock):
  scheduled  when the paced producer SHOULD have sent it (offered load)
  enqueued   producer.produce() called          -> lateness = enqueued - scheduled
  acked      broker acknowledgement (acks=all) seen by the producer's delivery callback
  received   projector's consume() returned the message
  batch      BatchProcessor.process() started (batch formed)
  tx_begin   PostgreSQL transaction opened (after decode/validation)
  pre_commit all SQL done, COMMIT about to be sent
  committed  COMMIT returned = visible to every new snapshot (synchronous_commit=on)
"""

from __future__ import annotations

import argparse
import gc
import math
import multiprocessing as mp
import os
import resource
import statistics
import sys
import threading
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parent))
from m2_benchmark import (  # noqa: E402
    API,
    BOOTSTRAP,
    FAULTS,
    REGISTRY,
    ROOT,
    docker_stats,
    drop_topic,
    environment,
    new_topic,
    owner_dsn,
    pg_counters,
    pg_delta,
    save,
)

from fleetforge.config import Settings  # noqa: E402
from fleetforge.simulator.fleet import SimConfig, Telemetry, iter_generate  # noqa: E402
from fleetforge.simulator.kafka_publisher import KnownRegistrar, publish, register_bulk  # noqa: E402
from fleetforge.simulator.oracle import expected  # noqa: E402

# Joined events are tuples (memory: ~400 B each instead of ~700 B as dicts).
PART, OFF, SCHED, ENQ, ACK, RECV, BATCH, TXB, PRE, COM, PID = range(11)
SEGMENTS = {
    "producer_lateness": (SCHED, ENQ),
    "enqueue_to_ack": (ENQ, ACK),
    "enqueue_to_receive": (ENQ, RECV),
    "receive_to_batch": (RECV, BATCH),
    "decode_validate": (BATCH, TXB),
    "sql_in_tx": (TXB, PRE),
    "commit": (PRE, COM),
    "e2e_from_enqueue": (ENQ, COM),
    "e2e_from_schedule": (SCHED, COM),
}


def pct(xs, p):
    return xs[min(len(xs) - 1, int(p * (len(xs) - 1)))] if xs else float("nan")


def dist_ms(xs_ns: list[int]) -> dict:
    xs = sorted(xs_ns)
    f = lambda v: round(v / 1e6, 2)  # noqa: E731
    return {"p50": f(pct(xs, 0.5)), "p95": f(pct(xs, 0.95)), "p99": f(pct(xs, 0.99)),
            "p999": f(pct(xs, 0.999)), "max": f(xs[-1]) if xs else None, "n": len(xs)}  # fmt: skip


def cfg_for(seed: int, i: int, robots: int, ticks: int) -> SimConfig:
    return SimConfig(seed=seed * 100 + i, robots=robots, ticks=ticks, faults=FAULTS)


# ------------------------------------------------------------------ projector process


def _projector_proc(topic, group, mode, batch, flush_s, idle_s, trace_every, q):
    from fleetforge.db.engine import make_engine
    from fleetforge.projector.main import Projector

    s = Settings(database_url=owner_dsn().replace("postgresql://", "postgresql+psycopg://"),
                 kafka_bootstrap=BOOTSTRAP, schema_registry_url=REGISTRY, projector_mode=mode,
                 projector_batch_size=batch, projector_flush_interval_s=flush_s,
                 db_pool_size=2, db_max_overflow=0)  # fmt: skip
    stamps: dict[str, int] = {}
    recv: list[tuple[int, int, int, int]] = []  # partition, offset, received_ns, batch index
    batches: list[tuple[int, int, int, int, int]] = []  # batch, tx_begin, pre_commit, commit, n
    consumes: list[tuple[int, int]] = []  # (returned_ns, messages)
    gcs: list[tuple[int, int, int]] = []  # (start_ns, duration_ns, generation) if > 1 ms
    rebalances: list[tuple[int, str, list[int]]] = []

    def hook(stage):
        stamps[stage] = time.monotonic_ns()

    p = Projector(s, make_engine(s), topic=topic, group=group, fault_hook=hook)

    class Tracing:
        def __init__(self, c):
            self._c = c

        def __getattr__(self, name):
            return getattr(self._c, name)

        def consume(self, *a, **k):
            msgs = self._c.consume(*a, **k)
            t = time.monotonic_ns()
            consumes.append((t, len(msgs)))
            nb = len(batches)
            for m in msgs:
                if not m.error() and m.offset() % trace_every == 0:
                    recv.append((m.partition(), m.offset(), t, nb))
            return msgs

    p.consumer = Tracing(p.consumer)
    orig = p.processor.process

    def spy(records):
        t0 = time.monotonic_ns()
        r = orig(records)
        batches.append((t0, stamps.get("tx_begin", 0), stamps.get("before_commit", 0),
                        time.monotonic_ns(), len(records)))  # fmt: skip
        return r

    p.processor.process = spy
    for name in ("_on_assign", "_on_revoke"):
        real = getattr(p, name)

        def wrapped(c, parts, _real=real, _name=name):
            rebalances.append((time.monotonic_ns(), _name, [tp.partition for tp in parts]))
            return _real(c, parts)

        setattr(p, name, wrapped)
    gc_start = {}

    def gc_cb(phase, info):
        if phase == "start":
            gc_start["t"] = time.monotonic_ns()
        elif "t" in gc_start:
            d = time.monotonic_ns() - gc_start["t"]
            if d > 1_000_000:
                gcs.append((gc_start["t"], d, info["generation"]))

    gc.callbacks.append(gc_cb)
    cpu0 = time.process_time()
    stats = p.run(max_idle_s=idle_s)
    q.put({"pid": os.getpid(), "records": stats.records, "batches": stats.batches,
           "retries": stats.retries, "recv": recv, "batch_log": batches, "consumes": consumes,
           "gc": gcs, "rebalances": rebalances, "cpu_s": time.process_time() - cpu0,
           "max_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024})  # fmt: skip


# ------------------------------------------------------------------ producer process


def _producer_proc(i, seed, robots, ticks, topic, pace, boot_numbers, trace_every, conf, q):
    items = iter_generate(cfg_for(seed, i, robots, ticks))
    r = publish(items, bootstrap=BOOTSTRAP, registry_url=REGISTRY, topic=topic,
                registrar=KnownRegistrar(boot_numbers), producer_id=f"bench-{i}",
                producer_conf=conf, pace_eps=pace, trace_every=trace_every)  # fmt: skip
    q.put({"i": i, "pid": os.getpid(), "produced": r.produced, "delivered": r.delivered,
           "failed": r.failed[:10], "n_failed": len(r.failed), "trace": r.trace,
           "elapsed_s": r.elapsed_s, "cpu_s": r.cpu_s, "max_behind_s": r.max_behind_s,
           "backpressure_waits": r.backpressure_waits, "faults": dict(r.faults),
           "ack_p99_ms": round(pct(sorted(r.ack_latencies_s), 0.99) * 1000, 2)})  # fmt: skip


# ------------------------------------------------------------------ monitor (harness)


class Monitor(threading.Thread):
    """1 Hz: durable lag (Kafka high watermark - PostgreSQL ledger), per-process CPU,
    host memory. Every ~5 s: container CPU/memory."""

    def __init__(self, topic, group, pids_ref):
        super().__init__(daemon=True)
        self.topic, self.group, self.pids_ref = topic, group, pids_ref
        self.samples: list[dict] = []
        self.docker: list[dict] = []
        self.stop = threading.Event()
        self.tick = os.sysconf("SC_CLK_TCK")

    def _cpu(self, pid):
        try:
            f = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            return (int(f[11]) + int(f[12])) / self.tick
        except (OSError, IndexError):
            return None

    def run(self):
        c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"mon-{uuid.uuid4()}"})
        db = psycopg.connect(owner_dsn(), autocommit=True)
        last_cpu: dict[int, float] = {}
        last_t = time.monotonic()
        dthread = threading.Thread(target=self._docker_loop, daemon=True)
        dthread.start()
        while not self.stop.is_set():
            t = time.monotonic()
            try:
                hw = sum(c.get_watermark_offsets(TopicPartition(self.topic, p), timeout=5,
                                                 cached=False)[1] for p in range(6))  # fmt: skip
            except Exception as exc:  # an unresponsive broker is a finding, not a crash
                self.samples.append({"t_ns": time.monotonic_ns(), "broker_error": str(exc)})
                continue
            (led,) = db.execute("SELECT coalesce(sum(next_offset), 0) FROM fleet.consumer_offsets"
                                " WHERE consumer_group = %s", (self.group,)).fetchone()  # fmt: skip
            cpu = {}
            for role, pid in list(self.pids_ref.items()):
                v = self._cpu(pid)
                if v is not None and pid in last_cpu:
                    cpu[role] = round((v - last_cpu[pid]) / (t - last_t), 2)
                if v is not None:
                    last_cpu[pid] = v
            mem = dict(
                line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines()
            )
            self.samples.append({"t_ns": time.monotonic_ns(), "high_watermark": hw,
                                 "ledger": int(led), "durable_lag": hw - int(led),
                                 "cpu_cores": cpu, "load1": os.getloadavg()[0],
                                 "mem_available_mb": int(mem["MemAvailable"].split()[0]) // 1024})  # fmt: skip
            last_t = t
            self.stop.wait(max(0.0, 1.0 - (time.monotonic() - t)))
        c.close()
        db.close()

    def _docker_loop(self):
        while not self.stop.is_set():
            t = time.monotonic_ns()
            self.docker.append({"t_ns": t, **docker_stats()})
            self.stop.wait(5)


# ------------------------------------------------------------------ analysis


def join(producers, projectors):
    sent = {}
    for pr in producers:
        for p, o, sched, enq, ack in pr["trace"]:
            sent[(p, o)] = (sched, enq, ack)
    events = []
    for pj in projectors:
        bl = pj["batch_log"]
        for p, o, rcv, bi in pj["recv"]:
            if bi >= len(bl) or (p, o) not in sent:
                continue
            b, txb, pre, com, _ = bl[bi]
            sched, enq, ack = sent[(p, o)]
            events.append((p, o, sched, enq, ack, rcv, b, txb, pre, com, pj["pid"]))
    return events


def analyse(events, t0_ns, monitor, projectors, threshold_s):
    seg = {k: dist_ms([e[b] - e[a] for e in events]) for k, (a, b) in SEGMENTS.items()}
    per_s: dict[int, list] = defaultdict(list)
    for e in events:
        per_s[(e[COM] - t0_ns) // 1_000_000_000].append(e)
    series = []
    for s in sorted(per_s):
        evs = per_s[s]
        worst = max(evs, key=lambda e: e[COM] - e[SCHED])
        series.append({"s": int(s), "n_sampled": len(evs),
                       "max_e2e_ms": round((worst[COM] - worst[SCHED]) / 1e6, 1),
                       "worst_breakdown_ms": {k: round((worst[b] - worst[a]) / 1e6, 1)
                                              for k, (a, b) in SEGMENTS.items()}})  # fmt: skip
    # Outlier episodes: consecutive seconds whose worst event exceeded the threshold.
    episodes, cur = [], []
    for row in series:
        if row["max_e2e_ms"] > threshold_s * 1000:
            cur.append(row)
        elif cur:
            episodes.append(cur)
            cur = []
    if cur:
        episodes.append(cur)
    gcs = [(g[0], g[1], g[2], pj["pid"]) for pj in projectors for g in pj["gc"]]
    rebal = [(r[0], r[1], r[2], pj["pid"]) for pj in projectors for r in pj["rebalances"]]
    out = []
    for ep in episodes:
        lo = t0_ns + (ep[0]["s"] - 2) * 10**9
        hi = t0_ns + (ep[-1]["s"] + 1) * 10**9
        evs = [e for row in ep for e in per_s[row["s"]]]
        dominant = Counter(max(SEGMENTS, key=lambda k: (e[SEGMENTS[k][1]] - e[SEGMENTS[k][0]])
                               if not k.startswith("e2e") else -1) for e in evs
                           if (e[COM] - e[SCHED]) > threshold_s * 1e9)  # fmt: skip
        out.append({
            "from_s": ep[0]["s"], "to_s": ep[-1]["s"],
            "worst_e2e_ms": max(r["max_e2e_ms"] for r in ep),
            "dominant_segment_counts": dict(dominant),
            "segment_ms_in_episode": {k: dist_ms([e[b] - e[a] for e in evs])
                                      for k, (a, b) in SEGMENTS.items()},
            "gc_pauses_ms": [(round((g[0] - t0_ns) / 1e9, 2), round(g[1] / 1e6, 1), g[2])
                             for g in gcs if lo <= g[0] <= hi],
            "rebalances": [(round((r[0] - t0_ns) / 1e9, 2), r[1], r[2]) for r in rebal
                           if lo <= r[0] <= hi],
            "monitor": [m for m in monitor.samples if lo <= m["t_ns"] <= hi],
        })  # fmt: skip
    return seg, series, out


# ------------------------------------------------------------------ run


def run_once(a, k: int) -> dict:
    seed = a.seed + k
    n_prod = a.producers
    robots = a.robots // n_prod
    ticks = max(1, math.ceil(a.rate * a.seconds / (robots * n_prod)))
    topic = new_topic(6)
    group = f"traced-{uuid.uuid4().hex[:8]}"
    # Registration pre-pass (streamed; only registrations are kept).
    boot_numbers, generated = {}, 0
    for i in range(n_prod):
        regs = []
        for it in iter_generate(cfg_for(seed, i, robots, ticks)):
            if isinstance(it, Telemetry):
                generated += 1
            else:
                regs.append(it)
        boot_numbers.update(register_bulk(regs, API))

    ctx = mp.get_context("fork")
    pq, cq = ctx.Queue(), ctx.Queue()
    mode, batch = a.mode.split(":")
    projs = [ctx.Process(target=_projector_proc, args=(topic, group, mode, int(batch), a.flush_s,
                                                       a.idle_s, a.trace_every, cq))
             for _ in range(a.consumers)]  # fmt: skip
    for pr in projs:
        pr.start()
    pids = {f"projector{j}": pr.pid for j, pr in enumerate(projs)}
    time.sleep(6)  # consumers join and the group settles before data arrives
    mon = Monitor(topic, group, pids)
    mon.start()
    before = pg_counters()
    t0_ns = time.monotonic_ns()
    conf = dict(kv.split("=", 1) for kv in a.producer_conf)
    prods = [ctx.Process(target=_producer_proc, args=(i, seed, robots, ticks, topic,
                                                      a.rate / n_prod, boot_numbers,
                                                      a.trace_every, conf, pq))
             for i in range(n_prod)]  # fmt: skip
    for pr in prods:
        pr.start()
    pids.update({f"producer{j}": pr.pid for j, pr in enumerate(prods)})
    producers = [pq.get() for _ in prods]
    for pr in prods:
        pr.join()
    produce_end = time.monotonic_ns()
    projectors = [cq.get() for _ in projs]
    for pr in projs:
        pr.join()
    mon.stop.set()
    mon.join(10)
    after = pg_counters(settle=True)

    acked = sum(p["delivered"] for p in producers)
    active_s = max(p["elapsed_s"] for p in producers)
    events = join(producers, projectors)
    seg, series, episodes = analyse(events, t0_ns, mon, projectors, a.threshold_s)
    # Lag: after warm-up (first 10 % of the producing window) it must stay bounded.
    broker_errors = [m for m in mon.samples if "broker_error" in m]
    mon.samples = [m for m in mon.samples if "broker_error" not in m]
    prod_samples = [m for m in mon.samples if t0_ns <= m["t_ns"] <= produce_end]
    warm = prod_samples[len(prod_samples) // 10 :]
    lags = [m["durable_lag"] for m in warm]
    half = len(lags) // 2
    drained_at = next((m["t_ns"] for m in mon.samples
                       if m["t_ns"] >= produce_end and m["durable_lag"] == 0), None)  # fmt: skip
    oracle = check(seed, n_prod, robots, ticks, topic)
    cpu_by_role = defaultdict(list)
    for m in prod_samples:
        for role, v in m["cpu_cores"].items():
            cpu_by_role[role.rstrip("0123456789")].append(v)
    result = {
        "label": a.label, "run": k, "seed": seed, "target_rate_eps": a.rate,
        "duration_target_s": a.seconds, "producer_active_s": round(active_s, 1),
        "config": {"mode": a.mode, "consumers": a.consumers, "producers": n_prod,
                   "robots": robots * n_prod, "flush_interval_s": a.flush_s,
                   "trace_every": a.trace_every, "producer_conf": conf},
        "counts": {"generated": generated, "acknowledged": acked,
                   "produce_failed": sum(p["n_failed"] for p in producers),
                   "consumed_records": sum(p["records"] for p in projectors),
                   "durably_processed_offsets": mon.samples[-1]["ledger"] if mon.samples else None,
                   "high_watermark": mon.samples[-1]["high_watermark"] if mon.samples else None,
                   "faults_injected": dict(sum((Counter(p["faults"]) for p in producers),
                                               Counter())),
                   "sampled_events_joined": len(events)},
        "throughput": {"achieved_produce_eps": round(acked / active_s, 1),
                       "processed_eps_during_produce": round(
                           (warm[-1]["ledger"] - warm[0]["ledger"])
                           / ((warm[-1]["t_ns"] - warm[0]["t_ns"]) / 1e9), 1) if len(warm) > 1 else None,
                       "peak_1s_processed_eps": max((b["ledger"] - a_["ledger"]) for a_, b in
                                                    zip(mon.samples, mon.samples[1:], strict=False)) if len(mon.samples) > 1 else None},
        "lag": {"max_after_warmup": max(lags) if lags else None,
                "mean_first_half": round(statistics.mean(lags[:half]), 1) if half else None,
                "mean_second_half": round(statistics.mean(lags[half:]), 1) if half else None,
                "at_producer_end": prod_samples[-1]["durable_lag"] if prod_samples else None,
                "drain_after_producer_end_s": round((drained_at - produce_end) / 1e9, 2)
                if drained_at else None},
        "producer": {"max_behind_schedule_s": round(max(p["max_behind_s"] for p in producers), 3),
                     "backpressure_waits": sum(p["backpressure_waits"] for p in producers),
                     "ack_p99_ms": [p["ack_p99_ms"] for p in producers],
                     "cpu_s": round(sum(p["cpu_s"] for p in producers), 1)},
        "projector": {"batches": sum(p["batches"] for p in projectors),
                      "retries": sum(p["retries"] for p in projectors),
                      "cpu_s": round(sum(p["cpu_s"] for p in projectors), 1),
                      "max_rss_mb": round(max(p["max_rss_mb"] for p in projectors), 1),
                      "gc_pauses_over_1ms": sum(len(p["gc"]) for p in projectors),
                      "gc_max_ms": round(max((g[1] for p in projectors for g in p["gc"]),
                                             default=0) / 1e6, 1),
                      "rebalance_callbacks": sum(len(p["rebalances"]) for p in projectors),
                      "consume_gap_ms": dist_ms([b[0] - a_[0] for p in projectors
                                                 for a_, b in zip(p["consumes"], p["consumes"][1:], strict=False)]),
                      "batch_size_mean": round(statistics.mean(b[4] for p in projectors
                                                               for b in p["batch_log"]), 1)},
        "cpu_cores_mean": {r: round(statistics.mean(v), 2) for r, v in cpu_by_role.items()},
        "cpu_cores_p95": {r: round(pct(sorted(v), 0.95), 2) for r, v in cpu_by_role.items()},
        "monitor_broker_timeouts": [round((m["t_ns"] - t0_ns) / 1e9, 1) for m in broker_errors],
        "mem_available_mb_min": min((m["mem_available_mb"] for m in mon.samples), default=None),
        "postgres": pg_delta(before, after, active_s, acked),
        "latency_ms": seg,
        "outlier_threshold_s": a.threshold_s,
        "outlier_episodes": episodes,
        "timeseries_1s": [{**row, **({"durable_lag": m["durable_lag"], "cpu": m["cpu_cores"],
                                       "load1": m["load1"]} if (m := _nearest(mon.samples, t0_ns, row["s"])) else {})}
                          for row in series],
        "docker_samples": mon.docker,
        "matches_oracle": oracle,
    }  # fmt: skip
    drop_topic(topic)
    e2e = seg["e2e_from_schedule"]
    print(f"[{a.label} #{k}] {a.rate} ev/s x {active_s:.0f}s: achieved "
          f"{result['throughput']['achieved_produce_eps']} | e2e(sched) p50/p99/max "
          f"{e2e['p50']}/{e2e['p99']}/{e2e['max']} ms | lag max {result['lag']['max_after_warmup']}"
          f" end {result['lag']['at_producer_end']} | episodes>{a.threshold_s}s: {len(episodes)}"
          f" | oracle={oracle}", flush=True)  # fmt: skip
    return result


def _nearest(samples, t0_ns, s):
    target = t0_ns + s * 10**9
    best = min(samples, key=lambda m: abs(m["t_ns"] - target), default=None)
    return best if best and abs(best["t_ns"] - target) < 2 * 10**9 else None


def check(seed, n_prod, robots, ticks, topic) -> bool:
    sys.path.insert(0, str(ROOT / "tests"))
    from sqlalchemy import create_engine, text
    from support import assert_state_matches_oracle, db_state

    eng = create_engine(owner_dsn().replace("postgresql://", "postgresql+psycopg://"))
    try:
        rows_expected: set = set()
        for i in range(n_prod):
            exp = expected(iter_generate(cfg_for(seed, i, robots, ticks)), transport="kafka",
                           track_event_outcomes=False)  # fmt: skip
            assert_state_matches_oracle(db_state(eng, {str(r) for r in exp.latest}), exp)
            rows_expected |= exp.rejection_rows
        with eng.connect() as c:
            got = {(r[0], r[1]) for r in c.execute(text(
                "SELECT dedup_key, reason_code FROM fleet.telemetry_rejections"
                " WHERE source_topic = :t"), {"t": topic})}  # fmt: skip
        assert got == rows_expected, f"quarantine differs: {len(got)} vs {len(rows_expected)}"
        return True
    except AssertionError as exc:
        print("ORACLE MISMATCH:", exc)
        return False
    finally:
        eng.dispose()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rate", type=int, required=True)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--consumers", type=int, default=3)
    ap.add_argument("--producers", type=int, default=2)
    ap.add_argument("--robots", type=int, default=1000)
    ap.add_argument("--mode", default="set:500")
    ap.add_argument("--flush-s", type=float, default=0.05)
    ap.add_argument("--idle-s", type=float, default=25.0)
    ap.add_argument("--trace-every", type=int, default=4)
    ap.add_argument("--threshold-s", type=float, default=1.0)
    ap.add_argument("--producer-conf", nargs="*", default=[], help="librdkafka key=value")
    ap.add_argument("--label", default="traced")
    ap.add_argument("--seed", type=int, default=int(time.time()) % 10**5)
    a = ap.parse_args()
    runs = [run_once(a, k) for k in range(a.repeat)]
    save(f"traced-{a.label}", {"experiment": "traced sustained rate", "args": vars(a),
                               "environment": environment(), "runs": runs})  # fmt: skip


if __name__ == "__main__":
    main()
