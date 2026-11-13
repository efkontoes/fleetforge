"""Python GC behaviour of the projector's CPU-bound stage (decode + validate).

    .venv/bin/python benchmarks/projector_gc.py

The traced benchmark showed a generation-2 collection every ~2.5 s in each
projector process, 60-100 ms each (up to 590 ms). Hypothesis: a 500-record batch
allocates thousands of temporaries, so gen0/gen1 collections run *during* the batch
while those temporaries are still referenced, promote them, and trigger frequent
full scans of every long-lived object (SQLAlchemy, Avro schemas, librdkafka...).

This runs BatchProcessor._prepare (no database, no broker) over encoded records in
batches of 500 under three settings and reports pause counts and CPU per event.
"""

from __future__ import annotations

import gc
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from support import mock_codec  # noqa: E402

from fleetforge.projector.processor import BatchProcessor, SourceRecord  # noqa: E402
from fleetforge.simulator.fleet import SimConfig, Telemetry, generate  # noqa: E402
from fleetforge.simulator.kafka_publisher import kafka_value  # noqa: E402

N_BATCHES = 120
BATCH = 500


def records():
    items = generate(SimConfig(seed=5, robots=200, ticks=300))
    enc, dec = mock_codec()
    boots, fw, out = {}, {}, []
    for it in items:
        if it.kind == "robot":
            fw[it.robot_id] = it.firmware_version
        elif it.kind == "boot":
            boots[it.boot_id] = 1
        elif isinstance(it, Telemetry):
            out.append(kafka_value(it, enc, boots, fw, "gc"))
    # Only bytes are kept (not tracked by the GC), like a consumer that holds one batch:
    # a long list of tracked objects would inflate the default mode's full scans.
    return out, dec


def run(label, setup):
    recs, dec = records()
    proc = BatchProcessor(None, dec)  # _prepare never touches the engine
    gc.collect()
    setup()
    pauses = []
    start = {}

    def cb(phase, info):
        if phase == "start":
            start["t"] = time.perf_counter_ns()
        else:
            pauses.append(((time.perf_counter_ns() - start["t"]) / 1e6, info["generation"]))

    gc.callbacks.append(cb)
    t0, c0 = time.perf_counter(), time.process_time()
    n = 0
    for b in range(N_BATCHES):
        first = (b * BATCH) % (len(recs) - BATCH)
        batch = [
            SourceRecord("t", 0, first + i, v) for i, v in enumerate(recs[first : first + BATCH])
        ]
        prepared = [proc._prepare(r) for r in batch]
        n += len(prepared)
        del prepared
    cpu = time.process_time() - c0
    wall = time.perf_counter() - t0
    gc.callbacks.remove(cb)
    gen2 = [p for p, g in pauses if g == 2]
    print(f"{label:<32} events {n:>6} | {cpu / n * 1e6:6.1f} us CPU/event | wall {wall:5.2f}s |"
          f" gen2 collections {len(gen2):>3}, max {max(gen2, default=0):6.1f} ms,"
          f" total {sum(gen2):7.1f} ms | all pauses > 5 ms: {sum(1 for p, _ in pauses if p > 5)}")  # fmt: skip
    gc.unfreeze()
    gc.set_threshold(700, 10, 10)
    return {"label": label, "cpu_us_per_event": cpu / n * 1e6, "gen2": len(gen2),
            "gen2_max_ms": max(gen2, default=0), "gen2_total_ms": sum(gen2),
            "median_pause_ms": statistics.median(p for p, _ in pauses) if pauses else 0}  # fmt: skip


if __name__ == "__main__":
    # Same order every time; each setting regenerates its inputs so heaps are comparable.
    import sqlalchemy  # noqa: F401  (load what the real process has loaded)

    from fleetforge.projector import main as _main  # noqa: F401

    run("default (700, 10, 10)", lambda: None)
    run("gc.freeze()", gc.freeze)
    run("gc.freeze() + (50000, 20, 100)", lambda: (gc.freeze(), gc.set_threshold(50_000, 20, 100)))
