# Benchmarks — Milestone 1 baseline

Every number below was measured. Raw results are in `results/*.json`.

## Setup
* **Host:** Windows + WSL2 (Ubuntu 24.04), 8 vCPUs, 3.7 GiB RAM visible to WSL.
  Client, API and database share this host.
* **Database:** PostgreSQL 17.11 in Docker, default config plus `wal_level=logical`,
  with `synchronous_commit=on` and `fsync=on` (durable).
* **API:** Docker, synchronous FastAPI handlers.
* **Client:** `benchmarks/ingest_benchmark.py`. One Python process with
  httpx; robots are partitioned across worker threads by `robot_id`.
* **Workload:** 500 robots × 20 observations (about 10,200 telemetry requests per
  step, including injected duplicates). Faults: 2% duplicate, 2% reorder,
  1% invalid battery, 1% missing field, 0.2% reboot. Registration traffic is
  sent before the timer starts.
* Every step was checked against the simulator oracle: **all steps matched**.

```
docker compose --profile app up -d --build --wait
.venv/bin/python benchmarks/ingest_benchmark.py --robots 500 --ticks 20 --workers 1,4,8,16,32
```

## Ingestion: one API process (`20261112T115327Z_api-1proc.json`)

| client workers | events/s | p50 ms | p95 ms | p99 ms | rejected* |
|---:|---:|---:|---:|---:|---:|
| 1 | 76 | 11.3 | 22.7 | 27.3 | 424 |
| 4 | 274 | 12.1 | 20.3 | 29.0 | 408 |
| 8 | 250 | 28.6 | 38.8 | 45.9 | 416 |
| 16 | 190 | 71.3 | 142.2 | 238.4 | 473 |
| 32 | 204 | 134.2 | 231.1 | 296.3 | 416 |

## Ingestion: four API processes, `WEB_CONCURRENCY=4` (`20261112T120539Z_api-4proc.json`)

| client workers | events/s | p50 ms | p95 ms | p99 ms | rejected* |
|---:|---:|---:|---:|---:|---:|
| 1 | 67 | 13.3 | 23.1 | 26.8 | 434 |
| 4 | 234 | 12.1 | 21.2 | 35.1 | 445 |
| 8 | 299 | 17.9 | 38.9 | 53.9 | 445 |
| 16 | 327 | 36.9 | 72.2 | 94.8 | 439 |
| 32 | 354 | 69.8 | 159.1 | 215.9 | 407 |

\* "rejected" counts every event not applied and not a duplicate: STALE,
BATTERY_OUT_OF_RANGE and MISSING_FIELD (about 4% of events by design).

## Reads

| measurement | value |
|---|---|
| `GET /v1/robots/{id}/state`, 2,000 sequential requests, client-side | p50 4.2 ms, p95 5.6 ms, p99 6.4 ms |
| State query inside PostgreSQL (`EXPLAIN ANALYZE`), about 5,300 robots | 0.08 ms; primary-key index scan on `robot_latest_state` plus index scan on `uq_missions_one_active_per_robot` |

## Where the time goes (diagnostics)

| probe | result |
|---|---|
| `pgbench`, 1 client, single-row insert, `synchronous_commit=on` | 4.6 ms per commit (218 tps) |
| same, `synchronous_commit=off` (diagnostic only, not used) | 0.13 ms (7,755 tps) |
| same, 8 clients, `synchronous_commit=on` (group commit) | 9.0 ms, 892 tps |
| `GET /healthz` round trip (no DB) | p50 1.6 ms, p95 2.6 ms |
| CPU during 16 workers, 1 API process | API 135–177%, PostgreSQL about 20% |
| CPU during 32 workers, 4 API processes | API 212% (unevenly spread across processes), PostgreSQL 61%, **client 134%**, host 37% idle |

## Interpretation
1. **Single-producer latency is dominated by the durable commit.** About 4.6 ms
   of the roughly 11–13 ms p50 is the WAL flush, about 1.6 ms is HTTP, and the
   rest is the API's CPU work across about 3 SQL round trips.
2. **One API process is CPU-bound by the Python GIL.** Throughput fell beyond
   4 workers as threads contended for the interpreter (190–204 ev/s at 16–32
   workers).
3. **Four processes raised the plateau to about 354 ev/s.** That is still not the
   server's ceiling. The single-process Python load generator was itself at
   134% CPU, and keep-alive connections distributed load unevenly across API
   processes. **Treat 354 ev/s as a lower bound for this API on this host.**
4. **The design target (5,000 events/s, 500 robots at 10 Hz) is not met by
   one-HTTP-request-and-one-commit-per-event.** We expected that. The M2 path is
   batching: N events per transaction amortizes the 4.6 ms flush. The Kafka state
   projector naturally consumes in batches.
5. Reads are cheap. 0.08 ms in the database, so the 4 ms client-side figure is
   mostly HTTP and Python overhead.

## Not measured yet
* A load generator that is not itself a bottleneck (multi-process, or a native
  tool such as `wrk`/`k6`).
* A batch ingestion path.
* Native Linux or cloud disks. WSL2 virtual-disk flush latency is probably
  higher than on a bare-metal NVMe or on RDS, but that has not been measured.
