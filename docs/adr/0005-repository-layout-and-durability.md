# ADR 0005 — One Python package for M1; durable per-event commits

Status: accepted (Milestone 1)

## Layout
The blueprint proposes `apps/fleet-api`, `apps/fleet-simulator` and
`packages/*`. Milestone 1 has one deployable and one dependency set, so it ships
as a single package `src/fleetforge/` with strict internal modules
(`contracts`, `domain`, `services`, `db`, `api`, `simulator`). The split happens
when a second deployable with different dependencies appears: the M2 Kafka
state projector. The simulator already does not import the API or the database,
so moving it out is mechanical.

## Durability
`synchronous_commit` stays `on` and each telemetry event is its own transaction.
The measured cost on this machine (WSL2, Docker volume) is about 4.6 ms per
commit, against about 0.13 ms without waiting for the WAL flush (pgbench,
single client). That cost dominates single-producer latency; see
`benchmarks/README.md`.

We do not turn durability off to improve numbers. An acknowledged event that
disappears after a crash would violate the platform's core promise. Throughput
will instead come from amortizing commits: a batch endpoint, or the M2 Kafka
consumer committing N events per transaction and committing offsets after the
database commit.

## Test database
Tests use the same durable configuration as the benchmark. They are slower
(about 1.5 minutes for the full suite of 85 tests on this machine), but there is no hidden
difference between tested and deployed behaviour.
