# FleetForge

A fault-tolerant data platform for autonomous robot fleets. It receives
unreliable telemetry from simulated warehouse robots, keeps a correct
operational state, and, in later milestones, produces trustworthy historical and
ML datasets.

| Milestone | Scope | Status |
|---|---|---|
| 1 | HTTP → PostgreSQL: ordering, idempotency, transactional outbox | done |
| 2 | Kafka ingestion, batched projector, Debezium CDC, quarantine/replay, observability, roles | **this branch** |
| 3 | Bronze/silver lakehouse | next |

## Architecture (Milestone 2)

```
robots ──HTTP──▶ Fleet API ──▶ PostgreSQL ◀── telemetry projector(s) ◀── Kafka telemetry.raw.v1 ◀── robots (Avro)
                     │             │ WAL
                     │             ▼
                     │          Debezium ──▶ fleet.mission-events.v1 ──▶ mission consumer ──▶ downstream.mission_status
                     └── outbox (same transaction as the mission change)
```

Guarantee: **at-least-once transport, effectively-once state changes**
(idempotent processing + ordering keys + database constraints + a transactional
offset ledger).

Measured on a 4-core laptop:
* **5,000 events/s sustained** end to end, typical p99 event→state ≈ 0.25 s
  (one run in four had a 2.1 s tail).
* Up to ~8,800 events/s still keeping up.
* Milestone 1's HTTP path manages 293 events/s.

Details: [benchmark report](benchmarks/milestone-2-report.md).

## Quick start

Requirements: Linux or WSL2, Docker with Compose v2, Python 3.12.

```bash
make setup env        # virtualenv, .env with random local secrets
make test             # unit + integration + failure tests (Testcontainers PostgreSQL 17)
make up-full          # whole stack; Grafana http://localhost:3000 (admin / see .env)
make publish          # fault-injected fleet into Kafka -> projector -> PostgreSQL
make test-stack       # tests against the live stack (Kafka, registry, Debezium)
make down
```

Smaller footprints: `make up-core` (Kafka, registry, API), `make up-processing`
(+ projector), `make up-cdc` (+ Debezium). See the [runbook](docs/operations/runbook.md).

## Tools

| Command | Purpose |
|---|---|
| `fleet-sim publish / run / generate` | deterministic fleet with fault injection → Kafka / HTTP / file |
| `fleet-projector` | telemetry projector (`FLEETFORGE_PROJECTOR_MODE=set`, batch 500) |
| `fleet-mission-consumer` | independent consumer of outbox CDC events |
| `fleet-replay event / offsets / quarantine / correct [--dry-run]` | controlled replay |
| `fleet-schemas register / check --revision N` | schema registration and compatibility gate |
| `python -m fleetforge.ops.retention` | guarded outbox retention |

## Layout

```
src/fleetforge/
  contracts/        HTTP contracts, Avro schemas (avro/*.avsc)
  domain/           pure rules: validation, ordering, batch planner, error taxonomy
  services/         Fleet API transaction boundaries
  db/               engine, single-row SQL (repository), set-based SQL (batch)
  api/              FastAPI app
  kafka/            schema registry + Avro codec
  projector/        telemetry projector, mission consumer, replay
  simulator/        fleet model, faults, HTTP sender, Kafka publisher, oracle
  ops/              role provisioning, ops exporter, retention
database/migrations/   0001-0007, hand-written SQL
infrastructure/        Kafka topics, Debezium connectors, Prometheus, Grafana
tests/{unit,integration,failure,stack}/
benchmarks/            harnesses, raw results, reports
docs/                  architecture, ADRs 0001-0010, learning notes, runbook
```

## Documentation
* [Milestone 2 architecture](docs/architecture/milestone-2.md) · [Milestone 1 architecture](docs/architecture/milestone-1.md)
* [Learning notes: Kafka, idempotency, WAL, CDC, outbox, backpressure, schema evolution, replay](docs/learning/milestone-2.md)
* [Milestone 2 benchmark report](benchmarks/milestone-2-report.md) · [Milestone 1 benchmarks](benchmarks/README.md)
* [Operations runbook](docs/operations/runbook.md)
* ADRs: [0001](docs/adr/0001-ordering-key-server-assigned-boot-number.md) ordering key ·
  [0002](docs/adr/0002-conditional-upsert-for-latest-state.md) conditional upsert ·
  [0003](docs/adr/0003-transactional-outbox-with-idempotency-keys.md) outbox ·
  [0004](docs/adr/0004-validation-layers-and-rejection-audit.md) validation ·
  [0005](docs/adr/0005-repository-layout-and-durability.md) layout & durability ·
  [0006](docs/adr/0006-kafka-ingestion-with-database-offset-ledger.md) Kafka + offset ledger ·
  [0007](docs/adr/0007-set-based-batch-projection.md) set-based batching ·
  [0008](docs/adr/0008-outbox-cdc-with-debezium.md) Debezium CDC ·
  [0009](docs/adr/0009-error-taxonomy-quarantine-replay.md) quarantine & replay ·
  [0010](docs/adr/0010-local-security-and-environment.md) security & environment
