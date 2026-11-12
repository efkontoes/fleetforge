# FleetForge

A fault-tolerant data platform for autonomous robot fleets. It receives
unreliable telemetry from simulated warehouse robots, keeps a correct
operational state, and (in later milestones) produces trustworthy historical
and ML datasets.

**Current status: Milestone 1, the operational foundation.** A robot sends
unreliable data, and PostgreSQL maintains the correct state. Kafka, Debezium,
Spark, Iceberg and Airflow are intentionally absent until there is a measured
problem for them to solve.

## Quick start

Requirements: Linux or WSL2, Docker with Compose v2, Python 3.12.

```bash
make setup          # .venv + dependencies
make test           # 85 tests against a throwaway PostgreSQL 17 (Testcontainers)
make app-up         # PostgreSQL + migrations + API -> http://localhost:8000/docs
make sim            # send a fault-injected fleet; checks results against the oracle
make bench          # ingestion / read benchmark (see benchmarks/README.md)
make down
```

`make sim` compares outcomes with an oracle that assumes an empty database.
Rerunning the same seed against existing data gives `matches_oracle: false`,
because every event is now a duplicate or stale. That is correct behaviour.

## What Milestone 1 guarantees, and the tests that prove it

| # | Acceptance scenario | Test |
|---|---|---|
| 1 | Observations 100, 101, 102 → state reflects 102 | `tests/integration/test_telemetry_ingestion.py::test_normal_operation_latest_observation_wins` |
| 2 | 102 redelivered → no new logical update | `…::test_duplicate_delivery_creates_no_logical_update` |
| 3 | 101 after 102 → state unchanged | `…::test_out_of_order_delivery_leaves_state_unchanged` |
| 4 | Concurrent writers → the correct version wins | `tests/integration/test_concurrency.py` (forced interleavings + stress) |
| 5 | Battery 150% → rejected, machine-readable reason | `…::test_battery_150_rejected_with_machine_readable_reason` |
| 6 | Mission assignment fails midway → nothing commits | `tests/failure/test_transaction_rollback.py` |
| 7 | Same seed → same events and same injected faults | `tests/unit/test_simulator.py`, `tests/integration/test_simulation_e2e.py` |

## Layout

```
src/fleetforge/
  contracts/   wire contracts (telemetry v1, API bodies)
  domain/      pure rules: validation, ordering, mission state machine
  services/    transaction boundaries
  db/          engine + every SQL statement
  api/         FastAPI app (HTTP mapping only)
  simulator/   deterministic fleet, fault injection, sender, oracle
database/migrations/   Alembic, hand-written SQL
tests/{unit,integration,failure}/
benchmarks/            benchmark script + measured results
docs/architecture/     data flow, ERD, contracts, failure handling
docs/adr/              architecture decision records
```

## Documentation
* [Milestone 1 architecture: data flow, ERD, API, ordering, failures](docs/architecture/milestone-1.md)
* ADRs: [ordering key](docs/adr/0001-ordering-key-server-assigned-boot-number.md) ·
  [conditional upsert](docs/adr/0002-conditional-upsert-for-latest-state.md) ·
  [outbox](docs/adr/0003-transactional-outbox-with-idempotency-keys.md) ·
  [validation & audit](docs/adr/0004-validation-layers-and-rejection-audit.md) ·
  [layout & durability](docs/adr/0005-repository-layout-and-durability.md)
* [Benchmark results](benchmarks/README.md)
