PY := .venv/bin/python
SEED ?= 42

.PHONY: help setup env up up-core up-processing up-cdc up-full down migrate test test-unit \
        test-stack test-stack-disruptive lint fmt sim publish bench bench-m2 bench-traced \
        backup-check dashboard up-bench

help:  ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-22s %s\n", $$1, $$2}'

setup:  ## create virtualenv and install dev dependencies
	python3.12 -m venv .venv
	.venv/bin/pip install -q --upgrade pip
	.venv/bin/pip install -q -e '.[dev]'

env:  ## generate .env with random local secrets (never overwrites)
	sh scripts/make-env.sh

up:  ## PostgreSQL only
	docker compose up -d --wait postgres

up-core:  ## + Kafka, schema registry, migrations, API
	docker compose --profile core up -d --build --wait

up-processing:  ## + telemetry projector
	docker compose --profile processing up -d --build --wait

up-cdc:  ## + Debezium (Kafka Connect) and the mission consumer
	docker compose --profile core --profile cdc up -d --build --wait

up-full:  ## everything, incl. Prometheus (:9090) and Grafana (:3000)
	docker compose --profile full up -d --build --wait

down:  ## stop everything (keeps volumes)
	docker compose --profile full down

migrate:  ## apply migrations to FLEETFORGE_DATABASE_URL
	.venv/bin/alembic upgrade head

test:  ## unit + integration + failure suites (Testcontainers PostgreSQL 17)
	.venv/bin/pytest -p no:logging tests/unit tests/integration tests/failure

test-unit:  ## fast, no database
	.venv/bin/pytest tests/unit

test-stack:  ## tests against the running stack (make up-full first)
	FLEETFORGE_STACK=1 .venv/bin/pytest -p no:logging tests/stack -m "not disruptive"

test-stack-disruptive:  ## stops/restarts Kafka, PostgreSQL, Debezium
	FLEETFORGE_STACK=1 .venv/bin/pytest -p no:logging tests/stack -m disruptive

lint:  ## static checks
	.venv/bin/ruff check src tests database benchmarks scripts
	.venv/bin/ruff format --check src tests database benchmarks scripts

fmt:  ## auto-format
	.venv/bin/ruff format src tests database benchmarks scripts

sim:  ## legacy HTTP path: fault-injected fleet to the API
	.venv/bin/fleet-sim run --seed $(SEED) --robots 20 --ticks 100 \
	  --duplicate-rate 0.05 --reorder-rate 0.05 --invalid-battery-rate 0.02 \
	  --missing-field-rate 0.02 --reboot-rate 0.01 --workers 4

publish:  ## Kafka path: fault-injected fleet to telemetry.raw.v1
	.venv/bin/fleet-sim publish --seed $(SEED) --robots 20 --ticks 100 \
	  --duplicate-rate 0.05 --reorder-rate 0.05 --invalid-battery-rate 0.02 \
	  --missing-field-rate 0.02 --reboot-rate 0.01

bench:  ## Milestone 1 HTTP benchmark
	$(PY) benchmarks/ingest_benchmark.py

bench-m2:  ## Milestone 2 drain sweep (see benchmarks/milestone-2-report.md)
	$(PY) benchmarks/m2_benchmark.py drain --modes per_event:1 loop:500 set:50 set:500

RATE ?= 5000
DURATION ?= 300

up-bench:  ## benchmark profile: core only (stops CDC, observability, compose projector)
	docker compose --profile full stop connect mission-consumer projector \
	  prometheus grafana kafka-exporter ops-exporter
	docker compose --profile core up -d --wait

bench-traced:  ## per-hop traced sustained run, e.g. make bench-traced RATE=5000 DURATION=300
	$(PY) benchmarks/m2_traced.py --rate $(RATE) --seconds $(DURATION) --label make

backup-check:  ## pg_dump + restore drill into a scratch database
	sh scripts/backup-restore-check.sh

dashboard:  ## regenerate the Grafana dashboard JSON
	$(PY) scripts/build_dashboard.py
