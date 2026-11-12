PY := .venv/bin/python
SEED ?= 42

.PHONY: help setup db-up app-up down migrate test test-unit lint fmt sim bench

help:  ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

setup:  ## create virtualenv and install dev dependencies
	python3.12 -m venv .venv
	.venv/bin/pip install -q --upgrade pip
	.venv/bin/pip install -q -e '.[dev]'

db-up:  ## start PostgreSQL only
	docker compose up -d --wait postgres

app-up:  ## start PostgreSQL + migrations + API (http://localhost:8000/docs)
	docker compose --profile app up -d --build --wait

down:  ## stop everything (keeps the data volume)
	docker compose --profile app down

migrate:  ## apply migrations to the database in FLEETFORGE_DATABASE_URL
	.venv/bin/alembic upgrade head

test:  ## full suite against a throwaway PostgreSQL 17 (Testcontainers)
	.venv/bin/pytest -p no:logging

test-unit:  ## fast, no database
	.venv/bin/pytest tests/unit

lint:  ## static checks
	.venv/bin/ruff check src tests database benchmarks
	.venv/bin/ruff format --check src tests database benchmarks

fmt:  ## auto-format
	.venv/bin/ruff format src tests database benchmarks

sim:  ## send a fault-injected fleet to a running API
	.venv/bin/fleet-sim run --seed $(SEED) --robots 20 --ticks 100 \
	  --duplicate-rate 0.05 --reorder-rate 0.05 --invalid-battery-rate 0.02 \
	  --missing-field-rate 0.02 --reboot-rate 0.01 --workers 4

bench:  ## benchmark a running API (see benchmarks/README.md)
	$(PY) benchmarks/ingest_benchmark.py
