# Milestone 3 lakehouse targets, kept out of the Milestone 2 Makefile until both
# milestones are merged:  make -f lakehouse.mk <target>
#
# NEVER run these while the Milestone 2 stack is up on the 3.7 GiB machine
# (docs/architecture/milestone-3.md, "Deferred execution"). lake-guard enforces it.

LAKE := docker compose --env-file .env.lakehouse -f infrastructure/lakehouse/compose.yaml
# dbt reads the working tree, so model changes need no image rebuild.
DBT_DEV := -e DBT_PROJECT_DIR=/workspace/dbt -e DBT_PROFILES_DIR=/workspace/dbt
LAKE_KAFKA := $(LAKE) -f infrastructure/lakehouse/compose.kafka.yaml

.PHONY: lake-guard lake-build lake-up lake-test lake-test-kafka lake-query lake-test-trino \
        lake-dbt lake-down lake-unit lake-gate-a lake-gate-a-trino

lake-unit:  ## pure lakehouse unit tests (no Java, no containers)
	.venv/bin/pytest tests/unit -k lakehouse

lake-guard:  ## refuse while Milestone 2 containers run (override: LAKE_FORCE=1)
	@if [ -z "$$LAKE_FORCE" ] && docker ps --format '{{.Names}}' | grep -v '^fleetforge-lakehouse-' | grep -q '^fleetforge-'; then \
	  echo "Milestone 2 containers are running; stop them first (or LAKE_FORCE=1)"; exit 1; fi

lake-build: lake-guard  ## build the Spark image
	$(LAKE) --profile jobs --profile query build

lake-up: lake-guard  ## Garage + catalog PostgreSQL (limits 0.45 GiB)
	mkdir -p build/lakehouse-evidence && chmod 777 build/lakehouse-evidence
	$(LAKE) up -d --wait garage catalog-db
	$(LAKE) up garage-init

lake-test: lake-up  ## Spark slice tests against the disposable test catalog (~1.6 GiB more)
	$(LAKE) --profile query stop trino  # never beside Spark on this machine
	$(LAKE) --profile jobs run --rm \
	  -e FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL=jdbc:postgresql://catalog-db:5432/iceberg_catalog_test \
	  -e FLEETFORGE_LAKEHOUSE_WAREHOUSE=s3://lakehouse/test-warehouse \
	  spark -m pytest -p no:cacheprovider --rootdir=/workspace /workspace/tests/lakehouse -m "lakehouse and not lakehouse_kafka and not lakehouse_trino" -v \
	  -o junit_family=legacy --junitxml=/evidence/junit-spark.xml

lake-query: lake-up  ## Trino on :8090 (~1.4 GiB; Spark must not be running)
	$(LAKE) --profile query up -d --wait trino

lake-test-trino: lake-query  ## dbt Gold over what lake-test left, twice (idempotent), then Trino checks
	$(LAKE) --profile query run --rm $(DBT_DEV) -e DBT_TARGET=test dbt build
	$(LAKE) --profile query run --rm $(DBT_DEV) -e DBT_TARGET=test dbt build
	$(LAKE) --profile jobs run --rm --no-deps \
	  spark -m pytest -p no:cacheprovider --rootdir=/workspace /workspace/tests/lakehouse -m lakehouse_trino -v \
	  -o junit_family=legacy --junitxml=/evidence/junit-trino.xml

lake-test-kafka:  ## Bronze from real Kafka: needs Milestone 2 core (Kafka, registry) up
	LAKE_FORCE=1 $(MAKE) -f lakehouse.mk lake-up
	$(LAKE_KAFKA) --profile jobs run --rm -e FLEETFORGE_LAKEHOUSE_DRIVER_MEMORY=768m \
	  -e FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL=jdbc:postgresql://catalog-db:5432/iceberg_catalog_test \
	  -e FLEETFORGE_LAKEHOUSE_WAREHOUSE=s3://lakehouse/test-warehouse \
	  spark -m pytest -p no:cacheprovider --rootdir=/workspace /workspace/tests/lakehouse -m lakehouse_kafka -v \
	  -o junit_family=legacy --junitxml=/evidence/junit-kafka.xml

lake-dbt: lake-query  ## dbt Gold models and data tests on the main catalog
	$(LAKE) --profile query run --rm dbt build

lake-down:  ## stop the lakehouse stack (keeps volumes)
	$(LAKE) --profile jobs --profile query down

# Gate A: Kafka -> Bronze -> incremental Silver -> releases -> dbt Gold -> Trino, on the
# disposable test catalog, with Milestone 2's Kafka and registry up (nothing else of it).
GATE_RUN := $(LAKE_KAFKA) --profile jobs run --rm \
  -e FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL=jdbc:postgresql://catalog-db:5432/iceberg_catalog_test \
  -e FLEETFORGE_LAKEHOUSE_WAREHOUSE=s3://lakehouse/test-warehouse spark
GATE_TRINO := $(LAKE) --profile jobs run --rm --no-deps spark /workspace/scripts/gate_a.py trino
GATE_DBT = $(LAKE) --profile query run --rm $(DBT_DEV) -e DBT_TARGET=test dbt
GATE_VARS = $(shell cat build/lakehouse-evidence/gate-a-vars.json)
GATE_REOPEN = $(shell cat build/lakehouse-evidence/gate-a-vars-reopen.json)

lake-gate-a:  ## Gate A end to end (needs Milestone 2 kafka and schema-registry up)
	LAKE_FORCE=1 $(MAKE) -f lakehouse.mk lake-up
	$(LAKE) --profile query stop trino
	$(GATE_RUN) /workspace/scripts/gate_a.py spark
	LAKE_FORCE=1 $(MAKE) -f lakehouse.mk lake-gate-a-trino

lake-gate-a-trino:  ## Gate A's Trino half: dbt Gold, frozen final days, reopen
	LAKE_FORCE=1 $(MAKE) -f lakehouse.mk lake-query
	$(GATE_DBT) build --vars '$(GATE_VARS)'
	$(GATE_TRINO) --stage first
	$(GATE_DBT) run --vars '$(GATE_VARS)'
	$(GATE_TRINO) --stage frozen
	$(GATE_DBT) run --vars '$(GATE_REOPEN)'
	$(GATE_TRINO) --stage reopened
	$(GATE_DBT) test --vars '$(GATE_VARS)'

lake-gate-c:  ## Gate C: export a Gate A release, train and register twice, check drift
	$(LAKE) --profile query stop trino
	docker build -q -f infrastructure/ml/Dockerfile -t fleetforge-ml:dev .
	sh scripts/gate_c.sh
