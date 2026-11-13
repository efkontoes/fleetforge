# Milestone 3 lakehouse targets, kept out of the Milestone 2 Makefile until both
# milestones are merged:  make -f lakehouse.mk <target>
#
# NEVER run these while the Milestone 2 stack is up on the 3.7 GiB machine
# (docs/architecture/milestone-3.md, "Deferred execution"). lake-guard enforces it.

LAKE := docker compose --env-file .env.lakehouse -f infrastructure/lakehouse/compose.yaml
LAKE_KAFKA := $(LAKE) -f infrastructure/lakehouse/compose.kafka.yaml

.PHONY: lake-guard lake-build lake-up lake-test lake-test-kafka lake-query lake-test-trino \
        lake-down lake-unit

lake-unit:  ## pure lakehouse unit tests (no Java, no containers)
	.venv/bin/pytest tests/unit -k lakehouse

lake-guard:  ## refuse while Milestone 2 containers run (override: LAKE_FORCE=1)
	@if [ -z "$$LAKE_FORCE" ] && docker ps --format '{{.Names}}' | grep -q '^fleetforge-'; then \
	  echo "Milestone 2 containers are running; stop them first (or LAKE_FORCE=1)"; exit 1; fi

lake-build: lake-guard  ## build MinIO (from source) and the Spark image
	$(LAKE) --profile jobs build

lake-up: lake-guard  ## MinIO + catalog PostgreSQL (~0.6 GiB)
	$(LAKE) up -d --wait minio catalog-db
	$(LAKE) up minio-init

lake-test: lake-up  ## Spark slice tests against the disposable test catalog (~1.6 GiB more)
	$(LAKE) --profile jobs run --rm \
	  -e FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL=jdbc:postgresql://catalog-db:5432/iceberg_catalog_test \
	  -e FLEETFORGE_LAKEHOUSE_WAREHOUSE=s3://lakehouse/test-warehouse \
	  spark -m pytest -p no:cacheprovider tests/lakehouse -m "lakehouse and not lakehouse_kafka and not lakehouse_trino" -v

lake-query: lake-up  ## Trino on :8090 (~1.4 GiB; Spark must not be running)
	$(LAKE) --profile query up -d --wait trino

lake-test-trino: lake-query  ## Trino reads what lake-test left (no JVM in the test process)
	$(LAKE) --profile jobs run --rm --no-deps \
	  spark -m pytest -p no:cacheprovider tests/lakehouse -m lakehouse_trino -v

lake-test-kafka:  ## Bronze from real Kafka: needs Milestone 2 core (Kafka, registry) up
	LAKE_FORCE=1 $(MAKE) -f lakehouse.mk lake-up
	$(LAKE_KAFKA) --profile jobs run --rm \
	  -e FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL=jdbc:postgresql://catalog-db:5432/iceberg_catalog_test \
	  -e FLEETFORGE_LAKEHOUSE_WAREHOUSE=s3://lakehouse/test-warehouse \
	  spark -m pytest -p no:cacheprovider tests/lakehouse -m lakehouse_kafka -v

lake-down:  ## stop the lakehouse stack (keeps volumes)
	$(LAKE) --profile jobs --profile query down
