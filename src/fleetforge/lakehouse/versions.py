"""Pinned lakehouse versions. One place; tests check every Dockerfile, compose file
and dependency pin against it.

Compatibility evidence (checked 2026-11-13 against Maven Central, PyPI, Docker Hub
and the Trino 484 documentation; see docs/architecture/milestone-3.md):
  * Iceberg 1.12.0 publishes Spark runtimes for Spark 3.5, 4.0 and 4.1 only. Spark
    4.2.1 and 4.3.0 exist but have no Iceberg runtime, so Spark is 4.1.x.
  * Spark 4.1 runs on Java 17 or 21 and Scala 2.13; PySpark 4.1.4 needs
    Python >= 3.10, pandas >= 2.2, pyarrow >= 15 (declared). The pinned Python set
    resolves with `pip check` clean; runtime compatibility is verified by the
    deferred lakehouse tests, not yet by execution.
  * Trino 484 reads Iceberg format v1/v2 (v3 experimental), supports the JDBC
    catalog on PostgreSQL and bundles its own Java 25.
  * MinIO: upstream archived its repository and no longer publishes images. The
    last source release is pinned and built from source (see ADR 0012).
"""

SPARK = "4.1.4"
SCALA = "2.13"
ICEBERG = "1.12.0"
POSTGRES_JDBC = "42.7.14"
TRINO = "484"
JAVA_IMAGE = "eclipse-temurin:21.0.12.1_1-jre-noble"
GO_IMAGE = "golang:1.24.8-bookworm"  # the toolchain MinIO's go.mod declares
MINIO_RELEASE = "RELEASE.2025-10-15T17-29-55Z"
MINIO_COMMIT = "9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a"
MC_RELEASE = "RELEASE.2025-08-13T08-35-41Z"
MC_COMMIT = "7394ce0dd2a80935aded936b09fa12cbb3cb8096"
ICEBERG_FORMAT_VERSION = 2

SPARK_PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.1_{SCALA}:{ICEBERG}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG}",
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA}:{SPARK}",
    f"org.postgresql:postgresql:{POSTGRES_JDBC}",
)

PYTHON_PINS = {"pyspark": SPARK, "pandas": "2.3.3", "pyarrow": "25.0.1", "trino": "0.340.0"}
