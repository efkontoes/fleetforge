# ADR 0012 — Lakehouse storage and catalog: Iceberg on S3-compatible storage, JDBC catalog

Status: accepted for slice 1. **Open decision for the user: MinIO's distribution.**

## Context
Milestone 3 needs versioned analytical tables that Spark writes and Trino reads,
on a development machine with 3.7 GiB of memory that already hosts Milestone 2.
Three choices: table format, catalog, object store.

On 2026-11-13, checking the requested object store:
* `github.com/minio/minio` and `minio/mc` are **archived**; the last source release
  is `RELEASE.2025-10-15T17-29-55Z` (commit `9e49d5e`).
* `docker manifest inspect minio/minio:<any tag>` returns *access denied*, and the
  quay.io mirror is gone too. The only prebuilt image left is
  `bitnamilegacy/minio:2025.7.23`, which is frozen and predates the fix for
  advisory GHSA-jjjj-jwhf-8rgr (privilege escalation through session policies),
  shipped in the last release.

## Decision
* **Table format: Apache Iceberg v2.** Engine-neutral specification, hidden
  partitioning, snapshots with time travel, optimistic concurrency, first-class
  Trino support. Format v3 is not used because Trino 484 calls its support
  experimental.
* **Catalog: Iceberg JDBC catalog in its own PostgreSQL 17 container.** A commit
  is a conditional `UPDATE` of one row (the table's current metadata location), so
  a commit is atomic and concurrent commits are detected. It needs no catalog
  service process, and both Spark and Trino 484 support it. It never shares the
  Milestone 2 database.
* **Object store: MinIO, built from the last source release, pinned by commit**
  (`infrastructure/lakehouse/minio/Dockerfile`). Everything else speaks only the
  S3 API (Iceberg `S3FileIO`, Trino native S3), so the store is replaceable
  without code changes.

## Alternatives
| Option | Why not (now) |
|---|---|
| Delta Lake | Strong with Spark. Trino reads it too, but Iceberg's specification is engine-neutral, and its snapshots and partition transforms are what we want to teach |
| Apache Hudi | Optimized for record-level upserts in streaming ingestion; heavier, with less natural Trino interplay |
| REST catalog (Polaris, Lakekeeper, Nessie, Gravitino) | Production direction (credential vending, multi-table commits in some), but one more service. Revisit in slice 4 |
| Hive Metastore | Heavy (JVM plus its own database) and legacy for new deployments |
| `bitnamilegacy/minio` image | No build cost, but frozen, unmaintained, and missing a security fix |
| Garage, SeaweedFS (maintained, S3-compatible) | Good fits; not what was requested. Garage v2.5.0 and SeaweedFS have current images. **The recommended switch if a from-source MinIO build is not wanted** |

## Consequences
* The MinIO image must be compiled once (Go, an estimated 1.5–3 GiB RAM, and only
  while Milestone 2 is down). It will never receive upstream security fixes. For
  anything beyond local development, use a maintained S3 implementation.
* Iceberg does not enforce primary keys: every writer guarantees identity
  uniqueness, and tests check it.
* No multi-table transactions with the JDBC catalog: Silver pairs tables by
  build id (ADR 0014).
* The Spark and Trino catalog configurations must use the same Iceberg catalog name
  (`lake`). It is stored in every catalog row.
