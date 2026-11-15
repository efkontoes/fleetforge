# ADR 0015 — Object storage: Garage, chosen by measurement

Status: accepted. Supersedes the object-store part of ADR 0012 (MinIO built from source).

## Context
ADR 0012 pinned MinIO's last source release because the project was archived and
its images withdrawn. The user asked for a maintained S3-compatible store instead,
chosen by verified capability, not by an "S3-compatible" label. The constraints are
the 3.7 GiB development machine, which Spark (1.6 GiB limit) and Trino (1.4 GiB)
must also fit on, no vulnerable legacy images, and no unnecessary source builds.

## What Iceberg and Trino actually need from S3
With a JDBC catalog, commits happen in PostgreSQL, so the store needs no conditional
writes or locking. It needs: put and overwrite with read-after-write, multipart
upload, ranged GETs (Parquet footers and column chunks), HEAD, `ListObjectsV2`
with pagination (orphan-file removal, purge), single and batch delete, and correct
404s. `tests/lakehouse/test_object_store.py` checks exactly these.

## Evaluation (2026-11-13; probes run one at a time on the same machine)

| Candidate | Version / image | License | S3 probe | Memory idle / peak | Setup |
|---|---|---|---|---|---|
| **Garage** | v2.5.0 (2026-10-24), `dxflrs/garage`, 28 MB, digest-pinned | AGPL-3.0 (run unmodified as a service) | **5/5 passed** | 6.5 MiB / **42.6 MiB** | Layout, key and bucket via admin API (`garage_init.py`, idempotent) |
| SeaweedFS | 4.51 (2026-10-26), `chrislusf/seaweedfs`, 197–282 MB | Apache-2.0 | 5/5 passed | 69 MiB / **500 MiB**, still holding 528 MiB afterwards | One process (`weed mini`), bucket from env |
| AIStor Free | `quay.io/minio/aistor/minio` | Proprietary (MinIO Software License; "redistribution is prohibited") | not run | — | Needs a licence file obtained by registration; without it "all S3 operations are blocked" |
| MinIO (archived) | `RELEASE.2025-10-15T17-29-55Z` from source | AGPL-3.0 | not run | — | Go source build (est. 1.5–3 GiB); no further security fixes |

The probe's peak is a burst of a 23 MiB multipart upload plus 1,050 parallel small
puts. Iceberg compatibility beyond raw S3 (Spark commits, snapshot reads, purge,
Trino reads) was then proven on Garage by the full lakehouse suite (see the spec's
test results).

## Decision
**Garage v2.5.0**, pinned by digest, single node, `replication_factor = 1`,
region `us-east-1` (the region Iceberg and Trino sign for), path-style access,
192 MiB container limit. Secrets (`GARAGE_RPC_SECRET`, `GARAGE_ADMIN_TOKEN`, S3 key)
come from `.env.lakehouse`; the config file holds none.

## Why not the others
* **SeaweedFS**: fully capable, Apache-2.0, and its built-in Iceberg REST catalog is
  interesting for slice 4. But it holds roughly 12× Garage's peak memory, and on this
  machine that is the difference between Spark fitting or not. It is the fallback
  (switch = image + init; the code speaks only S3).
* **AIStor Free**: proprietary, and gated on a licence file that CI and a fresh
  clone cannot obtain unattended.
* **Archived MinIO**: no maintenance, and a source build.

## Consequences
* Garage has no object versioning, lifecycle rules or object lock. Iceberg does not
  use them: table history lives in Iceberg snapshots, not in object versions.
* Garage needs an explicit layout before it serves data; `garage-init` must
  complete before Spark or Trino start (Compose `depends_on`).
* Production would run three Garage nodes with replication 3, or a managed S3.
