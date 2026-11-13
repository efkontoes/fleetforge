# FleetForge — Milestone 3 specification: lakehouse and data transformation

Status: **slice 1 implemented as code and unit tests; Spark, Iceberg, Trino and
Kafka execution deferred** (see [Deferred execution](#deferred-execution)).
Branch `milestone-3-lakehouse`, based on `e683d32` (the pushed head of
`milestone-2-distributed-ingestion`, PR #1), in a separate worktree. Milestone 2's
working tree, containers, volumes and database were not touched.

Companion documents: [exact table schemas](milestone-3-tables.md) (generated from
code), ADRs [0012](../adr/0012-lakehouse-storage-and-catalog.md),
[0013](../adr/0013-bronze-idempotent-ingestion.md),
[0014](../adr/0014-silver-deterministic-rebuild-and-outcome-lineage.md), and the
[learning guide](../learning/milestone-3.md).

## 1. Objective and scope

Turn unreliable raw robot telemetry into trustworthy, versioned, queryable
datasets:

    Kafka raw telemetry + CDC/outbox streams -> Bronze -> Silver -> Gold -> Trino

In scope: Spark for transformations, Iceberg tables (Parquet files) in
S3-compatible storage (MinIO), Trino for SQL, Python for validation logic.
Out of scope until later milestones: Airflow, dbt, the LLM assistant. Jobs are
plain CLI entry points; whatever schedules them later only needs their exit codes.

## 2. Architecture

```mermaid
flowchart LR
  subgraph M2["Milestone 2 (unchanged)"]
    P[robots / simulator] -->|Avro, key robot_id| T[(telemetry.raw.v1<br/>6 partitions, 7 d)]
    API[Fleet API] --> PG[(PostgreSQL)]
    PG -->|Debezium table CDC| FW[(fleetforge.fleet.firmware_versions<br/>1 partition, compacted)]
    PG -->|outbox EventRouter| RE[(fleet.robot-events.v1<br/>3 partitions, 30 d)]
    REG[schema registry]
  end
  subgraph M3["Milestone 3 lakehouse (separate Compose project)"]
    BJ[bronze_ingest<br/>Spark batch Kafka source] --> B[(bronze.kafka_records<br/>bronze.registry_schemas)]
    B --> SJ[silver_build<br/>mapInPandas row stage +<br/>window-function set stage]
    SJ --> S[(silver.telemetry_events<br/>silver.telemetry_record_outcomes<br/>silver.firmware_contracts<br/>silver.robot_registry)]
    S -.slice 3.-> G[(gold.*)]
    CAT[(Iceberg JDBC catalog<br/>PostgreSQL, own instance)]
    S3[(MinIO: Parquet + Iceberg metadata)]
    TR[Trino 484]
  end
  T --> BJ
  FW --> BJ
  RE --> BJ
  REG -->|schemas archived once per id| BJ
  B & S --- S3
  BJ & SJ & TR --- CAT
  TR --> S
```

Every table is an Iceberg table: Parquet data files and Iceberg metadata in
MinIO, one pointer per table in the JDBC catalog. Spark writes, Trino reads,
through the same catalog.

## 3. Milestone 2 contracts, as implemented

Read from the code at `e683d32`, not from intentions.

| Contract | What the implementation provides | Source |
|---|---|---|
| Telemetry topic | `telemetry.raw.v1`, 6 partitions, RF 1, `retention.ms` 7 days, delete policy; declared, never auto-created | `infrastructure/kafka/create-topics.sh` |
| Other topics | `fleet.mission-events.v1`, `fleet.robot-events.v1` (3 partitions, 30 days); `fleetforge.fleet.firmware_versions` (1 partition, compacted) | same |
| Key / partitioning | key = UTF-8 `robot_id`, so all of a robot's events share one partition per topic incarnation | `simulator/kafka_publisher.py` |
| Value framing | Confluent wire format: magic byte 0, 4-byte big-endian schema id, Avro body. Decoded with the *writer* schema looked up by id | `kafka/codec.py` |
| Schema revisions | v1 and v2 (`heading_rad` optional, BACKWARD compatible); v3 is deliberately incompatible and only used to prove the registry rejects it. Producers never auto-register | `contracts/avro/*.avsc`, `kafka/schemas.py` |
| Event identity | `event_id` (UUID), identical across retries and redeliveries | Avro doc |
| Robot and boot | `robot_id`, `boot_id`, server-assigned `boot_number` (≥ 1), `sequence_number` (≥ 0, monotonic within a boot) | Avro, `domain/ingest.py` |
| Ordering | business order = `(boot_number, sequence_number)` per robot. Kafka guarantees order within a partition only. Robot clock (`observed_at`) is "stored, never used for ordering" | Avro doc, ADR 0001 |
| Times | `observed_at` (robot clock, timestamp-micros), `producer.produced_at` (producer clock), Kafka timestamp = CreateTime (producer clock, topic default) | Avro, topic config |
| Firmware | the event carries `firmware_version` only. Contracts `(model_id, version) -> battery_unit ∈ {PERCENT, FRACTION}, position_unit ∈ {M, MM}`; every seeded contract is PERCENT/M; M2 never converts units, it only checks that the contract exists | migration 0001, `processor.py` |
| CDC of firmware contracts | Debezium JSON (schemas disabled), envelope `before/after/source/op/ts_ms`, `REPLICA IDENTITY FULL`, tombstones after deletes | `infrastructure/debezium/reference-connector.json` |
| Outbox events | EventRouter, expanded JSON payload, key = aggregate id, headers `eventType`, `outboxCreatedAt`. `RobotRegistered` carries `robot_id, robot_model, firmware_version, lifecycle_status, registered_at` | `services/robots.py`, outbox connector |
| Corrections | a correction is a new event (`event_id` = uuid5 of original and attempt, `correction_of` = original) with the original's ordering key | `projector/replay.py` |
| Quarantine | `fleet.telemetry_rejections` in PostgreSQL with reason, category, `retry_eligible`, status, Kafka coordinates, payload and bytes | migration 0004 |
| Data-quality rules | structural (`observation_from_record`: required fields, schema_version = 1, finite numbers, seq ≥ 0, boot ≥ 1), semantic (battery in [0, 100], observed_at ≤ processing time + 300 s), referential (robot, boot, boot number, firmware contract) | `domain/ingest.py`, `domain/telemetry.py`, `processor.py` |
| Delivery | at-least-once from producers (duplicates across producer sessions); the projector is idempotent; no Kafka transactions on telemetry, so its offsets are contiguous | `kafka_publisher.py`, ADR 0011 |

### Contract gaps (documented before transforming)

| # | Gap | Consequence for the lakehouse | Handling now | Proposed fix (M2 side, not done here) |
|---|---|---|---|---|
| G1 | Telemetry has no robot model; contracts are keyed by `(model, version)` | Unit conversion needs `RobotRegistered` from the outbox topic, which keeps 30 days. Robots registered before Bronze started and more than 30 days ago are unknown | `UNKNOWN_ROBOT`, retry-eligible; a rebuild accepts them once the registry is known | Table CDC of `fleet.robots` into a compacted topic, or a one-off registry export into Bronze |
| G2 | Boot registration emits no event | `UNKNOWN_BOOT` and `BOOT_NUMBER_MISMATCH` cannot be reproduced; `boot_number` is copied, not verified | Documented on the column | Outbox event `BootRegistered` |
| G3 | `fleet-replay correct` applies corrections directly; they never reach Kafka | M2 corrections are invisible to Bronze; the lakehouse's history keeps the rejected original | Silver supports corrections that *do* arrive through Kafka (`correction_of`) | Publish corrections to `telemetry.raw.v1` |
| G4 | The M2 quarantine lives in PostgreSQL only, judged with the projector's wall clock | Silver recomputes its own outcomes deterministically; the two can differ at the 300 s skew boundary and for registry checks (G1, G2) | Silver uses Bronze `ingested_at` as the reference time | Reconciliation report M2 quarantine vs Silver outcomes (slice 4) |
| G5 | No coordinate frame, origin, axis convention or heading zero/rotation sense in the contract; heading only from revision 2 | Positions and headings cannot be fused across sites or models | Heading only wrapped into [-π, π); no frame assumed | Add frame id and convention to the contract |
| G6 | All seeded firmware contracts are PERCENT/M; the firmware topic is compacted with the default `min.compaction.lag.ms` (0) | Conversion paths are never exercised by real data; intermediate contract versions can be compacted away before ingestion | Synthetic FRACTION/MM contract in the fixture, clearly labelled | Set `min.compaction.lag.ms` on the CDC topic |
| G7 | Telemetry has no mission id; missions use server time, telemetry robot time | Mission-aligned training episodes are not reliable | `episode_id` = `boot_id` | Mission id in telemetry, or clock-sync metadata |
| G8 | Only a future-skew check on `observed_at`; no clock-sync data | Old or reset robot clocks are accepted and land in old partitions | Not rejected (no rule exists); visible in Gold quality | Lower-bound rule plus clock offset estimation |
| G9 | Schema ids depend on the registry's storage (Apicurio, KafkaSQL topic) | If the registry is rebuilt, ids can change meaning and old Bronze bytes become undecodable | Bronze archives every schema id it sees (`bronze.registry_schemas`) | — |
| G10 | Mission events are JSON without a registered schema | Contract lives only in consumer code | Ingested to Bronze as bytes; not modelled in slice 1 | Register a JSON or Avro schema |
| G11 | Kafka timestamps are CreateTime (producer clock) | `kafka_timestamp` is not a trustworthy arrival time | Stored as metadata only; arrival order uses Bronze ingestion | `message.timestamp.type=LogAppendTime` on the topic |
| G12 | Telemetry is low-rate scalar state: no images, scans, commands or actions | A learning dataset has no actions or rich observations | Gold product is labelled training-*candidate* | New streams, out of scope for M3 |

## 4. Layer design

Exact columns, types, nullability, partitioning and sort order:
[milestone-3-tables.md](milestone-3-tables.md). Defined once in
`src/fleetforge/lakehouse/tables.py`; a unit test keeps the document identical.

### Bronze: immutable raw history

* `bronze.kafka_records`: one row per Kafka record of every source topic, bytes
  exactly as read (key, value, headers), plus topic, **topic id**, partition,
  offset, Kafka timestamp and type, value SHA-256, framing and schema id parsed
  from the bytes, ingestion run id and time. Identity
  `(kafka_topic_id, kafka_partition, kafka_offset)`.
* Partitioned by `source_topic, days(ingested_at)`: the CDC topics stay small,
  arrival-day partitions are append-only, and retention by age is a partition drop.
  Files are sorted by `(kafka_partition, kafka_offset)`, which gives tight offset
  min/max statistics.
* Duplicates, stale events, malformed and invalid records are all kept: Bronze
  records what was delivered, not what is true.
* `bronze.registry_schemas`: archive of every schema id seen (FOUND or an
  authoritative NOT_FOUND), so history stays decodable without the registry (G9).

Idempotency and restart (ADR 0013): checkpoint in the Iceberg commit,
insert-if-absent by identity, a fail-closed planner, and a single-writer lease.

### Silver: validated canonical telemetry

Silver = F(Bronze snapshot, reference data, `TRANSFORM_VERSION`), rebuilt in
full in slice 1 (ADR 0014). Two stages:

**Row stage** (`silver.prepare`, runs in `mapInPandas`), in order. The first
failure decides the reason:

| Step | Rule | Outcome | Rule source |
|---|---|---|---|
| framing | Confluent-framed Avro | `MALFORMED_PAYLOAD` | M2 codec |
| schema | id archived; `NOT_FOUND` → reject; **not archived → job aborts** (recoverable, never a quarantine) | `MALFORMED_PAYLOAD` / abort | M2 codec 404 rule |
| decode | writer-schema decode succeeds | `MALFORMED_PAYLOAD` | M2 codec |
| contract | required fields, `schema_version = 1`, finite numbers, seq ≥ 0, boot ≥ 1 | `MISSING_FIELD`, `UNSUPPORTED_SCHEMA_VERSION`, `INVALID_FIELD` | M2 `observation_from_record`, unchanged |
| reference | robot model known (G1); firmware contract for (model, version) | `UNKNOWN_ROBOT`, `UNKNOWN_FIRMWARE` (both retry-eligible) | M2 taxonomy |
| conversion | battery × {1, 100}; position ÷ {1, 1000} (exactly rounded) | — | firmware contract |
| semantic | battery in [0, 100] **after** conversion; observed_at ≤ Bronze `ingested_at` + 300 s | `BATTERY_OUT_OF_RANGE`, `OBSERVED_AT_IN_FUTURE` | M2 `check_semantics`, deterministic reference time |
| repair | non-finite optional values → NULL; heading wrapped into [-π, π) | flags `HEADING_NOT_FINITE`, `MOTOR_TEMP_NOT_FINITE`, `HEADING_WRAPPED` | Silver |

**Set stage** (`silver.build`; Spark mirror `jobs/silver_build.set_stage`).
"First" always means first in Bronze arrival order
`(ingested_at, kafka_topic_id, kafka_partition, kafka_offset)`:

1. Rejections from the row stage keep their reason.
2. Per `event_id`: the first copy is the representative. A later copy with the same
   content fingerprint (business fields, excluding producer metadata and schema
   revision) is a `DUPLICATE`; with different content, `EVENT_ID_CONFLICT`.
3. Per ordering slot `(robot_id, boot_number, sequence_number)` among non-corrections:
   the first holds it; later ones are `SEQUENCE_CONFLICT` (M2 semantics).
4. Corrections: per target, the latest-arriving correction wins; the target (an
   accepted event or a rejected record) and losing corrections get `superseded_by`;
   `is_current = superseded_by IS NULL`.
5. `arrived_out_of_order`: an accepted event of the same robot with a greater
   ordering key arrived earlier.

Event-time ordering is the business key, never `observed_at` (robot clock).
Late events are *kept* (history is append-only by nature); lateness is a flag in
Silver and a recomputation trigger for Gold. Every Bronze telemetry record gets
exactly one row in `silver.telemetry_record_outcomes` (`ACCEPTED`, `DUPLICATE` or
`REJECTED` with reason, category, retry eligibility and JSON details). The
quarantine is the `REJECTED` subset, and its bytes are joined from Bronze by
coordinates. **Malformed records** (permanent, quarantined) are kept strictly
apart from **infrastructure errors** (registry unavailable, schema not archived,
lease taken, object store or catalog down), which abort the run before anything is
committed and are retried.

Schema evolution: decoding uses the writer schema (v1 and v2 both decode; a v1
record equals a v2 record with `heading_rad = NULL`). A new Silver column is an
Iceberg `ADD COLUMN` (metadata only) plus a rebuild that fills history from Bronze
bytes. A new business major version (`schema_version = 2`) is
`UNSUPPORTED_SCHEMA_VERSION` until Silver implements it, and then a rebuild applies
it to history.

### Gold: data products (specified; slice 3)

1. **Fleet telemetry and operational quality.**
   `gold.robot_daily_telemetry` (robot × observed day: events, expected vs missing
   sequences, out-of-order, duplicates, corrections, battery, distance over
   consecutive steps only, lidar faults, `is_final`) and
   `gold.pipeline_daily_quality` (ingest day × topic × outcome × reason).
   Late arrivals: a Silver build reports the observed days it changed. Gold
   recomputes exactly those days. Days older than the allowed lateness (7 days,
   Kafka retention) are `is_final`.
2. **Training-candidate robot state observations.**
   `gold.robot_state_observations` + `gold.training_dataset_releases`: stable ids
   (`observation_id` = current Silver event id), provenance (Kafka coordinates,
   Silver build), deterministic leakage-safe split by episode
   (`hash(episode_id, seed)`), immutable releases with a content hash.

**Why passing SQL validation does not make it training-ready.** A release stays
`CANDIDATE` until, at minimum:

* *Synchronization*: a common clock or estimated per-robot clock offsets (G8),
  bounded inter-sample jitter, and alignment across modalities (none beyond scalar
  state today, G12).
* *Episodes*: boundaries that mean something for the task (missions, G7), with no
  episode split across train and test, and no near-duplicate episodes across splits.
* *Features and labels*: actions or commands and a defined prediction target
  (absent, G12); coordinate frames (G5); units normalized per firmware (done);
  documented normalization statistics computed on train only.
* *Quality gates*: per-episode completeness (missing sequences), out-of-order and
  correction rates under thresholds, no `UNKNOWN_*` rejections in the selected
  window, class or condition balance reported.
* *Reproducibility*: release pinned to a Silver build and an Iceberg snapshot,
  content hash recorded, retention of that snapshot guaranteed (tag), and a data
  card listing known limitations.

## 5. Reliability: failure handling and replay rules

Action vocabulary: **retry** (no commit happened; rerun is safe), **quarantine**
(permanent data problem, recorded with reason, processing continues),
**reject** (excluded with a reason in outcomes; not retryable without new data),
**escalate** (stop; an operator decides, and the decision is recorded).

| # | Failure | Detection | Action | Why it is safe |
|---|---|---|---|---|
| F1 | Spark job crash (any stage) | process exit | **retry** | Nothing is visible until the Iceberg commit; written-but-uncommitted files are orphans, removed by `remove_orphan_files` (older-than window) |
| F2 | Kafka replay / re-read of ingested offsets | anti-join on identity; checkpoint | none needed | Insert-if-absent: zero new rows (unit and stack tests) |
| F3 | Duplicate events (producer retries, new producer session) | Silver step 2 | `DUPLICATE` outcome; counted on the event | Content fingerprint excludes delivery metadata |
| F4 | Same event id, different content | Silver step 2 | **reject** `EVENT_ID_CONFLICT` | First copy kept; producer bug surfaced |
| F5 | Late / out-of-order arrival | Silver step 5; Gold changed-days set | kept, flagged; Gold days recomputed; days older than 7 days are final | History is append-only; flags are recomputed every build |
| F6 | Schema change: new compatible revision | decode with writer schema | none | Registry enforces BACKWARD; v1 = v2 with NULL heading |
| F6b | Schema id not archived / registry unavailable | Silver driver check; Bronze archiver | **retry** (abort before commit) | Never quarantined: the record may be fine |
| F6c | Unsupported business major version | row stage | **reject** `UNSUPPORTED_SCHEMA_VERSION`; rebuild after support is added | Bronze keeps the bytes |
| F7 | Iceberg commit conflict or failure | exception at commit | **retry** the run | Appends rebase automatically; overwrites validate; a failed commit leaves the table at the previous snapshot |
| F7b | Commit outcome unknown (timeout after the catalog update) | `CommitStateUnknownException` | **retry**; the run re-reads the checkpoint from the table | Checkpoint lives in the table, so the truth is re-read, never assumed |
| F8 | Partial backfill (some ranges, some days) | — | Bronze: ranges are independent, idempotent appends. Silver: full rebuild in slice 1, so partial Silver states cannot exist | Each Silver build is one atomic overwrite per table |
| F9 | Corrections to history: telemetry corrections | Silver step 4 | supersede, keep both versions, `is_current` | Original remains for audit |
| F9b | Corrections to reference data (firmware contract update) | reference fingerprint changes the build id | rebuild applies the new contract everywhere | Deterministic; previous build readable by snapshot |
| F9c | Correction of a Silver rule | `TRANSFORM_VERSION` bump | rebuild | Old build readable by snapshot until expired |
| F10 | Concurrent writers | single-writer lease (catalog advisory lock); post-commit parent check | second writer exits (**retry later**); a parent mismatch **escalates** | Lease covers process death; parent check covers lease loss |
| F11 | Object store interruption | S3 errors from Iceberg FileIO | **retry** (run fails before commit) | Uncommitted files are orphans; metadata pointer unchanged |
| F11b | Catalog database interruption | JDBC errors | **retry** | Commit is a conditional UPDATE; either applied or not |
| F12 | Retention gap: checkpoint < Kafka low watermark | planner | **escalate**; `--accept-gaps` records `[from, to)` in the snapshot summary | Mirrors ADR 0011 |
| F12b | Topic recreated (new topic id) | planner; topic id re-checked after the read | **escalate**; `--accept-new-incarnation` | Identity includes topic id; old rows stay distinct |
| F12c | Checkpoint beyond log end (truncation, unclean election) | planner | **escalate**, no override | Records already served were lost by Kafka |
| F12d | Read did not return the planned offsets | `verify_batch` | **retry**, then escalate | Nothing committed |
| F13 | Reference data missing (robot or firmware) | row stage | **quarantine**, retry-eligible; next rebuild re-evaluates | Rebuild is total, so recovery needs no replay tool |
| F14 | Malformed reference record (CDC/outbox) | `build_reference` | recorded as anomaly in the Silver commit summary, part of the build id | Never silently skipped |

### Exactly-once: what is and is not claimed

* **Bronze**: each Kafka record identity appears **at most once**, guaranteed by
  insert-if-absent plus a single writer. It appears **at least once** for every
  offset that Kafka still retains and that the planner covered, because the
  checkpoint only advances in the same atomic Iceberg commit as the data. Together
  that is effectively-once *per record identity*, within these boundaries: one
  writer at a time (lease), Kafka retention (beyond it, an escalated gap), and one
  topic incarnation (beyond it, an escalated new incarnation). Spark's Kafka source
  is *not* transactional with Iceberg. There is no distributed transaction. Safety
  comes from idempotence, not from atomicity across the two systems.
* **Silver**: a build is a pure function of a pinned Bronze snapshot. Each table
  is replaced in one atomic commit. **Across tables there is no transaction**:
  for a moment, outcomes can be from build N+1 while events are from build N.
  Consumers pair tables by `silver_build_id`; events are written last and act as
  the publication marker.
* **Logical events**: Silver keeps one row per `event_id`. Two different events
  for the same slot are a `SEQUENCE_CONFLICT`, never two "current" rows.
* Not claimed: exactly-once from robot to Kafka (M2: at-least-once), or
  completeness beyond Kafka retention.

## 6. Technology inventory (pinned, compatibility checked 2026-11-13)

| Component | Pin | Why this version | Evidence |
|---|---|---|---|
| Apache Spark | 4.1.4 (Scala 2.13), PySpark 4.1.4 | Newest Spark line with an Iceberg runtime; 4.2.1 and 4.3.0 exist, but Iceberg publishes no runtime for them | Maven Central: `iceberg-spark-runtime-4.2_2.13` returns 404; `-4.1_2.13:1.12.0` exists |
| Apache Iceberg | 1.12.0 (`iceberg-spark-runtime-4.1_2.13`, `iceberg-aws-bundle`) | Latest release (2026-09-25); table format v2 | Maven Central |
| Spark Kafka source | `spark-sql-kafka-0-10_2.13:4.1.4` | Must equal the Spark version | Maven Central |
| PostgreSQL JDBC | 42.7.14 | Iceberg JDBC catalog driver | Maven Central |
| Java (Spark) | Temurin 21 JRE `21.0.12.1_1` (digest-pinned) | Spark 4.1 supports 17 and 21 | Docker Hub |
| Python libs | pandas 2.3.3, pyarrow 25.0.1, trino 0.340.0 | PySpark 4.1.4 declares pandas ≥ 2.2, pyarrow ≥ 15. Resolved together with the project lock on Python 3.12, `pip check` clean (`requirements-lakehouse.lock`) | PyPI metadata; **runtime compatibility not yet executed** |
| Trino | 484 (digest-pinned) | Latest; JDBC catalog; reads Iceberg v1 and v2 (v3 "experimental", so tables pin v2); bundles Java 25 | Trino 484 docs |
| Catalog DB | PostgreSQL 17 (same digest as M2 tests), separate container | Iceberg JDBC catalog: no extra catalog service on a 3.7 GiB machine | ADR 0012 |
| Object storage | MinIO `RELEASE.2025-10-15T17-29-55Z` built from source at commit `9e49d5e`, `mc` `RELEASE.2025-08-13T08-35-41Z` | **Upstream archived MinIO and withdrew images** (Docker Hub denies `minio/minio`). Last release fixes GHSA-jjjj-jwhf-8rgr, which the last prebuilt image predates | GitHub API; ADR 0012 (open decision) |
| Go toolchain (build only) | `golang:1.24.8-bookworm` | The toolchain MinIO's `go.mod` declares | MinIO `go.mod` |

Alternatives considered (details in ADRs): Delta Lake or Hudi instead of Iceberg
(Iceberg is engine-neutral and Trino's Iceberg support is first-class); a REST
catalog (Polaris, Lakekeeper, Nessie) instead of JDBC (more realistic for
production, but one more service); Garage or SeaweedFS instead of MinIO (actively
maintained; the S3 API keeps the choice reversible); Spark Structured Streaming
instead of bounded batches (offsets live in a checkpoint directory outside the
table; batch ranges are explicit and testable); DuckDB or PyIceberg for local
tests (no JVM, but not the engines this milestone teaches).

## 7. Implementation plan and acceptance criteria

| Slice | Content | Acceptance criteria | Status |
|---|---|---|---|
| 0 | Isolation, contract inventory, gaps, schemas, versions, failure rules | This document; worktree at `e683d32`; nothing in M2 changed | done |
| 1a | Pure core: Bronze mapping and planner, reference data, Silver row and set stages, scenario | Every scenario record gets its stated outcome; order independence; bit-identical rebuild; replay inserts nothing; fail-closed planner cases; schema not archived → abort, not quarantine | **done: 58 unit tests pass** |
| 1b | Spark jobs and lakehouse stack | Spark Bronze columns = Python reference; insert-if-absent on Iceberg; Spark Silver = Python reference (differential); unchanged inputs → no new snapshot; late batch → new build, previous readable by snapshot; checkpoint survives compaction | **code written; 6 Spark tests deferred** |
| 1c | Trino query | Trino results = reference; time travel; quarantine joins to source bytes; types as documented | **4 Trino tests deferred** |
| 1d | Real Kafka source | ingest, replay, topic recreation refused, against a dedicated topic | **1 test deferred** |
| 2 | Incremental Silver | Process only new Bronze snapshots (Iceberg incremental read), MERGE by identity with per-robot state; **equals the full rebuild** on randomized scenarios; compaction and snapshot expiry jobs | planned |
| 3 | Gold products | Both products built from a pinned Silver build; changed-days recomputation; release table with content hash; training-readiness checks reported, release stays CANDIDATE | planned |
| 4 | Operations | Scheduling entry points, metrics (lag Bronze vs Kafka, rejection rates), reconciliation M2 quarantine vs Silver outcomes, retention policy (Bronze kept beyond Kafka's 7 days), runbook | planned |

## Deferred execution

Nothing below has run. It needs the machine without the Milestone 2 stack
(3.7 GiB WSL; the 11 running Milestone 2 containers used 1.8 GiB by `docker stats` at the
start of this work, with 0.7 GiB left available and swap full). Memory figures are container
**limits** from `infrastructure/lakehouse/compose.yaml`, not measurements.

| Phase | Command | Needs | Limits | Verifies |
|---|---|---|---|---|
| 0 | `sh scripts/make-lakehouse-env.sh && make -f lakehouse.mk lake-build` | network; M2 stack down (Go compile of MinIO estimated at 1.5–3 GiB) | build only | MinIO builds from the pinned commit; Spark image installs the lock; Ivy resolves the 4 packages; JRE runs in the Python base |
| 1 | `make -f lakehouse.mk lake-test` | MinIO, catalog DB, Spark | 0.38 + 0.25 + 1.6 GiB | 6 Spark tests (slice 1b), including the open question whether Iceberg commits an empty append (`empty_append_created_snapshot` in the JUnit properties) |
| 2 | `make -f lakehouse.mk lake-test-trino` | MinIO, catalog DB, Trino; Spark stopped | 0.63 + 1.4 GiB | 4 Trino tests (slice 1c) on the tables phase 1 left |
| 3 | M2: `docker compose --profile core up -d --wait kafka schema-registry` (and the dependencies Compose starts with them, nothing else), then `make -f lakehouse.mk lake-test-kafka` | M2 Kafka and registry, lakehouse, Spark | ≈ 3.4 GiB of limits; actual use lower; the target sets the driver to 768m | 1 Kafka test (slice 1d): creates and deletes its own topic only |
| 4 | `make -f lakehouse.mk lake-down` | — | — | — |

Record for each phase: JUnit XML (`--junitxml`), `docker stats` peaks, image
digests built, and any deviation. Expect first-run fixes in the Spark code paths
the unit tests cannot reach: pandas/Arrow conversion in `row_stage`, struct
comparison in the out-of-order window, empty-append behaviour, Trino health timing.

## Integration with Milestone 2

* Milestone 3 changes no Milestone 2 file except additive lines in `pyproject.toml`
  (an extra, two markers, one lint rule) and `.gitignore`. Its Makefile targets live
  in `lakehouse.mk`.
* It reuses Milestone 2 code read-only: `observation_from_record`,
  `check_semantics`, `identity_hints`, `payload_for_audit`, `json_safe`, `REASONS`,
  `schema_str`, and two private helpers (`kafka.codec._registry_down`,
  `kafka.offsets.topic_id`). Milestone 2's in-flight work modifies `codec.py`,
  `offsets.py` and `errors.py`, so after PR #1 merges, rebase this branch onto
  `main` (`git rebase --onto main e683d32 milestone-3-lakehouse`), rerun
  `pytest tests/unit`, and check those names first.
* Unverified Milestone 2 changes are never cherry-picked into this branch.
