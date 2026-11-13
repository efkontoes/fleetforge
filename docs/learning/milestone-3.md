# Milestone 3 — lakehouse engineering, explained with our own code

Twelve concepts, plus lessons from the first real run, in the same shape as the Milestone 2 guide: the **problem**, the
**mechanism**, **ours** (with file names), what it **guarantees** and what it
**does not**, **alternatives**, and how we **verify** it.

Everything named "verify" below has run: the pure parts in unit tests, the
engine parts in the staged stack tests (spec §8). Section 13 is what the first real
run taught us that the unit tests could not.

Try the pure parts now:
```bash
.venv/bin/pytest tests/unit -k lakehouse -v
.venv/bin/python -c "from fleetforge.lakehouse.tables import SILVER_TELEMETRY_EVENTS as t; print('\n'.join(t.create_statements()))"
```

---

## 1. Bronze, Silver, Gold: why three layers

**Problem.** Raw telemetry contains duplicates, garbage and late events, and the
rules for cleaning it will change. If cleaning destroys the input, a bug in the
rules is permanent.

**Mechanism.** Keep an immutable copy of what arrived (Bronze), derive a validated
canonical form from it (Silver), and build purpose-specific products on top (Gold).
Each layer can be rebuilt from the one below.

**Ours.** Bronze stores Kafka *bytes*, not decoded values
(`bronze.kafka_records.record_value`), so a future decoder can reread history.
Silver is rebuilt from a pinned Bronze snapshot (`jobs/silver_build.py`). Gold is
specified (`gold.*` in `tables.py`), built in slice 3.

**Guarantees.** Any Silver or Gold state can be recomputed while Bronze holds the
records. **Does not:** recover what never reached Bronze. Kafka keeps telemetry 7
days, so Bronze must ingest within that window (failure F12).

**Alternatives.** Transform-on-ingest (store only clean data: cheaper, but you
cannot fix past mistakes); a raw data lake of JSON files without a table format
(no atomic commits, no schema, no time travel).

**Verify.** `test_late_batch_adds_rows_without_rewriting_earlier_decisions`
rebuilds from Bronze twice and compares.

## 2. Spark execution: driver, executors, stages and shuffles

**Problem.** Silver logic must scale beyond one machine's memory, yet stay
deterministic.

**Mechanism.** PySpark builds a *logical plan* lazily. Nothing runs until an
*action* (`count`, `collect`, a write). The optimizer turns the plan into
*stages* separated by *shuffles*. A **narrow** transformation (filter, select,
`mapInPandas`) works partition by partition. A **wide** one (group by, join,
window) must move all rows with the same key to the same task. Shuffles are the
expensive part, both network and disk.

**Ours** (`silver_build.py`):

```
Bronze scan (pruned to source_topic = telemetry) ─ narrow
  └─ mapInPandas(row_stage) ─ narrow: Arrow batches -> Python worker -> Arrow
       ├─ window by event_id                         ─ shuffle 1
       ├─ window by (robot_id, boot_number, seq)     ─ shuffle 2
       ├─ window by correction_of                    ─ shuffle 3
       └─ window by robot_id ORDER BY arrival        ─ shuffle 4
            └─ joins (superseded, duplicates) -> overwrite Iceberg tables
```

Why split the work this way: the **row stage** is Python because it reuses
Milestone 2's validation code and decodes Avro with several writer schemas. It is
narrow, so Python cost scales linearly and never needs other rows. The **set
stage** needs other rows, so it uses window functions that run in the JVM and can
spill to disk. A Python `applyInPandas` per robot would need one robot's whole
history in one worker's memory. Locally, `local[2]` runs driver and executors in
one JVM; `spark.sql.shuffle.partitions=4` avoids 200 tiny tasks.

**Guarantees.** The same plan over the same snapshot gives the same rows: windows
are fully ordered by the unique arrival key. **Does not:** guarantee row *order*
in the output, or bound memory per key (a hot key is still one task).

**Verify.** `test_spark_silver_equals_the_python_reference` (Spark
output equals `silver.build()`).

## 3. Parquet: columnar files and why our sort order matters

**Problem.** Analytical queries read a few columns of many rows; row-oriented
files make them read everything.

**Mechanism.** Parquet stores each column contiguously, in *row groups* (Iceberg
default target 128 MB) split into *pages*, encoded (dictionary, run-length) and
compressed (we use zstd). Each column chunk carries **min/max statistics**, so a
reader can skip row groups, and Iceberg keeps the same statistics per file in its
manifests, so whole files are skipped before they are opened.

**Ours.** Bronze files are written sorted by `(kafka_partition, kafka_offset)`
(`WRITE ORDERED BY`). Each file therefore covers a narrow offset range, so the
anti-join predicate `kafka_offset >= 120 AND kafka_offset < 170`
(`range_predicate_sql`) can skip almost every file. Silver is sorted by
`(robot_id, boot_number, sequence_number)`, so a per-robot query touches few files.

**Guarantees.** Pruning never changes results, only how much is read. **Does
not:** help if data is unsorted (statistics then span everything), or with many
tiny files (statistics overhead dominates; compaction fixes it, slice 2).

**Alternatives.** ORC (similar, more Hive-centric), Avro (row-oriented: right for
Kafka messages, wrong for analytics). Kafka uses Avro; the lakehouse uses Parquet.
Each fits its access pattern.

**Try it.** `EXPLAIN` the anti-join, and read Iceberg's `files` metadata table
(`lower_bounds`, `upper_bounds`) after an ingest. (An exercise, not a test.)

## 4. Iceberg: what a "table" is when it is only files

**Problem.** A directory of Parquet files has no transactions. A reader can see
half a write, two writers can corrupt each other, and "the table at 10:00" does not
exist.

**Mechanism.** Iceberg adds a metadata tree:

```
catalog row (lake.silver.telemetry_events) -> metadata.json  (schemas, partition specs,
                                                              sort order, snapshots, properties)
    -> manifest list (one per snapshot) -> manifests (data files + partition values
                                                       + column statistics) -> Parquet files
```

A commit writes new files and a new `metadata.json`, then **atomically swaps the
catalog pointer**, but only if it still points to the version the writer started
from (optimistic concurrency). In our JDBC catalog that is one conditional
`UPDATE` in PostgreSQL. A loser re-reads and retries if its operation is still valid
(an append always is).

**Ours.** `infrastructure/lakehouse/compose.yaml` runs a separate PostgreSQL for
the catalog. Spark (`jobs/common.spark_conf`) and Trino
(`trino/catalog/lake.properties`) point at the same rows; the Iceberg catalog name
`lake` must match in both, because it is stored in each row (ADR 0012).

**Guarantees.** Readers see whole snapshots or nothing (snapshot isolation); a
failed writer leaves the table unchanged. **Does not:** enforce primary keys, or
commit across tables.

**Verify.** `test_rebuild_with_unchanged_inputs_is_a_no_op` (no new
snapshot), plus F1, F7 and F11 in the spec as manual drills.

## 5. Snapshots: time travel, and a checkpoint that cannot disagree with the data

**Problem.** "Which offsets are already in Bronze?" must be answered by
something that can never disagree with the data itself.

**Mechanism.** Each commit is a *snapshot* with an id, a parent and a **summary**
map. Writers can add their own keys (`snapshot-property.<key>`). Old snapshots stay
readable (`VERSION AS OF <id>`) until `expire_snapshots` removes them.

**Ours.** The Bronze append carries `fleetforge.bronze.checkpoint`, the next
offset per topic and partition, *in the same commit as the rows*
(`bronze.snapshot_properties`). That is Milestone 2's offset-ledger idea (ADR 0006)
moved into Iceberg. Compaction creates snapshots without a checkpoint, so
`latest_checkpoint` walks the parent chain. Silver commits carry
`fleetforge.silver.build-id`, which Trino can read from `"telemetry_events$snapshots"`.

**Guarantees.** Data and checkpoint can never disagree: one atomic commit. **Does
not:** survive snapshot expiry by itself. That is why `merge_checkpoints` also
derives a checkpoint from the data.

**Verify.** `test_checkpoint_survives_maintenance_snapshots_and_replays` (unit);
`test_bronze_checkpoint_walks_past_maintenance_snapshots` (real
compaction).

## 6. Partitioning: hidden, transformed, and chosen for the queries

**Problem.** Partitions let a query skip data, but each partition value creates
files. Too coarse, and nothing is skipped; too fine, and you get thousands of tiny
files.

**Mechanism.** Iceberg partitions by **transforms of columns** (`days(ts)`,
`bucket(16, id)`, `truncate`, identity), stored in metadata. Queries filter on the
column (`observed_at >= ...`) and Iceberg maps that to partitions: this is *hidden
partitioning*, so users never write a `dt` column. Changing the partition spec
later (*partition evolution*) does not rewrite old data.

**Ours.**
* Bronze: `source_topic, days(ingested_at)`. Arrival days are append-only, the
  small CDC topics are separated, and age-based retention is a partition drop. Not
  `days(kafka_timestamp)`: that is the producer's clock (gap G11).
* Silver: `days(observed_at), bucket(16, robot_id)`. Queries are by time and robot.
  `bucket` spreads 500 robots over 16 groups. Identity-partitioning by `robot_id`
  would create 500 directories per day. Late events land in *old* day partitions,
  which Iceberg handles fine; the cost appears in Gold, which must recompute those
  days.

**Guarantees.** Correct results whatever the filter. **Does not:** prevent small
files on our tiny test data (one file per partition per write); compaction is
slice 2.

**Verify.** `test_table_specs_are_internally_consistent` (transforms reference real
columns). Try it: `EXPLAIN` a query with a day filter.

## 7. Upserts: MERGE, copy-on-write, merge-on-read, and why slice 1 does neither

**Problem.** A correction or late duplicate changes rows that already exist.
Files are immutable, so "update" means writing something new.

**Mechanism.** `MERGE INTO t USING s ON key WHEN MATCHED THEN UPDATE ... WHEN NOT
MATCHED THEN INSERT`. Iceberg implements it two ways. **Copy-on-write** rewrites
every file containing a changed row: reads stay fast, writes are expensive.
**Merge-on-read** writes small *delete files* that readers apply: writes are cheap,
reads pay until compaction. Format v2 is what makes delete files possible.

**Ours.** Bronze needs only *insert-if-absent*: an anti-join plus append
(`append_bronze`), equivalent to an insert-only MERGE. Silver slice 1 **replaces**
each table atomically (`overwrite(lit(True))`). That makes Silver a pure function,
easy to test, at the cost of rebuild time. Slice 2 switches to MERGE by `event_id`
and must produce exactly the same rows as the rebuild.

**Guarantees** (MERGE, slice 2). Iceberg validates conflicting concurrent changes
at commit time (isolation level). **Does not:** deduplicate the *source*. If the
source holds two rows for one key, MERGE fails or produces duplicates, which is
why Silver's set stage resolves duplicates *before* any merge.

**Verify.** `test_replaying_a_batch_inserts_nothing` (unit),
`test_bronze_append_is_insert_if_absent_and_carries_the_checkpoint`.

## 8. Schema evolution, twice: Avro in Kafka, Iceberg in the lake

**Problem.** Producers and tables change over years; old data must stay readable
and new fields must not break old readers.

**Mechanism.** *Avro*: every message names its **writer schema** (by registry id).
A reader decodes with it and maps to what it needs. The registry's BACKWARD rule
rejects incompatible revisions (Milestone 2, v3). *Iceberg*: columns have
permanent **field ids**, so add, rename, drop and reorder are metadata-only, and
safe type widening (int → long, float → double) is allowed.

**Ours.** v1 and v2 records both decode with their own writer schema
(`silver.prepare`). The content fingerprint treats "v1 without heading" and "v2
with `heading_rad = NULL`" as the same event, so a producer upgrade does not create
fake conflicts (`test_fingerprint_ignores_delivery_metadata_and_schema_revision`).
`bronze.registry_schemas` archives each schema id, because ids belong to one
registry's storage (gap G9). If a schema is missing, the job **aborts**; it never
quarantines (`test_missing_schema_archive_entry_aborts_instead_of_quarantining`).
A new Silver column is `ALTER TABLE ... ADD COLUMN` plus a rebuild that fills it
from Bronze bytes.

**Guarantees.** Compatible revisions decode forever while their schema is
archived. **Does not:** make an *incompatible* business change compatible:
`schema_version = 2` is rejected until Silver implements it.

## 9. Data modeling: grain, identity, and three different "orders"

**Problem.** Every table needs a precise answer to "one row per what?", and
time-related words mean different things in different places.

**Ours.**

| Table | Grain (one row per) | Identity |
|---|---|---|
| `bronze.kafka_records` | delivered Kafka record | `(kafka_topic_id, kafka_partition, kafka_offset)` |
| `silver.telemetry_events` | accepted logical event | `event_id` |
| `silver.telemetry_record_outcomes` | Bronze telemetry record | same as Bronze |
| `gold.robot_daily_telemetry` | robot × observed day | `(observed_date, robot_id)` |

Three orders, never mixed:
* **business order** `(boot_number, sequence_number)`: what happened first on the
  robot. Used for ordering and conflicts.
* **arrival order** `(ingested_at, topic id, partition, offset)`: what reached us
  first. Used for "first copy wins" and for `arrived_out_of_order`.
* **event time** `observed_at`: the robot's clock. Used for daily grains and
  partitioning only, because clocks drift (gaps G8, G11).

**Reference data** (firmware contracts) changes rarely. Two classic choices: an
as-of join (SCD type 2: the version valid at event time) or "latest applies to
all history". We chose the second, because a contract describes how a firmware
emits values, so a changed row is a *correction*, and the rebuild reapplies it
everywhere (`reference.py`). With the first choice, a typo fix would apply only
from its own date onward.

**Outcomes instead of a separate quarantine.** Every source record has exactly
one outcome row, so the invariant `count(outcomes) = count(Bronze)` proves nothing
was silently dropped
(`test_every_record_gets_exactly_the_outcome_the_scenario_states`).

## 10. Idempotency, determinism and "exactly once"

**Problem.** Every job will crash, retry and be re-run on old data. The result
must not depend on how often or in what order that happened.

**Mechanism and ours.**
* *Idempotent writes*: Bronze insert-if-absent by identity; Silver replacement
  keyed by a build id.
* *Deterministic computation*: no wall clock (the future-timestamp check uses the
  stored `ingested_at`, not "now"), total orders everywhere, and exact arithmetic
  where possible. `9 * 0.001 == 0.009000000000000001`, but `9 / 1000 == 0.009`, so
  units are converted by division
  (`test_millimetres_convert_with_correct_rounding`).
* *Content-addressed builds*: `build_id = hash(Bronze snapshot, reference
  fingerprint, TRANSFORM_VERSION)`. An equal id means equal rows, so a re-run is
  skipped.

**Exactly once, precisely.** Kafka and Iceberg share no transaction. Bronze is
*effectively once per record identity* within explicit boundaries: one writer,
records still retained by Kafka, and one topic incarnation. Anything outside those
boundaries stops the job for an operator (the spec's §5 table). That is a stronger
statement than "exactly once", because it says when it stops being true.

**Verify.** `test_result_does_not_depend_on_input_order`,
`test_rebuild_is_deterministic_and_bit_identical`, the planner tests in
`test_lakehouse_bronze.py`.

## 11. Testing data pipelines without (and then with) the engines

**Problem.** A Spark job that "runs" can still be wrong, and a test that compares
the job with itself proves nothing.

**Ours, three layers.**
1. **A scenario with stated expectations** (`fixtures.py`): every record says
   which outcome it must get and why ("NaN battery", "late: boot 1 event after
   boot 2"). Tests check against the scenario, not against the code.
2. **Invariants and properties**: reconciliation (one outcome per record),
   identity uniqueness, order independence (shuffled input gives identical output),
   bit-identical rebuilds, one current version per ordering slot.
3. **Differential tests**: Spark output must equal the pure reference
   implementation on the same input. Error strings come from shared templates
   (`template_sql`) so the two can match byte for byte, and
   `test_spark_error_templates_render_exactly_the_python_strings` checks the SQL
   rendering without a JVM.

**What unit tests could not prove**, and the stack tests then did: pandas/Arrow
conversion in `mapInPandas`, Spark's struct comparison in the out-of-order window,
that Iceberg commits an empty append (it does), and that Trino reads Spark-written
metadata. Add the fourth layer, **failure injection**: fault hooks crash Bronze
before its commit and Silver between its two table writes, and the tests prove that
the restart repairs everything.

## 12. Trino: one catalog, a different engine

**Problem.** Analysts need SQL over the lake without running Spark, and without a
second copy of the data.

**Mechanism.** Storage and compute are separate: Trino reads the same Iceberg
metadata and Parquet files through the same catalog. Each query reads one snapshot
per table.

**Ours.** `trino/catalog/lake.properties` (JDBC catalog, native S3). Examples once
running:

```sql
-- quarantine with the exact source bytes, by coordinates
SELECT o.reason_code, o.errors, b.value_framing, b.record_value
FROM silver.telemetry_record_outcomes o
JOIN bronze.kafka_records b USING (kafka_topic_id, kafka_partition, kafka_offset)
WHERE o.outcome = 'REJECTED';

-- the previous Silver build, and which build each snapshot published
SELECT count(*) FROM silver.telemetry_events FOR VERSION AS OF <snapshot_id>;
SELECT committed_at, summary['fleetforge.silver.build-id'] FROM silver."telemetry_events$snapshots";
```

**Guarantees.** Consistent reads per table per query. **Does not:** pair
`telemetry_events` and `telemetry_record_outcomes` from the same build by itself:
filter both on `silver_build_id`. Trino 484's Iceberg v3 support is experimental,
which is why tables pin format v2.

**Verify.** `tests/lakehouse/test_trino.py`: results equal the reference,
time travel, types as documented.

## 13. What the first real run taught us

Five failures, none visible to the unit tests (spec §8 has the full table).

* **"S3-compatible" is not "Iceberg-compatible".** A pyarrow probe passed every S3
  operation on Garage, yet every Iceberg metadata write failed with *Invalid
  payload signature*. The AWS SDK v2 inside Iceberg (and Trino) sends uploads with a
  trailing checksum by default, a newer wire format that Garage rejects. Lesson:
  test with the client the engine actually uses. Fix: checksums only where S3
  requires them (`AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED`).
* **Optimizers have bugs too.** `dropDuplicates` before an Iceberg append made Spark
  4.1.4 produce an invalid plan, while the same DataFrame counted fine. Isolating it
  took four variants of the plan. The fix expresses the same rule ("first copy
  wins") as a window, which the optimizer handles.
* **The JVM heap is not the JVM's memory.** With a 768 MB heap the driver reached
  1.35 GB RSS and the container's cgroup killed it (`dmesg` showed it). Metaspace,
  direct buffers, code cache and GC threads live outside the heap, and Python
  `mapInPandas` workers add their own memory. Measured per process: JVM 1,280 MiB,
  Python workers 586 MiB, test driver 368 MiB. Cap every region and size the
  container for the sum.
* **Identifiers have more than one spelling.** Kafka topic ids printed by librdkafka
  use standard base64 (`+`, `/`); Java clients print URL-safe base64. A validator
  written from the Java form rejected the real broker's id.
* **Your own guard can block you.** A check "no `fleetforge-*` containers running"
  also matched the lakehouse's `fleetforge-lakehouse-*` containers.

