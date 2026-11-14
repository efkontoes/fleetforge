# Platform execution ledger (Milestones 3–6)

This ledger tracks each workstream with its acceptance criteria, commit, test evidence,
known defects and merge state. A row is "done" only when its evidence was produced on
the code in that commit. The machine is a 3.7 GiB WSL VM, so heavy profiles run one at
a time.

## Integration

| Item | Acceptance | Commit | Evidence | Status |
|---|---|---|---|---|
| Registry-outage hotfix (PR #3) | CI green, merge commit | `1735569` on main | PR #3 checks green | merged |
| M3 sync with main | merge, not rebase (branch is shared) | `bc0ee30` | pre-sync tag `m3-pre-sync-d3fb961` pushed | done |
| Schema archiver on ADR 0016 | only an explicit 404 is recorded, and only as "not found yet"; other errors stop Bronze | M3 branch | unit tests; Spark Bronze tests | done |

## Milestone 3: lakehouse

| Workstream | Acceptance | Status |
|---|---|---|
| Incremental Silver (ADR 0017) | U(S, ΔB, ΔR, V) ≡ F; checkpoint; corrections, conflicts, late registrations, firmware changes, schema ids resolving later; crash and conflict recovery | Python: 30 randomized multi-batch differential runs plus replay, crash and narrow-scope tests, mutation-checked. Spark: incremental equals a Spark full rebuild and the Python reference on a random corpus; crash after the prepared merge; stale writer detected and retried; rule-version change forces a full rebuild |
| UNRESOLVED outcome | a schema id the registry 404s is UNRESOLVED, retry-eligible, never MALFORMED | Python and Spark tests |
| Gold tables | daily telemetry, pipeline quality and rates, state observations, releases (spec §9) | in progress |
| Maintenance, backfill, retention | expiry and orphans with release tags kept; storage report | pending |
| Reconciliation | Kafka → Bronze → Silver → Gold counts recorded in `ops.reconciliation_runs` | pending |
| Gate A | real engines, tens of thousands of observations, replay, idempotence, old release reproduced, Spark killed at a commit boundary | pending |

## Milestone 4: orchestration and quality

| Workstream | Acceptance | Status |
|---|---|---|
| Airflow DAGs | pipeline, release, maintenance and backfill DAGs; idempotent, retried with backoff, bounded by timeouts; exit 65 fails without retry; one-slot writer pool | 4 DAGs pass the policy checks inside the Airflow 3.3.2 image; maintenance DAG ran end to end through the scheduler, the socket proxy and Spark job containers (it found D10 and D11) |
| dbt | analytical Gold on Trino, no second Silver; tests for keys, not-null, ranges, enums, referential integrity, cross-table and release consistency, freshness, custom invariants | 28 data tests pass in Gate A and the lakehouse CI |
| Lineage | OpenLineage RunEvents with Iceberg snapshot ids as dataset versions | written by every job to `lakehouse_ops.lineage_events` |
| Monitoring | exporter, alert rules with runbook links, freshness SLO measured | promtool rule tests pass; exporter reads real job runs; freshness job ships with the pipeline DAG |
| Gate B | orchestrated recovery from six isolated faults | runs on a CI runner (needs about 5.5 GiB) |

## Milestone 5: ML dataset and baseline

| Workstream | Acceptance | Status |
|---|---|---|
| Dataset export | Parquet per split, manifest, checksums, data card; equals the release hash | two exports of gate-r2 byte-identical; export refuses a hash mismatch |
| Baseline task | defensible synthetic label, documented as synthetic; baseline plus stronger model; train-only preprocessing; precision, recall, F1, PR-AUC | wall contact within 30 steps; baseline PR-AUC 0.629 beats gradient boosting 0.276 (episode split), explained in ADR 0020 |
| Tracking | MLflow or equivalent; registered model | mlflow-skinny, skops models, telemetry off; training twice gives identical metrics; model registered |
| Drift | PSI, KS, Wasserstein with caveats | position and battery drift between releases (PSI 0.21-0.30), velocity stable |
| LLM assistant | optional | not built (lowest priority; nothing depends on it) |
| Gate C | Kafka to a registered experiment, reproducible | passed (Gate A data, `make -f lakehouse.mk lake-gate-c`) |

## Milestone 6: infrastructure and operations
Pending.

## Defects found during execution

| # | Found by | Defect | Fix |
|---|---|---|---|
| D1 | Spark incremental tests | Spark 4.1 cannot plan a MERGE whose source temp view reads an Iceberg table (`No plan for TableReference`) | MERGE sources are computed with `localCheckpoint()` first |
| D2 | Spark incremental tests | `overwrite(robot_id IN ...)` fails because event files hold many robots | Events are swapped with one row-level MERGE (`NOT MATCHED BY SOURCE ... DELETE`) |
| D3 | Spark incremental tests | after a retry, rows merged by the failed attempt were re-prepared twice (MERGE cardinality violation) | the re-prepare set excludes rows newer than the checkpoint |
| D4 | Spark incremental tests | `random_corpus(runs=1)` crashed | fixed the range |
| D5 | concurrency test, full suite only | the session catalog caches a table for 30 s, so the stale-writer check read an old snapshot | `current_snapshot_id` refreshes the table first |
| D6 | maintenance tests | `remove_orphan_files` listed through Hadoop, which has no `s3` scheme here | `prefix_listing => true` (Iceberg's S3 listing) |
| D7 | Gate A, first run | a Bronze replay that inserted nothing committed an empty snapshot, and Silver rebuilt for it | Silver is a no-op when there is no new telemetry and the reference and archive are unchanged |
| D8 | review of the ADRs | an MWAA price had been written from memory | every price now comes from the AWS price list (`scripts/aws_prices.py`) |
| D9 | Gate A at 46,696 records (full rebuild and Python reference agreed; incremental differed in 1 outcome row) | a correction that a firmware change turned into a rejection lost its `correction_of`, so the closure no longer reached its old target, whose outcome kept a stale `superseded_by` | rejected prepared rows keep the `correction_of` they could read (rule version 3), and three corpora that reproduced it in Python are regression tests; this also holds after a crash, unlike capturing the old links in memory |
| D10 | first Airflow run on the main catalog | the JDBC catalog was created by Spark as schema V0 (no views), and `tableExists` fails there; the tests never saw it because Trino had upgraded the test catalog | `jdbc.schema-version=V1`, which Iceberg migrates on first use, plus a unit assertion |
| D11 | same Airflow run | reconciliation and freshness crashed on a lake with no Silver build yet | both report "nothing to check" instead |
