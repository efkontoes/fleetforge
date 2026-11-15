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
| Merges of #2, #4, #5, #6 | green CI, regular merge commits, in order | — | every branch pushed; ci, lakehouse and platform pass on all four heads (2026-11-15) | not merged yet, by request: waiting for the go-ahead |
| Schema archiver on ADR 0016 | only an explicit 404 is recorded, and only as "not found yet"; other errors stop Bronze | M3 branch | unit tests; Spark Bronze tests | done |

## Milestone 3: lakehouse

| Workstream | Acceptance | Status |
|---|---|---|
| Incremental Silver (ADR 0017) | U(S, ΔB, ΔR, V) ≡ F; checkpoint; corrections, conflicts, late registrations, firmware changes, schema ids resolving later; crash and conflict recovery | Python: 30 randomized multi-batch differential runs plus replay, crash and narrow-scope tests, mutation-checked. Spark: incremental equals a Spark full rebuild and the Python reference on a random corpus; crash after the prepared merge; stale writer detected and retried; rule-version change forces a full rebuild |
| UNRESOLVED outcome | a schema id the registry 404s is UNRESOLVED, retry-eligible, never MALFORMED | Python and Spark tests |
| Gold tables | daily telemetry, pipeline quality and rates, state observations, releases (spec §9) | dbt output equals the Python reference on Trino; final days stay frozen and reopen on request; releases reproduce after newer builds and after snapshot expiry (Gate A) |
| Maintenance, backfill, retention | expiry and orphans with release tags kept; storage report | Spark maintenance tests; Gate A expired snapshots, kept every release tag and wrote the storage report; the maintenance DAG ran end to end under Airflow |
| Reconciliation | Kafka → Bronze → Silver → Gold counts recorded in `ops.reconciliation_runs` | 10/10 checks pass in Gate A, locally and on a fresh CI runner; 12/12 after the DR restore (two checks added since) |
| Gate A | real engines, tens of thousands of observations, replay, idempotence, old release reproduced, Spark killed at a commit boundary | passed at 46,696 records, locally and on a fresh CI runner with identical results (`evidence/gate-a.json`, `evidence/ci-fresh-runner/`) |

## Milestone 4: orchestration and quality

| Workstream | Acceptance | Status |
|---|---|---|
| Airflow DAGs | pipeline, release, maintenance and backfill DAGs; idempotent, retried with backoff, bounded by timeouts; exit 65 fails without retry; one-slot writer pool | 4 DAGs pass the policy checks inside the Airflow 3.3.2 image; maintenance DAG ran end to end through the scheduler, the socket proxy and Spark job containers (it found D10 and D11) |
| dbt | analytical Gold on Trino, no second Silver; tests for keys, not-null, ranges, enums, referential integrity, cross-table and release consistency, freshness, custom invariants | 28 data tests pass in Gate A and the lakehouse CI |
| Lineage | OpenLineage RunEvents with Iceberg snapshot ids as dataset versions | written by every job to `lakehouse_ops.lineage_events` |
| Monitoring | exporter, alert rules with runbook links, freshness SLO measured | promtool rule tests pass; exporter reads real job runs; freshness SLO met over six 10-minute cycles: 19,140 records, p95 739 s, max 770 s, 100% within 15 minutes (`evidence/freshness-run.json`) |
| Gate B | orchestrated recovery from six isolated faults | passed on a CI runner (run 43664801824): eight scenarios, every DAG run succeeded, retries where faults hit (Silver 2 attempts after the scheduler and Spark kills, Bronze 2 while Garage was down), reconciliation 11/11 (`evidence/ci-gate-b/`) |

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

| Workstream | Acceptance | Status |
|---|---|---|
| Terraform | validate, static security checks, plan with a safe test config; nothing provisioned | fmt, validate and an offline plan of 51 resources pass with placeholder credentials; trivy finds nothing at any severity (two inline exceptions with reasons); never applied |
| Security | production versus local configuration | `docs/operations/security.md` |
| Backup and DR | real restore, restored data queried and reconciled; RPO/RTO | 15/15 tables identical, 2/2 releases reproduce, 12/12 checks pass; restore 24 s, verified 83 s |
| Performance | only measured bottlenecks | P1-P3 above |
| Capacity and cost | measured sizes and rates, real current prices | AWS Price List (eu-west-1); USD 1,030 (50 robots) and 1,198 (500 robots) a month, mostly optional MSK and MWAA |
| CLI and `.env.example` | developer entry point | `fleetforge` CLI; `.env.lakehouse.example` |

## Final validation (2026-11-14, isolated `fleetforge-final` project)

| Check | Result |
|---|---|
| End-to-end oracle | 12,607 deliveries: 0 differences between the oracle and the lakehouse, no event accepted twice; latest state equal in the oracle, PostgreSQL and Silver for all 40 robots |
| 5,000 events/s × 300 s | 1,515,094 events, 4,997.5/s, p99 388 ms, lag max 2,514, drained; oracle matched |
| 10,000 events/s × 60 s | 605,829 events, 9,995.9/s, peak 12,957/s, lag max 4,130, drained; oracle matched |
| Fault game day | Milestone 2: 19 + 7 stack tests pass; lakehouse: L1-L14 pass (fault matrix) |
| Scale | 1.06 M records: Bronze 128 s, full Silver 238-492 s, small delta 124 s (one core) |
| Clean-environment CI | full-system run 43646391081: Gates A and C passed with results identical to this machine; Gate B passed in run 43664801824 |

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
| D12 | final validation, API restart while publishing | Milestone 2's API died after a container restart: Docker remounts its metrics tmpfs as root 755 and the workers (uid 10001) cannot write | tmpfs mounted with the app's uid and mode 0700; `test_api_serves_again_after_a_container_restart` fails without the fix and passes with it |
| D13 | CI, Milestone 2 stack job | confluent-kafka 2.16's serializer first asks the registry for Confluent-only "associations", and with no client timeout one unanswered request hung the job for 600 s | subject name strategy named explicitly (topic, as the contract says), and a 10 s registry client timeout, which callers already treat as "registry unavailable" |
| D14 | CI, lakehouse Kafka phase | Spark 4.1 mis-plans an anti-join with an Iceberg table created earlier in the same session ("computeStats called before pushdown") | the schema archive, a few rows, is read into Python; snapshot ids come from Iceberg's API with `refresh()` instead of `refreshTable` |
| D15 | CI, ML job, once it could load its tests | the training test's random walks started too far from the walls, so with seed 1 the training split had no positive label; locally the test had only ever been skipped, because the dev venv has no scikit-learn | half the episodes drift into a wall (10-13% positives in every split across 50 seeds); in the ML image the old test fails and the new one passes |
| D16 | CI on #2 | the retention-gap stack test assumed its second fleet would write to a partition the first had used; with the clock-based seed of that run all six robots hashed elsewhere (0.7% of seeds) | publish up to five fleets until one lands; no seed among 3,000 fails |
| D17 | Gate B, second CI run | the driver decoded JSON from the first bracket on stdout, which in Airflow 3.3 is a structlog `[warning ]` line; it also read `try_number`, which the CLI never prints, so every retry would have been recorded as 0 | decode from the first line that parses (unit tests on output captured from the image); attempts come from Airflow's `task_instance` table |
| D18 | CI, unit jobs | the new parser test imported `scripts`, which plain `pytest` (unlike `python -m pytest`) cannot find | `pythonpath = ["."]` in the pytest configuration |

## Open observations

| # | Seen in | What happened | State |
|---|---|---|---|
| O1 | CI on #6 (4aaabd5) and #4 (de15f59), Milestone 2 stack job | `test_producer_survives_broker_interruption`: after an 8 s broker stop the idempotent producer delivered nothing more within its 300 s message timeout | open. Ruled out: the broker's startup `NotControllerException` (it appears on every restart, including passing ones), a dependency change (confluent-kafka 2.16.1 in CI and locally), and librdkafka's re-bootstrap (disabling it did not shorten reconnection: 9.5-16.6 s against 7.9-9.5 s). Not reproduced in 10 local runs. The test now logs the client's idempotence, broker and topic events, printed on failure, so the next occurrence records its cause. It passed on all four heads afterwards |

## Measured optimisations (Milestone 6)

Only measured bottlenecks were changed. Numbers are on one Spark core in a 2 GiB
container (`docs/reviews/evidence/scale-silver.json`, `gate-a.json`).

| # | Bottleneck (measured) | Change | Before | After |
|---|---|---|---|---|
| P1 | incremental scope covered every active robot's full history (Gate A run 3: 62 new rows, 45,442 recomputed) | bound the closure by boot (ADR 0017) | 45,442 rows | 2,785 rows |
| P2 | at 1.06 M rows, a 6,113-row delta took 223 s: the closure rescanned the prepared table (72.8 s) and `NOT MATCHED BY SOURCE` rewrote all events (61.0 s) | closure on a cached key projection; events swap as a keyed MERGE with explicit deletes | 223 s | 124 s (scope 31.8 s, swap 17.8 s) |
| P3 | re-preparing 98% of a 1 M-row table incrementally took 728 s against 238 s for a full rebuild | above 30% of a table of 100,000 rows or more, build in full | 728 s | full rebuild instead |

