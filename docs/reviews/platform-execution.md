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
Pending.

## Milestone 5: ML dataset and baseline
Pending.

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
| D12 | final validation, API restart while publishing | Milestone 2's API died after a container restart: Docker remounts its metrics tmpfs as root 755 and the workers (uid 10001) cannot write | tmpfs mounted with the app's uid and mode 0700; `test_api_serves_again_after_a_container_restart` fails without the fix and passes with it |
| D13 | CI, Milestone 2 stack job | confluent-kafka 2.16's serializer first asks the registry for Confluent-only "associations", and with no client timeout one unanswered request hung the job for 600 s | subject name strategy named explicitly (topic, as the contract says), and a 10 s registry client timeout, which callers already treat as "registry unavailable" |
| D14 | CI, lakehouse Kafka phase | Spark 4.1 mis-plans an anti-join with an Iceberg table created earlier in the same session ("computeStats called before pushdown") | the schema archive, a few rows, is read into Python; snapshot ids come from Iceberg's API with `refresh()` instead of `refreshTable` |

## Measured optimisations (Milestone 6)

Only measured bottlenecks were changed. Numbers are on one Spark core in a 2 GiB
container (`docs/reviews/evidence/scale-silver.json`, `gate-a.json`).

| # | Bottleneck (measured) | Change | Before | After |
|---|---|---|---|---|
| P1 | incremental scope covered every active robot's full history (Gate A run 3: 62 new rows, 45,442 recomputed) | bound the closure by boot (ADR 0017) | 45,442 rows | 2,785 rows |
| P2 | at 1.06 M rows, a 6,113-row delta took 223 s: the closure rescanned the prepared table (72.8 s) and `NOT MATCHED BY SOURCE` rewrote all events (61.0 s) | closure on a cached key projection; events swap as a keyed MERGE with explicit deletes | 223 s | 124 s (scope 31.8 s, swap 17.8 s) |
| P3 | re-preparing 98% of a 1 M-row table incrementally took 728 s against 238 s for a full rebuild | above 30% of a table of 100,000 rows or more, build in full | 728 s | full rebuild instead |

