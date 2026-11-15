# FleetForge: final engineering report

Date: 2026-11-15, release `v1.0.0`. Scope: Milestones 3–6 on top of the audited Milestone 2
platform.
Every number below comes from a run that happened on this code. Where a run was not
possible, the report says so.

## 1. Summary
FleetForge takes unreliable telemetry from a simulated robot fleet through two paths:
* an operational path (Kafka → PostgreSQL, Milestone 2);
* an analytical path (Kafka → Bronze → incremental Silver → Gold → dataset releases
  → a registered model).

The analytical path agrees with an independent oracle at every level tested:
* 12,607 deliveries in the end-to-end check, with 0 differences;
* 46,696 records in Gate A, where incremental equals both a full rebuild and a Python
  reference after a SIGKILL at a commit boundary;
* 1.06 M records in the scale run.

The operational path still holds 5,000 events/s for 5 minutes (p99 388 ms) and absorbs
10,000 events/s.

Final validation found and fixed 20 defects (D1–D20) and three measured bottlenecks
(P1–P3). Nothing was provisioned in AWS: the Terraform is planned and scanned, never
applied.

## 2. What was delivered
| Milestone | Delivered | PR (merge on main) |
|---|---|---|
| 3 Lakehouse | incremental Silver with checkpoint, dbt Gold on Trino, immutable tagged releases, maintenance, reconciliation, job runner with retry-aware exit codes, OpenLineage records, exporter and alerts | #2 (`6b6c028`) |
| 4 Orchestration | Airflow 3.3, four DAGs running jobs as containers through a restricted socket proxy, writer pool, Gate B | #4 (`a5d1953`) |
| 5 ML | reproducible Parquet export with manifest, checksums and data card; synthetic label; baseline and boosted model; MLflow; drift | #5 (`bd3c169`) |
| 6 Operations | Terraform (planned, scanned), security comparison, backup and restore test, AWS prices and cost model, CLI, final validation | #6 (`21da3b7`) |

The optional LLM assistant was not built; nothing depends on it.

## 3. Architecture
See [`docs/architecture/platform.md`](../architecture/platform.md) for the diagram and the
layer table. Spark (local mode, one core) owns Bronze and Silver, dbt owns analytical
Gold, a Spark job owns releases, and Airflow only orders and retries.

## 4. Data model and contracts
* Table specifications live in `lakehouse/tables.py` as the single source of truth for:
  * the Spark DDL;
  * the generated schema document;
  * the dbt contracts (a unit test fails if they drift).
* Identity:
  * Bronze is keyed by (Kafka topic id, partition, offset);
  * Silver events by `event_id`;
  * outcomes by Bronze coordinates;
  * Gold daily by (day, robot);
  * observations by (release, observation).
* Every Bronze telemetry record has exactly one outcome: ACCEPTED, DUPLICATE, REJECTED
  or UNRESOLVED. UNRESOLVED (a schema id the registry does not know yet) is never
  counted as bad data (ADR 0016) and is kept out of the rate denominators.

## 5. Correctness: guarantees and their proofs
| Guarantee | Proof |
|---|---|
| Bronze is insert-if-absent and the checkpoint commits with the data | crash before commit and replay tests; Gate A replay adds 0 rows; the Kafka count equals Bronze per partition |
| Silver = F(Bronze, reference, archive, rules) | Spark equals the Python reference row for row |
| Incremental Silver ≡ full rebuild | 37 differential unit tests (30 random multi-batch corpora + 3 regressions); three deliberately wrong closures each fail; Spark tests; Gate A at 46,696 records incl. SIGKILL recovery |
| One writer wins | stale-writer test: the build detects the foreign commit and retries |
| Gold equals its definition | dbt output equals the Python reference on Trino; 25 dbt data tests |
| Final days stay frozen | Gate A tampers with a final day: unchanged by a normal run, restored on reopen |
| Releases are immutable and reproducible | three hashes agree after newer builds and after expiring every untagged snapshot |
| Both paths agree with the truth | final E2E: the oracle, PostgreSQL and Silver agree on the latest state of all 40 robots and on all 12,607 deliveries |

## 6. Incremental processing and its limits
* **Scope.** The update recomputes only rows that share an `event_id` or a correction
  link with a change, plus rows of each touched robot from its first changed boot.
  Re-prepared rows count from boot 0.
* **Gate A.** The boot bound cut the rows recomputed in its last run from 45,442 to
  2,785.
* **Scale run (one core).**
  * A 6,113-row delta over 1.06 M rows went from 223 s to 124 s once the closure ran on
    a cached key projection and the events swap became a keyed MERGE.
  * A change that reaches most of a large table is rebuilt in full instead. Merging
    98% of a 1 M-row table took 728 s; a full rebuild takes 238–492 s.
* **Limit.** The closure still scans a key projection of every prepared row, so the
  fixed cost grows with history. A robot that never reboots keeps one ever-growing boot.
  Both need a per-robot frontier index, which is not built.

## 7. Gold and dataset releases
* **Daily telemetry:**
  * completeness is counted by distinct positions;
  * a gap across midnight is counted on the later day;
  * distance is summed only over consecutive steps with non-decreasing time;
  * `is_final` follows a written policy: 14 days, no open retry-eligible record for the
    robot, and an unreadable record counted by its Kafka key.
* **Pipeline quality and rates:** explicit denominators.
* **Observations:**
  * finite differences only over valid steps, otherwise NULL with a flag;
  * hash splits by episode and, stricter, by robot.
* **Releases:** pinned to a Silver build, every input snapshot tagged, published by a
  final row, and refused on reuse with different inputs.

## 8. Orchestration
* The four DAGs, the retry policy and the pool are described in ADR 0019.
* A scheduled maintenance run went end to end through the scheduler, the socket proxy
  and Spark job containers. Its first attempts found two real defects: a V0 JDBC
  catalog (D10) and a lake with no build (D11).
* **Gate B passed** on a CI runner (run 43704310730, 31 minutes, the release commit of
  PR #8), because it needs about 5.5 GiB and this machine has 3.7 GiB:

  | Scenario | Result | Attempts where the fault hit |
  |---|---|---|
  | baseline (2,009 records) | success | all tasks 1 |
  | repeat a completed run | success | all tasks 1 |
  | scheduler killed during Silver | success | Silver 2 |
  | Silver's own job container killed | success | Silver 2 |
  | Garage down at Bronze | success | Bronze 2 |
  | registry down at Bronze's lookup of a new schema id | success | Bronze 2 |
  | backfill overlapping the pipeline | both succeed | 1 |
  | release created and reproduced | success | 1 |

  Reconciliation then passed 11/11. In the registry scenario Bronze's first attempt
  asked the stopped registry for schema id 3, which it had never archived, and exited 75
  with `RegistryUnavailable`; the retry archived it.
* Each scenario now fails unless its fault visibly hit, because two earlier passes did
  not prove their fault:
  * run 43664801824's registry scenario never reached the registry (D19);
  * run 43701689766's Spark kill missed Silver, since every task ran once (D20).

  Two runs before those stopped before the first fault on driver defects (D17).

## 9. Testing strategy and results
| Level | What | Result on the final code |
|---|---|---|
| Fast (every PR) | 223 unit tests; DAG checks in the Airflow image; dbt parse; 4 ML tests; promtool rule tests; Terraform fmt, validate, plan, trivy; pip-audit | all pass in CI on the final heads of #2, #4, #5 and #6, and on `main` after the merges |
| Integration (every PR) | lakehouse stack: 21 Spark/S3 tests, dbt build twice (3 models and 25 tests, 28/28 each), 6 Trino checks, 2 real-Kafka tests; Milestone 2 stack tests (19 non-disruptive, 8 disruptive) | all pass in CI on the final heads of all four PRs; one Milestone 2 stack test failed twice earlier (O1, section 15) |
| Full system (manual or labelled) | Gates A, B and C on a fresh runner | Gates A and C reproduced this machine's results: the same build scopes, row counts, reconciliation, Gold checks and model metrics, and within each run one digest for incremental Silver, a full rebuild and the Python reference (digests differ between runs, which create their own topics). On `main` (`5c9b1fa`) Gates A, B and C passed in run 43708265800 |
| Final validation (this machine) | E2E oracle, benchmarks, game day, scale run, DR restore, freshness SLO | sections 10, 13, 14 |

## 10. Observability, SLOs and alerting
* Each job records its run and an OpenLineage event in `lakehouse_ops`. The exporter
  turns them into metrics, and six alert rules (tested with promtool) link to the
  runbook.
* During the Airflow smoke test, three failed attempts before a fix showed up as
  `failures_last_hour = 3`, so `LakehouseJobRetrying` would have fired.
* **Freshness SLO (95% of records in Silver within 15 minutes): met.** Records arrived
  continuously for six 10-minute cycles (about 72 minutes). Bronze and Silver ran after
  each cycle, taking 103–124 s on one core.

  | Records | p50 | p95 | p99 | max | within 15 min |
  |---:|---:|---:|---:|---:|---:|
  | 19,140 | 462 s | 739 s | 758 s | 770 s | 100% |

  The wait for the next run dominates the latency, not the processing. The SLO is
  measured to Silver; dbt Gold adds one more step. One hour is far shorter than the
  24-hour window the alert uses.

## 11. Machine learning baseline
| Split | Model | PR-AUC | F1 |
|---|---|---:|---:|
| episode | baseline (distance to wall) | 0.629 | 0.554 |
| episode | gradient boosting (capacity by validation) | 0.276 | 0.284 |
| robot | baseline | 0.844 | 0.756 |
| robot | gradient boosting | 0.731 | 0.650 |

The label is synthetic and says so. The baseline wins because the simulator moves robots
by independent random steps, so velocity history carries no signal (ADR 0020). Export
and training reproduce exactly. Drift between two releases is real for position and
battery (PSI 0.21–0.30), and absent for velocity.

## 12. Security
* Locally: secrets are generated per machine, never committed and never printed;
  services bind to 127.0.0.1; Airflow reaches Docker only through a proxy; MLflow
  telemetry is off.
* Production is compared item by item in `docs/operations/security.md`.
* The Terraform passes trivy at every severity. Two exceptions are inline, each with
  its reason: the log bucket uses SSE-S3 because access logs require it, and it is not
  logged itself because that would loop.

## 13. Backup and disaster recovery
* The backup is the catalog dumps plus every object, copied through the S3 API.
* The restore went into a disposable stack:
  * 15 of 15 tables were identical (rows and snapshot ids);
  * 2 of 2 releases reproduced;
  * 12 of 12 reconciliation checks passed;
  * the restore took 24 s, and 83 s including verification.
* RPO: Bronze loses nothing within Kafka's 7-day retention, because it resumes from the
  restored checkpoint. Silver and Gold are rebuilt (`docs/operations/backup-dr.md`).

## 14. Performance, capacity and cost
* **Operational path** (Milestone 2, final code, isolated stack):

  | Load | Events | Achieved | p99 / max | Lag |
  |---|---|---|---|---|
  | 5,000/s × 303 s | 1,515,094 | 4,997.5/s | 388 / 823 ms | max 2,514, 0 at the end |
  | 10,000/s × 61 s | 605,829 | 9,995.9/s | 582 ms | backlog drained at once |

  The oracle matched in both runs.
* **Lakehouse at scale** (one core, 2 GiB container):
  * Bronze: 1,036,574 records in 128 s;
  * full Silver: 1,057,918 records in 238–492 s, peak 1,693 MiB;
  * small delta: 124 s.
* **Cost** (eu-west-1 list prices fetched from the AWS Price List, published
  2026-11-03 or later; measured sizes and rates):

  | Fleet | Total per month | Job compute | Pipeline run |
  |---|---|---|---|
  | 50 robots at 1 Hz | USD 1,030 | USD 13 | 199 s per 10 minutes |
  | 500 robots at 1 Hz | USD 1,198 | USD 23 | 358 s per 10 minutes |

  Optional MSK Serverless (about USD 650–710) and MWAA (USD 212) dominate.
  Assumptions and caveats are listed in `docs/reviews/evidence/cost-model.md`.

## 15. Defects found, risks and limitations
* **Defects:** D1–D20 in `docs/reviews/platform-execution.md`, each with the test that
  found it and the fix. Two were Milestone 2 defects that only final validation
  exposed:
  * the API did not survive a container restart (D12);
  * a registry call could hang forever (D13).
* **Risks and limits:**
  * the incremental closure's fixed cost grows with history (section 6);
  * the bytes-per-event figure comes from a small corpus;
  * Gold daily tables rescan the pinned build;
  * the AWS deployment is unverified;
  * the simulator's JSONL `publish` keeps a whole stream in memory (an M2 CLI limit,
    worked around with chunks);
  * the cause of Milestone 2's consumer-group wedge remains unproven, as stated in its
    audit;
  * O1, [issue #7](https://github.com/efkontoes/fleetforge/issues/7): in 2 of 33 CI
    stack jobs the Milestone 2 producer stopped delivering after an 8 s broker restart
    and timed out after 300 s. In both, the producer saw leaderless partitions at the
    stop and then logged no reconnect attempt, while other clients reconnected within
    about 16 s. Ruled out: the broker's startup error, a dependency change, and
    librdkafka's re-bootstrap (measured). It did not reproduce locally. The test now
    prints the client's own log when it fails, and the issue holds the investigation
    plan. No data is lost silently: every unacknowledged message is reported as
    failed.

## 16. How to reproduce, and the evidence index
```sh
fleetforge env && fleetforge up core && make -f lakehouse.mk lake-gate-a   # Gate A
make -f lakehouse.mk lake-gate-c                                           # Gate C
sh scripts/lakehouse_dr.sh backup && sh scripts/lakehouse_dr.sh restore-check
python3 scripts/final_e2e.py m2   # then the lake phase in the Spark container
python3 scripts/freshness_run.py --cycles 6 --cycle-s 600                    # about 75 minutes
```
| Evidence | File |
|---|---|
| Execution ledger, defects, optimisations | `docs/reviews/platform-execution.md` |
| Fault matrix | `docs/reviews/fault-matrix.md` |
| Gate A | `docs/reviews/evidence/gate-a.json` |
| Gate C | `docs/reviews/evidence/gate-c-*.json`, `gate-c-export-gate-r2/` |
| End-to-end oracle | `docs/reviews/evidence/final-e2e.json` |
| Scale run | `docs/reviews/evidence/scale-silver.json` |
| DR | `docs/reviews/evidence/dr-*.json` |
| Benchmarks | `benchmarks/results/m2-20261114T2016*`, `…T2019*` |
| Prices and cost | `docs/reviews/evidence/aws-prices-eu-west-1.json`, `cost-model.md` |
| Gates A and C on a fresh CI runner | `docs/reviews/evidence/ci-fresh-runner/` |
| Gates A, B and C on `main` | `docs/reviews/evidence/ci-release/` |
| Freshness SLO | `docs/reviews/evidence/freshness-run.json` (`scripts/freshness_run.py`) |
| Gate B | `docs/reviews/evidence/ci-gate-b/gate-b.json` (earlier runs beside it) |
