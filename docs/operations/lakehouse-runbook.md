# Lakehouse runbook

Each alert in `infrastructure/lakehouse/prometheus/lakehouse-alerts.yml` links to a
section here. The jobs report to `lakehouse_ops.job_runs` in the catalog database,
which `python -m fleetforge.lakehouse.exporter` turns into metrics.

Jobs exit with:
* 0 on success;
* 75 on a temporary failure (Airflow retries with backoff);
* 65 when a person must decide (no retry);
* 1 when the failure is unknown (normal retries).

Useful queries (catalog PostgreSQL, port 5433 on the host):

```sql
SELECT job, exit_code, finished_at, report FROM lakehouse_ops.job_runs
ORDER BY finished_at DESC LIMIT 20;
```

## silver-stale

No Silver build has succeeded for 30 minutes.

1. In Airflow (`127.0.0.1:8085`), open `fleetforge_lakehouse`. Is it paused, queued
   behind the `lakehouse_writers` pool, or failing?
2. If a task is failing, read its log, then the matching section below.
3. If the pool is held by a long backfill or maintenance run, let it finish. Silver
   catches up in one incremental build, because the checkpoint covers everything since
   the last published build.
4. If Bronze has no new rows, check Milestone 2: is the producer publishing, and is
   Kafka up?

## permanent-failure

Exit code 65 means the job refuses to guess:
* **`BronzePositionError`**: a topic was recreated or its offsets moved backwards.
  Compare the topic id in the error with the checkpoint, then decide. Run
  `bronze_ingest --accept-new-incarnation TOPIC` if the new topic is legitimate, or
  `--accept-gaps` for records lost to retention. Both are written into the snapshot
  summary as an audit trail.
* **`ReleaseError`**: a release id was reused with other inputs. Release ids are
  immutable; pick a new id.

## retrying

A job keeps failing with temporary errors: lease taken, commit conflict, registry or
object store unreachable.
* **`LeaseUnavailable`**: another writer holds the lease. Only one of each job may run.
  Check for a stuck container: `docker ps --filter ancestor=fleetforge-lakehouse-spark:dev`.
* **`RegistryUnavailable`**: Bronze stops before archiving schemas. Nothing is
  half-written, and the next run continues from the checkpoint.
* **Commit conflicts**: Silver retries from its checkpoint by itself. Repeated
  conflicts mean two writers; find the second one.

## reconciliation

A reconciliation check failed. The row in `ops.reconciliation_runs` names it:

| Check | Meaning | Action |
|---|---|---|
| `bronze_coordinates_unique` | two Bronze rows for one Kafka record | stop ingestion and find the writer that bypassed `append_bronze` |
| `bronze_equals_outcomes` | a Bronze telemetry record has no Silver outcome, or one too many | run `silver_build --full`; if it persists, it is a Silver bug |
| `events_equal_accepted` / `event_ids_unique` | Silver tables disagree | `silver_build --full` |
| `unchanged_since_build:*` | a Silver table was written outside a build | find the writer; `silver_build --full` restores the tables |
| `gold_*` | Gold is stale or disagrees with Silver | rerun dbt (`fleetforge_lakehouse` gold task) |
| `release_rows:*` | a release lost rows | `release reproduce --release-id ID`; the tagged snapshot is the truth |
| `kafka_vs_bronze:*` | Kafka and Bronze counts differ below the checkpoint | check retention (records may have expired before ingestion) |

## freshness

Fewer than 95% of records reached Silver within 15 minutes, measured over 24 hours by
the `freshness` task.
* Look at `p95_s` and `max_s` in the newest `freshness` run.
* A long tail with no failures usually means slow builds: compare `silver_build`
  durations, and `scope_rows` against `new_bronze_rows` in `silver.build_log`. A
  scope far larger than the new rows means one robot's history dominates (ADR 0017).
* Gaps between runs mean scheduling: the pool was held, or the DAG was paused.

## exporter

The exporter cannot read `lakehouse_ops`. Check the catalog database
(`docker compose ... ps catalog-db`) and the exporter's log. While this alert fires,
the other lakehouse alerts see no data.

## Rollback

* **Code**: redeploy the previous image tag. Jobs are idempotent and resume from their
  checkpoints.
* **Silver rules**: revert the change and bump `TRANSFORM_VERSION` again. The next
  build is a full rebuild under the restored rules. Old builds stay readable through
  `silver.build_log` until their snapshots expire.
* **A bad Silver or Gold commit**: roll the table back to a snapshot from the build log
  with `CALL lake.system.rollback_to_snapshot('silver.telemetry_events', <id>)`, then
  roll back the other Silver tables of the same build, and delete the build-log rows
  after it.
* **Gold**: rerun dbt with `--vars '{"silver_build_id": "<previous build>"}'` to
  recompute from an older build.
* **Never roll back Bronze** except to undo a write that bypassed `append_bronze`.
  Bronze is the history everything else is rebuilt from.

## Backfill

Use the `fleetforge_backfill` DAG. Every kind is idempotent and holds the writer pool,
so a backfill never overlaps the pipeline:
* `bronze_replay` re-reads every topic from its earliest offset. Rows Bronze already
  has are skipped.
* `silver_full` rebuilds Silver from Bronze, then Gold.
* `gold_reopen` recomputes final Gold days, either all of them (`reopen_all`) or a
  date range. Final days change only this way.
