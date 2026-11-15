# ADR 0019: Airflow runs each lakehouse job as its own container

Status: accepted. The DAGs are checked in CI inside the Airflow image (imports,
retries, timeouts, pools, task order). Recovery under faults is Gate B.

## Context
Milestone 3 left five idempotent jobs (Bronze, Silver, release, maintenance,
reconciliation) and dbt. They need scheduling, retries and backfills. The scheduler
must tell a temporary failure from one that needs a person, and it must never let two
writers race.

## Decision
* **Airflow 3.3 with the LocalExecutor**, metadata in a database on the catalog's
  PostgreSQL. Every task is a `DockerOperator` that starts the same image and command
  an operator would run by hand (`python -m fleetforge.lakehouse.jobs.silver_build`).
  Airflow holds no data logic; a DAG is only order and policy.
* **Four DAGs:**
  * `fleetforge_lakehouse`, every 10 minutes: Bronze → Silver → dbt Gold →
    reconcile → freshness.
  * `fleetforge_release`, on demand: create → reproduce → release tests.
  * `fleetforge_maintenance`, daily.
  * `fleetforge_backfill`, on demand: replay Bronze, rebuild Silver, or reopen Gold days.
* **Exit codes carry the retry decision:**
  * 75 is temporary: lease taken, commit conflict, registry or store down.
  * 65 is permanent: a recreated topic, a clashing release, failed reconciliation.
    The operator wrapper turns 65 into `AirflowFailException`, so the task fails
    without retries.
  * Everything else is retried three times with exponential backoff, bounded by
    `execution_timeout`.
* **One writer at a time.** Writer tasks share the one-slot pool
  `lakehouse_writers`, so a backfill and the pipeline queue instead of colliding. The
  jobs' own PostgreSQL leases are the second line of defence.
* **Secrets** reach containers as Airflow Variables rendered at run time. Names
  containing `password`, `secret` or `access_key` are masked in logs and in rendered
  fields. Airflow reaches Docker only through a socket proxy that allows containers,
  images and networks.
* **Lineage and monitoring without another stack.** Every job records its run in
  `lakehouse_ops.job_runs`, and an OpenLineage RunEvent in
  `lakehouse_ops.lineage_events` with Iceberg snapshot ids as dataset versions. A small
  exporter turns job runs into Prometheus metrics, and the alert rules are tested with
  `promtool`.

## Alternatives
| Option | Why not |
|---|---|
| Celery or Kubernetes executor | More moving parts and memory for a handful of sequential tasks |
| PythonOperator importing the jobs | The scheduler would load Spark and share its memory and failures |
| Marquez for lineage | A web app, an API and a database only to display what a table already holds |
| Retry everything | A recreated topic would be retried for hours instead of paging someone |

## Consequences
* The socket proxy still lets Airflow start containers, which is root-equivalent on a
  single host. In AWS the same tasks map to ECS `RunTask` with a scoped IAM role (ADR 0021).
* Images must be rebuilt for a code change to reach scheduled runs. That is the point:
  a run is tied to an image.
