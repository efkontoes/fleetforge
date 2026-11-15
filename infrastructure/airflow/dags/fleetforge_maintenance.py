"""Daily maintenance: expire untagged snapshots, report orphans and storage, reconcile."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from lakejobs import DEFAULT_ARGS, spark_job

with DAG(
    dag_id="fleetforge_maintenance",
    schedule="0 3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse"],
):
    maintain = spark_job("maintain", "maintenance", "--keep-days", "7", minutes=60)
    reconcile = spark_job("reconcile", "reconcile", minutes=15, writer=False)
    maintain >> reconcile
