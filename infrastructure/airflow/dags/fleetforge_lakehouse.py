"""The lakehouse pipeline: Kafka -> Bronze -> Silver -> Gold, then reconcile and measure."""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import DAG
from lakejobs import DEFAULT_ARGS, dbt_job, spark_job

with DAG(
    dag_id="fleetforge_lakehouse",
    schedule=os.environ.get("FLEETFORGE_PIPELINE_SCHEDULE", "*/10 * * * *") or None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse"],
    doc_md="Every step is idempotent: a rerun of a finished run changes nothing.",
):
    bronze = spark_job("bronze", "bronze_ingest", minutes=20)
    silver = spark_job("silver", "silver_build", minutes=30)
    gold = dbt_job("gold", "build", "--exclude", "source:gold_releases+")
    reconcile = spark_job("reconcile", "reconcile", minutes=15, writer=False)
    freshness = spark_job("freshness", "freshness", "--hours", "24", minutes=10, writer=False)
    bronze >> silver >> gold >> reconcile >> freshness
