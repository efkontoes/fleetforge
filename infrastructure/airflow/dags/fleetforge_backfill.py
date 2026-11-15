"""Backfills, all idempotent: replay Bronze, rebuild Silver, or reopen final Gold days."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, Param, task
from lakejobs import DEFAULT_ARGS, dbt_job, spark_job

with DAG(
    dag_id="fleetforge_backfill",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "kind": Param("silver_full", enum=["bronze_replay", "silver_full", "gold_reopen"]),
        "reopen_all": Param(False, type="boolean", description="gold_reopen: every final day"),
        "reopen_from": Param("", type="string", description="gold_reopen: first day, YYYY-MM-DD"),
        "reopen_to": Param("", type="string", description="gold_reopen: last day"),
    },
    tags=["lakehouse"],
):

    @task.branch
    def choose(params=None) -> str:
        return {"bronze_replay": "bronze_replay", "silver_full": "silver_full",
                "gold_reopen": "gold"}[params["kind"]]  # fmt: skip

    replay = spark_job("bronze_replay", "bronze_ingest", "--replay", minutes=60)
    full = spark_job("silver_full", "silver_build", "--full", minutes=60,
                     trigger_rule="none_failed_min_one_success")  # fmt: skip
    reopen = (
        '"reopen": {{ params.reopen_all | tojson }}, '
        '"reopen_from": "{{ params.reopen_from }}", "reopen_to": "{{ params.reopen_to }}"'
    )
    gold = dbt_job("gold", "build", "--exclude", "source:gold_releases+", extra_vars=reopen,
                   trigger_rule="none_failed_min_one_success")  # fmt: skip
    branch = choose()
    branch >> [replay, full, gold]
    replay >> full >> gold
