"""Create a dataset release from a Silver build, prove it reproduces, and test it."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, Param
from lakejobs import DEFAULT_ARGS, dbt_job, spark_job

with DAG(
    dag_id="fleetforge_release",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "release_id": Param(type="string", pattern=r"^[a-z0-9][a-z0-9._-]{0,63}$"),
        "seed": Param(type="string", minLength=1),
        "silver_build": Param("", type="string", description="empty: the newest build"),
    },
    tags=["lakehouse"],
):
    create = spark_job("create", "release", "create", "--release-id", "{{ params.release_id }}",
                       "--seed", "{{ params.seed }}",
                       "--silver-build={{ params.silver_build }}")  # fmt: skip
    reproduce = spark_job("reproduce", "release", "reproduce", "--release-id",
                          "{{ params.release_id }}", writer=False)  # fmt: skip
    tests = dbt_job("release_tests", "test", "--select", "source:gold_releases+", writer=False)
    create >> reproduce >> tests
