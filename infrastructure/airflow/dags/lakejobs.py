"""Lakehouse tasks: each job runs in its own container, exactly as from the command line.

Exit code 65 marks a failure a retry cannot fix, so the task fails at once; any other
failure is retried with backoff. Writer tasks share a one-slot pool, so no two DAGs
write the lakehouse at the same time.
"""

from __future__ import annotations

import os
import re
from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.docker.exceptions import DockerContainerFailedException
from airflow.providers.docker.operators.docker import DockerOperator

PERMANENT_EXIT = 65
WRITERS_POOL = "lakehouse_writers"
NETWORK = os.environ.get("FLEETFORGE_JOB_NETWORK", "fleetforge_default")
DOCKER_URL = os.environ.get("FLEETFORGE_DOCKER_URL", "tcp://docker-proxy:2375")
SPARK_IMAGE = os.environ.get("FLEETFORGE_SPARK_IMAGE", "fleetforge-lakehouse-spark:dev")
DBT_IMAGE = os.environ.get("FLEETFORGE_DBT_IMAGE", "fleetforge-lakehouse-dbt:dev")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


def _var(name: str, default: str = "") -> str:
    # Rendered at run time; names with "password", "secret" or "access_key" are masked.
    return "{{ var.value.get('" + name + "', '" + default + "') }}"


LAKE_ENV = {
    "FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL": _var(
        "lakehouse_catalog_jdbc_url", "jdbc:postgresql://catalog-db:5432/iceberg_catalog"
    ),
    "FLEETFORGE_LAKEHOUSE_WAREHOUSE": _var("lakehouse_warehouse", "s3://lakehouse/warehouse"),
    "FLEETFORGE_LAKEHOUSE_CATALOG_USER": "iceberg",
    "FLEETFORGE_LAKEHOUSE_CATALOG_PASSWORD": _var("lakehouse_catalog_password"),
    "FLEETFORGE_LAKEHOUSE_S3_ENDPOINT": "http://garage:3900",
    "FLEETFORGE_LAKEHOUSE_S3_ACCESS_KEY": _var("lakehouse_s3_access_key"),
    "FLEETFORGE_LAKEHOUSE_S3_SECRET_KEY": _var("lakehouse_s3_secret_key"),
    "FLEETFORGE_LAKEHOUSE_TELEMETRY_TOPIC": _var("lakehouse_telemetry_topic", "telemetry.raw.v1"),
    "FLEETFORGE_LAKEHOUSE_DRIVER_MEMORY": "768m",
    "FLEETFORGE_LAKEHOUSE_SPARK_MASTER": "local[1]",
    "FLEETFORGE_KAFKA_BOOTSTRAP": "kafka:9092",
    "FLEETFORGE_SCHEMA_REGISTRY_URL": "http://schema-registry:8080/apis/ccompat/v7",
    "AWS_REQUEST_CHECKSUM_CALCULATION": "WHEN_REQUIRED",
    "AWS_RESPONSE_CHECKSUM_VALIDATION": "WHEN_REQUIRED",
    "MALLOC_ARENA_MAX": "2",
}


class LakeJob(DockerOperator):
    """A job container whose exit code 65 fails the task without retries."""

    def execute(self, context):
        try:
            return super().execute(context)
        except DockerContainerFailedException as exc:
            if re.search(rf"'StatusCode': {PERMANENT_EXIT}\b", str(exc)):
                raise AirflowFailException(f"permanent failure, not retried: {exc}") from exc
            raise


def _container(task_id: str, image: str, command: list[str], timeout: timedelta,
               writer: bool, mem_limit: str, environment: dict, **kwargs) -> LakeJob:  # fmt: skip
    return LakeJob(
        task_id=task_id, image=image, command=command, environment=environment,
        network_mode=NETWORK, docker_url=DOCKER_URL, auto_remove="success",
        mount_tmp_dir=False, mem_limit=mem_limit, execution_timeout=timeout,
        pool=WRITERS_POOL if writer else "default_pool", **kwargs,
    )  # fmt: skip


def spark_job(task_id: str, module: str, *args: str, minutes: int = 30, writer: bool = True,
              **kwargs) -> LakeJob:  # fmt: skip
    """python -m fleetforge.lakehouse.jobs.<module> in the Spark image."""
    command = ["python", "-m", f"fleetforge.lakehouse.jobs.{module}", *args]
    return _container(task_id, SPARK_IMAGE, command, timedelta(minutes=minutes), writer, "2g",
                      LAKE_ENV, **kwargs)  # fmt: skip


def dbt_job(task_id: str, *args: str, minutes: int = 20, writer: bool = True,
            extra_vars: str = "", **kwargs) -> LakeJob:  # fmt: skip
    """dbt <args> in the dbt image, against Trino; extra_vars is a JSON fragment."""
    env = {"DBT_TARGET": _var("lakehouse_dbt_target", "lake")}
    topic = _var("lakehouse_telemetry_topic", "telemetry.raw.v1")
    vars_ = '{"telemetry_topic": "' + topic + '"' + (f", {extra_vars}" if extra_vars else "") + "}"
    return _container(task_id, DBT_IMAGE, [*args, "--vars", vars_], timedelta(minutes=minutes),
                      writer, "512m", env, **kwargs)  # fmt: skip
