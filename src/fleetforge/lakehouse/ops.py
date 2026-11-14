"""Job runs and OpenLineage events, kept in the catalog database for monitoring and audit."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS lakehouse_ops;
CREATE TABLE IF NOT EXISTS lakehouse_ops.job_runs (
    run_id text PRIMARY KEY,
    job text NOT NULL,
    exit_code integer NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    report jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS job_runs_job_finished ON lakehouse_ops.job_runs (job, finished_at);
CREATE TABLE IF NOT EXISTS lakehouse_ops.lineage_events (
    id bigserial PRIMARY KEY,
    event_time timestamptz NOT NULL,
    job text NOT NULL,
    run_id text NOT NULL,
    event jsonb NOT NULL
);
"""
PRODUCER = "https://github.com/efkontoes/fleetforge"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
NAMESPACE = "fleetforge"


def dataset(namespace: str, name: str, version: Any = None) -> dict[str, Any]:
    facets = {}
    if version is not None:
        facets["version"] = {
            "_producer": PRODUCER,
            "datasetVersion": str(version),
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasetVersionDatasetFacet.json",
        }
    return {"namespace": namespace, "name": name, "facets": facets}


def table(name: str, snapshot: Any) -> dict[str, Any]:
    """An Iceberg table at a snapshot, e.g. table("silver.telemetry_events", 123)."""
    return dataset("iceberg://lake", name, snapshot)


def run_event(
    job: str,
    run_id: str,
    inputs: Iterable[Mapping],
    outputs: Iterable[Mapping],
    state: str = "COMPLETE",
    at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "eventType": state,
        "eventTime": (at or datetime.now(UTC)).isoformat(),
        "run": {"runId": run_id},
        "job": {"namespace": NAMESPACE, "name": job},
        "inputs": list(inputs),
        "outputs": list(outputs),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
    }


def record(
    dsn: str,
    job: str,
    exit_code: int,
    started: datetime,
    report: Mapping[str, Any],
    lineage: Mapping[str, Any] | None = None,
) -> str:
    """Store one finished job run (and its lineage event); returns the run id."""
    import psycopg

    run_id = str(uuid.uuid4())
    with psycopg.connect(dsn, connect_timeout=5) as conn:
        conn.execute(SCHEMA)
        conn.execute(
            "INSERT INTO lakehouse_ops.job_runs VALUES (%s, %s, %s, %s, %s, %s)",
            (run_id, job, exit_code, started, datetime.now(UTC), json.dumps(report, default=str)),
        )
        if lineage is not None:
            event = run_event(job, run_id, lineage.get("inputs", []), lineage.get("outputs", []))
            conn.execute(
                "INSERT INTO lakehouse_ops.lineage_events (event_time, job, run_id, event) "
                "VALUES (%s, %s, %s, %s)",
                (event["eventTime"], job, run_id, json.dumps(event)),
            )
    return run_id
