"""Gate B: the lakehouse under Airflow, recovering from isolated faults.

Needs Milestone 2's kafka and schema-registry, the lakehouse stack with Trino, and the
Airflow overlay (about 5.5 GiB: a CI runner, not the 3.7 GiB laptop). Each scenario
injects one fault, waits for the DAG run to finish, and then requires reconciliation to
pass. Evidence: build/lakehouse-evidence/gate-b.json.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

EVIDENCE = Path("build/lakehouse-evidence/gate-b.json")
LAKE = [
    "docker",
    "compose",
    "--env-file",
    ".env.lakehouse",
    "-f",
    "infrastructure/lakehouse/compose.yaml",
    "-f",
    "infrastructure/airflow/compose.yaml",
    "--profile",
    "airflow",
    "--profile",
    "query",
]
M2 = ["docker", "compose", "--profile", "core"]
SPARK_IMAGE = "fleetforge-lakehouse-spark:dev"


def sh(*cmd: str, check: bool = True) -> str:
    out = subprocess.run(cmd, capture_output=True, text=True)
    if check and out.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}\n{out.stdout[-2000:]}\n{out.stderr[-2000:]}")
    return out.stdout


def airflow(*args: str) -> str:
    return sh(*LAKE, "exec", "-T", "airflow-scheduler", "airflow", *args)


def cli_json(out: str):
    """The JSON in the CLI's stdout, which also carries log lines such as "... [warning ] ..."."""
    lines = out.splitlines(keepends=True)
    for i, line in enumerate(lines):
        if line.startswith(("[", "{")):
            try:
                return json.JSONDecoder().raw_decode("".join(lines[i:]))[0]
            except json.JSONDecodeError:
                continue  # a log line that happens to start with a bracket
    raise ValueError(f"no JSON in the Airflow CLI output:\n{out[-2000:]}")


def airflow_json(*args: str):
    return cli_json(airflow(*args, "--output", "json"))


def trigger(dag: str, conf: dict | None = None) -> str:
    run_id = f"gate-b-{dag}-{int(time.time() * 1000)}"
    airflow(
        "dags", "trigger", dag, "--run-id", run_id, *(["--conf", json.dumps(conf)] if conf else [])
    )
    return run_id


def states(dag: str, run_id: str) -> dict[str, str]:
    return {
        t["task_id"]: t["state"] for t in airflow_json("tasks", "states-for-dag-run", dag, run_id)
    }


def run_state(dag: str, run_id: str) -> str:
    return next(r["state"] for r in airflow_json("dags", "list-runs", dag) if r["run_id"] == run_id)


def wait(dag: str, run_id: str, minutes: int = 40) -> str:
    deadline = time.monotonic() + minutes * 60
    while time.monotonic() < deadline:
        state = run_state(dag, run_id)
        if state in ("success", "failed"):
            return state
        time.sleep(15)
    raise TimeoutError(f"{dag} {run_id} still running after {minutes} min")


def wait_task(dag: str, run_id: str, task: str, state: str = "running", minutes: int = 20) -> None:
    deadline = time.monotonic() + minutes * 60
    while time.monotonic() < deadline:
        if states(dag, run_id).get(task) == state:
            return
        time.sleep(5)
    raise TimeoutError(f"{task} never reached {state}")


def job_container() -> str | None:
    ids = sh("docker", "ps", "-q", "--filter", f"ancestor={SPARK_IMAGE}").split()
    return ids[0] if ids else None


def psql(db: str, sql: str) -> list[list[str]]:
    """Rows from a database on catalog-db (Airflow's or the catalog's), as tab-separated fields."""
    out = sh(*LAKE, "exec", "-T", "catalog-db", "psql", "-U", "iceberg", "-d", db, "-At",
             "-F", "\t", "-c", sql)  # fmt: skip
    return [line.split("\t") for line in out.splitlines() if line]


def tries(dag: str, run_id: str) -> dict[str, int]:
    """Attempts per task, read from Airflow's database because the CLI does not show them."""
    where = f"dag_id = '{dag}' AND run_id = '{run_id}'"
    sql = f"SELECT task_id, try_number FROM task_instance WHERE {where}"
    return {task: int(n) for task, n in psql("airflow", sql)}


def bronze_runs(dag: str, run_id: str) -> list[tuple[int, dict]]:
    """Bronze's recorded runs since the DAG run was queued, oldest first: exit code and report."""
    sql = f"SELECT queued_at FROM dag_run WHERE dag_id = '{dag}' AND run_id = '{run_id}'"
    queued = psql("airflow", sql)[0][0]
    sql = (
        "SELECT exit_code, report FROM lakehouse_ops.job_runs"
        f" WHERE job = 'bronze_ingest' AND started_at >= '{queued}' ORDER BY started_at"
    )
    return [(int(code), json.loads(report)) for code, report in psql("iceberg_catalog", sql)]


def publish(batch: int, v2_id: int | None = None) -> int:
    """Publish one corpus batch to the real topics; v2_id replaces revision 2's schema id."""
    code = f"from scripts.gate_b_publish import publish; print(publish({batch}, {v2_id}))"
    return int(spark_python(code))


def fresh_schema() -> int:
    """Register a schema no Bronze run has archived, and return its id."""
    code = "from scripts.gate_b_publish import fresh_schema; print(fresh_schema())"
    return int(spark_python(code))


def spark_python(code: str) -> str:
    """Run Python in the Spark image, which has the project's dependencies; return the last line."""
    out = sh(
        "docker",
        "compose",
        "--env-file",
        ".env.lakehouse",
        "-f",
        "infrastructure/lakehouse/compose.yaml",
        "-f",
        "infrastructure/lakehouse/compose.kafka.yaml",
        "--profile",
        "jobs",
        "run",
        "--rm",
        "--no-deps",
        "-e",
        "PYTHONPATH=/workspace/src:/workspace",
        "spark",
        "-c",
        code,
    )
    return out.strip().splitlines()[-1]


def scenario(
    evidence: dict,
    name: str,
    batch: int,
    fault,
    dag: str = "fleetforge_lakehouse",
    conf: dict | None = None,
    v2_id: int | None = None,
) -> None:
    published = publish(batch, v2_id) if batch is not None else 0
    start = time.monotonic()
    run_id = trigger(dag, conf)
    detail = fault(dag, run_id) if fault else None
    final = wait(dag, run_id)
    record = {
        "scenario": name,
        "published": published,
        "run_id": run_id,
        "state": final,
        "tries": tries(dag, run_id),
        "detail": detail,
        "seconds": round(time.monotonic() - start),
    }
    evidence["scenarios"].append(record)
    print(json.dumps(record), flush=True)
    assert final == "success", record


def kill_scheduler_during_silver(dag: str, run_id: str) -> str:
    wait_task(dag, run_id, "silver")
    time.sleep(20)
    sh("docker", "kill", "fleetforge-lakehouse-airflow-scheduler-1")
    time.sleep(30)
    sh(*LAKE, "up", "-d", "airflow-scheduler")
    return "scheduler killed while silver ran, then restarted"


def kill_spark_during_silver(dag: str, run_id: str) -> str:
    wait_task(dag, run_id, "silver")
    time.sleep(45)  # past session start, inside the build
    container = job_container()
    assert container, "no Spark job container"
    sh("docker", "kill", container)
    return f"killed job container {container[:12]}"


def garage_down_before_bronze(dag: str, run_id: str) -> str:
    sh(*LAKE, "stop", "garage")
    wait_task(dag, run_id, "bronze", "up_for_retry", minutes=15)
    sh(*LAKE, "start", "garage")
    return "Garage stopped until Bronze failed once, then started"


def registry_down_at_schema_lookup(dag: str, run_id: str, schema_id: int) -> str:
    """Bronze must look up a schema id it has never archived while the registry is stopped."""
    sh(*M2, "stop", "schema-registry")
    wait_task(dag, run_id, "bronze", "up_for_retry", minutes=15)
    sh(*M2, "start", "schema-registry")
    state = wait(dag, run_id)
    assert state == "success", f"run {state}"
    runs = bronze_runs(dag, run_id)
    (first_code, first), (last_code, last) = runs[0], runs[-1]
    error = first.get("error", "")
    assert first_code == 75 and f"RegistryUnavailable: schema {schema_id}:" in error, runs
    assert last_code == 0 and last["schemas_archived"] >= 1, runs
    return (
        f"registry stopped before Bronze looked up new schema id {schema_id}; attempt 1 exited 75"
        f" ({error[:160]}); attempt {len(runs)} archived it after the registry started"
    )


def overlapping_backfill(dag: str, run_id: str) -> str:
    wait_task(dag, run_id, "bronze")
    backfill = trigger("fleetforge_backfill", {"kind": "silver_full"})
    state = wait("fleetforge_backfill", backfill)
    assert state == "success", f"backfill {state}"
    return f"backfill {backfill} ran beside the pipeline: {state}"


def reconcile_ok() -> list[dict]:
    out = sh(
        "docker",
        "compose",
        "--env-file",
        ".env.lakehouse",
        "-f",
        "infrastructure/lakehouse/compose.yaml",
        "-f",
        "infrastructure/airflow/compose.yaml",
        "--profile",
        "jobs",
        "run",
        "--rm",
        "--no-deps",
        "spark",
        "-m",
        "fleetforge.lakehouse.jobs.reconcile",
    )
    rows = json.loads(out.strip().splitlines()[-1])
    assert rows and all(r["passed"] for r in rows), [r for r in rows if not r["passed"]]
    return rows


def wait_for_dags(minutes: int = 6) -> None:
    deadline = time.monotonic() + minutes * 60
    while time.monotonic() < deadline:
        listed = subprocess.run(
            [*LAKE, "exec", "-T", "airflow-scheduler", "airflow", "dags", "list"],
            capture_output=True,
            text=True,
        ).stdout
        if "fleetforge_lakehouse" in listed and "fleetforge_backfill" in listed:
            return
        time.sleep(10)
    raise TimeoutError("Airflow did not parse the DAGs")


def main() -> None:
    wait_for_dags()
    evidence: dict = {"started_at": datetime.now(UTC).isoformat(), "scenarios": []}
    for dag in ("fleetforge_lakehouse", "fleetforge_backfill", "fleetforge_release"):
        airflow("dags", "unpause", dag)
    scenario(evidence, "baseline run", 0, None)
    scenario(evidence, "repeat a completed run (idempotent)", None, None)
    scenario(evidence, "Airflow scheduler killed during Silver", 1, kill_scheduler_during_silver)
    scenario(evidence, "Spark job container killed during Silver", 2, kill_spark_during_silver)
    scenario(evidence, "Garage unavailable at Bronze", 3, garage_down_before_bronze)
    schema_id = fresh_schema()
    lookup = partial(registry_down_at_schema_lookup, schema_id=schema_id)
    scenario(evidence, "registry down at Bronze's schema lookup", 4, lookup, v2_id=schema_id)
    scenario(evidence, "backfill overlapping the pipeline", None, overlapping_backfill)
    scenario(
        evidence,
        "release created and reproduced",
        None,
        None,
        "fleetforge_release",
        {"release_id": "gate-b-r1", "seed": "b1", "silver_build": ""},
    )
    evidence["reconciliation"] = reconcile_ok()
    evidence["finished_at"] = datetime.now(UTC).isoformat()
    EVIDENCE.write_text(json.dumps(evidence, indent=2))
    print("Gate B passed", flush=True)


if __name__ == "__main__":
    threading.excepthook = lambda a: sys.exit(1)
    main()
