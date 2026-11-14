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


def airflow_json(*args: str):
    """The CLI's JSON, skipping the log lines Airflow prints around it."""
    out = airflow(*args, "--output", "json")
    start = min(i for i in (out.find("["), out.find("{")) if i >= 0)
    return json.JSONDecoder().raw_decode(out[start:])[0]


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


def tries(dag: str, run_id: str) -> dict[str, int]:
    rows = airflow_json("tasks", "states-for-dag-run", dag, run_id)
    return {t["task_id"]: int(t.get("try_number") or 0) for t in rows}


def publish(batch: int) -> int:
    """Publish one corpus batch to the real topics, through the Spark image's Python."""
    code = f"from scripts.gate_b_publish import publish; print(publish({batch}))"
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
    return int(out.strip().splitlines()[-1])


def scenario(
    evidence: dict,
    name: str,
    batch: int,
    fault,
    dag: str = "fleetforge_lakehouse",
    conf: dict | None = None,
) -> None:
    published = publish(batch) if batch is not None else 0
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


def restart_registry_during_bronze(dag: str, run_id: str) -> str:
    wait_task(dag, run_id, "bronze")
    time.sleep(25)
    sh(*M2, "restart", "schema-registry")
    return "schema registry restarted while Bronze ran"


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
    scenario(evidence, "registry restart during Bronze", 4, restart_registry_during_bronze)
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
