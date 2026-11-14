"""fleetforge: one command for the local platform (stacks, jobs, tests and gates).

Every subcommand prints the command it runs, so nothing here is magic.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys

LAKE = ["docker", "compose", "--env-file", ".env.lakehouse",
        "-f", "infrastructure/lakehouse/compose.yaml"]  # fmt: skip
KAFKA = [*LAKE, "-f", "infrastructure/lakehouse/compose.kafka.yaml"]
AIRFLOW = [*LAKE, "-f", "infrastructure/airflow/compose.yaml"]
CORE = ["docker", "compose", "--profile", "core"]
TEST_CATALOG = ["-e", "FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL="
                "jdbc:postgresql://catalog-db:5432/iceberg_catalog_test",
                "-e", "FLEETFORGE_LAKEHOUSE_WAREHOUSE=s3://lakehouse/test-warehouse"]  # fmt: skip

UP = {
    "core": [[*CORE, "up", "-d", "--wait", "kafka", "schema-registry"]],
    "m2": [["docker", "compose", "--profile", "core", "--profile", "ops", "up", "-d", "--wait"]],
    "lake": [["make", "-f", "lakehouse.mk", "lake-up"]],
    "query": [["make", "-f", "lakehouse.mk", "lake-query"]],
    "airflow": [[*AIRFLOW, "--profile", "airflow", "up", "-d"]],  # --wait fails on the init job
}
TESTS = {
    "unit": [[".venv/bin/pytest", "tests/unit", "tests/ml", "-q"]],
    "lake": [["make", "-f", "lakehouse.mk", "lake-test"]],
    "trino": [["make", "-f", "lakehouse.mk", "lake-test-trino"]],
    "kafka": [["make", "-f", "lakehouse.mk", "lake-test-kafka"]],
}
GATES = {
    "a": [["make", "-f", "lakehouse.mk", "lake-gate-a"]],
    "c": [["make", "-f", "lakehouse.mk", "lake-gate-c"]],
}


def run(commands: list[list[str]], dry_run: bool) -> int:
    for cmd in commands:
        print("+", shlex.join(cmd), flush=True)
        if not dry_run and (code := subprocess.call(cmd)) != 0:
            return code
    return 0


def plan(args: argparse.Namespace) -> list[list[str]]:
    if args.command == "env":
        return [["sh", "scripts/make-env.sh"], ["sh", "scripts/make-lakehouse-env.sh"]]
    if args.command == "up":
        return UP[args.stack]
    if args.command == "down":
        lake = [[*AIRFLOW, "--profile", "airflow", "--profile", "query", "--profile", "jobs",
                 "down"]]  # fmt: skip
        return lake + ([["docker", "compose", "--profile", "core", "--profile", "ops", "down"]]
                       if args.all else [])  # fmt: skip
    if args.command == "status":
        return [["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}"], ["free", "-m"]]
    if args.command == "test":
        return TESTS[args.level]
    if args.command == "job":
        catalog = TEST_CATALOG if args.test_catalog else []
        return [[*KAFKA, "--profile", "jobs", "run", "--rm", *catalog, "spark", "-m",
                 f"fleetforge.lakehouse.jobs.{args.name}", *args.rest]]  # fmt: skip
    if args.command == "dbt":
        return [[*LAKE, "--profile", "query", "run", "--rm", "dbt", *args.rest]]
    if args.command == "gate":
        return GATES[args.which]
    raise SystemExit(f"unknown command {args.command}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fleetforge", description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="print the commands only")
    sub = ap.add_subparsers(dest="command", required=True)
    sub.add_parser("env", help="create .env and .env.lakehouse with random local secrets")
    up = sub.add_parser("up", help="start a stack (one heavy stack at a time on 4 GiB)")
    up.add_argument("stack", choices=sorted(UP))
    down = sub.add_parser("down", help="stop the lakehouse and Airflow (volumes kept)")
    down.add_argument("--all", action="store_true", help="also stop Milestone 2")
    sub.add_parser("status", help="running containers and free memory")
    test = sub.add_parser("test", help="run a test level")
    test.add_argument("level", choices=sorted(TESTS))
    job = sub.add_parser("job", help="run one job, e.g. `job --test-catalog silver_build --full`")
    job.add_argument("name", choices=["bronze_ingest", "silver_build", "release", "export",
                                      "maintenance", "reconcile", "freshness"])  # fmt: skip
    job.add_argument("--test-catalog", action="store_true", help="use the disposable catalog")
    job.add_argument("rest", nargs=argparse.REMAINDER)
    dbt = sub.add_parser("dbt", help="run dbt against Trino, e.g. `dbt build`")
    dbt.add_argument("rest", nargs=argparse.REMAINDER)
    gate = sub.add_parser("gate", help="run an integration gate")
    gate.add_argument("which", choices=sorted(GATES))
    args = ap.parse_args(argv)
    return run(plan(args), args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
