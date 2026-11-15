"""The developer CLI only composes commands; check what it would run."""

from __future__ import annotations

from fleetforge import cli


def test_job_runs_in_the_spark_container_with_its_arguments(capsys):
    assert cli.main(["--dry-run", "job", "--test-catalog", "silver_build", "--full"]) == 0
    line = capsys.readouterr().out.strip()
    assert "spark -m fleetforge.lakehouse.jobs.silver_build --full" in line
    assert "iceberg_catalog_test" in line


def test_every_subcommand_has_a_plan(capsys):
    for argv in (["env"], ["up", "lake"], ["down", "--all"], ["status"], ["test", "unit"],
                 ["dbt", "build"], ["gate", "a"]):  # fmt: skip
        assert cli.main(["--dry-run", *argv]) == 0
    assert capsys.readouterr().out.count("+ ") >= 8
