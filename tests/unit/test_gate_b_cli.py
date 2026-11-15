"""Gate B reads the Airflow CLI's JSON from stdout that also carries log lines."""

import pytest

from scripts.gate_b import cli_json

# Captured from `airflow dags list-runs <dag> --output json` in the Airflow 3.3.2 image.
REAL = (
    "2026-11-15T01:42:14.175677Z [warning  ] Could not import graphviz. Rendering graph to"
    " the graphical format will not be possible. \n"
    "You might need to install the graphviz package and necessary system packages.\n"
    "Run `pip install graphviz` to attempt to install it. [py.warnings] category=UserWarning"
    " filename=/home/airflow/.local/lib/python3.12/site-packages/airflow/cli/commands/"
    "dag_command.py lineno=57\n"
    '[{"dag_id": "fleetforge_backfill", "run_id": "probe-1", "state": "queued",'
    ' "run_after": "2026-11-15T01:42:10.897554+00:00", "logical_date": "",'
    ' "start_date": "", "end_date": ""}]\n'
)


def test_json_after_structlog_warnings():
    assert cli_json(REAL)[0]["state"] == "queued"


def test_a_log_line_starting_with_a_bracket_is_skipped():
    out = '[2026-11-15T01:42:14] {dag.py:1} INFO - parsed\n{"state": "success"}\n'
    assert cli_json(out) == {"state": "success"}


def test_no_json_says_what_the_cli_printed():
    with pytest.raises(ValueError, match="graphviz"):
        cli_json(REAL.split("[{")[0])
