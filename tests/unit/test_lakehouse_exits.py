"""Job exit codes tell the scheduler whether a retry can help."""

from __future__ import annotations

import pytest

from fleetforge.lakehouse.jobs import exits


class BronzePositionError(RuntimeError):
    pass


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (RuntimeError("LeaseUnavailable: lake.bronze held"), exits.TEMPORARY),
        (RuntimeError("org.apache.iceberg.exceptions.CommitFailedException: ..."), exits.TEMPORARY),
        (ConnectionError("Connection refused"), exits.TEMPORARY),
        (BronzePositionError("topic was recreated"), exits.PERMANENT),
        (ValueError("something new"), 1),
    ],
)
def test_failures_are_classified(exc, code):
    assert exits.classify(exc) == code


def test_run_exits_with_the_classified_code(monkeypatch, capsys):
    monkeypatch.setenv("FLEETFORGE_LAKEHOUSE_RECORD_RUNS", "false")

    def broken():
        raise RuntimeError("ConcurrentWrite: events moved")

    with pytest.raises(SystemExit) as stop:
        exits.run(broken)
    assert stop.value.code == exits.TEMPORARY
    with pytest.raises(SystemExit) as ok:
        exits.run(lambda: (0, {"new_rows": 3}))
    assert ok.value.code == 0
    assert capsys.readouterr().out.strip().splitlines()[-1] == '{"new_rows": 3}'
