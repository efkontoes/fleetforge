"""Gate B kills Silver's own container, never just the first container from the Spark image."""

from scripts import gate_b

# Shaped like `docker ps --no-trunc --format "{{.ID}} {{.Command}}"` while Airflow runs jobs.
PS = (
    'aaa111 "python -m fleetforge.lakehouse.jobs.reconcile"\n'
    'bbb222 "python -m fleetforge.lakehouse.jobs.silver_build"\n'
)


def test_the_job_is_found_by_its_command(monkeypatch):
    monkeypatch.setattr(gate_b, "sh", lambda *cmd, **kw: PS)
    assert gate_b.job_container("silver_build") == "bbb222"


def test_no_container_when_the_job_is_not_running(monkeypatch):
    monkeypatch.setattr(gate_b, "sh", lambda *cmd, **kw: PS)
    assert gate_b.job_container("bronze_ingest") is None
