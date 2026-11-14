"""Prometheus metrics for the lakehouse, read from lakehouse_ops on every scrape.

python -m fleetforge.lakehouse.exporter   (port FLEETFORGE_LAKEHOUSE_EXPORTER_PORT, 9109)
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator

from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily

from fleetforge.lakehouse.settings import LakehouseSettings

log = logging.getLogger("fleetforge.lakehouse.exporter")

LATEST = """
SELECT DISTINCT ON (job) job, exit_code, extract(epoch FROM finished_at),
       extract(epoch FROM finished_at - started_at), report
FROM lakehouse_ops.job_runs ORDER BY job, finished_at DESC
"""
LAST_SUCCESS = """
SELECT job, extract(epoch FROM max(finished_at)) FROM lakehouse_ops.job_runs
WHERE exit_code = 0 GROUP BY job
"""
FAILURES = """
SELECT job, count(*) FROM lakehouse_ops.job_runs
WHERE exit_code <> 0 AND finished_at > now() - interval '1 hour' GROUP BY job
"""


class LakeCollector:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def collect(self) -> Iterator[GaugeMetricFamily]:
        up = GaugeMetricFamily("fleetforge_lake_ops_up", "1 if lakehouse_ops could be read")
        try:
            yield from self._metrics()
            up.add_metric([], 1)
        except Exception as exc:  # report, don't crash the scrape
            log.warning("lakehouse_ops collection failed: %s", type(exc).__name__)
            up.add_metric([], 0)
        yield up

    def _metrics(self) -> Iterator[GaugeMetricFamily]:
        import psycopg

        g = GaugeMetricFamily
        success = g(
            "fleetforge_lake_job_last_success_timestamp_seconds",
            "When the job last finished with exit code 0",
            labels=["job"],
        )
        last_exit = g(
            "fleetforge_lake_job_last_exit_code", "Exit code of the newest run", labels=["job"]
        )
        duration = g(
            "fleetforge_lake_job_duration_seconds", "Duration of the newest run", labels=["job"]
        )
        failures = g(
            "fleetforge_lake_job_failures_last_hour", "Failed runs in the last hour", labels=["job"]
        )
        new_rows = g("fleetforge_lake_bronze_new_rows", "Rows the newest Bronze run added")
        recon = g(
            "fleetforge_lake_reconciliation_failed_checks",
            "Failed checks in the newest reconciliation run",
        )
        fresh = g(
            "fleetforge_lake_freshness_seconds",
            "Kafka-to-Silver latency over 24 h",
            labels=["quantile"],
        )
        within = g(
            "fleetforge_lake_freshness_within_target_ratio",
            "Share of records available in Silver within 15 minutes (SLO: 0.95)",
        )
        with psycopg.connect(self.dsn, connect_timeout=3) as conn:
            for job, at in conn.execute(LAST_SUCCESS):
                success.add_metric([job], float(at))
            for job, n in conn.execute(FAILURES):
                failures.add_metric([job], n)
            for job, code, _, seconds, report in conn.execute(LATEST):
                last_exit.add_metric([job], code)
                duration.add_metric([job], float(seconds))
                if job == "bronze_ingest" and isinstance(report, dict):
                    new_rows.add_metric([], report.get("new_rows", 0))
                if job == "reconcile" and isinstance(report, list):
                    recon.add_metric([], sum(1 for r in report if not r.get("passed")))
                if job == "freshness" and isinstance(report, dict) and report.get("records"):
                    for q in ("p50", "p95", "p99"):
                        fresh.add_metric([q], float(report[f"{q}_s"]))
                    within.add_metric([], float(report["within_target"]))
        yield from (success, last_exit, duration, failures, new_rows, recon, fresh, within)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    port = int(os.environ.get("FLEETFORGE_LAKEHOUSE_EXPORTER_PORT", "9109"))
    REGISTRY.register(LakeCollector(LakehouseSettings().catalog_dsn))
    start_http_server(port)
    log.info("lakehouse exporter on :%s", port)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
