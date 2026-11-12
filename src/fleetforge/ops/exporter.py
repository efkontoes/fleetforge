"""fleet-ops-exporter: Prometheus metrics that no client library exposes for us.

Queried on every scrape (cheap catalog queries) with the read-only role:
  * Debezium connector/task state (Kafka Connect REST)
  * replication slot activity and retained WAL (the "abandoned slot fills the disk" risk)
  * connections per role, outbox backlog age, quarantine rows by reason/status,
    read-model staleness (time since the newest latest-state update)
"""

from __future__ import annotations

import logging
import os
import time

import httpx
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily
from sqlalchemy import text

from fleetforge.config import get_settings
from fleetforge.db.engine import make_engine
from fleetforge.observability import configure_logging

log = logging.getLogger("fleetforge.ops")
CONNECTORS = ("fleetforge-outbox", "fleetforge-reference")


class OpsCollector:
    def __init__(self, engine, connect_url: str):
        self.engine = engine
        self.connect = httpx.Client(base_url=connect_url, timeout=3.0)

    def collect(self):
        up = GaugeMetricFamily("fleetforge_ops_postgres_up", "1 if the exporter could query PG")
        try:
            yield from self._postgres()
            up.add_metric([], 1)
        except Exception as exc:  # report, don't crash the scrape
            log.warning("postgres collection failed", extra={"error": type(exc).__name__})
            up.add_metric([], 0)
        yield up
        yield from self._connect()

    def _postgres(self):
        g = GaugeMetricFamily
        slots = g("fleetforge_cdc_slot_retained_wal_bytes", "WAL kept by slot", labels=["slot"])
        active = g("fleetforge_cdc_slot_active", "1 if a consumer is attached", labels=["slot"])
        conns = g("fleetforge_pg_connections", "Backends by role/state", labels=["role", "state"])
        outbox = g("fleetforge_outbox_rows", "Rows in fleet.outbox_events")
        outbox_age = g("fleetforge_outbox_newest_age_seconds", "Seconds since newest outbox row")
        quarantine = g("fleetforge_quarantine_rows", "Audit rows", labels=["reason", "status"])
        stale = g("fleetforge_read_model_staleness_seconds", "Since newest latest-state update")
        with self.engine.connect() as c:
            for r in c.execute(text("""
                SELECT slot_name, active,
                       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0) AS retained
                FROM pg_replication_slots""")):  # fmt: skip
                slots.add_metric([r.slot_name], float(r.retained))
                active.add_metric([r.slot_name], 1.0 if r.active else 0.0)
            for r in c.execute(text("""
                SELECT COALESCE(usename, 'system') AS role, COALESCE(state, 'n/a') AS state,
                       count(*) AS n
                FROM pg_stat_activity WHERE datname IS NOT NULL GROUP BY 1, 2""")):  # fmt: skip
                conns.add_metric([r.role, r.state], r.n)
            n, age = c.execute(text("""
                SELECT count(*), EXTRACT(EPOCH FROM now() - max(created_at))
                FROM fleet.outbox_events""")).one()  # fmt: skip
            outbox.add_metric([], n)
            outbox_age.add_metric([], float(age or 0))
            for r in c.execute(text("""
                SELECT reason_code, status, count(*) AS n FROM fleet.telemetry_rejections
                GROUP BY 1, 2""")):  # fmt: skip
                quarantine.add_metric([r.reason_code, r.status], r.n)
            s = c.execute(text("SELECT EXTRACT(EPOCH FROM now() - max(updated_at))"
                               " FROM fleet.robot_latest_state")).scalar()  # fmt: skip
            stale.add_metric([], float(s or 0))
        yield from (slots, active, conns, outbox, outbox_age, quarantine, stale)

    def _connect(self):
        up = GaugeMetricFamily("fleetforge_cdc_connector_up",
                               "1 if the connector and all its tasks are RUNNING",
                               labels=["connector"])  # fmt: skip
        for name in CONNECTORS:
            try:
                st = self.connect.get(f"/connectors/{name}/status").json()
                tasks = st.get("tasks", [])
                ok = (
                    st["connector"]["state"] == "RUNNING"
                    and bool(tasks)
                    and all(t["state"] == "RUNNING" for t in tasks)
                )
            except Exception:
                ok = False
            up.add_metric([name], 1.0 if ok else 0.0)
        yield up


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    connect_url = os.environ.get("CONNECT_URL", "http://connect:8083")
    REGISTRY.register(OpsCollector(make_engine(settings), connect_url))
    start_http_server(9103)
    log.info("ops exporter listening on :9103")
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
