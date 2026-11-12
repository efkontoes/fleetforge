"""Structured (JSON) logging and Prometheus metrics."""

import json
import logging
import sys
from datetime import UTC, datetime

from prometheus_client import Counter, Histogram

TELEMETRY_EVENTS = Counter(
    "fleetforge_telemetry_events_total",
    "Telemetry events by ingestion outcome and reason",
    ["outcome", "reason"],
)
TELEMETRY_LATENCY = Histogram(
    "fleetforge_telemetry_ingest_seconds",
    "Server-side telemetry ingestion latency (parse + validate + DB transaction)",
    ["outcome"],
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
MISSION_TRANSITIONS = Counter(
    "fleetforge_mission_transitions_total",
    "Committed mission transitions",
    ["to_status"],
)
DB_UNAVAILABLE = Counter(
    "fleetforge_db_unavailable_total",
    "Requests answered 503 because the database was unreachable",
)

_RESERVED = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Anything passed via logger.info(..., extra={...}) becomes a top-level field.
        doc.update({k: v for k, v in record.__dict__.items() if k not in _RESERVED})
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
