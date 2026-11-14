"""Projector metrics. One process = one Prometheus target; dashboards aggregate with
sum()/max() across the `instance` label, so totals are never "whichever process
answered". No event ids or robot ids in labels (unbounded cardinality)."""

from prometheus_client import Counter, Gauge, Histogram

RECORDS = Counter(
    "fleetforge_projector_records_total",
    "Records processed, by outcome and reason",
    ["outcome", "reason"],
)
QUARANTINED = Counter(
    "fleetforge_projector_quarantined_total",
    "Records quarantined as permanent data errors, by reason",
    ["reason"],
)
PARKED = Counter(
    "fleetforge_projector_parked_total",
    "Records parked because the registry kept answering 404 for their schema id",
    ["reason"],
)
BATCH_SIZE = Histogram(
    "fleetforge_projector_batch_size",
    "Records per processed batch",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000),
)
BATCH_SECONDS = Histogram(
    "fleetforge_projector_batch_seconds",
    "Wall time to process one batch (decode + validate + DB transaction)",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
COMMIT_SECONDS = Histogram(
    "fleetforge_projector_db_commit_seconds",
    "Duration of the PostgreSQL COMMIT itself (WAL flush)",
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
)
TRANSACTIONS = Counter("fleetforge_projector_db_transactions_total", "Committed transactions")
RETRIES = Counter(
    "fleetforge_projector_retries_total", "Batch retries after recoverable errors", ["error"]
)
DB_AVAILABLE = Gauge("fleetforge_projector_db_available", "1 if the last DB attempt succeeded")
LAG = Gauge(
    "fleetforge_projector_consumer_lag",
    "High watermark minus consumer position (records), per assigned partition",
    ["partition"],
)
EVENT_TO_STATE = Histogram(
    "fleetforge_projector_event_to_state_seconds",
    "Kafka CreateTime (producer) -> state committed in PostgreSQL",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300),
)
LAST_COMMIT = Gauge(
    "fleetforge_projector_last_commit_timestamp_seconds", "Unix time of the last DB commit"
)
POOL_CHECKED_OUT = Gauge(
    "fleetforge_projector_db_pool_checked_out", "Pooled DB connections currently in use"
)
POOL_SIZE = Gauge("fleetforge_projector_db_pool_size", "Configured DB pool size")
