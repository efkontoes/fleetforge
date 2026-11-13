"""Generate infrastructure/grafana/dashboards/fleetforge.json.

The dashboard answers six questions, one row each. Every panel aggregates across
instances explicitly (sum/max by ...), so per-process metrics never masquerade as
global totals.   Run: python scripts/build_dashboard.py
"""

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "infrastructure/grafana/dashboards/fleetforge.json"
DS = {"type": "prometheus", "uid": "prometheus"}


def ts(title, exprs, unit="short", w=8, desc=""):
    return {"type": "timeseries", "title": title, "description": desc, "datasource": DS,
            "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
            "targets": [{"expr": e, "legendFormat": lf, "refId": chr(65 + i), "datasource": DS}
                        for i, (e, lf) in enumerate(exprs)], "gridPos": {"w": w, "h": 8}}  # fmt: skip


def stat(title, expr, unit="short", w=4, thresholds=None, desc=""):
    steps = [{"color": "green", "value": None}] + [
        {"color": c, "value": v} for v, c in (thresholds or [])]  # fmt: skip
    return {"type": "stat", "title": title, "description": desc, "datasource": DS,
            "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute",
                                                                      "steps": steps}}},
            "targets": [{"expr": expr, "refId": "A", "datasource": DS}],
            "gridPos": {"w": w, "h": 4}}  # fmt: skip


ROWS = [
    (
        "1. Is ingestion healthy?",
        [
            stat("Projector records/s", "sum(rate(fleetforge_projector_records_total[1m]))", "ops"),
            stat(
                "DB available (min over projectors)",
                "min(fleetforge_projector_db_available)",
                thresholds=[(0, "red"), (1, "green")],
            ),
            stat(
                "Batch retries (5m)",
                "sum(increase(fleetforge_projector_retries_total[5m]))",
                thresholds=[(1, "orange")],
            ),
            stat(
                "Kafka ingress msg/s (telemetry)",
                'sum(rate(kafka_topic_partition_current_offset{topic="telemetry.raw.v1"}[1m]))',
                "ops",
            ),
            stat("API readiness", 'up{job="api"}', thresholds=[(0, "red"), (1, "green")]),
            stat("Projector instances", 'count(up{job="projector"} == 1)'),
            ts(
                "Records/s by outcome",
                [
                    (
                        "sum by (outcome) (rate(fleetforge_projector_records_total[1m]))",
                        "{{outcome}}",
                    )
                ],
                "ops",
                12,
            ),
            ts(
                "Retries by error",
                [("sum by (error) (rate(fleetforge_projector_retries_total[1m]))", "{{error}}")],
                "ops",
                12,
            ),
        ],
    ),
    (
        "2. Are consumers keeping up?",
        [
            ts(
                "Projector lag (records, sum over partitions)",
                [
                    ("sum(fleetforge_projector_consumer_lag)", "projector (own view)"),
                    (
                        'sum(kafka_consumergroup_lag{consumergroup="telemetry-projector"})',
                        "broker view (advisory commits)",
                    ),
                ],
                "short",
                12,
                "Own view = high watermark - position. Broker view uses Kafka-committed offsets, "
                "which trail the DB ledger by design.",
            ),
            ts(
                "Ingress vs processed (msg/s)",
                [
                    (
                        'sum(rate(kafka_topic_partition_current_offset{topic="telemetry.raw.v1"}[1m]))',
                        "ingress",
                    ),
                    ("sum(rate(fleetforge_projector_records_total[1m]))", "processed"),
                ],
                "ops",
                12,
            ),
        ],
    ),
    (
        "3. Are database updates current?",
        [
            ts(
                "Event -> state latency (Kafka CreateTime -> commit)",
                [
                    (
                        f"histogram_quantile({q}, sum by (le) (rate(fleetforge_projector_event_to_state_seconds_bucket[1m])))",
                        f"p{int(q * 100)}",
                    )
                    for q in (0.5, 0.95, 0.99)
                ],
                "s",
                12,
            ),
            ts(
                "Read-model staleness and last commit age",
                [
                    ("fleetforge_read_model_staleness_seconds", "since newest state row"),
                    (
                        "time() - max(fleetforge_projector_last_commit_timestamp_seconds)",
                        "since last projector commit",
                    ),
                ],
                "s",
                12,
            ),
        ],
    ),
    (
        "4. Are messages being rejected?",
        [
            ts(
                "Quarantined/s by reason",
                [
                    (
                        "sum by (reason) (rate(fleetforge_projector_quarantined_total[1m]))",
                        "{{reason}}",
                    )
                ],
                "ops",
                8,
            ),
            ts(
                "Stale and duplicate/s",
                [
                    ('sum(rate(fleetforge_projector_records_total{outcome="STALE"}[1m]))', "stale"),
                    (
                        'sum(rate(fleetforge_projector_records_total{outcome="DUPLICATE"}[1m]))',
                        "duplicate",
                    ),
                    (
                        'sum(rate(fleetforge_projector_records_total{outcome="SKIPPED"}[1m]))',
                        "skipped (redelivery)",
                    ),
                ],
                "ops",
                8,
            ),
            ts(
                "Open quarantine (rows)",
                [
                    (
                        'sum by (reason) (fleetforge_quarantine_rows{status="QUARANTINED"})',
                        "{{reason}}",
                    )
                ],
                "short",
                8,
            ),
        ],
    ),
    (
        "5. Is CDC operating correctly?",
        [
            stat(
                "Connectors up",
                "min(fleetforge_cdc_connector_up)",
                thresholds=[(0, "red"), (1, "green")],
            ),
            stat(
                "Slots active",
                "min(fleetforge_cdc_slot_active)",
                thresholds=[(0, "red"), (1, "green")],
            ),
            stat(
                "Max WAL retained by a slot",
                "max(fleetforge_cdc_slot_retained_wal_bytes)",
                "bytes",
                thresholds=[(256 * 2**20, "orange"), (512 * 2**20, "red")],
            ),
            stat("Outbox newest row age", "fleetforge_outbox_newest_age_seconds", "s"),
            ts(
                "CDC end-to-end (outbox commit -> downstream applied)",
                [
                    (
                        f"histogram_quantile({q}, sum by (le) (rate(fleetforge_cdc_end_to_end_seconds_bucket[5m])))",
                        f"p{int(q * 100)}",
                    )
                    for q in (0.5, 0.95)
                ],
                "s",
                12,
            ),
            ts(
                "Mission consumer events/s",
                [
                    (
                        "sum by (outcome) (rate(fleetforge_mission_consumer_events_total[1m]))",
                        "{{outcome}}",
                    )
                ],
                "ops",
                12,
            ),
        ],
    ),
    (
        "6. Where is latency accumulating?",
        [
            ts(
                "Batch processing p95 vs DB commit p95",
                [
                    (
                        "histogram_quantile(0.95, sum by (le) (rate(fleetforge_projector_batch_seconds_bucket[1m])))",
                        "batch",
                    ),
                    (
                        "histogram_quantile(0.95, sum by (le) (rate(fleetforge_projector_db_commit_seconds_bucket[1m])))",
                        "commit (WAL flush)",
                    ),
                ],
                "s",
                8,
            ),
            ts(
                "Average batch size",
                [
                    (
                        "sum(rate(fleetforge_projector_batch_size_sum[1m])) / sum(rate(fleetforge_projector_batch_size_count[1m]))",
                        "records/batch",
                    )
                ],
                "short",
                8,
            ),
            ts(
                "DB transactions/s and pool use",
                [
                    ("sum(rate(fleetforge_projector_db_transactions_total[1m]))", "projector tx/s"),
                    ("sum(fleetforge_projector_db_pool_checked_out)", "projector pool checked out"),
                    (
                        'sum by (role) (fleetforge_pg_connections{role=~"fleetforge.*"})',
                        "{{role}} connections",
                    ),
                ],
                "short",
                8,
            ),
        ],
    ),
]


def build():
    panels, y, pid = [], 0, 1
    for title, row in ROWS:
        panels.append({"type": "row", "title": title, "collapsed": False, "id": pid,
                       "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}})  # fmt: skip
        pid, y, x, row_h = pid + 1, y + 1, 0, 0
        for p in row:
            g = p["gridPos"]
            if x + g["w"] > 24:
                x, y, row_h = 0, y + row_h, 0
            p["gridPos"] = {**g, "x": x, "y": y}
            p["id"] = pid
            pid, x, row_h = pid + 1, x + g["w"], max(row_h, g["h"])
            panels.append(p)
        y += row_h
    return {"uid": "fleetforge-m2", "title": "FleetForge — ingestion, CDC and reliability",
            "schemaVersion": 39, "version": 1, "refresh": "10s", "time": {"from": "now-30m", "to": "now"},
            "tags": ["fleetforge"], "panels": panels, "templating": {"list": []}, "annotations": {"list": []}}  # fmt: skip


if __name__ == "__main__":
    OUT.write_text(json.dumps(build(), indent=2) + "\n")
    print(f"wrote {OUT}")
