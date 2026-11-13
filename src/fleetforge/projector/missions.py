"""Independent consumer of fleet.mission-events.v1 (published by Debezium from the
transactional outbox). Maintains downstream.mission_status.

It shares nothing with the Fleet API except the event contract: it would work the
same if it lived in another service with another database.

Idempotency: each event carries the mission `version`. A row is updated only when
the incoming version is greater than the stored one, so redeliveries (same version)
and late deliveries (lower version) are harmless no-ops. Offsets are tracked in the
same transaction via fleet.consumer_offsets, exactly like the telemetry projector.
"""

from __future__ import annotations

import faulthandler
import json
import logging
import signal
import sys
import threading
import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from confluent_kafka import Consumer, KafkaException, TopicPartition
from prometheus_client import Counter, Histogram, start_http_server
from sqlalchemy import Engine

from fleetforge.config import Settings, get_settings
from fleetforge.db import batch as sqlb
from fleetforge.db.engine import make_engine
from fleetforge.kafka import offsets as kafka_offsets
from fleetforge.observability import configure_logging
from fleetforge.projector.main import _close_bounded

log = logging.getLogger("fleetforge.missions")
GROUP = "mission-consumer"

EVENTS = Counter("fleetforge_mission_consumer_events_total", "Mission events", ["outcome"])
CDC_LATENCY = Histogram(
    "fleetforge_cdc_end_to_end_seconds",
    "Outbox commit time (occurred_at) -> applied by the downstream consumer",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300),
)


def parse(value: bytes | None) -> dict[str, Any] | None:
    try:
        ev = json.loads(value or b"")
        return {
            "event_id": UUID(ev["event_id"]),
            "mission_id": UUID(ev["mission_id"]),
            "robot_id": UUID(ev["robot_id"]),
            "status": str(ev["to_status"]),
            "version": int(ev["version"]),
            "occurred_at": datetime.fromisoformat(ev["occurred_at"]),
        }
    except (ValueError, KeyError, TypeError):
        return None


def apply_events(engine: Engine, msgs: list[tuple[str, int, int, bytes | None]],
                 group: str = GROUP,
                 topic_ids: dict[str, str] | None = None) -> dict[str, int]:  # fmt: skip
    """msgs: (topic, partition, offset, value). One transaction for the batch."""
    outcome = {"applied": 0, "ignored": 0, "malformed": 0, "skipped": 0}
    with engine.begin() as conn:
        ledger = sqlb.lock_ledger(conn, group, {(t, p) for t, p, _, _ in msgs}, topic_ids)
        cur = conn.connection.driver_connection.cursor()
        events = []
        for t, p, o, v in msgs:
            if o < ledger.get((t, p), -1):
                outcome["skipped"] += 1
                continue
            ev = parse(v)
            if ev is None:
                # Kept with its coordinates and bytes, in the same transaction that
                # moves the ledger past it: it can never be both skipped and lost.
                outcome["malformed"] += 1
                log.warning("malformed mission event", extra={"partition": p, "offset": o})
                cur.execute(
                    "INSERT INTO downstream.mission_event_quarantine"
                    " (source_topic, source_partition, source_offset, payload_bytes, error)"
                    " VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (t, p, o, v, "unparseable or incomplete mission event"),
                )
                continue
            events.append(ev)
        for ev in events:  # few events per batch; keep per-row SQL simple and obvious
            cur.execute(
                """
                INSERT INTO downstream.mission_status AS m
                    (mission_id, robot_id, status, version, last_event_id, occurred_at)
                VALUES (%(mission_id)s, %(robot_id)s, %(status)s, %(version)s, %(event_id)s,
                        %(occurred_at)s)
                ON CONFLICT (mission_id) DO UPDATE SET
                    status = EXCLUDED.status, version = EXCLUDED.version,
                    last_event_id = EXCLUDED.last_event_id, occurred_at = EXCLUDED.occurred_at,
                    applied_at = now(), events_seen = m.events_seen + 1
                WHERE EXCLUDED.version > m.version
                """,
                ev,
            )
            if cur.rowcount == 1:
                outcome["applied"] += 1
                CDC_LATENCY.observe((datetime.now(UTC) - ev["occurred_at"]).total_seconds())
            else:
                outcome["ignored"] += 1  # duplicate or older version
        offsets: dict[tuple[str, int], int] = {}
        for t, p, o, _ in msgs:
            offsets[(t, p)] = max(offsets.get((t, p), 0), o + 1)
        sqlb.advance_ledger(conn, group, offsets, topic_ids)
    for k, n in outcome.items():
        EVENTS.labels(k).inc(n)
    return outcome


def run(settings: Settings, engine: Engine, stop: threading.Event, *, group: str = GROUP,
        topic: str | None = None, max_idle_s: float | None = None) -> dict[str, int]:  # fmt: skip
    topic = topic or settings.mission_events_topic
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": group,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})  # fmt: skip

    tid = kafka_offsets.topic_id(settings.kafka_bootstrap, topic)

    def on_assign(c, parts):
        with engine.connect() as conn:
            ledger = sqlb.read_ledger(conn, group, topic)
        kafka_offsets.resolve_start_offsets(c, parts, ledger, tid, group=group)
        c.assign(parts)

    consumer.subscribe([topic], on_assign=on_assign)
    totals = {"applied": 0, "ignored": 0, "malformed": 0, "skipped": 0}
    idle = time.monotonic()
    try:
        while not stop.is_set():
            batch = consumer.consume(num_messages=200, timeout=0.5)
            msgs = []
            for m in batch:
                if m.error():
                    if m.error().fatal():
                        raise KafkaException(m.error())
                    continue
                msgs.append((m.topic(), m.partition(), m.offset(), m.value()))
            if not msgs:
                if max_idle_s and time.monotonic() - idle > max_idle_s:
                    break
                continue
            idle = time.monotonic()
            for k, n in apply_events(engine, msgs, group, {topic: tid}).items():
                totals[k] += n
            try:  # advisory, as in the telemetry projector: never stops the loop
                consumer.commit(offsets=[TopicPartition(t, p, o + 1) for t, p, o, _ in msgs],
                                asynchronous=True)  # fmt: skip
            except KafkaException as exc:
                log.warning("advisory Kafka offset commit failed", extra={"error": str(exc)})
    finally:
        _close_bounded(consumer)
    return totals


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    faulthandler.enable()
    faulthandler.register(signal.SIGUSR1, all_threads=True)
    start_http_server(9102)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    log.info("mission consumer starting")
    try:
        run(settings, make_engine(settings), stop)
    except Exception:
        log.exception("mission consumer failed; exiting so the container restarts")
        sys.exit(1)


if __name__ == "__main__":
    main()
