"""Resume positions from the PostgreSQL ledger, checked against the Kafka log.

A ledger row must match the topic's UUID and lie within [low, high] watermarks;
otherwise the consumer stops (OffsetLedgerError) until `fleet-replay ledger-reset`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from confluent_kafka import Consumer, TopicCollection, TopicPartition
from confluent_kafka.admin import AdminClient
from sqlalchemy import Engine

from fleetforge.db import batch as sqlb
from fleetforge.db.batch import LedgerPosition
from fleetforge.domain.errors import OffsetLedgerError, is_recoverable
from fleetforge.kafka.retry import retry_transient

log = logging.getLogger(__name__)


def topic_id(bootstrap: str, topic: str, timeout: float = 10.0) -> str:
    """Kafka's UUID for the current incarnation of `topic` (base64, as Kafka prints it)."""

    def once():  # a fresh client per attempt: one stuck connection cannot poison retries
        admin = AdminClient({"bootstrap.servers": bootstrap})
        futures = admin.describe_topics(TopicCollection([topic]), request_timeout=timeout)
        return futures[topic].result(timeout + 5)

    return str(retry_transient(once).topic_id)


def read_ledger_with_retry(
    engine: Engine,
    group: str,
    topic: str,
    budget_s: float = 120.0,
    on_retry: Callable[[BaseException], None] | None = None,
) -> dict[int, LedgerPosition]:
    """Read the ledger during partition assignment, retrying recoverable failures."""
    deadline = time.monotonic() + budget_s
    backoff = 0.2
    while True:
        try:
            with engine.connect() as conn:
                return sqlb.read_ledger(conn, group, topic)
        except Exception as exc:
            if not is_recoverable(exc) or time.monotonic() + backoff > deadline:
                raise
            if on_retry:
                on_retry(exc)
            log.warning("offset ledger unavailable during assignment; retrying",
                        extra={"error": type(exc).__name__, "backoff_s": backoff})  # fmt: skip
            time.sleep(backoff)
            backoff = min(backoff * 2, 5.0)


def resolve_start_offsets(
    consumer: Consumer,
    partitions: list[TopicPartition],
    ledger: dict[int, LedgerPosition],
    current_topic_id: str | None,
    *,
    group: str,
    timeout: float = 10.0,
) -> None:
    """Set tp.offset from the ledger; partitions without a row keep auto.offset.reset."""
    for tp in partitions:
        pos = ledger.get(tp.partition)
        if pos is None:
            continue
        where = f"{group}/{tp.topic}/{tp.partition}"
        if current_topic_id and pos.kafka_topic_id and pos.kafka_topic_id != current_topic_id:
            raise OffsetLedgerError(
                f"{where}: ledger written for topic id {pos.kafka_topic_id}, topic is now"
                f" {current_topic_id} (deleted and recreated)"
            )
        # a committed 0 comes only from `ledger-reset --to 0` and is honoured
        low, high = retry_transient(
            lambda tp=tp: consumer.get_watermark_offsets(tp, timeout=timeout, cached=False)
        )
        if pos.next_offset > high:
            raise OffsetLedgerError(
                f"{where}: ledger next_offset {pos.next_offset} is beyond the end of the log"
                f" ({high}); the topic was recreated or truncated"
            )
        if pos.next_offset < low:
            raise OffsetLedgerError(
                f"{where}: records [{pos.next_offset}, {low}) were deleted by retention"
                " before being applied"
            )
        tp.offset = pos.next_offset
