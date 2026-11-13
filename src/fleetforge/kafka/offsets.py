"""Where to resume a partition: the durable ledger, checked against the Kafka log.

The offset ledger in PostgreSQL is the source of truth for "what has been applied".
It is only meaningful for the exact log it was written against. Before seeking to
it, a consumer verifies three things, and refuses to start otherwise:

  topic identity   the ledger row's Kafka topic UUID equals the current one
                   (a deleted-and-recreated topic keeps its name, not its UUID)
  not ahead        next_offset <= high watermark; otherwise the log was recreated or
                   truncated and seeking there would make librdkafka silently reset
                   to the earliest offset and skip everything below the old position
  no hole          next_offset >= low watermark; otherwise retention deleted records
                   that were never applied

Refusing is deliberate: each of these needs a human decision (replay from an
archive, accept the loss, or reset), recorded with `fleet-replay ledger-reset`.
"""

from __future__ import annotations

import logging

from confluent_kafka import Consumer, TopicCollection, TopicPartition
from confluent_kafka.admin import AdminClient

from fleetforge.db.batch import LedgerPosition
from fleetforge.domain.errors import OffsetLedgerError
from fleetforge.kafka.retry import retry_transient

log = logging.getLogger(__name__)


def topic_id(bootstrap: str, topic: str, timeout: float = 10.0) -> str:
    """Kafka's UUID for the current incarnation of `topic` (base64, as Kafka prints it)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    desc = retry_transient(
        lambda: admin.describe_topics(TopicCollection([topic]))[topic].result(timeout)
    )
    return str(desc.topic_id)


def resolve_start_offsets(
    consumer: Consumer,
    partitions: list[TopicPartition],
    ledger: dict[int, LedgerPosition],
    current_topic_id: str | None,
    *,
    group: str,
    timeout: float = 10.0,
) -> None:
    """Set tp.offset from the ledger for every assigned partition, or raise.

    Partitions without ledger history keep the consumer's default position
    (auto.offset.reset), which is correct for a brand-new consumer group.
    """
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
        # A committed row is honoured even at 0: normal batches seed and advance a row in
        # one transaction, so 0 only comes from `ledger-reset --to 0` (reprocess from the
        # start). Falling back to Kafka's advisory offset would silently skip that.
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
