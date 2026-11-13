"""Bounded retry for one-shot Kafka admin/metadata calls from fresh clients."""

from __future__ import annotations

import time
from collections.abc import Callable

from confluent_kafka import KafkaError, KafkaException

TRANSIENT = {
    KafkaError._DESTROY_BROKER,
    KafkaError._TRANSPORT,
    KafkaError._TIMED_OUT,
    KafkaError._ALL_BROKERS_DOWN,
    # a fresh client's metadata cache may not know the partition yet
    KafkaError._UNKNOWN_PARTITION,
}


def is_transient(exc: BaseException) -> bool:
    err = exc.args[0] if isinstance(exc, KafkaException) and exc.args else None
    return isinstance(err, KafkaError) and (err.code() in TRANSIENT or err.retriable())


def retry_transient[T](fn: Callable[[], T], attempts: int = 6, backoff_s: float = 0.2) -> T:
    """Retry transient Kafka errors and timed-out futures with exponential backoff."""
    for i in range(attempts):
        try:
            return fn()
        except (KafkaException, TimeoutError) as exc:
            if i == attempts - 1 or not (isinstance(exc, TimeoutError) or is_transient(exc)):
                raise
            time.sleep(backoff_s * 2**i)
    raise AssertionError("unreachable")
