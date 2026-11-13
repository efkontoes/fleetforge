"""Bounded retry for transient Kafka client errors on one-shot admin/metadata calls.

Found by the first CI run on GitHub's runners: a request issued by a brand-new client
right after start can fail with _DESTROY_BROKER ("Broker handle destroyed without
termination"). librdkafka 2.x retires the temporary bootstrap connection as soon as
metadata names the real broker, and a request in flight on it fails. The client does
not flag this error as retriable(), but the request is safe to repeat. Never used
for produce/consume paths, which have their own retry semantics.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from confluent_kafka import KafkaError, KafkaException

TRANSIENT = {
    KafkaError._DESTROY_BROKER,
    KafkaError._TRANSPORT,
    KafkaError._TIMED_OUT,
    KafkaError._ALL_BROKERS_DOWN,
    # Local: a fresh client's metadata cache does not know the partition yet (seen on
    # CI right after a PostgreSQL restart). A partition that really does not exist
    # still fails, after the bounded retries.
    KafkaError._UNKNOWN_PARTITION,
}


def is_transient(exc: BaseException) -> bool:
    err = exc.args[0] if isinstance(exc, KafkaException) and exc.args else None
    return isinstance(err, KafkaError) and (err.code() in TRANSIENT or err.retriable())


def retry_transient[T](fn: Callable[[], T], attempts: int = 6, backoff_s: float = 0.2) -> T:
    """Also retries a future that timed out (concurrent.futures.TimeoutError is the
    builtin TimeoutError): seen on CI when describe_topics ran right after a topic
    was deleted and recreated and the controller was briefly busy."""
    for i in range(attempts):
        try:
            return fn()
        except (KafkaException, TimeoutError) as exc:
            if i == attempts - 1 or not (isinstance(exc, TimeoutError) or is_transient(exc)):
                raise
            time.sleep(backoff_s * 2**i)
    raise AssertionError("unreachable")
