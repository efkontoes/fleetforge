"""Telemetry projector process: Kafka consumer loop around BatchProcessor.

Acknowledgement order for every batch:
    1. poll records from Kafka
    2. PostgreSQL transaction: state + quarantine + offset ledger   <- the real commit
    3. Kafka offset commit (async, advisory: lag monitoring, faster restarts)
A crash between 2 and 3 causes redelivery; the ledger turns it into SKIPPED records.
A failure in 2 rolls everything back; nothing is acknowledged anywhere.
"""

from __future__ import annotations

import gc
import logging
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException, TopicPartition
from prometheus_client import start_http_server
from sqlalchemy import Engine

from fleetforge.config import Settings, get_settings
from fleetforge.db import batch as sqlb
from fleetforge.db.engine import make_engine
from fleetforge.domain.errors import ErrorCategory, category_of, is_recoverable
from fleetforge.kafka import offsets as kafka_offsets
from fleetforge.kafka.codec import TelemetryDecoder
from fleetforge.kafka.schemas import registry_client
from fleetforge.observability import configure_logging
from fleetforge.projector import metrics as m
from fleetforge.projector.processor import BatchProcessor, BatchResult, Outcome, SourceRecord

log = logging.getLogger("fleetforge.projector")


@dataclass
class RunStats:
    batches: int = 0
    records: int = 0
    retries: int = 0


class Projector:
    def __init__(
        self,
        settings: Settings,
        engine: Engine,
        *,
        group: str | None = None,
        topic: str | None = None,
        processor: BatchProcessor | None = None,
        fault_hook: Callable[[str], None] | None = None,
        consumer_conf: dict | None = None,
        seek_to_ledger: bool = True,
        topic_id: str | None = None,
    ):
        self.s = settings
        self.seek_to_ledger = seek_to_ledger  # False only in tests: trust Kafka's offsets
        self.engine = engine
        self.topic = topic or settings.telemetry_topic
        self.group = group or settings.projector_group
        self.fault = fault_hook or (lambda stage: None)
        self.processor = processor or BatchProcessor(
            engine,
            TelemetryDecoder(registry_client(settings.schema_registry_url), self.topic),
            mode=settings.projector_mode,
            consumer_group=self.group,
            fault_hook=fault_hook,
        )
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": self.group,
                "enable.auto.commit": False,  # offsets are committed after the DB, by us
                "auto.offset.reset": "earliest",
                "max.poll.interval.ms": 300_000,
                "session.timeout.ms": 30_000,
                "fetch.wait.max.ms": 100,
                "client.id": f"projector-{self.group}",
                **(consumer_conf or {}),
            }
        )
        self.stop = threading.Event()
        self.stats = RunStats()
        self._revoked = False
        self._last_lag = 0.0
        m.POOL_SIZE.set(settings.db_pool_size)
        # The incarnation of the topic this ledger is bound to (see kafka/offsets.py).
        self.topic_id = topic_id or kafka_offsets.topic_id(settings.kafka_bootstrap, self.topic)
        self.processor.topic_ids = {self.topic: self.topic_id}

    # ------------------------------------------------------------------ rebalance

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """The DB ledger, not Kafka's committed offset, decides where to resume.
        Raises OffsetLedgerError (out of consume(), stopping the process) when the
        ledger cannot be reconciled with the log."""
        ledger = {}
        if self.seek_to_ledger:
            with self.engine.connect() as conn:
                ledger = sqlb.read_ledger(conn, self.group, self.topic)
            kafka_offsets.resolve_start_offsets(consumer, partitions, ledger, self.topic_id,
                                                group=self.group)  # fmt: skip
        consumer.assign(partitions)
        log.info("partitions assigned", extra={"partitions": [p.partition for p in partitions],
                                               "from_ledger": sorted(ledger)})  # fmt: skip

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        self._revoked = True  # abandon any in-flight retry; the new owner resumes from ledger
        log.info("partitions revoked", extra={"partitions": [p.partition for p in partitions]})

    # ------------------------------------------------------------------ loop

    def run(self, max_idle_s: float | None = None) -> RunStats:
        # Everything allocated so far (modules, SQLAlchemy, schemas, clients) lives for
        # the whole process. Freezing it removes it from every future full GC scan:
        # measured gen-2 pauses 60-160 ms every ~2.5 s per projector before, < 1 ms
        # after (benchmarks/projector_gc.py). Frozen objects are still freed by
        # reference counting; they are only excluded from cycle detection.
        gc.collect()
        gc.freeze()
        self.consumer.subscribe([self.topic], on_assign=self._on_assign,
                                on_revoke=self._on_revoke)  # fmt: skip
        idle_since = time.monotonic()
        try:
            while not self.stop.is_set():
                msgs = self.consumer.consume(
                    num_messages=self.s.projector_batch_size,
                    timeout=self.s.projector_flush_interval_s,
                )
                records = []
                for msg in msgs:
                    if msg.error():
                        if msg.error().fatal():
                            raise KafkaException(msg.error())
                        log.warning("consumer error", extra={"error": msg.error().str()})
                        continue
                    _, ts = msg.timestamp()
                    records.append(SourceRecord(msg.topic(), msg.partition(), msg.offset(),
                                                msg.value(), ts))  # fmt: skip
                self._update_lag()
                if not records:
                    if max_idle_s is not None and time.monotonic() - idle_since > max_idle_s:
                        break
                    continue
                idle_since = time.monotonic()
                self._handle(records)
        finally:
            self.consumer.close()
        return self.stats

    def _handle(self, records: list[SourceRecord]) -> None:
        self._revoked = False
        t0 = time.perf_counter()
        result = self._process_with_retry(records)
        if result is None:
            return  # partitions moved during an outage; the new owner takes over
        m.BATCH_SECONDS.observe(time.perf_counter() - t0)
        self.fault("after_db_commit")
        self.consumer.commit(
            offsets=[TopicPartition(t, p, o) for (t, p), o in result.next_offsets.items()],
            asynchronous=True,
        )
        self._observe(records, result)

    def _process_with_retry(self, records: list[SourceRecord]) -> BatchResult | None:
        backoff = 0.1
        while True:
            try:
                result = self.processor.process(records)
                m.DB_AVAILABLE.set(1)
                return result
            except Exception as exc:
                if not is_recoverable(exc):
                    raise  # unknown failure: crash; restart resumes from the ledger
                m.DB_AVAILABLE.set(0)
                m.RETRIES.labels(type(exc).__name__).inc()
                self.stats.retries += 1
                log.warning("recoverable error; batch will be retried",
                            extra={"error": type(exc).__name__, "backoff_s": backoff})  # fmt: skip
                assignment = self.consumer.assignment()
                self.consumer.pause(assignment)  # backpressure: stop fetching, lag grows
                deadline = time.monotonic() + backoff
                while time.monotonic() < deadline and not self.stop.is_set():
                    self.consumer.poll(0.1)  # keep group membership alive, serve callbacks
                if self._revoked or self.stop.is_set():
                    return None
                self.consumer.resume(self.consumer.assignment())
                backoff = min(backoff * 2, self.s.projector_max_retry_backoff_s)

    def _observe(self, records: list[SourceRecord], result: BatchResult) -> None:
        now = time.time()
        m.BATCH_SIZE.observe(len(records))
        m.TRANSACTIONS.inc(result.transactions)
        for c in result.commit_seconds:
            m.COMMIT_SECONDS.observe(c)
        m.LAST_COMMIT.set(now)
        for r in result.results:
            reason = r.reason.value if r.reason else ""
            m.RECORDS.labels(r.outcome.value, reason).inc()
            if r.reason and category_of(r.reason) is ErrorCategory.PERMANENT_DATA:
                m.QUARANTINED.labels(reason).inc()
            if r.outcome is not Outcome.SKIPPED and r.record.timestamp_ms:
                m.EVENT_TO_STATE.observe(max(0.0, now - r.record.timestamp_ms / 1000))
        pool = self.engine.pool
        m.POOL_CHECKED_OUT.set(getattr(pool, "checkedout", lambda: 0)())
        self.stats.batches += 1
        self.stats.records += len(records)

    def _update_lag(self) -> None:
        if time.monotonic() - self._last_lag < 5:
            return
        self._last_lag = time.monotonic()
        try:
            for tp in self.consumer.assignment():
                pos = self.consumer.position([tp])[0].offset
                _, high = self.consumer.get_watermark_offsets(tp, timeout=1.0, cached=False)
                if pos >= 0:
                    m.LAG.labels(str(tp.partition)).set(max(0, high - pos))
        except KafkaException:
            pass


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    start_http_server(settings.projector_metrics_port)
    engine = make_engine(settings)
    projector = Projector(settings, engine)
    signal.signal(signal.SIGTERM, lambda *_: projector.stop.set())
    signal.signal(signal.SIGINT, lambda *_: projector.stop.set())
    log.info("projector starting", extra={"mode": settings.projector_mode,
                                          "batch_size": settings.projector_batch_size})  # fmt: skip
    stats = projector.run()
    log.info("projector stopped", extra=stats.__dict__)


if __name__ == "__main__":
    main()
