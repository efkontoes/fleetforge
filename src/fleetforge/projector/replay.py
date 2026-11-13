"""Controlled replay of telemetry.

    fleet-replay event --event-id <uuid> [--dry-run]
    fleet-replay quarantine [--reason UNKNOWN_ROBOT] [--limit 1000] [--dry-run]
    fleet-replay offsets --partition 3 --from 1200 --to 1300 [--dry-run]
    fleet-replay correct --event-id <uuid> --set battery_pct=55.0 [--dry-run]
    fleet-replay ledger-reset --group telemetry-projector --partition 3 --to 0 \
                 --reason "topic recreated after incident 42" [--topic ...] [--dry-run]

Guarantees:
  * Replays run through exactly the same BatchProcessor as live traffic, including
    the conditional upsert. A replayed event can never overwrite newer state: at
    worst it is STALE or DUPLICATE.
  * Replays bypass the consumer offset ledger (they are not part of the live stream)
    and are tagged source='replay' in the audit trail.
  * Quarantine status changes are written in the SAME transaction as the state
    change they describe (BatchProcessor.before_commit).
  * --dry-run executes everything, reports the outcomes, then rolls back.
  * Every run, dry or not, is recorded in fleet.replay_runs.

A correction is a NEW event: new event_id (deterministic uuid5 of the original),
`correction_of` = original event_id, same ordering key. It supersedes the rejected
original only if that original never changed state; if the robot has since moved
on, the correction is STALE like any other late event.
"""

from __future__ import annotations

import argparse
import getpass
import json
import sys
import uuid
from collections import Counter
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import Connection, Engine, text

from fleetforge.config import Settings, get_settings
from fleetforge.db.engine import make_engine
from fleetforge.kafka import offsets as kafka_offsets
from fleetforge.kafka.codec import TelemetryDecoder, TelemetryEncoder
from fleetforge.kafka.retry import retry_transient
from fleetforge.kafka.schemas import registry_client
from fleetforge.projector.processor import (
    BatchProcessor,
    BatchResult,
    Mode,
    Outcome,
    RecordResult,
    SourceRecord,
)

CORRECTION_NS = uuid.UUID("5a0c1d8e-5c4b-4b58-9a4e-0d3b0f5f7a11")
RESOLVING = {Outcome.APPLIED, Outcome.DUPLICATE, Outcome.STALE}


class Replayer:
    def __init__(self, settings: Settings, engine: Engine, *, operator: str | None = None,
                 registry=None):  # fmt: skip
        self.s = settings
        self.engine = engine
        self.operator = operator or getpass.getuser()
        self.registry = registry or registry_client(settings.schema_registry_url)
        self.decoder = TelemetryDecoder(self.registry, settings.telemetry_topic)

    # ------------------------------------------------------------------ selectors

    def by_event(self, event_id: str, dry_run: bool) -> dict[str, Any]:
        rows = self._quarantine_rows("event_id = :e", {"e": event_id})
        return self._replay_rows(rows, "EVENT", {"event_id": event_id}, dry_run)

    def quarantined(self, reason: str | None, limit: int, dry_run: bool) -> dict[str, Any]:
        where = "status = 'QUARANTINED' AND retry_eligible"
        params: dict[str, Any] = {}
        if reason:
            where += " AND reason_code = :r"
            params["r"] = reason
        rows = self._quarantine_rows(where, params, limit)
        return self._replay_rows(rows, "QUARANTINE", {"reason": reason, "limit": limit}, dry_run)

    def offsets(self, partition: int, first: int, last: int, dry_run: bool) -> dict[str, Any]:
        topic = self.s.telemetry_topic
        consumer = Consumer(
            {
                "bootstrap.servers": self.s.kafka_bootstrap,
                "group.id": f"replay-{uuid.uuid4()}",  # never commits; ledger untouched
                "enable.auto.commit": False,
            }
        )
        consumer.assign([TopicPartition(topic, partition, first)])
        records: list[SourceRecord] = []
        try:
            while len(records) < last - first + 1:
                msgs = consumer.consume(num_messages=500, timeout=5.0)
                if not msgs:
                    break
                for m in msgs:
                    if m.error() or m.offset() > last:
                        continue
                    records.append(SourceRecord(topic, partition, m.offset(), m.value(),
                                                m.timestamp()[1]))  # fmt: skip
        finally:
            consumer.close()
        selector = {"topic": topic, "partition": partition, "from": first, "to": last}
        return self._run(records, "OFFSET_RANGE", selector, dry_run, rejection_ids={})

    def correct(self, event_id: str, changes: dict[str, Any], dry_run: bool) -> dict[str, Any]:
        rows = self._quarantine_rows("event_id = :e AND payload_bytes IS NOT NULL", {"e": event_id})
        if not rows:
            raise SystemExit(f"no quarantined record with bytes for event {event_id}")
        original = self.decoder.decode(bytes(rows[0]["payload_bytes"]))
        corrected = dict(original)
        for k, v in changes.items():
            if k in ("x_m", "y_m"):
                corrected["position"] = {**corrected["position"], k: v}
            else:
                corrected[k] = v
        attempt = 1 + self._previous_corrections(event_id)
        corrected["event_id"] = str(uuid.uuid5(CORRECTION_NS, f"{event_id}:{attempt}"))
        corrected["correction_of"] = str(event_id)
        value = TelemetryEncoder(self.registry, self.s.telemetry_topic).encode(corrected)
        rec = SourceRecord(self.s.telemetry_topic, -1, -1, value)
        ids = {(rec.topic, rec.partition, rec.offset): [r["rejection_id"] for r in rows]}
        selector = {"event_id": event_id, "changes": changes,
                    "correction_event_id": corrected["event_id"]}  # fmt: skip
        return self._run([rec], "CORRECTION", selector, dry_run, rejection_ids=ids,
                         resolved_status="RESOLVED")  # fmt: skip

    def ledger_reset(self, group: str, topic: str, partition: int, to: int, reason: str,
                     dry_run: bool) -> dict[str, Any]:  # fmt: skip
        """Audited move of a consumer's ledger position (stop the consumer first)."""
        if not reason.strip():
            raise SystemExit("--reason is required")
        current_id = kafka_offsets.topic_id(self.s.kafka_bootstrap, topic)
        probe = Consumer({"bootstrap.servers": self.s.kafka_bootstrap,
                          "group.id": f"ledger-reset-{uuid.uuid4()}"})  # fmt: skip
        try:
            low, high = retry_transient(lambda: probe.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10, cached=False))  # fmt: skip
        finally:
            probe.close()
        if not low <= to <= high:
            raise SystemExit(f"--to {to} is outside the current log [{low}, {high}]")
        replay_id = uuid.uuid4()
        with self.engine.connect() as c:
            tx = c.begin()
            before = c.execute(text(
                "SELECT next_offset, kafka_topic_id FROM fleet.consumer_offsets WHERE"
                " consumer_group = :g AND topic = :t AND partition = :p FOR UPDATE"),
                {"g": group, "t": topic, "p": partition}).first()  # fmt: skip
            c.execute(text(
                "INSERT INTO fleet.consumer_offsets AS o (consumer_group, topic, partition,"
                " next_offset, kafka_topic_id) VALUES (:g, :t, :p, :to, :id)"
                " ON CONFLICT (consumer_group, topic, partition) DO UPDATE SET"
                " next_offset = :to, kafka_topic_id = :id, updated_at = now()"),
                {"g": group, "t": topic, "p": partition, "to": to, "id": current_id})  # fmt: skip
            if dry_run:
                tx.rollback()
            else:
                tx.commit()
        selector = {"group": group, "topic": topic, "partition": partition, "to": to,
                    "reason": reason, "log": [low, high], "topic_id": current_id,
                    "before": None if before is None else
                    {"next_offset": before[0], "topic_id": before[1]}}  # fmt: skip
        with self.engine.begin() as c:
            c.execute(text("INSERT INTO fleet.replay_runs (replay_id, mode, selector, dry_run,"
                           " operator, outcomes) VALUES (:id, 'LEDGER_RESET',"
                           " CAST(:s AS jsonb), :d, :o, '{}'::jsonb)"),
                      {"id": replay_id, "s": json.dumps(selector), "d": dry_run,
                       "o": self.operator})  # fmt: skip
        return {"replay_id": str(replay_id), "mode": "LEDGER_RESET", "dry_run": dry_run,
                **selector}  # fmt: skip

    # ------------------------------------------------------------------ core

    def _quarantine_rows(self, where: str, params: dict, limit: int = 10_000) -> list[dict]:
        with self.engine.connect() as c:
            return [dict(r) for r in c.execute(text(
                "SELECT rejection_id, event_id, reason_code, payload_bytes, source_topic,"
                " source_partition, source_offset FROM fleet.telemetry_rejections"
                f" WHERE {where} AND payload_bytes IS NOT NULL"
                " ORDER BY source_topic, source_partition, source_offset, rejection_id"
                " LIMIT :limit"), {**params, "limit": limit}).mappings()]  # fmt: skip

    def _previous_corrections(self, event_id: str) -> int:
        with self.engine.connect() as c:
            return c.execute(text("SELECT count(*) FROM fleet.replay_runs WHERE mode = 'CORRECTION'"
                                  " AND NOT dry_run AND selector->>'event_id' = :e"),
                             {"e": event_id}).scalar_one()  # fmt: skip

    def _replay_rows(self, rows, mode, selector, dry_run) -> dict[str, Any]:
        records, ids = [], {}
        for i, r in enumerate(rows):
            # Synthetic, unique coordinates: replays are not offsets of the live stream.
            rec = SourceRecord(r["source_topic"] or self.s.telemetry_topic, -1, i,
                               bytes(r["payload_bytes"]))  # fmt: skip
            records.append(rec)
            ids[(rec.topic, rec.partition, rec.offset)] = [r["rejection_id"]]
        return self._run(records, mode, selector, dry_run, rejection_ids=ids)

    def _run(self, records, mode, selector, dry_run, *, rejection_ids,
             resolved_status: str = "REPLAYED") -> dict[str, Any]:  # fmt: skip
        replay_id = uuid.uuid4()

        def mark(conn: Connection, results: list[RecordResult]) -> None:
            for r in results:
                key = (r.record.topic, r.record.partition, r.record.offset)
                if r.outcome in RESOLVING and key in rejection_ids:
                    conn.execute(text(
                        "UPDATE fleet.telemetry_rejections SET status = :st, updated_at = now(),"
                        " resolution = CAST(:res AS jsonb) WHERE rejection_id = ANY(:ids)"),
                        {"st": resolved_status, "ids": rejection_ids[key],
                         "res": json.dumps({"replay_id": str(replay_id),
                                            "outcome": r.outcome.value})})  # fmt: skip

        proc = BatchProcessor(self.engine, self.decoder, mode=Mode.SET, consumer_group=None,
                              source="replay", before_commit=mark, dry_run=dry_run)  # fmt: skip
        result = proc.process(records) if records else BatchResult()
        counts = Counter(f"{r.outcome.value}/{r.reason.value if r.reason else '-'}"
                         for r in result.results)  # fmt: skip
        summary = {"replay_id": str(replay_id), "mode": mode, "dry_run": dry_run,
                   "records": len(records), "outcomes": dict(sorted(counts.items()))}  # fmt: skip
        with self.engine.begin() as c:
            c.execute(text("INSERT INTO fleet.replay_runs (replay_id, mode, selector, dry_run,"
                           " operator, outcomes) VALUES (:id, :m, CAST(:s AS jsonb), :d, :o,"
                           " CAST(:out AS jsonb))"),
                      {"id": replay_id, "m": mode, "s": json.dumps(selector), "d": dry_run,
                       "o": self.operator, "out": json.dumps(summary["outcomes"])})  # fmt: skip
        return summary


def _parse_changes(pairs: list[str]) -> dict[str, Any]:
    out = {}
    for p in pairs:
        k, _, v = p.partition("=")
        try:
            out[k] = json.loads(v)
        except ValueError:
            out[k] = v
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fleet-replay", description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("event")
    e.add_argument("--event-id", required=True)
    q = sub.add_parser("quarantine")
    q.add_argument("--reason")
    q.add_argument("--limit", type=int, default=1000)
    o = sub.add_parser("offsets")
    o.add_argument("--partition", type=int, required=True)
    o.add_argument("--from", dest="first", type=int, required=True)
    o.add_argument("--to", dest="last", type=int, required=True)
    c = sub.add_parser("correct")
    c.add_argument("--event-id", required=True)
    c.add_argument("--set", dest="changes", action="append", required=True)
    lr = sub.add_parser("ledger-reset")
    lr.add_argument("--group", required=True)
    lr.add_argument("--topic")
    lr.add_argument("--partition", type=int, required=True)
    lr.add_argument("--to", type=int, required=True)
    lr.add_argument("--reason", required=True)
    for p in (e, q, o, c, lr):
        p.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    settings = get_settings()
    r = Replayer(settings, make_engine(settings))
    if a.cmd == "event":
        out = r.by_event(a.event_id, a.dry_run)
    elif a.cmd == "quarantine":
        out = r.quarantined(a.reason, a.limit, a.dry_run)
    elif a.cmd == "offsets":
        out = r.offsets(a.partition, a.first, a.last, a.dry_run)
    elif a.cmd == "ledger-reset":
        out = r.ledger_reset(a.group, a.topic or settings.telemetry_topic, a.partition, a.to,
                             a.reason, a.dry_run)  # fmt: skip
    else:
        out = r.correct(a.event_id, _parse_changes(a.changes), a.dry_run)
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
