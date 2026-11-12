"""Telemetry projection: Kafka records -> fleet.robot_latest_state (+ audit, + ledger).

One call to `process(records)` = one unit of work. In every mode, the state changes,
the rejection audit rows AND the per-partition offset ledger are committed in the same
PostgreSQL transaction(s). Kafka's own offset commit happens afterwards, in the
consumer loop, and is only advisory: on restart the projector seeks to the ledger.

Modes (for the Milestone 2 benchmark; all produce identical results):
  per_event  one transaction per record (what a naive consumer does)
  loop       one transaction per batch, but one SQL statement per record
  set        one transaction per batch, set-based SQL: a constant number of round
             trips per batch regardless of its size
"""

from __future__ import annotations

import logging
import time
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import Connection, Engine

from fleetforge.db import batch as sqlb
from fleetforge.db import repository as repo
from fleetforge.domain.errors import PERMANENT_DB_ERRORS
from fleetforge.domain.ingest import Candidate, Observation, observation_from_record, plan_batch
from fleetforge.domain.telemetry import (
    Decision,
    OrderingKey,
    ReasonCode,
    Rejection,
    StoredPosition,
    check_semantics,
    classify,
    dedup_key,
    identity_hints,
    payload_for_audit,
)
from fleetforge.kafka.codec import DecodeError, json_safe

log = logging.getLogger(__name__)
CONSUMER_VERSION = "telemetry-projector/0.2.0"


class Mode(StrEnum):
    PER_EVENT = "per_event"
    LOOP = "loop"
    SET = "set"
    SET_COPY = "set_copy"  # set-based, rows staged with COPY instead of unnest()


class Outcome(StrEnum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    CONFLICT = "CONFLICT"
    REJECTED = "REJECTED"
    SKIPPED = "SKIPPED"  # already applied according to the offset ledger (redelivery)


@dataclass(frozen=True)
class SourceRecord:
    topic: str
    partition: int
    offset: int
    value: bytes | None
    timestamp_ms: int | None = None  # Kafka CreateTime (producer clock), transport metadata


@dataclass(frozen=True)
class RecordResult:
    record: SourceRecord
    outcome: Outcome
    reason: ReasonCode | None = None
    event_id: UUID | None = None


@dataclass
class BatchResult:
    results: list[RecordResult] = field(default_factory=list)
    next_offsets: dict[tuple[str, int], int] = field(default_factory=dict)
    transactions: int = 0
    commit_seconds: list[float] = field(default_factory=list)

    def counts(self) -> Counter:
        return Counter(
            (r.outcome.value, r.reason.value if r.reason else None) for r in self.results
        )


class Decoder(Protocol):
    def decode(self, value: bytes | None) -> dict[str, Any]: ...


class _DryRunRollback(Exception):
    def __init__(self, result: BatchResult):
        self.result = result


class PlanInvalidated(RuntimeError):
    """A concurrent writer changed a row the plan assumed; the batch is retried."""


# ---------------------------------------------------------------------------------- helpers


@dataclass
class _Prepared:
    """Result of the database-free stages for one record."""

    record: SourceRecord
    obs: Observation | None = None
    rejection: Rejection | None = None
    event_id: UUID | None = None
    robot_id: UUID | None = None
    payload: Any = None


class RegistryCache:
    """Positive-only cache of registry facts. Safe because they are immutable once
    written: a boot's number never changes, a robot's model never changes, firmware
    contracts are append-only. Unknown keys are never cached: they may be registered
    a moment later."""

    def __init__(self, max_entries: int = 200_000):
        self.boots: dict[tuple[UUID, UUID], int] = {}
        self.models: dict[UUID, str] = {}
        self.firmware: set[tuple[str, str]] = set()
        self.max_entries = max_entries

    def resolve(self, conn: Connection, obs_list: list[Observation]) -> None:
        missing_boots = list({(o.robot_id, o.boot_id) for o in obs_list} - self.boots.keys())
        if missing_boots:
            self.boots.update(sqlb.lookup_boots(conn, missing_boots))
        missing_robots = list({o.robot_id for o in obs_list} - self.models.keys())
        if missing_robots:
            self.models.update(sqlb.lookup_robots(conn, missing_robots))
        if any((self.models.get(o.robot_id), o.firmware_version) not in self.firmware
               for o in obs_list if o.robot_id in self.models):  # fmt: skip
            self.firmware = sqlb.load_firmware(conn)
        if len(self.boots) > self.max_entries:  # crude bound on memory; refills lazily
            self.boots.clear()

    def check(self, o: Observation) -> ReasonCode | None:
        model = self.models.get(o.robot_id)
        if model is None:
            return ReasonCode.UNKNOWN_ROBOT
        number = self.boots.get((o.robot_id, o.boot_id))
        if number is None:
            return ReasonCode.UNKNOWN_BOOT
        if number != o.boot_number:
            return ReasonCode.BOOT_NUMBER_MISMATCH
        if (model, o.firmware_version) not in self.firmware:
            return ReasonCode.UNKNOWN_FIRMWARE
        return None


def _rejection(code: ReasonCode, message: str, **extra: Any) -> Rejection:
    return Rejection(code, [{"code": code.value, "field": None, "message": message, **extra}])


# ---------------------------------------------------------------------------------- processor


class BatchProcessor:
    def __init__(
        self,
        engine: Engine,
        decoder: Decoder,
        *,
        mode: Mode | str = Mode.SET,
        consumer_group: str | None = "telemetry-projector",
        source: str = "kafka",
        max_clock_skew: timedelta = timedelta(minutes=5),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        fault_hook: Callable[[str], None] | None = None,
        before_commit: Callable[[Connection, list[RecordResult]], None] | None = None,
        dry_run: bool = False,
    ):
        self.engine = engine
        self.before_commit = before_commit  # extra writes inside the same transaction
        self.dry_run = dry_run  # run everything, then roll back
        self.decoder = decoder
        self.mode = Mode(mode)
        self.group = consumer_group  # None => no ledger (replay)
        self.source = source
        self.skew = max_clock_skew
        self.clock = clock
        self.fault = fault_hook or (lambda stage: None)
        self.cache = RegistryCache()

    # ------------------------------------------------------------------ public

    def process(self, records: list[SourceRecord]) -> BatchResult:
        if not records:
            return BatchResult()
        prepared = [self._prepare(r) for r in records]
        for attempt in range(3):
            try:
                if self.mode is Mode.PER_EVENT:
                    return self._per_event(prepared)
                return self._batched(prepared)
            except _DryRunRollback as dry:
                return dry.result
            except PlanInvalidated:
                log.warning("plan invalidated by a concurrent writer; retrying batch",
                            extra={"attempt": attempt})  # fmt: skip
            except PERMANENT_DB_ERRORS:
                # A record passed validation but the database refused it. Never let one
                # record block the partition: isolate it and process the rest.
                log.warning("batch refused by database; isolating records one by one")
                return self._isolate(prepared)
        raise RuntimeError("batch plan invalidated 3 times in a row")

    # ------------------------------------------------------------------ stage 1: no DB

    def _prepare(self, rec: SourceRecord) -> _Prepared:
        p = _Prepared(rec)
        try:
            decoded = self.decoder.decode(rec.value)
        except DecodeError as exc:
            p.payload = payload_for_audit(rec.value or b"")
            p.event_id, p.robot_id = identity_hints(p.payload)
            p.rejection = _rejection(ReasonCode.MALFORMED_PAYLOAD, str(exc))
            return p
        p.payload = json_safe(decoded)
        p.event_id, p.robot_id = identity_hints(p.payload)
        res = observation_from_record(decoded)
        if isinstance(res, Rejection):
            p.rejection = res
            return p
        p.event_id, p.robot_id = res.event_id, res.robot_id
        if (sem := check_semantics(res, self.clock(), self.skew)) is not None:
            p.rejection = sem
            return p
        p.obs = res
        return p

    def _reject_row(self, p: _Prepared, rej: Rejection) -> sqlb.RejectionRow:
        r = p.record
        return sqlb.RejectionRow(
            dedup_key=dedup_key(p.event_id, r.value or b""),
            event_id=p.event_id,
            robot_id=p.robot_id,
            reason=rej.reason_code,
            errors=rej.errors,
            payload=p.payload,
            payload_bytes=r.value,
            source=self.source,
            topic=r.topic if self.source == "kafka" else None,
            partition=r.partition if self.source == "kafka" else None,
            offset=r.offset if self.source == "kafka" else None,
        )

    # ------------------------------------------------------------------ batched modes

    def _batched(self, prepared: list[_Prepared]) -> BatchResult:
        result = BatchResult(transactions=1)
        now = self.clock()
        with self.engine.begin() as conn:
            todo = self._skip_already_applied(conn, prepared, result)
            valid = [p for p in todo if p.obs is not None]
            self.cache.resolve(conn, [p.obs for p in valid])
            rejections: list[sqlb.RejectionRow] = []
            outcome: dict[int, RecordResult] = {}
            candidates = []
            for i, p in enumerate(todo):
                if p.obs is not None and (code := self.cache.check(p.obs)) is not None:
                    p.rejection = _rejection(code, f"{code.value.lower().replace('_', ' ')}")
                    p.obs = None
                if p.rejection is not None:
                    rejections.append(self._reject_row(p, p.rejection))
                    outcome[i] = RecordResult(p.record, Outcome.REJECTED, p.rejection.reason_code,
                                              p.event_id)  # fmt: skip
                else:
                    candidates.append(Candidate(i, p.obs))

            if self.mode is Mode.LOOP:
                self._apply_loop(conn, todo, candidates, outcome, rejections, now)
            else:
                self._apply_set(conn, todo, candidates, outcome, rejections, now)

            sqlb.insert_rejections(conn, rejections, now, CONSUMER_VERSION)
            if self.group:
                sqlb.advance_ledger(conn, self.group, _next_offsets(todo))
            self.fault("before_commit")
            result.results.extend(outcome[i] for i in range(len(todo)))
            if self.before_commit:
                self.before_commit(conn, result.results)
            if self.dry_run:
                result.next_offsets = _next_offsets(prepared)
                raise _DryRunRollback(result)  # leaving the block rolls everything back
            t0 = time.perf_counter()
        result.commit_seconds.append(time.perf_counter() - t0)  # COMMIT ran on block exit
        result.next_offsets = _next_offsets(prepared)
        return result

    def _apply_set(self, conn, todo, candidates, outcome, rejections, now) -> None:
        robots = list({c.obs.robot_id for c in candidates})
        stored = sqlb.lock_states(conn, robots) if robots else {}
        plan = plan_batch(candidates, stored)
        rows = [sqlb.StateRow(w.obs, w.applied) for w in plan.winners.values()]
        upsert = sqlb.upsert_states_copy if self.mode is Mode.SET_COPY else sqlb.upsert_states
        written = upsert(conn, rows, now)
        if written != set(plan.winners):
            raise PlanInvalidated(f"{len(set(plan.winners) - written)} robots changed underneath")
        for pl in plan.decisions:
            p = todo[pl.index]
            outcome[pl.index] = self._ordering_outcome(p, pl.decision, pl.held, rejections)

    def _apply_loop(self, conn, todo, candidates, outcome, rejections, now) -> None:
        for c in candidates:
            p = todo[c.index]
            version = repo.apply_telemetry(conn, c.obs, c.obs.boot_number, now)
            if version is not None:
                outcome[c.index] = RecordResult(p.record, Outcome.APPLIED, None, c.obs.event_id)
                continue
            held = repo.get_stored_position(conn, c.obs.robot_id)
            key = OrderingKey(c.obs.boot_number, c.obs.sequence_number)
            decision = classify(key, c.obs.event_id, held)
            outcome[c.index] = self._ordering_outcome(p, decision, held, rejections)

    def _ordering_outcome(self, p: _Prepared, decision: Decision, held: StoredPosition | None,
                          rejections: list) -> RecordResult:  # fmt: skip
        eid = p.obs.event_id
        if decision is Decision.APPLY:
            return RecordResult(p.record, Outcome.APPLIED, None, eid)
        if decision is Decision.DUPLICATE:
            return RecordResult(p.record, Outcome.DUPLICATE, None, eid)
        code = (
            ReasonCode.STALE_SEQUENCE
            if decision is Decision.STALE
            else ReasonCode.SEQUENCE_CONFLICT
        )
        detail = {
            "held": {"boot_number": held.key.boot_number,
                     "sequence_number": held.key.sequence_number},
            "held_event_id": str(held.event_id),
            "incoming": {"boot_number": p.obs.boot_number,
                         "sequence_number": p.obs.sequence_number},
        }  # fmt: skip
        rej = _rejection(code, "not newer than the held state", **detail)
        rejections.append(self._reject_row(p, rej))
        out = Outcome.STALE if decision is Decision.STALE else Outcome.CONFLICT
        return RecordResult(p.record, out, code, eid)

    def _skip_already_applied(self, conn, prepared, result) -> list[_Prepared]:
        if not self.group:
            return prepared
        ledger = sqlb.lock_ledger(conn, self.group, {(p.record.topic, p.record.partition)
                                                      for p in prepared})  # fmt: skip
        todo = []
        for p in prepared:
            done = ledger.get((p.record.topic, p.record.partition), -1)
            if p.record.offset < done:
                result.results.append(RecordResult(p.record, Outcome.SKIPPED, None, p.event_id))
            else:
                todo.append(p)
        return todo

    # ------------------------------------------------------------------ per-event mode

    def _per_event(self, prepared: list[_Prepared]) -> BatchResult:
        result = BatchResult()
        for p in prepared:
            single = self._batched_one(p)
            result.results.extend(single.results)
            result.transactions += 1
            result.commit_seconds.extend(single.commit_seconds)
        result.next_offsets = _next_offsets(prepared)
        return result

    def _batched_one(self, p: _Prepared) -> BatchResult:
        mode, self.mode = self.mode, Mode.LOOP
        try:
            return self._batched([p])
        finally:
            self.mode = mode

    # ------------------------------------------------------------------ poison isolation

    def _isolate(self, prepared: list[_Prepared]) -> BatchResult:
        result = BatchResult()
        for p in prepared:
            try:
                single = self._batched_one(p)
            except PERMANENT_DB_ERRORS as exc:
                cause = type(getattr(exc, "orig", None) or exc).__name__
                rej = _rejection(ReasonCode.DB_CONSTRAINT_VIOLATION, cause)
                p.obs, p.rejection = None, rej
                now = self.clock()
                with self.engine.begin() as conn:
                    sqlb.insert_rejections(conn, [self._reject_row(p, rej)], now, CONSUMER_VERSION)
                    if self.group:
                        sqlb.advance_ledger(conn, self.group, _next_offsets([p]))
                single = BatchResult(
                    [RecordResult(p.record, Outcome.REJECTED, rej.reason_code, p.event_id)],
                    transactions=1,
                )
            result.results.extend(single.results)
            result.transactions += single.transactions
        result.next_offsets = _next_offsets(prepared)
        return result


def _next_offsets(prepared: list[_Prepared]) -> dict[tuple[str, int], int]:
    out: dict[tuple[str, int], int] = defaultdict(lambda: -1)
    for p in prepared:
        k = (p.record.topic, p.record.partition)
        out[k] = max(out[k], p.record.offset + 1)
    return dict(out)
