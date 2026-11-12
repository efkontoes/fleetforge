"""In-memory model of what the platform *should* do with a simulated stream.

The oracle reuses the pure domain rules (parse, semantics, classify) but none of
the SQL. Comparing the real database against it checks that the conditional
upsert, constraints and transaction handling implement those rules exactly.

Valid only for streams delivered with per-robot order preserved (sequentially, or
partitioned by robot_id), which is what the sender guarantees.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

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
    parse_event,
    payload_for_audit,
)
from fleetforge.simulator.fleet import Item, RegisterBoot, RegisterRobot

_OUTCOME = {
    Decision.APPLY: "APPLIED",
    Decision.DUPLICATE: "DUPLICATE",
    Decision.STALE: "STALE",
    Decision.CONFLICT: "CONFLICT",
}


@dataclass
class Expectation:
    outcomes: Counter = field(default_factory=Counter)  # (outcome, reason_code | None)
    rejection_rows: set[tuple[str, str]] = field(default_factory=set)  # (dedup_key, reason)
    latest: dict[UUID, dict[str, Any]] = field(default_factory=dict)  # robot -> payload
    state_versions: Counter = field(default_factory=Counter)  # robot -> # applied


def expected(items: list[Item], received_at: datetime | None = None) -> Expectation:
    received_at = received_at or datetime.now(UTC)
    skew = timedelta(minutes=5)
    exp = Expectation()
    robots: set[UUID] = set()
    boots: dict[UUID, tuple[UUID, int]] = {}
    boot_count: Counter = Counter()
    held: dict[UUID, StoredPosition] = {}

    def reject(rej: Rejection, event_id, body, outcome="REJECTED"):
        exp.outcomes[(outcome, rej.reason_code.value)] += 1
        exp.rejection_rows.add((dedup_key(event_id, body), rej.reason_code.value))

    for item in items:
        if isinstance(item, RegisterRobot):
            robots.add(UUID(item.robot_id))
            continue
        if isinstance(item, RegisterBoot):
            rid = UUID(item.robot_id)
            boot_count[rid] += 1
            boots[UUID(item.boot_id)] = (rid, boot_count[rid])
            continue

        body = item.body()
        parsed = parse_event(body)
        if isinstance(parsed, Rejection):
            event_id, _ = identity_hints(payload_for_audit(body))
            reject(parsed, event_id, body)
            continue
        ev = parsed
        if (rej := check_semantics(ev, received_at, skew)) is not None:
            reject(rej, ev.event_id, body)
            continue
        if ev.robot_id not in robots:
            reject(Rejection(ReasonCode.UNKNOWN_ROBOT), ev.event_id, body)
            continue
        boot = boots.get(ev.boot_id)
        if boot is None or boot[0] != ev.robot_id:
            reject(Rejection(ReasonCode.UNKNOWN_BOOT), ev.event_id, body)
            continue

        key = OrderingKey(boot[1], ev.sequence_number)
        decision = classify(key, ev.event_id, held.get(ev.robot_id))
        if decision is Decision.APPLY:
            held[ev.robot_id] = StoredPosition(key, ev.event_id)
            exp.latest[ev.robot_id] = item.payload
            exp.state_versions[ev.robot_id] += 1
            exp.outcomes[("APPLIED", None)] += 1
        elif decision is Decision.DUPLICATE:
            exp.outcomes[("DUPLICATE", None)] += 1
        else:
            code = "STALE_SEQUENCE" if decision is Decision.STALE else "SEQUENCE_CONFLICT"
            reject(Rejection(ReasonCode(code)), ev.event_id, body, _OUTCOME[decision])
    return exp
