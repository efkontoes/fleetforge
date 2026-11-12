"""Deterministic warehouse-fleet simulator with fault injection.

Determinism rules:
  * randomness comes from two independent streams derived from the seed:
      fleet    - identities, physics, reboots, which events get which fault;
      delivery - how late a reordered/duplicated event arrives.
    Keeping them separate means changing network conditions (e.g. max_delay_ticks)
    never changes the robots, boots or events themselves. With one shared RNG it
    did: a different delay range consumed different random bits and shifted every
    identifier generated afterwards.
  * time is a logical clock (start_time + tick / hz), never the wall clock.
Same config => byte-identical stream, including which faults were injected where.
"""

from __future__ import annotations

import heapq
import json
import random
import uuid
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

FIRMWARE = [("AMR-100", "1.0.0"), ("AMR-100", "1.1.0"), ("AMR-200", "2.0.0")]
REQUIRED_FIELDS = [
    "schema_version",
    "event_id",
    "robot_id",
    "boot_id",
    "sequence_number",
    "observed_at",
    "battery_pct",
    "x_m",
    "y_m",
]
WAREHOUSE_M = 50.0

Fault = Literal["duplicate", "reordered", "invalid_battery", "missing_field"]


@dataclass(frozen=True)
class FaultConfig:
    duplicate_rate: float = 0.0
    reorder_rate: float = 0.0
    invalid_battery_rate: float = 0.0
    missing_field_rate: float = 0.0
    reboot_rate: float = 0.0  # per robot per tick
    # How late a reordered/duplicated delivery may be, measured in the robot's OWN
    # observation periods. (Measured in raw stream positions, a delay shorter than the
    # fleet size would rarely overtake the same robot's next event, i.e. no real reorder.)
    max_delay_ticks: int = 3

    def __post_init__(self) -> None:
        total = (
            self.duplicate_rate
            + self.reorder_rate
            + self.invalid_battery_rate
            + self.missing_field_rate
        )
        if total > 1.0:
            raise ValueError("per-event fault rates must sum to <= 1")


@dataclass(frozen=True)
class SimConfig:
    seed: int
    robots: int
    ticks: int
    hz: float = 10.0
    start_time: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    faults: FaultConfig = field(default_factory=FaultConfig)


@dataclass(frozen=True)
class RegisterRobot:
    robot_id: str
    robot_model: str
    firmware_version: str
    kind: Literal["robot"] = "robot"


@dataclass(frozen=True)
class RegisterBoot:
    robot_id: str
    boot_id: str
    reboot: bool
    kind: Literal["boot"] = "boot"


@dataclass(frozen=True)
class Telemetry:
    robot_id: str
    payload: dict[str, Any]
    fault: Fault | None
    kind: Literal["telemetry"] = "telemetry"

    def body(self) -> bytes:
        return encode(self.payload)


Item = RegisterRobot | RegisterBoot | Telemetry


def encode(payload: dict[str, Any]) -> bytes:
    """Canonical wire bytes. Used by both the sender and the oracle."""
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()


def _uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


@dataclass
class _Robot:
    robot_id: str
    boot_id: str
    seq: int
    battery: float
    x: float
    y: float


def generate(cfg: SimConfig) -> list[Item]:
    # String seeds are hashed with SHA-512 by random.Random: stable across processes
    # and Python versions, and unaffected by PYTHONHASHSEED.
    fleet_rng = random.Random(f"fleetforge:{cfg.seed}:fleet")
    delivery_rng = random.Random(f"fleetforge:{cfg.seed}:delivery")
    max_delay_positions = max(1, cfg.faults.max_delay_ticks * cfg.robots)
    return list(
        _apply_delivery_faults(_in_order(cfg, fleet_rng), max_delay_positions, delivery_rng)
    )


def _in_order(cfg: SimConfig, rng: random.Random) -> Iterator[tuple[Item, str | None]]:
    """The fleet as it actually behaved, in observation order.
    Yields (item, pending_delivery_fault)."""
    f = cfg.faults
    fleet: list[_Robot] = []
    for _ in range(cfg.robots):
        model, firmware = FIRMWARE[rng.randrange(len(FIRMWARE))]
        r = _Robot(
            robot_id=_uuid(rng),
            boot_id=_uuid(rng),
            seq=0,
            battery=rng.uniform(40.0, 100.0),
            x=rng.uniform(0, WAREHOUSE_M),
            y=rng.uniform(0, WAREHOUSE_M),
        )
        fleet.append(r)
        yield RegisterRobot(r.robot_id, model, firmware), None
        yield RegisterBoot(r.robot_id, r.boot_id, reboot=False), None

    period = timedelta(seconds=1 / cfg.hz)
    for tick in range(cfg.ticks):
        observed_at = (cfg.start_time + tick * period).isoformat()
        for r in fleet:
            if rng.random() < f.reboot_rate:
                r.boot_id, r.seq = _uuid(rng), 0
                yield RegisterBoot(r.robot_id, r.boot_id, reboot=True), None

            # Physics: slow drain, recharge at the dock, bounded random walk.
            r.battery -= rng.uniform(0.0, 0.05)
            if r.battery < 15.0:
                r.battery = 100.0
            r.x = min(WAREHOUSE_M, max(0.0, r.x + rng.uniform(-0.2, 0.2)))
            r.y = min(WAREHOUSE_M, max(0.0, r.y + rng.uniform(-0.2, 0.2)))
            payload: dict[str, Any] = {
                "schema_version": 1,
                "event_id": _uuid(rng),
                "robot_id": r.robot_id,
                "boot_id": r.boot_id,
                "sequence_number": r.seq,
                "observed_at": observed_at,
                "battery_pct": round(r.battery, 3),
                "x_m": round(r.x, 3),
                "y_m": round(r.y, 3),
            }
            r.seq += 1

            # At most one fault per event, so ground-truth labels are unambiguous.
            roll = rng.random()
            fault: str | None = None
            for name, rate in (
                ("invalid_battery", f.invalid_battery_rate),
                ("missing_field", f.missing_field_rate),
                ("duplicate", f.duplicate_rate),
                ("reordered", f.reorder_rate),
            ):
                if roll < rate:
                    fault = name
                    break
                roll -= rate

            if fault == "invalid_battery":
                payload["battery_pct"] = rng.choice([150.0, -5.0, 100.5])
                yield Telemetry(r.robot_id, payload, "invalid_battery"), None
            elif fault == "missing_field":
                del payload[rng.choice(REQUIRED_FIELDS)]
                yield Telemetry(r.robot_id, payload, "missing_field"), None
            else:
                yield Telemetry(r.robot_id, payload, None), fault


def _apply_delivery_faults(
    items: Iterator[tuple[Item, str | None]], max_delay: int, rng: random.Random
) -> Iterator[Item]:
    """Network effects: delay some events (reorder) and redeliver others (duplicate).
    Events are only ever delayed, never sent early, so telemetry of a new boot cannot
    overtake that boot's registration."""
    pending: list[tuple[int, int, Item]] = []  # (release_position, tiebreak, item)
    tiebreak = 0
    for pos, (item, delivery_fault) in enumerate(items):
        while pending and pending[0][0] <= pos:
            yield heapq.heappop(pending)[2]
        delay = rng.randint(1, max_delay) if delivery_fault else 0
        if delivery_fault == "reordered":
            heapq.heappush(pending, (pos + delay, tiebreak, _relabel(item, "reordered")))
            tiebreak += 1
            continue
        yield item
        if delivery_fault == "duplicate":
            heapq.heappush(pending, (pos + delay, tiebreak, _relabel(item, "duplicate")))
            tiebreak += 1
    while pending:
        yield heapq.heappop(pending)[2]


def _relabel(item: Item, fault: Fault) -> Telemetry:
    assert isinstance(item, Telemetry)
    return Telemetry(item.robot_id, item.payload, fault)


# ------------------------------------------------------------------ (de)serialization


def to_jsonl(items: list[Item]) -> bytes:
    return b"".join(
        json.dumps(asdict(i), separators=(",", ":"), sort_keys=True).encode() + b"\n" for i in items
    )


def from_jsonl(data: bytes) -> list[Item]:
    out: list[Item] = []
    for line in data.splitlines():
        d = json.loads(line)
        kind = d.pop("kind")
        out.append(
            {"robot": RegisterRobot, "boot": RegisterBoot, "telemetry": Telemetry}[kind](**d)
        )
    return out
