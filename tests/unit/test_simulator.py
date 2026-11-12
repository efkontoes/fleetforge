"""Acceptance scenario 7: same seed => same logical events and same injected failures."""

import hashlib
from collections import Counter

from fleetforge.simulator.fleet import (
    FaultConfig,
    RegisterBoot,
    SimConfig,
    Telemetry,
    from_jsonl,
    generate,
    to_jsonl,
)

FAULTS = FaultConfig(
    duplicate_rate=0.05,
    reorder_rate=0.05,
    invalid_battery_rate=0.02,
    missing_field_rate=0.02,
    reboot_rate=0.01,
)


def digest(cfg: SimConfig) -> str:
    return hashlib.sha256(to_jsonl(generate(cfg))).hexdigest()


def test_same_seed_produces_byte_identical_stream_including_faults():
    cfg = SimConfig(seed=42, robots=25, ticks=80, faults=FAULTS)
    a, b = generate(cfg), generate(cfg)
    assert to_jsonl(a) == to_jsonl(b)
    assert [getattr(i, "fault", None) for i in a] == [getattr(i, "fault", None) for i in b]


def test_different_seed_produces_different_stream():
    base = SimConfig(seed=42, robots=5, ticks=20, faults=FAULTS)
    other = SimConfig(seed=43, robots=5, ticks=20, faults=FAULTS)
    assert digest(base) != digest(other)


def test_jsonl_round_trip_is_lossless():
    items = generate(SimConfig(seed=1, robots=3, ticks=30, faults=FAULTS))
    assert from_jsonl(to_jsonl(items)) == items


def test_all_fault_kinds_are_injected_at_roughly_configured_rates():
    items = generate(SimConfig(seed=7, robots=50, ticks=200, faults=FAULTS))
    telemetry = [i for i in items if isinstance(i, Telemetry)]
    faults = Counter(i.fault for i in telemetry)
    observations = 50 * 200
    for kind, rate in [
        ("duplicate", 0.05),
        ("reordered", 0.05),
        ("invalid_battery", 0.02),
        ("missing_field", 0.02),
    ]:
        assert abs(faults[kind] / observations - rate) < rate * 0.3, (kind, faults[kind])
    # Duplicates add deliveries; nothing is lost.
    assert len(telemetry) == observations + faults["duplicate"]
    assert sum(isinstance(i, RegisterBoot) and i.reboot for i in items) > 0


def test_no_faults_means_clean_in_order_stream():
    items = generate(SimConfig(seed=3, robots=4, ticks=50))
    telemetry = [i for i in items if isinstance(i, Telemetry)]
    assert all(i.fault is None for i in telemetry)
    for robot in {i.robot_id for i in telemetry}:
        seqs = [i.payload["sequence_number"] for i in telemetry if i.robot_id == robot]
        assert seqs == list(range(50))


def test_new_boot_telemetry_never_precedes_its_registration():
    items = generate(
        SimConfig(
            seed=11, robots=10, ticks=100, faults=FaultConfig(reorder_rate=0.3, reboot_rate=0.05)
        )
    )
    registered: set[str] = set()
    for i in items:
        if isinstance(i, RegisterBoot):
            registered.add(i.boot_id)
        elif isinstance(i, Telemetry) and "boot_id" in i.payload:
            assert i.payload["boot_id"] in registered


def test_reorder_fault_actually_reorders_a_robots_own_stream():
    """Regression: with delays in raw stream positions, a 20-robot fleet saw ~no
    per-robot reordering. Delays are now in the robot's own ticks."""
    items = generate(
        SimConfig(seed=42, robots=20, ticks=100, faults=FaultConfig(reorder_rate=0.05))
    )
    last: dict[str, int] = {}
    overtaken = 0
    for i in items:
        if isinstance(i, Telemetry):
            seq = i.payload["sequence_number"]
            if seq < last.get(i.robot_id, -1):
                overtaken += 1
            last[i.robot_id] = max(seq, last.get(i.robot_id, -1))
    assert overtaken > 30  # most of the ~100 reordered events arrive after a newer one


def test_delivery_conditions_do_not_change_the_logical_events():
    """Regression: changing only the delay range used to shift every generated ID."""

    def logical(max_delay_ticks):
        faults = FaultConfig(
            duplicate_rate=0.05, reorder_rate=0.05, max_delay_ticks=max_delay_ticks
        )
        items = generate(SimConfig(seed=42, robots=10, ticks=50, faults=faults))
        return sorted(
            (i.payload["event_id"], i.payload["boot_id"]) for i in items if isinstance(i, Telemetry)
        )

    assert logical(1) == logical(3) == logical(10)
