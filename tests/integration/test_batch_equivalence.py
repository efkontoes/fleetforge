"""Differential test: set-based batch projection == applying events one at a time.

The reference is per-event mode with batch size 1, i.e. the Milestone 1 rule (one
conditional upsert per event, in partition order). The subject is set mode with
random batch boundaries, two consumer instances alternating (a rebalance hands the
partitions to another process with a cold registry cache) and redelivered tails
(a consumer that lost the partition re-processes records already applied).

The stream is deliberately hostile: duplicates, reorders, invalid values, missing
fields, reboots, plus injected same-sequence events with a different identity and
payload (SEQUENCE_CONFLICT), in many cases inside the same batch as the event they
conflict with. Compared: final state including state_version, the rejection audit,
and the outcome of every source record.
"""

from __future__ import annotations

import random
import uuid

import pytest
from sqlalchemy import text
from support import db_rejection_rows, db_state, interleave, mock_codec, register_and_encode

from fleetforge.projector.processor import BatchProcessor, Mode, Outcome
from fleetforge.simulator.fleet import FaultConfig, SimConfig, Telemetry, generate


def hostile_stream(seed: int):
    cfg = SimConfig(seed=seed, robots=8, ticks=80, faults=FaultConfig(
        duplicate_rate=0.10, reorder_rate=0.15, invalid_battery_rate=0.04,
        missing_field_rate=0.03, reboot_rate=0.03))  # fmt: skip
    items = generate(cfg)
    rng = random.Random(seed)
    out = list(items)
    telemetry = [i for i, it in enumerate(items)
                 if isinstance(it, Telemetry) and it.fault is None]  # fmt: skip
    for idx in rng.sample(telemetry, len(telemetry) // 12):
        orig = items[idx]
        twin_id = str(uuid.UUID(int=rng.getrandbits(128)))
        twin = Telemetry(orig.robot_id, {**orig.payload, "event_id": twin_id, "battery_pct": 1.0},
                         None)  # fmt: skip
        # Usually within a few positions (same batch), sometimes much later.
        pos = min(len(out), out.index(orig) + 1 + rng.choice([0, 1, 3, 40, 200]))
        out.insert(pos, twin)
    return out


def outcomes(results):
    return {(r.record.partition, r.record.offset): (r.outcome, r.reason) for r in results}


def reset_projection(db):
    with db.begin() as c:
        c.execute(text("TRUNCATE fleet.robot_latest_state, fleet.telemetry_rejections,"
                       " fleet.consumer_offsets"))  # fmt: skip


@pytest.mark.parametrize("seed", [7, 1234, 98765])
def test_random_batches_and_consumer_switches_equal_sequential_application(db, seed):
    encoder, decoder = mock_codec()
    records = interleave(register_and_encode(db, hostile_stream(seed), encoder))

    reference = BatchProcessor(db, decoder, mode=Mode.PER_EVENT)
    ref = {}
    for r in records:
        ref.update(outcomes(reference.process([r]).results))
    ref_state, ref_rejections = db_state(db), db_rejection_rows(db)
    assert {o for o, _ in ref.values()} >= {Outcome.APPLIED, Outcome.DUPLICATE, Outcome.STALE,
                                            Outcome.CONFLICT, Outcome.REJECTED}  # fmt: skip

    reset_projection(db)
    rng = random.Random(seed)
    consumers = [BatchProcessor(db, decoder, mode=Mode.SET) for _ in range(2)]
    got, i, active, redelivered = {}, 0, 0, 0
    while i < len(records):
        size = rng.choice([1, 2, 5, 17, 64, 250])
        batch = records[i : i + size]
        if rng.random() < 0.25:
            active = 1 - active  # rebalance: the other process owns the partitions now
            if i and rng.random() < 0.5:
                tail = records[max(0, i - rng.randint(1, 40)) : i]
                res = consumers[active].process(tail)
                assert all(r.outcome is Outcome.SKIPPED for r in res.results)
                redelivered += len(tail)
        for r in consumers[active].process(batch).results:
            assert r.outcome is not Outcome.SKIPPED
            got[(r.record.partition, r.record.offset)] = (r.outcome, r.reason)
        i += size

    assert got == ref
    assert db_state(db) == ref_state  # includes state_version and the winning event_id
    assert db_rejection_rows(db) == ref_rejections
    assert redelivered > 0
