"""The batch processor against real PostgreSQL (no broker): correctness of every mode
and batch size against the independent oracle, offset-ledger idempotency, crash
windows, poison isolation and concurrent (zombie) consumers."""

from __future__ import annotations

import threading
from collections import Counter

import pytest
from sqlalchemy import text
from support import (
    assert_state_matches_oracle,
    batches,
    db_rejection_rows,
    db_state,
    interleave,
    mock_codec,
    register_and_encode,
)

from fleetforge.domain.telemetry import ReasonCode
from fleetforge.projector import processor as proc_mod
from fleetforge.projector.processor import BatchProcessor, Mode, Outcome
from fleetforge.simulator.fleet import FaultConfig, SimConfig, generate
from fleetforge.simulator.oracle import expected

CFG = SimConfig(
    seed=4242,
    robots=12,
    ticks=60,
    faults=FaultConfig(
        duplicate_rate=0.08,
        reorder_rate=0.08,
        invalid_battery_rate=0.03,
        missing_field_rate=0.03,
        reboot_rate=0.02,
    ),
)


@pytest.fixture
def stream(db):
    items = generate(CFG)
    encoder, decoder = mock_codec()
    records = interleave(register_and_encode(db, items, encoder))
    return items, records, decoder


def run(db, decoder, records, mode, size, **kw):
    p = BatchProcessor(db, decoder, mode=mode, **kw)
    results = []
    for b in batches(records, size):
        results.extend(p.process(b).results)
    return p, results


def identities(results):
    return Counter(
        (
            str(r.event_id) if r.event_id else None,
            r.outcome.value,
            r.reason.value if r.reason else None,
        )
        for r in results
    )


@pytest.mark.parametrize(
    ("mode", "size"),
    [
        (Mode.PER_EVENT, 1),
        (Mode.LOOP, 50),
        (Mode.SET, 1),
        (Mode.SET, 7),
        (Mode.SET, 100),
        (Mode.SET, 500),
        (Mode.SET_COPY, 250),
    ],
)
def test_every_mode_and_batch_size_matches_oracle(db, stream, mode, size):
    items, records, decoder = stream
    exp = expected(items, transport="kafka")
    _, results = run(db, decoder, records, mode, size)

    assert {"APPLIED", "DUPLICATE", "STALE", "REJECTED"} <= {o for o, _ in exp.outcomes}
    got = Counter((r.outcome.value, r.reason.value if r.reason else None) for r in results)
    assert got == exp.outcomes
    assert_state_matches_oracle(db_state(db), exp)
    assert db_rejection_rows(db) == exp.rejection_rows
    # Accepted and rejected identities, per delivery. Records with no readable
    # event_id are identified by a hash of their bytes in the oracle: "no-id" here.
    want = Counter()
    for (k, o, r), n in exp.event_outcomes.items():
        want[(k if not k.startswith("sha256:") else "no-id", o, r)] += n
    got_ids = Counter()
    for (e, o, r), n in identities(results).items():
        got_ids[(e or "no-id", o, r)] += n
    assert got_ids == want


def test_quarantine_rows_carry_kafka_coordinates_and_taxonomy(db, stream):
    _, records, decoder = stream
    run(db, decoder, records, Mode.SET, 100)
    with db.connect() as c:
        rows = c.execute(text("""
            SELECT reason_code, error_category, retry_eligible, status, source,
                   source_topic, source_partition, source_offset, consumer_version,
                   payload_bytes IS NOT NULL AS has_bytes
            FROM fleet.telemetry_rejections
        """)).mappings().all()  # fmt: skip
    assert rows
    for r in rows:
        assert r["source"] == "kafka" and r["source_topic"] == "telemetry.raw.v1"
        assert r["source_partition"] is not None and r["source_offset"] is not None
        assert r["consumer_version"].startswith("telemetry-projector/")
        assert r["has_bytes"]
        if r["reason_code"] == "STALE_SEQUENCE":
            assert (r["error_category"], r["status"]) == ("ORDERING", "RECORDED")
        else:
            assert (r["error_category"], r["status"]) == ("PERMANENT_DATA", "QUARANTINED")
    with db.connect() as c:
        view = c.execute(text("SELECT count(*) FROM fleet.telemetry_quarantine")).scalar()
    assert view == sum(r["error_category"] == "PERMANENT_DATA" for r in rows)


def test_redelivery_after_db_commit_is_skipped_by_the_ledger(db, stream):
    """Crash window: DB committed, Kafka offset not committed -> same records again."""
    items, records, decoder = stream
    p, first = run(db, decoder, records, Mode.SET, 100)
    state, rejections = db_state(db), db_rejection_rows(db)

    again = []
    for b in batches(records, 100):
        again.extend(p.process(b).results)

    assert {r.outcome for r in again} == {Outcome.SKIPPED}
    assert db_state(db) == state
    assert db_rejection_rows(db) == rejections


def test_redelivery_without_ledger_is_still_logically_idempotent(db, stream):
    """Defense in depth: even with the ledger bypassed (as a replay does), re-applying
    the same records cannot move state backwards or bump versions."""
    items, records, decoder = stream
    run(db, decoder, records, Mode.SET, 100)
    state = db_state(db)
    _, again = run(db, decoder, records, Mode.SET, 100, consumer_group=None)
    assert Outcome.APPLIED not in {r.outcome for r in again}
    assert db_state(db) == state


def test_crash_during_db_batch_leaves_no_trace(db, stream):
    items, records, decoder = stream
    batch = records[:200]

    def crash(stage):
        if stage == "before_commit":
            raise RuntimeError("simulated crash inside the transaction")

    p = BatchProcessor(db, decoder, mode=Mode.SET, fault_hook=crash)
    with pytest.raises(RuntimeError):
        p.process(batch)
    assert db_state(db) == {}
    assert db_rejection_rows(db) == set()
    with db.connect() as c:
        assert c.execute(text("SELECT count(*) FROM fleet.consumer_offsets")).scalar() == 0

    # The restarted consumer processes the same records from scratch, correctly.
    exp = expected(items, transport="kafka")
    run(db, decoder, records, Mode.SET, 100)
    assert_state_matches_oracle(db_state(db), exp)


@pytest.mark.parametrize("mode", [Mode.SET, Mode.LOOP])
def test_partial_batch_failure_isolates_the_poison_record(db, stream, monkeypatch, mode):
    """Validation is sabotaged so invalid batteries reach SQL. Whenever the database
    refuses a batch, it falls back to per-record processing: the refused record is
    quarantined as DB_CONSTRAINT_VIOLATION and every other record still progresses.

    Lesson recorded in the learning notes: in set mode only each robot's *winning*
    event is written, so a bad event superseded later in the same batch never meets
    the CHECK constraint. Constraints protect what is stored; validation in the
    application is what protects every event. It cannot be skipped when batching."""
    items, records, decoder = stream
    monkeypatch.setattr(proc_mod, "check_semantics", lambda *a, **k: None)  # sabotage

    _, results = run(db, decoder, records, mode, 100)

    got = Counter((r.outcome.value, r.reason.value if r.reason else None) for r in results)
    assert got[("REJECTED", "DB_CONSTRAINT_VIOLATION")] >= 1
    assert len(results) == len(records)  # nothing stuck behind the poison record
    with db.connect() as c:
        bad = c.execute(text(
            "SELECT count(*) FROM fleet.robot_latest_state WHERE battery_pct NOT BETWEEN 0 AND 100"
        )).scalar()  # fmt: skip
        quarantined = c.execute(text(
            "SELECT count(*) FROM fleet.telemetry_quarantine"
            " WHERE reason_code = 'DB_CONSTRAINT_VIOLATION' AND source_offset IS NOT NULL"
        )).scalar()  # fmt: skip
    assert bad == 0
    assert quarantined == got[("REJECTED", "DB_CONSTRAINT_VIOLATION")]


def test_two_consumers_racing_on_the_same_batch_apply_it_once(db, stream):
    """A 'zombie' still processing a partition after a rebalance: both run the same
    batch concurrently. The ledger row lock serializes them; the second skips."""
    items, records, decoder = stream
    exp = expected(items, transport="kafka")
    a = BatchProcessor(db, decoder, mode=Mode.SET)
    b = BatchProcessor(db, decoder, mode=Mode.SET)
    barrier = threading.Barrier(2)
    out: dict[str, list] = {"a": [], "b": []}

    def work(name, p):
        for batch in batches(records, 100):
            barrier.wait()
            out[name].extend(p.process(batch).results)

    threads = [threading.Thread(target=work, args=(n, p)) for n, p in (("a", a), ("b", b))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)

    skipped = sum(r.outcome is Outcome.SKIPPED for r in out["a"] + out["b"])
    assert skipped == len(records)  # every record skipped by exactly one of the two
    assert_state_matches_oracle(db_state(db), exp)


def test_unknown_boot_is_retry_eligible_quarantine(db):
    import json
    import uuid

    from fleetforge.kafka.codec import avro_record_from_payload
    from fleetforge.projector.processor import SourceRecord

    encoder, decoder = mock_codec("unknown-boot")
    payload = {"schema_version": 1, "event_id": str(uuid.uuid4()), "robot_id": str(uuid.uuid4()),
               "boot_id": str(uuid.uuid4()), "sequence_number": 1,
               "observed_at": "2026-01-01T00:00:00+00:00", "battery_pct": 50.0,
               "x_m": 0.0, "y_m": 0.0}  # fmt: skip
    value = encoder.encode(avro_record_from_payload(payload, boot_number=1,
                           firmware_version="1.0.0", producer_id="t"))  # fmt: skip
    r = BatchProcessor(db, decoder).process([SourceRecord("telemetry.raw.v1", 0, 0, value)])
    assert r.results[0].reason is ReasonCode.UNKNOWN_ROBOT
    with db.connect() as c:
        row = c.execute(
            text("SELECT retry_eligible, status, payload FROM fleet.telemetry_rejections")
        ).one()
    assert row.retry_eligible and row.status == "QUARANTINED"
    assert row.payload["event_id"] == payload["event_id"]
    assert json.dumps(row.payload)  # stored as JSON, not opaque bytes only
