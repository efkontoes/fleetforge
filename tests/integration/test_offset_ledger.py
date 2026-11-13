"""Offset-ledger failure modes found in the Milestone 2 audit: topic recreation,
offset gaps (retention overtaking a consumer), legacy rows without a topic id, and
mission events that cannot be parsed. Every refusal must leave nothing committed."""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import text
from support import TOPIC, db_state, mock_codec, register_and_encode

from fleetforge.domain.errors import OffsetLedgerError, is_recoverable
from fleetforge.projector import missions
from fleetforge.projector.processor import BatchProcessor, Mode, SourceRecord
from fleetforge.simulator.fleet import SimConfig, generate


@pytest.fixture
def records(db):
    encoder, decoder = mock_codec()
    recs = register_and_encode(db, generate(SimConfig(seed=11, robots=1, ticks=30)), encoder)
    return recs, decoder


def ledger(db, group="telemetry-projector"):
    with db.connect() as c:
        return {(r.partition): (r.next_offset, r.kafka_topic_id) for r in c.execute(text(
            "SELECT partition, next_offset, kafka_topic_id FROM fleet.consumer_offsets"
            " WHERE consumer_group = :g"), {"g": group})}  # fmt: skip


def test_ledger_is_bound_to_the_topic_incarnation(db, records):
    recs, decoder = records
    old = BatchProcessor(db, decoder, mode=Mode.SET)
    old.topic_ids = {TOPIC: "topic-id-A"}
    old.process(recs[:10])
    part = recs[0].partition
    assert ledger(db)[part] == (10, "topic-id-A")
    state = db_state(db)

    # Same topic name, recreated: offsets restart at 0 under a new Kafka topic id.
    new = BatchProcessor(db, decoder, mode=Mode.SET)
    new.topic_ids = {TOPIC: "topic-id-B"}
    with pytest.raises(OffsetLedgerError, match="recreated"):
        new.process(recs[:5])
    assert not is_recoverable(OffsetLedgerError("x"))  # the consumer stops, never retries
    assert ledger(db)[part] == (10, "topic-id-A")
    assert db_state(db) == state


def test_legacy_row_without_topic_id_adopts_the_current_one(db, records):
    recs, decoder = records
    legacy = BatchProcessor(db, decoder, mode=Mode.SET)  # no topic id known (pre-0008 rows)
    legacy.process(recs[:5])
    part = recs[0].partition
    assert ledger(db)[part] == (5, None)
    p = BatchProcessor(db, decoder, mode=Mode.SET)
    p.topic_ids = {TOPIC: "topic-id-A"}
    p.process(recs[5:10])
    assert ledger(db)[part] == (10, "topic-id-A")


@pytest.mark.parametrize("mode", [Mode.SET, Mode.LOOP, Mode.PER_EVENT])
def test_offset_gap_stops_the_consumer_without_committing(db, records, mode):
    recs, decoder = records
    p = BatchProcessor(db, decoder, mode=mode)
    p.process(recs[:10])
    state = db_state(db)
    with pytest.raises(OffsetLedgerError, match=r"\[10, 15\)"):
        p.process(recs[15:20])  # 10..14 never delivered (retention overtook the consumer)
    assert ledger(db)[recs[0].partition][0] == 10
    assert db_state(db) == state
    p.process(recs[10:20])  # the contiguous continuation is fine
    assert ledger(db)[recs[0].partition][0] == 20


def test_first_batch_of_a_new_group_may_start_anywhere(db, records):
    """A new consumer group on a topic whose head was already deleted by retention
    legitimately starts at the earliest available offset: there is no history."""
    recs, decoder = records
    BatchProcessor(db, decoder, mode=Mode.SET).process(recs[12:20])
    assert ledger(db)[recs[0].partition][0] == 20


def _mission_event(version: int) -> bytes:
    return json.dumps({"event_id": str(uuid.uuid4()), "mission_id": MISSION, "robot_id": ROBOT,
                       "to_status": "ASSIGNED", "version": version,
                       "occurred_at": "2026-11-12T10:00:00+00:00"}).encode()  # fmt: skip


MISSION, ROBOT = str(uuid.uuid4()), str(uuid.uuid4())


def test_malformed_mission_event_is_quarantined_with_coordinates(db):
    t = "fleet.mission-events.v1"
    msgs = [(t, 0, 0, _mission_event(1)), (t, 0, 1, b"{not json"), (t, 0, 2, None),
            (t, 0, 3, _mission_event(2))]  # fmt: skip
    out = missions.apply_events(db, msgs, "mc-test", {t: "tid"})
    assert out == {"applied": 2, "ignored": 0, "malformed": 2, "skipped": 0}
    again = missions.apply_events(db, msgs, "mc-test", {t: "tid"})  # redelivery
    assert again["skipped"] == 4
    with db.connect() as c:
        rows = c.execute(text("SELECT source_partition, source_offset, payload_bytes"
                              " FROM downstream.mission_event_quarantine"
                              " ORDER BY source_offset")).all()  # fmt: skip
        version = c.execute(text("SELECT version FROM downstream.mission_status")).scalar_one()
    assert [(r[0], r[1], r[2] and bytes(r[2])) for r in rows] == [(0, 1, b"{not json"),
                                                                  (0, 2, None)]  # fmt: skip
    assert version == 2
    assert ledger(db, "mc-test")[0] == (4, "tid")


def test_records_without_a_partition_are_never_gap_checked(db, records):
    """Replays carry synthetic coordinates and no consumer group: no ledger at all."""
    recs, decoder = records
    synthetic = [SourceRecord(TOPIC, -1, 100 + i, r.value) for i, r in enumerate(recs[:5])]
    res = BatchProcessor(db, decoder, mode=Mode.SET, consumer_group=None).process(synthetic)
    assert len(res.results) == 5
    assert ledger(db) == {}
