"""Controlled replay against real PostgreSQL (registry mocked, no broker needed)."""

from __future__ import annotations

import uuid
from uuid import UUID

import pytest
from sqlalchemy import text
from support import TOPIC, mock_codec

from fleetforge.config import Settings
from fleetforge.contracts.api import RobotRegistration
from fleetforge.kafka.codec import avro_record_from_payload
from fleetforge.projector.processor import BatchProcessor, SourceRecord
from fleetforge.projector.replay import Replayer
from fleetforge.services import robots


@pytest.fixture
def codec():
    return mock_codec(f"replay-{uuid.uuid4()}")


@pytest.fixture
def replayer(db, settings, codec):
    encoder, decoder = codec
    r = Replayer(Settings(database_url=settings.database_url), db, operator="test",
                 registry=encoder._ser._registry)  # fmt: skip
    r.decoder = decoder
    return r


def value(encoder, robot, boot, seq, battery=50.0, boot_number=1):
    p = {"schema_version": 1, "event_id": str(uuid.uuid4()), "robot_id": str(robot),
         "boot_id": str(boot), "sequence_number": seq,
         "observed_at": "2026-01-01T00:00:00+00:00", "battery_pct": battery,
         "x_m": float(seq), "y_m": 0.0}  # fmt: skip
    return p["event_id"], encoder.encode(
        avro_record_from_payload(p, boot_number=boot_number, firmware_version="1.0.0",
                                 producer_id="t"))  # fmt: skip


def ingest(db, decoder, values, start=0):
    recs = [SourceRecord(TOPIC, 0, start + i, v) for i, v in enumerate(values)]
    return BatchProcessor(db, decoder).process(recs)


def quarantine(db, event_id, source="kafka"):
    """The original quarantine row (replays may add their own audit rows)."""
    with db.connect() as c:
        sql = text(
            "SELECT reason_code, status, resolution FROM fleet.telemetry_rejections"
            " WHERE event_id = :e AND source = :s"
        )
        return c.execute(sql, {"e": event_id, "s": source}).mappings().one()


def state(db, robot):
    with db.connect() as c:
        return c.execute(text("SELECT sequence_number, battery_pct, event_id"
                              " FROM fleet.robot_latest_state WHERE robot_id = :r"),
                         {"r": robot}).mappings().first()  # fmt: skip


def register(db, robot, boot):
    robots.register_robot(db, RobotRegistration(robot_id=robot, robot_model="AMR-100",
                                                firmware_version="1.0.0"))  # fmt: skip
    return robots.register_boot(db, robot, boot)[0]


def test_dry_run_then_real_replay_of_retry_eligible_quarantine(db, codec, replayer):
    encoder, decoder = codec
    robot, boot = uuid.uuid4(), uuid.uuid4()
    eid, v = value(encoder, robot, boot, 5)
    ingest(db, decoder, [v])  # robot not registered yet
    assert quarantine(db, eid)["reason_code"] == "UNKNOWN_ROBOT"

    register(db, robot, boot)  # the cause is fixed

    dry = replayer.quarantined("UNKNOWN_ROBOT", 100, dry_run=True)
    assert dry["outcomes"] == {"APPLIED/-": 1}
    assert state(db, robot) is None  # rolled back
    assert quarantine(db, eid)["status"] == "QUARANTINED"

    real = replayer.quarantined("UNKNOWN_ROBOT", 100, dry_run=False)
    assert real["outcomes"] == {"APPLIED/-": 1}
    assert state(db, robot)["sequence_number"] == 5
    q = quarantine(db, eid)
    assert q["status"] == "REPLAYED" and q["resolution"]["outcome"] == "APPLIED"

    again = replayer.quarantined("UNKNOWN_ROBOT", 100, dry_run=False)
    assert again["records"] == 0  # nothing left to replay
    with db.connect() as c:
        runs = c.execute(text("SELECT dry_run FROM fleet.replay_runs ORDER BY requested_at")).all()
    assert [r.dry_run for r in runs] == [True, False, False]


def test_replay_never_overwrites_newer_state(db, codec, replayer):
    encoder, decoder = codec
    robot, boot = uuid.uuid4(), uuid.uuid4()
    old_id, old = value(encoder, robot, boot, 3)
    ingest(db, decoder, [old])  # quarantined: robot unknown at the time
    register(db, robot, boot)
    _, newer = value(encoder, robot, boot, 10)
    ingest(db, decoder, [newer], start=1)  # robot moves on to seq 10

    out = replayer.by_event(old_id, dry_run=False)

    assert out["outcomes"] == {"STALE/STALE_SEQUENCE": 1}
    assert state(db, robot)["sequence_number"] == 10
    assert quarantine(db, old_id)["status"] == "REPLAYED"
    replay_audit = quarantine(db, old_id, source="replay")
    assert (replay_audit["reason_code"], replay_audit["status"]) == ("STALE_SEQUENCE", "RECORDED")


def test_correction_supersedes_a_rejected_event_with_a_new_identity(db, codec, replayer):
    encoder, decoder = codec
    robot, boot = uuid.uuid4(), uuid.uuid4()
    register(db, robot, boot)
    _, ok = value(encoder, robot, boot, 3)
    bad_id, bad = value(encoder, robot, boot, 4, battery=150.0)
    ingest(db, decoder, [ok, bad])
    assert quarantine(db, bad_id)["reason_code"] == "BATTERY_OUT_OF_RANGE"

    out = replayer.correct(bad_id, {"battery_pct": 55.0}, dry_run=False)

    assert out["outcomes"] == {"APPLIED/-": 1}
    s = state(db, robot)
    assert (s["sequence_number"], s["battery_pct"]) == (4, 55.0)
    assert s["event_id"] == UUID(out_correction := _correction_id(db, bad_id))
    assert out_correction != bad_id
    q = quarantine(db, bad_id)
    assert q["status"] == "RESOLVED"

    # A second, different correction for the same position is now a conflict, not an
    # overwrite: the first correction already became the held state.
    second = replayer.correct(bad_id, {"battery_pct": 60.0}, dry_run=False)
    assert second["outcomes"] == {"CONFLICT/SEQUENCE_CONFLICT": 1}
    assert state(db, robot)["battery_pct"] == 55.0


def _correction_id(db, original):
    with db.connect() as c:
        sql = text(
            "SELECT selector->>'correction_event_id' FROM fleet.replay_runs"
            " WHERE mode = 'CORRECTION' AND selector->>'event_id' = :e"
            " ORDER BY requested_at LIMIT 1"
        )
        return c.execute(sql, {"e": original}).scalar_one()


class _RestartingRegistry:
    """Answers 404 for every schema id, like Apicurio while it reloads its store."""

    def get_schema(self, schema_id):
        from confluent_kafka.schema_registry.error import SchemaRegistryError

        raise SchemaRegistryError(404, 40403, "Schema not found")


def test_unresolved_schema_is_parked_durably_then_replayed(db, codec, replayer):
    """ADR 0016 invariant: a registry that keeps answering 404 parks valid events, with
    bytes and coordinates committed together with the ledger advance; they never enter
    the malformed quarantine, and replay applies them once the registry answers."""
    from fleetforge.kafka.codec import TelemetryDecoder

    encoder, _ = codec
    robot, boot = uuid.uuid4(), uuid.uuid4()
    register(db, robot, boot)
    good_id, good = value(encoder, robot, boot, 1)
    bad_id, bad = value(encoder, robot, boot, 2, battery=150.0)  # a real data error
    blind = TelemetryDecoder(_RestartingRegistry(), TOPIC, unresolved_hold_s=0)
    recs = [SourceRecord(TOPIC, 0, 7, good), SourceRecord(TOPIC, 0, 8, bad)]
    BatchProcessor(db, blind, consumer_group="parking").process(recs)

    with db.connect() as c:
        rows = c.execute(text(
            "SELECT reason_code, error_category, status, retry_eligible, payload_bytes,"
            " source_partition, source_offset FROM fleet.telemetry_rejections"
            " WHERE source = 'kafka' ORDER BY source_offset")).mappings().all()  # fmt: skip
        in_quarantine_view = c.execute(text(
            "SELECT count(*) FROM fleet.telemetry_quarantine")).scalar_one()  # fmt: skip
        ledger = c.execute(text(
            "SELECT next_offset FROM fleet.consumer_offsets WHERE consumer_group = 'parking'"
        )).scalar_one()  # fmt: skip
    parked = ("UNRESOLVED_SCHEMA_ID", "UNRESOLVED", "PARKED", True)
    assert [(r["reason_code"], r["error_category"], r["status"], r["retry_eligible"])
            for r in rows] == [parked, parked]  # fmt: skip
    assert [(bytes(r["payload_bytes"]), r["source_partition"], r["source_offset"])
            for r in rows] == [(good, 0, 7), (bad, 0, 8)]  # fmt: skip
    assert in_quarantine_view == 0  # parked is not "invalid data"
    assert ledger == 9  # advanced past both, backed by the durable rows above
    assert state(db, robot) is None

    out = replayer.quarantined("UNRESOLVED_SCHEMA_ID", 100, dry_run=False)
    assert out["outcomes"] == {"APPLIED/-": 1, "REJECTED/BATTERY_OUT_OF_RANGE": 1}
    assert str(state(db, robot)["event_id"]) == good_id
    with db.connect() as c:
        statuses = dict(c.execute(text(
            "SELECT source_offset, status FROM fleet.telemetry_rejections"
            " WHERE source = 'kafka'")).all())  # fmt: skip
        verdict = c.execute(text(
            "SELECT reason_code FROM fleet.telemetry_rejections WHERE event_id = :e"
            " AND source = 'replay'"), {"e": bad_id}).scalar_one()  # fmt: skip
    assert statuses == {7: "REPLAYED", 8: "RESOLVED"}  # nothing left parked
    assert verdict == "BATTERY_OUT_OF_RANGE"  # the real error, judged with the schema
