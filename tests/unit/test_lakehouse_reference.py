"""Reference data rebuilt from Bronze copies of firmware CDC and robot outbox events."""

from __future__ import annotations

import json

from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse.bronze import KafkaRecord, bronze_row
from fleetforge.lakehouse.reference import FIRMWARE_TOPIC, ROBOT_EVENTS_TOPIC, build_reference


def _rows(topic):
    return [r for r in fx.bronze_rows(fx.scenario(), (1,)) if r["source_topic"] == topic]


def _row(topic, offset, value, headers=()):
    rec = KafkaRecord(topic, 0, offset, None, value, None, 0, headers)
    return bronze_row(rec, topic_id="X", run_id="r", ingested_at=fx.RUNS[1][1])


def test_firmware_cdc_applies_snapshot_create_and_delete_in_order():
    ref = build_reference(_rows(FIRMWARE_TOPIC), [])
    assert set(ref.firmware) == {("AMR-100", "1.0.0"), ("AMR-100", "1.1.0"),
                                 ("AMR-200", "2.0.0"), ("AMR-300", "3.0.0-fixture")}  # fmt: skip
    c = ref.firmware[("AMR-300", "3.0.0-fixture")]
    assert (c.battery_unit, c.position_unit, c.cdc_op) == ("FRACTION", "MM", "c")
    assert ref.anomalies == ()


def test_update_changes_units_and_order_is_by_arrival_not_input():
    rows = _rows(FIRMWARE_TOPIC)
    env = json.loads(rows[1]["record_value"])
    env.update(op="u", before=env["after"], after={**env["after"], "position_unit": "MM"})
    rows.append(_row(FIRMWARE_TOPIC, 99, json.dumps(env).encode()))
    rows[-1]["kafka_topic_id"] = rows[0]["kafka_topic_id"]
    ref = build_reference(list(reversed(rows)), [])
    assert ref.firmware[("AMR-100", "1.1.0")].position_unit == "MM"


def test_schema_wrapped_envelopes_are_unwrapped():
    env = json.loads(_rows(FIRMWARE_TOPIC)[0]["record_value"])
    row = _row(FIRMWARE_TOPIC, 0, json.dumps({"schema": {}, "payload": env}).encode())
    assert ("AMR-100", "1.0.0") in build_reference([row], []).firmware


def test_malformed_reference_records_become_anomalies_not_silence():
    bad = [_row(FIRMWARE_TOPIC, 0, b"not json"),
           _row(FIRMWARE_TOPIC, 1, json.dumps({"op": "x", "after": {}}).encode()),
           _row(FIRMWARE_TOPIC, 2, json.dumps({"op": "c", "after": {
               "model_id": "M", "version": "1", "battery_unit": "VOLT",
               "position_unit": "M"}}).encode())]  # fmt: skip
    robots = [_row(ROBOT_EVENTS_TOPIC, 0, b"{}"),
              _row(ROBOT_EVENTS_TOPIC, 1, b"{}", (("eventType", b"RobotRegistered"),))]  # fmt: skip
    ref = build_reference(bad, robots)
    assert ref.firmware == {} and ref.robots == {}
    assert len(ref.anomalies) == 5
    assert ref.fingerprint() != build_reference([], []).fingerprint()


def test_robot_models_come_from_registered_events_and_are_immutable():
    rows = _rows(ROBOT_EVENTS_TOPIC)
    clash = json.loads(rows[0]["record_value"]) | {"robot_model": "AMR-200"}
    rows.append(_row(ROBOT_EVENTS_TOPIC, 5, json.dumps(clash).encode(),
                     (("eventType", b"RobotRegistered"),)))  # fmt: skip
    ref = build_reference([], rows)
    assert ref.robot_model(fx.ROBOT_A) == "AMR-100"
    assert ref.robot_model(fx.ROBOT_X) is None
    assert any("re-registered" in a.message for a in ref.anomalies)


def test_reference_fingerprint_is_order_independent():
    fw, rb = _rows(FIRMWARE_TOPIC), _rows(ROBOT_EVENTS_TOPIC)
    assert (build_reference(fw, rb).fingerprint()
            == build_reference(list(reversed(fw)), list(reversed(rb))).fingerprint())  # fmt: skip
