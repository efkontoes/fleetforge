import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from fleetforge.contracts.telemetry import TelemetryEventV1
from fleetforge.domain.telemetry import (
    Decision,
    OrderingKey,
    ReasonCode,
    Rejection,
    StoredPosition,
    check_semantics,
    classify,
    dedup_key,
    parse_event,
    payload_for_audit,
)

NOW = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
SKEW = timedelta(minutes=5)


def event_dict(**overrides) -> dict:
    base = {
        "schema_version": 1,
        "event_id": str(uuid4()),
        "robot_id": str(uuid4()),
        "boot_id": str(uuid4()),
        "sequence_number": 102,
        "observed_at": NOW.isoformat(),
        "battery_pct": 61.0,
        "x_m": 1.5,
        "y_m": 2.5,
    }
    base.update(overrides)
    return base


def parse(d) -> TelemetryEventV1 | Rejection:
    return parse_event(json.dumps(d).encode())


def event(**overrides) -> TelemetryEventV1:
    return TelemetryEventV1.model_validate_json(json.dumps(event_dict(**overrides)))


class TestStructural:
    def test_valid_event_parses(self):
        assert isinstance(parse(event_dict()), TelemetryEventV1)

    def test_missing_field(self):
        d = event_dict()
        del d["battery_pct"]
        r = parse(d)
        assert r.reason_code is ReasonCode.MISSING_FIELD
        assert r.errors[0]["field"] == "battery_pct"

    def test_string_number_is_not_coerced(self):
        r = parse(event_dict(sequence_number="102"))
        assert r.reason_code is ReasonCode.INVALID_FIELD

    def test_negative_sequence(self):
        assert parse(event_dict(sequence_number=-1)).reason_code is ReasonCode.INVALID_FIELD

    def test_naive_timestamp_rejected(self):
        r = parse(event_dict(observed_at="2026-11-12T10:00:00"))
        assert r.reason_code is ReasonCode.INVALID_FIELD

    def test_nan_rejected(self):
        body = json.dumps(event_dict()).replace("61.0", "NaN").encode()
        assert parse_event(body).reason_code is ReasonCode.INVALID_FIELD

    def test_unknown_field_rejected(self):
        assert parse(event_dict(speed=3)).reason_code is ReasonCode.INVALID_FIELD

    def test_unsupported_schema_version_takes_precedence(self):
        d = event_dict(schema_version=2)
        del d["x_m"]
        assert parse(d).reason_code is ReasonCode.UNSUPPORTED_SCHEMA_VERSION

    def test_malformed_json(self):
        assert parse_event(b'{"schema_version": 1,').reason_code is ReasonCode.MALFORMED_JSON


class TestSemantic:
    def test_battery_150_rejected_with_machine_readable_reason(self):
        r = check_semantics(event(battery_pct=150.0), NOW, SKEW)
        assert r.reason_code is ReasonCode.BATTERY_OUT_OF_RANGE
        assert r.errors == [
            {
                "code": "BATTERY_OUT_OF_RANGE",
                "field": "battery_pct",
                "message": "battery_pct=150.0 outside [0, 100]",
            }
        ]

    @pytest.mark.parametrize("battery", [0.0, 100.0])
    def test_battery_bounds_inclusive(self, battery):
        assert check_semantics(event(battery_pct=battery), NOW, SKEW) is None

    def test_future_observation_rejected(self):
        e = event(observed_at=(NOW + timedelta(minutes=6)).isoformat())
        assert check_semantics(e, NOW, SKEW).reason_code is ReasonCode.OBSERVED_AT_IN_FUTURE

    def test_small_clock_skew_tolerated(self):
        e = event(observed_at=(NOW + timedelta(minutes=4)).isoformat())
        assert check_semantics(e, NOW, SKEW) is None


class TestOrdering:
    """The spec's arrival-order table, as a pure function."""

    E100, E101, E102 = (UUID(int=n) for n in (100, 101, 102))

    def stored(self, seq, event_id, boot=1):
        return StoredPosition(OrderingKey(boot, seq), event_id)

    def test_first_event_applies(self):
        assert classify(OrderingKey(1, 100), self.E100, None) is Decision.APPLY

    def test_newer_applies(self):
        assert (
            classify(OrderingKey(1, 102), self.E102, self.stored(100, self.E100)) is Decision.APPLY
        )

    def test_older_is_stale(self):
        assert (
            classify(OrderingKey(1, 101), self.E101, self.stored(102, self.E102)) is Decision.STALE
        )

    def test_redelivery_is_duplicate(self):
        assert (
            classify(OrderingKey(1, 102), self.E102, self.stored(102, self.E102))
            is Decision.DUPLICATE
        )

    def test_same_position_different_event_is_conflict(self):
        assert (
            classify(OrderingKey(1, 102), uuid4(), self.stored(102, self.E102)) is Decision.CONFLICT
        )

    def test_new_boot_wins_despite_sequence_reset(self):
        assert (
            classify(OrderingKey(2, 0), uuid4(), self.stored(90_000, self.E100)) is Decision.APPLY
        )

    def test_old_boot_is_stale_even_with_higher_sequence(self):
        assert (
            classify(OrderingKey(1, 90_000), uuid4(), self.stored(0, self.E100, boot=2))
            is Decision.STALE
        )


class TestAuditHelpers:
    def test_payload_for_audit_keeps_non_json_as_text(self):
        assert payload_for_audit(b"not json") == "not json"

    def test_payload_for_audit_neutralises_nan_and_nul(self):
        assert payload_for_audit(b'{"a": NaN, "b": "x\\u0000y"}') == {"a": "NaN", "b": "x�y"}

    def test_dedup_key_prefers_event_id(self):
        eid = uuid4()
        assert dedup_key(eid, b"anything") == str(eid)
        assert dedup_key(None, b"abc") == dedup_key(None, b"abc")
        assert dedup_key(None, b"abc") != dedup_key(None, b"abd")
