"""Registry uncertainty must never become permanent "bad data".

Root cause of the incident (ADR 0016):
confluent-kafka's registry client raises httpx2 exceptions (it prefers httpx2 when
installed, and requirements.lock installs it), while the classifier only recognised
httpx/OSError transport errors. A connection reset during a registry restart was
therefore treated as "maybe the record's fault", and became MALFORMED_PAYLOAD as soon
as the subject listing succeeded a moment later.
"""

from __future__ import annotations

import json

import httpx2
import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError

FRAMED = b"\x00\x00\x00\x00\x01payload"


class Client:
    """A registry client whose get_schema raises `exc` (or returns `schema`)."""

    def __init__(self, exc=None, schema=None, subjects=("t-value",)):
        self.exc, self.schema, self.subjects, self.calls = exc, schema, subjects, 0

    def get_schema(self, schema_id):
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.schema

    def get_subjects(self, subject_prefix=None):
        return list(self.subjects)


@pytest.mark.parametrize(
    "exc",
    [
        httpx2.RemoteProtocolError("Server disconnected without sending a response."),
        httpx2.ConnectError("connection refused"),
        httpx2.ReadError("connection reset by peer"),
        httpx2.ReadTimeout("timed out"),
        json.JSONDecodeError("half-started server sent HTML", "<html>", 0),
        RuntimeError("anything the client did not expect"),
    ],
)
def test_any_failure_other_than_an_explicit_404_is_an_outage(exc):
    """Observed in a real Apicurio restart: RemoteProtocolError from get_schema while
    the subject listing already succeeded. It must be retried, never quarantined."""
    from fleetforge.kafka.codec import RegistryUnavailable, TelemetryDecoder

    with pytest.raises(RegistryUnavailable):
        TelemetryDecoder(Client(exc), "t").decode(FRAMED)


def test_a_404_while_the_subject_is_listed_is_not_proof():
    """A restarting Apicurio answers 404 for ids that exist while it already lists
    subjects (measured: id 36 404 for 0.1-0.3 s with subjects listed, 5 of 5 restarts)."""
    from fleetforge.kafka.codec import RegistryUnavailable, TelemetryDecoder

    with pytest.raises(RegistryUnavailable):
        TelemetryDecoder(Client(SchemaRegistryError(404, 40403, "not found")), "t").decode(FRAMED)


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _registry_with_schema():
    from confluent_kafka.schema_registry._sync.mock_schema_registry_client import (
        MockSchemaRegistryClient,
    )

    from fleetforge.kafka.schemas import register

    c = MockSchemaRegistryClient({"url": "mock://policy"})
    register(c, "telemetry.raw.v1-value", 1, set_rule=False)
    return c


def _valid_value(c):
    from datetime import UTC, datetime

    from fleetforge.kafka.codec import TelemetryEncoder

    t = datetime(2026, 11, 14, tzinfo=UTC)
    rec = {"schema_version": 1, "event_id": "8d0b8c4e-63b6-4a43-9a8f-2b7a3cdbdc11",
           "robot_id": "1f3c3a4e-0b0c-4d5f-8a8b-9c0d1e2f3a4b",
           "boot_id": "2a3b4c5d-6e7f-4081-92a3-b4c5d6e7f809", "boot_number": 1,
           "sequence_number": 1, "observed_at": t, "firmware_version": "1.0.0",
           "battery_pct": 50.0, "position": {"x_m": 1.0, "y_m": 2.0}, "sensors": None,
           "correction_of": None, "producer": {"producer_id": "p", "produced_at": t}}  # fmt: skip
    return TelemetryEncoder(c, "telemetry.raw.v1").encode(rec)


class Restarting:
    """Answers 404 until `ready` is set, then delegates to a real (mock) registry."""

    def __init__(self, real):
        self.real, self.ready, self.calls = real, False, 0

    def get_schema(self, schema_id):
        self.calls += 1
        if not self.ready:
            raise SchemaRegistryError(404, 40403, "Schema not found")
        return self.real.get_schema(schema_id)


def test_a_404_is_held_as_an_outage_then_parked_never_malformed():
    from fleetforge.kafka.codec import (
        DecodeError,
        RegistryUnavailable,
        SchemaUnresolved,
        TelemetryDecoder,
    )

    real, clock = _registry_with_schema(), Clock()
    value = _valid_value(real)
    reg = Restarting(real)
    dec = TelemetryDecoder(reg, "telemetry.raw.v1", unresolved_hold_s=120, clock=clock)
    for dt in (0, 30, 119.9):  # within the hold: offsets held, batch retried
        clock.t = 1000 + dt
        with pytest.raises(RegistryUnavailable):
            dec.decode(value)
    clock.t = 1000 + 120
    with pytest.raises(SchemaUnresolved) as parked:  # bounded: parked, not malformed
        dec.decode(value)
    assert not isinstance(parked.value, DecodeError)
    assert parked.value.schema_id == 1 and parked.value.unresolved_for_s == 120


def test_parked_ids_are_rechecked_at_a_bounded_rate_and_recover():
    from fleetforge.kafka.codec import SchemaUnresolved, TelemetryDecoder

    real, clock = _registry_with_schema(), Clock()
    value = _valid_value(real)
    reg = Restarting(real)
    dec = TelemetryDecoder(reg, "telemetry.raw.v1", unresolved_hold_s=0, recheck_s=5,
                           clock=clock)  # fmt: skip
    with pytest.raises(SchemaUnresolved):
        dec.decode(value)
    calls = reg.calls
    for _ in range(100):  # a flood of records with that id: no request storm
        with pytest.raises(SchemaUnresolved):
            dec.decode(value)
    assert reg.calls == calls
    reg.ready = True
    clock.t += 5
    assert dec.decode(value)["sequence_number"] == 1  # the registry is back: decoded
    reg.ready = False
    assert dec.decode(value)["sequence_number"] == 1  # found schemas stay cached


def test_verified_invalid_records_stay_permanent():
    from fleetforge.kafka.codec import DecodeError, TelemetryDecoder

    real = _registry_with_schema()
    value = _valid_value(real)
    dec = TelemetryDecoder(real, "telemetry.raw.v1")
    for bad in (b"", b"{not avro}", b"\x01" + value[1:], value[:5], value[:-3]):
        with pytest.raises(DecodeError):
            dec.decode(bad)

    class Protobuf:
        schema_type, schema_str = "PROTOBUF", "syntax = 'proto3';"

    with pytest.raises(DecodeError):
        TelemetryDecoder(Client(schema=Protobuf()), "t").decode(FRAMED)


def test_unresolved_is_parked_with_its_own_category_and_status():
    from fleetforge.domain.errors import (
        ErrorCategory,
        category_of,
        initial_status,
        is_recoverable,
        retry_eligible,
    )
    from fleetforge.domain.telemetry import ReasonCode
    from fleetforge.kafka.codec import RegistryUnavailable, SchemaUnresolved

    code = ReasonCode.UNRESOLVED_SCHEMA_ID
    assert category_of(code) is ErrorCategory.UNRESOLVED  # not PERMANENT_DATA
    assert retry_eligible(code) and initial_status(code) == "PARKED"
    assert is_recoverable(RegistryUnavailable("x"))
    assert not is_recoverable(SchemaUnresolved(1, 120, "404"))  # parked, not retried
