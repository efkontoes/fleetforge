"""Avro encode/decode of telemetry values (Confluent wire format: magic 0 + 4-byte
schema id + Avro body). Kafka keys are the plain UTF-8 robot_id, so any client in
any language can partition consistently without the registry."""

from __future__ import annotations

import io
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import fastavro
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

from fleetforge.kafka.schemas import schema_str


class DecodeError(Exception):
    """The bytes are verifiably not a valid telemetry record: no Confluent framing, a
    body that does not decode with the schema the registry returned, or a schema of
    another type. Permanent: quarantine."""


class RegistryUnavailable(Exception):
    """The schema registry could not answer, or answered 404 within the hold window.
    Says nothing about the record. Recoverable: retry the batch, never quarantine
    (that would turn an outage into 'bad data' permanently)."""


class SchemaUnresolved(Exception):
    """The registry kept answering 404 for a schema id for longer than the hold window.

    Not proof that the record is invalid: a reloading registry answers 404 for ids it
    holds. The record is parked (kept with its bytes and Kafka coordinates, never
    marked malformed, replayable) and an operator decides."""

    def __init__(self, schema_id: int, unresolved_for_s: float, detail: str):
        super().__init__(f"schema id {schema_id} unresolved for {unresolved_for_s:.0f}s: {detail}")
        self.schema_id = schema_id
        self.unresolved_for_s = unresolved_for_s


def schema_not_found(exc: BaseException) -> bool:
    """True only for the registry's explicit answer that a schema id does not exist.

    Every other failure is an outage, whatever its class or HTTP library. The previous
    allowlist of transport classes (httpx, OSError) missed httpx2, which
    confluent-kafka's client uses when installed, so a connection reset during a
    registry restart was quarantined as bad data."""
    return isinstance(exc, SchemaRegistryError) and exc.http_status_code == 404


def _registry_down(exc: BaseException) -> bool:
    """Inverse of schema_not_found(): unknown failures default to "registry down"."""
    return not schema_not_found(exc)


class TelemetryEncoder:
    def __init__(self, client: SchemaRegistryClient, topic: str, revision: int = 1):
        self.topic = topic
        self._ctx = SerializationContext(topic, MessageField.VALUE)
        self._ser = AvroSerializer(
            client,
            schema_str(revision),
            # Named explicitly: 2.16's default first asks the registry for Confluent-only
            # associations, a call Apicurio does not answer reliably (it hung CI for 600 s).
            conf={
                "auto.register.schemas": False,
                "use.latest.version": False,
                "subject.name.strategy": topic_subject_name_strategy,
            },  # fmt: skip
        )

    def encode(self, record: dict[str, Any]) -> bytes:
        return self._ser(record, self._ctx)


class TelemetryDecoder:
    """Confluent framing (magic byte + schema id) decoded with fastavro and the writer's
    schema. Found schemas are cached (ids are immutable).

    An unknown schema id (404) is never treated as proof of bad data, because a
    restarting registry answers 404 for ids it holds (measured; ADR 0016):
      * within `unresolved_hold_s` of the first 404: RegistryUnavailable, so the batch
        is retried and offsets are held, as in any outage;
      * after it: SchemaUnresolved, so the record is parked, not quarantined;
      * a parked id is asked again at most every `recheck_s`, and resolves normally
        once the registry returns it."""

    def __init__(self, client: SchemaRegistryClient, topic: str, *,
                 unresolved_hold_s: float = 120.0, recheck_s: float = 5.0,
                 clock: Callable[[], float] = time.monotonic):  # fmt: skip
        self._client = client
        self.topic = topic
        self._hold_s = unresolved_hold_s
        self._recheck_s = recheck_s
        self._clock = clock
        self._writers: dict[int, Any] = {}
        self._first_404: dict[int, float] = {}
        self._parked_at: dict[int, float] = {}

    def _writer(self, schema_id: int) -> Any:
        parsed = self._writers.get(schema_id)
        if parsed is not None:
            return parsed
        now = self._clock()
        parked = self._parked_at.get(schema_id)
        if parked is not None and now - parked < self._recheck_s:
            raise SchemaUnresolved(schema_id, now - self._first_404[schema_id],
                                   "registry answered 404 (re-checked recently)")  # fmt: skip
        try:
            registered = self._client.get_schema(schema_id)
        except Exception as exc:
            if not schema_not_found(exc):
                raise RegistryUnavailable(
                    f"schema id {schema_id}: {type(exc).__name__}: {exc}"
                ) from exc
            first = self._first_404.setdefault(schema_id, now)
            if now - first < self._hold_s:
                raise RegistryUnavailable(
                    f"schema id {schema_id} not found for {now - first:.1f}s"
                    f" (held up to {self._hold_s:.0f}s)"
                ) from exc
            self._parked_at[schema_id] = now
            raise SchemaUnresolved(schema_id, now - first, str(exc)) from exc
        self._first_404.pop(schema_id, None)
        self._parked_at.pop(schema_id, None)
        if registered.schema_type not in (None, "AVRO"):
            raise DecodeError(f"schema id {schema_id} is {registered.schema_type}, not AVRO")
        parsed = fastavro.parse_schema(json.loads(registered.schema_str))
        self._writers[schema_id] = parsed
        return parsed

    def decode(self, value: bytes | None) -> dict[str, Any]:
        if not value or len(value) < 5 or value[0] != 0:
            raise DecodeError("not Confluent-framed Avro (missing magic byte 0)")
        writer = self._writer(int.from_bytes(value[1:5], "big"))
        try:
            rec = fastavro.schemaless_reader(io.BytesIO(value[5:]), writer, None)
        except Exception as exc:  # truncated body, invalid union index, bad UTF-8, ...
            raise DecodeError(f"{type(exc).__name__}: {exc}") from exc
        if not isinstance(rec, dict):
            raise DecodeError("decoded value is not a record")
        return rec


def avro_record_from_payload(
    payload: dict[str, Any],
    *,
    boot_number: int,
    firmware_version: str,
    producer_id: str,
    produced_at: datetime | None = None,
    sensors: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Map a Milestone 1 JSON telemetry payload onto the Avro v1 record."""
    return {
        "schema_version": payload["schema_version"],
        "event_id": payload["event_id"],
        "robot_id": payload["robot_id"],
        "boot_id": payload["boot_id"],
        "boot_number": boot_number,
        "sequence_number": payload["sequence_number"],
        "observed_at": datetime.fromisoformat(payload["observed_at"]),
        "firmware_version": firmware_version,
        "battery_pct": float(payload["battery_pct"]),
        "position": {"x_m": float(payload["x_m"]), "y_m": float(payload["y_m"])},
        "sensors": sensors,
        "correction_of": payload.get("correction_of"),
        "producer": {
            "producer_id": producer_id,
            "produced_at": produced_at or datetime.now(UTC),
        },
    }


def json_safe(rec: dict[str, Any]) -> dict[str, Any]:
    """Decoded Avro (UUIDs, datetimes) -> JSON-serializable dict for the audit table."""

    def conv(v: Any) -> Any:
        if isinstance(v, dict):
            return {k: conv(x) for k, x in v.items()}
        if isinstance(v, list):
            return [conv(x) for x in v]
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
            return str(v)
        if isinstance(v, str | int | float | bool) or v is None:
            return v
        return str(v)

    return conv(rec)
