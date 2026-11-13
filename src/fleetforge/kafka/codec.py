"""Avro encode/decode of telemetry values (Confluent wire format: magic 0 + 4-byte
schema id + Avro body). Kafka keys are the plain UTF-8 robot_id, so any client in
any language can partition consistently without the registry."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from typing import Any

import fastavro
import httpx
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

from fleetforge.kafka.schemas import schema_str


class DecodeError(Exception):
    """The bytes are not a valid telemetry record. Permanent: quarantine."""


class RegistryUnavailable(Exception):
    """The schema registry could not be asked. Says nothing about the record.
    Recoverable: retry the batch, never quarantine (that would turn an outage into
    'bad data' permanently)."""


def _registry_down(exc: BaseException) -> bool:
    """True for transport errors and any registry answer except 404."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, SchemaRegistryError):
            # 404 = unknown schema id: possibly the record's fault (see TelemetryDecoder).
            # Everything else (5xx, auth, ...) is the registry's problem.
            return exc.http_status_code != 404
        if isinstance(exc, httpx.TransportError | OSError | TimeoutError):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


class TelemetryEncoder:
    def __init__(self, client: SchemaRegistryClient, topic: str, revision: int = 1):
        self.topic = topic
        self._ctx = SerializationContext(topic, MessageField.VALUE)
        self._ser = AvroSerializer(
            client,
            schema_str(revision),
            conf={"auto.register.schemas": False, "use.latest.version": False},
        )

    def encode(self, record: dict[str, Any]) -> bytes:
        return self._ser(record, self._ctx)


class TelemetryDecoder:
    """Confluent framing (magic byte + schema id) decoded with fastavro and the writer's
    schema. A 404 is permanent only while the registry holds the topic's subject."""

    def __init__(self, client: SchemaRegistryClient, topic: str):
        self._client = client
        self._subject = f"{topic}-value"
        self._writers: dict[int, Any] = {}

    def _registry_is_authoritative(self) -> bool:
        """Uncached check that the registry knows this topic's subject."""
        try:
            return self._subject in self._client.get_subjects(subject_prefix=self._subject)
        except Exception:
            return False

    def _writer(self, schema_id: int) -> Any:
        parsed = self._writers.get(schema_id)
        if parsed is None:
            try:
                registered = self._client.get_schema(schema_id)
            except Exception as exc:
                if _registry_down(exc) or not self._registry_is_authoritative():
                    raise RegistryUnavailable(str(exc)) from exc
                raise DecodeError(f"schema id {schema_id}: {type(exc).__name__}: {exc}") from exc
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
