"""Batch planner, Avro contract validation and error taxonomy (pure, no I/O)."""

import random
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from fleetforge.domain.errors import (
    REASONS,
    ErrorCategory,
    category_of,
    initial_status,
    is_recoverable,
    retry_eligible,
)
from fleetforge.domain.ingest import Candidate, Observation, observation_from_record, plan_batch
from fleetforge.domain.telemetry import (
    Decision,
    OrderingKey,
    ReasonCode,
    Rejection,
    StoredPosition,
    classify,
)

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def obs(robot: UUID, boot: int, seq: int, event_id: UUID | None = None) -> Observation:
    return Observation(
        event_id or uuid4(), robot, uuid4(), boot, seq, T0, 50.0, 0.0, 0.0, 1, "1.0.0"
    )


class TestPlanner:
    def test_batch_with_several_events_per_robot_writes_one_winner(self):
        r = uuid4()
        e = [obs(r, 1, s) for s in (5, 7, 6)]
        dup = obs(r, 1, 7, e[1].event_id)
        plan = plan_batch([Candidate(i, o) for i, o in enumerate([*e, dup])], {})
        assert [p.decision for p in plan.decisions] == [
            Decision.APPLY, Decision.APPLY, Decision.STALE, Decision.DUPLICATE]  # fmt: skip
        assert plan.winners[r].obs.sequence_number == 7
        assert plan.winners[r].applied == 2  # state_version += 2, same as one at a time

    def test_held_state_from_before_the_batch_is_respected(self):
        r = uuid4()
        held = {r: StoredPosition(OrderingKey(2, 0), uuid4())}
        plan = plan_batch([Candidate(0, obs(r, 1, 999))], held)
        assert plan.decisions[0].decision is Decision.STALE
        assert plan.winners == {}

    @pytest.mark.parametrize("seed", range(25))
    def test_equivalent_to_sequential_classification(self, seed):
        """Property: for random batches (several robots, reboots, duplicates, shuffles),
        the plan equals applying classify() one event at a time."""
        rng = random.Random(seed)
        robots = [uuid4() for _ in range(4)]
        events = []
        for _ in range(60):
            r = rng.choice(robots)
            e = obs(r, rng.randint(1, 3), rng.randint(0, 15))
            events.append(e)
            if rng.random() < 0.2:
                events.append(e)  # exact redelivery
        rng.shuffle(events)
        stored = {robots[0]: StoredPosition(OrderingKey(1, 5), uuid4())}

        plan = plan_batch([Candidate(i, o) for i, o in enumerate(events)], stored)

        current, applied = dict(stored), {}
        for i, e in enumerate(events):
            key = OrderingKey(e.boot_number, e.sequence_number)
            d = classify(key, e.event_id, current.get(e.robot_id))
            assert plan.decisions[i].decision is d
            if d is Decision.APPLY:
                current[e.robot_id] = StoredPosition(key, e.event_id)
                applied[e.robot_id] = applied.get(e.robot_id, 0) + 1
        assert {r: w.applied for r, w in plan.winners.items()} == applied
        for r, w in plan.winners.items():
            assert StoredPosition(OrderingKey(w.obs.boot_number, w.obs.sequence_number),
                                  w.obs.event_id) == current[r]  # fmt: skip


def record(**over):
    rec = {
        "schema_version": 1, "event_id": uuid4(), "robot_id": uuid4(), "boot_id": uuid4(),
        "boot_number": 1, "sequence_number": 3, "observed_at": T0, "firmware_version": "1.0.0",
        "battery_pct": 50.0, "position": {"x_m": 1.0, "y_m": 2.0}, "sensors": None,
        "correction_of": None, "producer": {"producer_id": "p", "produced_at": T0},
    }  # fmt: skip
    rec.update(over)
    return rec


class TestContract:
    def test_valid_record(self):
        o = observation_from_record(record())
        assert isinstance(o, Observation) and o.x_m == 1.0 and o.produced_at == T0

    def test_writer_schema_without_required_field(self):
        r = record()
        del r["battery_pct"]  # e.g. a registered revision that dropped the field
        assert observation_from_record(r).reason_code is ReasonCode.MISSING_FIELD

    @pytest.mark.parametrize("bad", [float("nan"), float("inf")])
    def test_non_finite_double_rejected(self, bad):
        res = observation_from_record(record(position={"x_m": bad, "y_m": 0.0}))
        assert isinstance(res, Rejection) and res.reason_code is ReasonCode.INVALID_FIELD

    def test_future_major_version_rejected(self):
        r = observation_from_record(record(schema_version=2))
        assert r.reason_code is ReasonCode.UNSUPPORTED_SCHEMA_VERSION

    def test_extra_fields_from_newer_compatible_revision_are_ignored(self):
        assert isinstance(observation_from_record(record(heading_rad=1.2)), Observation)


class TestTaxonomy:
    def test_every_reason_code_is_classified(self):
        assert set(REASONS) == set(ReasonCode)

    def test_categories_drive_status_and_retry(self):
        assert category_of(ReasonCode.STALE_SEQUENCE) is ErrorCategory.ORDERING
        assert initial_status(ReasonCode.STALE_SEQUENCE) == "RECORDED"
        assert initial_status(ReasonCode.BATTERY_OUT_OF_RANGE) == "QUARANTINED"
        assert retry_eligible(ReasonCode.UNKNOWN_BOOT)  # may be registered later
        assert not retry_eligible(ReasonCode.MALFORMED_PAYLOAD)  # same bytes, same result

    def test_infrastructure_errors_are_recoverable_data_errors_are_not(self):
        import psycopg

        assert is_recoverable(OperationalError("x", {}, Exception()))
        assert is_recoverable(psycopg.OperationalError("connection refused"))
        assert not is_recoverable(IntegrityError("x", {}, Exception()))
        assert not is_recoverable(ValueError("bug"))


class TestRegistryOutageClassification:
    """Regression: a registry outage must never be recorded as bad data."""

    def test_registry_5xx_is_recoverable_but_unknown_schema_id_is_permanent(self):
        from confluent_kafka.schema_registry.error import SchemaRegistryError

        from fleetforge.kafka.codec import (
            DecodeError,
            RegistryUnavailable,
            TelemetryDecoder,
        )

        class FailingClient:
            def __init__(self, exc, subjects=("t-value",)):
                self.exc, self.subjects = exc, subjects

            def get_schema(self, schema_id):
                raise self.exc

            def get_subjects(self, subject_prefix=None):
                if isinstance(self.subjects, Exception):
                    raise self.subjects
                return list(self.subjects)

        framed = b"\x00\x00\x00\x00\x01payload"
        dec = TelemetryDecoder(FailingClient(SchemaRegistryError(500, 50001, "db down")), "t")
        with pytest.raises(RegistryUnavailable):
            dec.decode(framed)
        assert is_recoverable(RegistryUnavailable("x"))
        dec = TelemetryDecoder(FailingClient(SchemaRegistryError(404, 40403, "not found")), "t")
        with pytest.raises(DecodeError):
            dec.decode(framed)  # registry holds our subject: the id really is unknown
        # A registry that is up but still loading its store (Apicurio KafkaSQL) answers
        # 404 for existing ids and cannot confirm our subject: never permanent.
        not_loaded = FailingClient(SchemaRegistryError(404, 40403, "not found"), subjects=())
        with pytest.raises(RegistryUnavailable):
            TelemetryDecoder(not_loaded, "t").decode(framed)
        flaky = FailingClient(SchemaRegistryError(404, 40403, "not found"),
                              subjects=SchemaRegistryError(503, 50301, "starting"))  # fmt: skip
        with pytest.raises(RegistryUnavailable):
            TelemetryDecoder(flaky, "t").decode(framed)
        with pytest.raises(DecodeError):
            dec.decode(b"{not avro}")
        assert dec._writers == {}  # failures are never cached


class TestDecoderEquivalence:
    """The projector decodes with fastavro directly (final audit: the Confluent
    deserializer's per-message rule/tag machinery was ~25 % of projector CPU). It must
    return exactly what confluent-kafka's AvroDeserializer returns."""

    def _clients(self):
        from confluent_kafka.schema_registry._sync.mock_schema_registry_client import (
            MockSchemaRegistryClient,
        )

        from fleetforge.kafka.schemas import register

        c = MockSchemaRegistryClient({"url": "mock://equivalence"})
        register(c, "telemetry.raw.v1-value", 1, set_rule=False)
        register(c, "telemetry.raw.v1-value", 2, set_rule=False)
        return c

    def test_v1_and_v2_records_decode_identically(self):
        from datetime import UTC, datetime

        from confluent_kafka.schema_registry.avro import AvroDeserializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        from fleetforge.kafka.codec import TelemetryDecoder, TelemetryEncoder

        c = self._clients()
        ref = AvroDeserializer(c)
        ctx = SerializationContext("telemetry.raw.v1", MessageField.VALUE)
        ours = TelemetryDecoder(c, "telemetry.raw.v1")
        t = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
        base = {"schema_version": 1, "event_id": "8d0b8c4e-63b6-4a43-9a8f-2b7a3cdbdc11",
                "robot_id": "1f3c3a4e-0b0c-4d5f-8a8b-9c0d1e2f3a4b",
                "boot_id": "2a3b4c5d-6e7f-4081-92a3-b4c5d6e7f809", "boot_number": 3,
                "sequence_number": 42, "observed_at": t, "firmware_version": "1.0.0",
                "battery_pct": 55.5, "position": {"x_m": 1.25, "y_m": -3.5}, "sensors": None,
                "correction_of": None,
                "producer": {"producer_id": "p", "produced_at": t}}  # fmt: skip
        sensors = {"motor_temp_c": 40.5, "lidar_ok": True}
        for rev, rec in [(1, base), (1, {**base, "sensors": sensors}),
                         (1, {**base, "correction_of": base["boot_id"]}),
                         (2, {**base, "heading_rad": 1.5, "sensors": sensors})]:  # fmt: skip
            value = TelemetryEncoder(c, "telemetry.raw.v1", revision=rev).encode(rec)
            assert ours.decode(value) == ref(value, ctx)

    def test_corrupt_bodies_are_permanent_decode_errors(self):
        from fleetforge.kafka.codec import DecodeError, TelemetryDecoder

        c = self._clients()
        dec = TelemetryDecoder(c, "telemetry.raw.v1")
        sid = c.get_latest_version("telemetry.raw.v1-value").schema_id
        framed = b"\x00" + sid.to_bytes(4, "big")
        for body in [b"", b"\x02", b"\xff" * 3]:
            with pytest.raises(DecodeError):
                dec.decode(framed + body)
        with pytest.raises(DecodeError):
            dec.decode(b"\x00\x00\x00\x27\x0f" + b"x")  # unknown schema id


class TestKafkaRetry:
    def test_transient_admin_errors_are_retried_and_others_are_not(self):
        from confluent_kafka import KafkaError, KafkaException

        from fleetforge.kafka.retry import retry_transient

        calls = []

        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise KafkaException(KafkaError(KafkaError._DESTROY_BROKER))
            return "ok"

        assert retry_transient(flaky, backoff_s=0) == "ok" and len(calls) == 3

        def permanent():
            raise KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))

        with pytest.raises(KafkaException):
            retry_transient(permanent, backoff_s=0)

        def always():
            raise KafkaException(KafkaError(KafkaError._TIMED_OUT))

        with pytest.raises(KafkaException):
            retry_transient(always, attempts=3, backoff_s=0)
