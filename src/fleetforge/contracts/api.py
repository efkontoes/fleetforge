"""Request/response contracts for the Fleet API (other than the telemetry event itself)."""

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RobotRegistration(_Strict):
    robot_id: UUID
    robot_model: str = Field(min_length=1, max_length=64)
    firmware_version: str = Field(min_length=1, max_length=64)


class Robot(BaseModel):
    robot_id: UUID
    robot_model: str
    firmware_version: str
    lifecycle_status: str
    registered_at: datetime


class BootRegistration(_Strict):
    boot_id: UUID


class Boot(BaseModel):
    robot_id: UUID
    boot_id: UUID
    boot_number: int


class TelemetryOutcome(StrEnum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    CONFLICT = "CONFLICT"
    REJECTED = "REJECTED"


class TelemetryError(BaseModel):
    # Reason-specific context (e.g. the position held when an event is stale) is kept.
    model_config = ConfigDict(extra="allow")

    code: str
    field: str | None = None
    message: str


class TelemetryResult(BaseModel):
    outcome: TelemetryOutcome
    event_id: UUID | None = None
    reason_code: str | None = None
    errors: list[TelemetryError] = []
    state_version: int | None = None


class MissionStatus(StrEnum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class MissionCreate(_Strict):
    mission_id: UUID
    robot_id: UUID


class MissionTransition(_Strict):
    transition_id: UUID = Field(description="Client-generated idempotency key")
    to_status: MissionStatus
    expected_version: int = Field(ge=0)


class Mission(BaseModel):
    mission_id: UUID
    robot_id: UUID
    status: MissionStatus
    version: int
    created_at: datetime
    updated_at: datetime


class RobotState(BaseModel):
    robot_id: UUID
    boot_id: UUID
    boot_number: int
    sequence_number: int
    event_id: UUID
    observed_at: datetime
    received_at: datetime
    battery_pct: float
    x_m: float
    y_m: float
    state_version: int
    active_mission_id: UUID | None
    active_mission_status: MissionStatus | None
    age_s: float
    is_fresh: bool
