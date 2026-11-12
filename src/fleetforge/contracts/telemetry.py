"""Telemetry event contract, schema version 1.

This module only answers "is this structurally a v1 telemetry event?".
Physical plausibility lives in fleetforge.domain.telemetry.
"""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1


class TelemetryEventV1(BaseModel):
    # strict:       "102" is not a sequence number and true is not a battery reading.
    #               (Validated from JSON, so UUIDs and timestamps still parse from strings.)
    # extra=forbid: an unknown field usually signals a producer/consumer contract mismatch;
    #               silently dropping it would hide that.
    # allow_inf_nan=False: NaN/Infinity are not readings.
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal[1]
    event_id: UUID
    robot_id: UUID
    boot_id: UUID
    sequence_number: int = Field(ge=0)
    observed_at: AwareDatetime  # naive timestamps are ambiguous across robots: reject them
    battery_pct: float
    x_m: float
    y_m: float
