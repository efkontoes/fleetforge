"""Error taxonomy. Different classes of failure get different treatment:

PERMANENT_DATA   the record itself is wrong. Retrying the same bytes cannot help.
                 -> quarantine with reason; continue with the next record.
                 Some are `retry_eligible`: the record is fine but refers to
                 something that may exist later (a robot registered after its first
                 telemetry). Those can be replayed after the cause is fixed.
RECOVERABLE      the infrastructure is temporarily wrong (DB down, timeout, broker
                 unreachable). The record is fine.
                 -> do NOT quarantine, do NOT advance offsets; back off and retry.
                    Lag grows; that is the backpressure signal.
ORDERING         the record is valid but not newer than what we hold (stale), or is
                 a redelivery (duplicate). -> recorded / counted, never retried.
"""

from enum import StrEnum

import psycopg
from sqlalchemy.exc import DataError, DBAPIError, IntegrityError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from fleetforge.domain.telemetry import ReasonCode


class ErrorCategory(StrEnum):
    PERMANENT_DATA = "PERMANENT_DATA"
    RECOVERABLE = "RECOVERABLE"
    ORDERING = "ORDERING"


# reason -> (category, retry_eligible)
REASONS: dict[ReasonCode, tuple[ErrorCategory, bool]] = {
    ReasonCode.MALFORMED_JSON: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.MALFORMED_PAYLOAD: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.MISSING_FIELD: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.INVALID_FIELD: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.UNSUPPORTED_SCHEMA_VERSION: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.BATTERY_OUT_OF_RANGE: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.OBSERVED_AT_IN_FUTURE: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.UNKNOWN_ROBOT: (ErrorCategory.PERMANENT_DATA, True),
    ReasonCode.UNKNOWN_BOOT: (ErrorCategory.PERMANENT_DATA, True),
    ReasonCode.BOOT_NUMBER_MISMATCH: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.UNKNOWN_FIRMWARE: (ErrorCategory.PERMANENT_DATA, True),
    ReasonCode.DB_CONSTRAINT_VIOLATION: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.SEQUENCE_CONFLICT: (ErrorCategory.PERMANENT_DATA, False),
    ReasonCode.STALE_SEQUENCE: (ErrorCategory.ORDERING, False),
}


def category_of(code: ReasonCode) -> ErrorCategory:
    return REASONS[code][0]


def retry_eligible(code: ReasonCode) -> bool:
    return REASONS[code][1]


def initial_status(code: ReasonCode) -> str:
    return "RECORDED" if category_of(code) is ErrorCategory.ORDERING else "QUARANTINED"


# Raised by SQLAlchemy-wrapped calls AND by raw psycopg cursors (set-based SQL runs on
# the driver connection, so its errors are NOT wrapped by SQLAlchemy).
PERMANENT_DB_ERRORS = (
    IntegrityError,
    DataError,
    psycopg.errors.IntegrityError,
    psycopg.errors.DataError,
)


def is_recoverable(exc: BaseException) -> bool:
    """Infrastructure failures worth retrying in place (never quarantined)."""
    if isinstance(exc, OperationalError | InterfaceError | PoolTimeoutError):
        return True
    if isinstance(exc, psycopg.OperationalError | psycopg.InterfaceError):
        return True
    return isinstance(exc, DBAPIError) and exc.connection_invalidated
