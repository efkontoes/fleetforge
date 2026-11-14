"""Parked records: registry uncertainty is never recorded as malformed data.

A schema id the registry keeps answering 404 for (beyond the projector's hold window)
is not proof that the record is invalid: a reloading registry answers 404 for ids it
holds (ADR 0016). Such records are kept in telemetry_rejections with reason
UNRESOLVED_SCHEMA_ID, category UNRESOLVED and status PARKED: bytes and Kafka
coordinates are stored in the transaction that advances the ledger, they are
replayable, and they stay out of fleet.telemetry_quarantine (PERMANENT_DATA only).

Revision ID: 0009
Revises: 0008
"""

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None

REASONS_OLD = """'MALFORMED_JSON', 'MALFORMED_PAYLOAD', 'MISSING_FIELD', 'INVALID_FIELD',
    'UNSUPPORTED_SCHEMA_VERSION', 'BATTERY_OUT_OF_RANGE', 'OBSERVED_AT_IN_FUTURE',
    'UNKNOWN_ROBOT', 'UNKNOWN_BOOT', 'BOOT_NUMBER_MISMATCH', 'UNKNOWN_FIRMWARE',
    'DB_CONSTRAINT_VIOLATION', 'STALE_SEQUENCE', 'SEQUENCE_CONFLICT'"""
REASONS_NEW = REASONS_OLD + ", 'UNRESOLVED_SCHEMA_ID'"
CATEGORIES_OLD = "'PERMANENT_DATA', 'ORDERING'"
CATEGORIES_NEW = CATEGORIES_OLD + ", 'UNRESOLVED'"
STATUSES_OLD = "'QUARANTINED', 'RECORDED', 'REPLAYED', 'RESOLVED', 'DISCARDED'"
STATUSES_NEW = STATUSES_OLD + ", 'PARKED'"


def _checks(reasons: str, categories: str, statuses: str) -> None:
    op.execute(f"""
        ALTER TABLE fleet.telemetry_rejections
            DROP CONSTRAINT telemetry_rejections_reason_code_check,
            ADD CONSTRAINT telemetry_rejections_reason_code_check
                CHECK (reason_code IN ({reasons})),
            DROP CONSTRAINT telemetry_rejections_error_category_check,
            ADD CONSTRAINT telemetry_rejections_error_category_check
                CHECK (error_category IN ({categories})),
            DROP CONSTRAINT telemetry_rejections_status_check,
            ADD CONSTRAINT telemetry_rejections_status_check
                CHECK (status IN ({statuses}))
    """)


def upgrade() -> None:
    _checks(REASONS_NEW, CATEGORIES_NEW, STATUSES_NEW)
    op.execute("""
        CREATE INDEX idx_telemetry_rejections_parked
        ON fleet.telemetry_rejections (reason_code, received_at)
        WHERE status = 'PARKED'
    """)


def downgrade() -> None:
    op.execute("DROP INDEX fleet.idx_telemetry_rejections_parked")
    # The old schema can only call such rows malformed, which is what this migration
    # exists to prevent. Refuse instead: replay or discard them first.
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM fleet.telemetry_rejections
                       WHERE reason_code = 'UNRESOLVED_SCHEMA_ID') THEN
                RAISE EXCEPTION 'parked records exist: replay or discard them before downgrading';
            END IF;
        END $$
    """)
    _checks(REASONS_OLD, CATEGORIES_OLD, STATUSES_OLD)
