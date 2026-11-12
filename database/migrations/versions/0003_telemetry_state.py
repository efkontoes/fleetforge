"""Latest accepted robot state and telemetry rejection audit.

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

REASON_CODES = """(
    'MALFORMED_JSON', 'MISSING_FIELD', 'INVALID_FIELD', 'UNSUPPORTED_SCHEMA_VERSION',
    'BATTERY_OUT_OF_RANGE', 'OBSERVED_AT_IN_FUTURE',
    'UNKNOWN_ROBOT', 'UNKNOWN_BOOT',
    'STALE_SEQUENCE', 'SEQUENCE_CONFLICT'
)"""


def upgrade() -> None:
    op.execute("""
        CREATE TABLE fleet.robot_latest_state (
            robot_id        UUID PRIMARY KEY REFERENCES fleet.robots (robot_id),
            boot_id         UUID NOT NULL,
            boot_number     BIGINT NOT NULL,
            sequence_number BIGINT NOT NULL CHECK (sequence_number >= 0),
            event_id        UUID NOT NULL,
            schema_version  INTEGER NOT NULL,
            observed_at     TIMESTAMPTZ NOT NULL,
            received_at     TIMESTAMPTZ NOT NULL,
            -- NaN sorts above every number in PostgreSQL, so these checks also reject NaN.
            battery_pct     DOUBLE PRECISION NOT NULL CHECK (battery_pct BETWEEN 0 AND 100),
            x_m             DOUBLE PRECISION NOT NULL
                CHECK (x_m > '-Infinity' AND x_m < 'Infinity'),
            y_m             DOUBLE PRECISION NOT NULL
                CHECK (y_m > '-Infinity' AND y_m < 'Infinity'),
            state_version   BIGINT NOT NULL DEFAULT 1 CHECK (state_version >= 1),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            FOREIGN KEY (robot_id, boot_id, boot_number)
                REFERENCES fleet.robot_boots (robot_id, boot_id, boot_number)
        )
    """)

    # One row per (rejected event, reason). dedup_key is the event_id when the
    # payload carried a valid one, otherwise a hash of the raw body, so that a
    # producer retrying a rejected event does not multiply audit rows.
    op.execute(f"""
        CREATE TABLE fleet.telemetry_rejections (
            rejection_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            dedup_key    TEXT NOT NULL,
            event_id     UUID,
            robot_id     UUID,
            reason_code  TEXT NOT NULL CHECK (reason_code IN {REASON_CODES}),
            errors       JSONB NOT NULL,
            payload      JSONB NOT NULL,
            received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_telemetry_rejections_dedup UNIQUE (dedup_key, reason_code)
        )
    """)
    op.execute("""
        CREATE INDEX idx_telemetry_rejections_robot
        ON fleet.telemetry_rejections (robot_id, received_at)
    """)
    op.execute("""
        CREATE INDEX idx_telemetry_rejections_reason
        ON fleet.telemetry_rejections (reason_code, received_at)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE fleet.telemetry_rejections")
    op.execute("DROP TABLE fleet.robot_latest_state")
