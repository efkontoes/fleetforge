"""Kafka ingestion: quarantine metadata, consumer offset ledger, replay audit,
downstream mission projection.

Revision ID: 0004
Revises: 0003
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

REASON_CODES = """(
    'MALFORMED_JSON', 'MALFORMED_PAYLOAD', 'MISSING_FIELD', 'INVALID_FIELD',
    'UNSUPPORTED_SCHEMA_VERSION', 'BATTERY_OUT_OF_RANGE', 'OBSERVED_AT_IN_FUTURE',
    'UNKNOWN_ROBOT', 'UNKNOWN_BOOT', 'BOOT_NUMBER_MISMATCH', 'UNKNOWN_FIRMWARE',
    'DB_CONSTRAINT_VIOLATION', 'STALE_SEQUENCE', 'SEQUENCE_CONFLICT'
)"""

M1_REASON_CODES = """(
    'MALFORMED_JSON', 'MISSING_FIELD', 'INVALID_FIELD', 'UNSUPPORTED_SCHEMA_VERSION',
    'BATTERY_OUT_OF_RANGE', 'OBSERVED_AT_IN_FUTURE',
    'UNKNOWN_ROBOT', 'UNKNOWN_BOOT',
    'STALE_SEQUENCE', 'SEQUENCE_CONFLICT'
)"""


def upgrade() -> None:
    # ------------------------------------------------------------------ quarantine
    # telemetry_rejections becomes the single audit of every event that did not
    # change operational state for a reason other than being a duplicate. Rows
    # carry their transport coordinates and a lifecycle status, so permanent data
    # errors can be inspected, corrected and replayed.
    op.execute(
        """
        ALTER TABLE fleet.telemetry_rejections
            DROP CONSTRAINT telemetry_rejections_reason_code_check,
            ADD CONSTRAINT telemetry_rejections_reason_code_check
                CHECK (reason_code IN """
        + REASON_CODES
        + """),
            ADD COLUMN source TEXT NOT NULL DEFAULT 'http'
                CHECK (source IN ('http', 'kafka', 'replay')),
            ADD COLUMN source_topic TEXT,
            ADD COLUMN source_partition INTEGER,
            ADD COLUMN source_offset BIGINT,
            ADD COLUMN error_category TEXT NOT NULL DEFAULT 'PERMANENT_DATA'
                CHECK (error_category IN ('PERMANENT_DATA', 'ORDERING')),
            ADD COLUMN retry_eligible BOOLEAN NOT NULL DEFAULT false,
            ADD COLUMN status TEXT NOT NULL DEFAULT 'QUARANTINED'
                CHECK (status IN ('QUARANTINED', 'RECORDED', 'REPLAYED', 'RESOLVED', 'DISCARDED')),
            ADD COLUMN consumer_version TEXT,
            ADD COLUMN payload_bytes BYTEA,
            ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            ADD COLUMN resolution JSONB,
            ADD CONSTRAINT telemetry_rejections_kafka_coordinates CHECK (
                source <> 'kafka'
                OR (source_topic IS NOT NULL AND source_partition IS NOT NULL
                    AND source_offset IS NOT NULL)
            )
    """
    )
    # Ordering outcomes are facts about delivery, not data errors: recorded, never retried.
    op.execute("""
        UPDATE fleet.telemetry_rejections
        SET error_category = 'ORDERING', status = 'RECORDED'
        WHERE reason_code IN ('STALE_SEQUENCE')
    """)
    op.execute("""
        UPDATE fleet.telemetry_rejections SET retry_eligible = true
        WHERE reason_code IN ('UNKNOWN_ROBOT', 'UNKNOWN_BOOT')
    """)
    op.execute("""
        CREATE INDEX idx_telemetry_rejections_open
        ON fleet.telemetry_rejections (reason_code, received_at)
        WHERE status = 'QUARANTINED'
    """)
    op.execute("""
        CREATE INDEX idx_telemetry_rejections_source
        ON fleet.telemetry_rejections (source_topic, source_partition, source_offset)
        WHERE source_topic IS NOT NULL
    """)
    op.execute("""
        CREATE VIEW fleet.telemetry_quarantine AS
        SELECT rejection_id, event_id, robot_id, reason_code, error_category, retry_eligible,
               status, source, source_topic, source_partition, source_offset,
               consumer_version, errors, payload, received_at AS detected_at, updated_at,
               resolution
        FROM fleet.telemetry_rejections
        WHERE error_category = 'PERMANENT_DATA'
    """)

    # ------------------------------------------------------------------ offset ledger
    # The projector stores "next offset to process" per partition IN THE SAME
    # TRANSACTION as the state changes it derived from those records. Kafka's own
    # committed offsets are updated afterwards and are only advisory (lag monitoring).
    op.execute("""
        CREATE TABLE fleet.consumer_offsets (
            consumer_group TEXT NOT NULL,
            topic          TEXT NOT NULL,
            partition      INTEGER NOT NULL CHECK (partition >= 0),
            next_offset    BIGINT NOT NULL CHECK (next_offset >= 0),
            updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (consumer_group, topic, partition)
        )
    """)

    # ------------------------------------------------------------------ replay audit
    op.execute("""
        CREATE TABLE fleet.replay_runs (
            replay_id    UUID PRIMARY KEY,
            requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            mode         TEXT NOT NULL
                CHECK (mode IN ('EVENT', 'OFFSET_RANGE', 'QUARANTINE', 'CORRECTION')),
            selector     JSONB NOT NULL,
            dry_run      BOOLEAN NOT NULL,
            operator     TEXT NOT NULL,
            outcomes     JSONB NOT NULL
        )
    """)

    # ------------------------------------------------------------------ downstream
    # Owned by the independent mission-events consumer, not by the Fleet API.
    op.execute("CREATE SCHEMA IF NOT EXISTS downstream")
    op.execute("""
        CREATE TABLE downstream.mission_status (
            mission_id    UUID PRIMARY KEY,
            robot_id      UUID NOT NULL,
            status        TEXT NOT NULL,
            version       BIGINT NOT NULL CHECK (version >= 0),
            last_event_id UUID NOT NULL,
            occurred_at   TIMESTAMPTZ NOT NULL,
            applied_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            events_seen   BIGINT NOT NULL DEFAULT 1
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE downstream.mission_status")
    op.execute("DROP SCHEMA downstream")
    op.execute("DROP TABLE fleet.replay_runs")
    op.execute("DROP TABLE fleet.consumer_offsets")
    op.execute("DROP VIEW fleet.telemetry_quarantine")
    op.execute("DELETE FROM fleet.telemetry_rejections WHERE reason_code NOT IN " + M1_REASON_CODES)
    op.execute(
        """
        ALTER TABLE fleet.telemetry_rejections
            DROP CONSTRAINT telemetry_rejections_kafka_coordinates,
            DROP CONSTRAINT telemetry_rejections_reason_code_check,
            ADD CONSTRAINT telemetry_rejections_reason_code_check
                CHECK (reason_code IN """
        + M1_REASON_CODES
        + """),
            DROP COLUMN source, DROP COLUMN source_topic, DROP COLUMN source_partition,
            DROP COLUMN source_offset, DROP COLUMN error_category, DROP COLUMN retry_eligible,
            DROP COLUMN status, DROP COLUMN consumer_version, DROP COLUMN payload_bytes,
            DROP COLUMN updated_at, DROP COLUMN resolution
    """
    )
