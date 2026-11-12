"""Missions, auditable mission transitions, transactional outbox.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

MISSION_STATUSES = "('PENDING', 'ASSIGNED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')"


def upgrade() -> None:
    op.execute(f"""
        CREATE TABLE fleet.missions (
            mission_id UUID PRIMARY KEY,
            robot_id   UUID NOT NULL REFERENCES fleet.robots (robot_id),
            status     TEXT NOT NULL CHECK (status IN {MISSION_STATUSES}),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            version    BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0)
        )
    """)
    op.execute("CREATE INDEX idx_missions_robot_status ON fleet.missions (robot_id, status)")
    op.execute("CREATE INDEX idx_missions_updated ON fleet.missions (updated_at)")
    # A robot can execute at most one mission at a time. Enforced by the
    # database, so two concurrent assignments cannot both succeed.
    op.execute("""
        CREATE UNIQUE INDEX uq_missions_one_active_per_robot
        ON fleet.missions (robot_id)
        WHERE status IN ('ASSIGNED', 'RUNNING')
    """)

    # One row per mission version: the audit trail of every transition.
    # event_id is the client's idempotency key and equals the outbox event_id.
    op.execute(f"""
        CREATE TABLE fleet.mission_events (
            event_id        UUID PRIMARY KEY,
            mission_id      UUID NOT NULL REFERENCES fleet.missions (mission_id),
            from_status     TEXT CHECK (from_status IN {MISSION_STATUSES}),
            to_status       TEXT NOT NULL CHECK (to_status IN {MISSION_STATUSES}),
            mission_version BIGINT NOT NULL,
            occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_mission_events_version UNIQUE (mission_id, mission_version)
        )
    """)

    # Column meanings match Debezium's Outbox Event Router (id, aggregatetype,
    # aggregateid, type, payload); the router's field names are configurable.
    op.execute("""
        CREATE TABLE fleet.outbox_events (
            event_id       UUID PRIMARY KEY,
            aggregate_type TEXT NOT NULL,
            aggregate_id   UUID NOT NULL,
            event_type     TEXT NOT NULL,
            payload        JSONB NOT NULL,
            created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX idx_outbox_created ON fleet.outbox_events (created_at)")


def downgrade() -> None:
    op.execute("DROP TABLE fleet.outbox_events")
    op.execute("DROP TABLE fleet.mission_events")
    op.execute("DROP TABLE fleet.missions")
