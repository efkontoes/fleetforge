"""CDC heartbeat table: keeps replication slots advancing when captured tables are quiet.

Found by measurement: after a benchmark wrote ~1 GB of telemetry WAL, both slots
pinned 531 MB. Debezium only confirms an LSN when it receives a change from its
publication, and the outbox/firmware tables were quiet, so the busy WAL around
them could never be released. Each connector's heartbeat.action.query INSERTs a
row here every 10 s; the row flows through the publication and moves the slot.
INSERT, not UPSERT: the outbox publication publishes inserts only.

Revision ID: 0007
Revises: 0006
"""

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE fleet.cdc_heartbeat (
            beat_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            connector TEXT NOT NULL,
            beat_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX idx_cdc_heartbeat_beat_at ON fleet.cdc_heartbeat (beat_at)")
    op.execute("GRANT INSERT, SELECT ON fleet.cdc_heartbeat TO fleetforge_cdc")
    op.execute("GRANT SELECT ON fleet.cdc_heartbeat TO fleetforge_readonly")
    op.execute("ALTER PUBLICATION fleetforge_outbox ADD TABLE fleet.cdc_heartbeat")
    op.execute("ALTER PUBLICATION fleetforge_reference ADD TABLE fleet.cdc_heartbeat")


def downgrade() -> None:
    op.execute("ALTER PUBLICATION fleetforge_reference DROP TABLE fleet.cdc_heartbeat")
    op.execute("ALTER PUBLICATION fleetforge_outbox DROP TABLE fleet.cdc_heartbeat")
    op.execute("DROP TABLE fleet.cdc_heartbeat")
