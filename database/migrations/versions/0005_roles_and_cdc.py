"""Least-privilege roles and logical-replication publications for Debezium.

Roles are created NOLOGIN here (no secrets in migrations). Login and passwords
are provisioned from environment variables by `python -m fleetforge.ops.provision`.

Revision ID: 0005
Revises: 0004
"""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

# role -> connection limit. Limits stop one misbehaving component from exhausting
# max_connections for everyone else.
ROLES = {
    "fleetforge_app": 60,  # Fleet API (4 processes x pool 10 + overflow 5)
    "fleetforge_projector": 20,  # telemetry projector instances
    "fleetforge_mission_consumer": 5,
    "fleetforge_cdc": 5,  # Debezium: 1 replication connection per connector + snapshot
    "fleetforge_readonly": 10,  # diagnostics, exporters, analysts
}

GRANTS = [
    # Fleet API: registry, missions, outbox, and the legacy HTTP telemetry path.
    "GRANT USAGE ON SCHEMA fleet TO fleetforge_app",
    "GRANT SELECT ON fleet.firmware_versions TO fleetforge_app",
    "GRANT SELECT, INSERT ON fleet.robots, fleet.robot_boots TO fleetforge_app",
    "GRANT SELECT, INSERT, UPDATE ON fleet.missions TO fleetforge_app",
    "GRANT SELECT, INSERT ON fleet.mission_events, fleet.outbox_events TO fleetforge_app",
    "GRANT SELECT, INSERT, UPDATE ON fleet.robot_latest_state TO fleetforge_app",
    "GRANT SELECT, INSERT ON fleet.telemetry_rejections TO fleetforge_app",
    # Telemetry projector: owns the read model, the quarantine and its offset ledger.
    "GRANT USAGE ON SCHEMA fleet TO fleetforge_projector",
    "GRANT SELECT ON fleet.robots, fleet.robot_boots, fleet.firmware_versions,"
    " fleet.missions TO fleetforge_projector",
    "GRANT SELECT, INSERT, UPDATE ON fleet.robot_latest_state, fleet.telemetry_rejections,"
    " fleet.consumer_offsets TO fleetforge_projector",
    "GRANT SELECT, INSERT ON fleet.replay_runs TO fleetforge_projector",
    "GRANT SELECT ON fleet.telemetry_quarantine TO fleetforge_projector",
    # Mission-events consumer: its own schema plus its offset ledger rows.
    "GRANT USAGE ON SCHEMA downstream, fleet TO fleetforge_mission_consumer",
    "GRANT SELECT, INSERT, UPDATE ON downstream.mission_status TO fleetforge_mission_consumer",
    "GRANT SELECT, INSERT, UPDATE ON fleet.consumer_offsets TO fleetforge_mission_consumer",
    # Debezium: read the captured tables (snapshot) + REPLICATION attribute below.
    "GRANT USAGE ON SCHEMA fleet TO fleetforge_cdc",
    "GRANT SELECT ON fleet.outbox_events, fleet.firmware_versions TO fleetforge_cdc",
    # Read-only diagnostics.
    "GRANT USAGE ON SCHEMA fleet, downstream TO fleetforge_readonly",
    "GRANT SELECT ON ALL TABLES IN SCHEMA fleet TO fleetforge_readonly",
    "GRANT SELECT ON ALL TABLES IN SCHEMA downstream TO fleetforge_readonly",
    "GRANT pg_monitor TO fleetforge_readonly",  # replication-slot and activity views
]


def upgrade() -> None:
    for role, limit in ROLES.items():
        op.execute(f"""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                    CREATE ROLE {role} NOLOGIN;
                END IF;
            END $$
        """)
        op.execute(f"ALTER ROLE {role} CONNECTION LIMIT {limit}")
    # Requires a superuser (local) or rds_replication membership (AWS RDS).
    op.execute("ALTER ROLE fleetforge_cdc REPLICATION")
    for grant in GRANTS:
        op.execute(grant)

    # Outbox: publish INSERTs only. Retention DELETEs are then invisible to CDC and
    # can never be misread downstream as business events.
    op.execute(
        "CREATE PUBLICATION fleetforge_outbox FOR TABLE fleet.outbox_events"
        " WITH (publish = 'insert')"
    )
    # Reference-data CDC (full insert/update/delete semantics) on firmware contracts.
    # REPLICA IDENTITY FULL puts the complete old row into UPDATE/DELETE events.
    op.execute("ALTER TABLE fleet.firmware_versions REPLICA IDENTITY FULL")
    op.execute("CREATE PUBLICATION fleetforge_reference FOR TABLE fleet.firmware_versions")


def downgrade() -> None:
    op.execute("DROP PUBLICATION IF EXISTS fleetforge_reference")
    op.execute("DROP PUBLICATION IF EXISTS fleetforge_outbox")
    op.execute("ALTER TABLE fleet.firmware_versions REPLICA IDENTITY DEFAULT")
    for role in ROLES:
        op.execute(f"""
            DO $$ BEGIN
                IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                    EXECUTE 'DROP OWNED BY {role}';
                    EXECUTE 'DROP ROLE {role}';
                END IF;
            END $$
        """)
