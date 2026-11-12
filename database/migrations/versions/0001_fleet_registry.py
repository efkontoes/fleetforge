"""Fleet registry: firmware contracts, robots, boot sessions.

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS fleet")

    # A firmware contract records the conventions a (model, firmware) pair emits.
    # Milestone 1 only accepts the canonical convention (PERCENT / M); unit
    # conversion for other conventions is deferred to the Silver layer (M2).
    op.execute("""
        CREATE TABLE fleet.firmware_versions (
            model_id      TEXT NOT NULL,
            version       TEXT NOT NULL,
            battery_unit  TEXT NOT NULL CHECK (battery_unit IN ('PERCENT', 'FRACTION')),
            position_unit TEXT NOT NULL CHECK (position_unit IN ('M', 'MM')),
            released_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (model_id, version)
        )
    """)

    # Reference data the application cannot run without lives in a migration,
    # so every environment has exactly the same contracts.
    op.execute("""
        INSERT INTO fleet.firmware_versions (model_id, version, battery_unit, position_unit)
        VALUES ('AMR-100', '1.0.0', 'PERCENT', 'M'),
               ('AMR-100', '1.1.0', 'PERCENT', 'M'),
               ('AMR-200', '2.0.0', 'PERCENT', 'M')
    """)

    op.execute("""
        CREATE TABLE fleet.robots (
            robot_id         UUID PRIMARY KEY,
            robot_model      TEXT NOT NULL,
            firmware_version TEXT NOT NULL,
            registered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            lifecycle_status TEXT NOT NULL DEFAULT 'ACTIVE'
                CHECK (lifecycle_status IN ('ACTIVE', 'MAINTENANCE', 'RETIRED')),
            FOREIGN KEY (robot_model, firmware_version)
                REFERENCES fleet.firmware_versions (model_id, version)
        )
    """)

    # One row per boot session. boot_number is assigned by the server, so the
    # ordering of boots never depends on robot clocks or robot-local storage.
    op.execute("""
        CREATE TABLE fleet.robot_boots (
            boot_id       UUID PRIMARY KEY,
            robot_id      UUID NOT NULL REFERENCES fleet.robots (robot_id),
            boot_number   BIGINT NOT NULL CHECK (boot_number >= 1),
            registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_robot_boots_number UNIQUE (robot_id, boot_number),
            -- Target for the composite FK from robot_latest_state: guarantees a
            -- stored (boot_id, boot_number) pair really belongs to that robot.
            CONSTRAINT uq_robot_boots_identity UNIQUE (robot_id, boot_id, boot_number)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE fleet.robot_boots")
    op.execute("DROP TABLE fleet.robots")
    op.execute("DROP TABLE fleet.firmware_versions")
