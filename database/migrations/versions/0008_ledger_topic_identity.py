"""Offset-ledger identity and fail-closed recovery; mission-event quarantine.

Found by the Milestone 2 audit: the ledger stored only (group, topic, partition,
next_offset). If a topic is deleted and recreated, its offsets restart at 0 while
the ledger still says, for example, 50 000. On restart the projector would then
either seek past the end (librdkafka silently resets to the earliest offset) and
mark every new record below 50 000 as SKIPPED, or, if the new topic had already
grown past 50 000, start in the middle of it. Both lose data silently.

* kafka_topic_id records Kafka's topic UUID, which changes when a topic is
  recreated even if the name does not. A mismatch stops the consumer.
* fleet.replay_runs gains mode LEDGER_RESET: the only sanctioned way to move a
  ledger backwards or across a gap is an audited operator command.
* downstream.mission_event_quarantine: malformed mission events used to be only
  logged and counted; they now keep their Kafka coordinates and bytes.
* Least privilege (audit): the projector and the mission consumer could each
  rewrite the other's ledger rows (table-level grants). Row-level security now
  confines each to its own topics. The projector's unused SELECT on fleet.missions
  is revoked.

Revision ID: 0008
Revises: 0007
"""

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None

MODES_OLD = "'EVENT', 'OFFSET_RANGE', 'QUARANTINE', 'CORRECTION'"
MODES_NEW = MODES_OLD + ", 'LEDGER_RESET'"
POLICIES = [
    ("ledger_projector", "fleetforge_projector", "topic LIKE 'telemetry.%'"),
    ("ledger_mission_consumer", "fleetforge_mission_consumer", "topic LIKE 'fleet.%-events.v%'"),
]


def upgrade() -> None:
    op.execute("ALTER TABLE fleet.consumer_offsets ADD COLUMN kafka_topic_id TEXT")
    op.execute("ALTER TABLE fleet.replay_runs DROP CONSTRAINT replay_runs_mode_check")
    op.execute(f"ALTER TABLE fleet.replay_runs ADD CONSTRAINT replay_runs_mode_check"
               f" CHECK (mode IN ({MODES_NEW}))")  # fmt: skip
    op.execute("""
        CREATE TABLE downstream.mission_event_quarantine (
            quarantine_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source_topic     TEXT NOT NULL,
            source_partition INTEGER NOT NULL,
            source_offset    BIGINT NOT NULL,
            payload_bytes    BYTEA,
            error            TEXT NOT NULL,
            received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (source_topic, source_partition, source_offset)
        )
    """)
    op.execute("GRANT SELECT, INSERT ON downstream.mission_event_quarantine"
               " TO fleetforge_mission_consumer")  # fmt: skip
    op.execute("GRANT SELECT ON downstream.mission_event_quarantine TO fleetforge_readonly")

    op.execute("REVOKE SELECT ON fleet.missions FROM fleetforge_projector")
    # Superusers and the table owner bypass RLS (migrations, tests, operator tools).
    op.execute("ALTER TABLE fleet.consumer_offsets ENABLE ROW LEVEL SECURITY")
    for name, role, predicate in POLICIES:
        op.execute(f"CREATE POLICY {name} ON fleet.consumer_offsets TO {role}"
                   f" USING ({predicate}) WITH CHECK ({predicate})")  # fmt: skip
    op.execute("CREATE POLICY ledger_readonly ON fleet.consumer_offsets FOR SELECT"
               " TO fleetforge_readonly USING (true)")  # fmt: skip


def downgrade() -> None:
    op.execute("DROP POLICY IF EXISTS ledger_readonly ON fleet.consumer_offsets")
    for name, _, _ in POLICIES:
        op.execute(f"DROP POLICY IF EXISTS {name} ON fleet.consumer_offsets")
    op.execute("ALTER TABLE fleet.consumer_offsets DISABLE ROW LEVEL SECURITY")
    op.execute("GRANT SELECT ON fleet.missions TO fleetforge_projector")
    op.execute("DROP TABLE downstream.mission_event_quarantine")
    op.execute("DELETE FROM fleet.replay_runs WHERE mode = 'LEDGER_RESET'")
    op.execute("ALTER TABLE fleet.replay_runs DROP CONSTRAINT replay_runs_mode_check")
    op.execute(f"ALTER TABLE fleet.replay_runs ADD CONSTRAINT replay_runs_mode_check"
               f" CHECK (mode IN ({MODES_OLD}))")  # fmt: skip
    op.execute("ALTER TABLE fleet.consumer_offsets DROP COLUMN kafka_topic_id")
