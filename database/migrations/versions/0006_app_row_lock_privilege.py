"""Let the Fleet API take row locks on robots.

Boot registration serializes per robot with SELECT ... FOR NO KEY UPDATE, and
PostgreSQL requires UPDATE privilege on at least one column to lock a row. Found by
running the API as fleetforge_app (permission denied); the column grant is the
smallest privilege that allows the lock.

Revision ID: 0006
Revises: 0005
"""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("GRANT UPDATE (lifecycle_status) ON fleet.robots TO fleetforge_app")


def downgrade() -> None:
    op.execute("REVOKE UPDATE (lifecycle_status) ON fleet.robots FROM fleetforge_app")
