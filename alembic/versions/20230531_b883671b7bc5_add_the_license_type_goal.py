"""Add the license type goal

Revision ID: b883671b7bc5
Revises: a9ed3f76d649
Create Date: 2023-05-31 10:50:32.045821+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "b883671b7bc5"
down_revision = "a9ed3f76d649"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # We need to use an autocommit blcok since the next migration is going to use
    # the new enum value immediately, so we must ensure the value is commited
    # before the next migration runs
    with op.get_context().autocommit_block():
        op.execute(f"ALTER TYPE goals ADD VALUE IF NOT EXISTS 'licenses'")


def downgrade() -> None:
    """There is no way to drop single values from an Enum from postgres"""
