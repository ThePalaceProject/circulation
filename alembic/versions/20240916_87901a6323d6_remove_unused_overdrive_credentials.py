"""Remove unused Overdrive credentials

Revision ID: 87901a6323d6
Revises: 350a29bf0ff0
Create Date: 2024-09-16 19:54:56.986491+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "87901a6323d6"
down_revision = "350a29bf0ff0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Clean out the old unused credential objects, so they are not hanging around and causing confusion
    # in the future.
    op.execute(
        "DELETE from credentials WHERE type is NULL and patron_id is NULL and data_source_id in "
        "(SELECT id from datasources where name = 'Overdrive')"
    )


def downgrade() -> None:
    pass
