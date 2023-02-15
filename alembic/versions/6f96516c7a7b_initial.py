"""initial

Revision ID: 6f96516c7a7b
Revises:
Create Date: 2022-10-06 06:50:45.512958+00:00

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "6f96516c7a7b"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Remove some tables that are hanging around in some instances
    # These have been removed from code some time ago
    op.execute(
        "ALTER TABLE IF EXISTS libraryalias DROP CONSTRAINT IF EXISTS ix_libraryalias_language"
    )
    op.execute(
        "ALTER TABLE IF EXISTS libraryalias DROP CONSTRAINT IF EXISTS ix_libraryalias_library_id"
    )
    op.execute(
        "ALTER TABLE IF EXISTS libraryalias DROP CONSTRAINT IF EXISTS ix_libraryalias_name"
    )
    op.execute("DROP TABLE IF EXISTS libraryalias")

    op.execute(
        "ALTER TABLE IF EXISTS complaints DROP CONSTRAINT IF EXISTS ix_complaints_license_pool_id"
    )
    op.execute(
        "ALTER TABLE IF EXISTS complaints DROP CONSTRAINT IF EXISTS ix_complaints_source"
    )
    op.execute(
        "ALTER TABLE IF EXISTS complaints DROP CONSTRAINT IF EXISTS ix_complaints_type"
    )
    op.execute("DROP TABLE IF EXISTS complaints")


def downgrade() -> None:
    # No need to re-add these tables, since they are long gone
    ...
