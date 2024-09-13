"""Update existing MarcExporter protocols

Revision ID: 350a29bf0ff0
Revises: 7a2fcaac8b63
Create Date: 2024-09-05 16:04:45.789665+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "350a29bf0ff0"
down_revision = "7a2fcaac8b63"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Update existing MarcExporter protocols to use the new protocol name
    op.execute(
        "UPDATE integration_configurations SET protocol = 'MarcExporter' "
        "WHERE protocol = 'MARCExporter' and goal = 'CATALOG_GOAL'"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE integration_configurations SET protocol = 'MARCExporter' "
        "WHERE protocol = 'MarcExporter' and goal = 'CATALOG_GOAL'"
    )
