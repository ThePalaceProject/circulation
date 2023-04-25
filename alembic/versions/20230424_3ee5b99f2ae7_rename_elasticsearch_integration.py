"""Rename elasticsearch integration

Revision ID: 3ee5b99f2ae7
Revises: dac99ae0c6fd
Create Date: 2023-04-24 06:24:45.721475+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "3ee5b99f2ae7"
down_revision = "dac99ae0c6fd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE externalintegrations SET protocol='Opensearch' where protocol='Elasticsearch'"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE externalintegrations SET protocol='Elasticsearch' where protocol='Opensearch'"
    )
