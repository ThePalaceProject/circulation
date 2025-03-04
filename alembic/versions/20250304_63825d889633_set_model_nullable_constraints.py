"""Set model nullable constraints

Revision ID: 63825d889633
Revises: 704dd5322783
Create Date: 2025-03-04 18:35:14.704827+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "63825d889633"
down_revision = "704dd5322783"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "classifications", "identifier_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "classifications", "subject_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "classifications", "data_source_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "classifications", "weight", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column("datasources", "name", existing_type=sa.VARCHAR(), nullable=False)
    op.alter_column("genres", "name", existing_type=sa.VARCHAR(), nullable=False)
    op.alter_column(
        "identifiers", "type", existing_type=sa.VARCHAR(length=64), nullable=False
    )
    op.alter_column(
        "identifiers", "identifier", existing_type=sa.VARCHAR(), nullable=False
    )
    op.alter_column("subjects", "type", existing_type=sa.VARCHAR(), nullable=False)


def downgrade() -> None:
    op.alter_column("subjects", "type", existing_type=sa.VARCHAR(), nullable=True)
    op.alter_column(
        "identifiers", "identifier", existing_type=sa.VARCHAR(), nullable=True
    )
    op.alter_column(
        "identifiers", "type", existing_type=sa.VARCHAR(length=64), nullable=True
    )
    op.alter_column("genres", "name", existing_type=sa.VARCHAR(), nullable=True)
    op.alter_column("datasources", "name", existing_type=sa.VARCHAR(), nullable=True)
    op.alter_column(
        "classifications", "weight", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "classifications", "data_source_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "classifications", "subject_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "classifications", "identifier_id", existing_type=sa.INTEGER(), nullable=True
    )
