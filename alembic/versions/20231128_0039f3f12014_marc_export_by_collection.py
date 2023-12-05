"""MARC Export by collection.

Revision ID: 0039f3f12014
Revises: 1c14468b74ce
Create Date: 2023-11-28 20:19:55.520740+00:00

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "0039f3f12014"
down_revision = "1c14468b74ce"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "marcfiles",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("library_id", sa.Integer(), nullable=True),
        sa.Column("collection_id", sa.Integer(), nullable=True),
        sa.Column("key", sa.Unicode(), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("since", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["collection_id"],
            ["collections.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["library_id"],
            ["libraries.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_marcfiles_collection_id"), "marcfiles", ["collection_id"], unique=False
    )
    op.create_index(
        op.f("ix_marcfiles_created"), "marcfiles", ["created"], unique=False
    )
    op.create_index(
        op.f("ix_marcfiles_library_id"), "marcfiles", ["library_id"], unique=False
    )
    op.add_column(
        "collections", sa.Column("export_marc_records", sa.Boolean(), nullable=True)
    )
    op.execute("UPDATE collections SET export_marc_records = 'f'")
    op.alter_column("collections", "export_marc_records", nullable=False)


def downgrade() -> None:
    op.drop_column("collections", "export_marc_records")
    op.drop_index(op.f("ix_marcfiles_library_id"), table_name="marcfiles")
    op.drop_index(op.f("ix_marcfiles_created"), table_name="marcfiles")
    op.drop_index(op.f("ix_marcfiles_collection_id"), table_name="marcfiles")
    op.drop_table("marcfiles")
