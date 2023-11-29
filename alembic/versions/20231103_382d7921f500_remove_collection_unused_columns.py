"""Remove collection unused columns.

Revision ID: 382d7921f500
Revises: e4b120a8d1d5
Create Date: 2023-11-03 00:09:10.761425+00:00

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "382d7921f500"
down_revision = "e4b120a8d1d5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("collections_libraries")
    op.drop_column("collections", "external_integration_id")
    op.drop_column("collections", "name")
    op.drop_column("collections", "external_account_id")


def downgrade() -> None:
    op.add_column(
        "collections",
        sa.Column(
            "external_account_id", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "collections",
        sa.Column("name", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "collections",
        sa.Column(
            "external_integration_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
    )
    op.create_table(
        "collections_libraries",
        sa.Column("collection_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.Column("library_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.UniqueConstraint(
            "collection_id",
            "library_id",
            name="collections_libraries_collection_id_library_id_key",
        ),
    )
