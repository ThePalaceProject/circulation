"""Add periodical tables

Revision ID: 01c829c141ba
Revises: 5ca948100902
Create Date: 2026-11-17 17:09:11.137683+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "01c829c141ba"
down_revision = "5ca948100902"
branch_labels = None
depends_on = None


# The kinds of serial we know about. This type is created with the
# periodicals table and dropped with it.
SERIAL_TYPE_ENUM = sa.Enum(
    "magazine", "journal", "newspaper", "periodical", name="serialtype"
)


def upgrade() -> None:
    # Everything here is additive: two new tables, and three new columns on
    # editions that are all nullable with no default. The previous release
    # does not know about any of it and keeps working, and nothing needs
    # backfilling.
    op.create_table(
        "periodicals",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("identifier_id", sa.Integer(), nullable=True),
        sa.Column("serial_type", SERIAL_TYPE_ENUM, nullable=True),
        sa.Column("title", sa.Unicode(), nullable=True),
        sa.Column("sort_title", sa.Unicode(), nullable=True),
        sa.Column("publisher", sa.Unicode(), nullable=True),
        sa.Column("language", sa.Unicode(), nullable=True),
        sa.Column("description", sa.Unicode(), nullable=True),
        sa.Column("cover_id", sa.Integer(), nullable=True),
        sa.Column("data_source_id", sa.Integer(), nullable=True),
        sa.Column("created", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["identifier_id"], ["identifiers.id"]),
        sa.ForeignKeyConstraint(["data_source_id"], ["datasources.id"]),
        sa.ForeignKeyConstraint(
            ["cover_id"], ["resources.id"], name="fk_periodicals_cover_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        # The identifier is the periodical's identity, so there can only be one
        # row per identifier. Nulls do not collide in Postgres, so any number of
        # periodicals may have no identifier at all.
        sa.UniqueConstraint("identifier_id"),
    )
    op.create_index(
        op.f("ix_periodicals_data_source_id"), "periodicals", ["data_source_id"]
    )
    op.create_index(op.f("ix_periodicals_cover_id"), "periodicals", ["cover_id"])
    # A periodical with no identifier is identified by its data source and its
    # sort title instead, so that pair is unique among those rows. Rows that do
    # have an identifier are left out: the unique constraint above already
    # covers them, and their data source is not part of their identity.
    op.create_index(
        "ix_periodicals_data_source_id_sort_title",
        "periodicals",
        ["data_source_id", "sort_title"],
        unique=True,
        postgresql_where=sa.text("identifier_id IS NULL"),
    )

    op.create_table(
        "periodical_volumes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("periodical_id", sa.Integer(), nullable=False),
        sa.Column("label", sa.Unicode(), nullable=True),
        sa.Column("position", sa.Integer(), nullable=True),
        sa.Column("created", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["periodical_id"], ["periodicals.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_periodical_volumes_periodical_id"),
        "periodical_volumes",
        ["periodical_id"],
    )

    op.add_column("editions", sa.Column("periodical_id", sa.Integer(), nullable=True))
    op.add_column(
        "editions", sa.Column("periodical_volume_id", sa.Integer(), nullable=True)
    )
    op.add_column("editions", sa.Column("issue_position", sa.Float(), nullable=True))
    op.create_foreign_key(
        "editions_periodical_id_fkey",
        "editions",
        "periodicals",
        ["periodical_id"],
        ["id"],
    )
    op.create_foreign_key(
        "editions_periodical_volume_id_fkey",
        "editions",
        "periodical_volumes",
        ["periodical_volume_id"],
        ["id"],
    )
    op.create_index(op.f("ix_editions_periodical_id"), "editions", ["periodical_id"])
    op.create_index(
        op.f("ix_editions_periodical_volume_id"), "editions", ["periodical_volume_id"]
    )
    # A volume only means anything together with its periodical. Checking this
    # costs nothing here: all three columns were added moments ago and are NULL
    # on every existing row, so there is nothing for the check to scan through.
    op.create_check_constraint(
        "ck_editions_periodical_volume_requires_periodical",
        "editions",
        "periodical_volume_id IS NULL OR periodical_id IS NOT NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_editions_periodical_volume_requires_periodical", "editions", type_="check"
    )
    op.drop_index(op.f("ix_editions_periodical_volume_id"), table_name="editions")
    op.drop_index(op.f("ix_editions_periodical_id"), table_name="editions")
    op.drop_constraint(
        "editions_periodical_volume_id_fkey", "editions", type_="foreignkey"
    )
    op.drop_constraint("editions_periodical_id_fkey", "editions", type_="foreignkey")
    op.drop_column("editions", "issue_position")
    op.drop_column("editions", "periodical_volume_id")
    op.drop_column("editions", "periodical_id")

    op.drop_index(
        op.f("ix_periodical_volumes_periodical_id"), table_name="periodical_volumes"
    )
    op.drop_table("periodical_volumes")

    op.drop_index("ix_periodicals_data_source_id_sort_title", table_name="periodicals")
    op.drop_index(op.f("ix_periodicals_cover_id"), table_name="periodicals")
    op.drop_index(op.f("ix_periodicals_data_source_id"), table_name="periodicals")
    op.drop_table("periodicals")

    # Dropping the tables leaves the enum type behind, so remove it by hand.
    SERIAL_TYPE_ENUM.drop(op.get_bind(), checkfirst=False)
