"""Drop cachedfeeds

Revision ID: 7fceb9488bc6
Revises: 0739d5558dda
Create Date: 2023-10-20 10:55:49.709820+00:00

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "7fceb9488bc6"
down_revision = "0739d5558dda"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_cachedfeeds_lane_id", table_name="cachedfeeds")
    op.drop_index("ix_cachedfeeds_library_id", table_name="cachedfeeds")
    op.drop_index(
        "ix_cachedfeeds_library_id_lane_id_type_facets_pagination",
        table_name="cachedfeeds",
    )
    op.drop_index("ix_cachedfeeds_timestamp", table_name="cachedfeeds")
    op.drop_index("ix_cachedfeeds_work_id", table_name="cachedfeeds")
    op.drop_table("cachedfeeds")
    op.drop_column("works", "simple_opds_entry")
    op.drop_column("works", "verbose_opds_entry")
    op.drop_column("editions", "simple_opds_entry")


def downgrade() -> None:
    op.add_column(
        "works",
        sa.Column(
            "verbose_opds_entry", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "works",
        sa.Column(
            "simple_opds_entry", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "editions",
        sa.Column(
            "simple_opds_entry", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.create_table(
        "cachedfeeds",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("lane_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.Column(
            "timestamp",
            postgresql.TIMESTAMP(timezone=True),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("type", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("unique_key", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("facets", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("pagination", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("content", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("library_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.Column("work_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.ForeignKeyConstraint(
            ["lane_id"], ["lanes.id"], name="cachedfeeds_lane_id_fkey"
        ),
        sa.ForeignKeyConstraint(
            ["library_id"], ["libraries.id"], name="cachedfeeds_library_id_fkey"
        ),
        sa.ForeignKeyConstraint(
            ["work_id"], ["works.id"], name="cachedfeeds_work_id_fkey"
        ),
        sa.PrimaryKeyConstraint("id", name="cachedfeeds_pkey"),
    )
    op.create_index("ix_cachedfeeds_work_id", "cachedfeeds", ["work_id"], unique=False)
    op.create_index(
        "ix_cachedfeeds_timestamp", "cachedfeeds", ["timestamp"], unique=False
    )
    op.create_index(
        "ix_cachedfeeds_library_id_lane_id_type_facets_pagination",
        "cachedfeeds",
        ["library_id", "lane_id", "type", "facets", "pagination"],
        unique=False,
    )
    op.create_index(
        "ix_cachedfeeds_library_id", "cachedfeeds", ["library_id"], unique=False
    )
    op.create_index("ix_cachedfeeds_lane_id", "cachedfeeds", ["lane_id"], unique=False)
