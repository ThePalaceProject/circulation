"""Add loan_identifier column to playtime tables.

Revision ID: 7a2fcaac8b63
Revises: 7ba553f3f80d
Create Date: 2024-08-21 23:23:48.085451+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm.session import Session

# revision identifiers, used by Alembic.
revision = "7a2fcaac8b63"
down_revision = "7ba553f3f80d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    session = Session(bind=op.get_bind())
    conn = session.connection()

    op.add_column(
        "playtime_entries",
        sa.Column("loan_identifier", sa.String(length=40), nullable=True, default=""),
    )

    op.add_column(
        "playtime_summaries",
        sa.Column("loan_identifier", sa.String(length=40), nullable=True, default=""),
    )

    # Migrate the existing playtime records before we set the new columns to not nullable.
    conn.execute(sa.text("UPDATE playtime_entries SET loan_identifier = ''"))
    conn.execute(sa.text("UPDATE playtime_summaries SET loan_identifier = ''"))

    op.alter_column(
        "playtime_entries",
        "loan_identifier",
        existing_type=sa.String(length=40),
        nullable=False,
    )
    op.alter_column(
        "playtime_summaries",
        "loan_identifier",
        existing_type=sa.String(length=40),
        nullable=False,
    )

    op.drop_constraint("unique_playtime_summary", "playtime_summaries", type_="unique")

    op.create_unique_constraint(
        "unique_playtime_summary",
        "playtime_summaries",
        [
            "timestamp",
            "identifier_str",
            "collection_name",
            "library_name",
            "loan_identifier",
        ],
    )


def downgrade() -> None:
    op.drop_column("playtime_entries", "loan_identifier")

    op.drop_constraint("unique_playtime_summary", "playtime_summaries", type_="unique")

    op.drop_column("playtime_summaries", "loan_identifier")

    op.create_unique_constraint(
        "unique_playtime_summary",
        "playtime_summaries",
        ["timestamp", "identifier_str", "collection_name", "library_name"],
    )
