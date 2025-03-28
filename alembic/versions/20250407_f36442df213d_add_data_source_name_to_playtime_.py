"""Add data_source_name to playtime entries and summaries

Revision ID: f36442df213d
Revises: df27b4867e56
Create Date: 2025-04-07 21:00:49.422754+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.sqlalchemy.model.time_tracking import PlaytimeEntry, PlaytimeSummary

# revision identifiers, used by Alembic.
revision = "f36442df213d"
down_revision = "df27b4867e56"
branch_labels = None
depends_on = None


def upgrade() -> None:
    session: Session = Session(bind=op.get_bind())
    conn = session.connection()

    op.add_column(
        "playtime_entries",
        sa.Column("data_source_name", sa.String(), nullable=False, default="Unknown"),
    )
    for p in session.scalars(
        select(PlaytimeEntry).where(PlaytimeEntry.collection_id is not None)
    ).all():
        p.data_source_name = p.collection.data_source.name

    op.add_column(
        "playtime_summaries",
        sa.Column("data_source_name", sa.String(), nullable=False, default="Unknown"),
    )

    for p in session.scalars(
        select(PlaytimeSummary).where(PlaytimeSummary.collection_id is not None)
    ).all():
        p.data_source_name = p.collection.data_source.name

    session.commit()

    op.alter_column(
        "playtime_summaries",
        "data_source_name",
        existing_type=sa.String(),
        nullable=False,
    )
    op.alter_column(
        "playtime_entries",
        "data_source_name",
        existing_type=sa.String(),
        nullable=False,
    )


def downgrade() -> None:
    op.drop_column("playtime_entries", "data_source_name")
    op.drop_column("playtime_summaries", "data_source_name")
