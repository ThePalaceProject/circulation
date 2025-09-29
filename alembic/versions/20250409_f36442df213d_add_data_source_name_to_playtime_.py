"""Add data_source_name to playtime entries and summaries

Revision ID: f36442df213d
Revises: b96d67e65177
Create Date: 2025-04-09 12:00:49.422754+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f36442df213d"
down_revision = "b96d67e65177"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    unknown_data_source_name = "Unknown"

    # add columns
    op.add_column(
        "playtime_entries",
        sa.Column("data_source_name", sa.String(), nullable=True),
    )

    op.add_column(
        "playtime_summaries",
        sa.Column("data_source_name", sa.String(), nullable=True),
    )

    # update existing playtime entries with associated data source name where available.
    conn.execute(
        sa.text(
            "update playtime_entries pe set data_source_name = ic.settings->>'data_source' "
            "from collections c, integration_configurations ic "
            "where pe.collection_id = c.id and  c.integration_configuration_id = ic.id and "
            "ic.settings->'data_source' is not null"
        )
    )

    # Add default value where data_source_name could not be determined.
    conn.execute(
        sa.text(
            f"update playtime_entries set data_source_name = '{unknown_data_source_name}' "
            f"where data_source_name is null"
        )
    )

    # update existing playtime summaries with associated data source name where available.

    conn.execute(
        sa.text(
            "update playtime_summaries ps set data_source_name = ic.settings->>'data_source' "
            "from collections c, integration_configurations ic "
            "where ps.collection_id = c.id and  c.integration_configuration_id = ic.id and "
            "ic.settings->'data_source' is not null"
        )
    )

    # Add default value where data_source_name could not be determined.
    conn.execute(
        sa.text(
            f"update playtime_summaries set data_source_name = '{unknown_data_source_name}' "
            f"where data_source_name is null"
        )
    )

    # make both columns non-nullable
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
