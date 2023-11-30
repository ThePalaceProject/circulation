"""Cleanup google anaytics integrations

Revision ID: 0739d5558dda
Revises: 21a65b8f391d
Create Date: 2023-10-19 05:23:00.694886+00:00

Note that this migration was changed for the v13.0.0 release, older migrations
were deleted from the repository history, and this was made the first migration
by changing the down_revision to None.

See: https://alembic.sqlalchemy.org/en/latest/cookbook.html#building-an-up-to-date-database-from-scratch
"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "0739d5558dda"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Remove all references to google analytics providers from the DB"""
    conn = op.get_bind()
    rows = conn.execute(
        "SELECT id from externalintegrations where goal='analytics' and protocol='api.google_analytics_provider';"
    ).all()
    analytics_ids = tuple(r[0] for r in rows)

    if len(analytics_ids):
        conn.execute(
            sa.text(
                "DELETE from externalintegrations_libraries where externalintegration_id IN :id_list;"
            ),
            id_list=analytics_ids,
        )
        conn.execute(
            sa.text(
                "DELETE from configurationsettings where external_integration_id IN :id_list"
            ),
            id_list=analytics_ids,
        )
        conn.execute(
            sa.text("DELETE from externalintegrations where id IN :id_list;"),
            id_list=analytics_ids,
        )


def downgrade() -> None:
    pass
