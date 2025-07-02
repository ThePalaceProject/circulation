"""Update axis datasource to boundless

Revision ID: 5e33dc35b5b9
Revises: bc471d8a83fb
Create Date: 2025-06-25 16:06:33.738280+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5e33dc35b5b9"
down_revision = "bc471d8a83fb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Lock the datasources table, to prevent concurrent modifications while
    # the migration transaction is in progress. That should prevent us from
    # ending up with two datasources, one with the old name and one with the
    # new name.
    op.execute(sa.text("LOCK TABLE datasources IN SHARE ROW EXCLUSIVE MODE"))
    op.execute(
        sa.text("UPDATE datasources SET name = 'Boundless' WHERE name = 'Axis 360'")
    )


def downgrade() -> None:
    op.execute(sa.text("LOCK TABLE datasources IN SHARE ROW EXCLUSIVE MODE"))
    op.execute(
        sa.text("UPDATE datasources SET name = 'Axis 360' WHERE name = 'Boundless'")
    )
