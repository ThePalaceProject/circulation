"""Remove sirsi library id

Revision ID: 23795e50c915
Revises: a5ee359c2d31
Create Date: 2026-03-19 23:55:41.909306+00:00

"""

import sqlalchemy as sa
from alembic import op

from palace.manager.util.migration.helpers import migration_logger

# revision identifiers, used by Alembic.
revision = "23795e50c915"
down_revision = "a5ee359c2d31"
branch_labels = None
depends_on = None

log = migration_logger(revision)


def upgrade() -> None:
    conn = op.get_bind()

    # Log the library_id values before removing them, in case they are ever needed.
    rows = conn.execute(
        sa.text(
            "SELECT ilc.parent_id, ilc.library_id, ic.name, ilc.settings -> 'library_id' AS sirsi_library_id "
            "FROM integration_library_configurations ilc "
            "JOIN integration_configurations ic ON ic.id = ilc.parent_id "
            "WHERE ilc.settings ? 'library_id' "
            "AND ic.protocol = 'api.sirsidynix_authentication_provider' "
            "AND ic.goal = 'PATRON_AUTH_GOAL'"
        )
    )
    for parent_id, library_id, name, sirsi_library_id in rows:
        log.info(
            f"Removing library_id={sirsi_library_id!r} from integration {name!r} "
            f"(parent_id={parent_id}, library_id={library_id})."
        )

    conn.execute(
        sa.text(
            "UPDATE integration_library_configurations "
            "SET settings = settings - 'library_id' "
            "WHERE settings ? 'library_id' "
            "AND parent_id IN ("
            "  SELECT id FROM integration_configurations "
            "  WHERE protocol = 'api.sirsidynix_authentication_provider' "
            "  AND goal = 'PATRON_AUTH_GOAL'"
            ")"
        )
    )


def downgrade() -> None:
    pass
