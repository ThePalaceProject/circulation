"""Remove short_name from library settings.

Revision ID: e4b120a8d1d5
Revises: 2d72d6876c52
Create Date: 2023-11-22 16:28:55.759169+00:00

"""
from alembic import op
from core.migration.util import migration_logger
from core.model import json_serializer

# revision identifiers, used by Alembic.
revision = "e4b120a8d1d5"
down_revision = "2d72d6876c52"
branch_labels = None
depends_on = None


log = migration_logger(revision)


def upgrade() -> None:
    conn = op.get_bind()

    # Find all the library configurations that have a short_name key in their settings.
    rows = conn.execute(
        "select parent_id, library_id, settings from integration_library_configurations where settings ? 'short_name'"
    ).all()

    for row in rows:
        settings = row.settings.copy()
        short_name = settings.get("short_name")
        del settings["short_name"]
        log.info(
            f"Removing short_name {short_name} from library configuration "
            f"(parent:{row.parent_id}/library:{row.library_id}) {settings}"
        )
        settings_json = json_serializer(settings)
        conn.execute(
            "update integration_library_configurations set settings = (%s) where parent_id = (%s) and library_id = (%s)",
            (settings_json, row.parent_id, row.library_id),
        )


def downgrade() -> None:
    # No need to do anything here. The key was never used.
    pass
