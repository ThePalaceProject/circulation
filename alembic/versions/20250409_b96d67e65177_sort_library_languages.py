"""Sort library languages

Revision ID: b96d67e65177
Revises: df27b4867e56
Create Date: 2025-04-09 14:24:59.558570+00:00

"""

import sqlalchemy as sa
from alembic import op

from palace.manager.util.json import json_serializer

# revision identifiers, used by Alembic.
revision = "b96d67e65177"
down_revision = "df27b4867e56"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    results = conn.execute(
        sa.text(
            """
            SELECT id, settings_dict
            FROM libraries WHERE settings_dict ?|
            array['large_collection_languages', 'small_collection_languages', 'tiny_collection_languages']
            FOR UPDATE
            """
        )
    )
    for library_id, settings_dict in results:
        # Sort the language lists
        for key in [
            "large_collection_languages",
            "small_collection_languages",
            "tiny_collection_languages",
        ]:
            if key in settings_dict:
                settings_dict[key] = sorted(settings_dict[key])

        # Update the library's settings
        conn.execute(
            sa.text(
                """
                UPDATE libraries
                SET settings_dict = :settings_dict
                WHERE id = :library_id
                """
            ),
            {"settings_dict": json_serializer(settings_dict), "library_id": library_id},
        )


def downgrade() -> None:
    # No need to unsort the languages
    pass
