"""Update axis settings

Revision ID: 87051f7b2905
Revises: 01b1e464a9d1
Create Date: 2025-06-23 13:15:35.619911+00:00

"""

from urllib.parse import urlparse

import sqlalchemy as sa
from alembic import op

from palace.manager.util.json import json_serializer
from palace.manager.util.migration.helpers import migration_logger

# revision identifiers, used by Alembic.
revision = "87051f7b2905"
down_revision = "01b1e464a9d1"
branch_labels = None
depends_on = None


log = migration_logger(revision)


def upgrade() -> None:
    conn = op.get_bind()
    results = conn.execute(
        sa.text(
            """
            SELECT id, name, settings
            FROM integration_configurations WHERE goal = 'LICENSE_GOAL' and protocol = 'Axis 360' and settings ? 'url'
            FOR UPDATE
            """
        )
    )

    for integration_id, name, settings_dict in results:
        existing_url = settings_dict["url"]
        del settings_dict["url"]
        log.info(
            f"Updating Axis 360 integration {name} ({integration_id}) settings to remove 'url' key. "
            f"Existing URL: {existing_url}"
        )
        parsed_url = urlparse(existing_url)
        if parsed_url.hostname == "axis360apiqa.baker-taylor.com":
            settings_dict["server_nickname"] = "QA"
        elif parsed_url.hostname != "axis360api.baker-taylor.com":
            raise ValueError(
                f"Axis 360 integration {name} ({integration_id}) has an unexpected URL: {existing_url}. "
                "Please check the settings manually."
            )
        conn.execute(
            sa.text(
                """
                UPDATE integration_configurations
                SET settings = :settings
                WHERE id = :integration_id
                """
            ),
            {
                "settings": json_serializer(settings_dict),
                "integration_id": integration_id,
            },
        )


def downgrade() -> None:
    conn = op.get_bind()
    results = conn.execute(
        sa.text(
            """
            SELECT id, name, settings
            FROM integration_configurations WHERE goal = 'LICENSE_GOAL' and protocol = 'Axis 360' and settings ? 'server_nickname'
            FOR UPDATE
            """
        )
    )

    for integration_id, name, settings_dict in results:
        existing_nickname = settings_dict["server_nickname"].lower()
        del settings_dict["server_nickname"]
        if existing_nickname == "qa":
            settings_dict["url"] = (
                "https://axis360apiqa.baker-taylor.com/Services/VendorAPI/"
            )
        conn.execute(
            sa.text(
                """
                UPDATE integration_configurations
                SET settings = :settings
                WHERE id = :integration_id
                """
            ),
            {
                "settings": json_serializer(settings_dict),
                "integration_id": integration_id,
            },
        )
