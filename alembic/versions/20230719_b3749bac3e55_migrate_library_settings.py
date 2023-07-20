"""Migrate library settings

Revision ID: b3749bac3e55
Revises: 3d380776c1bf
Create Date: 2023-07-19 16:13:14.831349+00:00

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op
from core.configuration.library import LibrarySettings
from core.migration.migrate_external_integration import _validate_and_load_settings
from core.model import json_serializer

# revision identifiers, used by Alembic.
revision = "b3749bac3e55"
down_revision = "3d380776c1bf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "libraries",
        sa.Column(
            "settings_dict", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
    )

    connection = op.get_bind()
    libraries = connection.execute("select id, short_name from libraries")
    for library in libraries:
        configuration_settings = connection.execute(
            "select key, value from configurationsettings "
            "where library_id = (%s) and external_integration_id IS NULL",
            (library.id,),
        )
        settings_dict = {}
        for key, value in configuration_settings:
            if key in ["announcements", "logo", "key-pair"]:
                continue
            if not value:
                continue
            settings_dict[key] = value

        settings = _validate_and_load_settings(LibrarySettings, settings_dict)
        connection.execute(
            "update libraries set settings_dict = (%s) where id = (%s)",
            (json_serializer(settings.dict()), library.id),
        )

    op.alter_column("libraries", "settings_dict", nullable=False)


def downgrade() -> None:
    op.drop_column("libraries", "settings_dict")
