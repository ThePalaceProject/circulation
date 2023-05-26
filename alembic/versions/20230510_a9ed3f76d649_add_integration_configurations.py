"""Add integration_configurations

Revision ID: a9ed3f76d649
Revises: 5a425ebe026c
Create Date: 2023-05-10 19:50:47.458800+00:00

"""
import json
from collections import defaultdict
from typing import Dict, Tuple, Type, TypeVar

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, Row

from alembic import op
from api.authentication.base import AuthenticationProvider
from api.integration.registry.patron_auth import PatronAuthRegistry
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItemType,
    FormFieldInfo,
)
from core.model import json_serializer

# revision identifiers, used by Alembic.
revision = "a9ed3f76d649"
down_revision = "5a425ebe026c"
branch_labels = None
depends_on = None


def _create_tables() -> None:
    op.create_table(
        "integration_configurations",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("protocol", sa.Unicode(), nullable=False),
        sa.Column("goal", sa.Enum("PATRON_AUTH_GOAL", name="goals"), nullable=False),
        sa.Column("name", sa.Unicode(), nullable=False),
        sa.Column("settings", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "self_test_results", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("status", sa.Enum("RED", "GREEN", name="status"), nullable=False),
        sa.Column("last_status_update", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(
        op.f("ix_integration_configurations_goal"),
        "integration_configurations",
        ["goal"],
        unique=False,
    )
    op.create_table(
        "integration_errors",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("time", sa.DateTime(), nullable=True),
        sa.Column("error", sa.Unicode(), nullable=True),
        sa.Column("integration_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["integration_id"],
            ["integration_configurations.id"],
            name="fk_integration_error_integration_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "integration_library_configurations",
        sa.Column("parent_id", sa.Integer(), nullable=False),
        sa.Column("library_id", sa.Integer(), nullable=False),
        sa.Column("settings", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["library_id"], ["libraries.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["parent_id"], ["integration_configurations.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("parent_id", "library_id"),
    )


T = TypeVar("T", bound=BaseSettings)


def _validate_and_load_settings(
    settings_class: Type[T], settings_dict: Dict[str, str]
) -> T:
    aliases = {
        f.alias: f.name
        for f in settings_class.__fields__.values()
        if f.alias is not None
    }
    parsed_settings_dict = {}
    for key, setting in settings_dict.items():
        if key in aliases:
            key = aliases[key]
        field = settings_class.__fields__.get(key)
        if field is None or not isinstance(field.field_info, FormFieldInfo):
            continue
        config_item = field.field_info.form
        if (
            config_item.type == ConfigurationFormItemType.LIST
            or config_item.type == ConfigurationFormItemType.MENU
        ):
            parsed_settings_dict[key] = json.loads(setting)
        else:
            parsed_settings_dict[key] = setting
    return settings_class(**parsed_settings_dict)


def _migrate_external_integration(
    connection: Connection,
    integration: Row,
    protocol_class: Type[AuthenticationProvider],
) -> Tuple[int, Dict[str, Dict[str, str]]]:
    settings = connection.execute(
        "select cs.library_id, cs.key, cs.value from configurationsettings cs "
        "where cs.external_integration_id = (%s)",
        (integration.id,),
    )
    settings_dict = {}
    library_settings: Dict[str, Dict[str, str]] = defaultdict(dict)
    self_test_results = json_serializer({})
    for setting in settings:
        if not setting.value:
            continue
        if setting.key == "self_test_results":
            self_test_results = setting.value
            continue
        if setting.library_id:
            library_settings[setting.library_id][setting.key] = setting.value
        else:
            settings_dict[setting.key] = setting.value

    # Load and validate the settings before storing them in the database.
    settings_class = protocol_class.settings_class()
    settings_obj = _validate_and_load_settings(settings_class, settings_dict)
    integration_configuration = connection.execute(
        "insert into integration_configurations "
        "(protocol, goal, name, settings, self_test_results, status) "
        "values (%s, 'PATRON_AUTH_GOAL', %s, %s, %s, 'GREEN')"
        "returning id",
        (
            integration.protocol,
            integration.name,
            json_serializer(settings_obj.dict()),
            self_test_results,
        ),
    ).fetchone()
    assert integration_configuration is not None
    return integration_configuration[0], library_settings


def _migrate_library_settings(
    connection: Connection,
    integration_id: int,
    library_id: int,
    library_settings: Dict[str, str],
    protocol_class: Type[AuthenticationProvider],
) -> None:
    library_settings_class = protocol_class.library_settings_class()
    library_settings_obj = _validate_and_load_settings(
        library_settings_class, library_settings
    )
    connection.execute(
        "insert into integration_library_configurations "
        "(parent_id, library_id, settings) "
        "values (%s, %s, %s)",
        (
            integration_id,
            library_id,
            json_serializer(library_settings_obj.dict()),
        ),
    )


def _migrate_settings() -> None:
    connection = op.get_bind()
    external_integrations = connection.execute(
        "select ei.id, ei.protocol, ei.name from externalintegrations ei "
        "where ei.goal = 'patron_auth'"
    )

    patron_auth_registry = PatronAuthRegistry()
    for external_integration in external_integrations:
        protocol_class = patron_auth_registry[external_integration.protocol]
        integration_id, library_settings = _migrate_external_integration(
            connection, external_integration, protocol_class
        )
        external_integration_library = connection.execute(
            "select library_id from externalintegrations_libraries where externalintegration_id = %s",
            (external_integration.id,),
        )
        for library in external_integration_library:
            _migrate_library_settings(
                connection,
                integration_id,
                library.library_id,
                library_settings[library.library_id],
                protocol_class,
            )


def upgrade() -> None:
    # Add new tables for tracking integration configurations and errors.
    _create_tables()

    # Migrate settings from the configurationsettings table into integration_configurations.
    # We leave the existing settings in the table, but they will no longer be used.
    _migrate_settings()


def downgrade() -> None:
    op.drop_table("integration_library_configurations")
    op.drop_table("integration_errors")
    op.drop_index(
        op.f("ix_integration_configurations_goal"),
        table_name="integration_configurations",
    )
    op.drop_table("integration_configurations")
    sa.Enum(name="goals").drop(op.get_bind(), checkfirst=False)
    sa.Enum(name="status").drop(op.get_bind(), checkfirst=False)
