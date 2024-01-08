"""Update license provider settings.

Revision ID: 735bf6ced8b9
Revises: d3cdbea3d43b
Create Date: 2024-01-04 16:24:32.895789+00:00

"""
from alembic import op
from api.integration.registry.license_providers import LicenseProvidersRegistry
from core.integration.base import HasChildIntegrationConfiguration
from core.migration.util import migration_logger
from core.model import json_serializer

# revision identifiers, used by Alembic.
revision = "735bf6ced8b9"
down_revision = "d3cdbea3d43b"
branch_labels = None
depends_on = None


log = migration_logger(revision)


def upgrade() -> None:
    conn = op.get_bind()

    rows = conn.execute(
        "SELECT ic.id as integration_id, ic.settings, ic.protocol, ic.goal, c.parent_id, ic.name "
        "FROM collections c JOIN integration_configurations ic ON c.integration_configuration_id = ic.id"
    ).all()

    registry = LicenseProvidersRegistry()
    for row in rows:
        settings_dict = row.settings.copy()
        impl_class = registry.get(row.protocol)
        if impl_class is None:
            raise RuntimeError(
                f"Could not find implementation for protocol {row.protocol} for "
                f"integration {row.name}({row.integration_id})."
            )
        if row.parent_id is not None:
            if issubclass(impl_class, HasChildIntegrationConfiguration):
                settings_obj = impl_class.child_settings_class()(**settings_dict)
            else:
                raise RuntimeError(
                    f"Integration {row.name}({row.integration_id}) is a child integration, "
                    f"but {row.protocol} does not support child integrations."
                )
        else:
            settings_obj = impl_class.settings_class()(**settings_dict)
        new_settings_dict = settings_obj.dict(exclude_extra=True)
        if row.settings != new_settings_dict:
            new_settings = json_serializer(new_settings_dict)
            log.info(
                f"Updating settings for integration {row.name}({row.integration_id}) "
                f"from {row.settings} to {new_settings}."
            )
            conn.execute(
                "UPDATE integration_configurations SET settings = (%s) WHERE id = (%s)",
                (new_settings, row.integration_id),
            )


def downgrade() -> None:
    pass
