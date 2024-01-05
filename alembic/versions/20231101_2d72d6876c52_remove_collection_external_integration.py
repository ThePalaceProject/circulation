"""Remove collection external integration.

Revision ID: 2d72d6876c52
Revises: cc084e35e037
Create Date: 2023-11-01 22:42:06.754873+00:00

"""
from collections import deque
from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op
from api.integration.registry.license_providers import LicenseProvidersRegistry
from core.integration.base import HasChildIntegrationConfiguration
from core.migration.util import migration_logger
from core.model import json_serializer

# revision identifiers, used by Alembic.
revision = "2d72d6876c52"
down_revision = "cc084e35e037"
branch_labels = None
depends_on = None


log = migration_logger(revision)


@dataclass
class RenameQueryRow:
    collection_id: int
    integration_id: int
    integration_name: str
    collection_name: str
    deferral_count: int = 0


def upgrade() -> None:
    conn = op.get_bind()

    # Our collection names have gotten out of sync with the integration names. The collection names
    # are what are being displayed to users, so before we stop using the collection name, we need
    # to update the integration name to match the collection name.
    # For now, we leave the collection name column in place, but we make it nullable and remove the
    # unique constraint.
    rows = conn.execute(
        "SELECT c.id as collection_id, ic.id as integration_id, ic.name as integration_name, "
        "c.name as collection_name from collections c JOIN integration_configurations ic "
        "ON c.integration_configuration_id = ic.id WHERE c.name != ic.name"
    ).all()

    integration_names = {row.integration_name for row in rows}
    collection_renames = deque(RenameQueryRow(**r) for r in rows)
    while collection_renames:
        rename = collection_renames.popleft()
        if rename.collection_name in integration_names:
            # The collection name is already in use by an integration, so we need to rename the
            # integration first.
            log.info(
                f"Collection name {rename.collection_name} is already in use. Deferring rename."
            )
            rename.deferral_count += 1
            if rename.deferral_count > 3:
                raise RuntimeError(
                    f"Unable to rename collection {rename.collection_id}. Max deferral count reached."
                )
            collection_renames.append(rename)
            continue
        log.info(
            f"Updating name for collection {rename.collection_id} from {rename.integration_name} to {rename.collection_name}."
        )
        conn.execute(
            "UPDATE integration_configurations SET name = (%s) WHERE id = (%s)",
            (rename.collection_name, rename.integration_id),
        )
        integration_names.remove(rename.integration_name)

    op.alter_column("collections", "name", existing_type=sa.VARCHAR(), nullable=True)
    op.drop_index("ix_collections_name", table_name="collections")

    # We have moved the setting for the TOKEN_AUTH integration from an external integration
    # to a new JSONB column on the integration_configurations table (context). We need to move
    # the data into the new column as part of this migration.
    # The context column is not nullable, so we need to set a default value for the existing
    # rows. We will use an empty JSON object. We create the column as nullable, set the default
    # value, then make it non-nullable.
    op.add_column(
        "integration_configurations",
        sa.Column("context", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    conn.execute("UPDATE integration_configurations SET context = '{}'")

    rows = conn.execute(
        "SELECT c.id, cs.value FROM collections c "
        "JOIN externalintegrations ei ON c.external_integration_id = ei.id "
        "JOIN configurationsettings cs ON ei.id = cs.external_integration_id "
        "WHERE key='token_auth_endpoint' and value <> ''"
    ).all()

    for row in rows:
        context = json_serializer({"token_auth_endpoint": row.value})
        log.info(f"Updating context for collection {row.id} to {context}.")
        conn.execute(
            "UPDATE integration_configurations SET context = (%s) "
            "FROM collections "
            "WHERE integration_configurations.id = collections.integration_configuration_id "
            "and collections.id = (%s)",
            (context, row.id),
        )

    op.alter_column("integration_configurations", "context", nullable=False)

    # We have moved the data that was in external_account_id into the settings column of the
    # integration, so we need to make sure that it gets moved as part of this migration. We
    # also make sure that the new settings are valid for the integration before saving them
    # to the database.
    rows = conn.execute(
        "SELECT ic.id as integration_id, ic.settings, ic.protocol, ic.goal, c.external_account_id, c.parent_id "
        "FROM collections c JOIN integration_configurations ic ON c.integration_configuration_id = ic.id"
    ).all()

    registry = LicenseProvidersRegistry()
    for row in rows:
        if row.external_account_id is None:
            continue
        settings_dict = row.settings.copy()
        settings_dict["external_account_id"] = row.external_account_id
        impl_class = registry.get(row.protocol)
        if impl_class is None:
            raise RuntimeError(
                f"Could not find implementation for protocol {row.protocol}"
            )
        if row.parent_id is not None:
            if issubclass(impl_class, HasChildIntegrationConfiguration):
                settings_obj = impl_class.child_settings_class()(**settings_dict)
            else:
                raise RuntimeError(
                    f"Integration {row.integration_id} is a child integration, "
                    f"but {row.protocol} does not support child integrations."
                )
        else:
            settings_obj = impl_class.settings_class()(**settings_dict)
        new_settings_dict = settings_obj.dict()
        if row.settings != new_settings_dict:
            new_settings = json_serializer(new_settings_dict)
            log.info(
                f"Updating settings for integration {row.integration_id} from {row.settings} to {new_settings}."
            )
            conn.execute(
                "UPDATE integration_configurations SET settings = (%s) WHERE id = (%s)",
                (new_settings, row.integration_id),
            )

    # Because collections now rely on integration_configurations, they can no longer
    # have a null value for integration_configuration_id. This should already be true
    # of our existing collections. We also drop our foreign key constraint, and recreate
    # it with the correct ondelete behavior.
    op.alter_column(
        "collections",
        "integration_configuration_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.drop_constraint(
        "collections_integration_configuration_id_fkey",
        "collections",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "collections_integration_configuration_id_fkey",
        "collections",
        "integration_configurations",
        ["integration_configuration_id"],
        ["id"],
    )

    # The data that was in the collections_libraries table is now tracked by
    # integration_library_configurations, we keep the data in the collections_libraries
    # table for now, but we remove the foreign key constraints and indexes.
    op.alter_column(
        "collections_libraries",
        "collection_id",
        existing_type=sa.INTEGER(),
        nullable=True,
    )
    op.alter_column(
        "collections_libraries", "library_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.drop_index(
        "ix_collections_libraries_collection_id", table_name="collections_libraries"
    )
    op.drop_index(
        "ix_collections_libraries_library_id", table_name="collections_libraries"
    )
    op.drop_constraint(
        "collections_libraries_collection_id_fkey",
        "collections_libraries",
        type_="foreignkey",
    )
    op.drop_constraint(
        "collections_libraries_library_id_fkey",
        "collections_libraries",
        type_="foreignkey",
    )

    # Collections have now been migrated entirely to use integration_configurations. We keep this column
    # for now, but we remove the foreign key constraint and index.
    op.drop_index("ix_collections_external_integration_id", table_name="collections")
    op.drop_constraint(
        "collections_external_integration_id_fkey", "collections", type_="foreignkey"
    )

    # We create a new index on the settings column of integration_configurations. This
    # will allow us to quickly find integrations that have a specific setting.
    op.create_index(
        "ix_integration_configurations_settings_dict",
        "integration_configurations",
        ["settings"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_integration_configurations_settings_dict",
        table_name="integration_configurations",
        postgresql_using="gin",
    )

    op.create_foreign_key(
        "collections_external_integration_id_fkey",
        "collections",
        "externalintegrations",
        ["external_integration_id"],
        ["id"],
    )
    op.create_index(
        "ix_collections_external_integration_id",
        "collections",
        ["external_integration_id"],
        unique=True,
    )

    op.create_foreign_key(
        "collections_libraries_collection_id_fkey",
        "collections_libraries",
        "collections",
        ["collection_id"],
        ["id"],
    )
    op.create_foreign_key(
        "collections_libraries_library_id_fkey",
        "collections_libraries",
        "libraries",
        ["library_id"],
        ["id"],
    )
    op.create_index(
        "ix_collections_libraries_library_id",
        "collections_libraries",
        ["library_id"],
        unique=False,
    )
    op.create_index(
        "ix_collections_libraries_collection_id",
        "collections_libraries",
        ["collection_id"],
        unique=False,
    )
    op.alter_column(
        "collections_libraries",
        "library_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "collections_libraries",
        "collection_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )

    op.drop_constraint(
        "collections_integration_configuration_id_fkey",
        "collections",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "collections_integration_configuration_id_fkey",
        "collections",
        "integration_configurations",
        ["integration_configuration_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.alter_column(
        "collections",
        "integration_configuration_id",
        existing_type=sa.INTEGER(),
        nullable=True,
    )

    op.drop_column("integration_configurations", "context")

    op.create_index("ix_collections_name", "collections", ["name"], unique=True)
    op.alter_column("collections", "name", existing_type=sa.VARCHAR(), nullable=False)
