"""Update Sirsi auth config

Revision ID: 457622962381
Revises: dac99ae0c6fd
Create Date: 2023-04-21 15:07:28.197192+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "457622962381"
down_revision = "dac99ae0c6fd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Update the SirsiDynix auth config to use library_identifier_restriction
    # instead of the old LIBRARY_PREFIX setting.
    # This migration leaves the old LIBRARY_PREFIX setting in place, but unused
    # in case we need to roll this back. We can clean up the old setting in a
    # later migration.
    connection = op.get_bind()
    settings = connection.execute(
        "select ei.id, cs.library_id, cs.value from externalintegrations as ei join "
        "configurationsettings cs on ei.id = cs.external_integration_id "
        "where ei.protocol = 'api.sirsidynix_authentication_provider' and "
        "ei.goal = 'patron_auth' and cs.key = 'LIBRARY_PREFIX'"
    )

    for setting in settings:
        connection.execute(
            "UPDATE configurationsettings SET value = (%s) "
            "WHERE external_integration_id = (%s) and library_id = (%s) "
            "and key = 'library_identifier_restriction'",
            (setting.value, setting.id, setting.library_id),
        )
        connection.execute(
            "UPDATE configurationsettings SET value = 'patronType' "
            "WHERE external_integration_id = (%s) and library_id = (%s) "
            "and key = 'library_identifier_field'",
            (setting.id, setting.library_id),
        )
        connection.execute(
            "UPDATE configurationsettings SET value = 'prefix' "
            "WHERE external_integration_id = (%s) and library_id = (%s) "
            "and key = 'library_identifier_restriction_type'",
            (setting.id, setting.library_id),
        )


def downgrade() -> None:
    # These updated settings shouldn't cause any issues if left in place
    # when downgrading so we leave them alone.
    pass
