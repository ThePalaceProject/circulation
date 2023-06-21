"""update sirsi auth config

Revision ID: 5dcbc92c20b2
Revises: 3ee5b99f2ae7
Create Date: 2023-04-27 22:53:36.584426+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "5dcbc92c20b2"
down_revision = "3ee5b99f2ae7"
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
