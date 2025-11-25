"""Update existing licensepools to use status and type enum

Revision ID: 7c8e14813018
Revises: 2ec8857ae150
Create Date: 2025-11-14 13:11:56.971127+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7c8e14813018"
down_revision = "2ec8857ae150"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Update AGGREGATED licensepools (those with License records).
    # Status is ACTIVE if any license has status='available', otherwise EXHAUSTED.
    op.execute(
        sa.text(
            """
            UPDATE licensepools
            SET type = 'aggregated'::licensepooltype,
                status = CASE
                    WHEN EXISTS (
                        SELECT 1 FROM licenses
                        WHERE licenses.license_pool_id = licensepools.id
                        AND licenses.status = 'available'
                    ) THEN 'active'::licensepoolstatus
                    ELSE 'exhausted'::licensepoolstatus
                END
            WHERE EXISTS (
                SELECT 1 FROM licenses
                WHERE licenses.license_pool_id = licensepools.id
            )
            """
        )
    )

    # Update UNLIMITED licensepools with licenses_owned = -1.
    # Skips pools already set to AGGREGATED. Status is always ACTIVE.
    op.execute(
        sa.text(
            """
            UPDATE licensepools
            SET type = 'unlimited'::licensepooltype,
                status = 'active'::licensepoolstatus
            WHERE type = 'metered'
            AND licenses_owned = -1
            """
        )
    )

    # Update UNLIMITED licensepools with licenses_owned = 0 in OPDS collections.
    # Skips pools already set to AGGREGATED. Status is REMOVED.
    op.execute(
        sa.text(
            """
            UPDATE licensepools
            SET type = 'unlimited'::licensepooltype,
                status = 'removed'::licensepoolstatus
            WHERE type = 'metered'
            AND licenses_owned = 0
            AND collection_id IN (
                SELECT collections.id
                FROM collections
                JOIN integration_configurations
                    ON collections.integration_configuration_id = integration_configurations.id
                WHERE integration_configurations.protocol IN (
                    'OPDS Import',
                    'OPDS 2.0 Import',
                    'OPDS for Distributors',
                    'ODL 2.0'
                )
            )
            """
        )
    )

    # Update remaining METERED licensepools with licenses_owned = 0 to EXHAUSTED.
    # These pools have no available licenses.
    op.execute(
        sa.text(
            """
            UPDATE licensepools
            SET status = 'exhausted'::licensepoolstatus
            WHERE type = 'metered'
            AND licenses_owned = 0
            """
        )
    )


def downgrade() -> None:
    pass
