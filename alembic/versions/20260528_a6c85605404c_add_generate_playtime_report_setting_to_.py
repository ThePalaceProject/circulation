"""Add generate_playtime_report setting to Blackstone collections

Enable the generate_playtime_report flag for all OPDS 2.0 Import and
OPDS for Distributors collections whose data_source name begins with
"Blackstone" or "Unlimited". All other collections default to False
(the Pydantic field default), so no update is required for them.

Revision ID: a6c85605404c
Revises: d856ff4dbefb
Create Date: 2026-05-28 19:52:19.605245+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import bindparam

from palace.manager.util.json import json_serializer
from palace.manager.util.migration.helpers import migration_logger

# revision identifiers, used by Alembic.
revision = "a6c85605404c"
down_revision = "d856ff4dbefb"
branch_labels = None
depends_on = None

log = migration_logger(revision)

# Protocol strings as registered in LicenseProvidersRegistry.
_ELIGIBLE_PROTOCOLS = ("OPDS 2.0 Import", "OPDS for Distributors")


def upgrade() -> None:
    conn = op.get_bind()

    results = conn.execute(
        sa.text(
            """
            SELECT id, name, settings
            FROM integration_configurations
            WHERE goal = 'LICENSE_GOAL'
              AND protocol IN :protocols
              AND (
                  settings->>'data_source' LIKE 'Blackstone%'
                  OR settings->>'data_source' LIKE 'Unlimited%'
              )
            FOR UPDATE
            """
        ).bindparams(bindparam("protocols", expanding=True)),
        {"protocols": list(_ELIGIBLE_PROTOCOLS)},
    )

    rows = list(results)
    for integration_id, name, settings_dict in rows:
        settings_dict["generate_playtime_report"] = True
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
        log.info(
            f"Enabled generate_playtime_report for integration {name!r} (id={integration_id})"
        )

    log.info(f"Enabled generate_playtime_report on {len(rows)} collection(s)")


def downgrade() -> None:
    conn = op.get_bind()

    # Remove the key entirely; absence is equivalent to False for the Pydantic default.
    result = conn.execute(
        sa.text(
            """
            UPDATE integration_configurations
            SET settings = settings - 'generate_playtime_report'
            WHERE goal = 'LICENSE_GOAL'
              AND protocol IN :protocols
              AND settings ? 'generate_playtime_report'
            """
        ).bindparams(bindparam("protocols", expanding=True)),
        {"protocols": list(_ELIGIBLE_PROTOCOLS)},
    )
    log.info(
        f"Removed generate_playtime_report from {result.rowcount} integration configuration(s)"
    )
