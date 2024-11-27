"""Update database nullable constraints

Revision ID: 7edcbb24154e
Revises: 272da5f400de
Create Date: 2024-11-27 18:12:09.243827+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "7edcbb24154e"
down_revision = "272da5f400de"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("annotations", "active", existing_type=sa.BOOLEAN(), nullable=False)
    op.alter_column(
        "collections", "marked_for_deletion", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "customlists", "auto_update_enabled", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "customlists",
        "auto_update_status",
        existing_type=postgresql.ENUM(
            "init", "updated", "repopulate", name="auto_update_status"
        ),
        nullable=False,
    )
    op.alter_column(
        "datasources", "offers_licenses", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "deliverymechanisms",
        "default_client_can_fulfill",
        existing_type=sa.BOOLEAN(),
        nullable=False,
    )
    op.alter_column(
        "editions", "data_source_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "editions", "primary_identifier_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "equivalents", "input_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column("equivalents", "votes", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "equivalents", "enabled", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "equivalentscoveragerecords",
        "equivalency_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column("holds", "patron_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "holds", "license_pool_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column("lanes", "display_name", existing_type=sa.VARCHAR(), nullable=False)
    op.alter_column(
        "libraries", "is_default", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "licensepools", "data_source_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "licensepools", "identifier_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "licensepools", "superceded", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "licensepools", "suppressed", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "licensepools", "licenses_owned", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "licensepools", "licenses_available", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "licensepools", "licenses_reserved", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "licensepools",
        "patrons_in_hold_queue",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "licenses", "license_pool_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column("loans", "patron_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "loans", "license_pool_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "measurements",
        "weight",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=False,
    )
    op.alter_column(
        "playtime_entries", "processed", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "playtime_summaries",
        "total_seconds_played",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "resources",
        "voted_quality",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=False,
    )
    op.alter_column(
        "resources", "votes_for_quality", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "resourcetransformations",
        "settings",
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        nullable=False,
    )
    op.alter_column("subjects", "locked", existing_type=sa.BOOLEAN(), nullable=False)
    op.alter_column("subjects", "checked", existing_type=sa.BOOLEAN(), nullable=False)
    op.alter_column(
        "workgenres", "genre_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column("workgenres", "work_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "workgenres",
        "affinity",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=False,
    )
    op.alter_column(
        "works", "presentation_ready", existing_type=sa.BOOLEAN(), nullable=False
    )


def downgrade() -> None:
    op.alter_column(
        "works", "presentation_ready", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "workgenres",
        "affinity",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=True,
    )
    op.alter_column("workgenres", "work_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("workgenres", "genre_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("subjects", "checked", existing_type=sa.BOOLEAN(), nullable=True)
    op.alter_column("subjects", "locked", existing_type=sa.BOOLEAN(), nullable=True)
    op.alter_column(
        "resourcetransformations",
        "settings",
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        nullable=True,
    )
    op.alter_column(
        "resources", "votes_for_quality", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "resources",
        "voted_quality",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=True,
    )
    op.alter_column(
        "playtime_summaries",
        "total_seconds_played",
        existing_type=sa.INTEGER(),
        nullable=True,
    )
    op.alter_column(
        "playtime_entries", "processed", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "measurements",
        "weight",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=True,
    )
    op.alter_column(
        "loans", "license_pool_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column("loans", "patron_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column(
        "licenses", "license_pool_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "licensepools",
        "patrons_in_hold_queue",
        existing_type=sa.INTEGER(),
        nullable=True,
    )
    op.alter_column(
        "licensepools", "licenses_reserved", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "licensepools", "licenses_available", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "licensepools", "licenses_owned", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "licensepools", "suppressed", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "licensepools", "superceded", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "licensepools", "identifier_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "licensepools", "data_source_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "libraries", "is_default", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column("lanes", "display_name", existing_type=sa.VARCHAR(), nullable=True)
    op.alter_column(
        "holds", "license_pool_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column("holds", "patron_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column(
        "equivalentscoveragerecords",
        "equivalency_id",
        existing_type=sa.INTEGER(),
        nullable=True,
    )
    op.alter_column("equivalents", "enabled", existing_type=sa.BOOLEAN(), nullable=True)
    op.alter_column("equivalents", "votes", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column(
        "equivalents", "input_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "editions", "primary_identifier_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "editions", "data_source_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "deliverymechanisms",
        "default_client_can_fulfill",
        existing_type=sa.BOOLEAN(),
        nullable=True,
    )
    op.alter_column(
        "datasources", "offers_licenses", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "customlists",
        "auto_update_status",
        existing_type=postgresql.ENUM(
            "init", "updated", "repopulate", name="auto_update_status"
        ),
        nullable=True,
    )
    op.alter_column(
        "customlists", "auto_update_enabled", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "collections", "marked_for_deletion", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column("annotations", "active", existing_type=sa.BOOLEAN(), nullable=True)
