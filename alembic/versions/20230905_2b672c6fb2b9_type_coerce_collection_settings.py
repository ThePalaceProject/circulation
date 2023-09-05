"""Type coerce collection settings

Revision ID: 2b672c6fb2b9
Revises: 0df58829fc1a
Create Date: 2023-09-05 06:40:35.739869+00:00

"""
import json

from alembic import op

# revision identifiers, used by Alembic.
revision = "2b672c6fb2b9"
down_revision = "0df58829fc1a"
branch_labels = None
depends_on = None


def _bool(value):
    return value in ("true", "True", True)


# All the settings types that have non-str types
ALL_SETTING_TYPES = {
    "verify_certificate": _bool,
    "default_reservation_period": _bool,
    "loan_limit": int,
    "hold_limit": int,
    "max_retry_count": int,
    "ebook_loan_duration": int,
    "default_loan_duration": int,
}


def _coerce_types(settings: dict) -> None:
    """Coerce the types, in-place"""
    for setting_name, setting_type in ALL_SETTING_TYPES.items():
        if setting_name in settings:
            settings[setting_name] = setting_type(settings[setting_name])


def upgrade() -> None:
    connection = op.get_bind()
    # Fetch all integration settings with the 'licenses' goal
    results = connection.execute(
        f"SELECT id, settings from integration_configurations where goal='LICENSE_GOAL';"
    ).fetchall()

    # For each integration setting, we check id any of the non-str
    # keys are present in the DB
    # We then type-coerce that value
    for settings_id, settings in results:
        _coerce_types(settings)
        connection.execute(
            "UPDATE integration_configurations SET settings=%s where id=%s",
            json.dumps(settings),
            settings_id,
        )

    # Do the same for any Library settings
    results = connection.execute(
        f"SELECT parent_id, settings from integration_library_configurations;"
    ).fetchall()

    for settings_id, settings in results:
        _coerce_types(settings)
        connection.execute(
            "UPDATE integration_library_configurations SET settings=%s where parent_id=%s",
            json.dumps(settings),
            settings_id,
        )


def downgrade() -> None:
    """There is no need to revert the types back to strings"""
