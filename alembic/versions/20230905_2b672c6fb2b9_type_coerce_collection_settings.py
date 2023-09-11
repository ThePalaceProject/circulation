"""Type coerce collection settings

Revision ID: 2b672c6fb2b9
Revises: 0df58829fc1a
Create Date: 2023-09-05 06:40:35.739869+00:00

"""
import json
import logging
from copy import deepcopy
from typing import Any, Dict, Optional, Tuple

from pydantic import PositiveInt, ValidationError, parse_obj_as

from alembic import op

# revision identifiers, used by Alembic.
revision = "2b672c6fb2b9"
down_revision = "0df58829fc1a"
branch_labels = None
depends_on = None


log = logging.getLogger(f"palace.migration.{revision}")
log.setLevel(logging.INFO)
log.disabled = False


# All the settings types that have non-str types
ALL_SETTING_TYPES: Dict[str, Any] = {
    "verify_certificate": Optional[bool],
    "default_reservation_period": Optional[PositiveInt],
    "loan_limit": Optional[PositiveInt],
    "hold_limit": Optional[PositiveInt],
    "max_retry_count": Optional[PositiveInt],
    "ebook_loan_duration": Optional[PositiveInt],
    "default_loan_duration": Optional[PositiveInt],
}


def _coerce_types(original_settings: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Coerce the types, in-place"""
    modified = False
    modified_settings = deepcopy(original_settings)
    for setting_name, setting_type in ALL_SETTING_TYPES.items():
        if setting_name in original_settings:
            # If the setting is an empty string, we set it to None
            if original_settings[setting_name] == "":
                setting = None
            else:
                setting = original_settings[setting_name]

            try:
                modified = True
                modified_settings[setting_name] = parse_obj_as(setting_type, setting)
            except ValidationError as e:
                log.error(
                    f"Error while parsing setting {setting_name}. Settings: {original_settings}."
                )
                raise e

    return modified, modified_settings


def upgrade() -> None:
    connection = op.get_bind()
    # Fetch all integration settings with the 'licenses' goal
    results = connection.execute(
        "SELECT id, settings from integration_configurations where goal='LICENSE_GOAL';"
    ).fetchall()

    # For each integration setting, we check id any of the non-str
    # keys are present in the DB
    # We then type-coerce that value
    for settings_id, settings in results:
        modified, updated_settings = _coerce_types(settings)
        if modified:
            log.info(
                f"Updating settings for integration_configuration (id:{settings_id}). "
                f"Original settings: {settings}. New settings: {updated_settings}."
            )
            # If any of the values were modified, we update the DB
            connection.execute(
                "UPDATE integration_configurations SET settings=%s where id=%s",
                json.dumps(updated_settings),
                settings_id,
            )

    # Do the same for any Library settings
    results = connection.execute(
        "SELECT ilc.parent_id, ilc.library_id, ilc.settings from integration_library_configurations ilc "
        "join integration_configurations ic on ilc.parent_id = ic.id where ic.goal='LICENSE_GOAL';"
    ).fetchall()

    for parent_id, library_id, settings in results:
        modified, updated_settings = _coerce_types(settings)
        if modified:
            log.info(
                f"Updating settings for integration_library_configuration (parent_id:{parent_id}/library_id:{library_id}). "
                f"Original settings: {settings}. New settings: {updated_settings}."
            )
            connection.execute(
                "UPDATE integration_library_configurations SET settings=%s where parent_id=%s and library_id=%s",
                json.dumps(updated_settings),
                parent_id,
                library_id,
            )


def downgrade() -> None:
    """There is no need to revert the types back to strings"""
