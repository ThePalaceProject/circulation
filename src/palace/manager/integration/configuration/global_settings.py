"""Global sitewide settings for a Palace Manager instance."""

from __future__ import annotations

import os
from typing import Annotated

from palace.manager.integration.settings import BaseSettings, FormMetadata

# Protocol string used to identify the global settings IntegrationConfiguration row.
GLOBAL_SETTINGS_PROTOCOL = "global_settings"

# Env vars that seed Tier-1 (lowest-priority) defaults for country/state resolution.
# Defined here so geo.py and the admin controller can share them without circular imports.
ENV_DEFAULT_COUNTRY = "PALACE_DEFAULT_COUNTRY"
ENV_DEFAULT_STATE = "PALACE_DEFAULT_STATE"

_HARDCODED_DEFAULT_COUNTRY = "US"
_HARDCODED_DEFAULT_STATE = "All"


class GlobalSettings(BaseSettings):
    """
    Sitewide settings configurable by system admins for a Palace Manager instance.

    These settings serve as defaults for all libraries in the instance and can be
    overridden at the library level. New sitewide settings should be added here
    rather than creating additional settings classes.
    """

    country: Annotated[
        str,
        FormMetadata(
            label="Default country",
            description=(
                "The default country for circulation events in this Palace Manager instance. "
                "Use ISO 3166-1 alpha-2 codes for countries (e.g. 'US' for United States, "
                "'CA' for Canada). This default can be overridden per library."
            ),
        ),
    ] = os.environ.get(ENV_DEFAULT_COUNTRY, _HARDCODED_DEFAULT_COUNTRY)

    state: Annotated[
        str,
        FormMetadata(
            label="Default state/province",
            description=(
                "The default state or province for circulation events in this Palace Manager "
                "instance (e.g. 'New York', 'Ontario'). Use 'All' to indicate all "
                "states/provinces. This default can be overridden per library."
            ),
        ),
    ] = os.environ.get(ENV_DEFAULT_STATE, _HARDCODED_DEFAULT_STATE)
