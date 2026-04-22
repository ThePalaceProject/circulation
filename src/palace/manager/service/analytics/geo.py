"""Geographic settings resolution for analytics events."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.configuration.global_settings import (
    GLOBAL_SETTINGS_PROTOCOL,
    GlobalSettings,
)
from palace.manager.integration.goals import Goals
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.util import get_one

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.library import Library

_ENV_DEFAULT_COUNTRY = "PALACE_DEFAULT_COUNTRY"
_ENV_DEFAULT_STATE = "PALACE_DEFAULT_STATE"


def resolve_geo(library: Library, session: Session) -> tuple[str, str]:
    """
    Resolve the (country, state) for a circulation event using a 3-tier hierarchy.

    Resolution order (highest priority last):
      1. Environment variables ``PALACE_DEFAULT_COUNTRY`` / ``PALACE_DEFAULT_STATE``
         (hard-coded fallbacks: ``"US"`` / ``"All"``)
      2. Sitewide :class:`GlobalSettings` stored in
         :class:`~palace.manager.sqlalchemy.model.integration.IntegrationConfiguration`
      3. Library-level ``country`` / ``state`` fields in
         :class:`~palace.manager.integration.configuration.library.LibrarySettings`

    :param library: The :class:`~palace.manager.sqlalchemy.model.library.Library` for
        which an event is being recorded.
    :param session: An active SQLAlchemy session.
    :return: A ``(country, state)`` tuple; never ``None``.
    """
    # Tier 1 (lowest priority): env var defaults with hard-coded fallbacks
    country: str = os.environ.get(_ENV_DEFAULT_COUNTRY, "US")
    state: str = os.environ.get(_ENV_DEFAULT_STATE, "All")

    # Tier 2: sitewide GlobalSettings stored in IntegrationConfiguration
    global_integration = get_one(
        session,
        IntegrationConfiguration,
        goal=Goals.SITEWIDE_SETTINGS,
        protocol=GLOBAL_SETTINGS_PROTOCOL,
    )
    if global_integration is not None:
        global_cfg = integration_settings_load(GlobalSettings, global_integration)
        if global_cfg.country:
            country = global_cfg.country
        if global_cfg.state:
            state = global_cfg.state

    # Tier 3 (highest priority): library-level settings
    lib_settings = library.settings
    if lib_settings.country is not None:
        country = lib_settings.country
    if lib_settings.state is not None:
        state = lib_settings.state

    return country, state
