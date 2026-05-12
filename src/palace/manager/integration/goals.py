from __future__ import annotations

from enum import Enum


class Goals(Enum):
    """The goal of an external integration"""

    PATRON_AUTH_GOAL = "patron_auth"
    LICENSE_GOAL = "licenses"
    DISCOVERY_GOAL = "discovery"
    CATALOG_GOAL = "catalog"
    METADATA_GOAL = "metadata"
