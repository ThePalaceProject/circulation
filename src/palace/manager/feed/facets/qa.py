from __future__ import annotations

from palace.manager.feed.facets.constants import FacetConfig
from palace.manager.feed.facets.feed import Facets
from palace.manager.sqlalchemy.model.library import Library


class JackpotFacets(Facets):
    """A faceting object for a jackpot feed.

    Unlike other faceting objects, AVAILABLE_NOT_NOW is an acceptable
    option for the availability facet.
    """

    @classmethod
    def default_facet(
        cls, config: Library | FacetConfig | None, facet_group_name: str
    ) -> str | None:
        if facet_group_name != cls.AVAILABILITY_FACET_GROUP_NAME:
            return super().default_facet(config, facet_group_name)
        return cls.AVAILABLE_NOW

    @classmethod
    def available_facets(
        cls, config: Library | FacetConfig | None, facet_group_name: str
    ) -> list[str]:
        if facet_group_name != cls.AVAILABILITY_FACET_GROUP_NAME:
            return super().available_facets(config, facet_group_name)

        return [
            cls.AVAILABLE_NOW,
            cls.AVAILABLE_NOT_NOW,
            cls.AVAILABLE_ALL,
            cls.AVAILABLE_OPEN_ACCESS,
        ]
