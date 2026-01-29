from palace.manager.feed.facets.feed import Facets


class JackpotFacets(Facets):
    """A faceting object for a jackpot feed.

    Unlike other faceting objects, AVAILABLE_NOT_NOW is an acceptable
    option for the availability facet.
    """

    @classmethod
    def default_facet(cls, config, facet_group_name):
        if facet_group_name != cls.AVAILABILITY_FACET_GROUP_NAME:
            return super().default_facet(config, facet_group_name)
        return cls.AVAILABLE_NOW

    @classmethod
    def available_facets(cls, config, facet_group_name):
        if facet_group_name != cls.AVAILABILITY_FACET_GROUP_NAME:
            return super().available_facets(config, facet_group_name)

        return [
            cls.AVAILABLE_NOW,
            cls.AVAILABLE_NOT_NOW,
            cls.AVAILABLE_ALL,
            cls.AVAILABLE_OPEN_ACCESS,
        ]
