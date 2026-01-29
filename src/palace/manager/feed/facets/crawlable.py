from palace.manager.feed.facets.feed import Facets


class CrawlableFacets(Facets):
    """A special Facets class for crawlable feeds."""

    # These facet settings are definitive of a crawlable feed.
    # Library configuration settings don't matter.
    SETTINGS = {
        Facets.ORDER_FACET_GROUP_NAME: [
            Facets.ORDER_LAST_UPDATE,
            Facets.ORDER_LICENSE_POOL_LAST_UPDATED,
        ],
        Facets.AVAILABILITY_FACET_GROUP_NAME: Facets.AVAILABLE_ALL,
        Facets.DISTRIBUTOR_FACETS_GROUP_NAME: Facets.DISTRIBUTOR_ALL,
        Facets.COLLECTION_NAME_FACETS_GROUP_NAME: Facets.COLLECTION_NAME_ALL,
    }

    @classmethod
    def available_facets(cls, config, facet_group_name):
        facets = cls.SETTINGS[facet_group_name]

        if not isinstance(facets, list):
            facets = [facets]
        if (
            facet_group_name == Facets.DISTRIBUTOR_FACETS_GROUP_NAME
            or facet_group_name == Facets.COLLECTION_NAME_FACETS_GROUP_NAME
        ) and config is not None:
            facets.extend(config.enabled_facets(facet_group_name))

        return facets

    @classmethod
    def default_facet(cls, config, facet_group_name):
        facets = cls.SETTINGS[facet_group_name]
        if isinstance(facets, list):
            return facets[0]
        else:
            return facets
