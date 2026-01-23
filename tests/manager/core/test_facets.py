from palace.manager.feed.facets.constants import FacetConfig, FacetConstants as Facets
from tests.fixtures.database import DatabaseTransactionFixture


class TestFacetConfig:
    def test_from_library(self, db: DatabaseTransactionFixture):
        library = db.default_library()
        order_by = Facets.ORDER_FACET_GROUP_NAME

        # When you create a FacetConfig from a Library it implements
        # enabled_facets() and default_facet() the same as the Library
        # does.
        config = FacetConfig.from_library(library)
        assert Facets.ORDER_RANDOM not in config.enabled_facets(order_by)
        for group in list(Facets.DEFAULT_FACET.keys()):
            assert config.enabled_facets(group) == library.enabled_facets(group)
            assert config.default_facet(group) == library.default_facet(group)

        # If you then modify the FacetConfig, it deviates from what
        # the Library would do.
        config.set_default_facet(order_by, Facets.ORDER_RANDOM)
        assert Facets.ORDER_RANDOM == config.default_facet(order_by)
        assert library.default_facet(order_by) != Facets.ORDER_RANDOM
        assert Facets.ORDER_RANDOM in config.enabled_facets(order_by)

    def test_enable_facet(self, db: DatabaseTransactionFixture):
        # You can enable a facet without making it the default for its
        # facet group.
        order_by = Facets.ORDER_FACET_GROUP_NAME
        config = FacetConfig.from_library(db.default_library())
        config.enable_facet(order_by, Facets.ORDER_RANDOM)
        assert Facets.ORDER_RANDOM in config.enabled_facets(order_by)
        assert config.default_facet(order_by) != Facets.ORDER_RANDOM
