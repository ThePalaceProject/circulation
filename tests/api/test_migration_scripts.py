import json

from core.facets import FacetConstants
from core.lane import Facets
from core.model import Library
from migartion_scripts import RandomSortOptionRemover
from tests.fixtures.database import DatabaseTransactionFixture


class TestRandomSortOptionRemover:
    """Contains tests ensuring that RandomSortOptionRemover correctly removes `random` sort option from CM."""

    @staticmethod
    def _get_library(db: DatabaseTransactionFixture) -> Library:
        """Return a library with randomly sorted facets.

        :return: Library with randomly sorted facets
        """
        library: Library = db.default_library()

        # Set the library's default sort option to `random`.
        library.default_facet_setting(
            Facets.ORDER_FACET_GROUP_NAME
        ).value = Facets.ORDER_RANDOM
        assert (
            library.default_facet(Facets.ORDER_FACET_GROUP_NAME) == Facets.ORDER_RANDOM
        )

        # Include `random` into the list of the library's available sort options.
        available_sort_options = FacetConstants.DEFAULT_ENABLED_FACETS.get(
            Facets.ORDER_FACET_GROUP_NAME, []
        )
        library.enabled_facets_setting(
            Facets.ORDER_FACET_GROUP_NAME
        ).value = json.dumps(available_sort_options + [Facets.ORDER_RANDOM])
        assert Facets.ORDER_RANDOM in library.enabled_facets(
            Facets.ORDER_FACET_GROUP_NAME
        )

        return library

    def test_random_sort_option_remover_removes_sort_options(
        self, db: DatabaseTransactionFixture
    ):
        """Ensure that RandomSortOptionRemover correctly removes `random` sort options from CM."""
        # Prepare a library with `random` set as the default sort option and part of the available sort options list.
        library = self._get_library(db)
        default_facet_order = FacetConstants.DEFAULT_FACET.get(
            Facets.ORDER_FACET_GROUP_NAME
        )

        # Run the script to remove `random` sort options.
        remover = RandomSortOptionRemover()
        remover.run(db.session)

        # Ensure that the default sort option changed and it's not `random` any more.
        assert (
            library.default_facet(Facets.ORDER_FACET_GROUP_NAME) == default_facet_order
        )
        assert (
            library.default_facet(Facets.ORDER_FACET_GROUP_NAME) != Facets.ORDER_RANDOM
        )

        # Ensure that `random` is not in the list of available sort options.
        assert Facets.ORDER_RANDOM not in library.enabled_facets(
            Facets.ORDER_FACET_GROUP_NAME
        )
