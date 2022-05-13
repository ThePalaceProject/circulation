import json
from importlib import import_module

import pytest
from sqlalchemy.exc import IntegrityError

from core.facets import FacetConstants
from core.lane import Facets
from core.model import Library
from core.model.admin import Admin
from core.testing import DatabaseTest
from migartion_scripts import RandomSortOptionRemover


class TestRandomSortOptionRemover(DatabaseTest):
    """Contains tests ensuring that RandomSortOptionRemover correctly removes `random` sort option from CM."""

    def _get_library(self) -> Library:
        """Return a library with randomly sorted facets.

        :return: Library with randomly sorted facets
        """
        library: Library = self._default_library

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

    def test_random_sort_option_remover_removes_sort_options(self):
        """Ensure that RandomSortOptionRemover correctly removes `random` sort options from CM."""
        # Prepare a library with `random` set as the default sort option and part of the available sort options list.
        library = self._get_library()
        default_facet_order = FacetConstants.DEFAULT_FACET.get(
            Facets.ORDER_FACET_GROUP_NAME
        )

        # Run the script to remove `random` sort options.
        remover = RandomSortOptionRemover()
        remover.run(self._db)

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


class TestCreateUniqueEmailConstraint(DatabaseTest):

    migration = import_module("migration.20220509-admin-email-unique-constraint")

    def setup_method(self):
        super().setup_method()
        admin = Admin(email="test@example.com")
        self._db.add(admin)

    def test_create_unique_email_constraint(self):
        print("migration", self.migration)
        success = self.migration.create_unique_email_constraint(self._db)
        assert success == True

        # rerun should return false
        success = self.migration.create_unique_email_constraint(self._db)
        assert success == False

    def test_fail_on_duplicate_email(self):
        admin = Admin(email="TEst@example.com")
        self._db.add(admin)

        # Duplicate email exists for UPPER()
        with pytest.raises(IntegrityError):
            self.migration.create_unique_email_constraint(self._db)
