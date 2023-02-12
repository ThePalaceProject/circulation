import json
import logging

import sqlalchemy.orm

from palace.core.facets import FacetConstants
from palace.core.lane import Facets
from palace.core.model import Library


class RandomSortOptionRemover:
    """Class designed to remove `random` sort options from Circulation Manager's library configuration."""

    def __init__(self) -> None:
        """Initialize a new instance of RandomSortOptionRemover class."""
        self._logger: logging.Logger = logging.getLogger(__name__)

    def _process_library_default_sort_option(self, library: Library) -> None:
        """Check the library's default sort option and, if it's `random`, replace it.

        :param library: Library object
        """
        default_facet_setting = library.default_facet_setting(
            Facets.ORDER_FACET_GROUP_NAME
        )

        self._logger.info(
            f"Library {library}'s default sort option: {default_facet_setting.value if default_facet_setting else None}"
        )

        if default_facet_setting and default_facet_setting.value == Facets.ORDER_RANDOM:
            default_facet_setting.value = FacetConstants.DEFAULT_FACET.get(
                Facets.ORDER_FACET_GROUP_NAME
            )

        self._logger.info(
            f"Library {library}'s new default sort option: {default_facet_setting.value}"
        )

    def _process_library_available_sort_options(self, library: Library) -> None:
        """Exclude `random` sort option from the library's available sort options.

        :param library: Library object
        """
        enabled_facets = library.enabled_facets(Facets.ORDER_FACET_GROUP_NAME)

        self._logger.info(
            f"Library {library}'s available sort options: {enabled_facets}"
        )

        if isinstance(enabled_facets, list) and Facets.ORDER_RANDOM in enabled_facets:
            library.enabled_facets_setting(
                Facets.ORDER_FACET_GROUP_NAME
            ).value = json.dumps(list(set(enabled_facets) - {Facets.ORDER_RANDOM}))

        enabled_facets = library.enabled_facets(Facets.ORDER_FACET_GROUP_NAME)

        self._logger.info(
            f"Library {library}'s updated available sort options: {enabled_facets}"
        )

    def run(self, db: sqlalchemy.orm.session.Session) -> None:
        """remove `random` sort options from Circulation Manager's library configuration.

        :param db: Database connection
        """
        libraries = db.query(Library).all()

        for library in libraries:
            self._logger.info(f"Started processing {library}")

            self._process_library_default_sort_option(library)
            self._process_library_available_sort_options(library)
            db.commit()

            self._logger.info(f"Finished processing {library}")
