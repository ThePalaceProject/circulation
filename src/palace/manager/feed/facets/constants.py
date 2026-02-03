from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from flask_babel import lazy_gettext as _

if TYPE_CHECKING:
    from palace.manager.core.entrypoint import EntryPoint
    from palace.manager.sqlalchemy.model.library import Library


class FacetConstants:
    # A special constant, basically an additional rel, indicating that
    # an OPDS facet group represents different entry points into a
    # WorkList.
    ENTRY_POINT_REL = "http://librarysimplified.org/terms/rel/entrypoint"
    ENTRY_POINT_FACET_GROUP_NAME = "entrypoint"

    # Subset the collection by availability.
    AVAILABILITY_FACET_GROUP_NAME = "available"
    AVAILABLE_NOW = "now"
    AVAILABLE_ALL = "all"
    AVAILABLE_OPEN_ACCESS = "always"
    AVAILABLE_NOT_NOW = "not_now"  # Used only in QA jackpot feeds -- real patrons don't
    # want to see this.
    AVAILABILITY_FACETS = [
        AVAILABLE_NOW,
        AVAILABLE_ALL,
        AVAILABLE_OPEN_ACCESS,
    ]

    # The names of the order facets.
    ORDER_FACET_GROUP_NAME = "order"
    ORDER_TITLE = "title"
    ORDER_AUTHOR = "author"
    ORDER_LAST_UPDATE = "last_update"
    ORDER_ADDED_TO_COLLECTION = "added"
    ORDER_SERIES_POSITION = "series"
    ORDER_WORK_ID = "work_id"
    ORDER_RANDOM = "random"

    # Reverse variants of order facets (opposite sort direction)
    ORDER_TITLE_DESC = "title_desc"
    ORDER_AUTHOR_DESC = "author_desc"
    ORDER_ADDED_TO_COLLECTION_ASC = "added_asc"

    # Some order facets, like series and work id,
    # only make sense in certain contexts.
    # These are the options that can be enabled
    # for all feeds as a library-wide setting.
    ORDER_FACETS = {
        ORDER_TITLE,
        ORDER_AUTHOR,
        ORDER_ADDED_TO_COLLECTION,
    }

    # Maps base order facets to their reverse variant.
    # When a base facet is enabled, its reverse variant is automatically
    # available in feeds.
    ORDER_FACET_TO_REVERSE_VARIANT: dict[str, str] = {
        ORDER_TITLE: ORDER_TITLE_DESC,
        ORDER_AUTHOR: ORDER_AUTHOR_DESC,
        ORDER_ADDED_TO_COLLECTION: ORDER_ADDED_TO_COLLECTION_ASC,
    }

    ORDER_ASCENDING = "asc"
    ORDER_DESCENDING = "desc"

    # Most facets should be ordered in ascending order by default (A->Z), but
    # these should be ordered descending by default.
    ORDER_DESCENDING_BY_DEFAULT = (
        ORDER_ADDED_TO_COLLECTION,
        ORDER_LAST_UPDATE,
        # Reverse variants that sort descending (Z->A)
        ORDER_TITLE_DESC,
        ORDER_AUTHOR_DESC,
    )
    # Note: ORDER_ADDED_TO_COLLECTION_ASC is NOT in this tuple - it sorts ascending (oldest first)

    DISTRIBUTOR_FACETS_GROUP_NAME = "distributor"
    DISTRIBUTOR_ALL = "All"

    COLLECTION_NAME_FACETS_GROUP_NAME = "collectionName"
    COLLECTION_NAME_ALL = "All"

    FACETS_BY_GROUP = {
        AVAILABILITY_FACET_GROUP_NAME: AVAILABILITY_FACETS,
        ORDER_FACET_GROUP_NAME: ORDER_FACETS,
    }

    GROUP_DISPLAY_TITLES = {
        ORDER_FACET_GROUP_NAME: _("Sort by"),
        AVAILABILITY_FACET_GROUP_NAME: _("Availability"),
        DISTRIBUTOR_FACETS_GROUP_NAME: _("Distributor"),
        COLLECTION_NAME_FACETS_GROUP_NAME: _("Collection Name"),
    }

    GROUP_DESCRIPTIONS = {
        ORDER_FACET_GROUP_NAME: _("Allow patrons to sort by"),
        AVAILABILITY_FACET_GROUP_NAME: _("Allow patrons to filter availability to"),
        DISTRIBUTOR_FACETS_GROUP_NAME: _("Allow patrons to filter by distributor"),
        COLLECTION_NAME_FACETS_GROUP_NAME: _(
            "Allow patrons to filter by collection name"
        ),
    }

    # Display titles shown in OPDS feeds (include sort direction)
    FACET_DISPLAY_TITLES = {
        ORDER_TITLE: _("Title (A-Z)"),
        ORDER_AUTHOR: _("Author (A-Z)"),
        ORDER_LAST_UPDATE: _("Last Update"),
        ORDER_LICENSE_POOL_LAST_UPDATED: _("License Pool Last Updated"),
        ORDER_ADDED_TO_COLLECTION: _("Date Added (New-Old)"),
        ORDER_SERIES_POSITION: _("Series Position"),
        ORDER_WORK_ID: _("Work ID"),
        # Reverse variants
        ORDER_TITLE_DESC: _("Title (Z-A)"),
        ORDER_AUTHOR_DESC: _("Author (Z-A)"),
        ORDER_ADDED_TO_COLLECTION_ASC: _("Date Added (Old-New)"),
        # Availability facets
        AVAILABLE_NOW: _("Available now"),
        AVAILABLE_ALL: _("All"),
        AVAILABLE_OPEN_ACCESS: _("Yours to keep"),
    }

    # Simple titles for admin interface (no direction - both directions are enabled together)
    ORDER_FACET_ADMIN_TITLES = {
        ORDER_TITLE: _("Title"),
        ORDER_AUTHOR: _("Author"),
        ORDER_LAST_UPDATE: _("Last Update"),
        ORDER_ADDED_TO_COLLECTION: _("Date Added"),
        ORDER_SERIES_POSITION: _("Series Position"),
        ORDER_WORK_ID: _("Work ID"),
    }

    # For titles generated based on some runtime value
    FACET_DISPLAY_TITLES_DYNAMIC = {
        DISTRIBUTOR_FACETS_GROUP_NAME: lambda facet: facet.distributor,
        COLLECTION_NAME_FACETS_GROUP_NAME: lambda facet: facet.collection_name,
    }

    # Unless a library offers an alternate configuration, patrons will
    # see these facet groups.
    DEFAULT_ENABLED_FACETS = {
        ORDER_FACET_GROUP_NAME: [ORDER_AUTHOR, ORDER_TITLE, ORDER_ADDED_TO_COLLECTION],
        AVAILABILITY_FACET_GROUP_NAME: [
            AVAILABLE_ALL,
            AVAILABLE_NOW,
            AVAILABLE_OPEN_ACCESS,
        ],
        DISTRIBUTOR_FACETS_GROUP_NAME: [DISTRIBUTOR_ALL],
        COLLECTION_NAME_FACETS_GROUP_NAME: [COLLECTION_NAME_ALL],
    }

    # Unless a library offers an alternate configuration, these
    # facets will be the default selection for the facet groups.
    DEFAULT_FACET = {
        ORDER_FACET_GROUP_NAME: ORDER_AUTHOR,
        AVAILABILITY_FACET_GROUP_NAME: AVAILABLE_ALL,
        DISTRIBUTOR_FACETS_GROUP_NAME: DISTRIBUTOR_ALL,
        COLLECTION_NAME_FACETS_GROUP_NAME: COLLECTION_NAME_ALL,
    }

    SORT_ORDER_TO_OPENSEARCH_FIELD_NAME: dict[str, str | list[str]] = {
        ORDER_TITLE: "sort_title",
        ORDER_AUTHOR: "sort_author",
        ORDER_LAST_UPDATE: "last_update_time",
        ORDER_ADDED_TO_COLLECTION: "licensepools.availability_time",
        ORDER_SERIES_POSITION: ["series_position", "sort_title"],
        ORDER_WORK_ID: "_id",
        # Reverse variants map to the same fields (direction is handled separately)
        ORDER_TITLE_DESC: "sort_title",
        ORDER_AUTHOR_DESC: "sort_author",
        ORDER_ADDED_TO_COLLECTION_ASC: "licensepools.availability_time",
    }


class FacetConfig(FacetConstants):
    """A class that implements the facet-related methods of
    Library, and allows modifications to the enabled
    and default facets. For use when a controller needs to
    use a facet configuration different from the site-wide
    facets.
    """

    @classmethod
    def from_library(cls, library: Library) -> FacetConfig:
        enabled_facets: dict[str, list[str]] = {}
        for group in list(FacetConstants.DEFAULT_ENABLED_FACETS.keys()):
            enabled_facets[group] = library.enabled_facets(group)

        default_facets: dict[str, str] = {}
        for group in list(FacetConstants.DEFAULT_FACET.keys()):
            default_facets[group] = library.default_facet(group)

        return FacetConfig(enabled_facets, default_facets)

    def __init__(
        self,
        enabled_facets: Mapping[str, list[str]],
        default_facets: Mapping[str, str],
        entrypoints: Sequence[type[EntryPoint]] = (),
    ) -> None:
        self._enabled_facets = dict(enabled_facets)
        self._default_facets = dict(default_facets)
        self.entrypoints = entrypoints

    def enabled_facets(self, group_name: str) -> list[str] | None:
        return self._enabled_facets.get(group_name)

    def default_facet(self, group_name: str) -> str | None:
        return self._default_facets.get(group_name)

    def enable_facet(self, group_name: str, facet: str) -> None:
        self._enabled_facets.setdefault(group_name, [])
        if facet not in self._enabled_facets[group_name]:
            self._enabled_facets[group_name] += [facet]

    def set_default_facet(self, group_name: str, facet: str) -> None:
        """Add `facet` to the list of possible values for `group_name`, even
        if the library does not have that facet configured.
        """
        self.enable_facet(group_name, facet)
        self._default_facets[group_name] = facet
