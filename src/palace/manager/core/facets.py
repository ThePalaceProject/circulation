from flask_babel import lazy_gettext as _


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
    # Some order facets, like series and work id,
    # only make sense in certain contexts.
    # These are the options that can be enabled
    # for all feeds as a library-wide setting.
    ORDER_FACETS = [
        ORDER_TITLE,
        ORDER_AUTHOR,
        ORDER_ADDED_TO_COLLECTION,
    ]

    ORDER_ASCENDING = "asc"
    ORDER_DESCENDING = "desc"

    # Most facets should be ordered in ascending order by default (A>-Z), but
    # these dates should be ordered descending by default (new->old).
    ORDER_DESCENDING_BY_DEFAULT = [ORDER_ADDED_TO_COLLECTION, ORDER_LAST_UPDATE]

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

    FACET_DISPLAY_TITLES = {
        ORDER_TITLE: _("Title"),
        ORDER_AUTHOR: _("Author"),
        ORDER_LAST_UPDATE: _("Last Update"),
        ORDER_ADDED_TO_COLLECTION: _("Recently Added"),
        ORDER_SERIES_POSITION: _("Series Position"),
        ORDER_WORK_ID: _("Work ID"),
        AVAILABLE_NOW: _("Available now"),
        AVAILABLE_ALL: _("All"),
        AVAILABLE_OPEN_ACCESS: _("Yours to keep"),
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

    SORT_ORDER_TO_OPENSEARCH_FIELD_NAME = {
        ORDER_TITLE: "sort_title",
        ORDER_AUTHOR: "sort_author",
        ORDER_LAST_UPDATE: "last_update_time",
        ORDER_ADDED_TO_COLLECTION: "licensepools.availability_time",
        ORDER_SERIES_POSITION: ["series_position", "sort_title"],
        ORDER_WORK_ID: "_id",
    }


class FacetConfig(FacetConstants):
    """A class that implements the facet-related methods of
    Library, and allows modifications to the enabled
    and default facets. For use when a controller needs to
    use a facet configuration different from the site-wide
    facets.
    """

    @classmethod
    def from_library(cls, library):
        enabled_facets = dict()
        for group in list(FacetConstants.DEFAULT_ENABLED_FACETS.keys()):
            enabled_facets[group] = library.enabled_facets(group)

        default_facets = dict()
        for group in list(FacetConstants.DEFAULT_FACET.keys()):
            default_facets[group] = library.default_facet(group)

        return FacetConfig(enabled_facets, default_facets)

    def __init__(self, enabled_facets, default_facets, entrypoints=[]):
        self._enabled_facets = dict(enabled_facets)
        self._default_facets = dict(default_facets)
        self.entrypoints = entrypoints

    def enabled_facets(self, group_name):
        return self._enabled_facets.get(group_name)

    def default_facet(self, group_name):
        return self._default_facets.get(group_name)

    def enable_facet(self, group_name, facet):
        self._enabled_facets.setdefault(group_name, [])
        if facet not in self._enabled_facets[group_name]:
            self._enabled_facets[group_name] += [facet]

    def set_default_facet(self, group_name, facet):
        """Add `facet` to the list of possible values for `group_name`, even
        if the library does not have that facet configured.
        """
        self.enable_facet(group_name, facet)
        self._default_facets[group_name] = facet
