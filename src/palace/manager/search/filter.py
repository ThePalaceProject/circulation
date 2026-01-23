from __future__ import annotations

import datetime
import time
from collections import defaultdict

from opensearchpy import SF
from opensearchpy.helpers.query import (
    Bool,
    Exists,
    MatchNone,
    Nested,
    Query as BaseQuery,
    Term,
    Terms,
)

from palace.manager.core.classifier import Classifier
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.feed.facets.constants import FacetConstants
from palace.manager.search.revision_directory import SearchRevisionDirectory
from palace.manager.search.search_base import SearchBase
from palace.manager.sqlalchemy.constants import IntegrationConfigurationConstants
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePoolStatus
from palace.manager.sqlalchemy.util import numericrange_to_tuple
from palace.manager.util.datetime_helpers import from_timestamp


class Filter(SearchBase):
    """A filter for search results.

    This covers every reason you might want to not exclude a search
    result that would otherise match the query string -- wrong media,
    wrong language, not available in the patron's library, etc.

    This also covers every way you might want to order the search
    results: either by relevance to the search query (the default), or
    by a specific field (e.g. author) as described by a Facets object.

    It also covers additional calculated values you might need when
    presenting the search results.
    """

    # When search results include known script fields, we need to
    # wrap the works we would be returning in WorkSearchResults so
    # the useful information from the search engine isn't lost.
    KNOWN_SCRIPT_FIELDS = ["last_update"]

    # In general, someone looking for things "by this person" is
    # probably looking for one of these roles.
    AUTHOR_MATCH_ROLES = list(Contributor.AUTHOR_ROLES) + [
        Contributor.Role.NARRATOR,
        Contributor.Role.EDITOR,
        Contributor.Role.DIRECTOR,
        Contributor.Role.ACTOR,
    ]

    @classmethod
    def from_worklist(cls, _db, worklist, facets):
        """Create a Filter that finds only works that belong in the given
        WorkList and EntryPoint.

        :param worklist: A WorkList
        :param facets: A SearchFacets object.
        """
        library = worklist.get_library(_db)
        # For most configuration settings there is a single value --
        # either defined on the WorkList or defined by its parent.
        inherit_one = worklist.inherited_value
        media = inherit_one("media")
        languages = inherit_one("languages")
        fiction = inherit_one("fiction")
        audiences = inherit_one("audiences")
        target_age = inherit_one("target_age")
        collections = inherit_one("collection_ids") or library

        license_datasource_id = inherit_one("license_datasource_id")

        # For genre IDs and CustomList IDs, we might get a separate
        # set of restrictions from every item in the WorkList hierarchy.
        # _All_ restrictions must be met for a work to match the filter.
        inherit_some = worklist.inherited_values
        genre_id_restrictions = inherit_some("genre_ids")
        customlist_id_restrictions = inherit_some("customlist_ids")

        if library is None:
            allow_holds = True
        else:
            allow_holds = library.settings.allow_holds
        return cls(
            collections,
            media,
            languages,
            fiction,
            audiences,
            target_age,
            genre_id_restrictions,
            customlist_id_restrictions,
            facets,
            allow_holds=allow_holds,
            license_datasource=license_datasource_id,
            lane_building=True,
            library=library,
        )

    def __init__(
        self,
        collections=None,
        media=None,
        languages=None,
        fiction=None,
        audiences=None,
        target_age=None,
        genre_restriction_sets=None,
        customlist_restriction_sets=None,
        facets=None,
        script_fields=None,
        **kwargs,
    ):
        """Constructor.

        All arguments are optional. Passing in an empty set of
        arguments will match everything in the search index that
        matches the universal filters (e.g. works must be
        presentation-ready).

        :param collections: Find only works that are licensed to one of
        these Collections.

        :param media: Find only works in this list of media (use the
        constants from Edition such as Edition.BOOK_MEDIUM).

        :param languages: Find only works in these languages (use
        ISO-639-2 alpha-3 codes).

        :param fiction: Find only works with this fiction status.

        :param audiences: Find only works with a target audience in this list.

        :param target_age: Find only works with a target age in this
        range. (Use a 2-tuple, or a number to represent a specific
        age.)

        :param genre_restriction_sets: A sequence of lists of Genre
        objects or IDs. Each list represents an independent
        restriction. For each restriction, a work only matches if it's
        in one of the appropriate Genres.

        :param customlist_restriction_sets: A sequence of lists of
        CustomList objects or IDs. Each list represents an independent
        restriction. For each restriction, a work only matches if it's
        in one of the appropriate CustomLists.

        :param facets: A faceting object that can put further restrictions
        on the match.

        :param script_fields: A list of registered script fields to
        run on the search results.

        (These minor arguments were made into unnamed keyword arguments
        to avoid cluttering the method signature:)

        :param excluded_audiobook_data_sources: A list of DataSources that
        provide audiobooks known to be unsupported on this system.
        Such audiobooks will always be excluded from results.

        :param identifiers: A list of Identifier or IdentifierData
        objects. Only books associated with one of these identifiers
        will be matched.

        :param allow_holds: If this is False, books with no available
        copies will be excluded from results.

        :param series: If this is set to a string, only books in a matching
        series will be included. If set to True, books that belong to _any_
        series will be included.

        :param author: If this is set to a Contributor or
        ContributorData, then only books where this person had an
        authorship role will be included.

        :param license_datasource: If this is set to a DataSource,
        only books with LicensePools from that DataSource will be
        included.

        :param updated_after: If this is set to a datetime, only books
        whose Work records (~bibliographic metadata) have been updated since
        that time will be included in results.

        :param match_nothing: If this is set to True, the search will
        not even be performed -- we know for some other reason that an
        empty set of search results should be returned.
        """

        if isinstance(collections, Library):
            # Find all works in this Library's active collections.
            collections = collections.active_collections
        self.collection_ids = self._filter_ids(collections)

        self.media = media
        self.languages = languages
        self.fiction = fiction
        self._audiences = audiences

        if target_age:
            if isinstance(target_age, int):
                self.target_age = (target_age, target_age)
            elif isinstance(target_age, tuple) and len(target_age) == 2:
                self.target_age = target_age
            else:
                # It's a SQLAlchemy range object. Convert it to a tuple.
                self.target_age = numericrange_to_tuple(target_age)
        else:
            self.target_age = None

        # Filter the lists of database IDs to make sure we aren't
        # storing any database objects.
        if genre_restriction_sets:
            self.genre_restriction_sets = [
                self._filter_ids(x) for x in genre_restriction_sets
            ]
        else:
            self.genre_restriction_sets = []
        if customlist_restriction_sets:
            self.customlist_restriction_sets = [
                self._filter_ids(x) for x in customlist_restriction_sets
            ]
        else:
            self.customlist_restriction_sets = []

        # Pull less-important values out of the keyword arguments.
        self.allow_holds = kwargs.pop("allow_holds", True)

        self.updated_after = kwargs.pop("updated_after", None)

        self.series = kwargs.pop("series", None)

        self.author = kwargs.pop("author", None)

        self.min_score = kwargs.pop("min_score", None)

        self.match_nothing = kwargs.pop("match_nothing", False)

        license_datasources = kwargs.pop("license_datasource", None)
        self.license_datasources = self._filter_ids(license_datasources)

        identifiers = kwargs.pop("identifiers", [])
        self.identifiers = list(self._scrub_identifiers(identifiers))

        self.lane_building = kwargs.pop("lane_building", False)

        # Store library-related filtering information.
        # We store the ID and settings values rather than the Library ORM object
        # to avoid session detachment issues if the Filter outlives the session.
        library: Library | None = kwargs.pop("library", None)
        self.library_id: int | None = library.id if library else None
        # Store content filtering settings from the library
        if library:
            settings = library.settings
            self.filtered_audiences: list[str] = settings.filtered_audiences
            self.filtered_genres: list[str] = settings.filtered_genres
        else:
            self.filtered_audiences = []
            self.filtered_genres = []

        # At this point there should be no keyword arguments -- you can't pass
        # whatever you want into this method.
        if kwargs:
            raise ValueError("Unknown keyword arguments: %r" % kwargs)

        # Establish default values for additional restrictions that may be
        # imposed by the Facets object.
        self.minimum_featured_quality = 0
        self.availability = None
        self.subcollection = None
        self.order = None
        self.order_ascending = False

        self.script_fields = script_fields or dict()

        # Give the Facets object a chance to modify any or all of this
        # information.
        if facets:
            facets.modify_search_filter(self)
            self.scoring_functions = facets.scoring_functions(self)
            self.search_type = getattr(facets, "search_type", "default")
        else:
            self.scoring_functions = []
            self.search_type = "default"

        # JSON type searches are exact matches and do not have scoring
        if self.search_type == "json":
            self.min_score = None

    @property
    def audiences(self):
        """Return the appropriate audiences for this query.

        This will be whatever audiences were provided, but it will
        probably also include the 'All Ages' audience.
        """

        if not self._audiences:
            return self._audiences

        as_is = self._audiences
        if isinstance(as_is, (bytes, str)):
            as_is = [as_is]

        # At this point we know we have a specific list of audiences.
        # We're either going to return that list as-is, or we'll
        # return that list plus ALL_AGES.
        with_all_ages = list(as_is) + [Classifier.AUDIENCE_ALL_AGES]

        if Classifier.AUDIENCE_ALL_AGES in as_is:
            # ALL_AGES is explicitly included.
            return as_is

        # If YOUNG_ADULT or ADULT is an audience, then ALL_AGES is
        # always going to be an additional audience.
        if any(
            x in as_is
            for x in [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_ADULT]
        ):
            return with_all_ages

        # At this point, if CHILDREN is _not_ included, we know that
        # ALL_AGES is not included. Specifically, ALL_AGES content
        # does _not_ belong in ADULTS_ONLY or RESEARCH.
        if Classifier.AUDIENCE_CHILDREN not in as_is:
            return as_is

        # Now we know that CHILDREN is an audience. It's going to come
        # down to the upper bound on the target age.
        if (
            self.target_age
            and self.target_age[1] is not None
            and self.target_age[1] < Classifier.ALL_AGES_AGE_CUTOFF
        ):
            # The audience for this query does not include any kids
            # who are expected to have the reading fluency necessary
            # for ALL_AGES books.
            return as_is
        return with_all_ages

    def build(self, _chain_filters=None):
        """Convert this object to an Opensearch Filter object.

        :return: A 2-tuple (filter, nested_filters). Filters on fields
           within nested documents (such as
           'licensepools.collection_id') must be applied as subqueries
           to the query that will eventually be created from this
           filter. `nested_filters` is a dictionary that maps a path
           to a list of filters to apply to that path.

        :param _chain_filters: Mock function to use instead of
            Filter._chain_filters
        """

        # Since a Filter object can be modified after it's created, we
        # need to scrub all the inputs, whether or not they were
        # scrubbed in the constructor.
        scrub_list = self._scrub_list
        filter_ids = self._filter_ids

        chain = _chain_filters or self._chain_filters

        f = None
        nested_filters = defaultdict(list)
        if self.match_nothing:
            # This Filter should match nothing. There's no need to
            # get fancy.
            return MatchNone(), nested_filters

        collection_ids = filter_ids(self.collection_ids)
        if collection_ids:
            collection_match = Terms(**{"licensepools.collection_id": collection_ids})
            nested_filters["licensepools"].append(collection_match)

        license_datasources = filter_ids(self.license_datasources)
        if license_datasources:
            datasource_match = Terms(
                **{"licensepools.data_source_id": license_datasources}
            )
            nested_filters["licensepools"].append(datasource_match)

        if self.author is not None:
            nested_filters["contributors"].append(self.author_filter)

        if self.library_id:
            f = chain(
                f, Bool(must_not=[Terms(**{"suppressed_for": [self.library_id]})])
            )

        # Apply library-level content filtering based on library settings.
        # This excludes works matching filtered audiences or genres.
        if self.filtered_audiences:
            excluded_audiences = scrub_list(self.filtered_audiences)
            f = chain(f, Bool(must_not=[Terms(audience=excluded_audiences)]))
        if self.filtered_genres:
            # Genres are nested documents, so we need a Nested query
            genre_exclusion = Nested(
                path="genres",
                query=Terms(**{"genres.name": self.filtered_genres}),
            )
            f = chain(f, Bool(must_not=[genre_exclusion]))

        if self.media:
            f = chain(f, Terms(medium=scrub_list(self.media)))

        if self.languages:
            f = chain(f, Terms(language=scrub_list(self.languages)))

        if self.fiction is not None:
            if self.fiction:
                value = "fiction"
            else:
                value = "nonfiction"
            f = chain(f, Term(fiction=value))

        if self.series:
            if self.series is True:
                # The book must belong to _some_ series.
                #
                # That is, series must exist (have a non-null value) and
                # have a value other than the empty string.
                f = chain(f, Exists(field="series"))
                f = chain(f, Bool(must_not=[Term(**{"series.keyword": ""})]))
            else:
                f = chain(f, Term(**{"series.keyword": self.series}))

        if self.audiences:
            f = chain(f, Terms(audience=scrub_list(self.audiences)))
        else:
            research = self._scrub(Classifier.AUDIENCE_RESEARCH)
            f = chain(f, Bool(must_not=[Term(audience=research)]))

        target_age_filter = self.target_age_filter
        if target_age_filter:
            f = chain(f, self.target_age_filter)

        for genre_ids in self.genre_restriction_sets:
            ids = filter_ids(genre_ids)
            nested_filters["genres"].append(
                Terms(**{"genres.term": filter_ids(genre_ids)})
            )

        for customlist_ids in self.customlist_restriction_sets:
            ids = filter_ids(customlist_ids)
            nested_filters["customlists"].append(Terms(**{"customlists.list_id": ids}))

        open_access = Term(**{"licensepools.open_access": True})
        if self.availability == FacetConstants.AVAILABLE_NOW:
            # Only open-access books and books with currently available
            # copies should be displayed.
            available = Term(**{"licensepools.available": True})
            nested_filters["licensepools"].append(
                Bool(should=[open_access, available], minimum_should_match=1)
            )
        elif self.availability == FacetConstants.AVAILABLE_OPEN_ACCESS:
            # Only open-access books should be displayed.
            nested_filters["licensepools"].append(open_access)
        elif self.availability == FacetConstants.AVAILABLE_NOT_NOW:
            # Only books that are _not_ currently available should be displayed.
            not_open_access = Term(**{"licensepools.open_access": False})
            licensed = Term(**{"licensepools.licensed": True})
            not_available = Term(**{"licensepools.available": False})
            nested_filters["licensepools"].append(
                Bool(must=[not_open_access, licensed, not_available])
            )

        if self.identifiers:
            # Check every identifier for a match.
            clauses = []
            for identifier in self._scrub_identifiers(self.identifiers):
                subclauses = []
                # Both identifier and type must match for the match
                # to count.
                for name, value in (
                    ("identifier", identifier.identifier),
                    ("type", identifier.type),
                ):
                    subclauses.append(Term(**{"identifiers.%s" % name: value}))
                clauses.append(Bool(must=subclauses))

            # At least one the identifiers must match for the work to
            # match.
            identifier_f = Bool(should=clauses, minimum_should_match=1)
            nested_filters["identifiers"].append(identifier_f)

        # If holds are not allowed, only license pools that are
        # currently available should be considered.
        if not self.allow_holds:
            licenses_available = Term(**{"licensepools.available": True})
            currently_available = Bool(should=[licenses_available, open_access])
            nested_filters["licensepools"].append(currently_available)

        # Perhaps only books whose bibliographic metadata was updated
        # recently should be included.
        if self.updated_after:
            # 'last update_time' is indexed as a number of seconds, but
            # .last_update is probably a datetime. Convert it here.
            updated_after = self.updated_after
            if isinstance(updated_after, datetime.datetime):
                updated_after = (updated_after - from_timestamp(0)).total_seconds()
            last_update_time_query = self._match_range(
                "last_update_time", "gte", updated_after
            )
            f = chain(f, Bool(must=last_update_time_query))

        return f, nested_filters

    @classmethod
    def universal_base_filter(cls, _chain_filters=None):
        """Build a set of restrictions on the main search document that are
        always applied, even in the absence of other filters.

        :param _chain_filters: Mock function to use instead of
            Filter._chain_filters

        :return: A Filter object.

        """

        _chain_filters = _chain_filters or cls._chain_filters

        base_filter = None

        # We only want to show works that are presentation-ready.
        base_filter = _chain_filters(base_filter, Term(**{"presentation_ready": True}))

        return base_filter

    @classmethod
    def universal_nested_filters(cls):
        """Build a set of restrictions on subdocuments that are
        always applied, even in the absence of other filters.
        """
        nested_filters = defaultdict(list)

        # TODO: It would be great to be able to filter out
        # LicensePools that have no delivery mechanisms. That's the
        # only part of Collection.restrict_to_ready_deliverable_works
        # not already implemented in this class.

        # We don't want to consider license pools that have been
        # suppressed, or of which there are currently no licensed
        # copies. This might lead to a Work being filtered out
        # entirely.
        #
        # It's easier to stay consistent by indexing all Works and
        # filtering them out later, than to do it by adding and
        # removing works from the index.
        not_suppressed = Term(**{"licensepools.suppressed": False})
        nested_filters["licensepools"].append(not_suppressed)

        active_status = Term(**{"licensepools.status": LicensePoolStatus.ACTIVE})
        nested_filters["licensepools"].append(active_status)

        return nested_filters

    @property
    def sort_order(self):
        """Create a description, for use in an Opensearch document,
        explaining how search results should be ordered.

        :return: A list of dictionaries, each dictionary mapping a
            field name to an explanation of how to sort that
            field. Usually the explanation is a simple string, either
            'asc' or 'desc'.
        """
        if not self.order:
            return []

        # These sort order fields are inserted as necessary between
        # the primary sort order field and the tiebreaker field (work
        # ID). This makes it more likely that the sort order makes
        # sense to a human, by putting off the opaque tiebreaker for
        # as long as possible. For example, a feed sorted by author
        # will be secondarily sorted by title and work ID, not just by
        # work ID.
        default_sort_order = ["sort_author", "sort_title", "work_id"]

        order_field_keys = self.order
        if not isinstance(order_field_keys, list):
            order_field_keys = [order_field_keys]
        order_fields = [self._make_order_field(key) for key in order_field_keys]

        # Apply any parts of the default sort order not yet covered,
        # concluding (in most cases) with work_id, the tiebreaker field.
        for x in default_sort_order:
            if x not in order_field_keys:
                order_fields.append({x: "asc"})
        return order_fields

    @property
    def asc(self):
        "Convert order_ascending to Opensearch-speak."
        if self.order_ascending is False:
            return "desc"
        else:
            return "asc"

    def _make_order_field(self, key):
        if key == "last_update_time":
            # Sorting by last_update_time may be very simple or very
            # complex, depending on whether or not the filter
            # involves collection or list membership.
            if self.collection_ids or self.customlist_restriction_sets:
                # The complex case -- use a helper method.
                return self._last_update_time_order_by
            else:
                # The simple case, handled below.
                pass

        if "." not in key:
            # A simple case.
            return {key: self.asc}

        # At this point we're sorting by a nested field.
        nested = None
        if key == "licensepools.availability_time":
            nested, mode = self._availability_time_sort_order
        elif key == "licensepools.last_updated":
            nested, mode = self._licensepool_last_updated_sort_order
        else:
            raise ValueError("I don't know how to sort by %s." % key)
        sort_description = dict(order=self.asc, mode=mode)
        if nested:
            sort_description["nested"] = nested
        return {key: sort_description}

    @property
    def _availability_time_sort_order(self):
        # We're sorting works by the time they became
        # available to a library. This means we only want to
        # consider the availability times of license pools
        # found in one of the library's collections.
        nested = None
        collection_ids = self._filter_ids(self.collection_ids)
        if collection_ids:
            nested = dict(
                path="licensepools",
                filter=dict(terms={"licensepools.collection_id": collection_ids}),
            )
        # If a book shows up in multiple collections, we're only
        # interested in the collection that had it the earliest.
        mode = "min"
        return nested, mode

    @property
    def _licensepool_last_updated_sort_order(self):
        # We're sorting works by the most recent update time among
        # license pools in relevant collections.
        nested = None
        collection_ids = self._filter_ids(self.collection_ids)
        if collection_ids:
            nested = dict(
                path="licensepools",
                filter=dict(terms={"licensepools.collection_id": collection_ids}),
            )
        mode = "max"
        return nested, mode

    @property
    def last_update_time_script_field(self):
        """Return the configuration for a script field that calculates the
        'last update' time of a work. An 'update' happens when the
        work's metadata is changed, when it's added to a collection
        used by this Filter, or when it's added to one of the lists
        used by this Filter.
        """
        # First, set up the parameters we're going to pass into the
        # script -- a list of custom list IDs relevant to this filter,
        # and a list of collection IDs relevant to this filter.
        collection_ids = self._filter_ids(self.collection_ids)

        # The different restriction sets don't matter here. The filter
        # part of the query ensures that we only match works present
        # on one list in every restriction set. Here, we need to find
        # the latest time a work was added to _any_ relevant list.
        all_list_ids = set()
        for restriction in self.customlist_restriction_sets:
            all_list_ids.update(self._filter_ids(restriction))
        nested = dict(
            path="customlists",
            filter=dict(terms={"customlists.list_id": list(all_list_ids)}),
        )
        params = dict(collection_ids=collection_ids, list_ids=list(all_list_ids))
        # Messy, but this is the only way to get the "current mapping" for the index
        script_name = (
            SearchRevisionDirectory.create().highest().script_name("work_last_update")
        )
        return dict(script=dict(stored=script_name, params=params))

    @property
    def _last_update_time_order_by(self):
        """We're sorting works by the time of their 'last update'.

        Add the 'last update' field to the dictionary of script fields
        (so we can use the result afterwards), and define it a second
        time as the script to use for a sort value.
        """
        field = self.last_update_time_script_field
        if not "last_update" in self.script_fields:
            self.script_fields["last_update"] = field
        return dict(
            _script=dict(
                type="number",
                script=field["script"],
                order=self.asc,
            ),
        )

    # The Painless script to generate a 'featurability' score for
    # a work.
    #
    # A higher-quality work is more featurable. But we don't want
    # to constantly feature the very highest-quality works, and if
    # there are no high-quality works, we want medium-quality to
    # outrank low-quality.
    #
    # So we establish a cutoff -- the minimum featured quality --
    # beyond which a work is considered 'featurable'. All featurable
    # works get the same (high) score.
    #
    # Below that point, we prefer higher-quality works to
    # lower-quality works, such that a work's score is proportional to
    # the square of its quality.
    #
    # We need work quality to perform the calculation, so we provide
    # a default, in case the work doesn't have one.
    FEATURABLE_SCRIPT_DEFAULT_WORK_QUALITY = 0.001
    FEATURABLE_SCRIPT = "Math.pow(Math.min({cutoff:0.5f}, doc['quality'].size() != 0 ? doc['quality'].value : {default_quality}), {exponent:0.5f}) * 5"

    # Used in tests to deactivate the random component of
    # featurability_scoring_functions.
    DETERMINISTIC = object()

    def featurability_scoring_functions(self, random_seed):
        """Generate scoring functions that weight works randomly, but
        with 'more featurable' works tending to be at the top.
        """

        exponent = 2
        cutoff = self.minimum_featured_quality**exponent
        script = self.FEATURABLE_SCRIPT.format(
            cutoff=cutoff,
            exponent=exponent,
            default_quality=self.FEATURABLE_SCRIPT_DEFAULT_WORK_QUALITY,
        )
        quality_field = SF("script_score", script=dict(source=script))

        # Currently available works are more featurable.
        available = Term(**{"licensepools.available": True})
        nested = Nested(path="licensepools", query=available)
        available_now = dict(filter=nested, weight=5)

        # Works with higher lane priority scores are more featurable
        lane_priority_level = SF(
            "field_value_factor",
            field="lane_priority_level",
            factor=1,
            modifier="none",
            missing=IntegrationConfigurationConstants.DEFAULT_LANE_PRIORITY_LEVEL,  # assume default if missing
        )

        function_scores = [
            quality_field,
            available_now,
            lane_priority_level,
        ]

        # Random chance can boost a lower-quality work, but not by
        # much -- this mainly ensures we don't get the exact same
        # books every time.

        if random_seed != self.DETERMINISTIC:
            random = SF(
                "random_score",
                seed=random_seed or int(time.time()),
                field="work_id",
                weight=1.1,
            )
            function_scores.append(random)

        if self.customlist_restriction_sets:
            list_ids = set()
            for restriction in self.customlist_restriction_sets:
                list_ids.update(restriction)
            # We're looking for works on certain custom lists. A work
            # that's _featured_ on one of these lists will be boosted
            # quite a lot versus one that's not.
            featured = Term(**{"customlists.featured": True})
            on_list = Terms(**{"customlists.list_id": list(list_ids)})
            featured_on_list = Bool(must=[featured, on_list])
            nested = Nested(path="customlists", query=featured_on_list)
            featured_on_relevant_list = dict(filter=nested, weight=11)
            function_scores.append(featured_on_relevant_list)
        return function_scores

    @property
    def target_age_filter(self):
        """Helper method to generate the target age subfilter.

        It's complicated because it has to handle cases where the upper
        or lower bound on target age is missing (indicating there is no
        upper or lower bound).
        """
        if not self.target_age:
            return None
        lower, upper = self.target_age
        if lower is None and upper is None:
            return None

        def does_not_exist(field):
            """A filter that matches if there is no value for `field`."""
            return Bool(must_not=[Exists(field=field)])

        def or_does_not_exist(clause, field):
            """Either the given `clause` matches or the given field
            does not exist.
            """
            return Bool(should=[clause, does_not_exist(field)], minimum_should_match=1)

        clauses = []

        both_limits = lower is not None and upper is not None

        if (
            self.lane_building
            and self.audiences
            and Classifier.AUDIENCE_CHILDREN in self.audiences
            and both_limits
        ):
            # If children is audience we want only works with defined age range that matches lane's range
            clauses.append(self._match_range("target_age.lower", "gte", lower))
            clauses.append(self._match_range("target_age.upper", "lte", upper))

            return Bool(must=clauses)

        if upper is not None:
            lower_does_not_exist = does_not_exist("target_age.lower")
            lower_in_range = self._match_range("target_age.lower", "lte", upper)
            lower_match = or_does_not_exist(lower_in_range, "target_age.lower")
            clauses.append(lower_match)

        if lower is not None:
            upper_does_not_exist = does_not_exist("target_age.upper")
            upper_in_range = self._match_range("target_age.upper", "gte", lower)
            upper_match = or_does_not_exist(upper_in_range, "target_age.upper")
            clauses.append(upper_match)

        if not clauses:
            # Neither upper nor lower age must match.
            return None

        if len(clauses) == 1:
            # Upper or lower age must match, but not both.
            return clauses[0]

        # Both upper and lower age must match.
        return Bool(must=clauses)

    @property
    def author_filter(self):
        """Build a filter that matches a 'contributors' subdocument only
        if it represents an author-level contribution by self.author.
        """
        if not self.author:
            return None
        authorship_role = Terms(**{"contributors.role": self.AUTHOR_MATCH_ROLES})
        clauses = []
        for field, value in [
            ("sort_name.keyword", self.author.sort_name),
            ("display_name.keyword", self.author.display_name),
            ("viaf", self.author.viaf),
            ("lc", self.author.lc),
        ]:
            if not value or value == Edition.UNKNOWN_AUTHOR:
                continue
            clauses.append(Term(**{"contributors.%s" % field: value}))

        same_person = Bool(should=clauses, minimum_should_match=1)
        return Bool(must=[authorship_role, same_person])

    @classmethod
    def _scrub(cls, s):
        """Modify a string for use in a filter match.

        e.g. "Young Adult" becomes "youngadult"

        :param s: The string to modify.
        """
        if not s:
            return s
        return s.lower().replace(" ", "")

    @classmethod
    def _scrub_list(cls, s):
        """The same as _scrub, except it always outputs
        a list of items.
        """
        if s is None:
            return []
        if isinstance(s, (bytes, str)):
            s = [s]
        return [cls._scrub(x) for x in s]

    @classmethod
    def _filter_ids(cls, ids):
        """Process a list of database objects, provided either as their
        IDs or as the objects themselves.

        :return: A list of IDs, or None if nothing was provided.
        """
        # Generally None means 'no restriction', while an empty list
        # means 'one of the values in this empty list' -- in other
        # words, they are opposites.
        if ids is None:
            return None

        processed = []

        if not isinstance(ids, list) and not isinstance(ids, set):
            ids = [ids]

        for id in ids:
            if not isinstance(id, int):
                # Turn a database object into an ID.
                id = id.id
            processed.append(id)
        return processed

    @classmethod
    def _scrub_identifiers(cls, identifiers):
        """Convert a mixed list of Identifier and IdentifierData objects
        into IdentifierData.
        """
        for i in identifiers:
            yield IdentifierData.from_identifier(i)

    @classmethod
    def _chain_filters(cls, existing, new):
        """Either chain two filters together or start a new chain."""
        if existing:
            # We're combining two filters.
            new = existing & new
        else:
            # There was no previous filter -- the 'new' one is it.
            pass
        return new


class SuppressedWorkFilter(Filter):
    """A Filter that matches ONLY works that are suppressed or policy-filtered for a library.

    This inverts the normal Filter behavior, which excludes suppressed/filtered works.
    Used by the admin suppressed feed search to search within hidden works.

    This filter intentionally only uses a subset of Filter properties:
    - collection_ids: Scopes results to the library's collections
    - library_id: Matches works manually suppressed for this library
    - filtered_audiences: Matches works with excluded audiences
    - filtered_genres: Matches works with excluded genres

    Other Filter properties (languages, media, audiences, etc.) are intentionally
    ignored since the admin suppressed feed should show all hidden works regardless
    of other filtering criteria.
    """

    def build(
        self, _chain_filters: None = None
    ) -> tuple[BaseQuery | None, defaultdict]:
        """Build filter that matches suppressed/filtered works.

        This overrides the parent build() to use 'should' clauses (inclusion)
        instead of 'must_not' (exclusion) for suppression and policy filtering.

        :return: A 2-tuple (filter, nested_filters) as described in parent class.
        """
        chain = _chain_filters or self._chain_filters
        nested_filters: defaultdict[str, list] = defaultdict(list)

        if self.match_nothing:
            return MatchNone(), nested_filters

        f = None

        # Collection filtering - scope to library's collections (standard behavior)
        collection_ids = self._filter_ids(self.collection_ids)
        if collection_ids:
            collection_match = Terms(**{"licensepools.collection_id": collection_ids})
            nested_filters["licensepools"].append(collection_match)

        # Build 'should' clauses to INCLUDE suppressed/filtered works
        # (opposite of normal Filter behavior which excludes them)
        should_clauses: list[BaseQuery] = []

        # Match manually suppressed works for this library
        if self.library_id:
            should_clauses.append(Terms(**{"suppressed_for": [self.library_id]}))

        # Match audience-filtered works
        if self.filtered_audiences:
            excluded_audiences = self._scrub_list(self.filtered_audiences)
            should_clauses.append(Terms(audience=excluded_audiences))

        # Match genre-filtered works (genres are nested documents)
        if self.filtered_genres:
            genre_match = Nested(
                path="genres",
                query=Terms(**{"genres.name": self.filtered_genres}),
            )
            should_clauses.append(genre_match)

        # At least one condition must match for work to be included
        if should_clauses:
            f = chain(f, Bool(should=should_clauses, minimum_should_match=1))
        else:
            # No suppression/filtering conditions - match nothing
            return MatchNone(), nested_filters

        return f, nested_filters
