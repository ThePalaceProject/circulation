from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import TYPE_CHECKING
from urllib.parse import quote_plus

from dependency_injector.wiring import Provide, inject
from flask_babel import lazy_gettext as _
from opensearchpy import OpenSearchException
from sqlalchemy.orm import Session

from palace.manager.core.classifier import Classifier
from palace.manager.search.pagination import Pagination
from palace.manager.util.opds_writer import OPDSFeed

if TYPE_CHECKING:
    from palace.manager.search.external_search import ExternalSearchIndex
    from palace.manager.search.result import WorkSearchResult


class WorkList:
    """An object that can obtain a list of Work objects for use
    in generating an OPDS feed.

    By default, these Work objects come from a search index.
    """

    # The default maximum cache time of a feed derived from a WorkList
    # is the default cache time for any OPDS feed.
    MAX_CACHE_AGE = OPDSFeed.DEFAULT_MAX_AGE

    # If a certain type of Worklist should always have its OPDS feeds
    # cached under a specific type, define that type as
    # CACHED_FEED_TYPE.
    CACHED_FEED_TYPE: str | None = None

    # By default, a WorkList is always visible.
    @property
    def visible(self) -> bool:
        return True

    def max_cache_age(self):
        """Determine how long a feed for this WorkList should be cached."""
        return self.MAX_CACHE_AGE

    @classmethod
    def top_level_for_library(self, _db, library, collection_ids=None):
        """Create a WorkList representing this library's collection
        as a whole.

        If no top-level visible lanes are configured, the WorkList
        will be configured to show every book in the collection.

        If a single top-level Lane is configured, it will returned as
        the WorkList.

        Otherwise, a WorkList containing the visible top-level lanes
        is returned.
        """
        # Local imports to avoid circular dependency
        from palace.manager.feed.worklist.top_level import TopLevelWorkList
        from palace.manager.sqlalchemy.model.edition import Edition
        from palace.manager.sqlalchemy.model.lane import Lane

        # Load all of this Library's visible top-level Lane objects
        # from the database.
        top_level_lanes = (
            _db.query(Lane)
            .filter(Lane.library == library)
            .filter(Lane.parent == None)
            .filter(Lane._visible == True)
            .order_by(Lane.priority)
            .all()
        )

        if len(top_level_lanes) == 1:
            # The site configuration includes a single top-level lane;
            # this can stand in for the library on its own.
            return top_level_lanes[0]

        # This WorkList contains every title available to this library
        # in one of the media supported by the default client.
        wl = TopLevelWorkList()

        wl.initialize(
            library,
            display_name=library.name,
            children=top_level_lanes,
            media=Edition.FULFILLABLE_MEDIA,
            entrypoints=library.entrypoints,
            collection_ids=collection_ids,
        )
        return wl

    def initialize(
        self,
        library,
        display_name=None,
        genres=None,
        audiences=None,
        languages=None,
        media=None,
        customlists=None,
        list_datasource=None,
        list_seen_in_previous_days=None,
        children=None,
        priority=None,
        entrypoints=None,
        fiction=None,
        license_datasource=None,
        target_age=None,
        collection_ids=None,
    ):
        """Initialize with basic data.

        This is not a constructor, to avoid conflicts with `Lane`, an
        ORM object that subclasses this object but does not use this
        initialization code.

        :param library: Only Works available in this Library will be
            included in lists.

        :param display_name: Name to display for this WorkList in the
            user interface.

        :param genres: Only Works classified under one of these Genres
            will be included in lists.

        :param audiences: Only Works classified under one of these audiences
            will be included in lists.

        :param languages: Only Works in one of these languages will be
            included in lists.

        :param media: Only Works in one of these media will be included
            in lists.

        :param fiction: Only Works with this fiction status will be included
            in lists.

        :param target_age: Only Works targeted at readers in this age range
            will be included in lists.

        :param license_datasource: Only Works with a LicensePool from this
            DataSource will be included in lists.

        :param customlists: Only Works included on one of these CustomLists
            will be included in lists.

        :param list_datasource: Only Works included on a CustomList
            associated with this DataSource will be included in
            lists. This overrides any specific CustomLists provided in
            `customlists`.

        :param list_seen_in_previous_days: Only Works that were added
            to a matching CustomList within this number of days will be
            included in lists.

        :param children: This WorkList has children, which are also
            WorkLists.

        :param priority: A number indicating where this WorkList should
            show up in relation to its siblings when it is the child of
            some other WorkList.

        :param entrypoints: A list of EntryPoint classes representing
            different ways of slicing up this WorkList.

        """
        self.library_id = None
        self.collection_ids = collection_ids
        if library:
            self.library_id = library.id
            if self.collection_ids is None:
                self.collection_ids = [
                    collection.id for collection in library.active_collections
                ]
        self.display_name = display_name
        if genres:
            self.genre_ids = [x.id for x in genres]
        else:
            self.genre_ids = None
        self.audiences = audiences
        self.languages = languages
        self.media = media
        self.fiction = fiction

        if license_datasource:
            self.license_datasource_id = license_datasource.id
        else:
            self.license_datasource_id = None

        # If a specific set of CustomLists was passed in, store their IDs.
        #
        # If a custom list DataSource was passed in, gather the IDs for
        # every CustomList associated with that DataSource, and store
        # those IDs.
        #
        # Either way, WorkList starts out with a specific list of IDs,
        # which simplifies the WorkList code in a way that isn't
        # available to Lane.
        self._customlist_ids = None
        self.list_datasource_id = None
        if list_datasource:
            customlists = list_datasource.custom_lists

            # We do also store the CustomList ID, which is used as an
            # optimization in customlist_filter_clauses().
            self.list_datasource_id = list_datasource.id

        # The custom list IDs are stored in _customlist_ids, for
        # compatibility with Lane.
        if customlists:
            self._customlist_ids = [x.id for x in customlists]
        self.list_seen_in_previous_days = list_seen_in_previous_days

        self.fiction = fiction
        self.target_age = target_age

        self.children = []
        if children:
            for child in children:
                self.append_child(child)
        self.priority = priority or 0

        if entrypoints:
            self.entrypoints = list(entrypoints)
        else:
            self.entrypoints = []

    def append_child(self, child):
        """Add one child to the list of children in this WorkList.

        This hook method can be overridden to modify the child's
        configuration so as to make it fit with what the parent is
        offering.
        """
        self.children.append(child)

    @property
    def customlist_ids(self):
        """Return the custom list IDs."""
        return self._customlist_ids

    @property
    def uses_customlists(self):
        """Does the works() implementation for this WorkList look for works on
        CustomLists?
        """
        if self._customlist_ids or self.list_datasource_id:
            return True
        return False

    def get_library(self, _db):
        """Find the Library object associated with this WorkList."""
        # Local import to avoid circular dependency
        from palace.manager.sqlalchemy.model.library import Library

        return Library.by_id(_db, self.library_id)

    def get_customlists(self, _db):
        """Get customlists associated with the Worklist."""
        # Local import to avoid circular dependency
        from palace.manager.sqlalchemy.model.customlist import CustomList

        if hasattr(self, "_customlist_ids") and self._customlist_ids is not None:
            return (
                _db.query(CustomList)
                .filter(CustomList.id.in_(self._customlist_ids))
                .all()
            )
        return []

    @property
    def display_name_for_all(self):
        """The display name to use when referring to the set of all books in
        this WorkList, as opposed to the WorkList itself.
        """
        return _("All %(worklist)s", worklist=self.display_name)

    @property
    def visible_children(self):
        """A WorkList's children can be used to create a grouped acquisition
        feed for that WorkList.
        """
        return sorted(
            (x for x in self.children if x.visible),
            key=lambda x: (x.priority, x.display_name or ""),
        )

    @property
    def has_visible_children(self):
        for lane in self.visible_children:
            if lane:
                return True
        return False

    @property
    def parent(self):
        """A WorkList has no parent. This method is defined for compatibility
        with Lane.
        """
        return None

    @property
    def parentage(self):
        """WorkLists have no parentage. This method is defined for compatibility
        with Lane.
        """
        return []

    def is_self_or_descendant(self, ancestor):
        """Is this WorkList the given WorkList or one of its descendants?

        :param ancestor: A WorkList.
        :return: A boolean.
        """
        for candidate in [self] + list(self.parentage):
            if candidate == ancestor:
                return True
        return False

    @property
    def inherit_parent_restrictions(self):
        """Since a WorkList has no parent, it cannot inherit any restrictions
        from its parent. This method is defined for compatibility
        with Lane.
        """
        return False

    @property
    def hierarchy(self):
        """The portion of the WorkList hierarchy that culminates in this
        WorkList.
        """
        return list(reversed(list(self.parentage))) + [self]

    def inherited_value(self, k):
        """Try to find this WorkList's value for the given key (e.g. 'fiction'
        or 'audiences').

        If it's not set, try to inherit a value from the WorkList's
        parent. This only works if this WorkList has a parent and is
        configured to inherit values from its parent.

        Note that inheritance works differently for genre_ids and
        customlist_ids -- use inherited_values() for that.
        """
        value = getattr(self, k)
        if value not in (None, []):
            return value
        else:
            if not self.parent or not self.inherit_parent_restrictions:
                return None
            parent = self.parent
            return parent.inherited_value(k)

    def inherited_values(self, k):
        """Find the values for the given key (e.g. 'genre_ids' or
        'customlist_ids') imposed by this WorkList and its parentage.

        This is for values like .genre_ids and .customlist_ids, where
        each member of the WorkList hierarchy can impose a restriction
        on query results, and the effects of the restrictions are
        additive.
        """
        values = []
        if not self.inherit_parent_restrictions:
            hierarchy = [self]
        else:
            hierarchy = self.hierarchy
        for wl in hierarchy:
            value = getattr(wl, k)
            if value not in (None, []):
                values.append(value)
        return values

    @property
    def full_identifier(self):
        """A human-readable identifier for this WorkList that
        captures its position within the heirarchy.
        """
        full_parentage = [str(x.display_name) for x in self.hierarchy]
        if getattr(self, "library", None):
            # This WorkList is associated with a specific library.
            # incorporate the library's name to distinguish between it
            # and other lanes in the same position in another library.
            full_parentage.insert(0, self.library.short_name)
        return " / ".join(full_parentage)

    @property
    def language_key(self):
        """Return a string identifying the languages used in this WorkList.
        This will usually be in the form of 'eng,spa' (English and Spanish).
        """
        key = ""
        if self.languages:
            key += ",".join(sorted(self.languages))
        return key

    @property
    def audience_key(self):
        """Translates audiences list into url-safe string"""
        key = ""
        if self.audiences and Classifier.AUDIENCES.difference(self.audiences):
            # There are audiences and they're not the default
            # "any audience", so add them to the URL.
            audiences = [quote_plus(a) for a in sorted(self.audiences)]
            key += ",".join(audiences)
        return key

    @property
    def unique_key(self):
        """A string key that uniquely describes this WorkList within
        its Library.

        This is used when caching feeds for this WorkList. For Lanes,
        the lane_id is used instead.
        """
        return "{}-{}-{}".format(
            self.display_name, self.language_key, self.audience_key
        )

    def accessible_to(self, patron):
        """As a matter of library policy, is the given `Patron` allowed
        to access this `WorkList`?
        """
        if not patron:
            # We have no lanes that are private, per se, so if there
            # is no active patron, every lane is accessible.
            return True

        _db = Session.object_session(patron)
        if patron.library != self.get_library(_db):
            # You can't access a WorkList from another library.
            return False

        if not patron.library.has_root_lanes:
            # The patron's library has no root lanes, so it's not necessary
            # to run the somewhat expensive check for a patron's root lane.
            # All lanes are accessible to all patrons.
            return True

        # Get the patron's root lane, if any.
        root = patron.root_lane
        if not root:
            # A patron with no root lane can access every one of the
            # library's WorkLists.
            return True

        # A WorkList is only accessible if the audiences and target age
        # of the WorkList are fully compatible with that of the
        # patron's root lane.
        if self.audiences:
            for work_audience in self.audiences:
                # work_audience represents a type of book that _might_
                # show up in this WorkList.
                if not patron.work_is_age_appropriate(work_audience, self.target_age):
                    # Books of this type would not be appropriate to show to
                    # this patron, so the lane itself is not accessible.
                    return False

        return True

    def overview_facets(self, _db, facets):
        """Convert a generic FeaturedFacets to some other faceting object,
        suitable for showing an overview of this WorkList in a grouped
        feed.
        """
        return facets

    @inject
    def groups(
        self,
        _db,
        include_sublanes=True,
        pagination=None,
        facets=None,
        *,
        search_engine: ExternalSearchIndex = Provide["search.index"],
        debug=False,
    ):
        """Extract a list of samples from each child of this WorkList.  This
        can be used to create a grouped acquisition feed for the WorkList.

        :param pagination: A Pagination object which may affect how many
            works each child of this WorkList may contribute.
        :param facets: A FeaturedFacets object that may restrict the works on view.
        :param search_engine: An ExternalSearchIndex to use when
            asking for the featured works in a given WorkList.
        :param debug: A debug argument passed into `search_engine` when
            running the search.
        :yield: A sequence of (Work, WorkList) 2-tuples, with each
            WorkList representing the child WorkList in which the Work is
            found.
        """
        # Local import to avoid circular dependency
        from palace.manager.sqlalchemy.model.lane import Lane

        if not include_sublanes:
            # We only need to find featured works for this lane,
            # not this lane plus its sublanes.
            adapted = self.overview_facets(_db, facets)
            for work in self.works(_db, pagination=pagination, facets=adapted):
                yield work, self
            return

        # This is a list rather than a dict because we want to
        # preserve the ordering of the children.
        relevant_lanes = []
        relevant_children = []

        # We use an explicit check for Lane.visible here, instead of
        # iterating over self.visible_children, because Lane.visible only
        # works when the Lane is merged into a database session.
        for child in self.children:
            if isinstance(child, Lane):
                child = _db.merge(child)

            if not child.visible:
                continue

            if isinstance(child, Lane):
                # Children that turn out to be Lanes go into
                # relevant_lanes. Their Works will be obtained from
                # the search index.
                relevant_lanes.append(child)
            # Both Lanes and WorkLists go into relevant_children.
            # This controls the yield order for Works.
            relevant_children.append(child)

        # _groups_for_lanes will run a query to pull featured works
        # for any children that are Lanes, and call groups()
        # recursively for any children that are not.
        for work, worklist in self._groups_for_lanes(
            _db,
            relevant_children,
            relevant_lanes,
            pagination=pagination,
            facets=facets,
            search_engine=search_engine,
            debug=debug,
        ):
            yield work, worklist

    @inject
    def works(
        self,
        _db,
        facets=None,
        pagination=None,
        *,
        search_engine: ExternalSearchIndex = Provide["search.index"],
        debug=False,
        **kwargs,
    ):
        """Use a search engine to obtain Work or Work-like objects that belong
        in this WorkList.

        Compare DatabaseBackedWorkList.works_from_database, which uses
        a database query to obtain the same Work objects.

        :param _db: A database connection.
        :param facets: A Facets object which may put additional
           constraints on WorkList membership.
        :param pagination: A Pagination object indicating which part of
           the WorkList the caller is looking at, and/or a limit on the
           number of works to fetch.
        :param kwargs: Different implementations may fetch the
           list of works from different sources and may need different
           keyword arguments.
        :return: A list of Work or Work-like objects, or a database query
            that generates such a list when executed.

        """
        filter = self.filter(_db, facets)
        hits = search_engine.query_works(
            query_string=None, filter=filter, pagination=pagination, debug=debug
        )
        return self.works_for_hits(_db, hits, facets=facets)

    def filter(self, _db, facets):
        """Helper method to instantiate a Filter object for this WorkList.

        Using this ensures that modify_search_filter_hook() is always
        called.
        """
        from palace.manager.search.filter import Filter

        filter = Filter.from_worklist(_db, self, facets)
        modified = self.modify_search_filter_hook(filter)
        if modified is None:
            # The Filter was modified in place, rather than a new
            # Filter being returned.
            modified = filter
        return modified

    def modify_search_filter_hook(self, filter):
        """A hook method allowing subclasses to modify a Filter
        object that's about to find all the works in this WorkList.

        This can avoid the need for complex subclasses of Facets.
        """
        return filter

    def works_for_hits(self, _db, hits, facets=None):
        """Convert a list of search results into Work objects.

        This works by calling works_for_resultsets() on a list
        containing a single list of search results.

        :param _db: A database connection
        :param hits: A list of Hit objects from Opensearch.
        :return: A list of Work or (if the search results include
            script fields), WorkSearchResult objects.
        """

        [results] = self.works_for_resultsets(_db, [hits], facets=facets)
        return results

    def works_for_resultsets(self, _db, resultsets, facets=None):
        """Convert a list of lists of Hit objects into a list
        of lists of Work objects.
        """
        from palace.manager.feed.worklist.specific import SpecificWorkList

        work_ids = set()
        for resultset in resultsets:
            for result in resultset:
                work_ids.add(result.work_id)

        # The simplest way to turn Hits into Works is to create a
        # DatabaseBackedWorkList that fetches those specific Works
        # while applying the general availability filters.
        #
        # If facets were passed in, then they are used to further
        # filter the list.
        #
        # TODO: There's a lot of room for improvement here, but
        # performance isn't a big concern -- it's just ugly.
        wl = SpecificWorkList(work_ids)
        wl.initialize(self.get_library(_db))
        # If we are specifically targeting a collection and not a library
        # ensure the worklist is aware of this
        if not self.library_id and self.collection_ids:
            wl.collection_ids = self.collection_ids
        qu = wl.works_from_database(_db, facets=facets)
        a = time.time()
        all_works = qu.all()

        # Create a list of lists with the same membership as the original
        # `resultsets`, but with Hit objects replaced with Work objects.
        work_by_id = dict()
        for w in all_works:
            work_by_id[w.id] = w

        work_lists = []
        for resultset in resultsets:
            works = []
            work_lists.append(works)
            for hit in resultset:
                if hit.work_id in work_by_id:
                    work = work_by_id[hit.work_id]
                    works.append(work)

        b = time.time()
        logging.info("Obtained %sxWork in %.2fsec", len(all_works), b - a)
        return work_lists

    @property
    def search_target(self):
        """By default, a WorkList is searchable."""
        return self

    def search(
        self, _db, query, search_client, pagination=None, facets=None, debug=False
    ):
        """Find works in this WorkList that match a search query.

        :param _db: A database connection.
        :param query: Search for this string.
        :param search_client: An ExternalSearchIndex object.
        :param pagination: A Pagination object.
        :param facets: A faceting object, probably a SearchFacets.
        :param debug: Pass in True to see a summary of results returned
            from the search index.
        """
        results = []
        hits = None
        if not search_client:
            # We have no way of actually doing a search. Return nothing.
            return results

        if not pagination:
            pagination = Pagination(offset=0, size=Pagination.DEFAULT_SEARCH_SIZE)

        filter = self.filter(_db, facets)
        try:
            hits = search_client.query_works(query, filter, pagination, debug)
        except OpenSearchException as e:
            logging.error(
                "Problem communicating with OpenSearch. Returning empty list of search results.",
                exc_info=e,
            )
        if hits:
            results = self.works_for_hits(_db, hits)

        return results

    @inject
    def _groups_for_lanes(
        self,
        _db,
        relevant_lanes,
        queryable_lanes,
        pagination,
        facets,
        *,
        search_engine: ExternalSearchIndex = Provide["search.index"],
        debug=False,
    ):
        """Ask the search engine for groups of featurable works in the
        given lanes. Fill in gaps as necessary.

        :param pagination: An optional Pagination object which will be
           used to paginate each group individually. Note that this
           means Pagination.page_loaded() method will be called once
           for each group.
        :param facets: A FeaturedFacets object.

        :param search_engine: An ExternalSearchIndex to use when
           asking for the featured works in a given WorkList.
        :param debug: A debug argument passed into `search_engine` when
           running the search.
        :yield: A sequence of (Work, WorkList) 2-tuples, with each
            WorkList representing the child WorkList in which the Work is
            found.

        """
        # Local import to avoid circular dependency
        from palace.manager.sqlalchemy.model.lane import Lane

        library = self.get_library(_db)
        if pagination is None:
            # No pagination object was provided. Our target size is
            # the featured lane size, but we'll ask for a few extra
            # works for each lane, to reduce the risk that we end up
            # reusing a book in two different lanes.
            target_size = library.settings.featured_lane_size

            # We ask for a few extra works for each lane, to reduce the
            # risk that we'll end up reusing a book in two different
            # lanes.
            ask_for_size = max(target_size + 1, int(target_size * 1.10))
            pagination = Pagination(size=ask_for_size)
        else:
            target_size = pagination.size

        if isinstance(self, Lane):
            parent_lane = self
        else:
            parent_lane = None

        queryable_lane_set = set(queryable_lanes)
        works_and_lanes = list(
            self._featured_works_with_lanes(
                _db,
                queryable_lanes,
                pagination=pagination,
                facets=facets,
                search_engine=search_engine,
                debug=debug,
            )
        )

        def _done_with_lane(lane):
            """Called when we're done with a Lane, either because
            the lane changes or we've reached the end of the list.
            """
            # Did we get enough items?
            num_missing = target_size - len(by_lane[lane])
            if num_missing > 0 and might_need_to_reuse:
                # No, we need to use some works we used in a
                # previous lane to fill out this lane. Stick
                # them at the end.
                by_lane[lane].extend(list(might_need_to_reuse.values())[:num_missing])

        used_works = set()
        by_lane: dict[Lane, list[WorkSearchResult]] = defaultdict(list)
        working_lane = None
        might_need_to_reuse: dict[int, WorkSearchResult] = dict()
        for work, lane in works_and_lanes:
            if lane != working_lane:
                # Either we're done with the old lane, or we're just
                # starting and there was no old lane.
                if working_lane:
                    _done_with_lane(working_lane)
                working_lane = lane
                used_works_this_lane = set()
                might_need_to_reuse = dict()
            if len(by_lane[lane]) >= target_size:
                # We've already filled this lane.
                continue

            if work.id in used_works:
                if work.id not in used_works_this_lane:
                    # We already used this work in another lane, but we
                    # might need to use it again to fill out this lane.
                    might_need_to_reuse[work.id] = work
            else:
                by_lane[lane].append(work)
                used_works.add(work.id)
                used_works_this_lane.add(work.id)

        # Close out the last lane encountered.
        _done_with_lane(working_lane)
        for lane in relevant_lanes:
            if lane in queryable_lane_set:
                # We found results for this lane through the main query.
                # Yield those results.
                for work in by_lane.get(lane, []):
                    yield (work, lane)
            else:
                # We didn't try to use the main query to find results
                # for this lane because we knew the results, if there
                # were any, wouldn't be representative. This is most
                # likely because this 'lane' is a WorkList and not a
                # Lane at all. Do a whole separate query and plug it
                # in at this point.
                yield from lane.groups(
                    _db,
                    include_sublanes=False,
                    pagination=pagination,
                    facets=facets,
                )

    def _featured_works_with_lanes(
        self, _db, lanes, pagination, facets, search_engine, debug=False
    ):
        """Find a sequence of works that can be used to
        populate this lane's grouped acquisition feed.

        :param lanes: Classify Work objects
            as belonging to one of these WorkLists (presumably sublanes
            of `self`).
        :param facets: A faceting object, presumably a FeaturedFacets
        :param pagination: A Pagination object explaining how many
            items to ask for. In most cases this should be slightly more than
            the number of items you actually want, so that you have some
            slack to remove duplicates across multiple lanes.
        :param search_engine: An ExternalSearchIndex to use when
           asking for the featured works in a given WorkList.
        :param debug: A debug argument passed into `search_engine` when
           running the search.

        :yield: A sequence of (Work, Lane) 2-tuples.
        """
        if not lanes:
            # We can't run this query at all.
            return

        # Ask the search engine for works from every lane we're given.
        queries = []
        for lane in lanes:
            overview_facets = lane.overview_facets(_db, facets)
            from palace.manager.search.filter import Filter

            filter = Filter.from_worklist(_db, lane, overview_facets)
            queries.append((None, filter, pagination))
        resultsets = list(search_engine.query_works_multi(queries))
        works = self.works_for_resultsets(_db, resultsets, facets=facets)

        for i, lane in enumerate(lanes):
            results = works[i]
            for work in results:
                yield work, lane
