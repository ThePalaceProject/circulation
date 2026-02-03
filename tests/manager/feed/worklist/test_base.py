import random
from dataclasses import dataclass, fields
from typing import cast
from unittest.mock import MagicMock, call

import pytest
from opensearchpy import OpenSearchException

from palace.manager.core.classifier import Classifier
from palace.manager.core.config import ConfigurationAttributeValue
from palace.manager.core.entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EntryPoint,
    EverythingEntryPoint,
)
from palace.manager.feed.facets.database import DatabaseBackedFacets
from palace.manager.feed.facets.feed import Facets, FeaturedFacets
from palace.manager.feed.facets.search import SearchFacets
from palace.manager.feed.worklist.base import WorkList
from palace.manager.feed.worklist.top_level import TopLevelWorkList
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.search.filter import Filter
from palace.manager.search.pagination import Pagination
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work, WorkGenre
from palace.manager.sqlalchemy.util import get_one_or_create, tuple_to_numericrange
from palace.manager.util.opds_writer import OPDSFeed
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixtureFake


class MockWork:
    """Acts enough like a Work to trick code that doesn't need to make
    database requests.
    """

    def __init__(self, id):
        self.id = id


class MockWorks(WorkList):
    """A WorkList that mocks works_from_database()."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.works = []
        self.works_from_database_calls = []
        self.random_sample_calls = []

    def queue_works(self, works):
        """Set the next return value for works_from_database()."""
        self.works.append(works)

    def works_from_database(self, _db, facets=None, pagination=None, featured=False):
        self.works_from_database_calls.append((facets, pagination, featured))
        try:
            return self.works.pop(0)
        except IndexError:
            return []

    def random_sample(self, query, target_size):
        # The 'query' is actually a list, and we're in a test
        # environment where randomness is not welcome. Just take
        # a sample from the front of the list.
        self.random_sample_calls.append((query, target_size))
        return query[:target_size]


class TestWorkList:
    def test_initialize(self, db: DatabaseTransactionFixture):
        default_library = db.default_library()
        active_collection = db.default_collection()
        inactive_collection = db.default_inactive_collection()

        wl = WorkList()
        child = WorkList()
        child.initialize(db.default_library())
        sf, ignore = Genre.lookup(db.session, "Science Fiction")
        romance, ignore = Genre.lookup(db.session, "Romance")

        # Create a WorkList that's associated with a Library, two genres,
        # and a child WorkList.
        wl.initialize(
            default_library,
            children=[child],
            genres=[sf, romance],
            entrypoints=[1, 2, 3],
        )

        # Access the Library.
        assert default_library == wl.get_library(db.session)

        # Only the library's active collections are associated
        # with the WorkList.
        assert set(default_library.associated_collections) == {
            active_collection,
            inactive_collection,
        }
        assert default_library.active_collections == [active_collection]
        assert set(wl.collection_ids) == {
            x.id for x in default_library.active_collections
        }

        # The Genres associated with the WorkList are the ones passed
        # in on the constructor.
        assert set(wl.genre_ids) == {x.id for x in [sf, romance]}

        # The WorkList's child is the WorkList passed in to the constructor.
        assert [child] == wl.visible_children

        # The Worklist's .entrypoints is whatever was passed in
        # to the constructor.
        assert [1, 2, 3] == wl.entrypoints

    def test_initialize_worklist_without_library(self):
        # It's possible to initialize a WorkList with no Library.
        worklist = WorkList()
        worklist.initialize(None)

        # No restriction is placed on the collection IDs of the
        # Works in this list.
        assert None == worklist.collection_ids

    def test_initialize_with_customlists(self, db: DatabaseTransactionFixture):
        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)

        customlist1, ignore = db.customlist(
            data_source_name=gutenberg.name, num_entries=0
        )
        customlist2, ignore = db.customlist(
            data_source_name=gutenberg.name, num_entries=0
        )
        customlist3, ignore = db.customlist(
            data_source_name=DataSource.OVERDRIVE, num_entries=0
        )

        # Make a WorkList based on specific CustomLists.
        worklist = WorkList()
        worklist.initialize(
            db.default_library(), customlists=[customlist1, customlist3]
        )
        assert [customlist1.id, customlist3.id] == worklist.customlist_ids
        assert None == worklist.list_datasource_id

        # Make a WorkList based on a DataSource, as a shorthand for
        # 'all the CustomLists from that DataSource'.
        worklist = WorkList()
        worklist.initialize(db.default_library(), list_datasource=gutenberg)
        assert {customlist1.id, customlist2.id} == set(worklist.customlist_ids)
        assert gutenberg.id == worklist.list_datasource_id

    def test_initialize_without_library(self, db: DatabaseTransactionFixture):
        wl = WorkList()
        sf, ignore = Genre.lookup(db.session, "Science Fiction")
        romance, ignore = Genre.lookup(db.session, "Romance")

        # Create a WorkList that's associated with two genres.
        wl.initialize(None, genres=[sf, romance])
        wl.collection_ids = [db.default_collection().id]

        # There is no Library.
        assert None == wl.get_library(db.session)

        # The Genres associated with the WorkList are the ones passed
        # in on the constructor.
        assert set(wl.genre_ids) == {x.id for x in [sf, romance]}

    def test_initialize_uses_append_child_hook_method(
        self, db: DatabaseTransactionFixture
    ):
        # When a WorkList is initialized with children, the children
        # are passed individually through the append_child() hook
        # method, not simply set to WorkList.children.
        class Mock(WorkList):
            append_child_calls = []

            def append_child(self, child):
                self.append_child_calls.append(child)
                return super().append_child(child)

        child = WorkList()
        parent = Mock()
        parent.initialize(db.default_library(), children=[child])
        assert [child] == parent.append_child_calls

        # They do end up in WorkList.children, since that's what the
        # default append_child() implementation does.
        assert [child] == parent.children

    def test_top_level_for_library(self, db: DatabaseTransactionFixture):
        """Test the ability to generate a top-level WorkList."""
        # These two top-level lanes should be children of the WorkList.
        lane1 = db.lane(display_name="Top-level Lane 1")
        lane1.priority = 0
        lane2 = db.lane(display_name="Top-level Lane 2")
        lane2.priority = 1

        # This lane is invisible and will be filtered out.
        invisible_lane = db.lane(display_name="Invisible Lane")
        invisible_lane.visible = False

        # This lane has a parent and will be filtered out.
        sublane = db.lane(display_name="Sublane")
        lane1.sublanes.append(sublane)

        # This lane belongs to a different library.
        other_library = db.library(name="Other Library", short_name="Other")
        other_library_lane = db.lane(
            display_name="Other Library Lane", library=other_library
        )

        # The default library gets a TopLevelWorkList with the two top-level lanes as children.
        wl = WorkList.top_level_for_library(db.session, db.default_library())
        assert isinstance(wl, TopLevelWorkList)
        assert [lane1, lane2] == wl.children
        assert Edition.FULFILLABLE_MEDIA == wl.media

        # The other library only has one top-level lane, so we use that lane.
        l = WorkList.top_level_for_library(db.session, other_library)
        assert other_library_lane == l

        # This library has no lanes configured at all.
        no_config_library = db.library(
            name="No configuration Library", short_name="No config"
        )
        wl = WorkList.top_level_for_library(db.session, no_config_library)
        assert isinstance(wl, TopLevelWorkList)
        assert [] == wl.children
        assert Edition.FULFILLABLE_MEDIA == wl.media

    def test_audience_key(self, db: DatabaseTransactionFixture):
        wl = WorkList()
        wl.initialize(library=db.default_library())

        # No audience.
        assert "" == wl.audience_key

        # All audiences.
        wl.audiences = Classifier.AUDIENCES
        assert "" == wl.audience_key

        # Specific audiences.
        wl.audiences = [Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT]
        assert "Children,Young+Adult" == wl.audience_key

    def test_parent(self):
        # A WorkList has no parent.
        assert None == WorkList().parent

    def test_parentage(self):
        # A WorkList has no parentage, since it has no parent.
        assert [] == WorkList().parentage

    def test_inherit_parent_restrictions(self):
        # A WorkList never inherits parent restrictions, because it
        # can't have a parent.
        assert False == WorkList().inherit_parent_restrictions

    def test_hierarchy(self):
        # A WorkList's hierarchy includes only itself, because it
        # can't have a parent.
        wl = WorkList()
        assert [wl] == wl.hierarchy

    def test_visible_children(self, db: DatabaseTransactionFixture):
        """Invisible children don't show up in WorkList.visible_children."""
        wl = WorkList()
        visible = db.lane()
        invisible = db.lane()
        invisible.visible = False
        child_wl = WorkList()
        child_wl.initialize(db.default_library())
        wl.initialize(db.default_library(), children=[visible, invisible, child_wl])
        assert {child_wl, visible} == set(wl.visible_children)

    def test_visible_children_sorted(self, db: DatabaseTransactionFixture):
        """Visible children are sorted by priority and then by display name."""
        wl = WorkList()

        lane_child = db.lane()
        lane_child.display_name = "ZZ"
        lane_child.priority = 0

        wl_child = WorkList()
        wl_child.priority = 1
        wl_child.display_name = "AA"

        wl.initialize(db.default_library(), children=[lane_child, wl_child])

        # lane_child has a higher priority so it shows up first even
        # though its display name starts with a Z.
        assert [lane_child, wl_child] == wl.visible_children

        # If the priorities are the same, wl_child shows up first,
        # because its display name starts with an A.
        wl_child.priority = 0
        assert [wl_child, lane_child] == wl.visible_children

    def test_is_self_or_descendant(self, db: DatabaseTransactionFixture):
        # Test the code that checks whether one WorkList is 'beneath'
        # another.

        class WorkListWithParent(WorkList):
            # A normal WorkList never has a parent; this subclass
            # makes it possible to explicitly set a WorkList's parent
            # and get its parentage.
            #
            # This way we can test WorkList code without bringing in Lane.
            def __init__(self):
                self._parent = None

            @property
            def parent(self):
                return self._parent

            @property
            def parentage(self):
                if not self._parent:
                    return []
                return [self._parent] + list(self._parent.parentage)

        # A WorkList matches itself.
        child = WorkListWithParent()
        child.initialize(db.default_library())
        assert True == child.is_self_or_descendant(child)

        # But not any other WorkList.
        parent = WorkListWithParent()
        parent.initialize(db.default_library())
        assert False == child.is_self_or_descendant(parent)

        grandparent = WorkList()
        grandparent.initialize(db.default_library())
        assert False == child.is_self_or_descendant(grandparent)

        # Unless it's a descendant of that WorkList.
        child._parent = parent
        parent._parent = grandparent
        assert True == child.is_self_or_descendant(parent)
        assert True == child.is_self_or_descendant(grandparent)
        assert True == parent.is_self_or_descendant(grandparent)

        assert False == parent.is_self_or_descendant(child)
        assert False == grandparent.is_self_or_descendant(parent)

    def test_accessible_to(self, db: DatabaseTransactionFixture):
        # Test the circumstances under which a Patron may or may not access a
        # WorkList.

        wl = WorkList()
        wl.initialize(db.default_library())

        # A WorkList is always accessible to unauthenticated users.
        m = wl.accessible_to
        assert True == m(None)

        # A WorkList is never accessible to patrons of a different library.
        other_library = db.library()
        other_library_patron = db.patron(library=other_library)
        assert False == m(other_library_patron)

        # A WorkList is always accessible to patrons with no root lane
        # set.
        patron = db.patron()
        assert True == m(patron)

        # Give the patron a root lane.
        lane = db.lane()
        lane.root_for_patron_type = ["1"]
        patron.external_type = "1"

        # Now that the patron has a root lane, WorkLists will become
        # inaccessible if they might contain content not
        # age-appropriate for that patron (as gauged by their root
        # lane).

        # As initialized, our worklist has no audience restrictions.
        assert True == m(patron)

        # Give it some audience restrictions.
        wl.audiences = [Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_CHILDREN]
        wl.target_age = tuple_to_numericrange((4, 5))

        # Now it depends on the return value of Patron.work_is_age_appropriate.
        # Mock that method.
        patron.work_is_age_appropriate = MagicMock(return_value=False)

        # Since our mock returns false, so does accessible_to
        assert False == m(patron)

        # work_is_age_appropriate was called once, with the
        # WorkList's target age and its first audience restriction.
        # When work_is_age_appropriate returned False, it short-circuited
        # the process and no second call was made.
        patron.work_is_age_appropriate.assert_called_once_with(
            wl.audiences[0], wl.target_age
        )

        # If we tell work_is_age_appropriate to always return true...
        patron.work_is_age_appropriate = MagicMock(return_value=True)

        # ...accessible_to starts returning True.
        assert True == m(patron)

        # The mock method was called once for each audience
        # restriction in our WorkList. Only if _every_ call returns
        # True is the WorkList considered age-appropriate for the
        # patron.
        patron.work_is_age_appropriate.assert_has_calls(
            [
                call(wl.audiences[0], wl.target_age),
                call(wl.audiences[1], wl.target_age),
            ]
        )

    def test_uses_customlists(self, db: DatabaseTransactionFixture):
        """A WorkList is said to use CustomLists if either ._customlist_ids
        or .list_datasource_id is set.
        """
        wl = WorkList()
        wl.initialize(db.default_library())
        assert False == wl.uses_customlists

        wl._customlist_ids = object()
        assert True == wl.uses_customlists

        wl._customlist_ids = None
        wl.list_datasource_id = object()
        assert True == wl.uses_customlists

    def test_max_cache_age(self):
        # By default, the maximum cache age of an OPDS feed based on a
        # WorkList is the default cache age for any type of OPDS feed,
        # no matter what type of feed is being generated.
        wl = WorkList()
        assert OPDSFeed.DEFAULT_MAX_AGE == wl.max_cache_age()

    def test_filter(self, db: DatabaseTransactionFixture):
        # Verify that filter() calls modify_search_filter_hook()
        # and can handle either a new Filter being returned or a Filter
        # modified in place.

        class ModifyInPlace(WorkList):
            # A WorkList that modifies its search filter in place.
            def modify_search_filter_hook(self, filter):
                filter.hook_called = True

        wl = ModifyInPlace()
        wl.initialize(db.default_library())
        facets = SearchFacets()
        filter = wl.filter(db.session, facets)
        assert isinstance(filter, Filter)
        assert True == filter.hook_called  # type: ignore[attr-defined]

        class NewFilter(WorkList):
            # A WorkList that returns a brand new Filter
            def modify_search_filter_hook(self, filter):
                return "A brand new Filter"

        new_filter = NewFilter()
        new_filter.initialize(db.default_library())
        facets = SearchFacets()
        filter = new_filter.filter(db.session, facets)
        assert "A brand new Filter" == filter

    def test_groups(
        self,
        db: DatabaseTransactionFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        w1 = MockWork(1)
        w2 = MockWork(2)
        w3 = MockWork(3)

        class MockWorkList:
            def __init__(self, works):
                self._works = works
                self.visible = True

            def groups(self, *args, **kwargs):
                for i in self._works:
                    yield i, self

        # This WorkList has one featured work.
        child1 = MockWorkList([w1])

        # This WorkList has two featured works.
        child2 = MockWorkList([w2, w1])

        # This WorkList has two children -- the two WorkLists created
        # above.
        wl = WorkList()
        wl.initialize(db.default_library(), children=[child1, child2])

        # Calling groups() on the parent WorkList returns three
        # 2-tuples; one for each work featured by one of its children
        # WorkLists. Note that the same work appears twice, through two
        # different children.
        [wwl1, wwl2, wwl3] = wl.groups(
            db.session, search_engine=external_search_fake_fixture.external_search
        )
        assert (w1, child1) == wwl1
        assert (w2, child2) == wwl2
        assert (w1, child2) == wwl3

    def test_groups_propagates_facets(self, db: DatabaseTransactionFixture):
        # Verify that the Facets object passed into groups() is
        # propagated to the methods called by groups().
        class MockWorkList(WorkList):
            overview_facets_called_with = None

            def works(self, _db, pagination, facets):
                self.works_called_with = (pagination, facets)
                return []

            def overview_facets(self, _db, facets):
                self.overview_facets_called_with = facets
                return "A new faceting object"

            def _groups_for_lanes(
                self,
                _db,
                relevant_children,
                relevant_lanes,
                pagination,
                facets,
                **kwargs,
            ):
                self._groups_for_lanes_called_with = (pagination, facets)
                return []

        mock = MockWorkList()
        mock.initialize(library=db.default_library())
        facets = object()

        # First, try the situation where we're trying to make a grouped feed
        # out of the (imaginary) sublanes of this lane.
        [x for x in mock.groups(db.session, facets=facets)]

        # overview_facets() was not called.
        assert None == mock.overview_facets_called_with

        # The _groups_for_lanes() method was called with the
        # (imaginary) list of sublanes and the original faceting
        # object. No pagination was provided. The _groups_for_lanes()
        # implementation is responsible for giving each sublane a
        # chance to adapt that faceting object to its own needs.
        assert (None, facets) == mock._groups_for_lanes_called_with
        mock._groups_for_lanes_called_with = None

        # Now try the case where we want to use a pagination object to
        # restrict the number of results per lane.
        pagination = object()
        [x for x in mock.groups(db.session, pagination=pagination, facets=facets)]
        # The pagination object is propagated to _groups_for_lanes.
        assert (pagination, facets) == mock._groups_for_lanes_called_with
        mock._groups_for_lanes_called_with = None

        # Now try the situation where we're just trying to get _part_ of
        # a grouped feed -- the part for which this lane is responsible.
        [x for x in mock.groups(db.session, facets=facets, include_sublanes=False)]
        # Now, the original faceting object was passed into
        # overview_facets().
        assert facets == mock.overview_facets_called_with

        # And the return value of overview_facets() was passed into
        # works()
        assert (None, "A new faceting object") == mock.works_called_with

        # _groups_for_lanes was not called.
        assert None == mock._groups_for_lanes_called_with

    def test_works(self, db: DatabaseTransactionFixture):
        # Test the method that uses the search index to fetch a list of
        # results appropriate for a given WorkList.

        class MockSearchClient:
            """Respond to search requests with some fake work IDs."""

            fake_work_ids = [1, 10, 100, 1000]

            def query_works(self, **kwargs):
                self.called_with = kwargs
                return self.fake_work_ids

        class MockWorkList(WorkList):
            """Mock the process of turning work IDs into Work
            objects."""

            fake_work_list = "a list of works"

            def works_for_hits(self, _db, work_ids, facets=None):
                self.called_with = (_db, work_ids)
                return self.fake_work_list

        # Here's a WorkList.
        wl = MockWorkList()
        wl.initialize(db.default_library(), languages=["eng"])
        facets = Facets(
            db.default_library(),
            None,
            order=Facets.ORDER_TITLE,
            distributor=None,
            collection_name=None,
        )
        mock_pagination = object()
        mock_debug = object()
        search_client = MockSearchClient()

        # Ask the WorkList for a page of works, using the search index
        # to drive the query instead of the database.
        result = wl.works(
            db.session,
            facets,
            mock_pagination,
            search_engine=cast(ExternalSearchIndex, search_client),
            debug=mock_debug,
        )

        # MockSearchClient.query_works was used to grab a list of work
        # IDs.
        query_works_kwargs = search_client.called_with

        # Our facets and the requirements of the WorkList were used to
        # make a Filter object, which was passed as the 'filter'
        # keyword argument.
        filter = query_works_kwargs.pop("filter")
        assert Filter.from_worklist(db.session, wl, facets).build() == filter.build()

        # The other arguments to query_works are either constants or
        # our mock objects.
        assert (
            dict(query_string=None, pagination=mock_pagination, debug=mock_debug)
            == query_works_kwargs
        )

        # The fake work IDs returned from query_works() were passed into
        # works_for_hits().
        assert (db.session, search_client.fake_work_ids) == wl.called_with

        # And the fake return value of works_for_hits() was used as
        # the return value of works(), the method we're testing.
        assert wl.fake_work_list == result

    def test_works_for_hits(self, db: DatabaseTransactionFixture):
        # Verify that WorkList.works_for_hits() just calls
        # works_for_resultsets().
        class Mock(WorkList):
            def works_for_resultsets(self, _db, resultsets, facets=None):
                self.called_with = (_db, resultsets)
                return [["some", "results"]]

        wl = Mock()
        results = wl.works_for_hits(db.session, ["hit1", "hit2"])

        # The list of hits was itself wrapped in a list, and passed
        # into works_for_resultsets().
        assert (db.session, [["hit1", "hit2"]]) == wl.called_with

        # The return value -- a list of lists of results, which
        # contained a single item -- was unrolled and used as the
        # return value of works_for_hits().
        assert ["some", "results"] == results

    def test_works_for_resultsets(self, db: DatabaseTransactionFixture):
        # Verify that WorkList.works_for_resultsets turns lists of
        # (mocked) Hit objects into lists of Work objects

        # Create the WorkList we'll be testing with.
        wl = WorkList()
        wl.initialize(db.default_library())
        m = wl.works_for_resultsets

        # Create two works.
        w1 = db.work(with_license_pool=True)
        w2 = db.work(with_license_pool=True)

        class MockHit:
            def __init__(self, work_id):
                if isinstance(work_id, Work):
                    self.work_id = work_id.id
                else:
                    self.work_id = work_id

        hit1 = MockHit(w1)
        hit2 = MockHit(w2)

        # For each list of hits passed in, a corresponding list of
        # Works is returned.
        assert [[w2]] == m(db.session, [[hit2]])
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])
        assert [[w1, w1], [w2, w2], []] == m(
            db.session, [[hit1, hit1], [hit2, hit2], []]
        )

        # Works are returned in the order we ask for.
        for ordering in ([hit1, hit2], [hit2, hit1]):
            [works] = m(db.session, [ordering])
            assert [x.work_id for x in ordering] == [x.id for x in works]

        # If we ask for a work ID that's not in the database,
        # we don't get it.
        assert [[]] == m(db.session, [[MockHit(-100)]])

        # Finally, test that undeliverable works are filtered out.
        for lpdm in w2.license_pools[0].delivery_mechanisms:
            db.session.delete(lpdm)
            assert [[]] == m(db.session, [[hit2]])

    def test_search_target(self):
        # A WorkList can be searched - it is its own search target.
        wl = WorkList()
        assert wl == wl.search_target

    def test_search(self, db: DatabaseTransactionFixture):
        # Test the successful execution of WorkList.search()

        class MockWorkList(WorkList):
            def works_for_hits(self, _db, work_ids):
                self.works_for_hits_called_with = (_db, work_ids)
                return "A bunch of Works"

        wl = MockWorkList()
        wl.initialize(db.default_library(), audiences=[Classifier.AUDIENCE_CHILDREN])
        query = "a query"

        class MockSearchClient:
            def query_works(self, query, filter, pagination, debug):
                self.query_works_called_with = (query, filter, pagination, debug)
                return "A bunch of work IDs"

        # Search with the default arguments.
        client = MockSearchClient()
        results = wl.search(db.session, query, client)

        # The results of query_works were passed into
        # MockWorkList.works_for_hits.
        assert (db.session, "A bunch of work IDs") == wl.works_for_hits_called_with

        # The return value of MockWorkList.works_for_hits is
        # used as the return value of query_works().
        assert "A bunch of Works" == results

        # From this point on we are only interested in the arguments
        # passed in to query_works, since MockSearchClient always
        # returns the same result.

        # First, let's see what the default arguments look like.
        qu, filter, pagination, debug = client.query_works_called_with

        # The query was passed through.
        assert query == qu
        assert False == debug

        # A Filter object was created to match only works that belong
        # in the MockWorkList.
        assert [
            Classifier.AUDIENCE_CHILDREN,
            Classifier.AUDIENCE_ALL_AGES,
        ] == filter.audiences

        # A default Pagination object was created.
        assert 0 == pagination.offset
        assert Pagination.DEFAULT_SEARCH_SIZE == pagination.size

        # Now let's try a search with specific Pagination and Facets
        # objects.
        facets = SearchFacets(languages=["chi"])
        pagination = object()
        results = wl.search(db.session, query, client, pagination, facets, debug=True)

        qu, filter, pag, debug = client.query_works_called_with
        assert query == qu
        assert pagination == pag
        assert True == debug

        # The Filter incorporates restrictions imposed by both the
        # MockWorkList and the Facets.
        assert [
            Classifier.AUDIENCE_CHILDREN,
            Classifier.AUDIENCE_ALL_AGES,
        ] == filter.audiences
        assert ["chi"] == filter.languages

    def test_search_failures(self, db: DatabaseTransactionFixture):
        # Test reasons why WorkList.search() might not work.
        wl = WorkList()
        wl.initialize(db.default_library())
        query = "a query"

        # If there is no SearchClient, there are no results.
        assert [] == wl.search(db.session, query, None)

        # If the SearchClient returns nothing, there are no results.
        class NoResults:
            def query_works(self, *args, **kwargs):
                return None

        assert [] == wl.search(db.session, query, NoResults())

        # If there's an Opensearch exception during the query,
        # there are no results.
        class RaisesException:
            def query_works(self, *args, **kwargs):
                raise OpenSearchException("oh no")

        assert [] == wl.search(db.session, query, RaisesException())

    def test_worklist_for_resultset_no_holds_allowed(
        self, db: DatabaseTransactionFixture
    ):
        wl = WorkList()
        wl.initialize(db.default_library())
        m = wl.works_for_resultsets

        # Create two works.
        w1: Work = db.work(with_license_pool=True)
        w2: Work = db.work(with_license_pool=True)

        w1.license_pools[0].licenses_available = 0
        collection1: Collection = w1.license_pools[0].collection
        integration1 = collection1.integration_configuration
        integration1_library_config = integration1.for_library(db.default_library())
        settings = BibliothecaAPI.library_settings_class()(
            dont_display_reserves=ConfigurationAttributeValue.NOVALUE
        )
        assert integration1_library_config is not None
        BibliothecaAPI.library_settings_update(integration1_library_config, settings)
        db.session.commit()

        class MockHit:
            def __init__(self, work_id, has_last_update=False):
                if isinstance(work_id, Work):
                    self.work_id = work_id.id
                else:
                    self.work_id = work_id
                self.has_last_update = has_last_update

            def __contains__(self, k):
                # Pretend to have the 'last_update' script field,
                # if necessary.
                return k == "last_update" and self.has_last_update

        hit1 = MockHit(w1)
        hit2 = MockHit(w2)

        # Basic test
        # For each list of hits passed in, a corresponding list of
        # Works is returned.
        assert [[w2]] == m(db.session, [[hit2]])
        assert [[w2], []] == m(db.session, [[hit2], [hit1]])
        assert [[], [w2, w2], []] == m(db.session, [[hit1, hit1], [hit2, hit2], []])

        # Restricted pool has availability
        w1.license_pools[0].licenses_available = 1
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

        # Revert back, no availablility
        w1.license_pools[0].licenses_available = 0

        # Work1 now has 2 licensepools, one of which has availability
        alternate_collection = db.collection()
        alternate_collection.associated_libraries.append(db.default_library())
        alternate_w1_lp: LicensePool = db.licensepool(
            w1.presentation_edition, collection=alternate_collection
        )
        alternate_w1_lp.work_id = w1.id
        db.session.add_all([alternate_collection, alternate_w1_lp])
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

        # Still show availability since alternate collection is not restricted
        alternate_w1_lp.licenses_available = 0
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

        # Now both collections are restricted and have no availability
        alternate_collection_library_config = (
            alternate_collection.integration_configuration.for_library(
                db.default_library()
            )
        )
        assert alternate_collection_library_config is not None
        BibliothecaAPI.library_settings_update(
            alternate_collection_library_config, settings
        )
        assert [[w2], []] == m(db.session, [[hit2], [hit1]])

        # Both restricted but one has availability
        alternate_w1_lp.licenses_available = 1
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

    def test_worklist_for_resultset_no_collection_holds_allowed(
        self, db: DatabaseTransactionFixture
    ):
        # This test mirrors the one above in `test_worklist_for_resultset_no_holds_allowed`,
        # but at the collection, instead of library, level.

        wl = WorkList()
        wl.initialize(db.default_library())
        m = wl.works_for_resultsets

        # Create two works.
        w1: Work = db.work(with_license_pool=True)
        w2: Work = db.work(with_license_pool=True)

        w1.license_pools[0].licenses_available = 0
        collection1: Collection = w1.license_pools[0].collection
        integration1 = collection1.integration_configuration
        opds2_with_odl_required_settings = dict(
            external_account_id="http://account_id",
            data_source="distributor X",
            username="user",
            password="pw",
        )
        setting_under_test = dict(hold_limit=0)
        settings = OPDS2WithODLApi.settings_class()(
            **opds2_with_odl_required_settings | setting_under_test
        )
        OPDS2WithODLApi.settings_update(integration1, settings)
        db.session.commit()

        class MockHit:
            def __init__(self, work_id, has_last_update=False):
                self.work_id = work_id.id if isinstance(work_id, Work) else work_id
                self.has_last_update = has_last_update

            def __contains__(self, k):
                # Pretend to have the 'last_update' script field,
                # if necessary.
                return k == "last_update" and self.has_last_update

        hit1 = MockHit(w1)
        hit2 = MockHit(w2)

        # Basic test
        # For each list of hits passed in, a corresponding list of
        # Works is returned.
        assert [[w2]] == m(db.session, [[hit2]])
        assert [[w2], []] == m(db.session, [[hit2], [hit1]])
        assert [[], [w2, w2], []] == m(db.session, [[hit1, hit1], [hit2, hit2], []])

        # Restricted pool has availability
        w1.license_pools[0].licenses_available = 1
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

        # Revert back, no availablility
        w1.license_pools[0].licenses_available = 0

        # Work1 now has 2 license pools, one of which has availability
        alternate_collection = db.collection()
        alternate_collection.associated_libraries.append(db.default_library())
        alternate_w1_lp: LicensePool = db.licensepool(
            w1.presentation_edition, collection=alternate_collection
        )
        alternate_w1_lp.work_id = w1.id
        db.session.add_all([alternate_collection, alternate_w1_lp])
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

        # Still show availability since alternate collection is not restricted
        alternate_w1_lp.licenses_available = 0
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

        # Now both collections are restricted and have no availability
        OPDS2WithODLApi.settings_update(
            alternate_collection.integration_configuration, settings
        )
        assert [[w2], []] == m(db.session, [[hit2], [hit1]])

        # Both restricted but one has availability
        alternate_w1_lp.licenses_available = 1
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])


class WorkListGroupsEndToEndFixture:
    @dataclass
    class WorkData:
        best_seller_list: CustomList
        hq_litfic: Work
        hq_ro: Work
        hq_sf: Work
        lq_litfic: Work
        lq_ro: Work
        lq_sf: Work
        mq_ro: Work
        mq_sf: Work
        nonfiction: Work
        children_with_age: Work
        children_without_age: Work
        staff_picks_list: CustomList

    @dataclass
    class LaneData:
        fiction: Lane
        best_sellers: Lane
        staff_picks: Lane
        sf_lane: Lane
        romance_lane: Lane
        discredited_nonfiction: Lane
        children: Lane

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        external_search_fixture: EndToEndSearchFixture,
        library_fixture: LibraryFixture,
    ):
        self.db = db
        self.external_search_fixture = external_search_fixture
        self.library_fixture = library_fixture

        # In this library, the groups feed includes at most two books
        # for each lane.
        self.library = db.default_library()
        self.library_settings = library_fixture.settings(self.library)
        self.library_settings.featured_lane_size = 2

    def populate_works(self) -> WorkData:
        db = self.db
        session = self.db.session

        # Create eight works.
        hq_litfic = db.work(
            title="HQ LitFic",
            fiction=True,
            genre="Literary Fiction",
            with_license_pool=True,
        )
        hq_litfic.quality = 0.8
        lq_litfic = db.work(
            title="LQ LitFic",
            fiction=True,
            genre="Literary Fiction",
            with_license_pool=True,
        )
        lq_litfic.quality = 0
        hq_sf = db.work(
            title="HQ SF", genre="Science Fiction", fiction=True, with_license_pool=True
        )

        # Create children works.
        children_with_age = db.work(
            title="Children work with target age",
            audience=Classifier.AUDIENCE_CHILDREN,
            with_license_pool=True,
        )
        children_with_age.target_age = tuple_to_numericrange((0, 3))

        children_without_age = db.work(
            title="Children work with out target age",
            audience=Classifier.AUDIENCE_CHILDREN,
            with_license_pool=True,
        )

        # Add a lot of irrelevant genres to one of the works. This
        # won't affect the results.
        for genre in ["Westerns", "Horror", "Erotica"]:
            genre_obj, is_new = Genre.lookup(session, genre)
            get_one_or_create(session, WorkGenre, work=hq_sf, genre=genre_obj)

        hq_sf.quality = 0.8
        mq_sf = db.work(
            title="MQ SF", genre="Science Fiction", fiction=True, with_license_pool=True
        )
        mq_sf.quality = 0.6
        lq_sf = db.work(
            title="LQ SF", genre="Science Fiction", fiction=True, with_license_pool=True
        )
        lq_sf.quality = 0.1
        hq_ro = db.work(
            title="HQ Romance", genre="Romance", fiction=True, with_license_pool=True
        )
        hq_ro.quality = 0.79
        mq_ro = db.work(
            title="MQ Romance", genre="Romance", fiction=True, with_license_pool=True
        )
        mq_ro.quality = 0.6
        # This work is in a different language -- necessary to run the
        # LQRomanceEntryPoint test below.
        lq_ro = db.work(
            title="LQ Romance",
            genre="Romance",
            fiction=True,
            language="lan",
            with_license_pool=True,
        )
        lq_ro.quality = 0.1
        nonfiction = db.work(title="Nonfiction", fiction=False, with_license_pool=True)

        # One of these works (mq_sf) is a best-seller and also a staff
        # pick.
        best_seller_list, ignore = db.customlist(num_entries=0)
        best_seller_list.add_entry(mq_sf)

        staff_picks_list, ignore = db.customlist(num_entries=0)
        staff_picks_list.add_entry(mq_sf)
        return self.WorkData(
            best_seller_list=best_seller_list,
            hq_litfic=hq_litfic,
            hq_ro=hq_ro,
            hq_sf=hq_sf,
            lq_litfic=lq_litfic,
            lq_ro=lq_ro,
            lq_sf=lq_sf,
            mq_ro=mq_ro,
            mq_sf=mq_sf,
            nonfiction=nonfiction,
            children_with_age=children_with_age,
            children_without_age=children_without_age,
            staff_picks_list=staff_picks_list,
        )

    def populate_search_index(self):
        self.external_search_fixture.populate_search_index()

    def create_lanes(self, data: WorkData) -> LaneData:
        db = self.db

        # Create a 'Fiction' lane with five sublanes.
        fiction = db.lane("Fiction")
        fiction.fiction = True

        # "Best Sellers", which will contain one book.
        best_sellers = db.lane("Best Sellers", parent=fiction)
        best_sellers.customlists.append(data.best_seller_list)

        # "Staff Picks", which will contain the same book.
        staff_picks = db.lane("Staff Picks", parent=fiction)
        staff_picks.customlists.append(data.staff_picks_list)

        # "Science Fiction", which will contain two books (but
        # will not contain the best-seller).
        sf_lane = db.lane("Science Fiction", parent=fiction, genres=["Science Fiction"])

        # "Romance", which will contain two books.
        romance_lane = db.lane("Romance", parent=fiction, genres=["Romance"])

        # "Discredited Nonfiction", which contains a book that would
        # not normally appear in 'Fiction'.
        discredited_nonfiction = db.lane(
            "Discredited Nonfiction", fiction=False, parent=fiction
        )
        discredited_nonfiction.inherit_parent_restrictions = False

        # "Children", which will contain one book, the one with audience children and defined target age.
        children = db.lane("Children")
        children.audiences = Classifier.AUDIENCE_CHILDREN
        children.target_age = (0, 4)

        return self.LaneData(
            fiction=fiction,
            best_sellers=best_sellers,
            staff_picks=staff_picks,
            sf_lane=sf_lane,
            romance_lane=romance_lane,
            discredited_nonfiction=discredited_nonfiction,
            children=children,
        )

    def database_facets(self, library: Library | None = None) -> DatabaseBackedFacets:
        library = library or self.db.default_library()
        return DatabaseBackedFacets(
            library,
            availability=Facets.AVAILABLE_ALL,
            order=Facets.ORDER_TITLE,
            distributor=None,
            collection_name=None,
        )

    def work_ids_from_search(self, lane: Lane) -> list[int]:
        session = self.db.session
        facets = self.database_facets()
        index = self.external_search_fixture.external_search_index
        return [
            work.id
            for work in lane.works(
                session,
                facets,
                search_engine=index,
            )
        ]

    def work_ids_from_db(self, lane: Lane) -> list[int]:
        session = self.db.session
        facets = self.database_facets()
        return [work.id for work in lane.works_from_database(session, facets)]


@pytest.fixture
def work_list_groups_end_to_end_fixture(
    db: DatabaseTransactionFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
    library_fixture: LibraryFixture,
) -> WorkListGroupsEndToEndFixture:
    return WorkListGroupsEndToEndFixture(db, end_to_end_search_fixture, library_fixture)


class TestWorkListGroupsEndToEnd:
    # A comprehensive end-to-end test of WorkList.groups()
    # using a real Opensearch index.
    #
    # Helper methods are tested in a different class, TestWorkListGroups
    def test_groups(
        self,
        work_list_groups_end_to_end_fixture: WorkListGroupsEndToEndFixture,
    ):
        fixture = work_list_groups_end_to_end_fixture
        db = work_list_groups_end_to_end_fixture.db
        search_fixture = fixture.external_search_fixture
        index = search_fixture.external_search_index
        session = db.session

        work_data = fixture.populate_works()
        fixture.populate_search_index()
        lane_data = fixture.create_lanes(work_data)

        def assert_contents(g, expect):
            """Assert that a generator yields the expected
            (Work, lane) 2-tuples.
            """
            results = list(g)
            expect = [(x[0].sort_title, x[1].display_name) for x in expect]
            actual = [(x[0].sort_title, x[1].display_name) for x in results]
            for i, expect_item in enumerate(expect):
                if i >= len(actual):
                    actual_item = None
                else:
                    actual_item = actual[i]
                assert expect_item == actual_item, (
                    "Mismatch in position %d: Expected %r, got %r.\nOverall, expected:\n%r\nGot:\n%r:"
                    % (i, expect_item, actual_item, expect, actual)
                )
            assert len(expect) == len(actual), (
                "Expect matches actual, but actual has extra members.\nOverall, expected:\n%r\nGot:\n%r:"
                % (expect, actual)
            )

        def make_groups(lane, facets=None, **kwargs):
            # Run the `WorkList.groups` method in a way that's
            # instrumented for this unit test.

            # Most of the time, we want a simple deterministic query.
            facets = facets or FeaturedFacets(1, random_seed=Filter.DETERMINISTIC)

            return lane.groups(
                session,
                facets=facets,
                search_engine=index,
                debug=True,
                **kwargs,
            )

        assert_contents(
            make_groups(lane_data.fiction),
            [
                # The lanes based on lists feature every title on the
                # list.  This isn't enough to pad out the lane to
                # FEATURED_LANE_SIZE, but nothing else belongs in the
                # lane.
                (work_data.mq_sf, lane_data.best_sellers),
                # In fact, both lanes feature the same title -- this
                # generally won't happen but it can happen when
                # multiple lanes are based on lists that feature the
                # same title.
                (work_data.mq_sf, lane_data.staff_picks),
                # The genre-based lanes contain FEATURED_LANE_SIZE
                # (two) titles each. The 'Science Fiction' lane
                # features a low-quality work because the
                # medium-quality work was already used above.
                (work_data.hq_sf, lane_data.sf_lane),
                (work_data.lq_sf, lane_data.sf_lane),
                (work_data.hq_ro, lane_data.romance_lane),
                (work_data.mq_ro, lane_data.romance_lane),
                # The 'Discredited Nonfiction' lane contains a single
                # book. There just weren't enough matching books to fill
                # out the lane to FEATURED_LANE_SIZE.
                (work_data.nonfiction, lane_data.discredited_nonfiction),
                # The 'Fiction' lane contains a title that fits in the
                # fiction lane but was not classified under any other
                # lane. It also contains a title that was previously
                # featured earlier. The search index knows about a
                # title (lq_litfix) that was not previously featured,
                # but we didn't see it because the Opensearch query
                # didn't happen to fetch it.
                #
                # Each lane gets a separate query, and there were too
                # many high-quality works in 'fiction' for the
                # low-quality one to show up.
                (work_data.hq_litfic, lane_data.fiction),
                (work_data.hq_sf, lane_data.fiction),
            ],
        )

        # If we ask only about 'Fiction', not including its sublanes,
        # we get only the subset of the books previously returned for
        # 'fiction'.
        assert_contents(
            make_groups(lane_data.fiction, include_sublanes=False),
            [
                (work_data.hq_litfic, lane_data.fiction),
                (work_data.hq_sf, lane_data.fiction),
            ],
        )

        # If we exclude 'Fiction' from its own grouped feed, we get
        # all the other books/lane combinations *except for* the books
        # associated directly with 'Fiction'.
        lane_data.fiction.include_self_in_grouped_feed = False
        assert_contents(
            make_groups(lane_data.fiction),
            [
                (work_data.mq_sf, lane_data.best_sellers),
                (work_data.mq_sf, lane_data.staff_picks),
                (work_data.hq_sf, lane_data.sf_lane),
                (work_data.lq_sf, lane_data.sf_lane),
                (work_data.hq_ro, lane_data.romance_lane),
                (work_data.mq_ro, lane_data.romance_lane),
                (work_data.nonfiction, lane_data.discredited_nonfiction),
            ],
        )
        lane_data.fiction.include_self_in_grouped_feed = True

        # When a lane has no sublanes, its behavior is the same whether
        # it is called with include_sublanes true or false.
        for include_sublanes in (True, False):
            assert_contents(
                lane_data.discredited_nonfiction.groups(
                    session, include_sublanes=include_sublanes
                ),
                [(work_data.nonfiction, lane_data.discredited_nonfiction)],
            )

        # When a lane's audience is "Children" we need work to have explicit target_age to be included in the lane
        assert_contents(
            make_groups(lane_data.children),
            [(work_data.children_with_age, lane_data.children)],
        )

        # If we make the lanes thirstier for content, we see slightly
        # different behavior.
        fixture.library_settings.featured_lane_size = 3
        assert_contents(
            make_groups(lane_data.fiction),
            [
                # The list-based lanes are the same as before.
                (work_data.mq_sf, lane_data.best_sellers),
                (work_data.mq_sf, lane_data.staff_picks),
                # After using every single science fiction work that
                # wasn't previously used, we reuse self.mq_sf to pad the
                # "Science Fiction" lane up to three items. It's
                # better to have self.lq_sf show up before self.mq_sf, even
                # though it's lower quality, because self.lq_sf hasn't been
                # used before.
                (work_data.hq_sf, lane_data.sf_lane),
                (work_data.lq_sf, lane_data.sf_lane),
                (work_data.mq_sf, lane_data.sf_lane),
                # The 'Romance' lane now contains all three Romance
                # titles, with the higher-quality titles first.
                (work_data.hq_ro, lane_data.romance_lane),
                (work_data.mq_ro, lane_data.romance_lane),
                (work_data.lq_ro, lane_data.romance_lane),
                # The 'Discredited Nonfiction' lane is the same as
                # before.
                (work_data.nonfiction, lane_data.discredited_nonfiction),
                # After using every single fiction work that wasn't
                # previously used, we reuse high-quality works to pad
                # the "Fiction" lane to three items. The
                # lowest-quality Romance title doesn't show up here
                # anymore, because the 'Romance' lane claimed it. If
                # we have to reuse titles, we'll reuse the
                # high-quality ones.
                (work_data.hq_litfic, lane_data.fiction),
                (work_data.hq_sf, lane_data.fiction),
                (work_data.hq_ro, lane_data.fiction),
            ],
        )

        # Let's see how entry points affect the feeds.
        #

        # There are no audiobooks in the system, so passing in a
        # FeaturedFacets scoped to the AudiobooksEntryPoint excludes everything.
        fetured_facets = FeaturedFacets(0, entrypoint=AudiobooksEntryPoint)
        _db = session
        assert [] == list(lane_data.fiction.groups(session, facets=fetured_facets))

        # Here's an entry point that applies a language filter
        # that only finds one book.
        class LQRomanceEntryPoint(EntryPoint):
            URI = ""

            @classmethod
            def modify_search_filter(cls, filter):
                filter.languages = ["lan"]

        fetured_facets = FeaturedFacets(
            1, entrypoint=LQRomanceEntryPoint, random_seed=Filter.DETERMINISTIC
        )
        assert_contents(
            make_groups(lane_data.fiction, facets=fetured_facets),
            [
                # The single recognized book shows up in both lanes
                # that can show it.
                (work_data.lq_ro, lane_data.romance_lane),
                (work_data.lq_ro, lane_data.fiction),
            ],
        )

        # Now, instead of relying on the 'Fiction' lane, make a
        # WorkList containing two different lanes, and call groups() on
        # the WorkList.

        class MockWorkList:
            display_name = "Mock"
            visible = True
            priority = 2

            def groups(slf, _db, include_sublanes, pagination=None, facets=None):
                yield work_data.lq_litfic, slf

        mock = MockWorkList()

        wl = WorkList()
        wl.initialize(
            db.default_library(),
            children=[lane_data.best_sellers, lane_data.staff_picks, mock],
        )

        # We get results from the two lanes and from the MockWorkList.
        # Since the MockWorkList wasn't a lane, its results were obtained
        # by calling groups() recursively.
        assert_contents(
            wl.groups(session),
            [
                (work_data.mq_sf, lane_data.best_sellers),
                (work_data.mq_sf, lane_data.staff_picks),
                (work_data.lq_litfic, mock),
            ],
        )

    def test_works_and_works_from_database(
        self,
        work_list_groups_end_to_end_fixture: WorkListGroupsEndToEndFixture,
    ):
        fixture = work_list_groups_end_to_end_fixture
        search_fixture = fixture.external_search_fixture

        # Create a bunch of lanes and works.
        data = fixture.populate_works()
        lane_data = fixture.create_lanes(data)
        fixture.populate_search_index()

        # Since we have a bunch of lanes and works, plus an
        # Opensearch index, let's take this opportunity to verify that
        # WorkList.works and DatabaseBackedWorkList.works_from_database
        # give the same results.
        for lane_name in fields(lane_data):
            lane = getattr(lane_data, lane_name.name)
            from_search = fixture.work_ids_from_search(lane)
            from_db = fixture.work_ids_from_db(lane)
            assert from_search == from_db

    def test_works_and_works_from_database_with_suppressed(
        self,
        work_list_groups_end_to_end_fixture: WorkListGroupsEndToEndFixture,
    ):
        db = work_list_groups_end_to_end_fixture.db
        fixture = work_list_groups_end_to_end_fixture
        index = fixture.external_search_fixture.external_search_index

        # Create a bunch of lanes and works.
        data = fixture.populate_works()
        lane_data = fixture.create_lanes(data)

        decoy_library = db.library()
        another_library = db.library()

        db.default_collection().associated_libraries += [decoy_library, another_library]

        # Add a couple suppressed works, to make sure they don't show up in the results.
        globally_suppressed_work = db.work(
            title="Suppressed LP",
            fiction=True,
            genre="Literary Fiction",
            with_license_pool=True,
        )
        globally_suppressed_work.quality = 0.95
        for license_pool in globally_suppressed_work.license_pools:
            license_pool.suppressed = True

        # This work is only suppressed for a specific library.
        library_suppressed_work = db.work(
            title="Suppressed 2",
            fiction=True,
            genre="Literary Fiction",
            with_license_pool=True,
        )
        library_suppressed_work.quality = 0.95
        library_suppressed_work.suppressed_for = [fixture.library, decoy_library]

        fixture.populate_search_index()

        for lane_name in fields(lane_data):
            lane = getattr(lane_data, lane_name.name)
            from_search = fixture.work_ids_from_search(lane)
            from_db = fixture.work_ids_from_db(lane)

            # The suppressed work is not included in the results.
            assert globally_suppressed_work.id not in from_search
            assert globally_suppressed_work.id not in from_db
            assert library_suppressed_work.id not in from_search
            assert library_suppressed_work.id not in from_db

        # Test the decoy libraries lane as well
        decoy_library_lane = db.lane("Fiction", fiction=True, library=decoy_library)
        from_search = fixture.work_ids_from_search(decoy_library_lane)
        from_db = fixture.work_ids_from_db(decoy_library_lane)
        assert globally_suppressed_work.id not in from_search
        assert globally_suppressed_work.id not in from_db
        assert library_suppressed_work.id not in from_search
        assert library_suppressed_work.id not in from_db

        # Test a lane for a different library, this time the globally suppressed work should
        # still be absent, but the work suppressed for the other library should be present.
        another_library_lane = db.lane("Fiction", fiction=True, library=another_library)
        from_search = fixture.work_ids_from_search(another_library_lane)
        from_db = fixture.work_ids_from_db(another_library_lane)
        assert globally_suppressed_work.id not in from_search
        assert globally_suppressed_work.id not in from_db
        assert library_suppressed_work.id in from_search
        assert library_suppressed_work.id in from_db

        # Make sure that the suppressed works are handled correctly when searching in a lane as well
        assert library_suppressed_work in another_library_lane.search(
            db.session, "suppressed", index
        )
        assert library_suppressed_work not in lane_data.fiction.search(
            db.session, "suppressed", index
        )
        assert library_suppressed_work not in decoy_library_lane.search(
            db.session, "suppressed", index
        )


class RandomSeedFixture:
    def __init__(self):
        random.seed(42)


@pytest.fixture
def random_seed_fixture() -> RandomSeedFixture:
    """A fixture that initializes the RNG to a predictable value each time."""
    return RandomSeedFixture()


class TestWorkListGroups:
    def test_groups_for_lanes_adapts_facets(
        self,
        random_seed_fixture: RandomSeedFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        db = end_to_end_search_fixture.db

        # Verify that _groups_for_lanes gives each of a WorkList's
        # non-queryable children the opportunity to adapt the incoming
        # FeaturedFacets objects to its own needs.

        class MockParent(WorkList):
            def _featured_works_with_lanes(
                self, _db, lanes, pagination, facets, *args, **kwargs
            ):
                self._featured_works_with_lanes_called_with = (
                    lanes,
                    pagination,
                    facets,
                )
                return super()._featured_works_with_lanes(
                    _db, lanes, pagination, facets, *args, **kwargs
                )

        class MockChild(WorkList):
            def __init__(self, work):
                self.work = work
                self.id = work.title
                super().__init__()

            def overview_facets(self, _db, facets):
                self.overview_facets_called_with = (_db, facets)
                return "Custom facets for %s." % self.id

            def works(self, _db, pagination, facets, *args, **kwargs):
                self.works_called_with = (pagination, facets)
                return [self.work]

        parent = MockParent()
        child1 = MockChild(db.work(title="Lane 1"))
        child2 = MockChild(db.work(title="Lane 2"))
        children = [child1, child2]

        for wl in children:
            wl.initialize(library=db.default_library())
        parent.initialize(library=db.default_library(), children=[child1, child2])

        # We're going to make a grouped feed in which both children
        # are relevant, but neither one is queryable.
        relevant = parent.children
        queryable: list = []
        pagination = Pagination(size=2)
        facets = FeaturedFacets(0)
        groups = list(
            parent._groups_for_lanes(
                db.session, relevant, queryable, pagination, facets
            )
        )

        # Each sublane was asked in turn to provide works for the feed.
        assert [(child1.work, child1), (child2.work, child2)] == groups

        # But we're more interested in what happened to the faceting objects.

        # The original faceting object was passed into
        # _featured_works_with_lanes, but none of the lanes were
        # queryable, so it ended up doing nothing.
        assert ([], pagination, facets) == parent._featured_works_with_lanes_called_with

        # Each non-queryable sublane was given a chance to adapt that
        # faceting object to its own needs.
        for wl in children:
            assert wl.overview_facets_called_with == ((db.session), facets)

        # Each lane's adapted faceting object was then passed into
        # works().
        assert (pagination, "Custom facets for Lane 1.") == child1.works_called_with

        assert (pagination, "Custom facets for Lane 2.") == child2.works_called_with

        # If no pagination object is passed in (the most common case),
        # a new Pagination object is created based on the featured lane
        # size for the library.
        groups = list(
            parent._groups_for_lanes(db.session, relevant, queryable, None, facets)
        )

        (ignore1, pagination, ignore2) = parent._featured_works_with_lanes_called_with
        assert isinstance(pagination, Pagination)

        # For each sublane, we ask for 10% more items than we need to
        # reduce the chance that we'll need to put the same item in
        # multiple lanes.
        assert (
            int(db.default_library().settings.featured_lane_size * 1.10)
            == pagination.size
        )

    def test_featured_works_with_lanes(
        self,
        db: DatabaseTransactionFixture,
        random_seed_fixture: RandomSeedFixture,
    ):
        # _featured_works_with_lanes builds a list of queries and
        # passes the list into search_engine.works_query_multi(). It
        # passes the search results into works_for_resultsets() to
        # create a sequence of (Work, Lane) 2-tuples.
        class MockWorkList(WorkList):
            """Mock the behavior of WorkList that's not being tested here --
            overview_facets() for the child lanes that are being
            searched, and works_for_resultsets() for the parent that's
            doing the searching.
            """

            def __init__(self, *args, **kwargs):
                # Track all the times overview_facets is called (it
                # should be called twice), plus works_for_resultsets
                # (which should only be called once).
                super().__init__(*args, **kwargs)
                self.works_for_resultsets_calls = []
                self.overview_facets_calls = []

            def overview_facets(self, _db, facets):
                # Track that overview_facets was called with a
                # FeaturedFacets object. Then call the superclass
                # implementation -- we need to return a real Facets
                # object so it can be turned into a Filter.
                assert isinstance(facets, FeaturedFacets)
                self.overview_facets_calls.append((_db, facets))
                return super().overview_facets(_db, facets)

            def works_for_resultsets(self, _db, resultsets, facets=None):
                # Take some lists of (mocked) of search results and turn
                # them into lists of (mocked) Works.
                self.works_for_resultsets_calls.append((_db, resultsets))
                one_lane_worth = [["Here is", "one lane", "of works"]]
                return one_lane_worth * len(resultsets)

        class MockSearchEngine:
            """Mock a multi-query call to an Opensearch server."""

            def __init__(self):
                self.called_with = None

            def query_works_multi(self, queries):
                # Pretend to run a multi-query and return three lists of
                # mocked results.
                self.called_with = queries
                return [["some"], ["search"], ["results"]]

        # Now the actual test starts. We've got a parent lane with two
        # children.
        parent = MockWorkList()
        child1 = MockWorkList()
        child2 = MockWorkList()
        parent.initialize(
            library=db.default_library(),
            children=[child1, child2],
            display_name="Parent lane -- call my _featured_works_with_lanes()!",
        )
        child1.initialize(library=db.default_library(), display_name="Child 1")
        child2.initialize(library=db.default_library(), display_name="Child 2")

        # We've got a search engine that's ready to find works in any
        # of these lanes.
        search = MockSearchEngine()

        # Set up facets and pagination, and call the method that's
        # being tested.
        facets = FeaturedFacets(0.1)
        pagination = object()
        results = parent._featured_works_with_lanes(
            db.session, [child1, child2], pagination, facets, search_engine=search
        )
        results = list(results)

        # MockSearchEngine.query_works_multi was called on a list of
        # queries it prepared from child1 and child2.
        q1, q2 = search.called_with

        # These queries are almost the same.
        for query in search.called_with:
            # Neither has a query string.
            assert None == query[0]
            # Both have the same pagination object.
            assert pagination == query[2]

        # But each query has a different Filter.
        f1 = q1[1]
        f2 = q2[1]
        assert f1 != f2

        # How did these Filters come about? Well, for each lane, we
        # called overview_facets() and passed in the same
        # FeaturedFacets object.
        assert ((db.session), facets) == child1.overview_facets_calls.pop()
        assert [] == child1.overview_facets_calls
        child1_facets = child1.overview_facets(db.session, facets)

        assert ((db.session), facets) == child2.overview_facets_calls.pop()
        assert [] == child2.overview_facets_calls
        child2_facets = child1.overview_facets(db.session, facets)

        # We then passed each result into Filter.from_worklist, along
        # with the corresponding lane.
        compare_f1 = Filter.from_worklist(db.session, child1, child1_facets)
        compare_f2 = Filter.from_worklist(db.session, child2, child2_facets)

        # Reproducing that code inside this test, which we just did,
        # gives us Filter objects -- compare_f1 and compare_f2 --
        # identical to the ones passed into query_works_multi -- f1
        # and f2. We know they're the same because they build() to
        # identical dictionaries.
        assert compare_f1.build() == f1.build()
        assert compare_f2.build() == f2.build()

        # So we ended up with q1 and q2, two queries to find the works
        # from child1 and child2. That's what was passed into
        # query_works_multi().

        # We know that query_works_multi() returned: a list
        # of lists of fake "results" that looked like this:
        # [["some"], ["search"], ["results"]]
        #
        # This was passed into parent.works_for_resultsets():
        call = parent.works_for_resultsets_calls.pop()
        assert call == ((db.session), [["some"], ["search"], ["results"]])
        assert [] == parent.works_for_resultsets_calls

        # The return value of works_for_resultsets -- another list of
        # lists -- was then turned into a sequence of ('work', Lane)
        # 2-tuples.
        assert [
            ("Here is", child1),
            ("one lane", child1),
            ("of works", child1),
            ("Here is", child2),
            ("one lane", child2),
            ("of works", child2),
        ] == results
        # And that's how we got a sequence of 2-tuples mapping out a
        # grouped OPDS feed.

    def test__size_for_facets(
        self,
        db: DatabaseTransactionFixture,
        random_seed_fixture: RandomSeedFixture,
    ):
        lane = db.lane()
        m = lane._size_for_facets

        ebooks, audio, everything, nothing = (
            FeaturedFacets(minimum_featured_quality=0.5, entrypoint=x)
            for x in (
                EbooksEntryPoint,
                AudiobooksEntryPoint,
                EverythingEntryPoint,
                None,
            )
        )

        # When Lane.size_by_entrypoint is not set, Lane.size is used.
        # This should only happen immediately after a site is upgraded.
        lane.size = 100
        for facets in (ebooks, audio):
            assert 100 == lane._size_for_facets(facets)

        # Once Lane.size_by_entrypoint is set, it's used when possible.
        lane.size_by_entrypoint = {
            EverythingEntryPoint.URI: 99,
            EbooksEntryPoint.URI: 1,
            AudiobooksEntryPoint.URI: 2,
        }
        assert 99 == m(None)
        assert 99 == m(nothing)
        assert 99 == m(everything)
        assert 1 == m(ebooks)
        assert 2 == m(audio)

        # If size_by_entrypoint contains no estimate for a given
        # EntryPoint URI, the overall lane size is used. This can
        # happen between the time an EntryPoint is enabled and the
        # lane size refresh script is run.
        del lane.size_by_entrypoint[AudiobooksEntryPoint.URI]
        assert 100 == m(audio)
