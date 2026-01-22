import datetime

from sqlalchemy import and_, text

from palace.manager.core.classifier import Classifier
from palace.manager.feed.facets.database import DatabaseBackedFacets
from palace.manager.feed.facets.feed import Facets
from palace.manager.feed.worklist.database import DatabaseBackedWorkList
from palace.manager.search.pagination import Pagination
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolStatus,
    LicensePoolType,
)
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import tuple_to_numericrange
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture


class TestDatabaseBackedWorkList:
    def test_works_from_database(self, db: DatabaseTransactionFixture):
        # Verify that the works_from_database() method calls the
        # methods we expect, in the right order.
        class MockQuery:
            # Simulates the behavior of a database Query object
            # without the need to pass around actual database clauses.
            #
            # This is a lot of instrumentation but it means we can
            # test what happened inside works() mainly by looking at a
            # string of method names in the result object.
            def __init__(self, clauses, distinct=False):
                self.clauses = clauses
                self._distinct = distinct

            def filter(self, clause):
                # Create a new MockQuery object with a new clause
                return MockQuery(self.clauses + [clause], self._distinct)

            def distinct(self, fields):
                return MockQuery(self.clauses, fields)

            def __repr__(self):
                return "<MockQuery %d clauses, most recent %s>" % (
                    len(self.clauses),
                    self.clauses[-1],
                )

        class MockWorkList(DatabaseBackedWorkList):
            def __init__(self, _db):
                super().__init__()
                session = _db  # We'll be using this in assertions.
                self.stages = []

            def _stage(self, method_name, _db, qu, qu_is_previous_stage=True):
                # _db must always be session; check it here and then
                # ignore it.
                assert _db == db.session

                if qu_is_previous_stage:
                    # qu must be the MockQuery returned from the
                    # previous call.
                    assert qu == self.stages[-1]
                else:
                    # qu must be a new object, and _not_ the MockQuery
                    # returned from the previous call.
                    assert qu != self.stages[-1]

                # Create a new MockQuery with an additional filter,
                # named after the method that was called.
                new_filter = qu.filter(method_name)
                self.stages.append(new_filter)
                return new_filter

            def base_query(self, _db):
                # This kicks off the process -- most future calls will
                # use _stage().
                assert _db == db.session
                query = MockQuery(["base_query"])
                self.stages.append(query)
                return query

            def only_show_ready_deliverable_works(self, _db, qu):
                return self._stage("only_show_ready_deliverable_works", _db, qu)

            def _restrict_query_for_no_hold_collections(self, _db, qu):
                return self._stage("_restrict_query_for_no_hold_collections", _db, qu)

            def bibliographic_filter_clauses(self, _db, qu):
                # This method is a little different, so we can't use
                # _stage().
                #
                # This implementation doesn't change anything; it will be
                # replaced with an implementation that does.
                assert _db == db.session
                self.bibliographic_filter_clauses_called_with = qu
                return qu, []

            def modify_database_query_hook(self, _db, qu):
                return self._stage("modify_database_query_hook", _db, qu)

            def active_bibliographic_filter_clauses(self, _db, qu):
                # This alternate implementation of
                # bibliographic_filter_clauses returns a brand new
                # MockQuery object and a list of filters.
                self.pre_bibliographic_filter = qu
                new_query = MockQuery(
                    ["new query made inside active_bibliographic_filter_clauses"]
                )
                self.stages.append(new_query)
                return (new_query, [text("clause 1"), text("clause 2")])

        # The simplest case: no facets or pagination,
        # and bibliographic_filter_clauses does nothing.
        wl = MockWorkList(db.session)
        result = wl.works_from_database(db.session, extra_kwarg="ignored")

        # We got a MockQuery.
        assert isinstance(result, MockQuery)

        # During the course of the works() call, we verified that the
        # MockQuery is constructed by chaining method calls.  Now we
        # just need to verify that all the methods were called and in
        # the order we expect.
        assert [
            "base_query",
            "only_show_ready_deliverable_works",
            "_restrict_query_for_no_hold_collections",
            "modify_database_query_hook",
        ] == result.clauses

        # bibliographic_filter_clauses used a different mechanism, but
        # since it stored the MockQuery it was called with, we can see
        # when it was called -- just after
        # only_show_ready_deliverable_works.
        assert [
            "base_query",
            "only_show_ready_deliverable_works",
            "_restrict_query_for_no_hold_collections",
        ] == wl.bibliographic_filter_clauses_called_with.clauses
        wl.bibliographic_filter_clauses_called_with = None

        # Since nobody made the query distinct, it was set distinct on
        # Work.id.
        assert Work.id == result._distinct

        # Now we're going to do a more complicated test, with
        # faceting, pagination, and a bibliographic_filter_clauses that
        # actually does something.
        wl.bibliographic_filter_clauses = wl.active_bibliographic_filter_clauses

        class MockFacets(DatabaseBackedFacets):
            def __init__(self, wl):
                self.wl = wl

            def modify_database_query(self, _db, qu):
                # This is the only place we pass in False for
                # qu_is_previous_stage. This is called right after
                # bibliographic_filter_clauses, which caused a brand
                # new MockQuery object to be created.
                #
                # Normally, _stage() will assert that `qu` is the
                # return value from the previous call, but this time
                # we want to assert the opposite.
                result = self.wl._stage("facets", _db, qu, qu_is_previous_stage=False)

                distinct = result.distinct("some other field")
                self.wl.stages.append(distinct)
                return distinct

        class MockPagination:
            def __init__(self, wl):
                self.wl = wl

            def modify_database_query(self, _db, qu):
                return self.wl._stage("pagination", _db, qu)

        result = wl.works_from_database(
            db.session, facets=MockFacets(wl), pagination=MockPagination(wl)
        )

        # Here are the methods called before bibliographic_filter_clauses.
        assert [
            "base_query",
            "only_show_ready_deliverable_works",
            "_restrict_query_for_no_hold_collections",
        ] == wl.pre_bibliographic_filter.clauses

        # bibliographic_filter_clauses created a brand new object,
        # which ended up as our result after some more methods were
        # called on it.
        assert (
            "new query made inside active_bibliographic_filter_clauses"
            == result.clauses.pop(0)
        )

        # bibliographic_filter_clauses() returned two clauses which were
        # combined with and_().
        bibliographic_filter_clauses = result.clauses.pop(0)
        assert str(and_(text("clause 1"), text("clause 2"))) == str(
            bibliographic_filter_clauses
        )

        # The rest of the calls are easy to trac.
        assert [
            "facets",
            "modify_database_query_hook",
            "pagination",
        ] == result.clauses

        # The query was made distinct on some other field, so the
        # default behavior (making it distinct on Work.id) wasn't
        # triggered.
        assert "some other field" == result._distinct

    def test_works_from_database_end_to_end(self, db: DatabaseTransactionFixture):
        # Verify that works_from_database() correctly locates works
        # that match the criteria specified by the
        # DatabaseBackedWorkList, the faceting object, and the
        # pagination object.
        #
        # This is a simple end-to-end test of functionality that's
        # tested in more detail elsewhere.

        default_library = db.default_library()
        active_collection = db.default_collection()
        inactive_collection = db.default_inactive_collection()

        # Create two books.
        oliver_twist = db.work(
            title="Oliver Twist",
            with_license_pool=True,
            language="eng",
            collection=active_collection,
        )
        barnaby_rudge = db.work(
            title="Barnaby Rudge",
            with_license_pool=True,
            language="spa",
            collection=active_collection,
        )
        # And one more in the inactive collection.
        grim_furry_tails = db.work(
            title="Grim Furry Tails",
            with_license_pool=True,
            language="fre",
            collection=inactive_collection,
        )

        # A standard DatabaseBackedWorkList will find both books
        # from the library's active collections.
        wl = DatabaseBackedWorkList()
        wl.initialize(default_library)
        assert set(default_library.associated_collections) == {
            active_collection,
            inactive_collection,
        }
        assert default_library.active_collections == [active_collection]
        assert 2 == wl.works_from_database(db.session).count()

        # A work list with a language restriction will only find books
        # in that language.
        wl.initialize(default_library, languages=["eng"])
        assert [oliver_twist] == [x for x in wl.works_from_database(db.session)]

        # A DatabaseBackedWorkList will only find books licensed
        # through one of its collections.
        active_collection.associated_libraries = []
        collection = db.collection()
        collection.associated_libraries.append(default_library)
        assert set(default_library.associated_collections) == {
            collection,
            inactive_collection,
        }
        assert default_library.active_collections == [collection]
        wl.initialize(default_library)
        assert 0 == wl.works_from_database(db.session).count()

        # If a DatabaseBackedWorkList's library has only
        # inactive collections, it has no books.
        collection.associated_libraries = []
        assert default_library.associated_collections == [inactive_collection]
        assert default_library.active_collections == []
        wl.initialize(default_library)
        assert 0 == wl.works_from_database(db.session).count()

        # If a DatabaseBackedWorkList has no collections, it has no
        # books.
        inactive_collection.associated_libraries = []
        assert default_library.associated_collections == []
        wl.initialize(default_library)
        assert 0 == wl.works_from_database(db.session).count()

        # A DatabaseBackedWorkList can be set up with collections
        # rather than a library, even if the collection is inactive.
        # TODO: The syntax here could be improved.
        wl = DatabaseBackedWorkList()
        wl.initialize(
            None, collection_ids=[active_collection.id, inactive_collection.id]
        )
        assert None == wl.get_library(db.session)
        assert 3 == wl.works_from_database(db.session).count()

        # Reset our collection library associations and re-initialize
        # our work list to the default library.
        active_collection.associated_libraries = [default_library]
        inactive_collection.associated_libraries = [default_library]
        wl.initialize(default_library)

        assert set(default_library.associated_collections) == {
            active_collection,
            inactive_collection,
        }
        assert default_library.active_collections == [active_collection]
        assert 2 == wl.works_from_database(db.session).count()

        # Facets and pagination can affect which entries and how many
        # are returned.
        facets = DatabaseBackedFacets(
            default_library,
            availability=Facets.AVAILABLE_ALL,
            order=Facets.ORDER_TITLE,
            distributor=None,
            collection_name=None,
        )
        pagination = Pagination(offset=1, size=1)
        assert [oliver_twist] == wl.works_from_database(
            db.session, facets, pagination
        ).all()

        facets.order_ascending = False
        assert [barnaby_rudge] == wl.works_from_database(
            db.session, facets, pagination
        ).all()

        # Ensure that availability facets are handled properly
        # We still have two works:
        # - barnaby_rudge is closed access and available
        # - oliver_twist's access and availability is varied below
        ot_lp = oliver_twist.license_pools[0]

        # open access (thus available)
        ot_lp.type = LicensePoolType.UNLIMITED
        ot_lp.open_access = True

        facets.availability = Facets.AVAILABLE_ALL
        assert 2 == wl.works_from_database(db.session, facets).count()

        facets.availability = Facets.AVAILABLE_NOW
        assert 2 == wl.works_from_database(db.session, facets).count()

        facets.availability = Facets.AVAILABLE_OPEN_ACCESS
        assert 1 == wl.works_from_database(db.session, facets).count()
        assert [oliver_twist] == wl.works_from_database(db.session, facets).all()

        # open access & removed
        ot_lp.status = LicensePoolStatus.REMOVED

        facets.availability = Facets.AVAILABLE_ALL
        assert [barnaby_rudge] == wl.works_from_database(db.session, facets).all()

        facets.availability = Facets.AVAILABLE_NOW
        assert [barnaby_rudge] == wl.works_from_database(db.session, facets).all()

        facets.availability = Facets.AVAILABLE_OPEN_ACCESS
        assert 0 == wl.works_from_database(db.session, facets).count()

        # closed access & active but unavailable
        ot_lp.type = LicensePoolType.METERED
        ot_lp.open_access = False
        ot_lp.status = LicensePoolStatus.ACTIVE
        ot_lp.licenses_owned = 1
        ot_lp.licenses_available = 0

        facets.availability = Facets.AVAILABLE_ALL
        assert 2 == wl.works_from_database(db.session, facets).count()

        facets.availability = Facets.AVAILABLE_NOW
        assert 1 == wl.works_from_database(db.session, facets).count()
        assert [barnaby_rudge] == wl.works_from_database(db.session, facets).all()

        facets.availability = Facets.AVAILABLE_OPEN_ACCESS
        assert 0 == wl.works_from_database(db.session, facets).count()

    def test_works_from_database_filters_library_audiences(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        library = db.default_library()
        settings = library_fixture.settings(library)
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]

        adult = db.work(
            title="Adults Only",
            audience=Classifier.AUDIENCE_ADULT,
            with_license_pool=True,
        )
        young_adult = db.work(
            title="Teens",
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
            with_license_pool=True,
        )
        no_audience = db.work(title="Mystery", with_license_pool=True)
        no_audience.audience = None

        wl = DatabaseBackedWorkList()
        wl.initialize(library)

        results = set(wl.works_from_database(db.session).all())
        assert results == {young_adult, no_audience}

    def test_works_from_database_filters_library_genres(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        library = db.default_library()
        settings = library_fixture.settings(library)
        settings.filtered_genres = ["Romance"]

        romance_genre, _ = Genre.lookup(db.session, "Romance")
        horror_genre, _ = Genre.lookup(db.session, "Horror")

        romance = db.work(title="Hearts", with_license_pool=True)
        romance.genres = [romance_genre]
        horror = db.work(title="Screams", with_license_pool=True)
        horror.genres = [horror_genre]
        no_genre = db.work(title="Untitled", with_license_pool=True)

        wl = DatabaseBackedWorkList()
        wl.initialize(library)

        results = set(wl.works_from_database(db.session).all())
        assert results == {horror, no_genre}

    def test_works_from_database_filters_combined_audience_and_genre(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        """Test that both audience and genre filters are applied together."""
        library = db.default_library()
        settings = library_fixture.settings(library)
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]
        settings.filtered_genres = ["Romance"]

        romance_genre, _ = Genre.lookup(db.session, "Romance")
        horror_genre, _ = Genre.lookup(db.session, "Horror")

        # Filtered by audience
        adult_horror = db.work(
            title="Adult Horror",
            audience=Classifier.AUDIENCE_ADULT,
            with_license_pool=True,
        )
        adult_horror.genres = [horror_genre]

        # Filtered by genre
        ya_romance = db.work(
            title="YA Romance",
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
            with_license_pool=True,
        )
        ya_romance.genres = [romance_genre]

        # Filtered by both audience and genre
        adult_romance = db.work(
            title="Adult Romance",
            audience=Classifier.AUDIENCE_ADULT,
            with_license_pool=True,
        )
        adult_romance.genres = [romance_genre]

        # Not filtered - passes both checks
        ya_horror = db.work(
            title="YA Horror",
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
            with_license_pool=True,
        )
        ya_horror.genres = [horror_genre]

        # Not filtered - no audience or genre
        plain_work = db.work(title="Plain Work", with_license_pool=True)
        plain_work.audience = None

        wl = DatabaseBackedWorkList()
        wl.initialize(library)

        results = set(wl.works_from_database(db.session).all())
        assert results == {ya_horror, plain_work}

    def test_base_query(self, db: DatabaseTransactionFixture):
        # Verify that base_query makes the query we expect and then
        # calls some optimization methods (not tested).
        class Mock(DatabaseBackedWorkList):
            @classmethod
            def _modify_loading(cls, qu):
                return [qu, "_modify_loading"]

        result = Mock.base_query(db.session)

        [base_query, m] = result
        expect = (
            db.session.query(Work)
            .join(Work.license_pools)
            .join(Work.presentation_edition)
        )
        assert str(expect) == str(base_query)
        assert "_modify_loading" == m

    def test_bibliographic_filter_clauses(self, db: DatabaseTransactionFixture):
        called = dict()

        class MockWorkList(DatabaseBackedWorkList):
            """Verifies that bibliographic_filter_clauses() calls various hook
            methods.

            The hook methods themselves are tested separately.
            """

            def __init__(self, parent):
                super().__init__()
                self._parent = parent
                self._inherit_parent_restrictions = False

            def audience_filter_clauses(self, _db, qu):
                called["audience_filter_clauses"] = (_db, qu)
                return []

            def customlist_filter_clauses(self, qu):
                called["customlist_filter_clauses"] = qu
                return qu, []

            def age_range_filter_clauses(self):
                called["age_range_filter_clauses"] = True
                return []

            def genre_filter_clause(self, qu):
                called["genre_filter_clause"] = qu
                return qu, None

            @property
            def parent(self):
                return self._parent

            @property
            def inherit_parent_restrictions(self):
                return self._inherit_parent_restrictions

        class MockParent:
            bibliographic_filter_clauses_called_with = None

            def bibliographic_filter_clauses(self, _db, qu):
                self.bibliographic_filter_clauses_called_with = (_db, qu)
                return qu, []

        parent = MockParent()

        # Create a MockWorkList with a parent.
        wl = MockWorkList(parent)
        wl.initialize(db.default_library())
        original_qu = DatabaseBackedWorkList.base_query(db.session)

        # If no languages or genre IDs are specified, and the hook
        # methods do nothing, then bibliographic_filter_clauses() has
        # no effect.
        final_qu, clauses = wl.bibliographic_filter_clauses(db.session, original_qu)
        assert original_qu == final_qu
        assert [] == clauses

        # But at least the apply_audience_filter was called with the correct
        # arguments.
        _db, qu = called["audience_filter_clauses"]
        assert db.session == _db
        assert original_qu == qu

        # age_range_filter_clauses was also called.
        assert True == called["age_range_filter_clauses"]

        # customlist_filter_clauses and genre_filter_clause were not
        # called because the WorkList doesn't do anything relating to
        # custom lists.
        assert "customlist_filter_clauses" not in called
        assert "genre_filter_clause" not in called

        # The parent's bibliographic_filter_clauses() implementation
        # was not called, because wl.inherit_parent_restrictions is
        # set to False.
        assert None == parent.bibliographic_filter_clauses_called_with

        # Set things up so that those other methods will be called.
        empty_list, ignore = db.customlist(num_entries=0)
        sf, ignore = Genre.lookup(db.session, "Science Fiction")
        wl.initialize(db.default_library(), customlists=[empty_list], genres=[sf])
        wl._inherit_parent_restrictions = True

        final_qu, clauses = wl.bibliographic_filter_clauses(db.session, original_qu)

        assert (
            (db.session),
            original_qu,
        ) == parent.bibliographic_filter_clauses_called_with
        assert original_qu == called["genre_filter_clause"]
        assert original_qu == called["customlist_filter_clauses"]

        # But none of those methods changed anything, because their
        # implementations didn't return anything.
        assert [] == clauses

        # Now test the clauses that are created directly by
        # bibliographic_filter_clauses.
        overdrive = DataSource.lookup(db.session, DataSource.OVERDRIVE)
        wl.initialize(
            db.default_library(),
            languages=["eng"],
            media=[Edition.BOOK_MEDIUM],
            fiction=True,
            license_datasource=overdrive,
        )

        final_qu, clauses = wl.bibliographic_filter_clauses(db.session, original_qu)
        assert original_qu == final_qu
        language, medium, fiction, datasource = clauses

        # NOTE: str() doesn't prove that the values are the same, only
        # that the constraints are similar.
        assert str(language) == str(Edition.language.in_(wl.languages))
        assert str(medium) == str(Edition.medium.in_(wl.media))
        assert str(fiction) == str(Work.fiction == True)
        assert str(datasource) == str(LicensePool.data_source_id == overdrive.id)

    def test_bibliographic_filter_clauses_end_to_end(
        self, db: DatabaseTransactionFixture
    ):
        # Verify that bibliographic_filter_clauses generates
        # SQLAlchemy clauses that give the expected results when
        # applied to a real `works` table.
        original_qu = DatabaseBackedWorkList.base_query(db.session)

        # Create a work that may or may not show up in various
        # DatabaseBackedWorkLists.
        sf, ignore = Genre.lookup(db.session, "Science Fiction")
        english_sf = db.work(
            title="English SF",
            language="eng",
            with_license_pool=True,
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
        )
        italian_sf = db.work(
            title="Italian SF",
            language="ita",
            with_license_pool=True,
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
        )
        english_sf.target_age = tuple_to_numericrange((12, 14))
        gutenberg = english_sf.license_pools[0].data_source
        english_sf.presentation_edition.medium = Edition.BOOK_MEDIUM
        english_sf.genres.append(sf)
        italian_sf.genres.append(sf)

        def worklist_has_books(expect_books, worklist=None, **initialize_kwargs):
            """Apply bibliographic filters to a query and verify
            that it finds only the given books.
            """
            if worklist is None:
                worklist = DatabaseBackedWorkList()
                worklist.initialize(db.default_library(), **initialize_kwargs)
            qu, clauses = worklist.bibliographic_filter_clauses(db.session, original_qu)
            if clauses:
                qu = qu.filter(and_(*clauses))
            expect_titles = sorted(x.sort_title for x in expect_books)
            actual_titles = sorted(x.sort_title for x in qu)
            assert expect_titles == actual_titles

        # A WorkList will find a book only if all restrictions
        # are met.
        worklist_has_books(
            [english_sf],
            languages=["eng"],
            genres=[sf],
            media=[Edition.BOOK_MEDIUM],
            fiction=True,
            license_datasource=gutenberg,
            audiences=[Classifier.AUDIENCE_YOUNG_ADULT],
            target_age=tuple_to_numericrange((13, 13)),
        )

        # This might be because there _are_ no restrictions.
        worklist_has_books([english_sf, italian_sf], fiction=None)

        # DatabaseBackedWorkLists with a contradictory setting for one
        # of the fields associated with the English SF book will not
        # find it.
        worklist_has_books([italian_sf], languages=["ita"], genres=[sf])
        romance, ignore = Genre.lookup(db.session, "Romance")
        worklist_has_books([], languages=["eng"], genres=[romance])
        worklist_has_books(
            [], languages=["eng"], genres=[sf], media=[Edition.AUDIO_MEDIUM]
        )
        worklist_has_books([], fiction=False)
        worklist_has_books(
            [], license_datasource=DataSource.lookup(db.session, DataSource.OVERDRIVE)
        )

        # If the WorkList has custom list IDs, then works will only show up if
        # they're on one of the matching CustomLists.
        sf_list, ignore = db.customlist(num_entries=0)
        sf_list.add_entry(english_sf)
        sf_list.add_entry(italian_sf)

        worklist_has_books([english_sf, italian_sf], customlists=[sf_list])

        empty_list, ignore = db.customlist(num_entries=0)
        worklist_has_books([], customlists=[empty_list])

        # Test parent restrictions.
        #
        # Ordinary DatabaseBackedWorkLists can't inherit restrictions
        # from their parent (TODO: no reason not to implement this)
        # but Lanes can, so let's use Lanes for the rest of this test.

        # This lane has books from a list of English books.
        english_list, ignore = db.customlist(num_entries=0)
        english_list.add_entry(english_sf)
        english_lane = db.lane()
        english_lane.customlists.append(english_list)

        # This child of that lane has books from the list of SF books.
        sf_lane = db.lane(parent=english_lane, inherit_parent_restrictions=False)
        sf_lane.customlists.append(sf_list)

        # When the child lane does not inherit its parent restrictions,
        # both SF books show up.
        worklist_has_books([english_sf, italian_sf], sf_lane)

        # When the child inherits its parent's restrictions, only the
        # works that are on _both_ lists show up in the lane,
        sf_lane.inherit_parent_restrictions = True
        worklist_has_books([english_sf], sf_lane)

        # Other restrictions are inherited as well. Here, a title must
        # show up on both lists _and_ be a nonfiction book. There are
        # no titles that meet all three criteria.
        sf_lane.fiction = False
        worklist_has_books([], sf_lane)

        sf_lane.fiction = True
        worklist_has_books([english_sf], sf_lane)

        # Parent restrictions based on genre can also be inherited.
        #

        # Here's a lane that finds only short stories.
        short_stories, ignore = Genre.lookup(db.session, "Short Stories")
        short_stories_lane = db.lane(genres=["Short Stories"])

        # Here's a child of that lane, which contains science fiction.
        sf_shorts = db.lane(
            genres=[sf], parent=short_stories_lane, inherit_parent_restrictions=False
        )
        db.session.flush()

        # Without the parent restriction in place, all science fiction
        # shows up in sf_shorts.
        worklist_has_books([english_sf, italian_sf], sf_shorts)

        # With the parent restriction in place, a book must be classified
        # under both science fiction and short stories to show up.
        sf_shorts.inherit_parent_restrictions = True
        worklist_has_books([], sf_shorts)
        english_sf.genres.append(short_stories)
        worklist_has_books([english_sf], sf_shorts)

    def test_age_range_filter_clauses_end_to_end(self, db: DatabaseTransactionFixture):
        # Standalone test of age_range_filter_clauses().
        def worklist_has_books(expect, **wl_args):
            """Make a DatabaseBackedWorkList and find all the works
            that match its age_range_filter_clauses.
            """
            wl = DatabaseBackedWorkList()
            wl.initialize(db.default_library(), **wl_args)
            qu = db.session.query(Work)
            clauses = wl.age_range_filter_clauses()
            qu = qu.filter(and_(*clauses))
            assert set(expect) == set(qu.all())

        adult = db.work(
            title="For adults",
            audience=Classifier.AUDIENCE_ADULT,
            with_license_pool=True,
        )
        assert None == adult.target_age
        fourteen_or_fifteen = db.work(
            title="For teens",
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
            with_license_pool=True,
        )
        fourteen_or_fifteen.target_age = tuple_to_numericrange((14, 15))

        # This DatabaseBackedWorkList contains the YA book because its
        # age range overlaps the age range of the book.
        worklist_has_books([fourteen_or_fifteen], target_age=(12, 14))

        worklist_has_books(
            [adult, fourteen_or_fifteen],
            audiences=[Classifier.AUDIENCE_ADULT],
            target_age=(12, 14),
        )

        # This lane contains no books because it skews too old for the YA
        # book, but books for adults are not allowed.
        older_ya = db.lane()
        older_ya.target_age = (16, 17)
        worklist_has_books([], target_age=(16, 17))

        # Expand it to include books for adults, and the adult book
        # shows up despite having no target age at all.
        worklist_has_books([adult], target_age=(16, 18))

    def test_audience_filter_clauses(self, db: DatabaseTransactionFixture):
        # Verify that audience_filter_clauses restricts a query to
        # reflect a DatabaseBackedWorkList's audience filter.

        # Create a children's book and a book for adults.
        adult = db.work(
            title="Diseases of the Horse",
            with_license_pool=True,
            with_open_access_download=True,
            audience=Classifier.AUDIENCE_ADULT,
        )

        children = db.work(
            title="Wholesome Nursery Rhymes For All Children",
            with_license_pool=True,
            with_open_access_download=True,
            audience=Classifier.AUDIENCE_CHILDREN,
        )

        def for_audiences(*audiences):
            """Invoke audience_filter_clauses using the given
            `audiences`, and return all the matching Work objects.
            """
            wl = DatabaseBackedWorkList()
            wl.audiences = audiences
            qu = wl.base_query(db.session)
            clauses = wl.audience_filter_clauses(db.session, qu)
            if clauses:
                qu = qu.filter(and_(*clauses))
            return qu.all()

        assert [adult] == for_audiences(Classifier.AUDIENCE_ADULT)
        assert [children] == for_audiences(Classifier.AUDIENCE_CHILDREN)

        # If no particular audiences are specified, no books are filtered.
        assert {adult, children} == set(for_audiences())

    def test_customlist_filter_clauses(self, db: DatabaseTransactionFixture):
        # Standalone test of customlist_filter_clauses

        # If a lane has nothing to do with CustomLists,
        # apply_customlist_filter does nothing.
        no_lists = DatabaseBackedWorkList()
        no_lists.initialize(db.default_library())
        qu = no_lists.base_query(db.session)
        new_qu, clauses = no_lists.customlist_filter_clauses(qu)
        assert qu == new_qu
        assert [] == clauses

        # Now set up a Work and a CustomList that contains the work.
        work = db.work(with_license_pool=True)
        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)
        assert gutenberg == work.license_pools[0].data_source
        gutenberg_list, ignore = db.customlist(num_entries=0)
        gutenberg_list.data_source = gutenberg
        gutenberg_list_entry, ignore = gutenberg_list.add_entry(work)

        # This DatabaseBackedWorkList gets every work on a specific list.
        works_on_list = DatabaseBackedWorkList()
        works_on_list.initialize(db.default_library(), customlists=[gutenberg_list])

        # This lane gets every work on every list associated with Project
        # Gutenberg.
        works_on_gutenberg_lists = DatabaseBackedWorkList()
        works_on_gutenberg_lists.initialize(
            db.default_library(), list_datasource=gutenberg
        )

        def _run(qu, clauses):
            # Run a query with certain clauses
            return qu.filter(and_(*clauses)).all()

        def results(wl=works_on_gutenberg_lists, must_be_featured=False):
            qu = wl.base_query(db.session)
            new_qu, clauses = wl.customlist_filter_clauses(qu)

            # The query comes out different than it goes in -- there's a
            # new join against CustomListEntry.
            assert new_qu != qu
            return _run(new_qu, clauses)

        # Both lanes contain the work.
        assert [work] == results(works_on_list)
        assert [work] == results(works_on_gutenberg_lists)

        # If there's another list with the same work on it, the
        # work only shows up once.
        gutenberg_list_2, ignore = db.customlist(num_entries=0)
        gutenberg_list_2_entry, ignore = gutenberg_list_2.add_entry(work)
        works_on_list._customlist_ids.append(gutenberg_list.id)
        assert [work] == results(works_on_list)

        # This WorkList gets every work on a list associated with Overdrive.
        # There are no such lists, so the lane is empty.
        overdrive = DataSource.lookup(db.session, DataSource.OVERDRIVE)
        works_on_overdrive_lists = DatabaseBackedWorkList()
        works_on_overdrive_lists.initialize(
            db.default_library(), list_datasource=overdrive
        )
        assert [] == results(works_on_overdrive_lists)

        # It's possible to restrict a WorkList to works that were seen on
        # a certain list recently.
        now = utc_now()
        two_days_ago = now - datetime.timedelta(days=2)
        gutenberg_list_entry.most_recent_appearance = two_days_ago

        # The lane will only show works that were seen within the last
        # day. There are no such works.
        works_on_gutenberg_lists.list_seen_in_previous_days = 1
        assert [] == results()

        # Now it's been loosened to three days, and the work shows up.
        works_on_gutenberg_lists.list_seen_in_previous_days = 3
        assert [work] == results()

        # Now let's test what happens when we chain calls to this
        # method.
        gutenberg_list_2_wl = DatabaseBackedWorkList()
        gutenberg_list_2_wl.initialize(
            db.default_library(), customlists=[gutenberg_list_2]
        )

        # These two lines won't work, because these are
        # DatabaseBackedWorkLists, not Lanes, but they show the
        # scenario in which this would actually happen. When
        # determining which works belong in the child lane,
        # Lane.customlist_filter_clauses() will be called on the
        # parent lane and then on the child. In this case, only want
        # books that are on _both_ works_on_list and gutenberg_list_2.
        #
        # TODO: There's no reason WorkLists shouldn't be able to have
        # parents and inherit parent restrictions.
        #
        # gutenberg_list_2_wl.parent = works_on_list
        # gutenberg_list_2_wl.inherit_parent_restrictions = True

        qu = works_on_list.base_query(db.session)
        list_1_qu, list_1_clauses = works_on_list.customlist_filter_clauses(qu)

        # The query has been modified -- we've added a join against
        # CustomListEntry.
        assert list_1_qu != qu
        assert [work] == list_1_qu.all()

        # Now call customlist_filter_clauses again so that the query
        # must only match books on _both_ lists. This simulates
        # what happens when the second lane is a child of the first,
        # and inherits its restrictions.
        both_lists_qu, list_2_clauses = gutenberg_list_2_wl.customlist_filter_clauses(
            list_1_qu,
        )
        # The query has been modified again -- we've added a second join
        # against CustomListEntry.
        assert both_lists_qu != list_1_qu
        both_lists_clauses = list_1_clauses + list_2_clauses

        # The combined query matches the work that shows up on
        # both lists.
        assert [work] == _run(both_lists_qu, both_lists_clauses)

        # If we remove `work` from either list, the combined query
        # matches nothing.
        for l in [gutenberg_list, gutenberg_list_2]:
            l.remove_entry(work)
            assert [] == _run(both_lists_qu, both_lists_clauses)
            l.add_entry(work)
