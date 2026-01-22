import pytest

from palace.manager.core.classifier import Classifier
from palace.manager.core.entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EverythingEntryPoint,
)
from palace.manager.feed.facets.feed import FeaturedFacets
from palace.manager.feed.facets.search import SearchFacets
from palace.manager.feed.worklist.base import WorkList
from palace.manager.feed.worklist.top_level import TopLevelWorkList
from palace.manager.search.pagination import Pagination
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.lane import (
    Lane,
)
from palace.manager.sqlalchemy.util import tuple_to_numericrange
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.search import EndToEndSearchFixture


class TestLane:
    def test_get_library(self, db: DatabaseTransactionFixture):
        lane = db.lane()
        assert db.default_library() == lane.get_library(db.session)

    def test_list_datasource(self, db: DatabaseTransactionFixture):
        """Test setting and retrieving the DataSource object and
        the underlying ID.
        """
        lane = db.lane()

        # This lane is based on a specific CustomList.
        customlist1, ignore = db.customlist(num_entries=0)
        customlist2, ignore = db.customlist(num_entries=0)
        lane.customlists.append(customlist1)
        assert None == lane.list_datasource
        assert None == lane.list_datasource_id
        assert [customlist1.id] == lane.customlist_ids

        # Now change it so it's based on all CustomLists from a given
        # DataSource.
        source = customlist1.data_source
        lane.list_datasource = source
        assert source == lane.list_datasource
        assert source.id == lane.list_datasource_id

        # The lane is now based on two CustomLists instead of one.
        assert {customlist1.id, customlist2.id} == set(lane.customlist_ids)

    def test_set_audiences(self, db: DatabaseTransactionFixture):
        """Setting Lane.audiences to a single value will
        auto-convert it into a list containing one value.
        """
        lane = db.lane()
        lane.audiences = Classifier.AUDIENCE_ADULT
        assert [Classifier.AUDIENCE_ADULT] == lane.audiences

    def test_update_size(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        class Mock:
            # Mock the ExternalSearchIndex.count_works() method to
            # return specific values without consulting an actual
            # search index.
            def count_works(self, filter):
                values_by_medium = {
                    None: 102,
                    Edition.AUDIO_MEDIUM: 3,
                    Edition.BOOK_MEDIUM: 99,
                }
                if filter.media:
                    [medium] = filter.media
                else:
                    medium = None
                return values_by_medium[medium]

        search_engine = Mock()

        # Make a lane with some incorrect values that will be fixed by
        # update_size().
        fiction = db.lane(display_name="Fiction", fiction=True)
        fiction.size = 44
        fiction.size_by_entrypoint = {"Nonexistent entrypoint": 33}
        fiction.update_size(db.session, search_engine=search_engine)

        # The lane size is also calculated individually for every
        # enabled entry point. EverythingEntryPoint is used for the
        # total size of the lane.
        assert {
            AudiobooksEntryPoint.URI: 3,
            EbooksEntryPoint.URI: 99,
            EverythingEntryPoint.URI: 102,
        } == fiction.size_by_entrypoint
        assert 102 == fiction.size

    def test_visibility(self, db: DatabaseTransactionFixture):
        parent = db.lane()
        visible_child = db.lane(parent=parent)
        invisible_child = db.lane(parent=parent)
        invisible_child.visible = False
        assert [visible_child] == list(parent.visible_children)

        grandchild = db.lane(parent=invisible_child)
        assert True == parent.visible
        assert True == visible_child.visible
        assert False == invisible_child.visible

        # The grandchild lane is set to visible in the database, but
        # it is not visible because its parent is not visible.
        assert True == grandchild._visible
        assert False == grandchild.visible

    def test_parentage(self, db: DatabaseTransactionFixture):
        worklist = WorkList()
        worklist.display_name = "A WorkList"
        lane = db.lane()
        child_lane = db.lane(parent=lane)
        grandchild_lane = db.lane(parent=child_lane)
        unrelated = db.lane()

        # A WorkList has no parentage.
        assert [] == list(worklist.parentage)
        assert "A WorkList" == worklist.full_identifier

        # The WorkList has the Lane as a child, but the Lane doesn't know
        # this.
        assert [] == list(lane.parentage)
        assert [lane] == list(child_lane.parentage)
        assert (
            f"{lane.library.short_name} / {lane.display_name}" == lane.full_identifier
        )

        assert (
            "%s / %s / %s / %s"
            % (
                lane.library.short_name,
                lane.display_name,
                child_lane.display_name,
                grandchild_lane.display_name,
            )
            == grandchild_lane.full_identifier
        )

        assert [lane, child_lane, grandchild_lane] == grandchild_lane.hierarchy

        # TODO: The error should be raised when we try to set the parent
        # to an illegal value, not afterwards.
        lane.parent = child_lane
        with pytest.raises(ValueError) as excinfo:
            list(lane.parentage)
        assert "Lane parentage loop detected" in str(excinfo.value)

    def test_is_self_or_descendant(self, db: DatabaseTransactionFixture):
        # Test the code that checks whether one Lane is 'beneath'
        # a WorkList.

        top_level = TopLevelWorkList()
        top_level.initialize(db.default_library())
        parent = db.lane()
        child = db.lane(parent=parent)

        # Generally this works the same as WorkList.is_self_or_descendant.
        assert True == parent.is_self_or_descendant(parent)
        assert True == child.is_self_or_descendant(child)

        assert True == child.is_self_or_descendant(parent)
        assert False == parent.is_self_or_descendant(child)

        # The big exception: a TopLevelWorkList is a descendant of any
        # Lane so long as they belong to the same library.
        assert True == child.is_self_or_descendant(top_level)
        assert True == parent.is_self_or_descendant(top_level)

        library2 = db.library()
        top_level.initialize(library2)
        assert False == child.is_self_or_descendant(top_level)
        assert False == parent.is_self_or_descendant(top_level)

    def test_depth(self, db: DatabaseTransactionFixture):
        child = db.lane("sublane")
        parent = db.lane("parent")
        parent.sublanes.append(child)
        assert 0 == parent.depth
        assert 1 == child.depth

    def test_url_name(self, db: DatabaseTransactionFixture):
        lane = db.lane("Fantasy / Science Fiction")
        assert lane.id == lane.url_name

    def test_display_name_for_all(self, db: DatabaseTransactionFixture):
        lane = db.lane("Fantasy / Science Fiction")
        assert "All Fantasy / Science Fiction" == lane.display_name_for_all

    def test_entrypoints(self, db: DatabaseTransactionFixture):
        """Currently a Lane can never have entrypoints."""
        assert [] == db.lane().entrypoints

    def test_affected_by_customlist(self, db: DatabaseTransactionFixture):
        # Two lists.
        l1, ignore = db.customlist(data_source_name=DataSource.GUTENBERG, num_entries=0)
        l2, ignore = db.customlist(data_source_name=DataSource.OVERDRIVE, num_entries=0)

        # A lane populated by specific lists.
        lane = db.lane()

        # Not affected by any lists.
        for l in [l1, l2]:
            assert 0 == Lane.affected_by_customlist(l1).count()

        # Add a lane to the list, and it becomes affected.
        lane.customlists.append(l1)
        assert [lane] == lane.affected_by_customlist(l1).all()
        assert 0 == lane.affected_by_customlist(l2).count()
        lane.customlists = []

        # A lane based on all lists with the GUTENBERG db source.
        lane2 = db.lane()
        lane2.list_datasource = l1.data_source

        # It's affected by the GUTENBERG list but not the OVERDRIVE
        # list.
        assert [lane2] == Lane.affected_by_customlist(l1).all()
        assert 0 == Lane.affected_by_customlist(l2).count()

    def test_inherited_value(self, db: DatabaseTransactionFixture):
        # Test WorkList.inherited_value.
        #
        # It's easier to test this in Lane because WorkLists can't have
        # parents.

        # This lane contains fiction.
        fiction_lane = db.lane(fiction=True)

        # This sublane contains nonfiction.
        nonfiction_sublane = db.lane(parent=fiction_lane, fiction=False)
        nonfiction_sublane.inherit_parent_restrictions = False

        # This sublane doesn't specify a value for .fiction.
        default_sublane = db.lane(parent=fiction_lane)
        default_sublane.inherit_parent_restrictions = False

        # When inherit_parent_restrictions is False,
        # inherited_value("fiction") returns whatever value is set for
        # .fiction.
        assert None == default_sublane.inherited_value("fiction")
        assert False == nonfiction_sublane.inherited_value("fiction")

        # When inherit_parent_restrictions is True,
        # inherited_value("fiction") returns False for the sublane
        # that sets no value for .fiction.
        default_sublane.inherit_parent_restrictions = True
        assert True == default_sublane.inherited_value("fiction")

        # The sublane that sets its own value for .fiction is unaffected.
        nonfiction_sublane.inherit_parent_restrictions = True
        assert False == nonfiction_sublane.inherited_value("fiction")

    def test_inherited_values(self, db: DatabaseTransactionFixture):
        # Test WorkList.inherited_values.
        #
        # It's easier to test this in Lane because WorkLists can't have
        # parents.

        # This lane contains best-sellers.
        best_sellers_lane = db.lane()
        best_sellers, ignore = db.customlist(num_entries=0)
        best_sellers_lane.customlists.append(best_sellers)

        # This sublane contains staff picks.
        staff_picks_lane = db.lane(parent=best_sellers_lane)
        staff_picks, ignore = db.customlist(num_entries=0)
        staff_picks_lane.customlists.append(staff_picks)

        # What does it mean that the 'staff picks' lane is *inside*
        # the 'best sellers' lane?

        # If inherit_parent_restrictions is False, it doesn't mean
        # anything in particular. This lane contains books that
        # are on the staff picks list.
        staff_picks_lane.inherit_parent_restrictions = False
        assert [[staff_picks]] == staff_picks_lane.inherited_values("customlists")

        # If inherit_parent_restrictions is True, then the lane
        # has *two* sets of restrictions: a book must be on both
        # the staff picks list *and* the best sellers list.
        staff_picks_lane.inherit_parent_restrictions = True
        x = staff_picks_lane.inherited_values("customlists")
        assert sorted([[staff_picks], [best_sellers]]) == sorted(
            staff_picks_lane.inherited_values("customlists")
        )

    def test_setting_target_age_locks_audiences(self, db: DatabaseTransactionFixture):
        lane = db.lane()
        lane.target_age = (16, 18)
        assert sorted(
            [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_ADULT]
        ) == sorted(lane.audiences)
        lane.target_age = (0, 2)
        assert [Classifier.AUDIENCE_CHILDREN] == lane.audiences
        lane.target_age = 14
        assert [Classifier.AUDIENCE_YOUNG_ADULT] == lane.audiences

        # It's not possible to modify .audiences to a value that's
        # incompatible with .target_age.
        lane.audiences = lane.audiences

        def doomed():
            lane.audiences = [Classifier.AUDIENCE_CHILDREN]

        with pytest.raises(ValueError) as excinfo:
            doomed()
        assert "Cannot modify Lane.audiences when Lane.target_age is set" in str(
            excinfo.value
        )

        # Setting target_age to None leaves preexisting .audiences in place.
        lane.target_age = None
        assert [Classifier.AUDIENCE_YOUNG_ADULT] == lane.audiences

        # But now you can modify .audiences.
        lane.audiences = [Classifier.AUDIENCE_CHILDREN]

    def test_target_age_treats_all_adults_equally(self, db: DatabaseTransactionFixture):
        """We don't distinguish between different age groups for adults."""
        lane = db.lane()
        lane.target_age = (35, 40)
        assert tuple_to_numericrange((18, 18)) == lane.target_age

    def test_uses_customlists(self, db: DatabaseTransactionFixture):
        lane = db.lane()
        assert False == lane.uses_customlists

        customlist, ignore = db.customlist(num_entries=0)
        lane.customlists = [customlist]
        assert True == lane.uses_customlists

        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)
        lane.list_datasource = gutenberg
        db.session.commit()
        assert True == lane.uses_customlists

        # Note that the specific custom list was removed from this
        # Lane when it switched to using all lists from a certain db
        # source.
        assert [] == lane.customlists

        # A Lane may use custom lists by virtue of inheriting
        # restrictions from its parent.
        child = db.lane(parent=lane)
        child.inherit_parent_restrictions = True
        assert True == child.uses_customlists

    def test_genre_ids(self, db: DatabaseTransactionFixture):
        # By default, when you add a genre to a lane, you are saying
        # that Works classified under it and all its subgenres should
        # show up in the lane.
        fantasy = db.lane()
        fantasy.add_genre("Fantasy")

        # At this point the lane picks up Fantasy and all of its
        # subgenres.
        expect = [
            Genre.lookup(db.session, genre)[0].id
            for genre in [
                "Fantasy",
                "Epic Fantasy",
                "Historical Fantasy",
                "Urban Fantasy",
            ]
        ]
        assert set(expect) == fantasy.genre_ids

        # Let's exclude one of the subgenres.
        fantasy.add_genre("Urban Fantasy", inclusive=False)
        urban_fantasy, ignore = Genre.lookup(db.session, "Urban Fantasy")
        # That genre's ID has disappeared from .genre_ids.
        assert urban_fantasy.id not in fantasy.genre_ids

        # Let's add Science Fiction, but not its subgenres.
        fantasy.add_genre("Science Fiction", recursive=False)
        science_fiction, ignore = Genre.lookup(db.session, "Science Fiction")
        space_opera, ignore = Genre.lookup(db.session, "Space Opera")
        assert science_fiction.id in fantasy.genre_ids
        assert space_opera.id not in fantasy.genre_ids

        # Let's add Space Opera, but exclude Science Fiction and its
        # subgenres.
        fantasy.lane_genres = []
        fantasy.add_genre("Space Opera")
        fantasy.add_genre("Science Fiction", inclusive=False, recursive=True)

        # That eliminates everything.
        assert set() == fantasy.genre_ids

        # NOTE: We don't have any doubly nested subgenres, so we can't
        # test the case where a genre is included recursively but one
        # of its subgenres is exclused recursively (in which case the
        # sub-subgenre would be excluded), but it should work.

        # We can exclude a genre even when no genres are explicitly included.
        # The lane will include all genres that aren't excluded.
        no_inclusive_genres = db.lane()
        no_inclusive_genres.add_genre("Science Fiction", inclusive=False)
        assert len(no_inclusive_genres.genre_ids) > 10
        assert science_fiction.id not in no_inclusive_genres.genre_ids

    def test_customlist_ids(self, db: DatabaseTransactionFixture):
        # WorkLists always return None for customlist_ids.
        wl = WorkList()
        wl.initialize(db.default_library())
        assert None == wl.customlist_ids

        # When you add a CustomList to a Lane, you are saying that works
        # from that CustomList can appear in the Lane.
        nyt1, ignore = db.customlist(num_entries=0, data_source_name=DataSource.NYT)
        nyt2, ignore = db.customlist(num_entries=0, data_source_name=DataSource.NYT)

        no_lists = db.lane()
        assert None == no_lists.customlist_ids

        has_list = db.lane()
        has_list.customlists.append(nyt1)
        assert [nyt1.id] == has_list.customlist_ids

        # When you set a Lane's list_datasource, you're saying that
        # works appear in the Lane if they are on _any_ CustomList from
        # that db source.
        has_list_source = db.lane()
        has_list_source.list_datasource = DataSource.lookup(db.session, DataSource.NYT)
        assert {nyt1.id, nyt2.id} == set(has_list_source.customlist_ids)

        # If there are no CustomLists from that db source, an empty
        # list is returned.
        has_no_lists = db.lane()
        has_no_lists.list_datasource = DataSource.lookup(
            db.session, DataSource.OVERDRIVE
        )
        assert [] == has_no_lists.customlist_ids

    def test_search_target(self, db: DatabaseTransactionFixture):
        # A Lane that is the root for a patron type can be
        # searched.
        root_lane = db.lane()
        root_lane.root_for_patron_type = ["A"]
        assert root_lane == root_lane.search_target

        # A Lane that's the descendant of a root Lane for a
        # patron type will search that root Lane.
        child = db.lane(parent=root_lane)
        assert root_lane == child.search_target

        grandchild = db.lane(parent=child)
        assert root_lane == grandchild.search_target

        # Any Lane that does not descend from a root Lane will
        # get a WorkList as its search target, with some
        # restrictions from the Lane.
        lane = db.lane()

        lane.languages = ["eng", "ger"]
        target = lane.search_target
        assert "English/Deutsch" == target.display_name
        assert ["eng", "ger"] == target.languages
        assert None == target.audiences
        assert None == target.media

        # If there are too many languages, they're left out of the
        # display name (so the search description will be "Search").
        lane.languages = ["eng", "ger", "spa", "fre"]
        target = lane.search_target
        assert "" == target.display_name
        assert ["eng", "ger", "spa", "fre"] == target.languages
        assert None == target.audiences
        assert None == target.media

        lane.languages = ["eng"]
        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert None == target.audiences
        assert None == target.media

        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert None == target.audiences
        assert None == target.media

        # Media aren't included in the description, but they
        # are used in search.
        lane.media = [Edition.BOOK_MEDIUM]
        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert None == target.audiences
        assert [Edition.BOOK_MEDIUM] == target.media

        # Audiences are only used in search if one of the
        # audiences is young adult or children.
        lane.audiences = [Classifier.AUDIENCE_ADULTS_ONLY]
        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert None == target.audiences
        assert [Edition.BOOK_MEDIUM] == target.media

        lane.audiences = [Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_YOUNG_ADULT]
        target = lane.search_target
        assert "English Adult and Young Adult" == target.display_name
        assert ["eng"] == target.languages
        assert [
            Classifier.AUDIENCE_ADULT,
            Classifier.AUDIENCE_YOUNG_ADULT,
        ] == target.audiences
        assert [Edition.BOOK_MEDIUM] == target.media

        # If there are too many audiences, they're left
        # out of the display name.
        lane.audiences = [
            Classifier.AUDIENCE_ADULT,
            Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_CHILDREN,
        ]
        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert [
            Classifier.AUDIENCE_ADULT,
            Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_CHILDREN,
        ] == target.audiences
        assert [Edition.BOOK_MEDIUM] == target.media

    def test_search(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # Searching a Lane calls search() on its search_target.
        #
        # TODO: This test could be trimmed down quite a bit with
        # mocks.

        work = db.work(with_license_pool=True)

        lane = db.lane()
        search_client = end_to_end_search_fixture.external_search_index
        end_to_end_search_fixture.populate_search_index()

        pagination = Pagination(offset=0, size=1)

        results = lane.search(
            db.session, work.title, search_client, pagination=pagination
        )
        target_results = lane.search_target.search(
            db.session, work.title, search_client, pagination=pagination
        )
        assert results == target_results

        # The single search result was returned as a Work.
        [result] = results
        assert work == result

        # This still works if the lane is its own search_target.
        lane.root_for_patron_type = ["A"]
        results = lane.search(
            db.session, work.title, search_client, pagination=pagination
        )
        target_results = lane.search_target.search(
            db.session, work.title, search_client, pagination=pagination
        )
        assert results == target_results

    def test_search_propagates_facets(self, db: DatabaseTransactionFixture):
        """Lane.search propagates facets when calling search() on
        its search target.
        """

        class Mock:
            def search(self, *args, **kwargs):
                self.called_with = kwargs["facets"]

        mock = Mock()
        lane = db.lane()

        old_lane_search_target = Lane.search_target
        old_wl_search = WorkList.search
        Lane.search_target = mock  # type: ignore[assignment]
        facets = SearchFacets()
        lane.search(db.session, "query", None, facets=facets)
        assert facets == mock.called_with

        # Now try the case where a lane is its own search target.  The
        # Facets object is propagated to the WorkList.search().
        mock.called_with = None
        Lane.search_target = lane
        WorkList.search = mock.search
        lane.search(db.session, "query", None, facets=facets)
        assert facets == mock.called_with

        # Restore methods that were mocked.
        Lane.search_target = old_lane_search_target
        WorkList.search = old_wl_search

    def test_explain(self, db: DatabaseTransactionFixture):
        parent = db.lane(display_name="Parent")
        parent.priority = 1
        child = db.lane(parent=parent, display_name="Child")
        child.priority = 2
        data = parent.explain()
        assert [
            "ID: %s" % parent.id,
            "Library: %s" % db.default_library().short_name,
            "Priority: 1",
            "Display name: Parent",
        ] == data

        data = child.explain()
        assert [
            "ID: %s" % child.id,
            "Library: %s" % db.default_library().short_name,
            "Parent ID: %s (Parent)" % parent.id,
            "Priority: 2",
            "Display name: Child",
        ] == data

    def test_groups_propagates_facets(self, db: DatabaseTransactionFixture):
        # Lane.groups propagates a received Facets object into
        # _groups_for_lanes.
        def mock(self, _db, relevant_lanes, queryable_lanes, facets, *args, **kwargs):
            self.called_with = facets
            return []

        old_value = Lane._groups_for_lanes
        Lane._groups_for_lanes = mock  # type: ignore[assignment]
        lane = db.lane()
        facets = FeaturedFacets(0)
        lane.groups(db.session, facets=facets)
        assert facets == lane.called_with
        Lane._groups_for_lanes = old_value

    def test_suppress_before_flush_listeners(self, db: DatabaseTransactionFixture):
        lane1 = db.lane()
        lane2 = db.lane()

        # Updating the flag on one lane does not impact others
        lane1._suppress_before_flush_listeners = True
        assert lane1._suppress_before_flush_listeners is True
        assert lane2._suppress_before_flush_listeners is False
