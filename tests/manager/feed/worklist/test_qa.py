from palace.manager.feed.facets.qa import JackpotFacets
from palace.manager.feed.worklist.qa import JackpotWorkList, KnownOverviewFacetsWorkList
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.sqlalchemy.model.edition import Edition
from tests.fixtures.database import DatabaseTransactionFixture


class TestKnownOverviewFacetsWorkList:
    """Test of the KnownOverviewFacetsWorkList class.

    This is an unusual class which should be used when hard-coding the
    faceting object to use for a given WorkList when generating a
    grouped feed.
    """

    def test_overview_facets(self, db: DatabaseTransactionFixture):
        # Show that we can hard-code the return value of overview_facets.
        #
        # core/tests/test_lanes.py#TestWorkList.test_groups_propagates_facets
        # verifies that WorkList.groups() calls
        # WorkList.overview_facets() and passes the return value
        # (which we hard-code here) into WorkList.works().

        # Pass in a known faceting object.
        known_facets = object()
        wl = KnownOverviewFacetsWorkList(known_facets)

        # That faceting object is always returned when we're
        # making a grouped feed.
        some_other_facets = object()
        assert known_facets == wl.overview_facets(db.session, some_other_facets)


class TestJackpotWorkList:
    """Test the 'jackpot' WorkList that always contains the information
    necessary to run a full suite of integration tests.
    """

    def test_constructor(self, db: DatabaseTransactionFixture):
        # Add some stuff to the default library to make sure we
        # test everything.

        # The default library comes with an active collection whose data
        # source is unspecified (there is also an inactive one). Make
        # another one whose data source _is_ specified.
        library = db.default_library()
        overdrive_collection = db.collection(
            "Test Overdrive Collection",
            protocol=OverdriveAPI,
        )
        overdrive_collection.associated_libraries.append(library)

        # Create another collection that is _not_ associated with this
        # library. It will not be used at all.
        ignored_collection = db.collection(
            "Ignored Collection",
            protocol=BibliothecaAPI,
        )

        # Pass in a JackpotFacets object
        facets = JackpotFacets.default(library)

        # The JackpotWorkList has no works of its own -- only its children
        # have works.
        wl = JackpotWorkList(library, facets)
        assert [] == wl.works(db.session)

        # Let's take a look at the children.

        # NOTE: This test is structured to make it easy to add other
        # groups of children later on. However it's more likely we will
        # test other features with totally different feeds.
        children = list(wl.children)
        available_now = children[:4]
        children = children[4:]

        # This group contains four similar
        # KnownOverviewFacetsWorkLists. They only show works that are
        # currently available.
        for i in available_now:
            # Each lane is associated with the JackpotFacets we passed
            # in.
            assert isinstance(i, KnownOverviewFacetsWorkList)
            internal_facets = i.facets
            assert facets == internal_facets

        # These worklists show ebooks and audiobooks from the two
        # collections associated with the default library.
        [
            default_audio,
            default_ebooks,
            overdrive_audio,
            overdrive_ebooks,
        ] = sorted(available_now, key=lambda x: x.display_name)

        assert (
            "License source {OPDS} - Medium {Book} - Collection name {%s}"
            % db.default_collection().name
            == default_ebooks.display_name
        )
        assert [db.default_collection().id] == default_ebooks.collection_ids
        assert [Edition.BOOK_MEDIUM] == default_ebooks.media

        assert (
            "License source {OPDS} - Medium {Audio} - Collection name {%s}"
            % db.default_collection().name
            == default_audio.display_name
        )
        assert [db.default_collection().id] == default_audio.collection_ids
        assert [Edition.AUDIO_MEDIUM] == default_audio.media

        assert (
            "License source {Overdrive} - Medium {Book} - Collection name {Test Overdrive Collection}"
            == overdrive_ebooks.display_name
        )
        assert [overdrive_collection.id] == overdrive_ebooks.collection_ids
        assert [Edition.BOOK_MEDIUM] == overdrive_ebooks.media

        assert (
            "License source {Overdrive} - Medium {Audio} - Collection name {Test Overdrive Collection}"
            == overdrive_audio.display_name
        )
        assert [overdrive_collection.id] == overdrive_audio.collection_ids
        assert [Edition.AUDIO_MEDIUM] == overdrive_audio.media

        # At this point we've looked at all the children of the
        # JackpotWorkList
        assert [] == children
