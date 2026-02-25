from unittest.mock import MagicMock

from palace.manager.feed.facets.crawlable import CrawlableFacets
from palace.manager.feed.worklist.crawlable import (
    CrawlableCollectionBasedLane,
    CrawlableCustomListBasedLane,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import ExternalSearchFixtureFake


class TestCrawlableCollectionBasedLane:
    def test_init(self, db: DatabaseTransactionFixture):
        # Collection-based crawlable feeds are cached for 5 minutes.
        assert 5 * 60 == CrawlableCollectionBasedLane.MAX_CACHE_AGE

        # This library has two collections.
        library = db.default_library()
        default_collection = db.default_collection()
        other_library_collection = db.collection()
        other_library_collection.associated_libraries.append(library)

        # This collection is not associated with any library.
        unused_collection = db.collection()

        # A lane for all the collections associated with a library.
        lane = CrawlableCollectionBasedLane()
        lane.initialize(library)
        assert "Crawlable feed: %s" % library.name == lane.display_name
        assert {x.id for x in library.active_collections} == set(lane.collection_ids)

        # A lane for specific collection, regardless of their library
        # affiliation.
        lane = CrawlableCollectionBasedLane()
        lane.initialize([unused_collection, other_library_collection])
        assert isinstance(unused_collection.name, str)
        assert isinstance(other_library_collection.name, str)
        assert (
            "Crawlable feed: %s / %s"
            % tuple(sorted([unused_collection.name, other_library_collection.name]))
            == lane.display_name
        )
        assert {unused_collection.id, other_library_collection.id} == set(
            lane.collection_ids
        )

        # Unlike pretty much all other lanes in the system, this lane
        # has no affiliated library.
        assert None == lane.get_library(db.session)

    def test_url_arguments(self, db: DatabaseTransactionFixture):
        library = db.default_library()
        other_collection = db.collection()

        # A lane for all the collections associated with a library.
        lane = CrawlableCollectionBasedLane()
        lane.initialize(library)
        route, kwargs = lane.url_arguments
        assert CrawlableCollectionBasedLane.LIBRARY_ROUTE == route
        assert None == kwargs.get("collection_name")

        # A lane for a collection not actually associated with a
        # library.
        lane = CrawlableCollectionBasedLane()
        lane.initialize([other_collection])
        route, kwargs = lane.url_arguments
        assert CrawlableCollectionBasedLane.COLLECTION_ROUTE == route
        assert other_collection.name == kwargs.get("collection_name")

    def test_works(
        self,
        db: DatabaseTransactionFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        w1 = db.work(collection=db.default_collection())
        w2 = db.work(collection=db.default_collection())
        w3 = db.work(collection=db.collection())

        lane = CrawlableCollectionBasedLane()
        lane.initialize([db.default_collection()])
        search = external_search_fake_fixture.external_search
        search.query_works = MagicMock(return_value=[])
        lane.works(
            db.session, facets=CrawlableFacets.default(None), search_engine=search
        )

        queries = search.query_works.call_args[1]
        assert search.query_works.call_count == 1
        # Only target a single collection
        assert queries["filter"].collection_ids == [db.default_collection().id]
        # without any search query
        assert None == queries["query_string"]


class TestCrawlableCustomListBasedLane:
    def test_initialize(self, db: DatabaseTransactionFixture):
        # These feeds are cached for 12 hours.
        assert 12 * 60 * 60 == CrawlableCustomListBasedLane.MAX_CACHE_AGE

        customlist, ignore = db.customlist()
        lane = CrawlableCustomListBasedLane()
        lane.initialize(db.default_library(), customlist)
        assert db.default_library().id == lane.library_id
        assert [customlist.id] == lane.customlist_ids
        assert customlist.name == lane.customlist_name
        assert "Crawlable feed: %s" % customlist.name == lane.display_name
        assert None == lane.audiences
        assert None == lane.languages
        assert None == lane.media
        assert [] == lane.children

    def test_url_arguments(self, db: DatabaseTransactionFixture):
        customlist, ignore = db.customlist()
        lane = CrawlableCustomListBasedLane()
        lane.initialize(db.default_library(), customlist)
        route, kwargs = lane.url_arguments
        assert CrawlableCustomListBasedLane.ROUTE == route
        assert customlist.name == kwargs.get("list_name")
