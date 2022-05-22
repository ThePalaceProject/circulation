import feedparser

from api.admin.opds import AdminAnnotator, AdminFeed
from api.opds import AcquisitionFeed
from core.lane import Pagination
from core.model import DataSource, ExternalIntegration, Measurement
from core.model.configuration import ExternalIntegrationLink
from core.testing import DatabaseTest


class TestOPDS(DatabaseTest):
    def links(self, entry, rel=None):
        if "feed" in entry:
            entry = entry["feed"]
        links = sorted(entry["links"], key=lambda x: (x["rel"], x.get("title")))
        r = []
        for l in links:
            if (
                not rel
                or l["rel"] == rel
                or (isinstance(rel, list) and l["rel"] in rel)
            ):
                r.append(l)
        return r

    def test_feed_includes_staff_rating(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]
        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        lp.identifier.add_measurement(
            staff_data_source, Measurement.RATING, 3, weight=1000
        )

        feed = AcquisitionFeed(
            self._db,
            "test",
            "url",
            [work],
            AdminAnnotator(None, self._default_library, test_mode=True),
        )
        [entry] = feedparser.parse(str(feed))["entries"]
        rating = entry["schema_rating"]
        assert 3 == float(rating["schema:ratingvalue"])
        assert Measurement.RATING == rating["additionaltype"]

    def test_feed_includes_refresh_link(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]
        lp.suppressed = False
        self._db.commit()

        # If the metadata wrangler isn't configured, the link is left out.
        feed = AcquisitionFeed(
            self._db,
            "test",
            "url",
            [work],
            AdminAnnotator(None, self._default_library, test_mode=True),
        )
        [entry] = feedparser.parse(str(feed))["entries"]
        assert [] == [
            x
            for x in entry["links"]
            if x["rel"] == "http://librarysimplified.org/terms/rel/refresh"
        ]

        # If we configure a metadata wrangler integration, the link appears.
        integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL,
            settings={ExternalIntegration.URL: "http://metadata"},
            password="pw",
        )
        integration.collections += [self._default_collection]
        feed = AcquisitionFeed(
            self._db,
            "test",
            "url",
            [work],
            AdminAnnotator(None, self._default_library, test_mode=True),
        )
        [entry] = feedparser.parse(str(feed))["entries"]
        [refresh_link] = [
            x
            for x in entry["links"]
            if x["rel"] == "http://librarysimplified.org/terms/rel/refresh"
        ]
        assert lp.identifier.identifier in refresh_link["href"]

    def test_feed_includes_suppress_link(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]
        lp.suppressed = False
        self._db.commit()

        feed = AcquisitionFeed(
            self._db,
            "test",
            "url",
            [work],
            AdminAnnotator(None, self._default_library, test_mode=True),
        )
        [entry] = feedparser.parse(str(feed))["entries"]
        [suppress_link] = [
            x
            for x in entry["links"]
            if x["rel"] == "http://librarysimplified.org/terms/rel/hide"
        ]
        assert lp.identifier.identifier in suppress_link["href"]
        unsuppress_links = [
            x
            for x in entry["links"]
            if x["rel"] == "http://librarysimplified.org/terms/rel/restore"
        ]
        assert 0 == len(unsuppress_links)

        lp.suppressed = True
        self._db.commit()

        feed = AcquisitionFeed(
            self._db,
            "test",
            "url",
            [work],
            AdminAnnotator(None, self._default_library, test_mode=True),
        )
        [entry] = feedparser.parse(str(feed))["entries"]
        [unsuppress_link] = [
            x
            for x in entry["links"]
            if x["rel"] == "http://librarysimplified.org/terms/rel/restore"
        ]
        assert lp.identifier.identifier in unsuppress_link["href"]
        suppress_links = [
            x
            for x in entry["links"]
            if x["rel"] == "http://librarysimplified.org/terms/rel/hide"
        ]
        assert 0 == len(suppress_links)

    def test_feed_includes_edit_link(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]

        feed = AcquisitionFeed(
            self._db,
            "test",
            "url",
            [work],
            AdminAnnotator(None, self._default_library, test_mode=True),
        )
        [entry] = feedparser.parse(str(feed))["entries"]
        [edit_link] = [x for x in entry["links"] if x["rel"] == "edit"]
        assert lp.identifier.identifier in edit_link["href"]

    def test_feed_includes_change_cover_link(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]
        library = self._default_library

        feed = AcquisitionFeed(
            self._db,
            "test",
            "url",
            [work],
            AdminAnnotator(None, library, test_mode=True),
        )
        [entry] = feedparser.parse(str(feed))["entries"]

        # Since there's no storage integration, the change cover link isn't included.
        assert [] == [
            x
            for x in entry["links"]
            if x["rel"] == "http://librarysimplified.org/terms/rel/change_cover"
        ]

        # There is now a covers storage integration that is linked to the external
        # integration for a collection that the work is in. It will use that
        # covers mirror and the change cover link is included.
        storage = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL
        )
        storage.username = "user"
        storage.password = "pass"

        collection = self._collection()
        purpose = ExternalIntegrationLink.COVERS
        external_integration_link = self._external_integration_link(
            integration=collection._external_integration,
            other_integration=storage,
            purpose=purpose,
        )
        library.collections.append(collection)
        work = self._work(with_open_access_download=True, collection=collection)
        lp = work.license_pools[0]
        feed = AcquisitionFeed(
            self._db,
            "test",
            "url",
            [work],
            AdminAnnotator(None, library, test_mode=True),
        )
        [entry] = feedparser.parse(str(feed))["entries"]

        [change_cover_link] = [
            x
            for x in entry["links"]
            if x["rel"] == "http://librarysimplified.org/terms/rel/change_cover"
        ]
        assert lp.identifier.identifier in change_cover_link["href"]

    def test_suppressed_feed(self):
        # Test the ability to show a paginated feed of suppressed works.

        work1 = self._work(with_open_access_download=True)
        work1.license_pools[0].suppressed = True

        work2 = self._work(with_open_access_download=True)
        work2.license_pools[0].suppressed = True

        # This work won't be included in the feed since its
        # suppressed pool is superceded.
        work3 = self._work(with_open_access_download=True)
        work3.license_pools[0].suppressed = True
        work3.license_pools[0].superceded = True

        pagination = Pagination(size=1)
        annotator = MockAnnotator(self._default_library)
        titles = [work1.title, work2.title]

        def make_page(pagination):
            return AdminFeed.suppressed(
                _db=self._db,
                title="Hidden works",
                url=self._url,
                annotator=annotator,
                pagination=pagination,
            )

        first_page = make_page(pagination)
        parsed = feedparser.parse(str(first_page))
        assert 1 == len(parsed["entries"])
        assert parsed["entries"][0].title in titles
        titles.remove(parsed["entries"][0].title)
        [remaining_title] = titles

        # Make sure the links are in place.
        [start] = self.links(parsed, "start")
        assert annotator.groups_url(None) == start["href"]
        assert annotator.top_level_title() == start["title"]

        [up] = self.links(parsed, "up")
        assert annotator.groups_url(None) == up["href"]
        assert annotator.top_level_title() == up["title"]

        [next_link] = self.links(parsed, "next")
        assert annotator.suppressed_url(pagination.next_page) == next_link["href"]

        # This was the first page, so no previous link.
        assert [] == self.links(parsed, "previous")

        # Now get the second page and make sure it has a 'previous' link.
        second_page = make_page(pagination.next_page)
        parsed = feedparser.parse(str(second_page))
        [previous] = self.links(parsed, "previous")
        assert annotator.suppressed_url(pagination) == previous["href"]
        assert 1 == len(parsed["entries"])
        assert remaining_title == parsed["entries"][0]["title"]

        # The third page is empty.
        third_page = make_page(pagination.next_page.next_page)
        parsed = feedparser.parse(str(third_page))
        [previous] = self.links(parsed, "previous")
        assert annotator.suppressed_url(pagination.next_page) == previous["href"]
        assert 0 == len(parsed["entries"])


class MockAnnotator(AdminAnnotator):
    def __init__(self, library):
        super().__init__(None, library, test_mode=True)

    def groups_url(self, lane):
        if lane:
            name = lane.name
        else:
            name = ""
        return "http://groups/%s" % name

    def suppressed_url(self, pagination):
        base = "http://suppressed/"
        sep = "?"
        if pagination:
            base += sep + pagination.query_string
        return base

    def annotate_feed(self, feed):
        super().annotate_feed(feed)
