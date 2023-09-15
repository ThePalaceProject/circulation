from core.feed.acquisition import OPDSAcquisitionFeed
from core.feed.admin import AdminFeed
from core.feed.annotator.admin import AdminAnnotator
from core.feed.types import FeedData
from core.lane import Pagination
from core.model.datasource import DataSource
from core.model.measurement import Measurement
from tests.api.feed.fixtures import PatchedUrlFor, patch_url_for  # noqa
from tests.fixtures.database import DatabaseTransactionFixture


class TestOPDS:
    def links(self, feed: FeedData, rel=None):
        all_links = feed.links + feed.facet_links + feed.breadcrumbs
        links = sorted(all_links, key=lambda x: (x.rel, getattr(x, "title", None)))
        r = []
        for l in links:
            if not rel or l.rel == rel or (isinstance(rel, list) and l.rel in rel):
                r.append(l)
        return r

    def test_feed_includes_staff_rating(
        self, db: DatabaseTransactionFixture, patch_url_for: PatchedUrlFor
    ):
        work = db.work(with_open_access_download=True)
        lp = work.license_pools[0]
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)
        lp.identifier.add_measurement(
            staff_data_source, Measurement.RATING, 3, weight=1000
        )

        feed = OPDSAcquisitionFeed(
            "test",
            "url",
            [work],
            AdminAnnotator(None, db.default_library()),
        )
        feed.generate_feed()

        [entry] = feed._feed.entries
        assert entry.computed is not None
        assert len(entry.computed.ratings) == 2
        assert 3 == float(entry.computed.ratings[1].ratingValue)  # type: ignore[attr-defined]
        assert Measurement.RATING == entry.computed.ratings[1].additionalType  # type: ignore[attr-defined]

    def test_feed_includes_refresh_link(
        self, db: DatabaseTransactionFixture, patch_url_for: PatchedUrlFor
    ):
        work = db.work(with_open_access_download=True)
        lp = work.license_pools[0]
        lp.suppressed = False
        db.session.commit()

        # If the metadata wrangler isn't configured, the link is left out.
        feed = OPDSAcquisitionFeed(
            "test",
            "url",
            [work],
            AdminAnnotator(None, db.default_library()),
        )
        [entry] = feed._feed.entries
        assert entry.computed is not None
        assert [] == [
            x
            for x in entry.computed.other_links
            if x.rel == "http://librarysimplified.org/terms/rel/refresh"
        ]

    def test_feed_includes_suppress_link(
        self, db: DatabaseTransactionFixture, patch_url_for: PatchedUrlFor
    ):
        work = db.work(with_open_access_download=True)
        lp = work.license_pools[0]
        lp.suppressed = False
        db.session.commit()

        feed = OPDSAcquisitionFeed(
            "test",
            "url",
            [work],
            AdminAnnotator(None, db.default_library()),
        )
        [entry] = feed._feed.entries
        assert entry.computed is not None
        [suppress_link] = [
            x
            for x in entry.computed.other_links
            if x.rel == "http://librarysimplified.org/terms/rel/hide"
        ]
        assert suppress_link.href and lp.identifier.identifier in suppress_link.href
        unsuppress_links = [
            x
            for x in entry.computed.other_links
            if x.rel == "http://librarysimplified.org/terms/rel/restore"
        ]
        assert 0 == len(unsuppress_links)

        lp.suppressed = True
        db.session.commit()

        feed = OPDSAcquisitionFeed(
            "test",
            "url",
            [work],
            AdminAnnotator(None, db.default_library()),
        )
        [entry] = feed._feed.entries
        assert entry.computed is not None
        [unsuppress_link] = [
            x
            for x in entry.computed.other_links
            if x.rel == "http://librarysimplified.org/terms/rel/restore"
        ]
        assert unsuppress_link.href and lp.identifier.identifier in unsuppress_link.href
        suppress_links = [
            x
            for x in entry.computed.other_links
            if x.rel == "http://librarysimplified.org/terms/rel/hide"
        ]
        assert 0 == len(suppress_links)

    def test_feed_includes_edit_link(
        self, db: DatabaseTransactionFixture, patch_url_for: PatchedUrlFor
    ):
        work = db.work(with_open_access_download=True)
        lp = work.license_pools[0]

        feed = OPDSAcquisitionFeed(
            "test",
            "url",
            [work],
            AdminAnnotator(None, db.default_library()),
        )
        [entry] = feed._feed.entries
        assert entry.computed is not None
        [edit_link] = [x for x in entry.computed.other_links if x.rel == "edit"]
        assert edit_link.href and lp.identifier.identifier in edit_link.href

    def test_suppressed_feed(
        self, db: DatabaseTransactionFixture, patch_url_for: PatchedUrlFor
    ):
        # Test the ability to show a paginated feed of suppressed works.

        work1 = db.work(with_open_access_download=True)
        work1.license_pools[0].suppressed = True

        work2 = db.work(with_open_access_download=True)
        work2.license_pools[0].suppressed = True

        # This work won't be included in the feed since its
        # suppressed pool is superceded.
        work3 = db.work(with_open_access_download=True)
        work3.license_pools[0].suppressed = True
        work3.license_pools[0].superceded = True

        pagination = Pagination(size=1)
        annotator = MockAnnotator(db.default_library())
        titles = [work1.title, work2.title]

        def make_page(pagination):
            return AdminFeed.suppressed(
                _db=db.session,
                title="Hidden works",
                url=db.fresh_url(),
                annotator=annotator,
                pagination=pagination,
            )

        feed = make_page(pagination)._feed
        assert 1 == len(feed.entries)
        assert feed.entries[0].computed.title.text in titles
        titles.remove(feed.entries[0].computed.title.text)
        [remaining_title] = titles

        # Make sure the links are in place.
        [start] = self.links(feed, "start")
        assert annotator.groups_url(None) == start.href
        assert annotator.top_level_title() == start.title

        [up] = self.links(feed, "up")
        assert annotator.groups_url(None) == up.href
        assert annotator.top_level_title() == up.title

        [next_link] = self.links(feed, "next")
        assert annotator.suppressed_url(pagination.next_page) == next_link.href

        # This was the first page, so no previous link.
        assert [] == self.links(feed, "previous")

        # Now get the second page and make sure it has a 'previous' link.
        second_page = make_page(pagination.next_page)._feed
        [previous] = self.links(second_page, "previous")
        assert annotator.suppressed_url(pagination) == previous.href
        assert 1 == len(second_page.entries)
        assert remaining_title == second_page.entries[0].computed.title.text

        # The third page is empty.
        third_page = make_page(pagination.next_page.next_page)._feed
        [previous] = self.links(third_page, "previous")
        assert annotator.suppressed_url(pagination.next_page) == previous.href
        assert 0 == len(third_page.entries)


class MockAnnotator(AdminAnnotator):
    def __init__(self, library):
        super().__init__(None, library)

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
