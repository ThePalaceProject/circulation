import urllib

from palace.manager.feed.acquisition import OPDSAcquisitionFeed
from palace.manager.feed.admin.suppressed import AdminSuppressedFeed
from palace.manager.feed.annotator.admin.suppressed import AdminSuppressedAnnotator
from palace.manager.feed.types import FeedData, Link
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.lane import Pagination
from palace.manager.sqlalchemy.model.measurement import Measurement
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import ExternalSearchFixtureFake
from tests.manager.feed.conftest import PatchedUrlFor


class TestAdminSuppressedFeed:
    def links(self, feed: FeedData, rel=None):
        all_links = feed.links + feed.facet_links + feed.breadcrumbs
        links = sorted(all_links, key=lambda x: (x.rel, getattr(x, "title", None)))
        return [
            lnk
            for lnk in links
            if not rel or lnk.rel == rel or (isinstance(rel, list) and lnk.rel in rel)
        ]

    def test_feed_includes_staff_rating(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
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
            AdminSuppressedAnnotator(None, db.default_library()),
        )
        feed.generate_feed()

        [entry] = feed._feed.entries
        assert entry.computed is not None
        assert len(entry.computed.ratings) == 2
        assert 3 == float(entry.computed.ratings[1].ratingValue)  # type: ignore[attr-defined]
        assert Measurement.RATING == entry.computed.ratings[1].additionalType  # type: ignore[attr-defined]

    def test_feed_includes_refresh_link(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
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
            AdminSuppressedAnnotator(None, db.default_library()),
        )
        [entry] = feed._feed.entries
        assert entry.computed is not None
        assert [] == [
            x
            for x in entry.computed.other_links
            if x.rel == "http://librarysimplified.org/terms/rel/refresh"
        ]

    def test_feed_includes_suppress_link(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        def _links_for_rel(links: list[Link], rel: str) -> list[Link]:
            return [link for link in links if link.rel == rel]

        work = db.work(with_open_access_download=True)
        lp = work.license_pools[0]
        library = db.default_library()

        lp.suppressed = False

        assert library not in work.suppressed_for

        feed = OPDSAcquisitionFeed(
            "test",
            "url",
            [work],
            AdminSuppressedAnnotator(None, library),
        )
        [entry] = feed._feed.entries
        assert entry.computed is not None

        [suppress_link] = _links_for_rel(
            entry.computed.other_links,
            AdminSuppressedAnnotator.REL_SUPPRESS_FOR_LIBRARY,
        )
        unsuppress_links = _links_for_rel(
            entry.computed.other_links,
            AdminSuppressedAnnotator.REL_UNSUPPRESS_FOR_LIBRARY,
        )
        assert len(unsuppress_links) == 0
        assert suppress_link.href and lp.identifier.identifier in suppress_link.href

        work.suppressed_for.append(library)
        feed = OPDSAcquisitionFeed(
            "test",
            "url",
            [work],
            AdminSuppressedAnnotator(None, library),
        )
        [entry] = feed._feed.entries
        assert entry.computed is not None

        suppress_links = _links_for_rel(
            entry.computed.other_links,
            AdminSuppressedAnnotator.REL_SUPPRESS_FOR_LIBRARY,
        )
        [unsuppress_link] = _links_for_rel(
            entry.computed.other_links,
            AdminSuppressedAnnotator.REL_UNSUPPRESS_FOR_LIBRARY,
        )
        assert len(suppress_links) == 0
        assert unsuppress_link.href and lp.identifier.identifier in suppress_link.href

        lp.suppressed = True

        feed = OPDSAcquisitionFeed(
            "test",
            "url",
            [work],
            AdminSuppressedAnnotator(None, db.default_library()),
        )
        [entry] = feed._feed.entries
        assert entry.computed is not None
        suppress_links = _links_for_rel(
            entry.computed.other_links,
            AdminSuppressedAnnotator.REL_SUPPRESS_FOR_LIBRARY,
        )
        unsuppress_links = _links_for_rel(
            entry.computed.other_links,
            AdminSuppressedAnnotator.REL_UNSUPPRESS_FOR_LIBRARY,
        )
        assert len(suppress_links) == 0
        assert len(unsuppress_links) == 0

    def test_feed_includes_edit_link(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        work = db.work(with_open_access_download=True)
        lp = work.license_pools[0]

        feed = OPDSAcquisitionFeed(
            "test",
            "url",
            [work],
            AdminSuppressedAnnotator(None, db.default_library()),
        )
        [entry] = feed._feed.entries
        assert entry.computed is not None
        [edit_link] = [x for x in entry.computed.other_links if x.rel == "edit"]
        assert edit_link.href and lp.identifier.identifier in edit_link.href

    def test_suppressed_feed(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        library = db.default_library()

        def library_in(url: str) -> bool:
            # Ensure library represented in href from `patch_url_for`.
            parsed = urllib.parse.urlparse(url)
            query = urllib.parse.parse_qs(parsed.query)
            [short_name] = query.get("library_short_name", [])
            return short_name == library.short_name

        # Test the ability to show a paginated feed of suppressed works.
        # Only works suppressed at the library level will be included.
        work1 = db.work(with_open_access_download=True)
        work2 = db.work(with_open_access_download=True)
        work1.suppressed_for.append(library)
        work2.suppressed_for.append(library)

        # This work won't be included in the feed, since it is
        # also suppressed at the collection (license pool) level.
        work3 = db.work(with_open_access_download=True)
        work3.license_pools[0].suppressed = True
        work3.suppressed_for.append(library)

        pagination = Pagination(size=1)
        pagination_page_1 = pagination
        pagination_page_2 = pagination_page_1.next_page
        pagination_page_3 = pagination_page_2.next_page

        annotator = AdminSuppressedAnnotator(None, db.default_library())
        titles = [work1.title, work2.title]

        def make_page(_pagination: Pagination):
            return AdminSuppressedFeed.suppressed(
                _db=db.session,
                title="Hidden works",
                annotator=annotator,
                pagination=_pagination,
            )

        # The start and first page URLs should always be the same.
        expected_start_url = annotator.suppressed_url()
        expected_first_url = annotator.suppressed_url_with_pagination(pagination_page_1)

        first_page = make_page(pagination_page_1)._feed
        assert 1 == len(first_page.entries)
        assert first_page.entries[0].computed.title.text in titles
        titles.remove(first_page.entries[0].computed.title.text)
        [remaining_title] = titles

        # Make sure the links are in place.
        [start1] = self.links(first_page, "start")
        assert expected_start_url == start1.href
        assert annotator.top_level_title() == start1.title
        assert library_in(start1.href)

        [next_link1] = self.links(first_page, "next")
        assert (
            annotator.suppressed_url_with_pagination(pagination_page_1.next_page)
            == next_link1.href
        )
        assert library_in(next_link1.href)

        # This was the first page, so no first or previous link.
        assert len(self.links(first_page, "first")) == 0
        assert len(self.links(first_page, "previous")) == 0

        # Now get the second page and make sure it has a 'previous' link, but no 'next' link.
        second_page = make_page(pagination_page_2)._feed
        [start2] = self.links(second_page, "start")
        [first2] = self.links(second_page, "first")
        [previous2] = self.links(second_page, "previous")
        assert len(self.links(second_page, "next")) == 0
        assert expected_start_url == start2.href
        assert expected_first_url == first2.href
        assert (
            annotator.suppressed_url_with_pagination(pagination_page_1)
            == previous2.href
        )
        assert library_in(previous2.href)
        assert 1 == len(second_page.entries)
        assert remaining_title == second_page.entries[0].computed.title.text

        # A normal crawl should not get here; but, for testing purposes,
        # we force a third page, which should be empty.
        third_page = make_page(pagination_page_3)._feed
        [start3] = self.links(third_page, "start")
        [first3] = self.links(third_page, "first")
        [previous3] = self.links(third_page, "previous")
        assert len(self.links(third_page, "next")) == 0
        assert expected_start_url == start3.href
        assert expected_first_url == first3.href
        assert (
            annotator.suppressed_url_with_pagination(pagination_page_2)
            == previous3.href
        )
        assert 0 == len(third_page.entries)
