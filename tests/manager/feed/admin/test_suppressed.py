import urllib

from palace.manager.core.classifier import Classifier
from palace.manager.feed.acquisition import OPDSAcquisitionFeed
from palace.manager.feed.admin.suppressed import (
    AdminSuppressedFeed,
    FacetGroup,
    SuppressedFacets,
    VisibilityFilter,
)
from palace.manager.feed.annotator.admin.suppressed import AdminSuppressedAnnotator
from palace.manager.feed.types import FeedData, Link
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.lane import Pagination
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixtureFake
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

    def test_suppressed_feed_includes_policy_filtered_works(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
        library_fixture: LibraryFixture,
    ):
        """Policy-filtered works (by audience or genre) appear in the suppressed feed."""
        library = db.default_library()
        settings = library_fixture.settings(library)

        # Create a work with Adult audience
        work_adult = db.work(with_open_access_download=True)
        work_adult.audience = Classifier.AUDIENCE_ADULT

        # Create a work with Romance genre (set audience to Young Adult to avoid Adult filter)
        work_romance = db.work(with_open_access_download=True)
        work_romance.audience = Classifier.AUDIENCE_YOUNG_ADULT
        romance_genre, _ = Genre.lookup(db.session, "Romance")
        work_romance.genres = [romance_genre]

        # Create a work that is manually suppressed (set audience to Children to avoid Adult filter)
        work_suppressed = db.work(with_open_access_download=True)
        work_suppressed.audience = Classifier.AUDIENCE_CHILDREN
        work_suppressed.suppressed_for.append(library)

        # Create a visible work (not filtered or suppressed)
        work_visible = db.work(with_open_access_download=True)
        work_visible.audience = Classifier.AUDIENCE_CHILDREN

        annotator = AdminSuppressedAnnotator(None, library)

        # With no filtering, only the manually suppressed work should appear
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
        )
        assert len(feed._feed.entries) == 1
        assert feed._feed.entries[0].work == work_suppressed

        # Filter by Adult audience - should now include work_adult
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
        )
        assert len(feed._feed.entries) == 2
        work_ids = {e.work.id for e in feed._feed.entries}
        assert work_adult.id in work_ids
        assert work_suppressed.id in work_ids

        # Filter by Romance genre - should now include work_romance too
        settings.filtered_genres = ["Romance"]
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
        )
        assert len(feed._feed.entries) == 3
        work_ids = {e.work.id for e in feed._feed.entries}
        assert work_adult.id in work_ids
        assert work_romance.id in work_ids
        assert work_suppressed.id in work_ids
        # Visible work should not be in the feed
        assert work_visible.id not in work_ids

        # Unsuppress the suppressed work - should only include filtered works now
        work_suppressed.suppressed_for = []
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
        )
        assert len(feed._feed.entries) == 2
        work_ids = {e.work.id for e in feed._feed.entries}
        assert work_adult.id in work_ids
        assert work_romance.id in work_ids
        # The visible and un-suppressed work should not be in the feed
        assert work_visible.id not in work_ids
        assert work_suppressed.id not in work_ids

    def test_suppressed_feed_policy_filtered_scoped_to_library_collections(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
        library_fixture: LibraryFixture,
    ):
        """Policy-filtered works should only appear for the library's collections."""
        library = db.default_library()
        settings = library_fixture.settings(library)

        other_library = library_fixture.library(name="Other Library")
        other_collection = db.collection("Other Collection", library=other_library)

        local_work = db.work(
            with_open_access_download=True, collection=db.default_collection()
        )
        local_work.audience = Classifier.AUDIENCE_ADULT

        other_work = db.work(
            with_open_access_download=True, collection=other_collection
        )
        other_work.audience = Classifier.AUDIENCE_ADULT

        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]

        query = AdminSuppressedFeed.suppressed_query(
            db.session, library, visibility_filter=VisibilityFilter.ALL
        )
        work_ids = {work.id for work in query.all()}
        assert local_work.id in work_ids
        assert other_work.id not in work_ids

    def test_suppressed_feed_visibility_status_category(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
        library_fixture: LibraryFixture,
    ):
        """Works in the suppressed feed get the correct visibility status category."""
        library = db.default_library()
        settings = library_fixture.settings(library)

        # Create a manually suppressed work
        work_suppressed = db.work(with_open_access_download=True)
        work_suppressed.suppressed_for.append(library)

        # Create a policy-filtered work
        work_filtered = db.work(with_open_access_download=True)
        work_filtered.audience = Classifier.AUDIENCE_ADULT
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]

        annotator = AdminSuppressedAnnotator(None, library)
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
        )

        # Find entries by work
        entries_by_work = {e.work.id: e for e in feed._feed.entries}

        # Check manually suppressed work has correct category
        suppressed_entry = entries_by_work[work_suppressed.id]
        assert suppressed_entry.computed is not None
        suppressed_categories = [
            c
            for c in suppressed_entry.computed.categories
            if getattr(c, "scheme", None)
            == AdminSuppressedAnnotator.VISIBILITY_STATUS_SCHEME
        ]
        assert len(suppressed_categories) == 1
        assert (
            getattr(suppressed_categories[0], "term", None)
            == AdminSuppressedAnnotator.VISIBILITY_MANUALLY_SUPPRESSED
        )
        assert getattr(suppressed_categories[0], "label", None) == "Manually Suppressed"

        # Check policy-filtered work has correct category
        filtered_entry = entries_by_work[work_filtered.id]
        assert filtered_entry.computed is not None
        filtered_categories = [
            c
            for c in filtered_entry.computed.categories
            if getattr(c, "scheme", None)
            == AdminSuppressedAnnotator.VISIBILITY_STATUS_SCHEME
        ]
        assert len(filtered_categories) == 1
        assert (
            getattr(filtered_categories[0], "term", None)
            == AdminSuppressedAnnotator.VISIBILITY_POLICY_FILTERED
        )
        assert getattr(filtered_categories[0], "label", None) == "Policy Filtered"

    def test_suppressed_feed_links_for_policy_filtered_works(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
        library_fixture: LibraryFixture,
    ):
        """Policy-filtered works do NOT get suppress/unsuppress links."""
        library = db.default_library()
        settings = library_fixture.settings(library)

        def _links_for_rel(links: list[Link], rel: str) -> list[Link]:
            return [link for link in links if link.rel == rel]

        # Create a policy-filtered work (not manually suppressed)
        work_filtered = db.work(with_open_access_download=True)
        work_filtered.audience = Classifier.AUDIENCE_ADULT
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]

        # Create a manually suppressed work
        work_suppressed = db.work(with_open_access_download=True)
        work_suppressed.suppressed_for.append(library)

        annotator = AdminSuppressedAnnotator(None, library)
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
        )

        entries_by_work = {e.work.id: e for e in feed._feed.entries}

        # Policy-filtered work should have NO suppress/unsuppress links
        filtered_entry = entries_by_work[work_filtered.id]
        assert filtered_entry.computed is not None
        suppress_links = _links_for_rel(
            filtered_entry.computed.other_links,
            AdminSuppressedAnnotator.REL_SUPPRESS_FOR_LIBRARY,
        )
        unsuppress_links = _links_for_rel(
            filtered_entry.computed.other_links,
            AdminSuppressedAnnotator.REL_UNSUPPRESS_FOR_LIBRARY,
        )
        assert len(suppress_links) == 0
        assert len(unsuppress_links) == 0

        # But it should still have an edit link
        edit_links = [
            link for link in filtered_entry.computed.other_links if link.rel == "edit"
        ]
        assert len(edit_links) == 1

        # Manually suppressed work should have unsuppress link
        suppressed_entry = entries_by_work[work_suppressed.id]
        assert suppressed_entry.computed is not None
        unsuppress_links = _links_for_rel(
            suppressed_entry.computed.other_links,
            AdminSuppressedAnnotator.REL_UNSUPPRESS_FOR_LIBRARY,
        )
        assert len(unsuppress_links) == 1

    def test_suppressed_feed_both_suppressed_and_filtered(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
        library_fixture: LibraryFixture,
    ):
        """A work that is both manually suppressed AND policy-filtered shows as
        manually suppressed (with unsuppress link).
        """
        library = db.default_library()
        settings = library_fixture.settings(library)

        def _links_for_rel(links: list[Link], rel: str) -> list[Link]:
            return [link for link in links if link.rel == rel]

        # Create a work that is both manually suppressed AND matches filter
        work = db.work(with_open_access_download=True)
        work.audience = Classifier.AUDIENCE_ADULT
        work.suppressed_for.append(library)
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]

        annotator = AdminSuppressedAnnotator(None, library)
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
        )

        assert len(feed._feed.entries) == 1
        entry = feed._feed.entries[0]
        assert entry.computed is not None

        # Should show as "Manually Suppressed" (manual takes precedence)
        visibility_categories = [
            c
            for c in entry.computed.categories
            if getattr(c, "scheme", None)
            == AdminSuppressedAnnotator.VISIBILITY_STATUS_SCHEME
        ]
        assert len(visibility_categories) == 1
        assert (
            getattr(visibility_categories[0], "term", None)
            == AdminSuppressedAnnotator.VISIBILITY_MANUALLY_SUPPRESSED
        )

        # Should have unsuppress link (since it's manually suppressed)
        unsuppress_links = _links_for_rel(
            entry.computed.other_links,
            AdminSuppressedAnnotator.REL_UNSUPPRESS_FOR_LIBRARY,
        )
        assert len(unsuppress_links) == 1

    def test_suppressed_feed_search_link_points_to_suppressed_search(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        """The search link in the suppressed feed should point to suppressed_search endpoint."""
        library = db.default_library()

        # Create a suppressed work so the feed has content
        work = db.work(with_open_access_download=True)
        work.suppressed_for.append(library)

        annotator = AdminSuppressedAnnotator(None, library)
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
        )

        # Find the search link in the feed
        search_links = [link for link in feed._feed.links if link.rel == "search"]
        assert len(search_links) == 1
        search_link = search_links[0]

        # Verify the search link points to suppressed_search endpoint
        assert search_link.href is not None
        assert "suppressed_search" in search_link.href
        assert library.short_name in search_link.href
        assert search_link.type == "application/opensearchdescription+xml"

    def test_suppressed_search_url_helper(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
    ):
        """Test the suppressed_search_url helper generates correct URLs."""
        library = db.default_library()
        annotator = AdminSuppressedAnnotator(None, library)

        # Basic URL with just query
        url = annotator.suppressed_search_url("test query")
        assert "suppressed_search" in url
        assert "q=test+query" in url or "q=test%20query" in url
        assert library.short_name in url

        # URL with pagination
        pagination = Pagination(offset=20, size=10)
        url_with_pagination = annotator.suppressed_search_url("test", pagination)
        assert "suppressed_search" in url_with_pagination
        assert "q=test" in url_with_pagination
        assert "size=10" in url_with_pagination
        assert "after=20" in url_with_pagination

    def test_suppressed_query_filter_all(
        self,
        db: DatabaseTransactionFixture,
        library_fixture: LibraryFixture,
    ):
        """Filter 'all' returns both manually suppressed and policy-filtered works."""
        library = db.default_library()
        settings = library_fixture.settings(library)

        # Create manually suppressed work
        work_suppressed = db.work(with_open_access_download=True)
        work_suppressed.audience = Classifier.AUDIENCE_CHILDREN
        work_suppressed.suppressed_for.append(library)

        # Create policy-filtered work
        work_filtered = db.work(with_open_access_download=True)
        work_filtered.audience = Classifier.AUDIENCE_ADULT
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]

        # Create visible work
        work_visible = db.work(with_open_access_download=True)
        work_visible.audience = Classifier.AUDIENCE_CHILDREN

        # Query with 'all' filter
        query = AdminSuppressedFeed.suppressed_query(
            db.session, library, VisibilityFilter.ALL
        )
        work_ids = {w.id for w in query.all()}

        assert work_suppressed.id in work_ids
        assert work_filtered.id in work_ids
        assert work_visible.id not in work_ids

    def test_suppressed_query_filter_manually_suppressed(
        self,
        db: DatabaseTransactionFixture,
        library_fixture: LibraryFixture,
    ):
        """Filter 'manually-suppressed' returns only manually suppressed works."""
        library = db.default_library()
        settings = library_fixture.settings(library)

        # Create manually suppressed work
        work_suppressed = db.work(with_open_access_download=True)
        work_suppressed.audience = Classifier.AUDIENCE_CHILDREN
        work_suppressed.suppressed_for.append(library)

        # Create policy-filtered work
        work_filtered = db.work(with_open_access_download=True)
        work_filtered.audience = Classifier.AUDIENCE_ADULT
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]

        # Query with 'manually-suppressed' filter
        query = AdminSuppressedFeed.suppressed_query(
            db.session, library, VisibilityFilter.MANUALLY_SUPPRESSED
        )
        work_ids = {w.id for w in query.all()}

        assert work_suppressed.id in work_ids
        assert work_filtered.id not in work_ids

    def test_suppressed_query_filter_policy_filtered(
        self,
        db: DatabaseTransactionFixture,
        library_fixture: LibraryFixture,
    ):
        """Filter 'policy-filtered' returns only policy-filtered works (not manually suppressed)."""
        library = db.default_library()
        settings = library_fixture.settings(library)

        # Create manually suppressed work
        work_suppressed = db.work(with_open_access_download=True)
        work_suppressed.audience = Classifier.AUDIENCE_CHILDREN
        work_suppressed.suppressed_for.append(library)

        # Create policy-filtered work
        work_filtered = db.work(with_open_access_download=True)
        work_filtered.audience = Classifier.AUDIENCE_ADULT
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]

        # Create a work that is both manually suppressed AND filtered
        work_both = db.work(with_open_access_download=True)
        work_both.audience = Classifier.AUDIENCE_ADULT
        work_both.suppressed_for.append(library)

        # Query with 'policy-filtered' filter
        query = AdminSuppressedFeed.suppressed_query(
            db.session, library, VisibilityFilter.POLICY_FILTERED
        )
        work_ids = {w.id for w in query.all()}

        # Only pure policy-filtered work should be included
        assert work_filtered.id in work_ids
        # Manually suppressed should not be included
        assert work_suppressed.id not in work_ids
        # Work that is both should not be included (it's manually suppressed)
        assert work_both.id not in work_ids

    def test_suppressed_feed_includes_facet_links(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
        library_fixture: LibraryFixture,
    ):
        """The suppressed feed includes facet links for visibility filtering."""
        library = db.default_library()

        # Create a suppressed work
        work = db.work(with_open_access_download=True)
        work.suppressed_for.append(library)

        annotator = AdminSuppressedAnnotator(None, library)
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
        )

        # Find facet links (stored in facet_links list)
        facet_links = feed._feed.facet_links

        # Should have 3 facet links (All, Manually Hidden, Policy Filtered)
        assert len(facet_links) == 3

        # Check facet group and titles
        titles = {getattr(link, "title", None) for link in facet_links}
        assert "All" in titles
        assert "Manually Hidden" in titles
        assert "Policy Filtered" in titles

        # Check that 'All' is marked as active (default)
        all_link = next(
            link for link in facet_links if getattr(link, "title", None) == "All"
        )
        assert getattr(all_link, "activeFacet", None) == "true"

    def test_suppressed_feed_facet_links_reflect_current_filter(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
        library_fixture: LibraryFixture,
    ):
        """Facet links correctly mark the active filter."""
        library = db.default_library()

        # Create a suppressed work
        work = db.work(with_open_access_download=True)
        work.suppressed_for.append(library)

        annotator = AdminSuppressedAnnotator(None, library)

        # Generate feed with 'manually-suppressed' filter
        facets = SuppressedFacets(visibility=VisibilityFilter.MANUALLY_SUPPRESSED)
        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
            facets=facets,
        )

        # Find facet links (stored in facet_links list)
        facet_links = feed._feed.facet_links

        # 'Manually Hidden' should be active
        manually_hidden_link = next(
            link
            for link in facet_links
            if getattr(link, "title", None) == "Manually Hidden"
        )
        assert getattr(manually_hidden_link, "activeFacet", None) == "true"

        # 'All' should NOT be active
        all_link = next(
            link for link in facet_links if getattr(link, "title", None) == "All"
        )
        assert getattr(all_link, "activeFacet", None) is None

    def test_suppressed_feed_pagination_preserves_facets(
        self,
        db: DatabaseTransactionFixture,
        patch_url_for: PatchedUrlFor,
        external_search_fake_fixture: ExternalSearchFixtureFake,
        library_fixture: LibraryFixture,
    ):
        """Pagination links preserve the visibility facet parameter."""
        library = db.default_library()

        # Create multiple suppressed works for pagination
        for _ in range(3):
            work = db.work(with_open_access_download=True)
            work.suppressed_for.append(library)

        annotator = AdminSuppressedAnnotator(None, library)
        facets = SuppressedFacets(visibility=VisibilityFilter.MANUALLY_SUPPRESSED)
        pagination = Pagination(size=1)

        feed = AdminSuppressedFeed.suppressed(
            _db=db.session,
            title="Hidden works",
            annotator=annotator,
            pagination=pagination,
            facets=facets,
        )

        # Find the 'next' link
        next_links = [link for link in feed._feed.links if link.rel == "next"]
        assert len(next_links) == 1
        next_link = next_links[0]

        # The next link should include the visibility parameter
        assert next_link.href is not None
        assert "visibility=manually-suppressed" in next_link.href

    # End-to-end search tests using actual search index

    def test_search_returns_suppressed_works(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        patch_url_for: PatchedUrlFor,
        library_fixture: LibraryFixture,
    ):
        """Search within suppressed works returns only suppressed/filtered works."""
        fixture = end_to_end_search_fixture
        db = fixture.db
        library = db.default_library()

        # Create a suppressed work with a distinctive title
        suppressed_work = db.work(
            title="Suppressed Mystery Novel",
            with_open_access_download=True,
        )
        suppressed_work.suppressed_for.append(library)

        # Create a visible work with similar title
        visible_work = db.work(
            title="Visible Mystery Novel",
            with_open_access_download=True,
        )

        # Index the works
        fixture.populate_search_index()

        annotator = AdminSuppressedAnnotator(None, library)
        result = AdminSuppressedFeed.suppressed_search(
            _db=db.session,
            title="Search Results",
            url="http://test/search",
            annotator=annotator,
            search_engine=fixture.external_search_index,
            query="Mystery Novel",
        )

        # Should not be a problem detail
        assert not isinstance(result, ProblemDetail)

        # Should only find the suppressed work, not the visible one
        work_ids = {entry.work.id for entry in result._feed.entries}
        assert suppressed_work.id in work_ids
        assert visible_work.id not in work_ids

    def test_search_returns_policy_filtered_works(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        patch_url_for: PatchedUrlFor,
        library_fixture: LibraryFixture,
    ):
        """Search returns works filtered by library policy (audience/genre)."""
        fixture = end_to_end_search_fixture
        db = fixture.db
        library = db.default_library()
        settings = library_fixture.settings(library)

        # Create an adult work (will be filtered)
        adult_work = db.work(
            title="Adult Content Book",
            with_open_access_download=True,
        )
        adult_work.audience = Classifier.AUDIENCE_ADULT

        # Create a children's work (will not be filtered)
        children_work = db.work(
            title="Children Content Book",
            with_open_access_download=True,
        )
        children_work.audience = Classifier.AUDIENCE_CHILDREN

        # Set up audience filtering
        settings.filtered_audiences = [Classifier.AUDIENCE_ADULT]

        # Index the works
        fixture.populate_search_index()

        annotator = AdminSuppressedAnnotator(None, library)
        result = AdminSuppressedFeed.suppressed_search(
            _db=db.session,
            title="Search Results",
            url="http://test/search",
            annotator=annotator,
            search_engine=fixture.external_search_index,
            query="Content Book",
        )

        assert not isinstance(result, ProblemDetail)

        # Should only find the adult (filtered) work
        work_ids = {entry.work.id for entry in result._feed.entries}
        assert adult_work.id in work_ids
        assert children_work.id not in work_ids

    def test_search_returns_genre_filtered_works(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        patch_url_for: PatchedUrlFor,
        library_fixture: LibraryFixture,
    ):
        """Search returns works filtered by library genre policy."""
        fixture = end_to_end_search_fixture
        db = fixture.db
        library = db.default_library()
        settings = library_fixture.settings(library)

        # Create a romance work (will be filtered)
        romance_work = db.work(
            title="Love Story Novel",
            with_open_access_download=True,
        )
        romance_genre, _ = Genre.lookup(db.session, "Romance")
        romance_work.genres = [romance_genre]

        # Create a sci-fi work (will not be filtered)
        scifi_work = db.work(
            title="Space Story Novel",
            with_open_access_download=True,
        )
        scifi_genre, _ = Genre.lookup(db.session, "Science Fiction")
        scifi_work.genres = [scifi_genre]

        # Set up genre filtering
        settings.filtered_genres = ["Romance"]

        # Index the works
        fixture.populate_search_index()

        annotator = AdminSuppressedAnnotator(None, library)
        result = AdminSuppressedFeed.suppressed_search(
            _db=db.session,
            title="Search Results",
            url="http://test/search",
            annotator=annotator,
            search_engine=fixture.external_search_index,
            query="Story Novel",
        )

        assert not isinstance(result, ProblemDetail)

        # Should only find the romance (filtered) work
        work_ids = {entry.work.id for entry in result._feed.entries}
        assert romance_work.id in work_ids
        assert scifi_work.id not in work_ids

    def test_search_empty_results(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        patch_url_for: PatchedUrlFor,
    ):
        """Search with no matching suppressed works returns empty feed."""
        fixture = end_to_end_search_fixture
        db = fixture.db
        library = db.default_library()

        # Create only visible works
        visible_work = db.work(
            title="Visible Book",
            with_open_access_download=True,
        )

        fixture.populate_search_index()

        annotator = AdminSuppressedAnnotator(None, library)
        result = AdminSuppressedFeed.suppressed_search(
            _db=db.session,
            title="Search Results",
            url="http://test/search",
            annotator=annotator,
            search_engine=fixture.external_search_index,
            query="Visible Book",
        )

        assert not isinstance(result, ProblemDetail)
        assert len(result._feed.entries) == 0

    def test_search_scoped_to_library_collections(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        patch_url_for: PatchedUrlFor,
        library_fixture: LibraryFixture,
    ):
        """Search only returns works from the library's collections."""
        fixture = end_to_end_search_fixture
        db = fixture.db
        library = db.default_library()

        # Create another library with its own collection
        other_library = library_fixture.library(name="Other Library")
        other_collection = db.collection("Other Collection", library=other_library)

        # Suppressed work in default library's collection
        local_work = db.work(
            title="Local Suppressed Book",
            with_open_access_download=True,
            collection=db.default_collection(),
        )
        local_work.suppressed_for.append(library)

        # Suppressed work in other library's collection
        other_work = db.work(
            title="Other Suppressed Book",
            with_open_access_download=True,
            collection=other_collection,
        )
        other_work.suppressed_for.append(other_library)

        fixture.populate_search_index()

        annotator = AdminSuppressedAnnotator(None, library)
        result = AdminSuppressedFeed.suppressed_search(
            _db=db.session,
            title="Search Results",
            url="http://test/search",
            annotator=annotator,
            search_engine=fixture.external_search_index,
            query="Suppressed Book",
        )

        assert not isinstance(result, ProblemDetail)

        # Should only find the local library's suppressed work
        work_ids = {entry.work.id for entry in result._feed.entries}
        assert local_work.id in work_ids
        assert other_work.id not in work_ids

    def test_search_includes_navigation_links(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        patch_url_for: PatchedUrlFor,
    ):
        """Search results include proper navigation links."""
        fixture = end_to_end_search_fixture
        db = fixture.db
        library = db.default_library()

        # Create a suppressed work
        work = db.work(
            title="Navigation Test Book",
            with_open_access_download=True,
        )
        work.suppressed_for.append(library)

        fixture.populate_search_index()

        annotator = AdminSuppressedAnnotator(None, library)
        result = AdminSuppressedFeed.suppressed_search(
            _db=db.session,
            title="Search Results",
            url="http://test/search",
            annotator=annotator,
            search_engine=fixture.external_search_index,
            query="Navigation Test",
        )

        assert not isinstance(result, ProblemDetail)

        # Check for navigation links
        links_by_rel = {link.rel: link for link in result._feed.links}

        # Should have start and up links
        assert "start" in links_by_rel
        assert "up" in links_by_rel
        assert links_by_rel["up"].title == "Hidden Books"


class TestSuppressedFacets:
    """Tests for the SuppressedFacets class."""

    def test_default_visibility(self):
        """Default visibility is 'all'."""
        facets = SuppressedFacets()
        assert facets.visibility == VisibilityFilter.ALL

    def test_from_request_valid_values(self):
        """from_request parses valid visibility values."""
        # Test 'all'
        facets = SuppressedFacets.from_request(lambda k, d: "all")
        assert facets.visibility == VisibilityFilter.ALL

        # Test 'manually-suppressed'
        facets = SuppressedFacets.from_request(lambda k, d: "manually-suppressed")
        assert facets.visibility == VisibilityFilter.MANUALLY_SUPPRESSED

        # Test 'policy-filtered'
        facets = SuppressedFacets.from_request(lambda k, d: "policy-filtered")
        assert facets.visibility == VisibilityFilter.POLICY_FILTERED

    def test_from_request_invalid_value(self):
        """from_request falls back to 'all' for invalid values."""
        facets = SuppressedFacets.from_request(lambda k, d: "invalid-value")
        assert facets.visibility == VisibilityFilter.ALL

    def test_from_request_uses_default(self):
        """from_request uses the default when key is not present."""

        def get_arg(key: str, default: str) -> str:
            return default

        facets = SuppressedFacets.from_request(get_arg)
        assert facets.visibility == VisibilityFilter.ALL

    def test_items_all(self):
        """items() returns empty for 'all' visibility."""
        facets = SuppressedFacets(visibility=VisibilityFilter.ALL)
        assert list(facets.items()) == []

    def test_items_filtered(self):
        """items() returns visibility param for non-'all' values."""
        facets = SuppressedFacets(visibility=VisibilityFilter.MANUALLY_SUPPRESSED)
        assert list(facets.items()) == [("visibility", "manually-suppressed")]

        facets = SuppressedFacets(visibility=VisibilityFilter.POLICY_FILTERED)
        assert list(facets.items()) == [("visibility", "policy-filtered")]

    def test_navigate(self):
        """navigate() creates a new facets object with different visibility."""
        facets = SuppressedFacets(visibility=VisibilityFilter.ALL)
        new_facets = facets.navigate(visibility=VisibilityFilter.MANUALLY_SUPPRESSED)

        # Original unchanged
        assert facets.visibility == VisibilityFilter.ALL
        # New facets has new visibility
        assert new_facets.visibility == VisibilityFilter.MANUALLY_SUPPRESSED

    def test_facet_groups(self):
        """facet_groups yields FacetGroup objects."""
        facets = SuppressedFacets(visibility=VisibilityFilter.MANUALLY_SUPPRESSED)
        groups = list(facets.facet_groups)

        assert len(groups) == 3
        assert all(isinstance(g, FacetGroup) for g in groups)

        # Check 'all' facet
        all_group = groups[0]
        assert all_group.group_name == "Visibility"
        assert all_group.filter_value == VisibilityFilter.ALL
        assert all_group.facets.visibility == VisibilityFilter.ALL
        assert all_group.is_selected is False
        assert all_group.is_default is True

        # Check 'manually-suppressed' facet (should be selected)
        manually_suppressed_group = groups[1]
        assert manually_suppressed_group.group_name == "Visibility"
        assert (
            manually_suppressed_group.filter_value
            == VisibilityFilter.MANUALLY_SUPPRESSED
        )
        assert (
            manually_suppressed_group.facets.visibility
            == VisibilityFilter.MANUALLY_SUPPRESSED
        )
        assert manually_suppressed_group.is_selected is True
        assert manually_suppressed_group.is_default is False

        # Check 'policy-filtered' facet
        policy_filtered_group = groups[2]
        assert policy_filtered_group.group_name == "Visibility"
        assert policy_filtered_group.filter_value == VisibilityFilter.POLICY_FILTERED
        assert (
            policy_filtered_group.facets.visibility == VisibilityFilter.POLICY_FILTERED
        )
        assert policy_filtered_group.is_selected is False
        assert policy_filtered_group.is_default is False
