from __future__ import annotations

from lxml import etree

from api.app import app
from api.opds import LibraryAnnotator as OldLibraryAnnotator
from api.opds import LibraryLoanAndHoldAnnotator as OldLibraryLoanAndHoldAnnotator
from core.external_search import MockExternalSearchIndex
from core.feed_protocol.acquisition import OPDSAcquisitionFeed
from core.feed_protocol.annotator.circulation import LibraryAnnotator
from core.lane import Facets, Pagination
from core.model.work import Work
from core.opds import AcquisitionFeed
from tests.api.feed_protocol.test_library_annotator import (  # noqa
    LibraryAnnotatorFixture,
    annotator_fixture,
)


def format_tags(tags1, tags2):
    result = ""
    result += "TAG1\n"
    for tag in tags1:
        result += f"{tag[1:]}\n"
    result += "TAG2\n"
    for tag in tags2:
        result += f"{tag[1:]}\n"
    return result


def assert_equal_xmls(xml1: str | etree._Element, xml2: str | etree._Element):
    if isinstance(xml1, str) or isinstance(xml1, bytes):
        parsed1 = etree.fromstring(xml1)
    else:
        parsed1 = xml1

    if isinstance(xml2, str) or isinstance(xml2, bytes):
        parsed2 = etree.fromstring(xml2)
    else:
        parsed2 = xml2

    # Pull out comparable information
    tags1 = [(tag, tag.tag, tag.text, tag.attrib) for tag in parsed1[1:]]
    tags2 = [(tag, tag.tag, tag.text, tag.attrib) for tag in parsed2[1:]]
    # Sort the tags on the information so it's easy to compare sequentially
    tags1.sort(key=lambda x: (x[1], x[2] or "", x[3].values()))
    tags2.sort(key=lambda x: (x[1], x[2] or "", x[3].values()))

    assert len(tags1) == len(tags2), format_tags(tags1, tags2)

    # Assert every tag is equal
    for ix, tag1 in enumerate(tags1):
        tag2 = tags2[ix]
        # Comparable information should be equivalent
        if tag1[1:] == tag2[1:]:
            assert_equal_xmls(tag1[0], tag2[0])
            break
        else:
            assert False, (format_tags([tag1], tags2), f"Did not find {tag1[1:]}")


class TestFeedEquivalence:
    def test_page_feed(self, annotator_fixture: LibraryAnnotatorFixture):
        db = annotator_fixture.db
        lane = annotator_fixture.lane
        library = db.default_library()

        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_open_access_download=True)

        search_index = MockExternalSearchIndex()
        search_index.bulk_update([work1, work2])

        with app.test_request_context("/"):
            new_annotator = LibraryAnnotator(None, lane, library)
            new_feed = OPDSAcquisitionFeed.page(
                db.session,
                lane.display_name,
                "http://test-url/",
                lane,
                new_annotator,
                Facets.default(library),
                Pagination.default(),
                search_index,
            )

            old_annotator = OldLibraryAnnotator(None, lane, library)
            old_feed = AcquisitionFeed.page(
                db.session,
                lane.display_name,
                "http://test-url/",
                lane,
                old_annotator,
                Facets.default(library),
                Pagination.default(),
                search_engine=search_index,
            )

        assert_equal_xmls(str(old_feed), new_feed.serialize())

    def test_page_feed_with_loan_annotator(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        db = annotator_fixture.db
        library = db.default_library()
        work1 = db.work(with_license_pool=True)
        patron = db.patron()
        work1.active_license_pool(library).loan_to(patron)

        with app.test_request_context("/"):
            new_feed = OPDSAcquisitionFeed.active_loans_for(None, patron)
            old_feed = OldLibraryLoanAndHoldAnnotator.active_loans_for(None, patron)

        assert_equal_xmls(str(old_feed), str(new_feed))

    def test_groups_feed(self, annotator_fixture: LibraryAnnotatorFixture):
        db = annotator_fixture.db
        lane = annotator_fixture.lane
        de_lane = db.lane(parent=lane, languages=["de"])
        library = db.default_library()

        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_open_access_download=True, language="de")

        search_index = MockExternalSearchIndex()
        search_index.bulk_update([work1, work2])

        patron = db.patron()
        work1.active_license_pool(library).loan_to(patron)

        with app.test_request_context("/"):
            new_annotator = LibraryAnnotator(None, lane, library)
            new_feed = OPDSAcquisitionFeed.groups(
                db.session,
                "Groups",
                "http://groups/",
                lane,
                new_annotator,
                Pagination.default(),
                Facets.default(library),
                search_index,
            )

            old_annotator = OldLibraryAnnotator(None, lane, library)
            old_feed = AcquisitionFeed.groups(
                db.session,
                "Groups",
                "http://groups/",
                lane,
                old_annotator,
                pagination=Pagination.default(),
                facets=Facets.default(library),
                search_engine=search_index,
            )

        assert_equal_xmls(str(old_feed), new_feed.serialize().decode())

    def test_search_feed(self, annotator_fixture: LibraryAnnotator):
        db = annotator_fixture.db
        lane = annotator_fixture.lane
        de_lane = db.lane(parent=lane, languages=["de"])
        library = db.default_library()

        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_open_access_download=True, language="de")

        search_index = MockExternalSearchIndex()
        search_index.bulk_update([work1, work2])

        patron = db.patron()
        work1.active_license_pool(library).loan_to(patron)

        with app.test_request_context("/"):
            new_annotator = LibraryAnnotator(None, lane, library)
            new_feed = OPDSAcquisitionFeed.search(
                db.session,
                "Search",
                "http://search/",
                lane,
                search_index,
                "query",
                Pagination.default(),
                Facets.default(library),
                new_annotator,
            )

            old_annotator = OldLibraryAnnotator(None, lane, library)
            old_feed = AcquisitionFeed.search(
                db.session,
                "Search",
                "http://search/",
                lane,
                search_index,
                "query",
                Pagination.default(),
                Facets.default(library),
                old_annotator,
            )

            assert_equal_xmls(str(old_feed), str(new_feed))

    def test_from_query_feed(self, annotator_fixture: LibraryAnnotator):
        db = annotator_fixture.db
        lane = annotator_fixture.lane
        de_lane = db.lane(parent=lane, languages=["de"])
        library = db.default_library()

        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_open_access_download=True, language="de")

        search_index = MockExternalSearchIndex()
        search_index.bulk_update([work1, work2])

        patron = db.patron()
        work1.active_license_pool(library).loan_to(patron)

        def url_fn(page):
            return f"http://pagination?page={page}"

        query = db.session.query(Work)

        with app.test_request_context("/"):
            new_annotator = LibraryAnnotator(None, lane, library)
            new_feed = OPDSAcquisitionFeed.from_query(
                query,
                db.session,
                "Search",
                "http://search/",
                Pagination(),
                url_fn,
                new_annotator,
            )

            old_annotator = OldLibraryAnnotator(None, lane, library)
            old_feed = AcquisitionFeed.from_query(
                query,
                db.session,
                "Search",
                "http://search/",
                Pagination(),
                url_fn,
                old_annotator,
            )

            assert_equal_xmls(str(old_feed), new_feed.serialize())
