import json
from datetime import datetime
from unittest.mock import Mock

import pytest

from core.classifier import Classifier
from core.external_search import MockExternalSearchIndex, SortKeyPagination
from core.lane import Facets, Lane, Pagination, SearchFacets
from core.model.classification import Subject
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.resource import Hyperlink
from core.opds2 import AcquisitonFeedOPDS2, OPDS2Annotator
from core.util.flask_util import OPDSFeedResponse
from tests.fixtures.database import DatabaseTransactionFixture


class TestOPDS2FeedFixture:
    transaction: DatabaseTransactionFixture
    search_engine: MockExternalSearchIndex
    fiction: Lane


@pytest.fixture
def opds2_feed_fixture(
    db: DatabaseTransactionFixture,
) -> TestOPDS2FeedFixture:
    data = TestOPDS2FeedFixture()
    data.transaction = db
    data.search_engine = MockExternalSearchIndex()
    data.fiction = db.lane("Fiction")
    data.fiction.fiction = True
    data.fiction.audiences = [Classifier.AUDIENCE_ADULT]
    return data


class TestOPDS2Feed:
    def test_publications_feed(self, opds2_feed_fixture: TestOPDS2FeedFixture):
        data, transaction, session = (
            opds2_feed_fixture,
            opds2_feed_fixture.transaction,
            opds2_feed_fixture.transaction.session,
        )

        work = transaction.work(
            with_open_access_download=True, authors="Author Name", fiction=True
        )
        data.search_engine.bulk_update([work])
        result = AcquisitonFeedOPDS2.publications(
            session,
            data.fiction,
            SearchFacets(),
            Pagination.default(),
            data.search_engine,
            OPDS2Annotator(
                "/", SearchFacets(), Pagination.default(), transaction.default_library()
            ),
        )

        assert type(result) == OPDSFeedResponse
        # assert result.works == [work]

    def test_publications_feed_json(self, opds2_feed_fixture: TestOPDS2FeedFixture):
        data, transaction, session = (
            opds2_feed_fixture,
            opds2_feed_fixture.transaction,
            opds2_feed_fixture.transaction.session,
        )

        works = [
            transaction.work(
                with_open_access_download=True,
                title="title1",
                authors="Author Name1",
                fiction=True,
            ),
            transaction.work(
                with_open_access_download=True,
                title="title2",
                authors="Author Name2",
                fiction=True,
            ),
            transaction.work(
                with_open_access_download=True,
                title="title3",
                authors="Author Name3",
                fiction=True,
            ),
            transaction.work(
                with_open_access_download=True,
                title="title4",
                authors="Author Name4",
                fiction=True,
            ),
        ]
        data.search_engine.bulk_update(works)
        annotator = OPDS2Annotator(
            "/",
            Facets.default(transaction.default_library()),
            Pagination.default(),
            transaction.default_library(),
        )
        result: OPDSFeedResponse = AcquisitonFeedOPDS2.publications(
            session,
            data.fiction,
            SearchFacets(),
            Pagination.default(),
            data.search_engine,
            annotator,
        )
        result = json.loads(result.data)
        assert len(result["publications"]) == len(works)


class TestOPDS2AnnotatorFixture:
    transaction: DatabaseTransactionFixture
    search_engine: MockExternalSearchIndex
    fiction: Lane
    annotator: OPDS2Annotator


@pytest.fixture
def opds2_annotator_fixture(
    db: DatabaseTransactionFixture,
) -> TestOPDS2AnnotatorFixture:
    data = TestOPDS2AnnotatorFixture()
    data.transaction = db
    data.search_engine = MockExternalSearchIndex()
    data.fiction = db.lane("Fiction")
    data.fiction.fiction = True
    data.fiction.audiences = [Classifier.AUDIENCE_ADULT]
    data.annotator = OPDS2Annotator(
        "http://example.org/feed",
        Facets.default(db.default_library()),
        SortKeyPagination("lastitemonpage"),
        db.default_library(),
    )
    return data


class TestOPDS2Annotator:
    def test_feed_links(self, opds2_annotator_fixture: TestOPDS2AnnotatorFixture):
        # Mock the pagination
        m = Mock()
        m.meta = Mock()
        m.meta.sort = ["Item"]
        opds2_annotator_fixture.annotator.pagination.page_loaded([m])
        links = opds2_annotator_fixture.annotator.feed_links()
        assert len(links) == 2
        assert links[0] == {
            "rel": "self",
            "href": "http://example.org/feed",
            "type": "application/opds+json",
        }
        assert "key=%5B%22Item%22%5D" in links[1]["href"]

    def test_image_links(self, opds2_annotator_fixture: TestOPDS2AnnotatorFixture):
        data, transaction, session = (
            opds2_annotator_fixture,
            opds2_annotator_fixture.transaction,
            opds2_annotator_fixture.transaction.session,
        )

        work = transaction.work()
        edition = work.presentation_edition
        idn: Identifier = edition.primary_identifier
        idn.add_link(
            Hyperlink.IMAGE,
            "https://example.org/image",
            edition.data_source,
            media_type="image/png",
        )
        idn.add_link(
            Hyperlink.THUMBNAIL_IMAGE,
            "https://example.org/thumb",
            edition.data_source,
            media_type="image/png",
        )
        data.search_engine.bulk_update([work])
        result = data.annotator.metadata_for_work(work)

        assert "images" in result
        assert len(result["images"]) == 2
        assert result["images"] == [
            dict(
                rel=Hyperlink.IMAGE, href="https://example.org/image", type="image/png"
            ),
            dict(
                rel=Hyperlink.THUMBNAIL_IMAGE,
                href="https://example.org/thumb",
                type="image/png",
            ),
        ]

    def test_work_metadata(self, opds2_annotator_fixture: TestOPDS2AnnotatorFixture):
        data, transaction, session = (
            opds2_annotator_fixture,
            opds2_annotator_fixture.transaction,
            opds2_annotator_fixture.transaction.session,
        )

        work = transaction.work(
            authors="Author Person", genre="Science", with_license_pool=True
        )
        edition: Edition = work.presentation_edition
        idn: Identifier = edition.primary_identifier

        modified = datetime.now()
        work.last_update_time = modified
        edition.license_pools[0].availability_time = modified
        edition.series = "A series"
        edition.series_position = 4

        data.search_engine.bulk_update([work])
        result = data.annotator.metadata_for_work(work)

        meta = result["metadata"]
        assert meta["@type"] == "http://schema.org/EBook"
        assert meta["title"] == work.title
        assert meta["subtitle"] == work.subtitle
        assert meta["identifier"] == idn.urn
        assert meta["modified"] == modified.date().isoformat()
        assert meta["published"] == modified.date().isoformat()
        assert meta["language"] == "en"
        assert meta["sortAs"] == work.sort_title
        assert meta["author"] == {"name": "Author Person"}
        assert meta["subject"] == [
            {"name": "Science", "sortAs": "Science", "scheme": Subject.SIMPLIFIED_GENRE}
        ]
        assert meta["belongsTo"] == {"series": {"name": "A series", "position": 4}}

    def test_feed_metadata(self, opds2_annotator_fixture: TestOPDS2AnnotatorFixture):
        meta = opds2_annotator_fixture.annotator.feed_metadata()
        assert meta == {
            "title": "OPDS2 Feed",
            "itemsPerPage": Pagination.DEFAULT_SIZE,
        }
