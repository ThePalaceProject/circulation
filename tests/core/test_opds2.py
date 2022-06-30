from datetime import datetime

from core.classifier import Classifier
from core.external_search import MockExternalSearchIndex
from core.lane import Facets, Pagination, SearchFacets
from core.model.classification import Subject
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.resource import Hyperlink
from core.opds2 import AcquisitonFeedOPDS2, OPDS2Annotator
from core.testing import DatabaseTest


class TestOPDS2Feed(DatabaseTest):
    def setup_method(self):
        super().setup_method()

        self.search_engine = MockExternalSearchIndex()

        self.fiction = self._lane("Fiction")
        self.fiction.fiction = True
        self.fiction.audiences = [Classifier.AUDIENCE_ADULT]

    def test_publications_feed(self):
        work = self._work(
            with_open_access_download=True, authors="Author Name", fiction=True
        )
        self.search_engine.bulk_update([work])
        result = AcquisitonFeedOPDS2.publications(
            self._db,
            self.fiction,
            SearchFacets(),
            Pagination.default(),
            self.search_engine,
            OPDS2Annotator(
                "/", SearchFacets(), Pagination.default(), self._default_library
            ),
        )

        assert type(result) == AcquisitonFeedOPDS2
        assert result.works == [work]

    def test_publications_feed_json(self):
        works = [
            self._work(
                with_open_access_download=True,
                title="title1",
                authors="Author Name1",
                fiction=True,
            ),
            self._work(
                with_open_access_download=True,
                title="title2",
                authors="Author Name2",
                fiction=True,
            ),
            self._work(
                with_open_access_download=True,
                title="title3",
                authors="Author Name3",
                fiction=True,
            ),
            self._work(
                with_open_access_download=True,
                title="title4",
                authors="Author Name4",
                fiction=True,
            ),
        ]
        self.search_engine.bulk_update(works)
        annotator = OPDS2Annotator(
            "/",
            Facets.default(self._default_library),
            Pagination.default(),
            self._default_library,
        )
        result = AcquisitonFeedOPDS2.publications(
            self._db,
            self.fiction,
            SearchFacets(),
            Pagination.default(),
            self.search_engine,
            annotator,
        )
        result = result.json()

        assert len(result["publications"]) == len(works)


class TestOPDS2Annotator(DatabaseTest):
    def setup_method(self):
        super().setup_method()

        self.search_engine = MockExternalSearchIndex()

        self.fiction = self._lane("Fiction")
        self.fiction.fiction = True
        self.fiction.audiences = [Classifier.AUDIENCE_ADULT]
        self.annotator = OPDS2Annotator(
            "http://example.org/feed?page=2",
            Facets.default(self._default_library),
            Pagination(),
            self._default_library,
        )

    def test_feed_links(self):
        links = self.annotator.feed_links()
        assert len(links) == 1
        assert links[0] == {
            "rel": "self",
            "href": "http://example.org/feed?page=2",
            "type": "application/opds+json",
        }

    def test_image_links(self):
        work = self._work()
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
        self.search_engine.bulk_update([work])

        result = self.annotator.metadata_for_work(work)

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

    def test_work_metadata(self):
        work = self._work(
            authors="Author Person", genre="Science", with_license_pool=True
        )
        edition: Edition = work.presentation_edition
        idn: Identifier = edition.primary_identifier

        modified = datetime.now()
        work.last_update_time = modified
        edition.license_pools[0].availability_time = modified
        edition.series = "A series"
        edition.series_position = 4

        self.search_engine.bulk_update([work])
        result = self.annotator.metadata_for_work(work)

        meta = result["metadata"]
        assert meta["@type"] == "http://schema.org/EBook"
        assert meta["title"] == work.title
        assert meta["subtitle"] == work.subtitle
        assert meta["identifier"] == idn.identifier
        assert meta["modified"] == modified.isoformat()
        assert meta["published"] == modified.isoformat()
        assert meta["language"] == "en"
        assert meta["sortAs"] == work.sort_title
        assert meta["author"] == {"name": "Author Person"}
        assert meta["subject"] == [
            {"name": "Science", "sortAs": "Science", "scheme": Subject.SIMPLIFIED_GENRE}
        ]
        assert meta["belongsTo"] == {"series": {"name": "A series", "position": 4}}

    def test_feed_metadata(self):
        meta = self.annotator.feed_metadata()
        assert meta == {
            "title": "OPDS2 Feed",
            "itemsPerPage": Pagination.DEFAULT_SIZE,
            "currentPage": 1,
        }
