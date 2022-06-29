from datetime import datetime

from core.classifier import Classifier
from core.external_search import MockExternalSearchIndex
from core.lane import SearchFacets
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
            self._db, "/", self.fiction, SearchFacets(), self.search_engine
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
        last_ix = len(works) - 1
        modified = datetime.now()
        works[-1].last_update_time = modified
        works[-1].presentation_edition.series = "A series"
        works[-1].presentation_edition.primary_identifier.add_link(
            Hyperlink.SAMPLE,
            "https://example.org/sample",
            works[-1].presentation_edition.data_source,
            media_type="application/zip+epub",
        )

        self.search_engine.bulk_update(works)
        annotator = OPDS2Annotator(self.fiction, self._default_library)
        result = AcquisitonFeedOPDS2.publications(
            self._db, "/", self.fiction, SearchFacets(), self.search_engine, annotator
        )
        result = result.json()

        assert len(result["publications"]) == len(works)
        for ix, pub in enumerate(result["publications"]):
            work = works[ix]
            metadata = pub["metadata"]
            assert (
                metadata["identifier"]
                == work.presentation_edition.primary_identifier.identifier
            )
            assert metadata["title"] == work.presentation_edition.title
            assert metadata["author"] == {"name": work.presentation_edition.author}

            links = sorted(metadata["links"], key=lambda x: x["rel"])
            if ix == last_ix:
                assert metadata["series"] == {"name": "A series"}
                assert metadata["position"] == 1
                assert metadata["modified"] == modified
                assert len(links) == 2
                assert links[1]["href"] == "https://example.org/sample"
                assert links[1]["rel"] == Hyperlink.SAMPLE
                assert links[0]["rel"] == Hyperlink.OPEN_ACCESS_DOWNLOAD
