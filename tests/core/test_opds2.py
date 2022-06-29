from datetime import datetime

from core.classifier import Classifier
from core.external_search import MockExternalSearchIndex
from core.lane import SearchFacets
from core.opds2 import AcquisitonFeedOPDS2
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
            self._db, self.fiction, SearchFacets(), self.search_engine
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

        self.search_engine.bulk_update(works)
        result = AcquisitonFeedOPDS2.publications(
            self._db, self.fiction, SearchFacets(), self.search_engine
        )
        result = result.json()

        assert len(result["publications"]) == len(works)
        for ix, pub in enumerate(result["publications"]):
            work = works[ix]
            metadata = pub["metadata"]
            assert metadata["collection"]["name"] == self._default_collection.name
            assert (
                metadata["identifier"]
                == work.presentation_edition.primary_identifier.identifier
            )
            assert metadata["title"] == work.presentation_edition.title

            if ix == last_ix:
                assert metadata["series"] == {"name": "A series"}
                assert metadata["position"] == 1
                assert metadata["modified"] == modified
