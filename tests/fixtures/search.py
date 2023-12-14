import logging
import os
from collections.abc import Iterable

import pytest
from opensearchpy import OpenSearch

from core.external_search import ExternalSearchIndex, SearchIndexCoverageProvider
from core.model import ExternalIntegration, Work
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.search import SearchServiceFake


class ExternalSearchFixture:
    """
    These tests require opensearch to be running locally. If it's not, or there's
    an error creating the index, the tests will pass without doing anything.

    Tests for opensearch are useful for ensuring that we haven't accidentally broken
    a type of search by changing analyzers or queries, but search needs to be tested manually
    to ensure that it works well overall, with a realistic index.
    """

    integration: ExternalIntegration
    db: DatabaseTransactionFixture
    search: OpenSearch
    _indexes_created: list[str]

    def __init__(self):
        self._indexes_created = []
        self._logger = logging.getLogger(ExternalSearchFixture.__name__)

    @classmethod
    def create(cls, db: DatabaseTransactionFixture) -> "ExternalSearchFixture":
        fixture = ExternalSearchFixture()
        fixture.db = db
        fixture.integration = db.external_integration(
            ExternalIntegration.OPENSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
            url=fixture.url,
            settings={
                ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY: "test_index",
                ExternalSearchIndex.TEST_SEARCH_TERM_KEY: "test_search_term",
            },
        )
        fixture.search = OpenSearch(fixture.url, use_ssl=False, timeout=20, maxsize=25)
        return fixture

    @property
    def url(self) -> str:
        env = os.environ.get("SIMPLIFIED_TEST_OPENSEARCH")
        if env is None:
            raise OSError("SIMPLIFIED_TEST_OPENSEARCH is not defined.")
        return env

    def record_index(self, name: str):
        self._logger.info(f"Recording index {name} for deletion")
        self._indexes_created.append(name)

    def close(self):
        for index in self._indexes_created:
            try:
                self._logger.info(f"Deleting index {index}")
                self.search.indices.delete(index)
            except Exception as e:
                self._logger.info(f"Failed to delete index {index}: {e}")

        # Force test index deletion
        self.search.indices.delete("test_index*")
        self._logger.info("Waiting for operations to complete.")
        self.search.indices.refresh()
        return None

    def default_work(self, *args, **kwargs):
        """Convenience method to create a work with a license pool in the default collection."""
        work = self.db.work(
            *args,
            with_license_pool=True,
            collection=self.db.default_collection(),
            **kwargs,
        )
        work.set_presentation_ready()
        return work

    def init_indices(self):
        client = ExternalSearchIndex(self.db.session)
        client.initialize_indices()


@pytest.fixture(scope="function")
def external_search_fixture(
    db: DatabaseTransactionFixture,
) -> Iterable[ExternalSearchFixture]:
    """Ask for an external search system."""
    """Note: You probably want EndToEndSearchFixture instead."""
    data = ExternalSearchFixture.create(db)
    yield data
    data.close()


class EndToEndSearchFixture:
    """An external search system fixture that can be populated with data for end-to-end tests."""

    """Tests are expected to call the `populate()` method to populate the fixture with test-specific data."""
    external_search: ExternalSearchFixture
    external_search_index: ExternalSearchIndex
    db: DatabaseTransactionFixture

    def __init__(self):
        self._logger = logging.getLogger(EndToEndSearchFixture.__name__)

    @classmethod
    def create(cls, transaction: DatabaseTransactionFixture) -> "EndToEndSearchFixture":
        data = EndToEndSearchFixture()
        data.db = transaction
        data.external_search = ExternalSearchFixture.create(transaction)
        data.external_search_index = ExternalSearchIndex(transaction.session)
        return data

    def populate_search_index(self):
        """Populate the search index with a set of works. The given callback is passed this fixture instance."""

        # Create some works.
        if not self.external_search.search:
            # No search index is configured -- nothing to do.
            return

        # Add all the works created in the setup to the search index.
        SearchIndexCoverageProvider(
            self.external_search.db.session,
            search_index_client=self.external_search_index,
        ).run()
        self.external_search.search.indices.refresh()

    @staticmethod
    def assert_works(description, expect, actual, should_be_ordered=True):
        """Verify that two lists of works are the same."""
        # Get the titles of the works that were actually returned, to
        # make comparisons easier.
        actual_ids = []
        actual_titles = []
        for work in actual:
            actual_titles.append(work.title)
            actual_ids.append(work.id)

        expect_ids = []
        expect_titles = []
        for work in expect:
            expect_titles.append(work.title)
            expect_ids.append(work.id)

        # We compare IDs rather than objects because the Works may
        # actually be WorkSearchResults.
        expect_compare = expect_ids
        actual_compare = actual_ids
        if not should_be_ordered:
            expect_compare = set(expect_compare)
            actual_compare = set(actual_compare)

        assert (
            expect_compare == actual_compare
        ), "%r did not find %d works\n (%s/%s).\nInstead found %d\n (%s/%s)" % (
            description,
            len(expect),
            ", ".join(map(str, expect_ids)),
            ", ".join(expect_titles),
            len(actual),
            ", ".join(map(str, actual_ids)),
            ", ".join(actual_titles),
        )

    def expect_results(
        self, expect, query_string=None, filter=None, pagination=None, **kwargs
    ):
        """Helper function to call query_works() and verify that it
        returns certain work IDs.

        :param ordered: If this is True (the default), then the
        assertion will only succeed if the search results come in in
        the exact order specified in `works`. If this is False, then
        those exact results must come up, but their order is not
        what's being tested.
        """
        if isinstance(expect, Work):
            expect = [expect]
        should_be_ordered = kwargs.pop("ordered", True)
        hits = self.external_search_index.query_works(
            query_string, filter, pagination, debug=True, **kwargs
        )

        query_args = (query_string, filter, pagination)
        self._compare_hits(expect, hits, query_args, should_be_ordered, **kwargs)

    def expect_results_multi(self, expect, queries, **kwargs):
        """Helper function to call query_works_multi() and verify that it
        returns certain work IDs.

        :param expect: A list of lists of Works that you expect
            to get back from each query in `queries`.
        :param queries: A list of (query string, Filter, Pagination)
            3-tuples.
        :param ordered: If this is True (the default), then the
           assertion will only succeed if the search results come in
           in the exact order specified in `works`. If this is False,
           then those exact results must come up, but their order is
           not what's being tested.
        """
        should_be_ordered = kwargs.pop("ordered", True)
        resultset = list(
            self.external_search_index.query_works_multi(queries, debug=True, **kwargs)
        )
        for i, expect_one_query in enumerate(expect):
            hits = resultset[i]
            query_args = queries[i]
            self._compare_hits(
                expect_one_query, hits, query_args, should_be_ordered, **kwargs
            )

    def _compare_hits(self, expect, hits, query_args, should_be_ordered=True, **kwargs):
        query_string, filter, pagination = query_args
        results = [x.work_id for x in hits]
        actual = (
            self.external_search.db.session.query(Work)
            .filter(Work.id.in_(results))
            .all()
        )
        if should_be_ordered:
            # Put the Work objects in the same order as the IDs returned
            # in `results`.
            works_by_id = dict()
            for w in actual:
                works_by_id[w.id] = w
            actual = [
                works_by_id[result] for result in results if result in works_by_id
            ]

        query_args = (query_string, filter, pagination)
        self.assert_works(query_args, expect, actual, should_be_ordered)

        if query_string is None and pagination is None and not kwargs:
            # Only a filter was provided -- this means if we pass the
            # filter into count_works() we'll get all the results we
            # got from query_works(). Take the opportunity to verify
            # that count_works() gives the right answer.
            count = self.external_search_index.count_works(filter)
            assert count == len(expect)

    def close(self):
        for index in self.external_search_index.search_service().indexes_created():
            self.external_search.record_index(index)

        self.external_search.close()


@pytest.fixture(scope="function")
def end_to_end_search_fixture(
    db: DatabaseTransactionFixture,
) -> Iterable[EndToEndSearchFixture]:
    """Ask for an external search system that can be populated with data for end-to-end tests."""
    data = EndToEndSearchFixture.create(db)
    try:
        yield data
    except Exception:
        raise
    finally:
        data.close()


class ExternalSearchFixtureFake:
    integration: ExternalIntegration
    db: DatabaseTransactionFixture
    search: SearchServiceFake
    external_search: ExternalSearchIndex


@pytest.fixture(scope="function")
def external_search_fake_fixture(
    db: DatabaseTransactionFixture,
) -> ExternalSearchFixtureFake:
    """Ask for an external search system that can be populated with data for end-to-end tests."""
    data = ExternalSearchFixtureFake()
    data.db = db
    data.integration = db.external_integration(
        ExternalIntegration.OPENSEARCH,
        goal=ExternalIntegration.SEARCH_GOAL,
        url="http://does-not-exist.com/",
        settings={
            ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY: "test_index",
            ExternalSearchIndex.TEST_SEARCH_TERM_KEY: "a search term",
        },
    )
    data.search = SearchServiceFake()
    data.external_search = ExternalSearchIndex(
        _db=db.session, custom_client_service=data.search
    )
    return data
