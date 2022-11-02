import logging
import os
import time
from typing import Any, Callable, Iterable, List, Optional
from unittest import mock

import pytest

from core import external_search
from core.external_search import (
    ExternalSearchIndex,
    MockExternalSearchIndex,
    SearchIndexCoverageProvider,
)
from core.model import ExternalIntegration, Work
from core.testing import SearchClientForTesting
from tests.fixtures.database import DatabaseTransactionFixture


class ExternalSearchFixture:
    """
    These tests require elasticsearch to be running locally. If it's not, or there's
    an error creating the index, the tests will pass without doing anything.

    Tests for elasticsearch are useful for ensuring that we haven't accidentally broken
    a type of search by changing analyzers or queries, but search needs to be tested manually
    to ensure that it works well overall, with a realistic index.
    """

    SIMPLIFIED_TEST_ELASTICSEARCH = os.environ.get(
        "SIMPLIFIED_TEST_ELASTICSEARCH", "http://localhost:9200"
    )

    indexes: List[Any]
    integration: ExternalIntegration
    search: Optional[SearchClientForTesting]
    transaction: DatabaseTransactionFixture

    @classmethod
    def create(cls, data: DatabaseTransactionFixture) -> "ExternalSearchFixture":
        fixture = ExternalSearchFixture()
        fixture.transaction = data
        fixture.indexes = []
        fixture.integration = data.external_integration(
            ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
            url=ExternalSearchFixture.SIMPLIFIED_TEST_ELASTICSEARCH,
            settings={
                ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY: "test_index",
                ExternalSearchIndex.TEST_SEARCH_TERM_KEY: "test_search_term",
            },
        )

        try:
            fixture.search = SearchClientForTesting(data.session())
        except Exception as e:
            fixture.search = None
            logging.error(
                "Unable to set up elasticsearch index, search tests will be skipped.",
                exc_info=e,
            )
        return fixture

    def close(self):
        if self.search:
            # Delete the works_index, which is almost always created.
            if self.search.works_index:
                self.search.indices.delete(self.search.works_index, ignore=[404])
            # Delete any other indexes created over the course of the test.
            for index in self.indexes:
                self.search.indices.delete(index, ignore=[404])
            ExternalSearchIndex.reset()

    def setup_index(self, new_index):
        """Create an index and register it to be destroyed during teardown."""
        self.search.setup_index(new_index=new_index)
        self.indexes.append(new_index)

    def default_work(self, *args, **kwargs):
        """Convenience method to create a work with a license pool in the default collection."""
        work = self.transaction.work(
            *args,
            with_license_pool=True,
            collection=self.transaction.default_collection(),
            **kwargs
        )
        work.set_presentation_ready()
        return work


@pytest.fixture
def external_search_fixture(
    database_transaction: DatabaseTransactionFixture,
) -> Iterable[ExternalSearchFixture]:
    """Ask for an external search system."""
    """Note: You probably want EndToEndSearchFixture instead."""
    data = ExternalSearchFixture.create(database_transaction)
    yield data
    data.close()


class EndToEndSearchFixture:
    external_search: ExternalSearchFixture

    @classmethod
    def create(cls, data: DatabaseTransactionFixture) -> "EndToEndSearchFixture":
        fixture = EndToEndSearchFixture()
        fixture.external_search = ExternalSearchFixture.create(data)
        return fixture

    def populate(self, callback: Callable[["EndToEndSearchFixture"], Any]):
        # Create some works.
        if not self.external_search.search:
            # No search index is configured -- nothing to do.
            return

        callback(self)

        # Add all the works created in the setup to the search index.
        SearchIndexCoverageProvider(
            self.external_search.transaction.session(),
            search_index_client=self.external_search.search,
        ).run_once_and_update_timestamp()

        # Sleep to give the index time to catch up.
        time.sleep(2)

    def _assert_works(self, description, expect, actual, should_be_ordered=True):
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

    def _expect_results(
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
        hits = self.external_search.search.query_works(
            query_string, filter, pagination, debug=True, **kwargs
        )

        query_args = (query_string, filter, pagination)
        self._compare_hits(expect, hits, query_args, should_be_ordered, **kwargs)

    def _expect_results_multi(self, expect, queries, **kwargs):
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
            self.external_search.search.query_works_multi(queries, debug=True, **kwargs)
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
            self.external_search.transaction.session()
            .query(Work)
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
        self._assert_works(query_args, expect, actual, should_be_ordered)

        if query_string is None and pagination is None and not kwargs:
            # Only a filter was provided -- this means if we pass the
            # filter into count_works() we'll get all the results we
            # got from query_works(). Take the opportunity to verify
            # that count_works() gives the right answer.
            count = self.external_search.search.count_works(filter)
            assert count == len(expect)

    def close(self):
        self.external_search.close()


@pytest.fixture
def end_to_end_search_fixture(
    database_transaction: DatabaseTransactionFixture,
) -> Iterable[EndToEndSearchFixture]:
    """Ask for an external search system that can be populated with data for end-to-end tests."""
    data = EndToEndSearchFixture.create(database_transaction)
    yield data
    data.close()


class ExternalSearchPatchFixture:
    """A class that represents the fact that the external search class has been patched with a mock."""


@pytest.fixture
def external_search_patch_fixture(request) -> Iterable[ExternalSearchPatchFixture]:
    """Ask for the external search class to be patched with a mock."""
    fixture = ExternalSearchPatchFixture()

    # Only setup the elasticsearch mock if the elasticsearch mark isn't set
    elasticsearch_mark = request.node.get_closest_marker("elasticsearch")
    if elasticsearch_mark is not None:
        raise RuntimeError(
            "This fixture should not be combined with @pytest.mark.elasticsearch"
        )

    fixture.search_mock = mock.patch(
        external_search.__name__ + ".ExternalSearchIndex",
        MockExternalSearchIndex,
    )
    fixture.search_mock.start()

    yield fixture

    if fixture.search_mock:
        fixture.search_mock.stop()
