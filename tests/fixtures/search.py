from __future__ import annotations

from collections.abc import Generator

import pytest
from opensearchpy import OpenSearch
from pydantic import AnyHttpUrl

from core.external_search import ExternalSearchIndex
from core.model import Work
from core.search.coverage_provider import SearchIndexCoverageProvider
from core.search.service import SearchServiceOpensearch1
from core.service.configuration import ServiceConfiguration
from core.service.container import Services, wire_container
from core.service.search.container import Search
from core.util.log import LoggerMixin
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.search import SearchServiceFake


class SearchTestConfiguration(ServiceConfiguration):
    url: AnyHttpUrl
    index_prefix: str = "test_index"
    timeout: int = 20
    maxsize: int = 25

    class Config:
        env_prefix = "PALACE_TEST_SEARCH_"


class ExternalSearchFixture(LoggerMixin):
    """
    These tests require opensearch to be running locally.

    Tests for opensearch are useful for ensuring that we haven't accidentally broken
    a type of search by changing analyzers or queries, but search needs to be tested manually
    to ensure that it works well overall, with a realistic index.
    """

    def __init__(self, db: DatabaseTransactionFixture, services: Services):
        self.search_config = SearchTestConfiguration()
        self.services_container = services

        # Set up our testing search instance in the services container
        self.search_container = Search()
        self.search_container.config.from_dict(self.search_config.dict())
        self.services_container.search.override(self.search_container)

        self._indexes_created: list[str] = []
        self.db = db
        self.client: OpenSearch = services.search.client()
        self.service: SearchServiceOpensearch1 = services.search.service()
        self.index: ExternalSearchIndex = services.search.index()
        self._indexes_created = []

        # Make sure the services container is wired up with the newly created search container
        wire_container(self.services_container)

    def record_index(self, name: str):
        self.log.info(f"Recording index {name} for deletion")
        self._indexes_created.append(name)

    def close(self):
        for index in self._indexes_created:
            try:
                self.log.info(f"Deleting index {index}")
                self.client.indices.delete(index)
            except Exception as e:
                self.log.info(f"Failed to delete index {index}: {e}")

        # Force test index deletion
        self.client.indices.delete("test_index*")
        self.log.info("Waiting for operations to complete.")
        self.client.indices.refresh()

        # Unwire the services container
        self.services_container.unwire()
        self.services_container.search.reset_override()
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
        self.index.initialize_indices()


@pytest.fixture(scope="function")
def external_search_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> Generator[ExternalSearchFixture, None, None]:
    """Ask for an external search system."""
    """Note: You probably want EndToEndSearchFixture instead."""
    fixture = ExternalSearchFixture(db, services_fixture.services)
    yield fixture
    fixture.close()


class EndToEndSearchFixture:
    """An external search system fixture that can be populated with data for end-to-end tests."""

    """Tests are expected to call the `populate()` method to populate the fixture with test-specific data."""

    def __init__(self, search_fixture: ExternalSearchFixture):
        self.db = search_fixture.db
        self.external_search = search_fixture
        self.external_search_index = search_fixture.index

    def populate_search_index(self):
        """Populate the search index with a set of works. The given callback is passed this fixture instance."""
        # Add all the works created in the setup to the search index.
        SearchIndexCoverageProvider(
            self.external_search.db.session,
            search_index_client=self.external_search_index,
        ).run()
        self.external_search.client.indices.refresh()

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


@pytest.fixture(scope="function")
def end_to_end_search_fixture(
    external_search_fixture: ExternalSearchFixture,
) -> Generator[EndToEndSearchFixture, None, None]:
    """Ask for an external search system that can be populated with data for end-to-end tests."""
    fixture = EndToEndSearchFixture(external_search_fixture)
    yield fixture
    fixture.close()


class ExternalSearchFixtureFake:
    def __init__(self, db: DatabaseTransactionFixture, services: Services):
        self.db = db
        self.services = services
        self.search_container = Search()
        self.services.search.override(self.search_container)

        self.service = SearchServiceFake()
        self.search_container.service.override(self.service)
        self.external_search: ExternalSearchIndex = self.services.search.index()

        wire_container(self.services)

    def close(self):
        self.services.unwire()
        self.services.search.reset_override()


@pytest.fixture(scope="function")
def external_search_fake_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> Generator[ExternalSearchFixtureFake, None, None]:
    """Ask for an external search system that can be populated with data for end-to-end tests."""
    fixture = ExternalSearchFixtureFake(
        db=db,
        services=services_fixture.services,
    )
    yield fixture
    fixture.close()
