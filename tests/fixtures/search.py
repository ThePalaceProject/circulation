from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import pytest
from opensearchpy import OpenSearch
from pydantic_settings import SettingsConfigDict

from palace.manager.celery.tasks.search import get_work_search_documents
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.search.revision import SearchSchemaRevision
from palace.manager.search.service import SearchServiceOpensearch1
from palace.manager.service.container import Services
from palace.manager.service.search.container import Search
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.log import LoggerMixin
from palace.manager.util.pydantic import HttpUrl
from tests.fixtures.config import FixtureTestUrlConfiguration
from tests.fixtures.database import DatabaseTransactionFixture, TestIdFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.search import SearchServiceFake


class SearchTestConfiguration(FixtureTestUrlConfiguration):
    url: HttpUrl
    timeout: int = 20
    maxsize: int = 25
    model_config = SettingsConfigDict(env_prefix="PALACE_TEST_SEARCH_")


class ExternalSearchFixture(LoggerMixin):
    """
    These tests require opensearch to be running locally.

    Tests for opensearch are useful for ensuring that we haven't accidentally broken
    a type of search by changing analyzers or queries, but search needs to be tested manually
    to ensure that it works well overall, with a realistic index.
    """

    def __init__(
        self, db: DatabaseTransactionFixture, services: Services, test_id: TestIdFixture
    ):
        self.search_config = SearchTestConfiguration.from_env()
        self.index_prefix = test_id.id

        # Set up our testing search instance in the services container
        self.search_container = Search()
        search_config_dict = self.search_config.model_dump()
        search_config_dict["index_prefix"] = self.index_prefix
        self.search_container.config.from_dict(search_config_dict)
        services.search.override(self.search_container)

        self.db = db
        self.client: OpenSearch = services.search().client()
        self.service: SearchServiceOpensearch1 = services.search().service()
        self.index: ExternalSearchIndex = services.search().index()
        self.revision: SearchSchemaRevision = (
            services.search().revision_directory().highest()
        )

    def close(self):
        # Delete our index prefix
        self.client.indices.delete(index=f"{self.index_prefix}*")
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

    @classmethod
    @contextmanager
    def fixture(
        cls, db: DatabaseTransactionFixture, services: Services, test_id: TestIdFixture
    ):
        fixture = cls(db, services, test_id)
        try:
            yield fixture
        finally:
            fixture.close()


@pytest.fixture(scope="function")
def external_search_fixture(
    db: DatabaseTransactionFixture,
    services_fixture: ServicesFixture,
    function_test_id: TestIdFixture,
) -> Generator[ExternalSearchFixture]:
    """Ask for an external search system."""
    """Note: You probably want EndToEndSearchFixture instead."""
    with ExternalSearchFixture.fixture(
        db, services_fixture.services, function_test_id
    ) as fixture:
        yield fixture


class EndToEndSearchFixture:
    """An external search system fixture that can be populated with data for end-to-end tests."""

    """Tests are expected to call the `populate()` method to populate the fixture with test-specific data."""

    def __init__(self, search_fixture: ExternalSearchFixture):
        self.db = search_fixture.db
        self.external_search = search_fixture
        self.external_search_index = search_fixture.index

        # Set up the search indices and mapping
        self.external_search.service.index_create(self.external_search.revision)
        self.external_search.service.index_set_mapping(self.external_search.revision)
        self.external_search.service.write_pointer_set(self.external_search.revision)
        self.external_search.service.read_pointer_set(self.external_search.revision)

    def populate_search_index(self):
        """Populate the search index with a set of works. The given callback is passed this fixture instance."""
        # Add all the works created in the setup to the search index.
        documents = get_work_search_documents(self.db.session, 1000, 0)
        self.external_search_index.add_documents(documents)
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

    @classmethod
    @contextmanager
    def fixture(cls, search_fixture: ExternalSearchFixture):
        fixture = cls(search_fixture)
        yield fixture


@pytest.fixture(scope="function")
def end_to_end_search_fixture(
    external_search_fixture: ExternalSearchFixture,
) -> Generator[EndToEndSearchFixture]:
    """Ask for an external search system that can be populated with data for end-to-end tests."""
    with EndToEndSearchFixture.fixture(external_search_fixture) as fixture:
        yield fixture


class ExternalSearchFixtureFake:
    def __init__(self, db: DatabaseTransactionFixture, services: Services):
        self.db = db
        self.search_container = Search()
        services.search.override(self.search_container)

        self.service = SearchServiceFake()
        self.search_container.service.override(self.service)
        self.external_search: ExternalSearchIndex = services.search().index()

    @classmethod
    @contextmanager
    def fixture(cls, db: DatabaseTransactionFixture, services: Services):
        fixture = cls(db, services)
        yield fixture


@pytest.fixture(scope="function")
def external_search_fake_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> Generator[ExternalSearchFixtureFake]:
    """Ask for an external search system that can be populated with data for end-to-end tests."""
    with ExternalSearchFixtureFake.fixture(db, services_fixture.services) as fixture:
        yield fixture


class WorkQueueIndexingFixture:
    """
    In normal operation, when external_index_needs_updating is called on Work, it
    adds the Work's ID to a set in Redis. This set is then used to determine which
    Works need to be indexed in the search index.

    For testing, we mock this out to just use a Python set. This allows us to
    check whether a Work is queued for indexing without actually needing to
    interact with Redis.
    """

    def __init__(self):
        self.queued_works = set()
        self.patch = patch.object(Work, "queue_indexing", self.queue)

    def queue(self, work_id: int | None, *, redis_client: Any = None) -> None:
        return self.queued_works.add(work_id)

    def clear(self):
        self.queued_works.clear()

    def disable_fixture(self):
        self.patch.stop()

    def is_queued(self, work: int | Work, *, clear: bool = False) -> bool:
        if isinstance(work, Work):
            work_id = work.id
        else:
            work_id = work
        queued = work_id in self.queued_works

        if clear:
            self.clear()

        return queued

    @classmethod
    @contextmanager
    def fixture(cls):
        fixture = cls()
        fixture.patch.start()
        try:
            yield fixture
        finally:
            fixture.patch.stop()


@pytest.fixture(scope="function")
def work_queue_indexing() -> Generator[WorkQueueIndexingFixture]:
    with WorkQueueIndexingFixture.fixture() as fixture:
        yield fixture
