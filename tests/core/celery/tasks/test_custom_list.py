import datetime
import json
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Literal
from unittest.mock import create_autospec, patch

import pytest
from freezegun import freeze_time
from sqlalchemy.orm import sessionmaker

from core.celery.tasks.custom_list import (
    AutoUpdateCustomListJob,
    update_custom_list,
    update_custom_lists,
)
from core.external_search import ExternalSearchIndex, Filter
from core.model import CustomList, CustomListEntry, Work
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import EndToEndSearchFixture
from tests.fixtures.services import ServicesFixture


@dataclass
class CustomListWorks:
    populated_books: list[Work]
    unpopular_books: list[Work]


class CustomListFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db

    def list(
        self,
        *,
        last_updated: datetime.datetime | None | Literal[False] = False,
        query: dict[str, str] | None = None,
        facets: dict[str, Any] | None = None,
        status: str = CustomList.UPDATED
    ) -> CustomList:
        if last_updated is False:
            last_updated = datetime.datetime.now() - datetime.timedelta(hours=1)

        if query is None:
            query = dict(key="title", value="Populated Book")

        query_json = json.dumps(dict(query=query))
        custom_list, _ = self.db.customlist()
        custom_list.library = self.db.default_library()
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = query_json
        custom_list.auto_update_last_update = last_updated
        custom_list.auto_update_status = status

        if facets is not None:
            custom_list.auto_update_facets = json.dumps(facets)

        return custom_list

    @cached_property
    def works(self) -> CustomListWorks:
        result = CustomListWorks(
            populated_books=[
                self.db.work(with_license_pool=True, title="Populated Book")
                for _ in range(5)
            ],
            unpopular_books=[
                self.db.work(with_license_pool=True, title="Unpopular Book")
                for _ in range(3)
            ],
        )

        # This is for back population only
        result.populated_books[0].license_pools[
            0
        ].availability_time = datetime.datetime(1900, 1, 1)
        return result

    def create_works(self) -> CustomListWorks:
        return self.works


@pytest.fixture
def custom_list_fixture(db: DatabaseTransactionFixture) -> CustomListFixture:
    return CustomListFixture(db)


class TestAutoUpdateCustomListJob:
    def test_process_custom_list(
        self,
        mock_session_maker: sessionmaker,
        end_to_end_search_fixture: EndToEndSearchFixture,
        custom_list_fixture: CustomListFixture,
    ):
        custom_list_fixture.create_works()
        end_to_end_search_fixture.populate_search_index()

        populated_book_custom_list = custom_list_fixture.list()
        unpopular_book_custom_list = custom_list_fixture.list(
            query=dict(key="title", value="Unpopular Book")
        )

        # Do the process
        populated_book_job = AutoUpdateCustomListJob(
            mock_session_maker,
            end_to_end_search_fixture.external_search_index,
            populated_book_custom_list.id,
        )
        unpopular_book_job = AutoUpdateCustomListJob(
            mock_session_maker,
            end_to_end_search_fixture.external_search_index,
            unpopular_book_custom_list.id,
        )
        with freeze_time("2022-01-01") as frozen_time:
            populated_book_job.run()
            unpopular_book_job.run()

        assert (
            len(populated_book_custom_list.entries)
            == 1 + len(custom_list_fixture.works.populated_books) - 1
        )  # default + new - one past availability time
        assert (
            populated_book_custom_list.size
            == 1 + len(custom_list_fixture.works.populated_books) - 1
        )
        assert len(unpopular_book_custom_list.entries) == 1 + len(
            custom_list_fixture.works.unpopular_books
        )  # default + new
        assert unpopular_book_custom_list.size == 1 + len(
            custom_list_fixture.works.unpopular_books
        )
        # last updated time has updated correctly
        assert populated_book_custom_list.auto_update_last_update == frozen_time()
        assert unpopular_book_custom_list.auto_update_last_update == frozen_time()

    def test_search_facets(
        self,
        db: DatabaseTransactionFixture,
        mock_session_maker: sessionmaker,
        custom_list_fixture: CustomListFixture,
    ):
        mock_index = create_autospec(ExternalSearchIndex)
        custom_list = custom_list_fixture.list(
            facets=dict(order="title", languages="fr", media=["book", "audio"])
        )

        AutoUpdateCustomListJob(mock_session_maker, mock_index, custom_list.id).run()

        assert mock_index.query_works.call_count == 1
        filter: Filter = mock_index.query_works.call_args_list[0][0][1]
        assert filter.sort_order[0] == {
            "sort_title": "asc"
        }  # since we asked for title ordering this should come up first
        assert filter.languages == ["fr"]
        assert filter.media == ["book", "audio"]

    @freeze_time("2022-01-01", as_kwarg="frozen_time")
    def test_no_last_update(
        self,
        mock_session_maker: sessionmaker,
        custom_list_fixture: CustomListFixture,
        frozen_time=None,
    ):
        mock_index = create_autospec(ExternalSearchIndex)

        # No previous timestamp
        custom_list = custom_list_fixture.list(last_updated=None)
        AutoUpdateCustomListJob(mock_session_maker, mock_index, custom_list.id).run()
        assert custom_list.auto_update_last_update == frozen_time.time_to_freeze

    def test_init_backpopulates(
        self,
        db: DatabaseTransactionFixture,
        mock_session_maker: sessionmaker,
        custom_list_fixture: CustomListFixture,
    ):
        mock_index = create_autospec(ExternalSearchIndex)

        custom_list = custom_list_fixture.list(status=CustomList.INIT)
        with patch("core.celery.tasks.custom_list.CustomListQueries") as mock_queries:
            AutoUpdateCustomListJob(
                mock_session_maker, mock_index, custom_list.id
            ).run()

        args = mock_queries.populate_query_pages.call_args.kwargs
        assert args["json_query"] is None
        assert args["start_page"] == 2
        assert custom_list.auto_update_status == CustomList.UPDATED

    def test_repopulate_state(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        custom_list_fixture: CustomListFixture,
        db: DatabaseTransactionFixture,
        mock_session_maker: sessionmaker,
    ):
        """The repopulate deletes all entries and runs the query again"""
        custom_list_fixture.create_works()
        end_to_end_search_fixture.populate_search_index()
        custom_list = custom_list_fixture.list(status=CustomList.REPOPULATE)
        # Previously the list would have had Unpopular books
        for w in custom_list_fixture.works.unpopular_books:
            custom_list.add_entry(w)
        prev_entry_ids = {e.id for e in custom_list.entries if e.id is not None}

        AutoUpdateCustomListJob(
            mock_session_maker,
            end_to_end_search_fixture.external_search_index,
            custom_list.id,
        ).run()

        # Now the entries are only the Popular books
        assert {e.work_id for e in custom_list.entries} == {
            w.id for w in custom_list_fixture.works.populated_books
        }

        # The previous entries should have been deleted, not just un-related
        for entry_id in prev_entry_ids:
            assert db.session.query(CustomListEntry).get(entry_id) is None

        assert custom_list.auto_update_status == CustomList.UPDATED
        assert custom_list.size == len(custom_list_fixture.works.populated_books)


def test_update_custom_list(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    services_fixture: ServicesFixture,
):
    custom_list, _ = db.customlist()

    with patch("core.celery.tasks.custom_list.AutoUpdateCustomListJob") as mock_job:
        update_custom_list.delay(custom_list.id).wait()

    mock_job.assert_called_once_with(
        celery_fixture.session_maker,
        services_fixture.services.search.index(),
        custom_list_id=custom_list.id,
    )


def test_update_custom_lists(
    celery_fixture: CeleryFixture,
    custom_list_fixture: CustomListFixture,
):
    custom_lists = [custom_list_fixture.list() for _ in range(4)]
    custom_list_ids = [cl.id for cl in custom_lists if cl.id is not None]

    with patch("core.celery.tasks.custom_list.update_custom_list") as mock_update:
        update_custom_lists.delay().wait()

    assert mock_update.delay.call_count == len(custom_list_ids)
    for custom_list_id in custom_list_ids:
        mock_update.delay.assert_any_call(custom_list_id)
