from __future__ import annotations

import datetime
import json
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time
from sqlalchemy.exc import InvalidRequestError

from palace.manager.scripts.customlist import (
    CustomListUpdateEntriesScript,
    UpdateCustomListSizeScript,
)
from palace.manager.search.filter import Filter
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import EndToEndSearchFixture
from tests.fixtures.services import ServicesFixture


class TestUpdateCustomListSizeScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        customlist, ignore = db.customlist(num_entries=1)
        customlist.library = db.default_library()
        customlist.size = 100
        UpdateCustomListSizeScript(db.session).do_run(cmd_args=[])
        assert 1 == customlist.size


class TestCustomListUpdateEntriesScriptData:
    populated_books: list[Work]
    unpopular_books: list[Work]


class TestCustomListUpdateEntriesScript:
    @staticmethod
    def _populate_works(
        data: EndToEndSearchFixture,
    ) -> TestCustomListUpdateEntriesScriptData:
        db = data.external_search.db

        result = TestCustomListUpdateEntriesScriptData()
        result.populated_books = [
            db.work(with_license_pool=True, title="Populated Book") for _ in range(5)
        ]
        result.unpopular_books = [
            db.work(with_license_pool=True, title="Unpopular Book") for _ in range(3)
        ]
        # This is for back population only
        result.populated_books[0].license_pools[0].availability_time = (
            datetime.datetime(1900, 1, 1)
        )
        db.session.commit()
        return result

    def test_process_custom_list(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        fixture = end_to_end_search_fixture
        db, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        last_updated = datetime.datetime.now() - datetime.timedelta(hours=1)
        custom_list, _ = db.customlist()
        custom_list.library = db.default_library()
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Populated Book"))
        )
        custom_list.auto_update_last_update = last_updated
        custom_list.auto_update_status = CustomList.UPDATED

        custom_list1, _ = db.customlist()
        custom_list1.library = db.default_library()
        custom_list1.auto_update_enabled = True
        custom_list1.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Unpopular Book"))
        )
        custom_list1.auto_update_last_update = last_updated
        custom_list1.auto_update_status = CustomList.UPDATED

        # Do the process
        script = CustomListUpdateEntriesScript(session)
        mock_parse = MagicMock()
        mock_parse.return_value.libraries = [db.default_library()]
        script.parse_command_line = mock_parse

        with freeze_time("2022-01-01") as frozen_time:
            script.run()

        session.refresh(custom_list)
        session.refresh(custom_list1)
        assert (
            len(custom_list.entries) == 1 + len(data.populated_books) - 1
        )  # default + new - one past availability time
        assert custom_list.size == 1 + len(data.populated_books) - 1
        assert len(custom_list1.entries) == 1 + len(
            data.unpopular_books
        )  # default + new
        assert custom_list1.size == 1 + len(data.unpopular_books)
        # last updated time has updated correctly
        assert custom_list.auto_update_last_update == frozen_time()
        assert custom_list1.auto_update_last_update == frozen_time()

    def test_search_facets(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        mock_index = services_fixture.search_index

        last_updated = datetime.datetime.now() - datetime.timedelta(hours=1)
        custom_list, _ = db.customlist()
        custom_list.library = db.default_library()
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Populated Book"))
        )
        custom_list.auto_update_facets = json.dumps(
            dict(order="title", languages="fr", media=["book", "audio"])
        )
        custom_list.auto_update_last_update = last_updated

        script = CustomListUpdateEntriesScript(db.session)
        script.process_custom_list(custom_list)

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
        end_to_end_search_fixture: EndToEndSearchFixture,
        frozen_time=None,
    ):
        fixture = end_to_end_search_fixture
        db, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        # No previous timestamp
        custom_list, _ = db.customlist()
        custom_list.library = db.default_library()
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Populated Book"))
        )
        script = CustomListUpdateEntriesScript(session)
        script.process_custom_list(custom_list)
        assert custom_list.auto_update_last_update == frozen_time.time_to_freeze

    def test_init_backpopulates(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        with patch(
            "palace.manager.scripts.customlist.CustomListQueries"
        ) as mock_queries:
            fixture = end_to_end_search_fixture
            db, session = (
                fixture.external_search.db,
                fixture.external_search.db.session,
            )
            data = self._populate_works(fixture)
            fixture.populate_search_index()

            custom_list, _ = db.customlist()
            custom_list.library = db.default_library()
            custom_list.auto_update_enabled = True
            custom_list.auto_update_query = json.dumps(
                dict(query=dict(key="title", value="Populated Book"))
            )
            script = CustomListUpdateEntriesScript(session)
            script.process_custom_list(custom_list)

            args = mock_queries.populate_query_pages.call_args_list[0]
            assert args[1]["json_query"] == None
            assert args[1]["start_page"] == 2
            assert custom_list.auto_update_status == CustomList.UPDATED

    def test_repopulate_state(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        """The repopulate deletes all entries and runs the query again"""
        fixture = end_to_end_search_fixture
        db, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        custom_list, _ = db.customlist()
        custom_list.library = db.default_library()
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Populated Book"))
        )
        custom_list.auto_update_status = CustomList.REPOPULATE

        # Previously the list would have had Unpopular books
        for w in data.unpopular_books:
            custom_list.add_entry(w)
        prev_entry = custom_list.entries[0]

        script = CustomListUpdateEntriesScript(session)
        script.process_custom_list(custom_list)
        # Commit the process changes and refresh the list
        session.commit()
        session.refresh(custom_list)

        # Now the entries are only the Popular books
        assert {e.work_id for e in custom_list.entries} == {
            w.id for w in data.populated_books
        }
        # The previous entries should have been deleted, not just un-related
        with pytest.raises(InvalidRequestError):
            session.refresh(prev_entry)
        assert custom_list.auto_update_status == CustomList.UPDATED
        assert custom_list.size == len(data.populated_books)
