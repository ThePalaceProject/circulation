from unittest import mock
from unittest.mock import create_autospec

from palace.manager.core.query.customlist import CustomListQueries
from palace.manager.search.external_search import ExternalSearchIndex
from tests.fixtures.database import DatabaseTransactionFixture


def page_count_property_mock(mock_page: mock.MagicMock):
    """The next_page property should return the same pagination mock"""
    next_page = mock.PropertyMock()

    def same_page():
        return mock_page.return_value

    next_page.side_effect = same_page
    type(mock_page.return_value).next_page = next_page
    return next_page


@mock.patch("palace.manager.core.query.customlist.SortKeyPagination")
@mock.patch("palace.manager.core.query.customlist.WorkList")
class TestCustomListQueries:
    def test_populate_query_pages_no_auto_update_query_returns_early(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        """When auto_update_query is not set, returns (0, None) immediately."""
        mock_search = create_autospec(ExternalSearchIndex)
        custom_list, _ = db.customlist(num_entries=0)
        # auto_update_query is None by default on a fresh custom list

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list
        )

        assert count == 0
        assert next_key is None
        mock_wl().search.assert_not_called()

    def test_populate_query_pages_explicit_json_query_skips_parse(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        """When json_query is provided directly it is used as-is, bypassing
        the parse of auto_update_query."""
        mock_search = create_autospec(ExternalSearchIndex)
        w1 = db.work()
        mock_wl().search.side_effect = [[w1], []]
        custom_list, _ = db.customlist(num_entries=0)
        # Set auto_update_query to something different to prove it is ignored.
        custom_list.auto_update_query = '{"should": "be ignored"}'
        explicit_query = {"query": {"key": "title", "op": "eq", "value": "Test"}}

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list, json_query=explicit_query
        )

        assert count == 1
        assert next_key is None
        # Confirm the explicit query was forwarded to the search call.
        call_args = mock_wl().search.call_args_list[0]
        assert call_args.args[1] == explicit_query

    def test_populate_query_pages_single(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        mock_search = create_autospec(ExternalSearchIndex)
        w1 = db.work()
        mock_wl().search.side_effect = [[w1], []]
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list
        )
        assert count == 1
        assert next_key is None
        assert mock_wl().search.call_count == 2
        assert [e.work_id for e in custom_list.entries] == [w1.id]

    def test_populate_query_multi_page(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        mock_search = create_autospec(ExternalSearchIndex)
        w1 = db.work()
        w2 = db.work()
        mock_wl().search.side_effect = [[w1], [w2], []]
        next_page = page_count_property_mock(mock_page)

        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list
        )
        assert count == 2
        assert next_key is None
        assert mock_wl().search.call_count == 3
        assert next_page.call_count == 2
        assert [e.work_id for e in custom_list.entries] == [w1.id, w2.id]

    def test_populate_query_pages(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        mock_search = create_autospec(ExternalSearchIndex)
        w1 = db.work()
        w2 = db.work()
        w3 = db.work()
        next_page = page_count_property_mock(mock_page)
        mock_wl().search.side_effect = [[w1], [w2], [w3], []]
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        count, _ = CustomListQueries.populate_query_pages(
            db.session,
            mock_search,
            custom_list,
            max_pages=1,
            start_page=2,
            page_size=10,
        )
        # The search will be paged through from 0, but only the 2nd page onwards should be populated
        assert count == 1
        assert mock_wl().search.call_count == 2
        assert next_page.call_count == 2
        assert [e.work_id for e in custom_list.entries] == [w2.id]

    def test_populate_query_pages_resumes_from_pagination_key(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        """When pagination_key is provided, SortKeyPagination is initialised with
        that cursor and start_page is ignored (no fast-forward search calls)."""
        mock_search = create_autospec(ExternalSearchIndex)
        w1 = db.work()
        mock_wl().search.side_effect = [[w1], []]
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"
        pagination_key = ["sort_key_value", 42]

        count, next_key = CustomListQueries.populate_query_pages(
            db.session,
            mock_search,
            custom_list,
            pagination_key=pagination_key,
        )

        assert count == 1
        assert next_key is None
        assert [e.work_id for e in custom_list.entries] == [w1.id]
        # SortKeyPagination must have been constructed with the cursor, not a bare size.
        mock_page.assert_called_with(
            last_item_on_previous_page=pagination_key, size=100
        )

    def test_populate_query_pages_fast_forward_empty_returns_early(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        """If a fast-forward search returns no results, the function returns
        early with (0, None) after writing metadata."""
        mock_search = create_autospec(ExternalSearchIndex)
        mock_wl().search.side_effect = [[]]  # fast-forward page is empty
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list, start_page=2
        )

        assert count == 0
        assert next_key is None
        assert mock_wl().search.call_count == 1  # only the fast-forward call
        # Metadata should have been updated (update_metadata defaults to True)
        assert custom_list.auto_update_last_update is not None

    def test_populate_query_pages_fast_forward_no_next_page_returns_early(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        """If fast-forward pagination has no next page, the function returns
        early with (0, None) after writing metadata."""
        mock_search = create_autospec(ExternalSearchIndex)
        w1 = db.work()
        mock_wl().search.side_effect = [[w1]]
        # Make next_page return None so the fast-forward early-exit triggers.
        mock_page.return_value.next_page = None
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list, start_page=2
        )

        assert count == 0
        assert next_key is None
        assert mock_wl().search.call_count == 1  # only the fast-forward call
        assert custom_list.auto_update_last_update is not None

    def test_populate_query_pages_main_loop_no_next_page_stops(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        """When the search returns results but next_page is None, the main loop
        breaks and returns the entries found so far (no cursor)."""
        mock_search = create_autospec(ExternalSearchIndex)
        w1 = db.work()
        mock_wl().search.side_effect = [[w1]]
        mock_page.return_value.next_page = None
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list
        )

        assert count == 1
        assert next_key is None
        assert [e.work_id for e in custom_list.entries] == [w1.id]

    def test_populate_query_pages_fast_forward_empty_no_metadata_update(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        """When update_metadata=False and fast-forward hits empty results,
        metadata is not written before the early return."""
        mock_search = create_autospec(ExternalSearchIndex)
        mock_wl().search.side_effect = [[]]
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list, start_page=2, update_metadata=False
        )

        assert count == 0
        assert next_key is None
        assert custom_list.auto_update_last_update is None  # not written

    def test_populate_query_pages_fast_forward_no_next_page_no_metadata_update(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        """When update_metadata=False and fast-forward hits next_page=None,
        metadata is not written before the early return."""
        mock_search = create_autospec(ExternalSearchIndex)
        w1 = db.work()
        mock_wl().search.side_effect = [[w1]]
        mock_page.return_value.next_page = None
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list, start_page=2, update_metadata=False
        )

        assert count == 0
        assert next_key is None
        assert custom_list.auto_update_last_update is None  # not written

    def test_populate_query_pages_update_metadata_false(
        self, mock_wl, mock_page, db: DatabaseTransactionFixture
    ):
        """When update_metadata=False, auto_update_last_update and size are
        not written, even on a successful run."""
        mock_search = create_autospec(ExternalSearchIndex)
        w1 = db.work()
        mock_wl().search.side_effect = [[w1], []]
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        count, next_key = CustomListQueries.populate_query_pages(
            db.session, mock_search, custom_list, update_metadata=False
        )

        assert count == 1
        assert next_key is None
        assert [e.work_id for e in custom_list.entries] == [w1.id]
        # Metadata must NOT have been updated.
        assert custom_list.auto_update_last_update is None
