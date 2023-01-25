from unittest import mock

from core.query.customlist import CustomListQueries
from tests.fixtures.database import DatabaseTransactionFixture


@mock.patch("core.query.customlist.ExternalSearchIndex")
@mock.patch("core.query.customlist.WorkList")
class TestCustomListQueries:
    def test_populate_query_pages_single(
        self, mock_wl, mock_search, db: DatabaseTransactionFixture
    ):
        w1 = db.work()
        mock_wl().search.side_effect = [[w1], []]
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        assert 1 == CustomListQueries.populate_query_pages(db.session, custom_list)
        assert mock_wl().search.call_count == 2
        assert [e.work_id for e in custom_list.entries] == [w1.id]

    def test_populate_query_multi_page(
        self, mock_wl, mock_search, db: DatabaseTransactionFixture
    ):
        w1 = db.work()
        w2 = db.work()
        mock_wl().search.side_effect = [[w1], [w2], []]
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        assert 2 == CustomListQueries.populate_query_pages(db.session, custom_list)
        assert mock_wl().search.call_count == 3
        assert [e.work_id for e in custom_list.entries] == [w1.id, w2.id]

    def test_populate_query_pages(
        self, mock_wl, mock_search, db: DatabaseTransactionFixture
    ):
        w1 = db.work()
        w2 = db.work()
        w3 = db.work()
        mock_wl().search.side_effect = [[w1], [w2], [w3], []]
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_query = "{}"

        assert 1 == CustomListQueries.populate_query_pages(
            db.session, custom_list, max_pages=1, start_page=2, page_size=10
        )
        assert mock_wl().search.call_count == 1
        assert [e.work_id for e in custom_list.entries] == [w1.id]
        assert mock_wl().search.call_args_list[0][1]["pagination"].offset == 10
