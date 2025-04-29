from unittest.mock import create_autospec, patch

import pytest
from _pytest.logging import LogCaptureFixture
from fixtures.celery import CeleryFixture
from fixtures.database import DatabaseTransactionFixture

from palace.manager.api.metadata.nyt import NYTBestSellerAPI, NYTBestSellerList
from palace.manager.celery.tasks.nyt import update_nyt_best_sellers_lists


@pytest.mark.parametrize(
    "include_history",
    [
        pytest.param(
            True,
        ),
        pytest.param(
            False,
        ),
    ],
)
def test_update_nyt_best_sellers_lists(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: LogCaptureFixture,
    include_history: bool,
) -> None:
    with patch("palace.manager.celery.tasks.nyt.NYTBestSellerAPI") as nyt_api:
        list_b = {"list_name_encoded": "list b"}
        list_a = {"list_name_encoded": "list a"}
        mock_api = create_autospec(NYTBestSellerAPI)
        mock_api.list_of_lists.return_value = {
            "results": [
                list_b,
                list_a,
            ]
        }
        nyt_api.from_config.return_value = mock_api

        best_seller_list = create_autospec(NYTBestSellerList)
        mock_api.best_seller_list.return_value = best_seller_list

        update_nyt_best_sellers_lists.delay(include_history=include_history).wait()

        assert mock_api.list_of_lists.call_count == 1
        assert mock_api.best_seller_list.call_count == 2
        # verify that best seller lists are retrieved in sorted order
        assert [
            x.args[0]["list_name_encoded"]
            for x in mock_api.best_seller_list.call_args_list
        ] == ["list a", "list b"]
        if include_history:
            assert mock_api.fill_in_history.call_count == 2
            assert mock_api.update.call_count == 0
        else:
            assert mock_api.fill_in_history.call_count == 0
            assert mock_api.update.call_count == 2

        assert best_seller_list.to_customlist.call_count == 2
