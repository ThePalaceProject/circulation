from unittest.mock import patch

import pytest

from palace.manager.scripts.nyt import NYTBestSellerListsScript


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
def test_do_run(include_history: bool):
    with patch("palace.manager.scripts.nyt.update_nyt_best_sellers_lists") as mock:
        NYTBestSellerListsScript(include_history=include_history).do_run()
        mock.delay.assert_called_once_with(include_history=include_history)
