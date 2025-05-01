from unittest.mock import patch

import pytest

from palace.manager.scripts.nyt import NYTBestSellerListsScript
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


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
def test_do_run(
    include_history: bool,
    db: DatabaseTransactionFixture,
    services_fixture: ServicesFixture,
):
    with patch("palace.manager.scripts.nyt.update_nyt_best_sellers_lists") as mock:
        command_args = []
        if include_history:
            command_args.append("--include-history")
        NYTBestSellerListsScript(db.session, services_fixture.services).do_run(
            command_args
        )
        mock.delay.assert_called_once_with(include_history=include_history)
