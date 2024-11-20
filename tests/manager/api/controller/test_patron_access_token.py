from unittest.mock import MagicMock

import pytest
from werkzeug.datastructures import Authorization

from palace.manager.api.controller.patron_auth_token import PatronAuthTokenController
from palace.manager.api.problem_details import PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class PatronAuthTokenControllerFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        mock_manager = MagicMock()
        mock_manager._db = db.session
        self.controller = PatronAuthTokenController(mock_manager)


@pytest.fixture(scope="function")
def patron_auth_token_fixture(db: DatabaseTransactionFixture):
    return PatronAuthTokenControllerFixture(db)


class TestPatronAuthTokenController:
    def test_get_token(
        self,
        patron_auth_token_fixture: PatronAuthTokenControllerFixture,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        patron = db.patron()
        with flask_app_fixture.test_request_context(
            auth=Authorization(
                auth_type="basic", data=dict(username="user", password="pass")
            ),
            patron=patron,
        ):
            token = patron_auth_token_fixture.controller.get_token()
            assert ("accessToken", "tokenType", "expiresIn") == tuple(token.keys())
            assert token["expiresIn"] == 3600
            assert token["tokenType"] == "Bearer"

    def test_get_token_errors(
        self,
        patron_auth_token_fixture: PatronAuthTokenControllerFixture,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        patron = db.patron()
        with flask_app_fixture.test_request_context(
            patron=patron, auth=Authorization(auth_type="bearer", token="Some-token")
        ):
            assert (
                patron_auth_token_fixture.controller.get_token()
                == PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE
            )

        with flask_app_fixture.test_request_context(patron=patron):
            assert (
                patron_auth_token_fixture.controller.get_token()
                == PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE
            )

        with flask_app_fixture.test_request_context(
            auth=Authorization(
                auth_type="basic", data=dict(username="user", password="pass")
            )
        ):
            assert (
                patron_auth_token_fixture.controller.get_token()
                == PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE
            )
