from typing import TYPE_CHECKING

import pytest
from werkzeug.datastructures import Authorization

from api.problem_details import PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture

if TYPE_CHECKING:
    from api.controller.patron_auth_token import PatronAuthTokenController


class PatronAuthTokenControllerFixture(CirculationControllerFixture):
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        super().__init__(db)
        self.controller: PatronAuthTokenController = self.manager.patron_auth_token


@pytest.fixture(scope="function")
def patron_auth_token_fixture(db: DatabaseTransactionFixture):
    return PatronAuthTokenControllerFixture(db)


class TestPatronAuthTokenController:
    def test_get_token(
        self, patron_auth_token_fixture: PatronAuthTokenControllerFixture
    ):
        fxtr = patron_auth_token_fixture
        db = fxtr.db
        patron = db.patron()
        with fxtr.request_context_with_library("/") as ctx:
            ctx.request.authorization = Authorization(
                auth_type="Basic", data=dict(username="user", password="pass")
            )
            ctx.request.patron = patron
            token = fxtr.controller.get_token()
            assert ("accessToken", "tokenType", "expiresIn") == tuple(token.keys())
            assert token["expiresIn"] == 3600
            assert token["tokenType"] == "Bearer"

    def test_get_token_errors(
        self, patron_auth_token_fixture: PatronAuthTokenControllerFixture
    ):
        fxtr = patron_auth_token_fixture
        db = fxtr.db
        patron = db.patron()
        with fxtr.request_context_with_library("/") as ctx:
            ctx.request.authorization = Authorization(
                auth_type="Bearer", token="Some-token"
            )
            ctx.request.patron = patron
            assert fxtr.controller.get_token() == PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE

            ctx.request.authorization = Authorization(
                auth_type="Basic", data=dict(username="user", password="pass")
            )
            ctx.request.patron = None
            assert fxtr.controller.get_token() == PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE
