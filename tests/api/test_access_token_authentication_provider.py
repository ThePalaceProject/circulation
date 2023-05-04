from unittest.mock import patch

from api.authentication.patron_authentication_provider import (
    PatronAccessTokenAuthenticationProvider,
)
from api.problem_details import PATRON_AUTH_ACCESS_TOKEN_INVALID
from tests.fixtures.database import DatabaseTransactionFixture


class TestAccessTokenAuthenticationProvider:
    def test_authenticated_patron(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        provider = PatronAccessTokenAuthenticationProvider(db.session)
        with patch(
            "api.authentication.patron_authentication_provider.AccessTokenProvider"
        ) as token_provider:
            token_provider.decode_token.return_value = dict(
                id=patron.id, typ="patron", pwd="password"
            )
            got_patron = provider.authenticated_patron(db.session, "token-string")

            assert got_patron.id == patron.id
            assert got_patron.plaintext_password == "password"
            assert got_patron.patrondata == None

            # Any incorrect data would mean an invalid token
            token_provider.decode_token.return_value = dict(id=patron.id, typ="patron")
            assert PATRON_AUTH_ACCESS_TOKEN_INVALID == provider.authenticated_patron(
                db.session, "token-string"
            )

            token_provider.decode_token.return_value = dict(
                id=patron.id, pwd="password"
            )
            assert PATRON_AUTH_ACCESS_TOKEN_INVALID == provider.authenticated_patron(
                db.session, "token-string"
            )

            token_provider.decode_token.return_value = dict(
                typ="patron", pwd="password"
            )
            assert PATRON_AUTH_ACCESS_TOKEN_INVALID == provider.authenticated_patron(
                db.session, "token-string"
            )

            # Nonexistent patron
            token_provider.decode_token.return_value = dict(
                id=999999999, typ="patron", pwd="password"
            )
            assert None == provider.authenticated_patron(db.session, "token-string")

            # A sirsi type patron
            token_provider.decode_token.return_value = dict(
                id=patron.id, typ="sirsi", pwd="password", session_token="xxx"
            )
            got_patron = provider.authenticated_patron(db.session, "token-string")
            assert got_patron.patrondata is not None
            assert got_patron.patrondata.session_token == "xxx"
