from unittest.mock import Mock, patch

from werkzeug.datastructures import Authorization

from api.authentication.access_token import AccessTokenProvider
from api.authentication.basic import BasicAuthenticationProvider
from api.authentication.basic_token import BasicTokenAuthenticationProvider
from api.problem_details import PATRON_AUTH_ACCESS_TOKEN_INVALID
from core.model.patron import Patron
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestBasicTokenAuthenticationProvider:
    def test_authenticated_patron(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        provider = BasicTokenAuthenticationProvider(
            db.session, db.default_library(), Mock()
        )
        with patch(
            "api.authentication.basic_token.AccessTokenProvider"
        ) as token_provider:
            token_provider.decode_token.return_value = dict(
                id=patron.id, pwd="password"
            )
            got_patron = provider.authenticated_patron(db.session, "token-string")

            assert type(got_patron) is Patron
            assert got_patron.id == patron.id

            # Any incorrect data would mean an invalid token
            token_provider.decode_token.return_value = dict(id=patron.id, typ="patron")
            assert PATRON_AUTH_ACCESS_TOKEN_INVALID == provider.authenticated_patron(
                db.session, "token-string"
            )

            token_provider.decode_token.return_value = dict(pwd="password")
            assert PATRON_AUTH_ACCESS_TOKEN_INVALID == provider.authenticated_patron(
                db.session, "token-string"
            )

            # Nonexistent patron
            token_provider.decode_token.return_value = dict(
                id=999999999, pwd="password"
            )
            assert None == provider.authenticated_patron(db.session, "token-string")

    def test_authenticated_patron_errors(self, db: DatabaseTransactionFixture):
        provider = BasicTokenAuthenticationProvider(
            db.session, db.default_library(), Mock()
        )

        # Bad token type
        error = provider.authenticated_patron(db.session, {})
        assert error == None

        error = provider.authenticated_patron(db.session, "some-token")
        assert error == PATRON_AUTH_ACCESS_TOKEN_INVALID

    def test_credential_from_header(self, db: DatabaseTransactionFixture):
        provider = BasicTokenAuthenticationProvider(
            db.session, db.default_library(), Mock()
        )
        patron = db.patron()
        token = AccessTokenProvider.generate_token(db.session, patron, "passworx")

        pwd = provider.get_credential_from_header(
            Authorization(auth_type="Bearer", token=token)
        )
        assert pwd == "passworx"

        pwd = provider.get_credential_from_header(Authorization(auth_type="Basic"))
        assert pwd == None

    def test_authentication_flow_document(
        self, db: DatabaseTransactionFixture, controller_fixture: ControllerFixture
    ):
        provider = BasicTokenAuthenticationProvider(
            db.session, db.default_library(), Mock(spec=BasicAuthenticationProvider)
        )
        provider.basic_provider._authentication_flow_document.return_value = dict(  # type: ignore[attr-defined]
            mock=True
        )
        with controller_fixture.request_context_with_library(
            "/", library=db.default_library()
        ):
            auth_doc = provider.authentication_flow_document(db.session)

        # From the basic provider
        assert auth_doc["mock"] == True
        # Overrides in the token provider
        assert auth_doc["links"][0]["href"].endswith("/default/patrons/me/token/")
        assert auth_doc["type"] == provider.flow_type
        assert auth_doc["description"] == provider.label()
