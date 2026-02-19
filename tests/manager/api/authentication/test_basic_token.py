from unittest.mock import Mock, patch

from werkzeug.datastructures import Authorization

from palace.manager.api.authentication.access_token import (
    PatronJWEAccessTokenProvider,
    TokenPatronInfo,
)
from palace.manager.api.authentication.basic import BasicAuthenticationProvider
from palace.manager.api.authentication.basic_token import (
    BasicTokenAuthenticationProvider,
)
from palace.manager.api.problem_details import PATRON_AUTH_ACCESS_TOKEN_INVALID
from palace.manager.opds.authentication import AuthenticationLabels
from palace.manager.opds.palace_authentication import (
    AuthenticationInputs,
    InputDescriptor,
    PalaceAuthentication,
)
from palace.manager.sqlalchemy.model.patron import Patron
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestBasicTokenAuthenticationProvider:
    def test_authenticated_patron(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        provider = BasicTokenAuthenticationProvider(
            db.session, db.default_library(), Mock()
        )
        with patch(
            "palace.manager.api.authentication.basic_token.PatronJWEAccessTokenProvider"
        ) as token_provider:
            assert isinstance(patron.id, int)
            token_provider.decrypt_token.return_value = TokenPatronInfo(
                id=patron.id, pwd="password"
            )
            got_patron = provider.authenticated_patron(db.session, "token-string")

            assert isinstance(got_patron, Patron)
            assert got_patron.id == patron.id

            # Nonexistent patron
            token_provider.decrypt_token.return_value = TokenPatronInfo(
                id=999999999, pwd="password"
            )
            assert provider.authenticated_patron(db.session, "token-string") is None

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
        token = PatronJWEAccessTokenProvider.generate_token(
            db.session, patron, "passworx"
        )

        assert (
            provider.get_credential_from_header(
                Authorization(auth_type="Bearer", token=token)
            )
            == "passworx"
        )

        assert (
            provider.get_credential_from_header(Authorization(auth_type="Basic"))
            is None
        )
        assert (
            provider.get_credential_from_header(
                Authorization(auth_type="Bearer", token="junk")
            )
            is None
        )

    def test_authentication_flow_document(
        self, db: DatabaseTransactionFixture, controller_fixture: ControllerFixture
    ):
        provider = BasicTokenAuthenticationProvider(
            db.session, db.default_library(), Mock(spec=BasicAuthenticationProvider)
        )
        basic_flow = PalaceAuthentication(
            type="http://opds-spec.org/auth/basic",
            description="Mock Basic",
            labels=AuthenticationLabels(login="Barcode", password="PIN"),
            inputs=AuthenticationInputs(
                login=InputDescriptor(keyboard="Default"),
                password=InputDescriptor(keyboard="Default"),
            ),
        )
        provider.basic_provider._authentication_flow_document.return_value = basic_flow  # type: ignore[attr-defined]
        with controller_fixture.request_context_with_library(
            "/", library=db.default_library()
        ):
            auth_doc = provider.authentication_flow_document(db.session)

        # Labels and inputs should come from the basic provider
        assert auth_doc.labels is not None
        assert auth_doc.labels.login == "Barcode"
        assert auth_doc.inputs is not None
        assert auth_doc.inputs.login.keyboard == "Default"
        # Overrides in the token provider
        assert len(auth_doc.links) == 1
        assert auth_doc.links[0].href.endswith("/default/patrons/me/token/")
        assert auth_doc.type == provider.flow_type
        assert auth_doc.description == provider.label()
