from typing import Optional

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.controller.patron import PatronController
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import NO_SUCH_PATRON
from api.adobe_vendor_id import AuthdataUtility
from api.authentication.base import PatronData
from core.model import AdminRole
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture


class PatronControllerFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)
        self.admin.add_role(AdminRole.LIBRARIAN, self.ctrl.db.default_library())


@pytest.fixture(scope="function")
def patron_controller_fixture(
    controller_fixture: ControllerFixture,
) -> PatronControllerFixture:
    return PatronControllerFixture(controller_fixture)


class TestPatronController:
    def test__load_patrondata(self, patron_controller_fixture: PatronControllerFixture):
        """Test the _load_patrondata helper method."""

        class MockAuthenticator:
            def __init__(self, providers):
                self.unique_patron_lookup_providers = providers

        class MockAuthenticationProvider:
            def __init__(self, patron_dict):
                self.patron_dict = patron_dict

            def remote_patron_lookup(self, patrondata):
                return self.patron_dict.get(patrondata.authorization_identifier)

        authenticator = MockAuthenticator([])
        auth_provider = MockAuthenticationProvider({})
        identifier = "Patron"

        form = ImmutableMultiDict([("identifier", identifier)])
        m = patron_controller_fixture.manager.admin_patron_controller._load_patrondata

        # User doesn't have admin permission
        with patron_controller_fixture.ctrl.request_context_with_library("/"):
            pytest.raises(AdminNotAuthorized, m, authenticator)

        # No form data specified
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            response = m(authenticator)
            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert "Please enter a patron identifier" == response.detail

        # AuthenticationProvider has no Authenticators.
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert (
                "This library has no authentication providers, so it has no patrons."
                == response.detail
            )

        # Authenticator can't find patron with this identifier
        authenticator.unique_patron_lookup_providers.append(auth_provider)
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert (
                "No patron with identifier %s was found at your library" % identifier
                == response.detail
            )

    def test_lookup_patron(self, patron_controller_fixture: PatronControllerFixture):
        # Here's a patron.
        patron = patron_controller_fixture.ctrl.db.patron()
        patron.authorization_identifier = patron_controller_fixture.ctrl.db.fresh_str()

        # This PatronController will always return information about that
        # patron, no matter what it's asked for.
        class MockPatronController(PatronController):
            def _load_patrondata(self, authenticator):
                self.called_with = authenticator
                return PatronData(
                    authorization_identifier="An Identifier",
                    personal_name="A Patron",
                )

        controller = MockPatronController(patron_controller_fixture.manager)

        authenticator = object()
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            response = controller.lookup_patron(authenticator)
            # The authenticator was passed into _load_patrondata()
            assert authenticator == controller.called_with

            # _load_patrondata() returned a PatronData object. We
            # converted it to a dictionary, which will be dumped to
            # JSON on the way out.
            assert "An Identifier" == response["authorization_identifier"]
            assert "A Patron" == response["personal_name"]

    def test_reset_adobe_id(self, patron_controller_fixture: PatronControllerFixture):
        # Here's a patron with an Adobe-relevant credential.
        patron = patron_controller_fixture.ctrl.db.patron()
        patron.authorization_identifier = patron_controller_fixture.ctrl.db.fresh_str()

        patron_controller_fixture.ctrl.db.credential(
            patron=patron, type=AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER
        )

        # This PatronController will always return a specific
        # PatronData object, no matter what is asked for.
        class MockPatronController(PatronController):
            mock_patrondata: Optional[PatronData] = None

            def _load_patrondata(self, authenticator):
                self.called_with = authenticator
                return self.mock_patrondata

        controller = MockPatronController(patron_controller_fixture.manager)
        controller.mock_patrondata = PatronData(
            authorization_identifier=patron.authorization_identifier
        )

        # We reset their Adobe ID.
        authenticator = object()
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            form = ImmutableMultiDict([("identifier", patron.authorization_identifier)])
            flask.request.form = form

            response = controller.reset_adobe_id(authenticator)
            assert 200 == response.status_code

            # _load_patrondata was called and gave us information about
            # which Patron to modify.
            controller.called_with = authenticator

        # Both of the Patron's credentials are gone.
        assert patron.credentials == []

        # Here, the AuthenticationProvider finds a PatronData, but the
        # controller can't turn it into a Patron because it's too vague.
        controller.mock_patrondata = PatronData()
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = controller.reset_adobe_id(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert "Could not create local patron object" in response.detail
