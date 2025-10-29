from unittest.mock import MagicMock

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from palace.manager.api.admin.controller.patron import PatronController
from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.problem_details import NO_SUCH_PATRON
from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.api.authentication.base import PatronData, PatronLookupNotSupported
from palace.manager.api.authenticator import LibraryAuthenticator
from palace.manager.sqlalchemy.model.admin import AdminRole
from palace.manager.util.problem_detail import ProblemDetail
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

        def mock_authenticator(providers):
            mock_authenticator_ = MagicMock(spec=LibraryAuthenticator)
            mock_authenticator_.unique_patron_lookup_providers = providers
            return mock_authenticator_

        class MockAuthenticationProvider:
            def __init__(self, patron_data_dict: dict[str, PatronData]):
                self.patron_dict = patron_data_dict

            def remote_patron_lookup(self, patrondata) -> PatronData | None:
                return self.patron_dict.get(patrondata.authorization_identifier)

        authenticator = mock_authenticator([])
        identifier = "Patron"

        form = ImmutableMultiDict([("identifier", identifier)])
        m = patron_controller_fixture.manager.admin_patron_controller._load_patron_data

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
        auth_provider = MockAuthenticationProvider({})
        authenticator = mock_authenticator([auth_provider])
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert (
                "No patron with identifier %s was found at your library" % identifier
                == response.detail
            )

        # Authenticator can find patron with this identifier
        auth_provider = MockAuthenticationProvider(
            {identifier: PatronData(authorization_identifier=identifier)}
        )
        authenticator = mock_authenticator([auth_provider])

        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)
            assert not isinstance(response, ProblemDetail)
            assert identifier == response.authorization_identifier

        # Create a patron in the local database
        patron = patron_controller_fixture.ctrl.db.patron()
        patron.authorization_identifier = identifier
        patron.external_identifier = "external_id_456"
        patron.username = "patron_username"
        patron.external_type = "adult"
        patron.fines = "5.50"
        patron.block_reason = None

        # Provider raises PatronLookupNotSupported and local lookup succeeds
        class MockProviderWithLocalFallback:
            library_id = patron_controller_fixture.ctrl.db.default_library().id

            def remote_patron_lookup(self, patrondata):
                raise PatronLookupNotSupported()

            def local_patron_lookup(self, _db, username, patrondata):
                # This will find the patron we created above
                return patron

        auth_provider_local_fallback = MockProviderWithLocalFallback()
        authenticator = mock_authenticator([auth_provider_local_fallback])

        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            # Should successfully return PatronData from local lookup
            assert not isinstance(response, ProblemDetail)
            assert response.authorization_identifier == identifier
            assert response.permanent_id == "external_id_456"
            assert response.username == "patron_username"
            assert response.external_type == "adult"
            assert response.fines == "5.50"
            assert response.block_reason is None
            assert response.complete is True

        # Provider raises PatronLookupNotSupported and local lookup fails
        class MockProviderLocalNotFound:
            library_id = patron_controller_fixture.ctrl.db.default_library().id

            def remote_patron_lookup(self, patrondata):
                raise PatronLookupNotSupported()

            def local_patron_lookup(self, _db, username, patrondata):
                return None

        auth_provider_not_found = MockProviderLocalNotFound()
        authenticator = mock_authenticator([auth_provider_not_found])

        identifier_not_found = "nonexistent_patron"
        form_not_found = ImmutableMultiDict([("identifier", identifier_not_found)])

        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form_not_found
            response = m(authenticator)

            # Should return NO_SUCH_PATRON error
            assert isinstance(response, ProblemDetail)
            assert response.status_code == 404
            assert NO_SUCH_PATRON.uri == response.uri
            assert (
                "No patron with identifier %s was found at your library"
                % identifier_not_found
                == response.detail
            )

        # Provider returns a ProblemDetail (e.g., ILS is down)
        error_detail = ProblemDetail(
            "http://librarysimplified.org/terms/problem/remote-integration-failed",
            status_code=502,
            title="Integration error",
            detail="Failed to communicate with ILS",
        )

        class MockProviderWithError:
            def remote_patron_lookup(self, patrondata):
                return error_detail

        auth_provider_with_error = MockProviderWithError()
        authenticator = mock_authenticator([auth_provider_with_error])

        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            # Should return the ProblemDetail from the provider
            assert isinstance(response, ProblemDetail)
            assert response.status_code == 502
            assert response.title == "Integration error"
            assert response.detail == "Failed to communicate with ILS"

        # Test _load_patrondata with multiple providers, some raising PatronLookupNotSupported.
        # First provider raises PatronLookupNotSupported and local lookup fails
        class FirstProvider:
            library_id = patron_controller_fixture.ctrl.db.default_library().id

            def remote_patron_lookup(self, patrondata):
                raise PatronLookupNotSupported()

            def local_patron_lookup(self, _db, username, patrondata):
                return None

        # Second provider succeeds with remote lookup
        successful_patron_data = PatronData(
            authorization_identifier=identifier,
            permanent_id="found_by_second_provider",
            username="multi_user",
        )

        class SecondProvider:
            def remote_patron_lookup(self, patrondata):
                return successful_patron_data

        providers = [FirstProvider(), SecondProvider()]
        authenticator = mock_authenticator(providers)

        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            # Should get result from second provider
            assert not isinstance(response, ProblemDetail)
            assert response.authorization_identifier == identifier
            assert response.permanent_id == "found_by_second_provider"
            assert response.username == "multi_user"

    def test_lookup_patron(self, patron_controller_fixture: PatronControllerFixture):
        # Here's a patron.
        patron = patron_controller_fixture.ctrl.db.patron()
        patron.authorization_identifier = patron_controller_fixture.ctrl.db.fresh_str()

        # This PatronController will always return information about that
        # patron, no matter what it's asked for.
        class MockPatronController(PatronController):
            def _load_patron_data(self, authenticator):
                self.called_with = authenticator
                return PatronData(
                    authorization_identifier="An Identifier",
                    personal_name="A Patron",
                )

        controller = MockPatronController(patron_controller_fixture.manager)

        authenticator = MagicMock()
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            response = controller.lookup_patron(authenticator)
            # The authenticator was passed into _load_patrondata()
            assert authenticator == controller.called_with

            # _load_patrondata() returned a PatronData object. We
            # converted it to a dictionary, which will be dumped to
            # JSON on the way out.
            assert not isinstance(response, ProblemDetail)
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
            mock_patrondata: PatronData | None = None

            def _load_patron_data(self, authenticator):
                self.called_with = authenticator
                return self.mock_patrondata

        controller = MockPatronController(patron_controller_fixture.manager)
        controller.mock_patrondata = PatronData(
            authorization_identifier=patron.authorization_identifier
        )

        # We reset their Adobe ID.
        authenticator = MagicMock()
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

            assert isinstance(response, ProblemDetail)
            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert response.detail is not None
            assert "Could not create local patron object" in response.detail
