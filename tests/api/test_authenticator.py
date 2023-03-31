"""Test the base authentication framework: that is, the classes that
don't interact with any particular source of truth.
"""
import datetime
import json
import os
import re
from copy import deepcopy
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import flask
import pytest
from flask import url_for
from flask_babel import lazy_gettext as _
from money import Money

from api.annotations import AnnotationWriter
from api.announcements import Announcements
from api.authenticator import (
    AuthenticationProvider,
    Authenticator,
    BasicAuthenticationProvider,
    CirculationPatronProfileStorage,
    LibraryAuthenticator,
    PatronData,
)
from api.config import CannotLoadConfiguration, Configuration
from api.millenium_patron import MilleniumPatronAPI
from api.opds import LibraryAnnotator
from api.problem_details import *
from api.problem_details import PATRON_OF_ANOTHER_LIBRARY
from api.simple_authentication import SimpleAuthenticationProvider
from api.sip import SIP2AuthenticationProvider
from api.sirsidynix_authentication_provider import (
    SirsiBlockReasons,
    SirsiDynixHorizonAuthenticationProvider,
    SirsiDynixPatronData,
)
from api.util.patron import PatronUtility
from core.mock_analytics_provider import MockAnalyticsProvider
from core.model import (
    CirculationEvent,
    ConfigurationSetting,
    ExternalIntegration,
    Library,
    Patron,
    Session,
    create,
)
from core.model.constants import LinkRelations
from core.opds import OPDSFeed
from core.testing import MockRequestsResponse
from core.user_profile import ProfileController
from core.util.authentication_for_opds import AuthenticationForOPDSDocument
from core.util.datetime_helpers import utc_now
from core.util.http import IntegrationException

from ..fixtures.api_controller import ControllerFixture
from ..fixtures.database import DatabaseTransactionFixture
from ..fixtures.vendor_id import VendorIDFixture


class BasicConcreteAuthenticationProvider(BasicAuthenticationProvider):
    def __init__(self, library, integration, analytics=None):
        super().__init__(library, integration, analytics)


class MockAuthenticationProvider:
    """An AuthenticationProvider that always authenticates requests for
    the given Patron and always returns the given PatronData when
    asked to look up data.
    """

    def __init__(self, patron=None, patrondata=None):
        self.patron = patron
        self.patrondata = patrondata

    def authenticate(self, _db, header):
        return self.patron


class MockBasicAuthenticationProvider(
    BasicAuthenticationProvider, MockAuthenticationProvider
):
    """A mock basic authentication provider for use in testing the overall
    authentication process.
    """

    def __init__(
        self,
        library,
        integration,
        analytics=None,
        patron=None,
        patrondata=None,
        *args,
        **kwargs
    ):
        super().__init__(library, integration, analytics, *args, **kwargs)
        self.patron = patron
        self.patrondata = patrondata

    def authenticate(self, _db, header):
        return self.patron

    def remote_authenticate(self, username, password):
        return self.patrondata

    def remote_patron_lookup(self, patrondata):
        return self.patrondata


class MockBasic(BasicAuthenticationProvider):
    """A second mock basic authentication provider for use in testing
    the workflow around Basic Auth.
    """

    NAME = "Mock Basic Auth provider"
    LOGIN_BUTTON_IMAGE = "BasicButton.png"

    def __init__(
        self,
        library,
        integration,
        analytics=None,
        patrondata=None,
        remote_patron_lookup_patrondata=None,
        *args,
        **kwargs
    ):
        super().__init__(library, integration, analytics)
        self.patrondata = patrondata
        self.remote_patron_lookup_patrondata = remote_patron_lookup_patrondata

    def remote_authenticate(self, username, password):
        return self.patrondata

    def remote_patron_lookup(self, patrondata):
        return self.remote_patron_lookup_patrondata


class AuthenticatorFixture:

    db: DatabaseTransactionFixture
    mock_basic_integration: ExternalIntegration

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.mock_basic_integration = self.db.external_integration(
            self.db.fresh_str(), ExternalIntegration.PATRON_AUTH_GOAL
        )

    def mock_basic(self, *args, **kwargs):
        """Convenience method to instantiate a MockBasic object with the
        default library.
        """
        return MockBasic(
            self.db.default_library(), self.mock_basic_integration, *args, **kwargs
        )


@pytest.fixture(scope="function")
def authenticator_fixture(db: DatabaseTransactionFixture) -> AuthenticatorFixture:
    return AuthenticatorFixture(db)


class PatronDataFixture:
    def __init__(self, auth: AuthenticatorFixture):
        self.auth = auth
        self.expiration_time = utc_now()
        self.data = PatronData(
            permanent_id="1",
            authorization_identifier="2",
            username="3",
            personal_name="4",
            email_address="5",
            authorization_expires=self.expiration_time,
            fines=Money(6, "USD"),
            block_reason=PatronData.NO_VALUE,
        )


@pytest.fixture(scope="function")
def patron_data_fixture(
    authenticator_fixture: AuthenticatorFixture,
) -> PatronDataFixture:
    return PatronDataFixture(authenticator_fixture)


class TestPatronData:
    def test_to_dict(self, patron_data_fixture: PatronDataFixture):
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data

        data = p_data.to_dict
        expect = dict(
            permanent_id="1",
            authorization_identifier="2",
            authorization_identifiers=["2"],
            external_type=None,
            username="3",
            personal_name="4",
            email_address="5",
            authorization_expires=patron_data_fixture.expiration_time.strftime(
                "%Y-%m-%d"
            ),
            fines="6",
            block_reason=None,
        )
        assert data == expect

        # Test with an empty fines field
        p_data.fines = PatronData.NO_VALUE
        data = p_data.to_dict
        expect["fines"] = None
        assert data == expect

        # Test with a zeroed-out fines field
        p_data.fines = Decimal(0.0)
        data = p_data.to_dict
        expect["fines"] = "0"
        assert data == expect

        # Test with an empty expiration time
        p_data.authorization_expires = PatronData.NO_VALUE
        data = p_data.to_dict
        expect["authorization_expires"] = None
        assert data == expect

    def test_apply(self, patron_data_fixture: PatronDataFixture):
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data

        patron = db.patron()
        p_data.cached_neighborhood = "Little Homeworld"

        p_data.apply(patron)
        assert p_data.permanent_id == patron.external_identifier
        assert p_data.authorization_identifier == patron.authorization_identifier
        assert p_data.username == patron.username
        assert p_data.authorization_expires == patron.authorization_expires
        assert p_data.fines == patron.fines
        assert None == patron.block_reason
        assert "Little Homeworld" == patron.cached_neighborhood

        # This data is stored in PatronData but not applied to Patron.
        assert "4" == p_data.personal_name
        assert False == hasattr(patron, "personal_name")
        assert "5" == p_data.email_address
        assert False == hasattr(patron, "email_address")

        # This data is stored on the Patron object as a convenience,
        # but it's not stored in the database.
        assert "Little Homeworld" == patron.neighborhood

    def test_apply_block_reason(self, patron_data_fixture: PatronDataFixture):
        """If the PatronData has a reason why a patron is blocked,
        the reason is put into the Patron record.
        """
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data
        p_data.block_reason = PatronData.UNKNOWN_BLOCK
        patron = db.patron()
        p_data.apply(patron)
        assert PatronData.UNKNOWN_BLOCK == patron.block_reason

    def test_apply_multiple_authorization_identifiers(
        self, patron_data_fixture: PatronDataFixture
    ):
        """If there are multiple authorization identifiers, the first
        one is chosen.
        """
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data
        patron = db.patron()
        patron.authorization_identifier = None
        data = PatronData(authorization_identifier=["2", "3"], complete=True)
        data.apply(patron)
        assert "2" == patron.authorization_identifier

        # If Patron.authorization_identifier is already set, it will
        # not be changed, so long as its current value is acceptable.
        data = PatronData(authorization_identifier=["3", "2"], complete=True)
        data.apply(patron)
        assert "2" == patron.authorization_identifier

        # If Patron.authorization_identifier ever turns out not to be
        # an acceptable value, it will be changed.
        data = PatronData(authorization_identifier=["3", "4"], complete=True)
        data.apply(patron)
        assert "3" == patron.authorization_identifier

    def test_apply_sets_last_external_sync_if_data_is_complete(
        self, patron_data_fixture: PatronDataFixture
    ):
        """Patron.last_external_sync is only updated when apply() is called on
        a PatronData object that represents a full set of metadata.
        What constitutes a 'full set' depends on the authentication
        provider.
        """
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data
        patron = db.patron()
        p_data.complete = False
        p_data.apply(patron)
        assert None == patron.last_external_sync
        p_data.complete = True
        p_data.apply(patron)
        assert None != patron.last_external_sync

    def test_apply_sets_first_valid_authorization_identifier(
        self, patron_data_fixture: PatronDataFixture
    ):
        """If the ILS has multiple authorization identifiers for a patron, the
        first one is used.
        """
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data
        patron = db.patron()
        patron.authorization_identifier = None
        p_data.set_authorization_identifier(["identifier 1", "identifier 2"])
        p_data.apply(patron)
        assert "identifier 1" == patron.authorization_identifier

    def test_apply_leaves_valid_authorization_identifier_alone(
        self, patron_data_fixture: PatronDataFixture
    ):
        """If the ILS says a patron has a new preferred authorization
        identifier, but our Patron record shows them using an
        authorization identifier that still works, we don't change it.
        """
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data
        patron = db.patron()
        patron.authorization_identifier = "old identifier"
        p_data.set_authorization_identifier(
            ["new identifier", patron.authorization_identifier]
        )
        p_data.apply(patron)
        assert "old identifier" == patron.authorization_identifier

    def test_apply_overwrites_invalid_authorization_identifier(
        self, patron_data_fixture: PatronDataFixture
    ):
        """If the ILS says a patron has a new preferred authorization
        identifier, and our Patron record shows them using an
        authorization identifier that no longer works, we change it.
        """
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data
        patron = db.patron()
        p_data.set_authorization_identifier(["identifier 1", "identifier 2"])
        p_data.apply(patron)
        assert "identifier 1" == patron.authorization_identifier

    def test_apply_on_incomplete_information(
        self, patron_data_fixture: PatronDataFixture
    ):
        """When we call apply() based on incomplete information (most
        commonly, the fact that a given string was successfully used
        to authenticate a patron), we are very careful about modifying
        data already in the database.
        """
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data
        now = utc_now()

        # If the only thing we know about a patron is that a certain
        # string authenticated them, we set
        # Patron.authorization_identifier to that string but we also
        # indicate that we need to perform an external sync on them
        # ASAP.
        authenticated = PatronData(authorization_identifier="1234", complete=False)
        patron = db.patron()
        patron.authorization_identifier = None
        patron.last_external_sync = now
        authenticated.apply(patron)
        assert "1234" == patron.authorization_identifier
        assert None == patron.last_external_sync

        # If a patron authenticates by username, we leave their Patron
        # record alone.
        patron = db.patron()
        patron.authorization_identifier = "1234"
        patron.username = "user"
        patron.last_external_sync = now
        patron.fines = Money(10, "USD")
        authenticated_by_username = PatronData(
            authorization_identifier="user", complete=False
        )
        authenticated_by_username.apply(patron)
        assert now == patron.last_external_sync

        # If a patron authenticates with a string that is neither
        # their authorization identifier nor their username, we leave
        # their Patron record alone, except that we indicate that we
        # need to perform an external sync on them ASAP.
        patron.last_external_sync = now
        authenticated_by_weird_identifier = PatronData(
            authorization_identifier="5678", complete=False
        )
        authenticated_by_weird_identifier.apply(patron)
        assert "1234" == patron.authorization_identifier
        assert None == patron.last_external_sync

    def test_get_or_create_patron(self, patron_data_fixture: PatronDataFixture):
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data
        analytics = MockAnalyticsProvider()

        # The patron didn't exist yet, so it was created
        # and an analytics event was sent.
        default_library = db.default_library()
        patron, is_new = p_data.get_or_create_patron(
            db.session, default_library.id, analytics
        )
        assert "2" == patron.authorization_identifier
        assert default_library == patron.library
        assert True == is_new
        assert CirculationEvent.NEW_PATRON == analytics.event_type
        assert 1 == analytics.count

        # Patron.neighborhood was set, even though there is no
        # value and that's not a database field.
        assert None == patron.neighborhood

        # Set a neighborhood and try again.
        p_data.neighborhood = "Achewood"

        # The same patron is returned, and no analytics
        # event was sent.
        patron, is_new = p_data.get_or_create_patron(
            db.session, default_library.id, analytics
        )
        assert "2" == patron.authorization_identifier
        assert False == is_new
        assert "Achewood" == patron.neighborhood
        assert 1 == analytics.count

    def test_to_response_parameters(self, patron_data_fixture: PatronDataFixture):
        db, p_data = patron_data_fixture.auth.db, patron_data_fixture.data
        params = p_data.to_response_parameters
        assert dict(name="4") == params

        p_data.personal_name = None
        params = p_data.to_response_parameters
        assert dict() == params


class TestCirculationPatronProfileStorage:
    def test_profile_document(
        self, controller_fixture: ControllerFixture, vendor_id_fixture: VendorIDFixture
    ):
        def mock_url_for(endpoint, library_short_name, _external=True):
            return (
                "http://host/"
                + endpoint
                + "?"
                + "library_short_name="
                + library_short_name
            )

        patron = controller_fixture.db.patron()
        storage = CirculationPatronProfileStorage(patron, mock_url_for)
        doc = storage.profile_document
        assert "settings" in doc
        # Since there's no authdata configured, the DRM fields are not present
        assert "drm:vendor" not in doc
        assert "drm:clientToken" not in doc
        assert "drm:scheme" not in doc
        assert "links" not in doc

        # Now there's authdata configured, and the DRM fields are populated with
        # the vendor ID and a short client token
        vendor_id_fixture.initialize_adobe(patron.library)

        doc = storage.profile_document
        [adobe] = doc["drm"]
        assert adobe["drm:vendor"] == "vendor id"
        assert adobe["drm:clientToken"].startswith(
            patron.library.short_name.upper() + "TOKEN"
        )
        assert (
            adobe["drm:scheme"] == "http://librarysimplified.org/terms/drm/scheme/ACS"
        )
        [device_link, annotations_link] = doc["links"]
        assert (
            device_link["rel"] == "http://librarysimplified.org/terms/drm/rel/devices"
        )
        assert (
            device_link["href"]
            == "http://host/adobe_drm_devices?library_short_name=default"
        )
        assert annotations_link["rel"] == "http://www.w3.org/ns/oa#annotationService"
        assert (
            annotations_link["href"]
            == "http://host/annotations?library_short_name=default"
        )
        assert annotations_link["type"] == AnnotationWriter.CONTENT_TYPE


class MockAuthenticator(Authenticator):
    """Allows testing Authenticator methods outside of a request context."""

    def __init__(self, current_library, authenticators, analytics=None):
        _db = Session.object_session(current_library)
        super().__init__(_db, analytics)
        self.current_library_name = current_library.short_name
        self.library_authenticators = authenticators

    def populate_authenticators(self, *args, **kwargs):
        """Do nothing -- authenticators were set in the constructor."""

    @property
    def current_library_short_name(self):
        return self.current_library_name


class TestAuthenticator:
    def test_init(self, controller_fixture: ControllerFixture):
        db = controller_fixture.db

        # The default library has already been configured to use the
        # SimpleAuthenticationProvider for its basic auth.
        l1 = db.default_library()
        l1.short_name = "l1"

        # This library uses Millenium Patron.
        l2, ignore = create(db.session, Library, short_name="l2")
        integration = db.external_integration(
            "api.millenium_patron", goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        integration.url = "http://url/"
        l2.integrations.append(integration)

        db.session.commit()

        analytics = MockAnalyticsProvider()

        auth = Authenticator(db.session, db.session.query(Library), analytics)

        # A LibraryAuthenticator has been created for each Library.
        assert "l1" in auth.library_authenticators
        assert "l2" in auth.library_authenticators
        assert isinstance(auth.library_authenticators["l1"], LibraryAuthenticator)
        assert isinstance(auth.library_authenticators["l2"], LibraryAuthenticator)

        # Each LibraryAuthenticator has been associated with an
        # appropriate AuthenticationProvider.

        assert isinstance(
            auth.library_authenticators["l1"].basic_auth_provider,
            SimpleAuthenticationProvider,
        )
        assert isinstance(
            auth.library_authenticators["l2"].basic_auth_provider, MilleniumPatronAPI
        )

        # Each provider has the analytics set.
        assert (
            analytics == auth.library_authenticators["l1"].basic_auth_provider.analytics
        )
        assert (
            analytics == auth.library_authenticators["l2"].basic_auth_provider.analytics
        )

    def test_methods_call_library_authenticators(
        self, controller_fixture: ControllerFixture
    ):
        db, app = controller_fixture.db, controller_fixture.app

        class MockLibraryAuthenticator(LibraryAuthenticator):
            def __init__(self, name):
                self.name = name

            def authenticated_patron(self, _db, header):
                return "authenticated patron for %s" % self.name

            def create_authentication_document(self):
                return "authentication document for %s" % self.name

            def create_authentication_headers(self):
                return "authentication headers for %s" % self.name

            def get_credential_from_header(self, header):
                return "credential for %s" % self.name

            def create_bearer_token(self, *args, **kwargs):
                return "bearer token for %s" % self.name

            def oauth_provider_lookup(self, *args, **kwargs):
                return "oauth provider for %s" % self.name

            def decode_bearer_token(self, *args, **kwargs):
                return "decoded bearer token for %s" % self.name

        l1, ignore = create(db.session, Library, short_name="l1")
        l2, ignore = create(db.session, Library, short_name="l2")

        auth = Authenticator(db.session, db.session.query(Library))
        auth.library_authenticators["l1"] = MockLibraryAuthenticator("l1")
        auth.library_authenticators["l2"] = MockLibraryAuthenticator("l2")

        # This new library isn't in the authenticator.
        l3, ignore = create(db.session, Library, short_name="l3")

        with app.test_request_context("/"):
            flask.request.library = l3  # type:ignore
            assert LIBRARY_NOT_FOUND == auth.authenticated_patron(db.session, {})  # type: ignore
            assert LIBRARY_NOT_FOUND == auth.create_authentication_document()  # type: ignore
            assert LIBRARY_NOT_FOUND == auth.create_authentication_headers()  # type: ignore
            assert LIBRARY_NOT_FOUND == auth.get_credential_from_header({})  # type: ignore
            assert LIBRARY_NOT_FOUND == auth.create_bearer_token()  # type: ignore
            assert LIBRARY_NOT_FOUND == auth.oauth_provider_lookup()  # type: ignore

        # The other libraries are in the authenticator.
        with app.test_request_context("/"):
            flask.request.library = l1  # type:ignore
            assert "authenticated patron for l1" == auth.authenticated_patron(
                db.session, {}
            )
            assert (
                "authentication document for l1"
                == auth.create_authentication_document()
            )
            assert (
                "authentication headers for l1" == auth.create_authentication_headers()
            )
            assert "credential for l1" == auth.get_credential_from_header({})
            assert "bearer token for l1" == auth.create_bearer_token()
            assert "oauth provider for l1" == auth.oauth_provider_lookup()
            assert "decoded bearer token for l1" == auth.decode_bearer_token()

        with app.test_request_context("/"):
            flask.request.library = l2  # type:ignore
            assert "authenticated patron for l2" == auth.authenticated_patron(
                db.session, {}
            )
            assert (
                "authentication document for l2"
                == auth.create_authentication_document()
            )
            assert (
                "authentication headers for l2" == auth.create_authentication_headers()
            )
            assert "credential for l2" == auth.get_credential_from_header({})
            assert "bearer token for l2" == auth.create_bearer_token()
            assert "oauth provider for l2" == auth.oauth_provider_lookup()
            assert "decoded bearer token for l2" == auth.decode_bearer_token()


class TestLibraryAuthenticator:
    def test_from_config_basic_auth_only(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db

        # Only a basic auth provider.
        millenium = db.external_integration(
            "api.millenium_patron",
            ExternalIntegration.PATRON_AUTH_GOAL,
            libraries=[db.default_library()],
        )
        millenium.url = "http://url/"
        auth = LibraryAuthenticator.from_config(db.session, db.default_library())

        assert auth.basic_auth_provider != None
        assert isinstance(auth.basic_auth_provider, MilleniumPatronAPI)
        assert {} == auth.oauth_providers_by_name

    def test_with_custom_patron_catalog(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        """Instantiation of a LibraryAuthenticator may
        include instantiation of a CustomPatronCatalog.
        """
        db = authenticator_fixture.db
        mock_catalog = object()

        class MockCustomPatronCatalog:
            @classmethod
            def for_library(self, library):
                self.called_with = library
                return mock_catalog

        authenticator = LibraryAuthenticator.from_config(
            db.session,
            db.default_library(),
            custom_catalog_source=MockCustomPatronCatalog,
        )
        assert (
            db.default_library() == MockCustomPatronCatalog.called_with  # type:ignore
        )

        # The custom patron catalog is stored as
        # authentication_document_annotator.
        assert mock_catalog == authenticator.authentication_document_annotator

    def test_config_succeeds_when_no_providers_configured(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        # You can call from_config even when there are no authentication
        # providers configured.

        # This should not happen in normal usage, but there will be an
        # interim period immediately after a library is created where
        # this will be its configuration.
        db = authenticator_fixture.db
        authenticator = LibraryAuthenticator.from_config(
            db.session, db.default_library()
        )
        assert [] == list(authenticator.providers)

    def test_configuration_exception_during_from_config_stored(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        # If the initialization of an AuthenticationProvider from config
        # raises CannotLoadConfiguration or ImportError, the exception
        # is stored with the LibraryAuthenticator rather than being
        # propagated.
        db = authenticator_fixture.db
        # Create an integration destined to raise CannotLoadConfiguration..
        misconfigured = db.external_integration(
            "api.millenium_patron",
            ExternalIntegration.PATRON_AUTH_GOAL,
        )

        # ... and one destined to raise ImportError.
        unknown = db.external_integration(
            "unknown protocol", ExternalIntegration.PATRON_AUTH_GOAL
        )
        for integration in [misconfigured, unknown]:
            db.default_library().integrations.append(integration)
        auth = LibraryAuthenticator.from_config(db.session, db.default_library())

        # The LibraryAuthenticator exists but has no AuthenticationProviders.
        assert None == auth.basic_auth_provider
        assert {} == auth.oauth_providers_by_name

        # Both integrations have left their trace in
        # initialization_exceptions.
        not_configured = auth.initialization_exceptions[misconfigured.id]
        assert isinstance(not_configured, CannotLoadConfiguration)
        assert "Millenium Patron API server not configured." == str(not_configured)

        not_found = auth.initialization_exceptions[unknown.id]
        assert isinstance(not_found, ImportError)
        assert "No module named 'unknown protocol'" == str(not_found)

    def test_register_fails_when_integration_has_wrong_goal(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        integration = db.external_integration("protocol", "some other goal")
        auth = LibraryAuthenticator(_db=db.session, library=db.default_library())
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert (
            "Was asked to register an integration with goal=some other goal as though it were a way of authenticating patrons."
            in str(excinfo.value)
        )

    def test_register_fails_when_integration_not_associated_with_library(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        integration = db.external_integration(
            "protocol", ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth = LibraryAuthenticator(_db=db.session, library=db.default_library())
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert "Was asked to register an integration with library {}, which doesn't use it.".format(
            db.default_library().name
        ) in str(
            excinfo.value
        )

    def test_register_fails_when_integration_module_does_not_contain_provider_class(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        library = db.default_library()
        integration = db.external_integration(
            "api.lanes", ExternalIntegration.PATRON_AUTH_GOAL
        )
        library.integrations.append(integration)
        auth = LibraryAuthenticator(_db=db.session, library=library)
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert (
            "Loaded module api.lanes but could not find a class called AuthenticationProvider inside."
            in str(excinfo.value)
        )

    def test_register_provider_fails_but_does_not_explode_on_remote_integration_error(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        library = db.default_library()
        # We're going to instantiate the a mock authentication provider that
        # immediately raises a RemoteIntegrationException, which will become
        # a CannotLoadConfiguration.
        integration = db.external_integration(
            "tests.api.mock_authentication_provider",
            ExternalIntegration.PATRON_AUTH_GOAL,
        )
        library.integrations.append(integration)
        auth = LibraryAuthenticator(_db=db.session, library=library)
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert "Could not instantiate" in str(excinfo.value)
        assert "authentication provider for library {}, possibly due to a network connection problem.".format(
            db.default_library().name
        ) in str(
            excinfo.value
        )

    def test_register_provider_basic_auth(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        sip2 = db.external_integration(
            "api.sip",
            ExternalIntegration.PATRON_AUTH_GOAL,
        )
        sip2.url = "http://url/"
        sip2.password = "secret"
        db.default_library().integrations.append(sip2)
        auth = LibraryAuthenticator(_db=db.session, library=db.default_library())
        auth.register_provider(sip2)
        assert isinstance(auth.basic_auth_provider, SIP2AuthenticationProvider)

    def test_supports_patron_authentication(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        authenticator = LibraryAuthenticator.from_config(
            db.session, db.default_library()
        )

        # This LibraryAuthenticator does not actually support patron
        # authentication because it has no auth providers.
        #
        # (This isn't necessarily a deal breaker, but most libraries
        # do authenticate their patrons.)
        assert False == authenticator.supports_patron_authentication

        # Adding a basic auth provider will make it start supporting
        # patron authentication.
        authenticator.basic_auth_provider = object()
        assert True == authenticator.supports_patron_authentication
        authenticator.basic_auth_provider = None

        # So will adding an OAuth provider.
        authenticator.oauth_providers_by_name[object()] = object()
        assert True == authenticator.supports_patron_authentication

    def test_identifies_individuals(self, authenticator_fixture: AuthenticatorFixture):
        db = authenticator_fixture.db
        # This LibraryAuthenticator does not authenticate patrons at
        # all, so it does not identify patrons as individuals.
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
        )

        # This LibraryAuthenticator has two Authenticators, but
        # neither of them identify patrons as individuals.
        class MockAuthenticator:
            NAME = "mock"
            IDENTIFIES_INDIVIDUALS = False

        basic = MockAuthenticator()
        saml = MockAuthenticator()
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            basic_auth_provider=basic,
            saml_providers=[saml],
            bearer_token_signing_secret=db.fresh_str(),
        )
        assert False == authenticator.identifies_individuals

        # If some Authenticators identify individuals and some do not,
        # the library as a whole does not (necessarily) identify
        # individuals.
        basic.IDENTIFIES_INDIVIDUALS = True
        assert False == authenticator.identifies_individuals

        # If every Authenticator identifies individuals, then so does
        # the library as a whole.
        saml.IDENTIFIES_INDIVIDUALS = True
        assert True == authenticator.identifies_individuals

    def test_provider_registration(self, authenticator_fixture: AuthenticatorFixture):
        """You can register the same provider multiple times,
        but you can't register two different basic auth providers
        """
        db = authenticator_fixture.db
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            bearer_token_signing_secret="foo",
        )
        integration = db.external_integration(db.fresh_str())
        basic1 = MockBasicAuthenticationProvider(db.default_library(), integration)
        basic2 = MockBasicAuthenticationProvider(db.default_library(), integration)

        authenticator.register_basic_auth_provider(basic1)
        authenticator.register_basic_auth_provider(basic1)

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            authenticator.register_basic_auth_provider(basic2)
        assert "Two basic auth providers configured" in str(excinfo.value)

    def test_authenticated_patron_basic(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        patron = db.patron()
        patrondata = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=patron.authorization_identifier,
            username=patron.username,
            neighborhood="Achewood",
        )
        integration = db.external_integration(db.fresh_str())
        basic = MockBasicAuthenticationProvider(
            db.default_library(), integration, patron=patron, patrondata=patrondata
        )
        authenticator = LibraryAuthenticator(
            _db=db.session, library=db.default_library(), basic_auth_provider=basic
        )
        assert patron == authenticator.authenticated_patron(
            db.session, dict(username="foo", password="bar")
        )

        # Neighborhood information is being temporarily stored in the
        # Patron object for use elsewhere in request processing. It
        # won't be written to the database because there's no field in
        # `patrons` to store it.
        assert "Achewood" == patron.neighborhood

        # OAuth doesn't work.
        problem = authenticator.authenticated_patron(db.session, "Bearer abcd")
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == problem  # type: ignore

    def test_authenticated_patron_unsupported_mechanism(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
        )
        problem = authenticator.authenticated_patron(db.session, object())
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == problem  # type: ignore

    def test_get_credential_from_header(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        integration = db.external_integration(db.fresh_str())
        basic = MockBasicAuthenticationProvider(db.default_library(), integration)

        # We can pull the password out of a Basic Auth credential
        # if a Basic Auth authentication provider is configured.
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            basic_auth_provider=basic,
            bearer_token_signing_secret="secret",
        )
        credential = dict(password="foo")
        assert "foo" == authenticator.get_credential_from_header(credential)

        # We can't pull the password out if no basic auth provider
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            basic_auth_provider=None,
            bearer_token_signing_secret="secret",
        )
        assert None == authenticator.get_credential_from_header(credential)

    def test_create_authentication_document(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db

        class MockAuthenticator(LibraryAuthenticator):
            """Mock the _geographic_areas method."""

            AREAS = ["focus area", "service area"]

            @classmethod
            def _geographic_areas(cls, library):
                return cls.AREAS

        integration = db.external_integration(db.fresh_str())
        library = db.default_library()
        basic = MockBasicAuthenticationProvider(library, integration)
        library.name = "A Fabulous Library"
        authenticator = MockAuthenticator(
            _db=db.session,
            library=library,
            basic_auth_provider=basic,
            bearer_token_signing_secret="secret",
        )

        class MockAuthenticationDocumentAnnotator:
            def annotate_authentication_document(self, library, doc, url_for):
                self.called_with = library, doc, url_for
                doc["modified"] = "Kilroy was here"
                return doc

        annotator = MockAuthenticationDocumentAnnotator()
        authenticator.authentication_document_annotator = annotator

        # We're about to call url_for, so we must create an
        # application context.
        os.environ["AUTOINITIALIZE"] = "False"
        from api.app import app

        self.app = app
        del os.environ["AUTOINITIALIZE"]

        # Set up configuration settings for links.
        link_config = {
            LibraryAnnotator.TERMS_OF_SERVICE: "http://terms",
            LibraryAnnotator.PRIVACY_POLICY: "http://privacy",
            LibraryAnnotator.COPYRIGHT: "http://copyright",
            LibraryAnnotator.ABOUT: "http://about",
            LibraryAnnotator.LICENSE: "http://license/",
            LibraryAnnotator.REGISTER: "custom-registration-hook://library/",
            LinkRelations.PATRON_PASSWORD_RESET: "https://example.org/reset",
            Configuration.LOGO: "image data",
            Configuration.WEB_CSS_FILE: "http://style.css",
        }

        for rel, value in link_config.items():
            ConfigurationSetting.for_library(rel, db.default_library()).value = value

        ConfigurationSetting.for_library(
            Configuration.LIBRARY_DESCRIPTION, library
        ).value = "Just the best."

        # Set the URL to the library's web page.
        ConfigurationSetting.for_library(
            Configuration.WEBSITE_URL, library
        ).value = "http://library/"

        # Set the color scheme a mobile client should use.
        ConfigurationSetting.for_library(
            Configuration.COLOR_SCHEME, library
        ).value = "plaid"

        # Set the colors a web client should use.
        ConfigurationSetting.for_library(
            Configuration.WEB_PRIMARY_COLOR, library
        ).value = "#012345"
        ConfigurationSetting.for_library(
            Configuration.WEB_SECONDARY_COLOR, library
        ).value = "#abcdef"

        # Configure the various ways a patron can get help.
        ConfigurationSetting.for_library(
            Configuration.HELP_EMAIL, library
        ).value = "help@library"
        ConfigurationSetting.for_library(
            Configuration.HELP_WEB, library
        ).value = "http://library.help/"
        ConfigurationSetting.for_library(
            Configuration.HELP_URI, library
        ).value = "custom:uri"

        base_url = ConfigurationSetting.sitewide(db.session, Configuration.BASE_URL_KEY)
        base_url.value = "http://circulation-manager/"

        # Configure three announcements: two active and one
        # inactive.
        format = "%Y-%m-%d"
        today_date = datetime.date.today()
        tomorrow = (today_date + datetime.timedelta(days=1)).strftime(format)
        yesterday = (today_date - datetime.timedelta(days=1)).strftime(format)
        two_days_ago = (today_date - datetime.timedelta(days=2)).strftime(format)
        today = today_date.strftime(format)
        announcements = [
            dict(
                id="a1",
                content="this is announcement 1",
                start=yesterday,
                finish=today,
            ),
            dict(
                id="a2",
                content="this is announcement 2",
                start=two_days_ago,
                finish=yesterday,
            ),
            dict(
                id="a3",
                content="this is announcement 3",
                start=yesterday,
                finish=today,
            ),
        ]
        announcement_setting = ConfigurationSetting.for_library(
            Announcements.SETTING_NAME, library
        )
        announcement_setting.value = json.dumps(announcements)
        announcement_for_all_setting = ConfigurationSetting.sitewide(
            db.session, Announcements.GLOBAL_SETTING_NAME
        )
        announcement_for_all_setting.value = json.dumps(
            [
                dict(
                    id="all1",
                    content="test announcement",
                    start=yesterday,
                    finish=today,
                ),
                dict(
                    id="all2",
                    content="test announcement",
                    start=two_days_ago,
                    finish=yesterday,
                ),
            ]
        )

        with self.app.test_request_context("/"):
            url = authenticator.authentication_document_url(library)
            assert url.endswith("/%s/authentication_document" % library.short_name)

            doc = json.loads(authenticator.create_authentication_document())
            # The main thing we need to test is that the
            # authentication sub-documents are assembled properly and
            # placed in the right position.
            flows = doc["authentication"]
            oauth_doc, basic_doc = sorted(flows, key=lambda x: x["type"])

            expect_basic = basic.authentication_flow_document(db.session)
            assert expect_basic == basic_doc

            # We also need to test that the library's name and ID
            # were placed in the document.
            assert "A Fabulous Library" == doc["title"]
            assert "Just the best." == doc["service_description"]
            assert url == doc["id"]

            # The mobile color scheme and web colors are correctly reported.
            assert "plaid" == doc["color_scheme"]
            assert "#012345" == doc["web_color_scheme"]["primary"]
            assert "#abcdef" == doc["web_color_scheme"]["secondary"]

            # _geographic_areas was called and provided the library's
            # focus area and service area.
            assert "focus area" == doc["focus_area"]
            assert "service area" == doc["service_area"]

            # We also need to test that the links got pulled in
            # from the configuration.
            (
                about,
                alternate,
                copyright,
                help_uri,
                help_web,
                help_email,
                copyright_agent,
                reset_link,
                profile,
                loans,
                license,
                logo,
                privacy_policy,
                register,
                start,
                stylesheet,
                terms_of_service,
            ) = sorted(doc["links"], key=lambda x: (x["rel"], x["href"]))
            assert "http://terms" == terms_of_service["href"]
            assert "http://privacy" == privacy_policy["href"]
            assert "http://copyright" == copyright["href"]
            assert "http://about" == about["href"]
            assert "http://license/" == license["href"]
            assert "image data" == logo["href"]
            assert "http://style.css" == stylesheet["href"]

            assert "/loans" in loans["href"]
            assert "http://opds-spec.org/shelf" == loans["rel"]
            assert OPDSFeed.ACQUISITION_FEED_TYPE == loans["type"]

            assert "/patrons/me" in profile["href"]
            assert ProfileController.LINK_RELATION == profile["rel"]
            assert ProfileController.MEDIA_TYPE == profile["type"]

            expect_start = url_for(
                "index",
                library_short_name=db.default_library().short_name,
                _external=True,
            )
            assert expect_start == start["href"]

            # The start link points to an OPDS feed.
            assert OPDSFeed.ACQUISITION_FEED_TYPE == start["type"]

            # Most of the other links have type='text/html'
            assert "text/html" == about["type"]

            # The registration link doesn't have a type, because it
            # uses a non-HTTP URI scheme.
            assert "type" not in register
            assert "custom-registration-hook://library/" == register["href"]

            assert "https://example.org/reset" == reset_link["href"]

            # The logo link has type "image/png".
            assert "image/png" == logo["type"]

            # We have three help links.
            assert "custom:uri" == help_uri["href"]
            assert "http://library.help/" == help_web["href"]
            assert "text/html" == help_web["type"]
            assert "mailto:help@library" == help_email["href"]

            # Since no special address was given for the copyright
            # designated agent, the help address was reused.
            copyright_rel = (
                "http://librarysimplified.org/rel/designated-agent/copyright"
            )
            assert copyright_rel == copyright_agent["rel"]
            assert "mailto:help@library" == copyright_agent["href"]

            # The public key is correct.
            assert authenticator.public_key == doc["public_key"]["value"]
            assert "RSA" == doc["public_key"]["type"]

            # The library's web page shows up as an HTML alternate
            # to the OPDS server.
            assert (
                dict(rel="alternate", type="text/html", href="http://library/")
                == alternate
            )

            # Active announcements are published; inactive announcements are not.
            all1, a1, a3 = doc["announcements"]
            assert dict(id="a1", content="this is announcement 1") == a1
            assert dict(id="a3", content="this is announcement 3") == a3
            assert dict(id="all1", content="test announcement") == all1

            # Features that are enabled for this library are communicated
            # through the 'features' item.
            features = doc["features"]
            assert [] == features["disabled"]
            assert [Configuration.RESERVATIONS_FEATURE] == features["enabled"]

            # If a separate copyright designated agent is configured,
            # that email address is used instead of the default
            # patron support address.
            ConfigurationSetting.for_library(
                Configuration.COPYRIGHT_DESIGNATED_AGENT_EMAIL, library
            ).value = "mailto:dmca@library.org"
            doc = json.loads(authenticator.create_authentication_document())
            [agent] = [x for x in doc["links"] if x["rel"] == copyright_rel]
            assert "mailto:dmca@library.org" == agent["href"]

            # If no focus area or service area are provided, those fields
            # are not added to the document.
            MockAuthenticator.AREAS = [None, None]  # type:ignore
            doc = json.loads(authenticator.create_authentication_document())
            for key in ("focus_area", "service_area"):
                assert key not in doc

            # Only global anouncements
            announcement_setting.value = None
            doc = json.loads(authenticator.create_authentication_document())
            assert [dict(id="all1", content="test announcement")] == doc[
                "announcements"
            ]
            # If there are no announcements, the list of announcements is present
            # but empty.
            announcement_for_all_setting.value = None
            doc = json.loads(authenticator.create_authentication_document())
            assert [] == doc["announcements"]

            # The annotator's annotate_authentication_document method
            # was called and successfully modified the authentication
            # document.
            assert (library, doc, url_for) == annotator.called_with
            assert "Kilroy was here" == doc["modified"]

            # While we're in this context, let's also test
            # create_authentication_headers.

            # So long as the authenticator includes a basic auth
            # provider, that provider's .authentication_header is used
            # for WWW-Authenticate.
            headers = authenticator.create_authentication_headers()
            assert AuthenticationForOPDSDocument.MEDIA_TYPE == headers["Content-Type"]
            assert basic.authentication_header == headers["WWW-Authenticate"]

            # The response contains a Link header pointing to the authentication
            # document
            expect = "<{}>; rel={}".format(
                authenticator.authentication_document_url(db.default_library()),
                AuthenticationForOPDSDocument.LINK_RELATION,
            )
            assert expect == headers["Link"]

            # If the authenticator does not include a basic auth provider,
            # no WWW-Authenticate header is provided.
            authenticator = LibraryAuthenticator(
                _db=db.session,
                library=library,
                bearer_token_signing_secret="secret",
            )
            headers = authenticator.create_authentication_headers()
            assert "WWW-Authenticate" not in headers

    def test_key_pair(self, authenticator_fixture: AuthenticatorFixture):
        """Test the public/private key pair associated with a library."""
        db = authenticator_fixture.db
        library = db.default_library()

        # Initially, the KEY_PAIR setting is not set.
        def keys():
            return ConfigurationSetting.for_library(
                Configuration.KEY_PAIR, library
            ).json_value

        assert None == keys()

        # Instantiating a LibraryAuthenticator for a library automatically
        # generates a public/private key pair.
        auth = LibraryAuthenticator.from_config(db.session, library)
        public, private = keys()
        assert "BEGIN PUBLIC KEY" in public
        assert "BEGIN RSA PRIVATE KEY" in private

        # The public key is stored in the
        # LibraryAuthenticator.public_key property.
        assert public == auth.public_key

        # The private key is not stored in the LibraryAuthenticator
        # object, but it can be obtained from the database by
        # using the key_pair property.
        assert not hasattr(auth, "private_key")
        assert (public, private) == auth.key_pair

    def test_key_pair_per_library(self, authenticator_fixture: AuthenticatorFixture):
        # Ensure that each library obtains its own key pair.
        db = authenticator_fixture.db
        library1 = db.default_library()
        library2 = db.library()

        # We mock the key_pair function here, and make sure its called twice, with
        # different settings because the get_mock_config_key_pair mock always returns
        # the same key. So we need to do a bit more work to verify that different
        # libraries get different keys.
        with patch.object(Configuration, "key_pair") as patched:
            patched.return_value = ("public", "private")
            LibraryAuthenticator.from_config(db.session, library1)
            assert patched.call_count == 1
            LibraryAuthenticator.from_config(db.session, library2)
            assert patched.call_count == 2
            assert patched.call_args_list[0] != patched.call_args_list[1]

    def test__geographic_areas(self, authenticator_fixture: AuthenticatorFixture):
        """Test the _geographic_areas helper method."""
        db = authenticator_fixture.db

        class Mock(LibraryAuthenticator):
            values = {
                Configuration.LIBRARY_FOCUS_AREA: "focus",
                Configuration.LIBRARY_SERVICE_AREA: "service",
            }

            @classmethod
            def _geographic_area(cls, key, library):
                cls.called_with = library
                return cls.values.get(key)

        # _geographic_areas calls _geographic_area twice and
        # reutrns the results in a 2-tuple.
        m = Mock._geographic_areas
        library = object()
        assert ("focus", "service") == m(library)
        assert library == Mock.called_with

        # If only one value is provided, the same value is given for both
        # areas.
        del Mock.values[Configuration.LIBRARY_FOCUS_AREA]
        assert ("service", "service") == m(library)

        Mock.values[Configuration.LIBRARY_FOCUS_AREA] = "focus"
        del Mock.values[Configuration.LIBRARY_SERVICE_AREA]
        assert ("focus", "focus") == m(library)

    def test__geographic_area(self, authenticator_fixture: AuthenticatorFixture):
        """Test the _geographic_area helper method."""
        db = authenticator_fixture.db
        library = db.default_library()
        key = "a key"
        setting = ConfigurationSetting.for_library(key, library)

        def m():
            return LibraryAuthenticator._geographic_area(key, library)

        # A missing value is returned as None.
        assert None == m()

        # The literal string "everywhere" is returned as is.
        setting.value = "everywhere"
        assert "everywhere" == m()

        # A string that makes sense as JSON is returned as its JSON
        # equivalent.
        two_states = ["NY", "NJ"]
        setting.value = json.dumps(two_states)
        assert two_states == m()

        # A string that does not make sense as JSON is put in a
        # single-element list.
        setting.value = "Arvin, CA"
        assert ["Arvin, CA"] == m()


class TestAuthenticationProvider:

    credentials = dict(username="user", password="")

    def test_external_integration(self, authenticator_fixture: AuthenticatorFixture):
        db = authenticator_fixture.db
        provider = authenticator_fixture.mock_basic(patrondata=None)
        assert (
            authenticator_fixture.mock_basic_integration
            == provider.external_integration(db.session)
        )

    def test_private_remote_patron_lookup(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        provider = authenticator_fixture.mock_basic(patrondata=None)

        # Passing a type other than Patron or PatronData to _remote_patron_lookup
        # will raise a ValueError.
        with pytest.raises(ValueError):
            provider._remote_patron_lookup(MagicMock())

    def test_authenticated_patron_passes_on_none(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        provider = authenticator_fixture.mock_basic(patrondata=None)
        patron = provider.authenticated_patron(db.session, self.credentials)
        assert None == patron

    def test_authenticated_patron_passes_on_problem_detail(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        provider = authenticator_fixture.mock_basic(
            patrondata=UNSUPPORTED_AUTHENTICATION_MECHANISM  # type: ignore
        )
        patron = provider.authenticated_patron(db.session, self.credentials)
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == patron  # type: ignore

    def test_authenticated_patron_allows_access_to_expired_credentials(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        """Even if your card has expired, you can log in -- you just can't
        borrow books.
        """
        db = authenticator_fixture.db
        yesterday = utc_now() - datetime.timedelta(days=1)

        expired = PatronData(
            permanent_id="1",
            authorization_identifier="2",
            authorization_expires=yesterday,
        )
        provider = authenticator_fixture.mock_basic(
            patrondata=expired, remote_patron_lookup_patrondata=expired
        )
        patron = provider.authenticated_patron(db.session, self.credentials)
        assert "1" == patron.external_identifier
        assert "2" == patron.authorization_identifier

    def test_authenticated_patron_updates_metadata_if_necessary(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        patron = db.patron()
        assert True == PatronUtility.needs_external_sync(patron)

        # If we authenticate this patron by username we find out their
        # permanent ID but not any other information about them.
        username = "user"
        barcode = "1234"
        incomplete_data = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=username,
            complete=False,
        )

        # If we do a lookup for this patron we will get more complete
        # information.
        complete_data = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=barcode,
            username=username,
            cached_neighborhood="Little Homeworld",
            complete=True,
        )

        provider = authenticator_fixture.mock_basic(
            patrondata=incomplete_data, remote_patron_lookup_patrondata=complete_data
        )
        patron2 = provider.authenticated_patron(db.session, self.credentials)

        # We found the right patron.
        assert patron == patron2

        # We updated their metadata.
        assert "user" == patron.username
        assert barcode == patron.authorization_identifier
        assert "Little Homeworld" == patron.cached_neighborhood

        # .cached_neighborhood (stored in the database) was reused as
        # .neighborhood (destroyed at the end of the request)
        assert "Little Homeworld" == patron.neighborhood

        # We did a patron lookup, which means we updated
        # .last_external_sync.
        assert patron.last_external_sync != None
        assert barcode == patron.authorization_identifier
        assert username == patron.username

        # Looking up the patron a second time does not cause another
        # metadata refresh, because we just did a refresh and the
        # patron has borrowing privileges.
        last_sync = patron.last_external_sync
        assert False == PatronUtility.needs_external_sync(patron)
        patron = provider.authenticated_patron(db.session, dict(username=username))
        assert last_sync == patron.last_external_sync
        assert barcode == patron.authorization_identifier
        assert username == patron.username

        # Here, patron.neighborhood was copied over from
        # patron.cached_neighborhood. It couldn't have been set by a
        # metadata refresh, because there was no refresh.
        assert "Little Homeworld" == patron.neighborhood

        # If we somehow authenticate with an identifier other than
        # the ones in the Patron record, we trigger another metadata
        # refresh to see if anything has changed.
        incomplete_data = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier="some other identifier",
            complete=False,
        )
        provider.patrondata = incomplete_data
        patron = provider.authenticated_patron(
            db.session, dict(username="someotheridentifier")
        )
        assert patron.last_external_sync > last_sync

        # But Patron.authorization_identifier doesn't actually change
        # to "some other identifier", because when we do the metadata
        # refresh we get the same data as before.
        assert barcode == patron.authorization_identifier
        assert username == patron.username

    def test_update_patron_metadata(self, authenticator_fixture: AuthenticatorFixture):
        db = authenticator_fixture.db
        patron = db.patron()
        patron.authorization_identifier = "2345"
        assert None == patron.last_external_sync
        assert None == patron.username

        patrondata = PatronData(username="user", neighborhood="Little Homeworld")
        provider = authenticator_fixture.mock_basic(
            remote_patron_lookup_patrondata=patrondata
        )
        provider.external_type_regular_expression = re.compile("^(.)")
        provider.update_patron_metadata(patron)

        # The patron's username has been changed.
        assert "user" == patron.username

        # last_external_sync has been updated.
        assert patron.last_external_sync != None

        # external_type was updated based on the regular expression
        assert "2" == patron.external_type

        # .neighborhood was not stored in .cached_neighborhood.  In
        # this case, it must be cheap to get .neighborhood every time,
        # and it's better not to store information we can get cheaply.
        assert "Little Homeworld" == patron.neighborhood
        assert None == patron.cached_neighborhood

    def test_update_patron_metadata_noop_if_no_remote_metadata(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        patron = db.patron()
        provider = authenticator_fixture.mock_basic(patrondata=None)
        provider.update_patron_metadata(patron)

        # We can tell that update_patron_metadata was a no-op because
        # patron.last_external_sync didn't change.
        assert None == patron.last_external_sync

    def test_remote_patron_lookup(self, authenticator_fixture: AuthenticatorFixture):
        """The default implementation of remote_patron_lookup returns whatever was passed in."""
        db = authenticator_fixture.db
        provider = BasicConcreteAuthenticationProvider(
            db.default_library(), db.external_integration(db.fresh_str())
        )
        assert None == provider.remote_patron_lookup(None)
        patron = db.patron()
        assert patron == provider.remote_patron_lookup(patron)
        patrondata = PatronData()
        assert patrondata == provider.remote_patron_lookup(patrondata)

    def test_update_patron_external_type(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        patron = db.patron()
        patron.authorization_identifier = "A123"
        patron.external_type = "old value"
        library = patron.library
        integration = db.external_integration(db.fresh_str())

        class MockProvider(AuthenticationProvider):
            NAME = "Just a mock"

        setting = ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MockProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            library,
            integration,
        )
        setting.value = None

        # If there is no EXTERNAL_TYPE_REGULAR_EXPRESSION, calling
        # update_patron_external_type does nothing.
        MockProvider(library, integration).update_patron_external_type(patron)
        assert "old value" == patron.external_type

        setting.value = "([A-Z])"
        MockProvider(library, integration).update_patron_external_type(patron)
        assert "A" == patron.external_type

        setting.value = "([0-9]$)"
        MockProvider(library, integration).update_patron_external_type(patron)
        assert "3" == patron.external_type

        # These regexp has no groups, so it has no power to change
        # external_type.
        setting.value = "A"
        MockProvider(library, integration).update_patron_external_type(patron)
        assert "3" == patron.external_type

        # This regexp is invalid, so it isn't used.
        setting.value = "(not a valid regexp"
        provider = MockProvider(library, integration)
        assert None == provider.external_type_regular_expression

    def test_restriction_matches(self):
        """Test the behavior of the library identifier restriction algorithm."""
        m = AuthenticationProvider._restriction_matches

        # If restriction is none, we always return True.
        assert True == m(
            "123",
            None,
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX,
        )
        assert True == m(
            "123",
            None,
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING,
        )
        assert True == m(
            "123",
            None,
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX,
        )
        assert True == m(
            "123", None, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST
        )

        # If field is None we always return False.
        assert False == m(
            None,
            "1234",
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX,
        )
        assert False == m(
            None,
            "1234",
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING,
        )
        assert False == m(
            None,
            re.compile(".*"),
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX,
        )
        assert False == m(
            None,
            ["1", "2"],
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST,
        )

        # Test prefix
        assert True == m(
            "12345a",
            "1234",
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX,
        )
        assert False == m(
            "a1234",
            "1234",
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX,
        )

        # Test string
        assert False == m(
            "12345a",
            "1234",
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING,
        )
        assert False == m(
            "a1234",
            "1234",
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING,
        )
        assert True == m(
            "1234",
            "1234",
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING,
        )

        # Test list
        assert True == m(
            "1234",
            ["1234", "4321"],
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST,
        )
        assert True == m(
            "4321",
            ["1234", "4321"],
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST,
        )
        assert False == m(
            "12345",
            ["1234", "4321"],
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST,
        )
        assert False == m(
            "54321",
            ["1234", "4321"],
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST,
        )

        # Test Regex
        assert True == m(
            "123",
            re.compile("^(12|34)"),
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX,
        )
        assert True == m(
            "345",
            re.compile("^(12|34)"),
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX,
        )
        assert False == m(
            "abc",
            re.compile("^bc"),
            AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX,
        )

    def test_enforce_library_identifier_restriction(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        """Test the enforce_library_identifier_restriction method."""
        db = authenticator_fixture.db
        provider = authenticator_fixture.mock_basic()
        m = provider.enforce_library_identifier_restriction
        patron = db.patron()
        patrondata = PatronData()

        # Test with patron rather than patrondata as argument
        assert patron == m(object(), patron)
        patron.library_id = -1
        assert False == m(object(), patron)

        # Test no restriction
        provider.library_identifier_restriction_type = (
            MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE
        )
        provider.library_identifier_restriction = "2345"
        provider.library_identifier_field = (
            MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE
        )
        assert patrondata == m("12365", patrondata)

        # Test regex against barcode
        provider.library_identifier_restriction_type = (
            MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX
        )
        provider.library_identifier_restriction = re.compile("23[46]5")
        provider.library_identifier_field = (
            MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE
        )
        assert patrondata == m("23456", patrondata)
        assert patrondata == m("2365", patrondata)
        assert False == m("2375", provider.patrondata)

        # Test prefix against barcode
        provider.library_identifier_restriction_type = (
            MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX
        )
        provider.library_identifier_restriction = "2345"
        provider.library_identifier_field = (
            MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE
        )
        assert patrondata == m("23456", patrondata)
        assert False == m("123456", patrondata)

        # Test string against barcode
        provider.library_identifier_restriction_type = (
            MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING
        )
        provider.library_identifier_restriction = "2345"
        provider.library_identifier_field = (
            MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE
        )
        assert False == m("123456", patrondata)
        assert patrondata == m("2345", patrondata)

        # Test match applied to field on patrondata not barcode
        provider.library_identifier_restriction_type = (
            MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING
        )
        provider.library_identifier_restriction = "2345"
        provider.library_identifier_field = "agent"
        patrondata.library_identifier = "2345"
        assert patrondata == m("123456", patrondata)
        patrondata.library_identifier = "12345"
        assert False == m("2345", patrondata)

    def test_patron_identifier_restriction(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        library = db.default_library()
        integration = db.external_integration(db.fresh_str())

        class MockProvider(AuthenticationProvider):
            NAME = "Just a mock"

        string_setting = ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MockProvider.LIBRARY_IDENTIFIER_RESTRICTION,
            library,
            integration,
        )

        type_setting = ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE,
            library,
            integration,
        )

        # If the type is regex its converted into a regular expression.
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX
        string_setting.value = "^abcd"
        provider = MockProvider(library, integration)
        assert "^abcd" == provider.library_identifier_restriction.pattern

        # If its type is list, make sure its converted into a list
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST
        string_setting.value = "a,b,c"
        provider = MockProvider(library, integration)
        assert ["a", "b", "c"] == provider.library_identifier_restriction

        # If its type is prefix make sure its a string
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX
        string_setting.value = "abc"
        provider = MockProvider(library, integration)
        assert "abc" == provider.library_identifier_restriction

        # If its type is string make sure its a string
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING
        string_setting.value = "abc"
        provider = MockProvider(library, integration)
        assert "abc" == provider.library_identifier_restriction

        # If its type is none make sure its actually None
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE
        string_setting.value = "abc"
        provider = MockProvider(library, integration)
        assert None == provider.library_identifier_restriction


class TestBasicAuthenticationProvider:
    def test_constructor(self, authenticator_fixture: AuthenticatorFixture):
        db = authenticator_fixture.db
        b = BasicAuthenticationProvider

        class ConfigAuthenticationProvider(BasicAuthenticationProvider):
            NAME = "Config loading test"

        integration = db.external_integration(
            db.fresh_str(), goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        db.default_library().integrations.append(integration)
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = "idre"
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = "pwre"
        integration.setting(b.TEST_IDENTIFIER).value = "username"
        integration.setting(b.TEST_PASSWORD).value = "pw"

        provider = ConfigAuthenticationProvider(db.default_library(), integration)
        assert "idre" == provider.identifier_re.pattern
        assert "pwre" == provider.password_re.pattern
        assert "username" == provider.test_username
        assert "pw" == provider.test_password

        # Test the defaults.
        integration = db.external_integration(
            db.fresh_str(), goal=ExternalIntegration.PATRON_AUTH_GOAL
        )

        provider = ConfigAuthenticationProvider(db.default_library(), integration)
        assert (
            re.compile(b.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION)
            == provider.identifier_re
        )
        assert None == provider.password_re

    def test_testing_patron(self, authenticator_fixture: AuthenticatorFixture):
        db = authenticator_fixture.db

        class MockAuthenticatedPatron(MockBasicAuthenticationProvider):
            def __init__(self, *args, **kwargs):
                self._authenticated_patron_returns = kwargs.pop(
                    "_authenticated_patron_returns", None
                )
                super().__init__(*args, **kwargs)

            def authenticated_patron(self, *args, **kwargs):
                return self._authenticated_patron_returns

        # You don't have to have a testing patron.
        integration = db.external_integration(db.fresh_str())
        no_testing_patron = BasicConcreteAuthenticationProvider(
            db.default_library(), integration
        )
        assert (None, None) == no_testing_patron.testing_patron(db.session)

        # But if you don't, testing_patron_or_bust() will raise an
        # exception.
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            no_testing_patron.testing_patron_or_bust(db.session)
        assert "No test patron identifier is configured" in str(excinfo.value)

        # We configure a testing patron but their username and
        # password don't actually authenticate anyone. We don't crash,
        # but we can't look up the testing patron either.
        b = BasicAuthenticationProvider
        integration = db.external_integration(db.fresh_str())
        integration.setting(b.TEST_IDENTIFIER).value = "1"
        integration.setting(b.TEST_PASSWORD).value = "2"
        missing_patron = MockBasicAuthenticationProvider(
            db.default_library(), integration, patron=None
        )
        value = missing_patron.testing_patron(db.session)
        assert (None, "2") == value

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as excinfo:
            missing_patron.testing_patron_or_bust(db.session)
        assert "Remote declined to authenticate the test patron." in str(excinfo.value)

        # We configure a testing patron but authenticating them
        # results in a problem detail document.
        b = BasicAuthenticationProvider
        patron = db.patron()
        integration = db.external_integration(db.fresh_str())
        integration.setting(b.TEST_IDENTIFIER).value = "1"
        integration.setting(b.TEST_PASSWORD).value = "2"
        problem_patron = MockAuthenticatedPatron(
            db.default_library(),
            integration,
            patron=patron,
            _authenticated_patron_returns=PATRON_OF_ANOTHER_LIBRARY,
        )
        value = problem_patron.testing_patron(db.session)
        assert patron != PATRON_OF_ANOTHER_LIBRARY
        assert (PATRON_OF_ANOTHER_LIBRARY, "2") == value

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as excinfo:
            problem_patron.testing_patron_or_bust(db.session)
        assert "Test patron lookup returned a problem detail" in str(excinfo.value)

        # We configure a testing patron but authenticating them
        # results in something (non None) that's not a Patron
        # or a problem detail document.
        not_a_patron = "<not a patron>"
        b = BasicAuthenticationProvider
        patron = db.patron()
        integration = db.external_integration(db.fresh_str())
        integration.setting(b.TEST_IDENTIFIER).value = "1"
        integration.setting(b.TEST_PASSWORD).value = "2"
        problem_patron = MockAuthenticatedPatron(
            db.default_library(),
            integration,
            patron=patron,
            _authenticated_patron_returns=not_a_patron,
        )
        value = problem_patron.testing_patron(db.session)
        assert patron != not_a_patron
        assert (not_a_patron, "2") == value

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as excinfo:
            problem_patron.testing_patron_or_bust(db.session)
        assert "Test patron lookup returned invalid value for patron" in str(
            excinfo.value
        )

        # Here, we configure a testing patron who is authenticated by
        # their username and password.
        patron = db.patron()
        present_patron = MockBasicAuthenticationProvider(
            db.default_library(), integration, patron=patron
        )
        value = present_patron.testing_patron(db.session)
        assert (patron, "2") == value

        # Finally, testing_patron_or_bust works, returning the same
        # value as testing_patron()
        assert value == present_patron.testing_patron_or_bust(db.session)

    def test__run_self_tests(self, authenticator_fixture: AuthenticatorFixture):
        _db = object()

        class CantAuthenticateTestPatron(BasicAuthenticationProvider):
            def __init__(self):
                pass

            def testing_patron_or_bust(self, _db):
                self.called_with = _db
                raise Exception("Nope")

        # If we can't authenticate a test patron, the rest of the tests
        # aren't even run.
        provider = CantAuthenticateTestPatron()
        [result] = list(provider._run_self_tests(_db))
        assert _db == provider.called_with
        assert False == result.success
        assert "Nope" == result.exception.args[0]

        # If we can authenticate a test patron, the patron and their
        # password are passed into the next test.

        class Mock(BasicAuthenticationProvider):
            def __init__(self, patron, password):
                self.patron = patron
                self.password = password

            def testing_patron_or_bust(self, _db):
                return self.patron, self.password

            def update_patron_metadata(self, patron):
                # The patron obtained from testing_patron_or_bust
                # is passed into update_patron_metadata.
                assert patron == self.patron
                return "some metadata"

        provider = Mock("patron", "password")
        [get_patron, update_metadata] = provider._run_self_tests(object())
        assert "Authenticating test patron" == get_patron.name
        assert True == get_patron.success
        assert (provider.patron, provider.password) == get_patron.result

        assert "Syncing patron metadata" == update_metadata.name
        assert True == update_metadata.success
        assert "some metadata" == update_metadata.result

    def test_client_configuration(self, authenticator_fixture: AuthenticatorFixture):
        """Test that client-side configuration settings are retrieved from
        ConfigurationSetting objects.
        """
        db = authenticator_fixture.db
        b = BasicConcreteAuthenticationProvider
        integration = db.external_integration(db.fresh_str())
        integration.setting(b.IDENTIFIER_KEYBOARD).value = b.EMAIL_ADDRESS_KEYBOARD
        integration.setting(b.PASSWORD_KEYBOARD).value = b.NUMBER_PAD
        integration.setting(b.IDENTIFIER_LABEL).value = "Your Library Card"
        integration.setting(b.PASSWORD_LABEL).value = "Password"
        integration.setting(b.IDENTIFIER_BARCODE_FORMAT).value = "some barcode"

        provider = b(db.default_library(), integration)

        assert b.EMAIL_ADDRESS_KEYBOARD == provider.identifier_keyboard
        assert b.NUMBER_PAD == provider.password_keyboard
        assert "Your Library Card" == provider.identifier_label
        assert "Password" == provider.password_label
        assert "some barcode" == provider.identifier_barcode_format

    def test_server_side_validation(self, authenticator_fixture: AuthenticatorFixture):
        db = authenticator_fixture.db
        b = BasicConcreteAuthenticationProvider
        integration = db.external_integration(db.fresh_str())
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = "foo"
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = "bar"

        provider = b(db.default_library(), integration)

        assert True == provider.server_side_validation("food", "barbecue")
        assert False == provider.server_side_validation("food", "arbecue")
        assert False == provider.server_side_validation("ood", "barbecue")
        assert False == provider.server_side_validation(None, None)

        # If this authenticator does not look at provided passwords,
        # then the only values that will pass validation are null
        # and the empty string.
        provider.password_keyboard = provider.NULL_KEYBOARD
        assert False == provider.server_side_validation("food", "barbecue")
        assert False == provider.server_side_validation("food", "is good")
        assert False == provider.server_side_validation("food", " ")
        assert True == provider.server_side_validation("food", None)
        assert True == provider.server_side_validation("food", "")
        provider.password_keyboard = provider.DEFAULT_KEYBOARD

        # It's okay not to provide anything for server side validation.
        # The default settings will be used.
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = None
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = None
        provider = b(db.default_library(), integration)
        assert b.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION == provider.identifier_re.pattern
        assert None == provider.password_re
        assert True == provider.server_side_validation("food", "barbecue")
        assert True == provider.server_side_validation("a", None)
        assert False == provider.server_side_validation("!@#$", None)

        # Test maximum length of identifier and password.
        integration.setting(b.IDENTIFIER_MAXIMUM_LENGTH).value = "5"
        integration.setting(b.PASSWORD_MAXIMUM_LENGTH).value = "10"
        provider = b(db.default_library(), integration)

        assert True == provider.server_side_validation("a", "1234")
        assert False == provider.server_side_validation("a", "123456789012345")
        assert False == provider.server_side_validation("abcdefghijklmnop", "1234")

        # You can disable the password check altogether by setting maximum
        # length to zero.
        integration.setting(b.PASSWORD_MAXIMUM_LENGTH).value = "0"
        provider = b(db.default_library(), integration)
        assert True == provider.server_side_validation("a", None)

    def test_local_patron_lookup(self, authenticator_fixture: AuthenticatorFixture):
        db = authenticator_fixture.db
        # This patron of another library looks just like the patron
        # we're about to create, but will never be selected.
        other_library = db.library()
        other_library_patron = db.patron("patron1_ext_id", library=other_library)
        other_library_patron.authorization_identifier = "patron1_auth_id"
        other_library_patron.username = "patron1"

        patron1 = db.patron("patron1_ext_id")
        patron1.authorization_identifier = "patron1_auth_id"
        patron1.username = "patron1"

        patron2 = db.patron("patron2_ext_id")
        patron2.authorization_identifier = "patron2_auth_id"
        patron2.username = "patron2"
        db.session.commit()

        provider = authenticator_fixture.mock_basic()

        # If we provide PatronData associated with patron1, we look up
        # patron1, even though we provided the username associated
        # with patron2.
        for patrondata_args in [
            dict(permanent_id=patron1.external_identifier),
            dict(authorization_identifier=patron1.authorization_identifier),
            dict(username=patron1.username),
            dict(
                permanent_id=PatronData.NO_VALUE,
                username=PatronData.NO_VALUE,
                authorization_identifier=patron1.authorization_identifier,
            ),
        ]:
            patrondata = PatronData(**patrondata_args)
            assert patron1 == provider.local_patron_lookup(
                db.session, patron2.authorization_identifier, patrondata
            )

        # If no PatronData is provided, we can look up patron1 either
        # by authorization identifier or username, but not by
        # permanent identifier.
        assert patron1 == provider.local_patron_lookup(
            db.session, patron1.authorization_identifier, None
        )
        assert patron1 == provider.local_patron_lookup(
            db.session, patron1.username, None
        )
        assert None == provider.local_patron_lookup(
            db.session, patron1.external_identifier, None
        )

    def test_get_credential_from_header(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        provider = authenticator_fixture.mock_basic()
        assert None == provider.get_credential_from_header("Bearer [some token]")
        assert None == provider.get_credential_from_header(dict())
        assert "foo" == provider.get_credential_from_header(dict(password="foo"))

    def test_authentication_flow_document(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        """Test the default authentication provider document."""
        db = authenticator_fixture.db
        provider = authenticator_fixture.mock_basic()
        provider.identifier_maximum_length = 22
        provider.password_maximum_length = 7
        provider.identifier_barcode_format = provider.BARCODE_FORMAT_CODABAR

        # We're about to call url_for, so we must create an
        # application context.
        os.environ["AUTOINITIALIZE"] = "False"
        from api.app import app

        self.app = app
        del os.environ["AUTOINITIALIZE"]
        with self.app.test_request_context("/"):
            doc = provider.authentication_flow_document(db.session)
            assert _(provider.DISPLAY_NAME) == doc["description"]
            assert provider.FLOW_TYPE == doc["type"]

            labels = doc["labels"]
            assert provider.identifier_label == labels["login"]
            assert provider.password_label == labels["password"]

            inputs = doc["inputs"]
            assert provider.identifier_keyboard == inputs["login"]["keyboard"]
            assert provider.password_keyboard == inputs["password"]["keyboard"]

            assert provider.BARCODE_FORMAT_CODABAR == inputs["login"]["barcode_format"]

            assert (
                provider.identifier_maximum_length == inputs["login"]["maximum_length"]
            )
            assert (
                provider.password_maximum_length == inputs["password"]["maximum_length"]
            )

            [logo_link] = doc["links"]
            assert "logo" == logo_link["rel"]
            assert (
                "http://localhost/images/" + MockBasic.LOGIN_BUTTON_IMAGE
                == logo_link["href"]
            )

    def test_remote_patron_lookup(self, authenticator_fixture: AuthenticatorFixture):
        # remote_patron_lookup does the lookup by calling _remote_patron_lookup,
        # then calls enforce_library_identifier_restriction to make sure that the patron
        # is associated with the correct library
        db = authenticator_fixture.db

        class Mock(BasicAuthenticationProvider):
            def _remote_patron_lookup(self, patron_or_patrondata):
                self._remote_patron_lookup_called_with = patron_or_patrondata
                return patron_or_patrondata

            def enforce_library_identifier_restriction(self, identifier, patrondata):
                self.enforce_library_identifier_restriction_called_with = (
                    identifier,
                    patrondata,
                )
                return "Result"

        integration = db.external_integration(
            db.fresh_str(), ExternalIntegration.PATRON_AUTH_GOAL
        )
        provider = Mock(db.default_library(), integration)
        patron = db.patron()
        assert "Result" == provider.remote_patron_lookup(patron)
        assert provider._remote_patron_lookup_called_with == patron
        assert provider.enforce_library_identifier_restriction_called_with == (
            patron.authorization_identifier,
            patron,
        )

    def test_scrub_credential(self, authenticator_fixture: AuthenticatorFixture):
        # Verify that the scrub_credential helper method strips extra whitespace
        # and nothing else.
        db = authenticator_fixture.db

        integration = db.external_integration(
            db.fresh_str(), ExternalIntegration.PATRON_AUTH_GOAL
        )
        provider = BasicConcreteAuthenticationProvider(
            db.default_library(), integration
        )
        m = provider.scrub_credential

        assert None == provider.scrub_credential(None)
        assert 1 == provider.scrub_credential(1)
        o = object()
        assert o == provider.scrub_credential(o)
        assert "user" == provider.scrub_credential("user")
        assert "user" == provider.scrub_credential(" user")
        assert "user" == provider.scrub_credential(" user ")
        assert "user" == provider.scrub_credential("    \ruser\t     ")
        assert b"user" == provider.scrub_credential(b" user ")


class TestBasicAuthenticationProviderAuthenticate:
    """Test the complex BasicAuthenticationProvider.authenticate method."""

    # A dummy set of credentials, for use when the exact details of
    # the credentials passed in are not important.
    credentials = dict(username="user", password="pass")

    def test_success(self, authenticator_fixture: AuthenticatorFixture):
        db = authenticator_fixture.db
        patron = db.patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = authenticator_fixture.mock_basic(patrondata=patrondata)

        # authenticate() calls remote_authenticate(), which returns the
        # queued up PatronData object. The corresponding Patron is then
        # looked up in the database.

        # BasicAuthenticationProvider scrubs leading and trailing spaces from
        # the credentials.
        credentials_with_spaces = dict(username="  user ", password=" pass \t ")
        for creds in (self.credentials, credentials_with_spaces):
            assert patron == provider.authenticate(db.session, self.credentials)

        # All the different ways the database lookup might go are covered in
        # test_local_patron_lookup. This test only covers the case where
        # the server sends back the permanent ID of the patron.

    def _inactive_patron(self, db: DatabaseTransactionFixture):
        """Simulate a patron who has not logged in for a really long time.

        :return: A 2-tuple (Patron, PatronData). The Patron contains
        'out-of-date' data and the PatronData containing 'up-to-date'
        data.
        """
        now = utc_now()
        long_ago = now - datetime.timedelta(hours=10000)
        patron = db.patron()
        patron.last_external_sync = long_ago

        # All of their authorization information has changed in the
        # meantime, but -- crucially -- their permanent ID has not.
        patron.authorization_identifier = "old auth id"
        patron.username = "old username"

        # Here is the up-to-date information about this patron,
        # as found in the 'ILS'.
        patrondata = PatronData(
            permanent_id=patron.external_identifier,
            username="new username",
            authorization_identifier="new authorization identifier",
            complete=True,
        )

        return patron, patrondata

    def test_success_but_local_patron_needs_sync(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db

        # This patron has not logged on in a really long time.
        patron, complete_patrondata = self._inactive_patron(db)

        # The 'ILS' will respond to an authentication request with a minimal
        # set of information.
        #
        # It will respond to a patron lookup request with more detailed
        # information.
        minimal_patrondata = PatronData(
            permanent_id=patron.external_identifier, complete=False
        )
        provider = authenticator_fixture.mock_basic(
            patrondata=minimal_patrondata,
            remote_patron_lookup_patrondata=complete_patrondata,
        )

        # The patron can be authenticated.
        assert patron == provider.authenticate(db.session, self.credentials)

        # The Authenticator noticed that the patron's account was out
        # of sync, and since the authentication response did not
        # provide a complete set of patron information, the
        # Authenticator performed a more detailed lookup to make sure
        # that the patron's details were correct going forward.
        assert "new username" == patron.username
        assert "new authorization identifier" == patron.authorization_identifier
        assert (utc_now() - patron.last_external_sync).total_seconds() < 10

    def test_success_with_immediate_patron_sync(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        # This patron has not logged on in a really long time.
        db = authenticator_fixture.db
        patron, complete_patrondata = self._inactive_patron(db)

        # The 'ILS' will respond to an authentication request with a complete
        # set of information. If a remote patron lookup were to happen,
        # it would explode.
        provider = authenticator_fixture.mock_basic(
            patrondata=complete_patrondata, remote_patron_lookup_patrondata=object()
        )

        # The patron can be authenticated.
        assert patron == provider.authenticate(db.session, self.credentials)

        # Since the authentication response provided a complete
        # overview of the patron, the Authenticator was able to sync
        # the account immediately, without doing a separate remote
        # patron lookup.
        assert "new username" == patron.username
        assert "new authorization identifier" == patron.authorization_identifier
        assert (utc_now() - patron.last_external_sync).total_seconds() < 10

    def test_failure_when_remote_authentication_returns_problemdetail(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        patron = db.patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = authenticator_fixture.mock_basic(
            patrondata=UNSUPPORTED_AUTHENTICATION_MECHANISM  # type: ignore
        )
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == provider.authenticate(  # type: ignore
            db.session, self.credentials
        )

    def test_failure_when_remote_authentication_returns_none(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        patron = db.patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = authenticator_fixture.mock_basic(patrondata=None)
        assert None == provider.authenticate(db.session, self.credentials)

    def test_server_side_validation_runs(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        patron = db.patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)

        b = MockBasic
        integration = db.external_integration(db.fresh_str())
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = "foo"
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = "bar"
        provider = b(db.default_library(), integration, patrondata=patrondata)

        # This would succeed, but we don't get to remote_authenticate()
        # because we fail the regex test.
        assert None == provider.authenticate(db.session, self.credentials)

        # This succeeds because we pass the regex test.
        assert patron == provider.authenticate(
            db.session, dict(username="food", password="barbecue")
        )

    def test_authentication_succeeds_but_patronlookup_fails(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        """This case should never happen--it indicates a malfunctioning
        authentication provider. But we handle it.
        """
        db = authenticator_fixture.db
        patrondata = PatronData(permanent_id=db.fresh_str())
        provider = authenticator_fixture.mock_basic(patrondata=patrondata)

        # When we call remote_authenticate(), we get patrondata, but
        # there is no corresponding local patron, so we call
        # remote_patron_lookup() for details, and we get nothing.  At
        # this point we give up -- there is no authenticated patron.
        assert None == provider.authenticate(db.session, self.credentials)

    def test_authentication_creates_missing_patron(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        # The authentication provider knows about this patron,
        # but this is the first we've heard about them.
        patrondata = PatronData(
            permanent_id=db.fresh_str(),
            authorization_identifier=db.fresh_str(),
            fines=Money(1, "USD"),
        )

        library = db.library()
        integration = db.external_integration(
            db.fresh_str(), ExternalIntegration.PATRON_AUTH_GOAL
        )
        provider = MockBasic(
            library,
            integration,
            patrondata=patrondata,
            remote_patron_lookup_patrondata=patrondata,
        )
        patron = provider.authenticate(db.session, self.credentials)

        # A server side Patron was created from the PatronData.
        assert isinstance(patron, Patron)
        assert library == patron.library  # type: ignore
        assert patrondata.permanent_id == patron.external_identifier
        assert patrondata.authorization_identifier == patron.authorization_identifier

        # Information not relevant to the patron's identity was stored
        # in the Patron object after it was created.
        assert 1 == patron.fines

    def test_authentication_updates_outdated_patron_on_permanent_id_match(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        # A patron's permanent ID won't change.
        permanent_id = db.fresh_str()

        # But this patron has not used the circulation manager in a
        # long time, and their other identifiers are out of date.
        old_identifier = "1234"
        old_username = "user1"
        patron = db.patron(old_identifier)
        patron.external_identifier = permanent_id
        patron.username = old_username

        # The authorization provider has all the new information about
        # this patron.
        new_identifier = "5678"
        new_username = "user2"
        patrondata = PatronData(
            permanent_id=permanent_id,
            authorization_identifier=new_identifier,
            username=new_username,
        )

        provider = authenticator_fixture.mock_basic(patrondata=patrondata)
        provider.external_type_regular_expression = re.compile("^(.)")
        patron2 = provider.authenticate(db.session, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider.
        assert patron2 == patron

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        assert new_identifier == patron.authorization_identifier
        assert new_username == patron.username
        assert patron.authorization_identifier[0] == patron.external_type

    def test_authentication_updates_outdated_patron_on_username_match(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        # This patron has no permanent ID. Their library card number has
        # changed but their username has not.
        old_identifier = "1234"
        new_identifier = "5678"
        username = "user1"
        patron = db.patron(old_identifier)
        patron.external_identifier = None
        patron.username = username

        # The authorization provider has all the new information about
        # this patron.
        patrondata = PatronData(
            authorization_identifier=new_identifier,
            username=username,
        )

        provider = authenticator_fixture.mock_basic(patrondata=patrondata)
        patron2 = provider.authenticate(db.session, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider, based on the username match.
        assert patron2 == patron

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        assert new_identifier == patron.authorization_identifier

    def test_authentication_updates_outdated_patron_on_authorization_identifier_match(
        self, authenticator_fixture: AuthenticatorFixture
    ):
        db = authenticator_fixture.db
        # This patron has no permanent ID. Their username has
        # changed but their library card number has not.
        identifier = "1234"
        old_username = "user1"
        new_username = "user2"
        patron = db.patron()
        patron.external_identifier = None
        patron.authorization_identifier = identifier
        patron.username = old_username

        # The authorization provider has all the new information about
        # this patron.
        patrondata = PatronData(
            authorization_identifier=identifier,
            username=new_username,
        )

        provider = authenticator_fixture.mock_basic(patrondata=patrondata)
        patron2 = provider.authenticate(db.session, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider, based on the username match.
        assert patron2 == patron

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        assert new_username == patron.username

    # Notice what's missing: If a patron has no permanent identifier,
    # _and_ their username and authorization identifier both change,
    # then we have no way of locating them in our database. They will
    # appear no different to us than a patron who has never used the
    # circulation manager before.


class SirsiDynixAuthenticatorFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.integration = db.external_integration(
            "api.sirsidynix",
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            settings={
                ExternalIntegration.URL: "http://example.org/sirsi",
                SirsiDynixHorizonAuthenticationProvider.Keys.CLIENT_ID: "clientid",
                SirsiDynixHorizonAuthenticationProvider.Keys.LIBRARY_ID: "libraryid",
                SirsiDynixHorizonAuthenticationProvider.Keys.LIBRARY_PREFIX: "test",
            },
        )

        with patch.dict(os.environ, {Configuration.SIRSI_DYNIX_APP_ID: "UNITTEST"}):
            self.api = SirsiDynixHorizonAuthenticationProvider(
                db.default_library(), self.integration
            )


@pytest.fixture(scope="function")
def sirsi_fixture(db: DatabaseTransactionFixture) -> SirsiDynixAuthenticatorFixture:
    return SirsiDynixAuthenticatorFixture(db)


class TestSirsiDynixAuthenticationProvider:
    def _headers(self, api):
        return {
            "SD-Originating-App-Id": api.sirsi_app_id,
            "SD-Working-LibraryID": api.sirsi_library_id,
            "x-sirs-clientID": api.sirsi_client_id,
        }

    def test_settings(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        # trailing slash appended to the preset server url
        assert sirsi_fixture.api.server_url == "http://example.org/sirsi/"
        assert sirsi_fixture.api.sirsi_client_id == "clientid"
        assert sirsi_fixture.api.sirsi_app_id == "UNITTEST"
        assert sirsi_fixture.api.sirsi_library_id == "libraryid"
        assert sirsi_fixture.api.sirsi_library_prefix == "test"

    def test_api_patron_login(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        response_dict = {"sessionToken": "xxxx", "patronKey": "test"}
        with patch(
            "api.sirsidynix_authentication_provider.HTTP.request_with_timeout"
        ) as mock_request:
            mock_request.return_value = MockRequestsResponse(200, content=response_dict)
            response = sirsi_fixture.api.api_patron_login("username", "pwd")

            assert mock_request.call_count == 1
            assert mock_request.call_args == call(
                "POST",
                "http://example.org/sirsi/user/patron/login",
                json=dict(login="username", password="pwd"),
                headers=self._headers(sirsi_fixture.api),
            )
            assert response == response_dict

            mock_request.return_value = MockRequestsResponse(401, content=response_dict)
            assert sirsi_fixture.api.api_patron_login("username", "pwd") == False

    def test_remote_authenticate(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        with patch(
            "api.sirsidynix_authentication_provider.HTTP.request_with_timeout"
        ) as mock_request:
            response_dict = {"sessionToken": "xxxx", "patronKey": "test"}
            mock_request.return_value = MockRequestsResponse(200, content=response_dict)

            response = sirsi_fixture.api.remote_authenticate("username", "pwd")
            assert type(response) == SirsiDynixPatronData
            assert response.authorization_identifier == "username"
            assert response.username == "username"
            assert response.permanent_id == "test"

            mock_request.return_value = MockRequestsResponse(401, content=response_dict)
            assert sirsi_fixture.api.remote_authenticate("username", "pwd") == False

    def test_remote_patron_lookup(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        # Test the happy path, patron OK, some fines
        ok_patron_resp = {
            "fields": {
                "displayName": "Test User",
                "approved": True,
                "patronType": {"key": "testtype"},
            }
        }
        patron_status_resp = {
            "fields": {
                "estimatedFines": {
                    "amount": "50.00",
                    "currencyCode": "USD",
                }
            }
        }
        sirsi_fixture.api.api_read_patron_data = MagicMock(return_value=ok_patron_resp)
        sirsi_fixture.api.api_patron_status_info = MagicMock(
            return_value=patron_status_resp
        )
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )

        assert sirsi_fixture.api.api_read_patron_data.call_count == 1
        assert sirsi_fixture.api.api_patron_status_info.call_count == 1
        assert patrondata.personal_name == "Test User"
        assert patrondata.fines == 50.00
        assert patrondata.block_reason == PatronData.NO_VALUE

        # Test the defensive code
        # Test no session token
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token=None)
        )
        assert patrondata == None

        # Test incorrect patrondata type
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            PatronData(permanent_id="xxxx")
        )
        assert patrondata == None

        # Test bad patron read data
        bad_patron_resp = {"bad": "yes"}
        sirsi_fixture.api.api_read_patron_data = MagicMock(return_value=bad_patron_resp)
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata == None

        not_approved_patron_resp = {
            "fields": {"approved": False, "patronType": {"key": "testtype"}}
        }
        sirsi_fixture.api.api_read_patron_data = MagicMock(
            return_value=not_approved_patron_resp
        )
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata.block_reason == SirsiBlockReasons.NOT_APPROVED

        # Test bad patronType prefix
        bad_prefix_patron_resp = {
            "fields": {"approved": True, "patronType": {"key": "nottesttype"}}
        }
        sirsi_fixture.api.api_read_patron_data = MagicMock(
            return_value=bad_prefix_patron_resp
        )
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata == PATRON_OF_ANOTHER_LIBRARY

        # Test blocked patron types
        bad_prefix_patron_resp = {
            "fields": {"approved": True, "patronType": {"key": "testblocked"}}
        }
        sirsi_fixture.api.sirsi_disallowed_suffixes = ["blocked"]
        sirsi_fixture.api.api_read_patron_data = MagicMock(
            return_value=bad_prefix_patron_resp
        )
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata.block_reason == SirsiBlockReasons.PATRON_BLOCKED

        # Test bad patron status info
        sirsi_fixture.api.api_read_patron_data.return_value = ok_patron_resp
        sirsi_fixture.api.api_patron_status_info.return_value = False
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata == None

    def test__request(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        # Leading slash on the path is not allowed, as it overwrites the urljoin prefix
        with pytest.raises(ValueError):
            sirsi_fixture.api._request("GET", "/leadingslash")

    def test_blocked_patron_status_info(
        self, sirsi_fixture: SirsiDynixAuthenticatorFixture
    ):
        patron_info = {
            "itemsCheckedOutCount": 0,
            "itemsCheckedOutMax": 25,
            "hasMaxItemsCheckedOut": False,
            "fines": {"currencyCode": "USD", "amount": "0.00"},
            "finesMax": {"currencyCode": "USD", "amount": "5.00"},
            "hasMaxFines": False,
            "itemsClaimsReturnedCount": 0,
            "itemsClaimsReturnedMax": 10,
            "hasMaxItemsClaimsReturned": False,
            "lostItemCount": 0,
            "lostItemMax": 15,
            "hasMaxLostItem": False,
            "overdueItemCount": 0,
            "overdueItemMax": 50,
            "hasMaxOverdueItem": False,
            "overdueDays": 0,
            "overdueDaysMax": 9999,
            "hasMaxOverdueDays": False,
            "daysWithFines": 0,
            "daysWithFinesMax": None,
            "hasMaxDaysWithFines": False,
            "availableHoldCount": 0,
            "datePrivilegeExpires": "2024-09-14",
            "estimatedOverdueCount": 0,
            "expired": False,
            "amountOwed": {"currencyCode": "USD", "amount": "0.00"},
        }

        statuses = [
            ({"hasMaxDaysWithFines": True}, PatronData.EXCESSIVE_FINES),
            ({"hasMaxFines": True}, PatronData.EXCESSIVE_FINES),
            ({"hasMaxLostItem": True}, PatronData.TOO_MANY_LOST),
            ({"hasMaxOverdueDays": True}, PatronData.TOO_MANY_OVERDUE),
            ({"hasMaxOverdueItem": True}, PatronData.TOO_MANY_OVERDUE),
            ({"hasMaxItemsCheckedOut": True}, PatronData.TOO_MANY_LOANS),
            ({"expired": True}, SirsiBlockReasons.EXPIRED),
            ({}, PatronData.NO_VALUE),  # No bad data = not blocked
        ]
        ok_patron_resp = {
            "fields": {
                "displayName": "Test User",
                "approved": True,
                "patronType": {"key": "testtype"},
            }
        }

        for status, reason in statuses:
            info_copy = deepcopy(patron_info)
            info_copy.update(status)

            sirsi_fixture.api.api_read_patron_data = MagicMock(
                return_value=ok_patron_resp
            )
            sirsi_fixture.api.api_patron_status_info = MagicMock(
                return_value={"fields": info_copy}
            )

            data = sirsi_fixture.api.remote_patron_lookup(
                SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
            )
            assert data.block_reason == reason

    def test_api_methods(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        """The patron data and patron status methods are almost identical in functionality
        They just hit different APIs, so we only test the difference in endpoints
        """
        api_methods = [
            ("api_read_patron_data", "http://localhost/user/patron/key/patronkey"),
            (
                "api_patron_status_info",
                "http://localhost/user/patronStatusInfo/key/patronkey",
            ),
        ]
        with patch(
            "api.sirsidynix_authentication_provider.HTTP.request_with_timeout"
        ) as mock_request:
            for api_method, uri in api_methods:
                test_method = getattr(sirsi_fixture.api, api_method)

                mock_request.return_value = MockRequestsResponse(
                    200, content=dict(success=True)
                )
                response = test_method("patronkey", "sessiontoken")
                args = mock_request.call_args
                args.args == ("GET", uri)
                assert response == dict(success=True)

                mock_request.return_value = MockRequestsResponse(400)
                response = test_method("patronkey", "sessiontoken")
                assert response == False
