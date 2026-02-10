"""Test the base authentication framework: that is, the classes that
don't interact with any particular source of truth.
"""

from __future__ import annotations

import datetime
import json
import re
from collections.abc import Callable
from contextlib import nullcontext
from decimal import Decimal
from functools import partial
from typing import TYPE_CHECKING, Literal, cast
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from _pytest._code import ExceptionInfo
from flask import url_for
from freezegun import freeze_time
from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from palace.manager.api.annotations import AnnotationWriter
from palace.manager.api.authentication.access_token import PatronJWEAccessTokenProvider
from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authentication.basic import (
    BarcodeFormats,
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
    Keyboards,
    LibraryIdenfitierRestrictionField,
    LibraryIdentifierRestriction,
)
from palace.manager.api.authentication.basic_token import (
    BasicTokenAuthenticationProvider,
)
from palace.manager.api.authenticator import (
    Authenticator,
    BaseSAMLAuthenticationProvider,
    BearerTokenType,
    CirculationPatronProfileStorage,
    LibraryAuthenticator,
)
from palace.manager.api.config import Configuration
from palace.manager.api.problem_details import (
    LIBRARY_NOT_FOUND,
    PATRON_AUTH_ACCESS_TOKEN_EXPIRED,
    PATRON_OF_ANOTHER_LIBRARY,
    UNSUPPORTED_AUTHENTICATION_MECHANISM,
)
from palace.manager.api.util.patron import PatronUtility
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.exceptions import IntegrationException
from palace.manager.core.user_profile import ProfileController
from palace.manager.integration.goals import Goals
from palace.manager.integration.patron_auth.millenium_patron import (
    MilleniumPatronAPI,
    MilleniumPatronSettings,
)
from palace.manager.integration.patron_auth.simple_authentication import (
    SimpleAuthenticationProvider,
)
from palace.manager.integration.patron_auth.sip2.provider import (
    SIP2AuthenticationProvider,
    SIP2Settings,
)
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.service.integration_registry.base import IntegrationRegistry
from palace.manager.sqlalchemy.constants import LinkRelations
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library, LibraryLogo
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util import MoneyUtility
from palace.manager.util.authentication_for_opds import AuthenticationForOPDSDocument
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import RemoteIntegrationException
from palace.manager.util.opds_writer import OPDSFeed
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.announcements import AnnouncementFixture
from tests.fixtures.library import LibraryFixture
from tests.mocks.analytics_provider import MockAnalyticsProvider

if TYPE_CHECKING:
    from tests.fixtures.api_controller import ControllerFixture
    from tests.fixtures.database import DatabaseTransactionFixture
    from tests.fixtures.vendor_id import VendorIDFixture


class MockBasic(
    BasicAuthenticationProvider[
        BasicAuthProviderSettings, BasicAuthProviderLibrarySettings
    ]
):
    """A second mock basic authentication provider for use in testing
    the workflow around Basic Auth.
    """

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: BasicAuthProviderSettings | None = None,
        library_settings: BasicAuthProviderLibrarySettings | None = None,
        analytics: Analytics | None = None,
        patrondata: PatronData | ProblemDetail | None = None,
        lookup_patrondata: PatronData | ProblemDetail | None | Literal[False] = False,
    ):
        settings = settings or self.settings_class()()
        library_settings = library_settings or self.library_settings_class()()
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )
        self.patrondata = patrondata
        if lookup_patrondata is False:
            lookup_patrondata = patrondata
        self.lookup_patrondata = lookup_patrondata

    @classmethod
    def settings_class(cls) -> type[BasicAuthProviderSettings]:
        return BasicAuthProviderSettings

    @classmethod
    def library_settings_class(cls) -> type[BasicAuthProviderLibrarySettings]:
        return BasicAuthProviderLibrarySettings

    @classmethod
    def label(cls) -> str:
        return "Mock"

    @classmethod
    def description(cls) -> str:
        return "A mock authentication provider"

    @property
    def login_button_image(self) -> str | None:
        return "login.png"

    def remote_authenticate(self, username, password):
        return self.patrondata

    def remote_patron_lookup(self, patrondata):
        return self.lookup_patrondata


MockBasicFixture = Callable[..., MockBasic]


@pytest.fixture
def mock_integration_id() -> int:
    return 1


@pytest.fixture
def mock_basic(
    db: DatabaseTransactionFixture, mock_integration_id: int
) -> MockBasicFixture:
    library_id = db.default_library().id
    assert library_id is not None
    return partial(
        MockBasic,
        library_id=library_id,
        integration_id=mock_integration_id,
    )


@pytest.fixture
def patron_data() -> PatronData:
    return PatronData(
        permanent_id="1",
        authorization_identifier="2",
        username="3",
        personal_name="4",
        email_address="5",
        authorization_expires=utc_now(),
        fines=MoneyUtility.parse(6),
        block_reason=PatronData.NO_VALUE,
    )


InactivePatronFixture = tuple[Patron, PatronData]


@pytest.fixture
def inactive_patron(db: DatabaseTransactionFixture) -> InactivePatronFixture:
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


class TestPatronData:
    def test_to_dict(self, patron_data: PatronData):
        data = patron_data.to_dict
        expect = dict(
            permanent_id="1",
            authorization_identifier="2",
            authorization_identifiers=["2"],
            external_type=None,
            username="3",
            personal_name="4",
            email_address="5",
            authorization_expires=patron_data.authorization_expires.strftime(
                "%Y-%m-%d"
            ),
            fines="6.00",
            block_reason=None,
        )
        assert data == expect

        # Test with an empty fines field
        patron_data.fines = PatronData.NO_VALUE
        data = patron_data.to_dict
        expect["fines"] = None
        assert data == expect

        # Test with a zeroed-out fines field
        patron_data.fines = Decimal(0.0)
        data = patron_data.to_dict
        expect["fines"] = "0"
        assert data == expect

        # Test with an empty expiration time
        patron_data.authorization_expires = PatronData.NO_VALUE
        data = patron_data.to_dict
        expect["authorization_expires"] = None
        assert data == expect

    def test_apply(self, patron_data: PatronData, db: DatabaseTransactionFixture):
        patron = db.patron()

        patron_data.apply(patron)
        assert patron_data.permanent_id == patron.external_identifier
        assert patron_data.authorization_identifier == patron.authorization_identifier
        assert patron_data.username == patron.username
        assert patron_data.authorization_expires == patron.authorization_expires
        assert patron_data.fines == patron.fines
        assert None == patron.block_reason

        # This data is stored in PatronData but not applied to Patron.
        assert "4" == patron_data.personal_name
        assert False == hasattr(patron, "personal_name")
        assert "5" == patron_data.email_address
        assert False == hasattr(patron, "email_address")

    def test_apply_block_reason(
        self, patron_data: PatronData, db: DatabaseTransactionFixture
    ):
        """If the PatronData has a reason why a patron is blocked,
        the reason is put into the Patron record.
        """
        patron_data.block_reason = PatronData.UNKNOWN_BLOCK
        patron = db.patron()
        patron_data.apply(patron)
        assert PatronData.UNKNOWN_BLOCK == patron.block_reason

    def test_apply_multiple_authorization_identifiers(
        self, db: DatabaseTransactionFixture
    ):
        """If there are multiple authorization identifiers, the first
        one is chosen.
        """
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
        self, patron_data: PatronData, db: DatabaseTransactionFixture
    ):
        """Patron.last_external_sync is only updated when apply() is called on
        a PatronData object that represents a full set of metadata.
        What constitutes a 'full set' depends on the authentication
        provider.
        """
        patron = db.patron()
        patron_data.complete = False
        patron_data.apply(patron)
        assert None == patron.last_external_sync
        patron_data.complete = True
        patron_data.apply(patron)
        assert None != patron.last_external_sync

    def test_apply_sets_first_valid_authorization_identifier(
        self, patron_data: PatronData, db: DatabaseTransactionFixture
    ):
        """If the ILS has multiple authorization identifiers for a patron, the
        first one is used.
        """
        patron = db.patron()
        patron.authorization_identifier = None
        patron_data.set_authorization_identifier(["identifier 1", "identifier 2"])
        patron_data.apply(patron)
        assert "identifier 1" == patron.authorization_identifier

    def test_apply_leaves_valid_authorization_identifier_alone(
        self, patron_data: PatronData, db: DatabaseTransactionFixture
    ):
        """If the ILS says a patron has a new preferred authorization
        identifier, but our Patron record shows them using an
        authorization identifier that still works, we don't change it.
        """
        patron = db.patron()
        patron.authorization_identifier = "old identifier"
        patron_data.set_authorization_identifier(
            ["new identifier", patron.authorization_identifier]
        )
        patron_data.apply(patron)
        assert "old identifier" == patron.authorization_identifier

    def test_apply_overwrites_invalid_authorization_identifier(
        self, patron_data: PatronData, db: DatabaseTransactionFixture
    ):
        """If the ILS says a patron has a new preferred authorization
        identifier, and our Patron record shows them using an
        authorization identifier that no longer works, we change it.
        """
        patron = db.patron()
        patron_data.set_authorization_identifier(["identifier 1", "identifier 2"])
        patron_data.apply(patron)
        assert "identifier 1" == patron.authorization_identifier

    def test_apply_on_incomplete_information(self, db: DatabaseTransactionFixture):
        """When we call apply() based on incomplete information (most
        commonly, the fact that a given string was successfully used
        to authenticate a patron), we are very careful about modifying
        data already in the database.
        """
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
        patron.fines = MoneyUtility.parse(10)
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
        assert patron.last_external_sync is None

    def test_get_or_create_patron(
        self, patron_data: PatronData, db: DatabaseTransactionFixture
    ):
        analytics = MockAnalyticsProvider()

        # The patron didn't exist yet, so it was created
        # and an analytics event was sent.
        default_library = db.default_library()
        patron, is_new = patron_data.get_or_create_patron(
            db.session, default_library.id, analytics
        )
        assert patron.authorization_identifier == "2"
        assert default_library == patron.library
        assert is_new is True
        assert analytics.last_event_type == CirculationEvent.NEW_PATRON
        assert analytics.count == 1

        # The same patron is returned, and no analytics
        # event was sent.
        patron, is_new = patron_data.get_or_create_patron(
            db.session, default_library.id, analytics
        )
        assert patron.authorization_identifier == "2"
        assert is_new is False
        assert analytics.count == 1

    def test_to_response_parameters(self, patron_data: PatronData):
        params = patron_data.to_response_parameters
        assert dict(name="4") == params
        patron_data.personal_name = None
        params = patron_data.to_response_parameters
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
        assert len(doc["links"]) == 1

        # Now there's authdata configured, and the DRM fields are populated with
        # the vendor ID and a short client token
        vendor_id_fixture.initialize_adobe(patron.library)

        doc = storage.profile_document
        [adobe] = doc["drm"]
        assert adobe["drm:vendor"] == "vendor id"
        assert patron.library.short_name is not None
        assert adobe["drm:clientToken"].startswith(
            patron.library.short_name.upper() + "TOKEN"
        )
        assert (
            adobe["drm:scheme"] == "http://librarysimplified.org/terms/drm/scheme/ACS"
        )
        [devices_link, annotations_link] = doc["links"]
        assert annotations_link["rel"] == "http://www.w3.org/ns/oa#annotationService"
        assert (
            annotations_link["href"]
            == "http://host/annotations?library_short_name=default"
        )
        assert annotations_link["type"] == AnnotationWriter.CONTENT_TYPE
        assert devices_link["rel"] == LinkRelations.DEVICE_REGISTRATION


class TestAuthenticator:
    def test_init(
        self,
        controller_fixture: ControllerFixture,
    ):
        db = controller_fixture.db

        # The default library has already been configured to use the
        # SimpleAuthenticationProvider for its basic auth.
        l1 = db.default_library()
        l1.short_name = "l1"

        # This library uses Millenium Patron.
        l2 = db.library(short_name="l2")
        db.auth_integration(
            MilleniumPatronAPI,
            l2,
            settings=MilleniumPatronSettings(url="http://url.com/"),
        )

        db.session.flush()

        analytics = cast(Analytics, MockAnalyticsProvider())

        auth = Authenticator(db.session, db.session.query(Library), analytics)

        # A LibraryAuthenticator has been created for each Library.
        assert "l1" in auth.library_authenticators
        assert "l2" in auth.library_authenticators
        assert isinstance(auth.library_authenticators["l1"], LibraryAuthenticator)
        assert isinstance(auth.library_authenticators["l2"], LibraryAuthenticator)

        # Each LibraryAuthenticator has been associated with an
        # appropriate AuthenticationProvider.

        assert auth.library_authenticators["l1"].basic_auth_provider is not None
        assert isinstance(
            auth.library_authenticators["l1"].basic_auth_provider,
            SimpleAuthenticationProvider,
        )
        assert auth.library_authenticators["l2"].basic_auth_provider is not None
        assert isinstance(
            auth.library_authenticators["l2"].basic_auth_provider,
            MilleniumPatronAPI,
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

            def decode_bearer_token(self, *args, **kwargs):
                return "decoded bearer token for %s" % self.name

        l1 = db.library(short_name="l1")
        l2 = db.library(short_name="l2")

        auth = Authenticator(db.session, db.session.query(Library))
        auth.library_authenticators["l1"] = MockLibraryAuthenticator("l1")
        auth.library_authenticators["l2"] = MockLibraryAuthenticator("l2")

        # This new library isn't in the authenticator.
        l3 = db.library(short_name="l3")

        with app.test_request_context("/") as ctx:
            setattr(ctx.request, "library", l3)
            assert LIBRARY_NOT_FOUND == auth.authenticated_patron(db.session, {})
            assert LIBRARY_NOT_FOUND == auth.create_authentication_document()
            assert LIBRARY_NOT_FOUND == auth.create_authentication_headers()
            assert LIBRARY_NOT_FOUND == auth.get_credential_from_header({})
            assert LIBRARY_NOT_FOUND == auth.create_bearer_token()

        # The other libraries are in the authenticator.
        with app.test_request_context("/") as ctx:
            setattr(ctx.request, "library", l1)
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
            assert "decoded bearer token for l1" == auth.decode_bearer_token()

        with app.test_request_context("/") as ctx:
            setattr(ctx.request, "library", l2)
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
            assert "decoded bearer token for l2" == auth.decode_bearer_token()


class TestLibraryAuthenticator:
    def test_from_config_basic_auth_only(
        self,
        db: DatabaseTransactionFixture,
    ):
        # Only a basic auth provider.
        db.simple_auth_integration(db.default_library())
        auth = LibraryAuthenticator.from_config(db.session, db.default_library())

        assert auth.basic_auth_provider is not None
        assert isinstance(auth.basic_auth_provider, SimpleAuthenticationProvider)

    def test_config_succeeds_when_no_providers_configured(
        self,
        db: DatabaseTransactionFixture,
    ):
        # You can call from_config even when there are no authentication
        # providers configured.

        # This should not happen in normal usage, but there will be an
        # interim period immediately after a library is created where
        # this will be its configuration.
        authenticator = LibraryAuthenticator.from_config(
            db.session, db.default_library()
        )

        assert len(list(authenticator.providers)) == 0

    def test_configuration_exception_during_from_config_stored(
        self,
        db: DatabaseTransactionFixture,
    ):
        # If the initialization of an AuthenticationProvider from config
        # raises CannotLoadConfiguration or ImportError, the exception
        # is stored with the LibraryAuthenticator rather than being
        # propagated.
        # Create an integration destined to raise CannotLoadConfiguration..
        library = db.default_library()
        unknown = db.integration_configuration(
            "unknown protocol", goal=Goals.PATRON_AUTH_GOAL, libraries=[library]
        )

        auth = LibraryAuthenticator.from_config(db.session, db.default_library())

        # The LibraryAuthenticator exists but has no AuthenticationProviders.
        assert auth.basic_auth_provider is None

        # The integration has left its trace in initialization_exceptions.
        not_found = auth.initialization_exceptions[(unknown.id, library.id)]
        assert isinstance(not_found, CannotLoadConfiguration)
        assert "Unable to load implementation for external integration" in str(
            not_found
        )

    def test_register_fails_when_integration_has_wrong_goal(
        self, db: DatabaseTransactionFixture
    ):
        auth = LibraryAuthenticator(_db=db.session, library=db.default_library())
        integration = MagicMock(spec=IntegrationLibraryConfiguration)
        type(integration.parent).goal = PropertyMock(return_value="some other goal")
        type(integration.parent).protocol = PropertyMock(return_value="protocol")
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert (
            "Was asked to register an integration with goal=some other goal as though it were a way of authenticating patrons."
            in str(excinfo.value)
        )

    def test_register_fails_when_integration_not_associated_with_library(
        self, db: DatabaseTransactionFixture
    ):
        auth = LibraryAuthenticator(_db=db.session, library=db.default_library())
        integration = MagicMock(spec=IntegrationLibraryConfiguration)
        type(integration.parent).goal = PropertyMock(
            return_value=Goals.PATRON_AUTH_GOAL
        )
        type(integration).library_id = PropertyMock(return_value=None)
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert "Was asked to register an integration with library {}, which doesn't use it.".format(
            db.default_library().name
        ) in str(
            excinfo.value
        )

    def test_register_provider_fails_but_does_not_explode_on_remote_integration_error(
        self, db: DatabaseTransactionFixture
    ):
        # We're going to instantiate a mock authentication provider that
        # immediately raises a RemoteIntegrationException, which will become
        # a CannotLoadConfiguration.

        class ExplodingProvider(MockBasic):
            def __init__(self, *args, **kwargs):
                raise RemoteIntegrationException("oops", "exploded")

        library = db.default_library()
        registry = MagicMock(spec=IntegrationRegistry)
        registry.get = MagicMock(return_value=ExplodingProvider)
        integration = MagicMock(spec=IntegrationLibraryConfiguration)
        type(integration.parent).goal = PropertyMock(
            return_value=Goals.PATRON_AUTH_GOAL
        )
        type(integration.parent).settings_dict = PropertyMock(return_value={})
        type(integration).library_id = PropertyMock(return_value=library.id)
        type(integration).settings_dict = PropertyMock(return_value={})
        auth = LibraryAuthenticator(
            _db=db.session, library=library, integration_registry=registry
        )
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert "Could not instantiate" in str(excinfo.value)
        assert f"authentication provider for library {library.name}." in str(
            excinfo.value
        )

    def test_register_provider_basic_auth(
        self,
        db: DatabaseTransactionFixture,
    ):
        library = db.default_library()
        integration = db.auth_integration(
            SIP2AuthenticationProvider, library, settings=SIP2Settings(url="url")
        )
        library_integration = integration.for_library(library)
        assert isinstance(library_integration, IntegrationLibraryConfiguration)
        auth = LibraryAuthenticator(_db=db.session, library=library)
        auth.register_provider(library_integration)
        assert auth.basic_auth_provider is not None
        assert isinstance(auth.basic_auth_provider, SIP2AuthenticationProvider)

    def test_supports_patron_authentication(
        self,
        db: DatabaseTransactionFixture,
    ):
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
        authenticator.basic_auth_provider = MagicMock(spec=BasicAuthenticationProvider)
        assert True == authenticator.supports_patron_authentication
        authenticator.basic_auth_provider = None

    def test_identifies_individuals(self, db: DatabaseTransactionFixture):
        # This LibraryAuthenticator does not authenticate patrons at
        # all, so it does not identify patrons as individuals.
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
        )
        assert authenticator.identifies_individuals is False

        # This LibraryAuthenticator has two Authenticators, but
        # neither of them identify patrons as individuals.

        basic = MagicMock(spec=BasicAuthenticationProvider)
        type(basic).identifies_individuals = PropertyMock(return_value=False)
        basic_auth_info = basic

        saml = MagicMock(spec=BaseSAMLAuthenticationProvider)
        type(saml).identifies_individuals = PropertyMock(return_value=False)
        saml_auth_info = saml

        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            basic_auth_provider=basic_auth_info,
            saml_providers=[saml_auth_info],
            bearer_token_signing_secret=db.fresh_str(),
        )
        assert authenticator.identifies_individuals is False

        # If some Authenticators identify individuals and some do not,
        # the library as a whole does not (necessarily) identify
        # individuals.
        type(basic).identifies_individuals = PropertyMock(return_value=True)
        assert authenticator.identifies_individuals is False

        # If every Authenticator identifies individuals, then so does
        # the library as a whole.
        type(saml).identifies_individuals = PropertyMock(return_value=True)
        assert authenticator.identifies_individuals is True

    def test_provider_registration(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        """You can register the same provider multiple times,
        but you can't register two different basic auth providers
        """
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
        )
        basic1 = mock_basic()
        basic2 = mock_basic()

        authenticator.register_basic_auth_provider(basic1)
        authenticator.register_basic_auth_provider(basic1)

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            authenticator.register_basic_auth_provider(basic2)
        assert "Two basic auth providers configured" in str(excinfo.value)

    def test_authenticated_patron_basic(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        patron = db.patron()
        patrondata = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=patron.authorization_identifier,
            username=patron.username,
        )
        basic = mock_basic(patrondata=patrondata)
        basic.authenticate = MagicMock(return_value=patron)
        basic.integration = PropertyMock(
            return_value=MagicMock(spec=IntegrationConfiguration)
        )
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            basic_auth_provider=basic,
        )
        assert patron == authenticator.authenticated_patron(
            db.session,
            Authorization(auth_type="basic", data=dict(username="foo", password="bar")),
        )

        # OAuth doesn't work.
        problem = authenticator.authenticated_patron(
            db.session, Authorization(auth_type="bearer", token="abcd")
        )
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == problem

    def test_authenticated_patron_bearer(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        saml = MagicMock(spec=BaseSAMLAuthenticationProvider, wraps=mock_basic())
        integration = MagicMock(spec=IntegrationConfiguration)
        type(integration).available = PropertyMock(return_value=True)
        saml.integration = PropertyMock(return_value=integration)
        saml.authenticated_patron = MagicMock(return_value="foo")

        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            saml_providers=[saml],
            bearer_token_signing_secret="xx-testing-bearer-token-secret!!",
        )

        # Mock the sign verification
        with patch.object(authenticator, "decode_bearer_token") as decode:
            decode.return_value = ("Mock", "decoded-token")
            bearer_token = authenticator.create_bearer_token("test", "test")
            response = authenticator.authenticated_patron(
                db.session, Authorization(auth_type="Bearer", token=bearer_token)
            )
            # The token was decoded
            assert decode.call_count == 1
            decode.assert_called_with(bearer_token)
            # The right saml provider was used
            assert response == "foo"
            assert saml.authenticated_patron.call_count == 1

    def test_authenticated_patron_bearer_access_token(
        self,
        db: DatabaseTransactionFixture,
        mock_basic: MockBasicFixture,
    ):
        now = utc_now()
        two_hours_in_the_future = now + datetime.timedelta(hours=2)

        basic = mock_basic()
        authenticator = LibraryAuthenticator(
            _db=db.session, library=db.default_library(), basic_auth_provider=basic
        )

        token_auth_provider, basic_auth_provider = authenticator.providers
        [patron_lookup_provider] = authenticator.unique_patron_lookup_providers
        assert (
            cast(BasicTokenAuthenticationProvider, token_auth_provider).basic_provider
            == basic_auth_provider
        )
        assert patron_lookup_provider == basic_auth_provider

        patron = db.patron()
        token = PatronJWEAccessTokenProvider.generate_token(db.session, patron, "pass")
        auth = Authorization(auth_type="bearer", token=token)

        # Token is valid
        with freeze_time(now):
            auth_patron = authenticator.authenticated_patron(db.session, auth)
            assert type(auth_patron) == Patron
            assert auth_patron.id == patron.id

        # The token is expired
        with freeze_time(two_hours_in_the_future):
            problem = authenticator.authenticated_patron(db.session, auth)
            assert PATRON_AUTH_ACCESS_TOKEN_EXPIRED == problem

    def test_authenticated_patron_unsupported_mechanism(
        self, db: DatabaseTransactionFixture
    ):
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
        )
        problem = authenticator.authenticated_patron(
            db.session, Authorization(auth_type="advanced")
        )
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == problem

    def test_get_credential_from_header(
        self,
        db: DatabaseTransactionFixture,
        mock_basic: MockBasicFixture,
    ):
        def get_library_authenticator(
            basic_auth_provider: BasicAuthenticationProvider | None,
        ) -> LibraryAuthenticator:
            return LibraryAuthenticator(
                _db=db.session,
                library=db.default_library(),
                basic_auth_provider=basic_auth_provider,
            )

        basic = mock_basic()

        # We can pull the password out of a Basic Auth credential
        # if a Basic Auth authentication provider is configured.
        authenticator = get_library_authenticator(basic_auth_provider=basic)
        credential = Authorization(auth_type="basic", data=dict(password="foo"))
        assert "foo" == authenticator.get_credential_from_header(credential)

        # We can't pull the password out if no basic auth provider
        authenticator = get_library_authenticator(basic_auth_provider=None)
        assert authenticator.get_credential_from_header(credential) is None

        authenticator = get_library_authenticator(basic_auth_provider=basic)
        patron = db.patron()
        token = PatronJWEAccessTokenProvider.generate_token(
            db.session, patron, "passworx"
        )
        credential = Authorization(auth_type="bearer", token=token)
        assert authenticator.get_credential_from_header(credential) == "passworx"

    def test_create_authentication_document(
        self,
        db: DatabaseTransactionFixture,
        mock_basic: MockBasicFixture,
        announcement_fixture: AnnouncementFixture,
        library_fixture: LibraryFixture,
    ):
        class MockAuthenticator(LibraryAuthenticator):
            """Mock the _geographic_areas method."""

            AREAS = ["focus area", "service area"]

            @classmethod
            def _geographic_areas(cls, library):
                return cls.AREAS

        library = library_fixture.library()
        library_settings = library_fixture.settings(library)
        basic = mock_basic()
        library.name = "A Fabulous Library"
        authenticator = MockAuthenticator(
            _db=db.session,
            library=library,
            basic_auth_provider=basic,
        )

        # We're about to call url_for, so we must create an
        # application context.
        from palace.manager.api.app import app

        # Set up configuration settings for links.
        library_settings.terms_of_service = "http://terms.com"
        library_settings.privacy_policy = "http://privacy.com"
        library_settings.copyright = "http://copyright.com"
        library_settings.license = "http://license.ca/"
        library_settings.about = "http://about.io"
        library_settings.registration_url = "https://library.org/register"
        library_settings.patron_password_reset = "https://example.org/reset"
        library_settings.web_css_file = "http://style.css"

        library.logo = LibraryLogo(content=b"image data")

        library_settings.library_description = "Just the best."

        # Set the URL to the library's web page.
        library_settings.website = "http://library.org/"

        # Set the color scheme a mobile client should use.
        library_settings.color_scheme = "plaid"

        # Set the colors a web client should use.
        library_settings.web_primary_color = "#012345"
        library_settings.web_secondary_color = "#abcdef"

        # Configure the various ways a patron can get help.
        library_settings.help_email = "help@library.org"
        library_settings.help_web = "http://library.help/"

        # Configure three library announcements: two active and one inactive.
        a1_db = announcement_fixture.create_announcement(
            db.session,
            content="this is announcement 1",
            start=announcement_fixture.yesterday,
            finish=announcement_fixture.today,
            library=library,
        )
        announcement_fixture.create_announcement(
            db.session,
            content="this is announcement 2",
            start=announcement_fixture.a_week_ago,
            finish=announcement_fixture.yesterday,
            library=library,
        )
        a3_db = announcement_fixture.create_announcement(
            db.session,
            content="this is announcement 3",
            start=announcement_fixture.yesterday,
            finish=announcement_fixture.today,
            library=library,
        )

        # Configure two site-wide announcements: one active and one inactive.
        a4_db = announcement_fixture.create_announcement(
            db.session,
            content="this is announcement 4",
            start=announcement_fixture.yesterday,
            finish=announcement_fixture.today,
        )
        announcement_fixture.create_announcement(
            db.session,
            content="this is announcement 5",
            start=announcement_fixture.a_week_ago,
            finish=announcement_fixture.yesterday,
        )

        with app.test_request_context("/"):
            url = authenticator.authentication_document_url()
            assert url.endswith("/%s/authentication_document" % library.short_name)

            doc = json.loads(authenticator.create_authentication_document())
            # The main thing we need to test is that the
            # authentication sub-documents are assembled properly and
            # placed in the right position.
            # TODO: token doc will be here only when correct environment variable set to true.
            # If basic token auth is enabled, then there should be two authentication
            # mechanisms and the first should be for token auth.
            authenticators = doc["authentication"]
            assert 2 == len(authenticators)
            [token_doc, basic_doc] = authenticators
            assert BasicTokenAuthenticationProvider.FLOW_TYPE == token_doc["type"]

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

            # We also need to test that the links got pulled in
            # from the configuration.
            (
                about,
                alternate,
                copyright,
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
            assert "http://terms.com" == terms_of_service["href"]
            assert "http://privacy.com" == privacy_policy["href"]
            assert "http://copyright.com" == copyright["href"]
            assert "http://about.io" == about["href"]
            assert "http://license.ca/" == license["href"]
            assert "data:image/png;base64,image data" == logo["href"]
            assert "http://style.css" == stylesheet["href"]

            assert "/loans" in loans["href"]
            assert "http://opds-spec.org/shelf" == loans["rel"]
            assert OPDSFeed.ACQUISITION_FEED_TYPE == loans["type"]

            assert "/patrons/me" in profile["href"]
            assert ProfileController.LINK_RELATION == profile["rel"]
            assert ProfileController.MEDIA_TYPE == profile["type"]

            expect_start = url_for(
                "index",
                library_short_name=library.short_name,
                _external=True,
            )
            assert expect_start == start["href"]

            # The start link points to an OPDS feed.
            assert OPDSFeed.ACQUISITION_FEED_TYPE == start["type"]

            # Most of the other links have type='text/html'
            assert "text/html" == about["type"]

            # The registration link
            assert "https://library.org/register" == register["href"]

            assert "https://example.org/reset" == reset_link["href"]

            # The logo link has type "image/png".
            assert "image/png" == logo["type"]

            # We have two help links.
            assert "http://library.help/" == help_web["href"]
            assert "text/html" == help_web["type"]
            assert "mailto:help@library.org" == help_email["href"]

            # Since no special address was given for the copyright
            # designated agent, the help address was reused.
            copyright_rel = (
                "http://librarysimplified.org/rel/designated-agent/copyright"
            )
            assert copyright_rel == copyright_agent["rel"]
            assert "mailto:help@library.org" == copyright_agent["href"]

            # The public key is correct.
            assert authenticator.library is not None
            assert authenticator.library.public_key is not None
            assert authenticator.library.public_key == doc["public_key"]["value"]
            assert "RSA" == doc["public_key"]["type"]

            # The library's web page shows up as an HTML alternate
            # to the OPDS server.
            assert (
                dict(rel="alternate", type="text/html", href="http://library.org/")
                == alternate
            )

            # Active announcements are published; inactive announcements are not.
            a4, a1, a3 = doc["announcements"]
            assert dict(id=str(a1_db.id), content="this is announcement 1") == a1
            assert dict(id=str(a3_db.id), content="this is announcement 3") == a3
            assert dict(id=str(a4_db.id), content="this is announcement 4") == a4

            # Features that are enabled for this library are communicated
            # through the 'features' item.
            features = doc["features"]
            assert [] == features["disabled"]
            assert [Configuration.RESERVATIONS_FEATURE] == features["enabled"]

            # If a separate copyright designated agent is configured,
            # that email address is used instead of the default
            # patron support address.
            library_settings.copyright_designated_agent_email_address = (
                "dmca@library.org"
            )
            doc = json.loads(authenticator.create_authentication_document())
            [agent] = [x for x in doc["links"] if x["rel"] == copyright_rel]
            assert "mailto:dmca@library.org" == agent["href"]

            # If no focus area or service area are provided, those fields
            # are not added to the document.
            MockAuthenticator.AREAS = [None, None]  # type:ignore
            doc = json.loads(authenticator.create_authentication_document())
            for key in ("focus_area", "service_area"):
                assert key not in doc

            # Only global announcements
            for announcement in [a1_db, a3_db]:
                db.session.delete(announcement)
            doc = json.loads(authenticator.create_authentication_document())
            assert [dict(id=str(a4_db.id), content="this is announcement 4")] == doc[
                "announcements"
            ]
            # If there are no announcements, the list of announcements is present
            # but empty.
            db.session.delete(a4_db)
            doc = json.loads(authenticator.create_authentication_document())
            assert [] == doc["announcements"]

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
                authenticator.authentication_document_url(),
                AuthenticationForOPDSDocument.LINK_RELATION,
            )
            assert expect == headers["Link"]

            # If the authenticator does not include a basic auth provider,
            # no WWW-Authenticate header is provided.
            real_authenticator = LibraryAuthenticator(
                _db=db.session,
                library=library,
            )
            headers = real_authenticator.create_authentication_headers()
            assert "WWW-Authenticate" not in headers

    def test_create_authentication_document_no_delete_adobe_id_link_when_authdata_utility_is_none(
        self,
        db: DatabaseTransactionFixture,
        mock_basic: MockBasicFixture,
        library_fixture: LibraryFixture,
    ):
        """When the library has no Adobe Vendor ID config, the authentication document
        must not include a delete-adobe-id link.
        """
        from palace.manager.api.app import app

        library = library_fixture.library()
        basic = mock_basic()
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=library,
            basic_auth_provider=basic,
        )

        with app.test_request_context("/"):
            doc = json.loads(authenticator.create_authentication_document())

        delete_adobe_id_rel = "http://palaceproject.io/terms/rel/delete-adobe-id"
        delete_adobe_id_links = [
            link for link in doc["links"] if link.get("rel") == delete_adobe_id_rel
        ]
        assert [] == delete_adobe_id_links


class TestBasicAuthenticationProvider:
    credentials = dict(username="user", password="")

    def test_authenticated_patron_passes_on_none(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        provider = mock_basic()
        patron = provider.authenticated_patron(db.session, self.credentials)
        assert patron is None

    def test_authenticated_patron_passes_on_problem_detail(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        provider = mock_basic(patrondata=UNSUPPORTED_AUTHENTICATION_MECHANISM)
        patron = provider.authenticated_patron(db.session, self.credentials)
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == patron

    def test_authenticated_patron_allows_access_to_expired_credentials(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        """Even if your card has expired, you can log in -- you just can't
        borrow books.
        """
        yesterday = utc_now() - datetime.timedelta(days=1)

        expired = PatronData(
            permanent_id="1",
            authorization_identifier="2",
            authorization_expires=yesterday,
        )
        provider = mock_basic(patrondata=expired)
        patron = provider.authenticated_patron(db.session, self.credentials)
        assert isinstance(patron, Patron)
        assert "1" == patron.external_identifier
        assert "2" == patron.authorization_identifier

    def test_authenticated_patron_updates_metadata_if_necessary(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        patron = db.patron()
        assert PatronUtility.needs_external_sync(patron) is True

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
            complete=True,
        )

        provider = mock_basic(
            patrondata=incomplete_data, lookup_patrondata=complete_data
        )
        patron2 = provider.authenticated_patron(db.session, self.credentials)

        # We found the right patron.
        assert patron == patron2

        # We updated their metadata.
        assert "user" == patron.username
        assert barcode == patron.authorization_identifier

        # We did a patron lookup, which means we updated
        # .last_external_sync.
        assert patron.last_external_sync is not None
        assert barcode == patron.authorization_identifier
        assert username == patron.username

        # Looking up the patron a second time does not cause another
        # metadata refresh, because we just did a refresh and the
        # patron has borrowing privileges.
        last_sync = patron.last_external_sync
        assert PatronUtility.needs_external_sync(patron) is False
        patron2 = provider.authenticated_patron(db.session, self.credentials)
        # we found the right patron
        assert patron == patron2
        assert last_sync == patron.last_external_sync
        assert barcode == patron.authorization_identifier
        assert username == patron.username

        # If we somehow authenticate with an identifier other than
        # the ones in the Patron record, we trigger another metadata
        # refresh to see if anything has changed.
        incomplete_data = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier="some other identifier",
            complete=False,
        )
        provider.patrondata = incomplete_data
        patron2 = provider.authenticated_patron(
            db.session,
            dict(username="someotheridentifier", password=""),
        )
        assert patron == patron2
        assert patron.last_external_sync is not None
        assert patron.last_external_sync > last_sync

        # But Patron.authorization_identifier doesn't actually change
        # to "some other identifier", because when we do the metadata
        # refresh we get the same data as before.
        assert barcode == patron.authorization_identifier
        assert username == patron.username

    @pytest.mark.parametrize(
        "auth_return,enforce_return,lookup_return,"
        "calls_lookup,create_patron,expected",
        [
            # If we don't get a Patrondata from remote_authenticate, we don't call remote_patron_lookup
            # or enforce_library_identifier_restriction
            (None, None, None, 0, False, None),
            # If we get a complete patrondata from remote_authenticate, we don't call remote_patron_lookup
            (
                PatronData(
                    authorization_identifier="a", external_type="xyz", complete=True
                ),
                PatronData(
                    authorization_identifier="a", external_type="xyz", complete=True
                ),
                None,
                0,
                False,
                True,
            ),
            # If we get an incomplete patrondata from remote_authenticate, but get a complete patrondata
            # from enforce_library_identifier_restriction, we don't call remote_patron_lookup
            (
                PatronData(authorization_identifier="a", complete=False),
                PatronData(
                    authorization_identifier="a", external_type="xyz", complete=True
                ),
                None,
                0,
                False,
                True,
            ),
            # If we get an incomplete patrondata from remote_authenticate, and enforce_library_identifier_restriction
            # raises a ProblemDetail, then we don't call remote_patron_lookup and get that ProblemDetail.
            (
                PatronData(authorization_identifier="a", complete=False),
                PATRON_OF_ANOTHER_LIBRARY.with_debug("some debug details"),
                None,
                0,
                False,
                PATRON_OF_ANOTHER_LIBRARY.with_debug("some debug details"),
            ),
            # If we get an incomplete patrondata from remote_authenticate, and enforce_library_identifier_restriction
            # returns an incomplete patrondata, we call remote_patron_lookup
            (
                PatronData(authorization_identifier="a", complete=False),
                PatronData(authorization_identifier="a", complete=False),
                PatronData(
                    authorization_identifier="a", external_type="xyz", complete=True
                ),
                1,
                False,
                True,
            ),
            # If the patron already exists and we have a complete patrondata we don't
            # call remote_patron_lookup.
            (
                PatronData(
                    authorization_identifier="a", external_type="xyz", complete=True
                ),
                PatronData(
                    authorization_identifier="a", external_type="xyz", complete=True
                ),
                None,
                0,
                True,
                True,
            ),
            # If the patron already exists but we have an incomplete patrondata, we call
            # remote_patron_lookup, and update the patron record.
            (
                PatronData(authorization_identifier="a", complete=False),
                PatronData(authorization_identifier="a", complete=False),
                PatronData(
                    authorization_identifier="a", external_type="xyz", complete=True
                ),
                1,
                True,
                True,
            ),
            # If the patron already exists and we have an incomplete patrondata, but
            # remote_patron_lookup returns None, we don't get a patron record.
            (
                PatronData(authorization_identifier="a", complete=False),
                PatronData(authorization_identifier="a", complete=False),
                None,
                1,
                True,
                None,
            ),
        ],
    )
    def test_authenticated_patron_only_calls_remote_patron_lookup_once(
        self,
        db: DatabaseTransactionFixture,
        mock_basic: MockBasicFixture,
        auth_return,
        enforce_return,
        lookup_return,
        calls_lookup,
        create_patron,
        expected: Literal[True] | ProblemDetail,
    ):
        # The call to remote_patron_lookup is potentially expensive, so we want to avoid calling it
        # more than once. This test makes sure that if we have a complete patrondata from remote_authenticate,
        # or from enforce_library_identifier_restriction, we don't call remote_patron_lookup.
        provider = mock_basic()
        provider.remote_authenticate = MagicMock(return_value=auth_return)
        if isinstance(enforce_return, ProblemDetail):
            provider.enforce_library_identifier_restriction = MagicMock(
                side_effect=ProblemDetailException(enforce_return)
            )
        else:
            provider.enforce_library_identifier_restriction = MagicMock(
                return_value=enforce_return
            )
        provider.remote_patron_lookup = MagicMock(return_value=lookup_return)

        username = "a"
        password = "b"
        credentials = {"username": username, "password": password}

        # Create a patron before doing auth and make sure we can find it
        if create_patron:
            db_patron = db.patron()
            db_patron.authorization_identifier = username

        context_manager = (
            pytest.raises(ProblemDetailException)
            if isinstance(expected, ProblemDetail)
            else nullcontext()
        )
        with context_manager as ctx:
            patron = provider.authenticated_patron(db.session, credentials)

        provider.remote_authenticate.assert_called_once_with(username, password)

        if auth_return is not None:
            provider.enforce_library_identifier_restriction.assert_called_once_with(
                auth_return
            )
        else:
            provider.enforce_library_identifier_restriction.assert_not_called()
        assert provider.remote_patron_lookup.call_count == calls_lookup

        if isinstance(expected, ProblemDetail):
            assert isinstance(ctx, ExceptionInfo)
            problem_detail = ctx.value.problem_detail
            assert problem_detail == expected
        elif expected is True:
            # Make sure we get a Patron object back and that the patrondata has been
            # properly applied to it
            assert isinstance(patron, Patron)
            assert patron.external_type == "xyz"
        else:
            assert patron is expected

    def test_update_patron_metadata(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        patron = db.patron()
        patron.authorization_identifier = "2345"
        assert patron.last_external_sync is None
        assert patron.username is None

        patrondata = PatronData(username="user")
        provider = mock_basic(lookup_patrondata=patrondata)
        provider.update_patron_metadata(patron)

        # The patron's username has been changed.
        assert "user" == patron.username

        # last_external_sync has been updated.
        assert patron.last_external_sync is not None

    def test_update_patron_metadata_noop_if_no_remote_metadata(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        patron = db.patron()
        provider = mock_basic(patrondata=None)
        provider.update_patron_metadata(patron)

        # We can tell that update_patron_metadata was a no-op because
        # patron.last_external_sync didn't change.
        assert patron.last_external_sync is None

    def test_update_patron_metadata_returns_none_different_library(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        patron = db.patron()
        library1 = patron.library
        library2 = db.library()
        provider = mock_basic(library_id=library2.id)
        patron_metadata = provider.update_patron_metadata(patron)

        assert library1 != library2
        assert patron_metadata is None

    @pytest.mark.parametrize(
        "field_value, restriction_value, restriction_type,"
        "expect_success, expected_reason",
        (
            # If restriction is none, we always return True.
            (123, None, LibraryIdentifierRestriction.PREFIX, True, ""),
            (123, None, LibraryIdentifierRestriction.STRING, True, ""),
            (123, None, LibraryIdentifierRestriction.REGEX, True, ""),
            (123, None, LibraryIdentifierRestriction.LIST, True, ""),
            # If field is None we always return False.
            (
                None,
                "1234",
                LibraryIdentifierRestriction.PREFIX,
                False,
                "No value in field",
            ),
            (
                None,
                "1234",
                LibraryIdentifierRestriction.STRING,
                False,
                "No value in field",
            ),
            (
                None,
                re.compile(".*"),
                LibraryIdentifierRestriction.REGEX,
                False,
                "No value in field",
            ),
            (
                None,
                ["1", "2"],
                LibraryIdentifierRestriction.LIST,
                False,
                "No value in field",
            ),
            # Test PREFIX
            ("12345a", "1234", LibraryIdentifierRestriction.PREFIX, True, ""),
            (
                "a1234",
                "1234",
                LibraryIdentifierRestriction.PREFIX,
                False,
                "'a1234' does not start with '1234'",
            ),
            # Test STRING/exact
            (
                "12345a",
                "1234",
                LibraryIdentifierRestriction.STRING,
                False,
                "'12345a' does not exactly match '1234'",
            ),
            (
                "a1234",
                "1234",
                LibraryIdentifierRestriction.STRING,
                False,
                "'a1234' does not exactly match '1234'",
            ),
            ("1234", "1234", LibraryIdentifierRestriction.STRING, True, ""),
            # Test LIST
            ("1234", ["1234", "4321"], LibraryIdentifierRestriction.LIST, True, ""),
            ("4321", ["1234", "4321"], LibraryIdentifierRestriction.LIST, True, ""),
            (
                "12345",
                ["1234", "4321"],
                LibraryIdentifierRestriction.LIST,
                False,
                "'12345' not in list ['1234', '4321']",
            ),
            (
                "54321",
                ["1234", "4321"],
                LibraryIdentifierRestriction.LIST,
                False,
                "'54321' not in list ['1234', '4321']",
            ),
            # Test PREFIX_LIST
            (
                "12345",
                ["123", "456"],
                LibraryIdentifierRestriction.PREFIX_LIST,
                True,
                "",
            ),
            (
                "45678",
                ["123", "456"],
                LibraryIdentifierRestriction.PREFIX_LIST,
                True,
                "",
            ),
            ("123", ["123", "456"], LibraryIdentifierRestriction.PREFIX_LIST, True, ""),
            (
                "789",
                ["123", "456"],
                LibraryIdentifierRestriction.PREFIX_LIST,
                False,
                "'789' does not match any of the prefixes in list ['123', '456']",
            ),
            (
                "abc123",
                ["123", "456"],
                LibraryIdentifierRestriction.PREFIX_LIST,
                False,
                "'abc123' does not match any of the prefixes in list ['123', '456']",
            ),
            # Test PREFIX_LIST with None field value
            (
                None,
                ["123", "456"],
                LibraryIdentifierRestriction.PREFIX_LIST,
                False,
                "No value in field",
            ),
            # Test PREFIX_LIST with None restriction (should pass)
            ("anything", None, LibraryIdentifierRestriction.PREFIX_LIST, True, ""),
            # Test Regex
            (
                "123",
                re.compile(r"^(12|34)"),
                LibraryIdentifierRestriction.REGEX,
                True,
                "",
            ),
            (
                "345",
                re.compile(r"^(12|34)"),
                LibraryIdentifierRestriction.REGEX,
                True,
                "",
            ),
            (
                "abc",
                re.compile(r"^bc"),
                LibraryIdentifierRestriction.REGEX,
                False,
                "'abc' does not match regular expression '^bc'",
            ),
        ),
    )
    def test_restriction_matches(
        self,
        field_value: str | None,
        restriction_value: str | list[str] | re.Pattern | None,
        restriction_type: LibraryIdentifierRestriction,
        expect_success: bool,
        expected_reason: str,
    ):
        """Test the behavior of the library identifier restriction algorithm."""
        success, reason = BasicAuthenticationProvider._restriction_matches(
            field_value, restriction_value, restriction_type
        )

        # Reason should always be absent when we expect success and always
        # present when we don't; so, ensure that our test cases reflect that.
        assert expected_reason == "" if expect_success else expected_reason != ""
        assert success == expect_success
        assert reason == expected_reason

    @pytest.mark.parametrize(
        "restriction_type, criteria_string, expected_result",
        [
            # Test NONE - should return None
            (LibraryIdentifierRestriction.NONE, "anything", None),
            # Test REGEX - should return compiled pattern
            (LibraryIdentifierRestriction.REGEX, "^test.*", re.compile("^test.*")),
            # Test LIST - should return list of stripped items
            (
                LibraryIdentifierRestriction.LIST,
                "item1, item2, item3",
                ["item1", "item2", "item3"],
            ),
            (
                LibraryIdentifierRestriction.LIST,
                "item1,item2,item3",
                ["item1", "item2", "item3"],
            ),
            (
                LibraryIdentifierRestriction.LIST,
                " item1 , item2 , item3 ",
                ["item1", "item2", "item3"],
            ),
            # Test PREFIX_LIST - should return list of stripped items
            (
                LibraryIdentifierRestriction.PREFIX_LIST,
                "pre1, pre2, pre3",
                ["pre1", "pre2", "pre3"],
            ),
            (
                LibraryIdentifierRestriction.PREFIX_LIST,
                "pre1,pre2,pre3",
                ["pre1", "pre2", "pre3"],
            ),
            (
                LibraryIdentifierRestriction.PREFIX_LIST,
                " pre1 , pre2 , pre3 ",
                ["pre1", "pre2", "pre3"],
            ),
            # Test PREFIX - should return stripped string
            (LibraryIdentifierRestriction.PREFIX, "  prefix  ", "prefix"),
            # Test STRING - should return stripped string
            (LibraryIdentifierRestriction.STRING, "  exact  ", "exact"),
            # Test with None criteria - should return None
            (LibraryIdentifierRestriction.PREFIX, None, None),
            (LibraryIdentifierRestriction.LIST, None, None),
            (LibraryIdentifierRestriction.PREFIX_LIST, None, None),
        ],
    )
    def test_process_library_identifier_restriction_criteria(
        self,
        mock_basic: MockBasicFixture,
        restriction_type: LibraryIdentifierRestriction,
        criteria_string: str | None,
        expected_result: str | list[str] | re.Pattern | None,
    ):
        """Test that process_library_identifier_restriction_criteria properly processes different restriction types."""
        library_settings = BasicAuthProviderLibrarySettings(
            library_identifier_restriction_type=restriction_type,
            library_identifier_restriction_criteria=criteria_string,
        )

        provider = mock_basic(library_settings=library_settings)
        result = provider.library_identifier_restriction_criteria

        if isinstance(expected_result, re.Pattern):
            # For regex, compare the pattern string
            assert isinstance(result, re.Pattern)
            assert result.pattern == expected_result.pattern
        else:
            assert result == expected_result

    @pytest.mark.parametrize(
        "restriction_type, restriction, restriction_as_string, identifier, expected_success",
        [
            # Test regex
            (
                LibraryIdentifierRestriction.REGEX,
                re.compile("23[46]5"),
                "23[46]5",
                "23456",
                True,
            ),
            (
                LibraryIdentifierRestriction.REGEX,
                re.compile("23[46]5"),
                "23[46]5",
                "2365",
                True,
            ),
            (
                LibraryIdentifierRestriction.REGEX,
                re.compile("23[46]5"),
                "23[46]5",
                "2375",
                False,
            ),
            # Test prefix
            (
                LibraryIdentifierRestriction.PREFIX,
                "2345",
                "2345",
                "23456",
                True,
            ),
            (
                LibraryIdentifierRestriction.PREFIX,
                "2345",
                "2345",
                "123456",
                False,
            ),
            # Test string
            (
                LibraryIdentifierRestriction.STRING,
                "2345",
                "2345",
                "2345",
                True,
            ),
            (
                LibraryIdentifierRestriction.STRING,
                "2345",
                "2345",
                "12345",
                False,
            ),
        ],
    )
    def test_enforce_library_identifier_restriction(
        self,
        mock_basic: MockBasicFixture,
        restriction_type: LibraryIdentifierRestriction,
        restriction: str | list[str] | re.Pattern | None,
        restriction_as_string: str,
        identifier: str,
        expected_success: bool,
    ):
        def assert_problem_detail(pd: ProblemDetail, field_name: str) -> None:
            debug_message = pd.debug_message
            # Aside from whatever's in the `debug_message`, our ProblemDetail
            # should be a PATRON_OF_ANOTHER_LIBRARY.
            assert pd.with_debug("") == PATRON_OF_ANOTHER_LIBRARY.with_debug("")
            assert debug_message is not None
            assert debug_message.startswith(
                f"'{field_name}' does not match library restriction: "
            )
            assert identifier in debug_message
            assert restriction_as_string in debug_message

        """Test the enforce_library_identifier_restriction method."""
        provider = mock_basic()
        provider.library_identifier_restriction_type = restriction_type
        provider.library_identifier_restriction_criteria = restriction

        # Test match applied to barcode
        provider.library_identifier_field = (
            LibraryIdenfitierRestrictionField.BARCODE.value
        )
        patrondata = PatronData(authorization_identifier=identifier)
        if expected_success:
            assert (
                provider.enforce_library_identifier_restriction(patrondata)
                == patrondata
            )
        else:
            with pytest.raises(ProblemDetailException) as exc:
                provider.enforce_library_identifier_restriction(patrondata)
            assert_problem_detail(exc.value.problem_detail, "barcode")

        # Test match applied to patron library code.
        # It's not in the local data, so we need a complete PatronData.
        provider.library_identifier_field = (
            LibraryIdenfitierRestrictionField.PATRON_LIBRARY.value
        )
        local_patrondata = PatronData(complete=False, authorization_identifier="123")
        remote_patrondata = PatronData(
            library_identifier=identifier, authorization_identifier="123"
        )
        provider.remote_patron_lookup = MagicMock(return_value=remote_patrondata)
        if expected_success:
            assert (
                provider.enforce_library_identifier_restriction(local_patrondata)
                == remote_patrondata
            )
        else:
            with pytest.raises(ProblemDetailException) as exc:
                provider.enforce_library_identifier_restriction(local_patrondata)
            assert_problem_detail(exc.value.problem_detail, "patron location")
        provider.remote_patron_lookup.assert_called_once_with(local_patrondata)

        # Test match applied to library_identifier field on complete patrondata
        provider.library_identifier_field = "Other"
        patrondata = PatronData(library_identifier=identifier)
        if expected_success:
            assert (
                provider.enforce_library_identifier_restriction(patrondata)
                == patrondata
            )
        else:
            with pytest.raises(ProblemDetailException) as exc:
                provider.enforce_library_identifier_restriction(patrondata)
            assert_problem_detail(exc.value.problem_detail, "Other")

        # Test match applied to library_identifier field on incomplete patrondata
        provider.library_identifier_field = "other"
        local_patrondata = PatronData(complete=False, authorization_identifier="123")
        remote_patrondata = PatronData(
            library_identifier=identifier, authorization_identifier="123"
        )
        provider.remote_patron_lookup = MagicMock(return_value=remote_patrondata)
        if expected_success:
            assert (
                provider.enforce_library_identifier_restriction(local_patrondata)
                == remote_patrondata
            )
        else:
            with pytest.raises(ProblemDetailException) as exc:
                provider.enforce_library_identifier_restriction(local_patrondata)
            assert_problem_detail(exc.value.problem_detail, "other")
        provider.remote_patron_lookup.assert_called_once_with(local_patrondata)

    def test_enforce_library_identifier_restriction_library_identifier_field_none(
        self,
        mock_basic: MockBasicFixture,
    ):
        # Test library_identifier_field field is blank, we just return the patrondata passed in
        provider = mock_basic()
        provider.library_identifier_restriction_type = (
            LibraryIdentifierRestriction.STRING
        )
        provider.library_identifier_field = None  # type: ignore[assignment]
        patrondata = PatronData(authorization_identifier="12345")
        assert provider.enforce_library_identifier_restriction(patrondata) == patrondata

    def test_enforce_library_identifier_restriction_none(
        self,
        mock_basic: MockBasicFixture,
    ):
        """Test the enforce_library_identifier_restriction method."""
        provider = mock_basic()
        provider.library_identifier_restriction_type = LibraryIdentifierRestriction.NONE
        provider.library_identifier_restriction_criteria = "2345"

        patrondata = PatronData(authorization_identifier="12345")
        assert provider.enforce_library_identifier_restriction(patrondata) == patrondata

    def test_patron_identifier_restriction(self, mock_basic: MockBasicFixture):
        # If the type is regex its converted into a regular expression.
        provider = mock_basic(
            library_settings=BasicAuthProviderLibrarySettings(
                library_identifier_restriction_type=LibraryIdentifierRestriction.REGEX,
                library_identifier_restriction_criteria="^abcd",
            )
        )
        assert isinstance(provider.library_identifier_restriction_criteria, re.Pattern)
        assert "^abcd" == provider.library_identifier_restriction_criteria.pattern

        # If its type is list, make sure its converted into a list
        provider = mock_basic(
            library_settings=BasicAuthProviderLibrarySettings(
                library_identifier_restriction_type=LibraryIdentifierRestriction.LIST,
                library_identifier_restriction_criteria="a,b,c",
            )
        )
        assert ["a", "b", "c"] == provider.library_identifier_restriction_criteria

        # If its type is prefix make sure its a string
        provider = mock_basic(
            library_settings=BasicAuthProviderLibrarySettings(
                library_identifier_restriction_type=LibraryIdentifierRestriction.PREFIX,
                library_identifier_restriction_criteria="abc",
            )
        )
        assert "abc" == provider.library_identifier_restriction_criteria

        # If its type is string make sure its a string
        provider = mock_basic(
            library_settings=BasicAuthProviderLibrarySettings(
                library_identifier_restriction_type=LibraryIdentifierRestriction.STRING,
                library_identifier_restriction_criteria="abc",
            )
        )
        assert "abc" == provider.library_identifier_restriction_criteria

        # If its type is none make sure its actually None
        provider = mock_basic(
            library_settings=BasicAuthProviderLibrarySettings(
                library_identifier_restriction_type=LibraryIdentifierRestriction.NONE,
                library_identifier_restriction_criteria="abc",
            )
        )
        assert provider.library_identifier_restriction_criteria is None

    def test_constructor(self, mock_basic: MockBasicFixture):
        provider = mock_basic(
            settings=BasicAuthProviderSettings(
                test_identifier="username",
                test_password="pw",
                identifier_regular_expression=re.compile("idre"),
                password_regular_expression=re.compile("pwre"),
            ),
        )
        assert isinstance(provider.identifier_re, re.Pattern)
        assert "idre" == provider.identifier_re.pattern
        assert isinstance(provider.password_re, re.Pattern)
        assert "pwre" == provider.password_re.pattern
        assert "username" == provider.test_username
        assert "pw" == provider.test_password

        # Test the defaults.
        provider = mock_basic()
        assert isinstance(provider.identifier_re, re.Pattern)
        assert provider.password_re is None

    def test_testing_patron(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        # You don't have to have a testing patron.
        no_testing_patron = mock_basic()
        assert (None, None) == no_testing_patron.testing_patron(db.session)

        # But if you don't, testing_patron_or_bust() will raise an
        # exception.
        with pytest.raises(CannotLoadConfiguration) as cannot_load:
            no_testing_patron.testing_patron_or_bust(db.session)
        assert "No test patron identifier is configured" in str(cannot_load.value)

        # We configure a testing patron but their username and
        # password don't actually authenticate anyone. We don't crash,
        # but we can't look up the testing patron either.
        missing_patron = mock_basic(
            settings=BasicAuthProviderSettings(
                test_identifier="1",
                test_password="2",
            )
        )
        missing_patron.authenticated_patron = MagicMock(return_value=None)
        value = missing_patron.testing_patron(db.session)
        assert (None, "2") == value
        missing_patron.authenticated_patron.assert_called_once()

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as integration_exception:
            missing_patron.testing_patron_or_bust(db.session)
        assert "Remote declined to authenticate the test patron." in str(
            integration_exception.value
        )

        # We configure a testing patron but authenticating them
        # results in a problem detail document.
        problem_patron = mock_basic(
            settings=BasicAuthProviderSettings(
                test_identifier="1",
                test_password="2",
            )
        )
        problem_patron.authenticated_patron = MagicMock(
            return_value=PATRON_OF_ANOTHER_LIBRARY
        )
        value = problem_patron.testing_patron(db.session)
        assert (PATRON_OF_ANOTHER_LIBRARY, "2") == value

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as integration_exception:
            problem_patron.testing_patron_or_bust(db.session)
        assert "Test patron lookup returned a problem detail" in str(
            integration_exception.value
        )

        # And testing_patron_or_bust() returns a similar result if the
        # problem details comes is wrapped in an exception.
        problem_patron.authenticated_patron = MagicMock(
            side_effect=ProblemDetailException(
                problem_detail=PATRON_OF_ANOTHER_LIBRARY.with_debug(
                    "some debug message"
                )
            )
        )
        with pytest.raises(IntegrationException) as integration_exception:
            problem_patron.testing_patron_or_bust(db.session)
        message = str(integration_exception.value)
        assert message.startswith("Test patron lookup returned a problem detail")
        assert message.endswith("[some debug message]")

        # We configure a testing patron but authenticating them
        # results in something (non None) that's not a Patron
        # or a problem detail document.
        not_a_patron = "<not a patron>"
        problem_patron.authenticated_patron = MagicMock(return_value=not_a_patron)
        value = problem_patron.testing_patron(db.session)
        assert (not_a_patron, "2") == value

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as integration_exception:
            problem_patron.testing_patron_or_bust(db.session)
        assert "Test patron lookup returned invalid value for patron" in str(
            integration_exception.value
        )

        # Here, we configure a testing patron who is authenticated by
        # their username and password.
        patron = db.patron()
        present_patron = mock_basic(
            settings=BasicAuthProviderSettings(
                test_identifier="1",
                test_password="2",
            )
        )
        present_patron.authenticated_patron = MagicMock(return_value=patron)
        value = present_patron.testing_patron(db.session)
        assert (patron, "2") == value

        # Finally, testing_patron_or_bust works, returning the same
        # value as testing_patron()
        assert value == present_patron.testing_patron_or_bust(db.session)

    def test__run_self_tests(self, mock_basic: MockBasicFixture):
        _db = MagicMock()

        # If we can't authenticate a test patron, the rest of the tests
        # aren't even run.
        provider = mock_basic()
        exception = Exception("Nope")
        provider.testing_patron_or_bust = MagicMock(side_effect=exception)
        [result] = list(provider._run_self_tests(_db))
        provider.testing_patron_or_bust.assert_called_once_with(_db)
        assert result.success is False
        assert exception == result.exception

        # If we can authenticate a test patron, the patron and their
        # password are passed into the next test.
        provider = mock_basic()
        provider.testing_patron_or_bust = MagicMock(return_value=("patron", "password"))
        provider.update_patron_metadata = MagicMock(return_value="some metadata")

        [get_patron, update_metadata] = provider._run_self_tests(_db)
        provider.testing_patron_or_bust.assert_called_once_with(_db)
        provider.update_patron_metadata.assert_called_once_with("patron")
        assert "Authenticating test patron" == get_patron.name
        assert get_patron.success is True
        assert ("patron", "password") == get_patron.result

        assert "Syncing patron metadata" == update_metadata.name
        assert update_metadata.success is True
        assert "some metadata" == update_metadata.result

        #

    def test_server_side_validation(self, mock_basic: MockBasicFixture):
        provider = mock_basic(
            settings=BasicAuthProviderSettings(
                identifier_regular_expression=re.compile("foo"),
                password_regular_expression=re.compile("bar"),
            )
        )
        assert provider.server_side_validation("food", "barbecue") is True
        assert provider.server_side_validation("food", "arbecue") is False
        assert provider.server_side_validation("ood", "barbecue") is False
        assert provider.server_side_validation(None, None) is False

        # If this authenticator does not look at provided passwords,
        # then the only values that will pass validation are null
        # and the empty string.
        provider = mock_basic(
            settings=BasicAuthProviderSettings(
                identifier_regular_expression=re.compile("foo"),
                password_regular_expression=re.compile("bar"),
                password_keyboard=Keyboards.NULL,
            )
        )
        assert provider.server_side_validation("food", "barbecue") is False
        assert provider.server_side_validation("food", "is good") is False
        assert provider.server_side_validation("food", " ") is False
        assert provider.server_side_validation("food", None) is True
        assert provider.server_side_validation("food", "") is True

        # It's okay not to provide anything for server side validation.
        # The default settings will be used.
        provider = mock_basic()
        assert isinstance(provider.identifier_re, re.Pattern)
        assert provider.password_re is None
        assert provider.server_side_validation("food", "barbecue") is True
        assert provider.server_side_validation("a", "abc") is True
        assert provider.server_side_validation("!@#$", "def") is False

        # Test maximum length of identifier and password.
        provider = mock_basic(
            settings=BasicAuthProviderSettings(
                identifier_maximum_length=5,
                password_maximum_length=10,
            )
        )
        assert provider.server_side_validation("a", "1234") is True
        assert provider.server_side_validation("a", "123456789012345") is False
        assert provider.server_side_validation("abcdefghijklmnop", "1234") is False

    def test_local_patron_lookup(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
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

        provider = mock_basic()

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

    def test_get_credential_from_header(self, mock_basic: MockBasicFixture):
        provider = mock_basic()
        assert (
            provider.get_credential_from_header(
                Authorization(auth_type="bearer", token="Some Token")
            )
            is None
        )
        assert (
            provider.get_credential_from_header(Authorization(auth_type="basic"))
            is None
        )
        assert (
            provider.get_credential_from_header(
                Authorization(auth_type="basic", data=dict(password="foo"))
            )
            == "foo"
        )

    def test_authentication_flow_document(self, mock_basic: MockBasicFixture):
        """Test the default authentication provider document."""
        provider = mock_basic(
            settings=BasicAuthProviderSettings(
                identifier_maximum_length=22,
                password_maximum_length=7,
                identifier_barcode_format=BarcodeFormats.CODABAR,
            )
        )

        db = MagicMock(spec=Session)

        # Mock url_for so that the document can be generated.
        with patch("palace.manager.api.authentication.basic.url_for") as url_for_patch:
            url_for_patch.return_value = "http://localhost/"
            doc = provider.authentication_flow_document(db)
            assert doc["description"] == provider.label()
            assert doc["type"] == provider.flow_type

            labels = doc["labels"]
            assert labels["login"] == provider.identifier_label
            assert labels["password"] == provider.password_label

            inputs = doc["inputs"]
            assert inputs["login"]["keyboard"] == provider.identifier_keyboard.value
            assert inputs["password"]["keyboard"] == provider.password_keyboard.value

            assert (
                inputs["login"]["barcode_format"]
                == provider.identifier_barcode_format.value
            )

            assert (
                inputs["login"]["maximum_length"] == provider.identifier_maximum_length
            )
            assert (
                inputs["password"]["maximum_length"] == provider.password_maximum_length
            )

            [logo_link] = doc["links"]
            assert "logo" == logo_link["rel"]
            assert "http://localhost/" == logo_link["href"]
            url_for_patch.assert_called_once()
            assert "filename" in url_for_patch.call_args.kwargs
            assert (
                url_for_patch.call_args.kwargs["filename"]
                == provider.login_button_image
            )

    def test_scrub_credential(self, mock_basic: MockBasicFixture):
        # Verify that the scrub_credential helper method strips extra whitespace
        # and nothing else.
        provider = mock_basic()

        assert provider.scrub_credential(None) is None
        assert provider.scrub_credential(1) == 1  # type: ignore[arg-type]
        o = object()
        assert provider.scrub_credential(o) == o  # type: ignore[arg-type]
        assert provider.scrub_credential("user") == "user"
        assert provider.scrub_credential(" user") == "user"
        assert provider.scrub_credential(" user ") == "user"
        assert provider.scrub_credential("    \ruser\t     ") == "user"


class TestBasicAuthenticationProviderAuthenticate:
    """Test the complex BasicAuthenticationProvider.authenticate method."""

    # A dummy set of credentials, for use when the exact details of
    # the credentials passed in are not important.
    credentials = dict(username="user", password="pass")

    def test_success(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        patron = db.patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = mock_basic(patrondata=patrondata)

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

    @freeze_time()
    def test_success_but_local_patron_needs_sync(
        self,
        db: DatabaseTransactionFixture,
        mock_basic: MockBasicFixture,
        inactive_patron: InactivePatronFixture,
    ):
        # This patron has not logged on in a really long time.
        patron, complete_patrondata = inactive_patron

        # The 'ILS' will respond to an authentication request with a minimal
        # set of information.
        #
        # It will respond to a patron lookup request with more detailed
        # information.
        minimal_patrondata = PatronData(
            permanent_id=patron.external_identifier, complete=False
        )
        provider = mock_basic(
            patrondata=minimal_patrondata,
            lookup_patrondata=complete_patrondata,
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
        assert utc_now() == patron.last_external_sync

    @freeze_time()
    def test_success_with_immediate_patron_sync(
        self,
        db: DatabaseTransactionFixture,
        mock_basic: MockBasicFixture,
        inactive_patron: InactivePatronFixture,
    ):
        # This patron has not logged on in a really long time.
        patron, complete_patrondata = inactive_patron

        # The 'ILS' will respond to an authentication request with a complete
        # set of information. If a remote patron lookup were to happen,
        # it would explode.
        provider = mock_basic(patrondata=complete_patrondata)
        provider.remote_patron_lookup = MagicMock(
            side_effect=Exception("Should not be called.")
        )

        # The patron can be authenticated.
        assert patron == provider.authenticate(db.session, self.credentials)

        # Since the authentication response provided a complete
        # overview of the patron, the Authenticator was able to sync
        # the account immediately, without doing a separate remote
        # patron lookup.
        assert "new username" == patron.username
        assert "new authorization identifier" == patron.authorization_identifier
        assert utc_now() == patron.last_external_sync

    def test_failure_when_remote_authentication_returns_problemdetail(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        provider = mock_basic(patrondata=UNSUPPORTED_AUTHENTICATION_MECHANISM)
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == provider.authenticate(
            db.session, self.credentials
        )

    def test_failure_when_remote_authentication_returns_none(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        provider = mock_basic(patrondata=None)
        assert provider.authenticate(db.session, self.credentials) is None

    def test_server_side_validation_runs(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        patron = db.patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)

        provider = mock_basic(
            patrondata=patrondata,
            settings=BasicAuthProviderSettings(
                identifier_regular_expression=re.compile("foo"),
                password_regular_expression=re.compile("bar"),
            ),
        )

        # This would succeed, but we don't get to remote_authenticate()
        # because we fail the regex test.
        assert provider.authenticate(db.session, self.credentials) is None

        # This succeeds because we pass the regex test.
        assert patron == provider.authenticate(
            db.session, dict(username="food", password="barbecue")
        )

    def test_authentication_succeeds_but_patronlookup_fails(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        """This case should never happen--it indicates a malfunctioning
        authentication provider. But we handle it.
        """
        patrondata = PatronData(permanent_id=db.fresh_str(), complete=False)
        provider = mock_basic(patrondata=patrondata, lookup_patrondata=None)

        # When we call remote_authenticate(), we get patrondata, but
        # there is no corresponding local patron, so we call
        # remote_patron_lookup() for details, and we get nothing.  At
        # this point we give up -- there is no authenticated patron.
        assert provider.authenticate(db.session, self.credentials) is None

    def test_authentication_creates_missing_patron(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        # The authentication provider knows about this patron,
        # but this is the first we've heard about them.
        library = db.default_library()
        patrondata = PatronData(
            permanent_id=db.fresh_str(),
            authorization_identifier=db.fresh_str(),
            fines=MoneyUtility.parse(1),
        )

        provider = mock_basic(
            patrondata=patrondata,
        )
        patron = provider.authenticate(db.session, self.credentials)

        # A server side Patron was created from the PatronData.
        assert isinstance(patron, Patron)
        assert library == patron.library
        assert patrondata.permanent_id == patron.external_identifier
        assert patrondata.authorization_identifier == patron.authorization_identifier

        # Information not relevant to the patron's identity was stored
        # in the Patron object after it was created.
        assert 1 == patron.fines

    def test_authentication_updates_outdated_patron_on_permanent_id_match(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
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

        provider = mock_basic(patrondata=patrondata)
        patron2 = provider.authenticate(db.session, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider.
        assert patron2 == patron

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        assert new_identifier == patron.authorization_identifier
        assert new_username == patron.username

    def test_authentication_updates_outdated_patron_on_username_match(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
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

        provider = mock_basic(patrondata=patrondata)
        patron2 = provider.authenticate(db.session, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider, based on the username match.
        assert patron2 == patron

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        assert new_identifier == patron.authorization_identifier

    def test_authentication_updates_outdated_patron_on_authorization_identifier_match(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
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

        provider = mock_basic(patrondata=patrondata)
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


class TestBearerTokenType:
    def test_from_token(self, db: DatabaseTransactionFixture) -> None:
        PatronJWEAccessTokenProvider.create_key(db.session)
        patron = db.patron()
        jwe_token = PatronJWEAccessTokenProvider.generate_token(
            db.session, patron, "password"
        )

        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            bearer_token_signing_secret="secret-for-testing-bearer-tokens",
        )
        jwt_token = authenticator.create_bearer_token("test", "test")

        assert BearerTokenType.from_token(jwt_token) == BearerTokenType.JWT
        assert BearerTokenType.from_token(jwe_token) == BearerTokenType.JWE
        assert BearerTokenType.from_token("test") == BearerTokenType.UNKNOWN
