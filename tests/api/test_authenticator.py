"""Test the base authentication framework: that is, the classes that
don't interact with any particular source of truth.
"""
from __future__ import annotations

import datetime
import json
import os
import re
from decimal import Decimal
from functools import partial
from typing import TYPE_CHECKING, Callable, Literal, Optional, Tuple, cast
from unittest.mock import MagicMock, PropertyMock, patch

import flask
import pytest
from flask import url_for
from freezegun import freeze_time
from money import Money
from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from api.annotations import AnnotationWriter
from api.announcements import Announcements
from api.authentication.base import PatronData
from api.authentication.basic import (
    BarcodeFormats,
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
    Keyboards,
    LibraryIdentifierRestriction,
)
from api.authenticator import (
    Authenticator,
    BaseSAMLAuthenticationProvider,
    CirculationPatronProfileStorage,
    LibraryAuthenticator,
)
from api.config import CannotLoadConfiguration, Configuration
from api.custom_patron_catalog import CustomPatronCatalog
from api.integration.registry.patron_auth import PatronAuthRegistry
from api.millenium_patron import MilleniumPatronAPI
from api.opds import LibraryAnnotator
from api.problem_details import *
from api.problem_details import PATRON_OF_ANOTHER_LIBRARY
from api.simple_authentication import SimpleAuthenticationProvider
from api.sip import SIP2AuthenticationProvider
from api.util.patron import PatronUtility
from core.analytics import Analytics
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.mock_analytics_provider import MockAnalyticsProvider
from core.model import CirculationEvent, ConfigurationSetting, Library, Patron, create
from core.model.constants import LinkRelations
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.opds import OPDSFeed
from core.user_profile import ProfileController
from core.util.authentication_for_opds import AuthenticationForOPDSDocument
from core.util.datetime_helpers import utc_now
from core.util.http import IntegrationException, RemoteIntegrationException
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from ..fixtures.api_controller import ControllerFixture
    from ..fixtures.authenticator import AuthProviderFixture
    from ..fixtures.database import DatabaseTransactionFixture
    from ..fixtures.vendor_id import VendorIDFixture


class MockBasic(BasicAuthenticationProvider):
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
    return partial(
        MockBasic,
        library_id=db.default_library().id,
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
        fines=Money(6, "USD"),
        block_reason=PatronData.NO_VALUE,
    )


InactivePatronFixture = Tuple[Patron, PatronData]


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
            fines="6",
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
        patron_data.cached_neighborhood = "Little Homeworld"

        patron_data.apply(patron)
        assert patron_data.permanent_id == patron.external_identifier
        assert patron_data.authorization_identifier == patron.authorization_identifier
        assert patron_data.username == patron.username
        assert patron_data.authorization_expires == patron.authorization_expires
        assert patron_data.fines == patron.fines
        assert None == patron.block_reason
        assert "Little Homeworld" == patron.cached_neighborhood

        # This data is stored in PatronData but not applied to Patron.
        assert "4" == patron_data.personal_name
        assert False == hasattr(patron, "personal_name")
        assert "5" == patron_data.email_address
        assert False == hasattr(patron, "email_address")

        # This data is stored on the Patron object as a convenience,
        # but it's not stored in the database.
        assert "Little Homeworld" == patron.neighborhood

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
        assert "2" == patron.authorization_identifier
        assert default_library == patron.library
        assert True == is_new
        assert CirculationEvent.NEW_PATRON == analytics.event_type
        assert 1 == analytics.count

        # Patron.neighborhood was set, even though there is no
        # value and that's not a database field.
        assert None == patron.neighborhood

        # Set a neighborhood and try again.
        patron_data.neighborhood = "Achewood"

        # The same patron is returned, and no analytics
        # event was sent.
        patron, is_new = patron_data.get_or_create_patron(
            db.session, default_library.id, analytics
        )
        assert "2" == patron.authorization_identifier
        assert False == is_new
        assert "Achewood" == patron.neighborhood
        assert 1 == analytics.count

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
        assert "links" not in doc

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


class TestAuthenticator:
    def test_init(
        self,
        controller_fixture: ControllerFixture,
        create_millenium_auth_integration: Callable[..., AuthProviderFixture],
    ):
        db = controller_fixture.db

        # The default library has already been configured to use the
        # SimpleAuthenticationProvider for its basic auth.
        l1 = db.default_library()
        l1.short_name = "l1"

        # This library uses Millenium Patron.
        l2, ignore = create(db.session, Library, short_name="l2")
        create_millenium_auth_integration(l2)

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

        l1, ignore = create(db.session, Library, short_name="l1")
        l2, ignore = create(db.session, Library, short_name="l2")

        auth = Authenticator(db.session, db.session.query(Library))
        auth.library_authenticators["l1"] = MockLibraryAuthenticator("l1")
        auth.library_authenticators["l2"] = MockLibraryAuthenticator("l2")

        # This new library isn't in the authenticator.
        l3, ignore = create(db.session, Library, short_name="l3")

        with app.test_request_context("/"):
            flask.request.library = l3  # type:ignore
            assert LIBRARY_NOT_FOUND == auth.authenticated_patron(db.session, {})
            assert LIBRARY_NOT_FOUND == auth.create_authentication_document()
            assert LIBRARY_NOT_FOUND == auth.create_authentication_headers()
            assert LIBRARY_NOT_FOUND == auth.get_credential_from_header({})
            assert LIBRARY_NOT_FOUND == auth.create_bearer_token()

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
            assert "decoded bearer token for l2" == auth.decode_bearer_token()


class TestLibraryAuthenticator:
    def test_from_config_basic_auth_only(
        self,
        db: DatabaseTransactionFixture,
        create_millenium_auth_integration: Callable[..., AuthProviderFixture],
    ):
        # Only a basic auth provider.
        create_millenium_auth_integration(db.default_library())
        auth = LibraryAuthenticator.from_config(db.session, db.default_library())

        assert auth.basic_auth_provider is not None
        assert isinstance(auth.basic_auth_provider, MilleniumPatronAPI)

    def test_with_custom_patron_catalog(
        self,
        db: DatabaseTransactionFixture,
    ):
        """Instantiation of a LibraryAuthenticator may
        include instantiation of a CustomPatronCatalog.
        """
        mock_catalog = object()

        class MockCustomPatronCatalog:
            @classmethod
            def for_library(self, library):
                self.called_with = library
                return mock_catalog

        authenticator = LibraryAuthenticator.from_config(
            db.session,
            db.default_library(),
            custom_catalog_source=MockCustomPatronCatalog,  # type:ignore
        )
        assert (
            db.default_library() == MockCustomPatronCatalog.called_with  # type:ignore
        )

        # The custom patron catalog is stored as
        # authentication_document_annotator.
        assert mock_catalog == authenticator.authentication_document_annotator

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
        assert [] == list(authenticator.providers)

    def test_configuration_exception_during_from_config_stored(
        self,
        db: DatabaseTransactionFixture,
        create_millenium_auth_integration: Callable[..., AuthProviderFixture],
        create_auth_integration_configuration: Callable[..., AuthProviderFixture],
    ):
        # If the initialization of an AuthenticationProvider from config
        # raises CannotLoadConfiguration or ImportError, the exception
        # is stored with the LibraryAuthenticator rather than being
        # propagated.
        # Create an integration destined to raise CannotLoadConfiguration..
        library = db.default_library()
        misconfigured, _ = create_millenium_auth_integration(library, url="millenium")

        # ... and one destined to raise ImportError.
        unknown, _ = create_auth_integration_configuration("unknown protocol", library)

        auth = LibraryAuthenticator.from_config(db.session, db.default_library())

        # The LibraryAuthenticator exists but has no AuthenticationProviders.
        assert auth.basic_auth_provider is None

        # Both integrations have left their trace in
        # initialization_exceptions.
        not_configured = auth.initialization_exceptions[(misconfigured.id, library.id)]
        assert isinstance(not_configured, CannotLoadConfiguration)
        assert "Could not instantiate MilleniumPatronAPI" in str(not_configured)

        not_found = auth.initialization_exceptions[(unknown.id, library.id)]
        assert isinstance(not_configured, CannotLoadConfiguration)
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
        type(integration.parent).settings = PropertyMock(return_value={})
        type(integration).library_id = PropertyMock(return_value=library.id)
        type(integration).settings = PropertyMock(return_value={})
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
        create_auth_integration_configuration: Callable[..., AuthProviderFixture],
        patron_auth_registry: PatronAuthRegistry,
    ):
        library = db.default_library()
        protocol = patron_auth_registry.get_protocol(SIP2AuthenticationProvider)
        _, integration = create_auth_integration_configuration(
            protocol,
            library,
            settings={
                "url": "http://url/",
                "password": "secret",
            },
        )
        assert isinstance(integration, IntegrationLibraryConfiguration)
        auth = LibraryAuthenticator(_db=db.session, library=library)
        auth.register_provider(integration)
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
            neighborhood="Achewood",
        )
        basic = mock_basic(patrondata=patrondata)
        basic.authenticate = MagicMock(return_value=patron)  # type: ignore[method-assign]
        basic.integration = PropertyMock(return_value=MagicMock(spec=IntegrationConfiguration))  # type: ignore[method-assign]
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            basic_auth_provider=basic,
        )
        assert patron == authenticator.authenticated_patron(
            db.session,
            Authorization(auth_type="basic", data=dict(username="foo", password="bar")),
        )

        # Neighborhood information is being temporarily stored in the
        # Patron object for use elsewhere in request processing. It
        # won't be written to the database because there's no field in
        # `patrons` to store it.
        assert "Achewood" == patron.neighborhood

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
            bearer_token_signing_secret="xx",
        )

        # Mock the sign verification
        with patch.object(authenticator, "decode_bearer_token") as decode:
            decode.return_value = ("Mock", "decoded-token")
            response = authenticator.authenticated_patron(
                db.session, Authorization(auth_type="Bearer", token="some-bearer-token")
            )
            # The token was decoded
            assert decode.call_count == 1
            decode.assert_called_with("some-bearer-token")
            # The right saml provider was used
            assert response == "foo"
            assert saml.authenticated_patron.call_count == 1

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
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        basic = mock_basic()

        # We can pull the password out of a Basic Auth credential
        # if a Basic Auth authentication provider is configured.
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            basic_auth_provider=basic,
        )
        credential = Authorization(auth_type="basic", data=dict(password="foo"))
        assert "foo" == authenticator.get_credential_from_header(credential)

        # We can't pull the password out if no basic auth provider
        authenticator = LibraryAuthenticator(
            _db=db.session,
            library=db.default_library(),
            basic_auth_provider=None,
        )
        assert authenticator.get_credential_from_header(credential) is None

    def test_create_authentication_document(
        self, db: DatabaseTransactionFixture, mock_basic: MockBasicFixture
    ):
        class MockAuthenticator(LibraryAuthenticator):
            """Mock the _geographic_areas method."""

            AREAS = ["focus area", "service area"]

            @classmethod
            def _geographic_areas(cls, library):
                return cls.AREAS

        library = db.default_library()
        basic = mock_basic()
        library.name = "A Fabulous Library"
        authenticator = MockAuthenticator(
            _db=db.session,
            library=library,
            basic_auth_provider=basic,
        )

        def annotate_authentication_document(library, doc, url_for):
            doc["modified"] = "Kilroy was here"
            return doc

        annotator = MagicMock(spec=CustomPatronCatalog)
        annotator.annotate_authentication_document = MagicMock(
            side_effect=annotate_authentication_document
        )
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
            url = authenticator.authentication_document_url()
            assert url.endswith("/%s/authentication_document" % library.short_name)

            doc = json.loads(authenticator.create_authentication_document())
            # The main thing we need to test is that the
            # authentication sub-documents are assembled properly and
            # placed in the right position.
            [basic_doc] = doc["authentication"]

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
            annotator.annotate_authentication_document.assert_called_with(
                library, doc, url_for
            )
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

    def test_key_pair(self, db: DatabaseTransactionFixture):
        """Test the public/private key pair associated with a library."""
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

    def test_key_pair_per_library(self, db: DatabaseTransactionFixture):
        # Ensure that each library obtains its own key pair.
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

    def test__geographic_areas(self, db: DatabaseTransactionFixture):
        """Test the _geographic_areas helper method."""

        class Mock(LibraryAuthenticator):
            called_with: Optional[Library] = None

            values = {
                Configuration.LIBRARY_FOCUS_AREA: "focus",
                Configuration.LIBRARY_SERVICE_AREA: "service",
            }

            @classmethod
            def _geographic_area(cls, key, library):
                cls.called_with = library
                return cls.values.get(key)

        # _geographic_areas calls _geographic_area twice and
        # returns the results in a 2-tuple.
        m = Mock._geographic_areas
        library = object()
        assert ("focus", "service") == m(library)  # type: ignore
        assert library == Mock.called_with

        # If only one value is provided, the same value is given for both
        # areas.
        del Mock.values[Configuration.LIBRARY_FOCUS_AREA]
        assert ("service", "service") == m(library)  # type: ignore

        Mock.values[Configuration.LIBRARY_FOCUS_AREA] = "focus"
        del Mock.values[Configuration.LIBRARY_SERVICE_AREA]
        assert ("focus", "focus") == m(library)  # type: ignore

    def test__geographic_area(self, db: DatabaseTransactionFixture):
        """Test the _geographic_area helper method."""
        library = db.default_library()
        key = "a key"
        setting = ConfigurationSetting.for_library(key, library)

        def m():
            return LibraryAuthenticator._geographic_area(key, library)

        # A missing value is returned as None.
        assert m() is None

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
            cached_neighborhood="Little Homeworld",
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
        assert "Little Homeworld" == patron.cached_neighborhood

        # .cached_neighborhood (stored in the database) was reused as
        # .neighborhood (destroyed at the end of the request)
        assert "Little Homeworld" == patron.neighborhood

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
            # returns None, we don't call remote_patron_lookup and get a ProblemDetail
            (
                PatronData(authorization_identifier="a", complete=False),
                None,
                None,
                0,
                False,
                PATRON_OF_ANOTHER_LIBRARY,
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
        expected,
    ):
        # The call to remote_patron_lookup is potentially expensive, so we want to avoid calling it
        # more than once. This test makes sure that if we have a complete patrondata from remote_authenticate,
        # or from enforce_library_identifier_restriction, we don't call remote_patron_lookup.
        provider = mock_basic()
        provider.remote_authenticate = MagicMock(return_value=auth_return)  # type: ignore[method-assign]
        provider.enforce_library_identifier_restriction = MagicMock(  # type: ignore[method-assign]
            return_value=enforce_return
        )
        provider.remote_patron_lookup = MagicMock(return_value=lookup_return)  # type: ignore[method-assign]

        username = "a"
        password = "b"
        credentials = {"username": username, "password": password}

        # Create a patron before doing auth and make sure we can find it
        if create_patron:
            db_patron = db.patron()
            db_patron.authorization_identifier = username

        patron = provider.authenticated_patron(db.session, credentials)
        provider.remote_authenticate.assert_called_once_with(username, password)
        if auth_return is not None:
            provider.enforce_library_identifier_restriction.assert_called_once_with(
                auth_return
            )
        else:
            provider.enforce_library_identifier_restriction.assert_not_called()
        assert provider.remote_patron_lookup.call_count == calls_lookup
        if expected is True:
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

        patrondata = PatronData(username="user", neighborhood="Little Homeworld")
        provider = mock_basic(lookup_patrondata=patrondata)
        provider.update_patron_metadata(patron)

        # The patron's username has been changed.
        assert "user" == patron.username

        # last_external_sync has been updated.
        assert patron.last_external_sync is not None

        # .neighborhood was not stored in .cached_neighborhood.  In
        # this case, it must be cheap to get .neighborhood every time,
        # and it's better not to store information we can get cheaply.
        assert "Little Homeworld" == patron.neighborhood  # type: ignore[unreachable]
        assert patron.cached_neighborhood is None

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

    def test_restriction_matches(self):
        """Test the behavior of the library identifier restriction algorithm."""
        m = BasicAuthenticationProvider._restriction_matches

        # If restriction is none, we always return True.
        assert (
            m(
                "123",
                None,
                LibraryIdentifierRestriction.PREFIX,
            )
            is True
        )
        assert (
            m(
                "123",
                None,
                LibraryIdentifierRestriction.STRING,
            )
            is True
        )
        assert (
            m(
                "123",
                None,
                LibraryIdentifierRestriction.REGEX,
            )
            is True
        )
        assert (
            m(
                "123",
                None,
                LibraryIdentifierRestriction.LIST,
            )
            is True
        )

        # If field is None we always return False.
        assert (
            m(
                None,
                "1234",
                LibraryIdentifierRestriction.PREFIX,
            )
            is False
        )
        assert (
            m(
                None,
                "1234",
                LibraryIdentifierRestriction.STRING,
            )
            is False
        )
        assert (
            m(
                None,
                re.compile(".*"),
                LibraryIdentifierRestriction.REGEX,
            )
            is False
        )
        assert (
            m(
                None,
                ["1", "2"],
                LibraryIdentifierRestriction.LIST,
            )
            is False
        )

        # Test prefix
        assert (
            m(
                "12345a",
                "1234",
                LibraryIdentifierRestriction.PREFIX,
            )
            is True
        )
        assert (
            m(
                "a1234",
                "1234",
                LibraryIdentifierRestriction.PREFIX,
            )
            is False
        )

        # Test string
        assert (
            m(
                "12345a",
                "1234",
                LibraryIdentifierRestriction.STRING,
            )
            is False
        )
        assert (
            m(
                "a1234",
                "1234",
                LibraryIdentifierRestriction.STRING,
            )
            is False
        )
        assert (
            m(
                "1234",
                "1234",
                LibraryIdentifierRestriction.STRING,
            )
            is True
        )

        # Test list
        assert (
            True
            == m(
                "1234",
                ["1234", "4321"],
                LibraryIdentifierRestriction.LIST,
            )
            is True
        )
        assert (
            m(
                "4321",
                ["1234", "4321"],
                LibraryIdentifierRestriction.LIST,
            )
            is True
        )
        assert (
            m(
                "12345",
                ["1234", "4321"],
                LibraryIdentifierRestriction.LIST,
            )
            is False
        )
        assert (
            m(
                "54321",
                ["1234", "4321"],
                LibraryIdentifierRestriction.LIST,
            )
            is False
        )

        # Test Regex
        assert (
            m(
                "123",
                re.compile("^(12|34)"),
                LibraryIdentifierRestriction.REGEX,
            )
            is True
        )
        assert (
            m(
                "345",
                re.compile("^(12|34)"),
                LibraryIdentifierRestriction.REGEX,
            )
            is True
        )
        assert (
            m(
                "abc",
                re.compile("^bc"),
                LibraryIdentifierRestriction.REGEX,
            )
            is False
        )

    @pytest.mark.parametrize(
        "restriction_type, restriction, identifier, expected",
        [
            # Test regex
            (
                LibraryIdentifierRestriction.REGEX,
                re.compile("23[46]5"),
                "23456",
                True,
            ),
            (
                LibraryIdentifierRestriction.REGEX,
                re.compile("23[46]5"),
                "2365",
                True,
            ),
            (
                LibraryIdentifierRestriction.REGEX,
                re.compile("23[46]5"),
                "2375",
                False,
            ),
            # Test prefix
            (
                LibraryIdentifierRestriction.PREFIX,
                "2345",
                "23456",
                True,
            ),
            (
                LibraryIdentifierRestriction.PREFIX,
                "2345",
                "123456",
                False,
            ),
            # Test string
            (
                LibraryIdentifierRestriction.STRING,
                "2345",
                "2345",
                True,
            ),
            (
                LibraryIdentifierRestriction.STRING,
                "2345",
                "12345",
                False,
            ),
        ],
    )
    def test_enforce_library_identifier_restriction(
        self,
        mock_basic: MockBasicFixture,
        restriction_type,
        restriction,
        identifier,
        expected,
    ):
        """Test the enforce_library_identifier_restriction method."""
        provider = mock_basic()
        provider.library_identifier_restriction_type = restriction_type
        provider.library_identifier_restriction_criteria = restriction

        # Test match applied to barcode
        provider.library_identifier_field = "barcode"
        patrondata = PatronData(authorization_identifier=identifier)
        if expected:
            assert (
                provider.enforce_library_identifier_restriction(patrondata)
                == patrondata
            )
        else:
            assert provider.enforce_library_identifier_restriction(patrondata) is None

        # Test match applied to library_identifier field on complete patrondata
        provider.library_identifier_field = "other"
        patrondata = PatronData(library_identifier=identifier)
        if expected:
            assert (
                provider.enforce_library_identifier_restriction(patrondata)
                == patrondata
            )
        else:
            assert provider.enforce_library_identifier_restriction(patrondata) is None

        # Test match applied to library_identifier field on incomplete patrondata
        provider.library_identifier_field = "other"
        local_patrondata = PatronData(complete=False, authorization_identifier="123")
        remote_patrondata = PatronData(
            library_identifier=identifier, authorization_identifier="123"
        )
        provider.remote_patron_lookup = MagicMock(return_value=remote_patrondata)  # type: ignore[method-assign]
        if expected:
            assert (
                provider.enforce_library_identifier_restriction(local_patrondata)
                == remote_patrondata
            )
        else:
            assert (
                provider.enforce_library_identifier_restriction(local_patrondata)
                is None
            )
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
                identifier_regular_expression="idre",  # type: ignore[arg-type]
                password_regular_expression="pwre",  # type: ignore[arg-type]
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
        missing_patron.authenticated_patron = MagicMock(return_value=None)  # type: ignore[method-assign]
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
        problem_patron.authenticated_patron = MagicMock(return_value=PATRON_OF_ANOTHER_LIBRARY)  # type: ignore[method-assign]
        value = problem_patron.testing_patron(db.session)
        assert (PATRON_OF_ANOTHER_LIBRARY, "2") == value

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as integration_exception:
            problem_patron.testing_patron_or_bust(db.session)
        assert "Test patron lookup returned a problem detail" in str(
            integration_exception.value
        )

        # We configure a testing patron but authenticating them
        # results in something (non None) that's not a Patron
        # or a problem detail document.
        not_a_patron = "<not a patron>"
        problem_patron.authenticated_patron = MagicMock(return_value=not_a_patron)  # type: ignore[method-assign]
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
        present_patron.authenticated_patron = MagicMock(return_value=patron)  # type: ignore[method-assign]
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
        provider.testing_patron_or_bust = MagicMock(side_effect=exception)  # type: ignore[method-assign]
        [result] = list(provider._run_self_tests(_db))
        provider.testing_patron_or_bust.assert_called_once_with(_db)
        assert result.success is False
        assert exception == result.exception

        # If we can authenticate a test patron, the patron and their
        # password are passed into the next test.
        provider = mock_basic()
        provider.testing_patron_or_bust = MagicMock(return_value=("patron", "password"))  # type: ignore[method-assign]
        provider.update_patron_metadata = MagicMock(return_value="some metadata")  # type: ignore[method-assign]

        [get_patron, update_metadata] = provider._run_self_tests(_db)
        provider.testing_patron_or_bust.assert_called_once_with(_db)
        provider.update_patron_metadata.assert_called_once_with("patron")
        assert "Authenticating test patron" == get_patron.name
        assert get_patron.success is True
        assert ("patron", "password") == get_patron.result

        assert "Syncing patron metadata" == update_metadata.name
        assert update_metadata.success is True
        assert "some metadata" == update_metadata.result

    def test_server_side_validation(self, mock_basic: MockBasicFixture):
        provider = mock_basic(
            settings=BasicAuthProviderSettings(
                identifier_regular_expression="foo",  # type: ignore[arg-type]
                password_regular_expression="bar",  # type: ignore[arg-type]
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
                identifier_regular_expression="foo",  # type: ignore[arg-type]
                password_regular_expression="bar",  # type: ignore[arg-type]
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
                identifier_maximum_length="5",  # type: ignore[arg-type]
                password_maximum_length="10",  # type: ignore[arg-type]
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
            patrondata = PatronData(**patrondata_args)  # type: ignore[arg-type]
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
        with patch("api.authentication.basic.url_for") as url_for_patch:
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
        provider.remote_patron_lookup = MagicMock(side_effect=Exception("Should not be called."))  # type: ignore[method-assign]

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
                identifier_regular_expression="foo",  # type: ignore[arg-type]
                password_regular_expression="bar",  # type: ignore[arg-type]
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
            fines=Money(1, "USD"),
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
