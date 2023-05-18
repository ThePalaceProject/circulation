from __future__ import annotations

import json
import logging
import re
import sys
from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Pattern,
    Tuple,
    Type,
)

import flask
import jwt
from flask import url_for
from flask_babel import lazy_gettext as _
from money import Money
from pydantic import PositiveInt, validator
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import or_
from werkzeug.datastructures import Authorization, Headers

from api.adobe_vendor_id import AuthdataUtility
from api.annotations import AnnotationWriter
from api.announcements import Announcements
from api.custom_patron_catalog import CustomPatronCatalog
from api.opds import LibraryAnnotator
from core.analytics import Analytics
from core.integration.base import HasLibraryIntegrationConfiguration
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
    SettingsValidationError,
)
from core.model import (
    CirculationEvent,
    ConfigurationSetting,
    Library,
    Patron,
    PatronProfileStorage,
    get_one,
    get_one_or_create,
)
from core.model.hybrid import hybrid_property
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.opds import OPDSFeed
from core.selftest import HasSelfTestsIntegrationConfiguration, SelfTestResult
from core.user_profile import ProfileController
from core.util.authentication_for_opds import (
    AuthenticationForOPDSDocument,
    OPDSAuthenticationFlow,
)
from core.util.datetime_helpers import utc_now
from core.util.http import RemoteIntegrationException
from core.util.log import elapsed_time_logging
from core.util.problem_detail import ProblemDetail, ProblemError

from .admin.problem_details import (
    INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION,
)
from .config import CannotLoadConfiguration, Configuration, IntegrationException
from .integration.registry.patron_auth import PatronAuthRegistry
from .problem_details import *
from .util.patron import PatronUtility

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class CannotCreateLocalPatron(Exception):
    """A remote system provided information about a patron, but we could
    not put it into our database schema.

    Probably because it was too vague.
    """


class PatronData:
    """A container for basic information about a patron.

    Like Metadata and CirculationData, this offers a layer of
    abstraction between various account managment systems and the
    circulation manager database. Unlike with those classes, some of
    this data cannot be written to the database for data retention
    reasons. But it can be passed from the account management system
    to the client application.
    """

    # Used to distinguish between "value has been unset" and "value
    # has not changed".
    class NoValue:
        def __bool__(self):
            """We want this object to act like None or False."""
            return False

    NO_VALUE = NoValue()

    # Reasons why a patron might be blocked.
    UNKNOWN_BLOCK = "unknown"
    CARD_REPORTED_LOST = "card reported lost"
    EXCESSIVE_FINES = "excessive fines"
    EXCESSIVE_FEES = "excessive fees"
    NO_BORROWING_PRIVILEGES = "no borrowing privileges"
    TOO_MANY_LOANS = "too many active loans"
    TOO_MANY_RENEWALS = "too many renewals"
    TOO_MANY_OVERDUE = "too many items overdue"
    TOO_MANY_LOST = "too many items lost"

    # Patron is being billed for too many items (as opposed to
    # excessive fines, which means patron's fines have exceeded a
    # certain amount).
    TOO_MANY_ITEMS_BILLED = "too many items billed"

    # Patron was asked to return an item so someone else could borrow it,
    # but didn't return the item.
    RECALL_OVERDUE = "recall overdue"

    def __init__(
        self,
        permanent_id=None,
        authorization_identifier=None,
        username=None,
        personal_name=None,
        email_address=None,
        authorization_expires=None,
        external_type=None,
        fines=None,
        block_reason=None,
        library_identifier=None,
        neighborhood=None,
        cached_neighborhood=None,
        complete=True,
    ):
        """Store basic information about a patron.

        :param permanent_id: A unique and unchanging identifier for
        the patron, as used by the account management system and
        probably never seen by the patron. This is not required, but
        it is very useful to have because other identifiers tend to
        change.

        :param authorization_identifier: One or more assigned
        identifiers (usually numeric) the patron may use to identify
        themselves. This may be a list, because patrons may have
        multiple authorization identifiers. For example, an NYPL
        patron may have an NYPL library card, a Brooklyn Public
        Library card, and an IDNYC card: three different barcodes that
        all authenticate the same patron.

        The circulation manager does the best it can to maintain
        continuity of the patron's identity in the face of changes to
        this list. The two assumptions made are:

        1) A patron tends to pick one of their authorization
        identifiers and stick with it until it stops working, rather
        than switching back and forth. This identifier is the one
        stored in Patron.authorization_identifier.

        2) In the absence of any other information, the authorization
        identifier at the _beginning_ of this list is the one that
        should be stored in Patron.authorization_identifier.

        :param username: An identifier (usually alphanumeric) chosen
        by the patron and used to identify themselves.

        :param personal_name: The name of the patron. This information
        is not stored in the circulation manager database but may be
        passed on to the client.

        :param authorization_expires: The date, if any, at which the patron's
        authorization to borrow items from the library expires.

        :param external_type: A string classifying the patron
        according to some library-specific scheme.

        :param fines: A Money object representing the amount the
        patron owes in fines. Note that only the value portion of the
        Money object will be stored in the database; the currency portion
        will be ignored. (e.g. "20 USD" will become 20)

        :param block_reason: A string indicating why the patron is
        blocked from borrowing items. (Even if this is set to None, it
        may turn out the patron cannot borrow items because their card
        has expired or their fines are excessive.)

        :param library_identifier: A string pulled from the ILS that
        is used to determine if this user belongs to the current library.

        :param neighborhood: A string pulled from the ILS that
        identifies the patron's geographic location in a deliberately
        imprecise way that makes sense to the library -- maybe the
        patron's ZIP code or the name of their home branch. This data
        is never stored in a way that can be associated with an
        individual patron. Depending on library policy, this data may
        be associated with circulation events -- but a circulation
        event is not associated with the patron who triggered it.

        :param cached_neighborhood: This is the same as neighborhood,
        but it _will_ be cached in the patron's database record, for
        up to twelve hours. This should only be used by ILS systems
        that would have performance problems fetching patron
        neighborhood on demand.

        If cached_neighborhood is set but neighborhood is not,
        cached_neighborhood will be used as neighborhood.

        :param complete: Does this PatronData represent the most
        complete data we are likely to get for this patron from this
        data source, or is it an abbreviated version of more complete
        data we could get some other way?
        """
        self.permanent_id = permanent_id

        self.set_authorization_identifier(authorization_identifier)
        self.username = username
        self.authorization_expires = authorization_expires
        self.external_type = external_type
        self.fines = fines
        self.block_reason = block_reason
        self.library_identifier = library_identifier
        self.complete = complete

        # We do not store personal_name in the database, but we provide
        # it to the client if possible.
        self.personal_name = personal_name

        # We do not store email address in the database, but we need
        # to have it available for notifications.
        self.email_address = email_address

        # If cached_neighborhood (cached in the database) is provided
        # but neighborhood (destroyed at end of request) is not, use
        # cached_neighborhood as neighborhood.
        self.neighborhood = neighborhood or cached_neighborhood
        self.cached_neighborhood = cached_neighborhood

    def __eq__(self, other):
        """
        Compares two PatronData objects

        :param other: PatronData object
        :type other: PatronData

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """

        if not isinstance(other, PatronData):
            return False

        return (
            self.permanent_id == other.permanent_id
            and self.username == other.username
            and self.authorization_expires == other.authorization_expires
            and self.external_type == other.external_type
            and self.fines == other.fines
            and self.block_reason == other.block_reason
            and self.library_identifier == other.library_identifier
            and self.complete == other.complete
            and self.personal_name == other.personal_name
            and self.email_address == other.email_address
            and self.neighborhood == other.neighborhood
            and self.cached_neighborhood == other.cached_neighborhood
        )

    def __repr__(self):
        return (
            "<PatronData permanent_id=%r authorization_identifier=%r username=%r>"
            % (self.permanent_id, self.authorization_identifier, self.username)
        )

    @hybrid_property
    def fines(self):
        return self._fines

    @fines.setter
    def fines(self, value):
        """When setting patron fines, only store the numeric portion of
        a Money object.
        """
        if isinstance(value, Money):
            value = value.amount
        self._fines = value

    def apply(self, patron: Patron):
        """Take the portion of this data that can be stored in the database
        and write it to the given Patron record.
        """

        # First, handle the easy stuff -- everything except authorization
        # identifier.
        self.set_value(patron, "external_identifier", self.permanent_id)
        self.set_value(patron, "username", self.username)
        self.set_value(patron, "external_type", self.external_type)
        self.set_value(patron, "authorization_expires", self.authorization_expires)
        self.set_value(patron, "fines", self.fines)
        self.set_value(patron, "block_reason", self.block_reason)
        self.set_value(patron, "cached_neighborhood", self.cached_neighborhood)

        # Patron neighborhood (not a database field) is set as a
        # convenience.
        patron.neighborhood = self.neighborhood or self.cached_neighborhood

        # Now handle authorization identifier.
        if self.complete:
            # We have a complete picture of data from the ILS,
            # so we can be comfortable setting the authorization
            # identifier if necessary.
            if (
                patron.authorization_identifier is None
                or patron.authorization_identifier not in self.authorization_identifiers
            ):
                # The patron's authorization_identifier is not set, or is
                # set to a value that is no longer valid. Set it again.
                self.set_value(
                    patron, "authorization_identifier", self.authorization_identifier
                )
        elif patron.authorization_identifier != self.authorization_identifier:
            # It looks like we need to change
            # Patron.authorization_identifier.  However, we do not
            # have a complete picture of the patron's record. We don't
            # know if the current identifier is better than the one
            # the patron provided.

            # However, we can provisionally
            # Patron.authorization_identifier if it's not already set.
            if not patron.authorization_identifier:
                self.set_value(
                    patron, "authorization_identifier", self.authorization_identifier
                )

            if patron.username and self.authorization_identifier == patron.username:
                # This should be fine. It looks like the patron's
                # .authorization_identifier is set to their barcode,
                # and they authenticated with their username. In this
                # case we can be confident there is no need to change
                # Patron.authorization_identifier.
                pass
            else:
                # We don't know what's going on and we need to sync
                # with the remote ASAP.
                patron.last_external_sync = None

        # Note that we do not store personal_name or email_address in the
        # database model.
        if self.complete:
            # We got a complete dataset from the ILS, which is what an
            # external sync does, so we can reset the timer on
            # external sync.
            patron.last_external_sync = utc_now()

    def set_value(self, patron, field_name, value):
        if value is None:
            # Do nothing
            return
        elif value is self.NO_VALUE:
            # Unset a previous value.
            value = None
        setattr(patron, field_name, value)

    def get_or_create_patron(self, _db, library_id, analytics=None):
        """Create a Patron with this information.

        TODO: I'm concerned in the general case with race
        conditions. It's theoretically possible that two newly created
        patrons could have the same username or authorization
        identifier, violating a uniqueness constraint. This could
        happen if one was identified by permanent ID and the other had
        no permanent ID and was identified by username. (This would
        only come up if the authentication provider has permanent IDs
        for some patrons but not others.)

        Something similar can happen if the authentication provider
        provides username and authorization identifier, but not
        permanent ID, and the patron's authorization identifier (but
        not their username) changes while two different circulation
        manager authentication requests are pending.

        When these race conditions do happen, I think the worst that
        will happen is the second request will fail. But it's very
        important that authorization providers give some unique,
        preferably unchanging way of identifying patrons.

        :param library_id: Database ID of the Library with which this
            patron is associated.

        :param analytics: Analytics instance to track the new patron
            creation event.
        """

        # We must be very careful when checking whether the patron
        # already exists because three different fields might be in use
        # as the patron identifier.
        if self.permanent_id:
            search_by = dict(external_identifier=self.permanent_id)
        elif self.username:
            search_by = dict(username=self.username)
        elif self.authorization_identifier:
            search_by = dict(authorization_identifier=self.authorization_identifier)
        else:
            raise CannotCreateLocalPatron(
                "Cannot create patron without some way of identifying them uniquely."
            )
        search_by["library_id"] = library_id
        __transaction = _db.begin_nested()
        patron, is_new = get_one_or_create(_db, Patron, **search_by)

        if is_new and analytics:
            # Send out an analytics event to record the fact
            # that a new patron was created.
            analytics.collect_event(patron.library, None, CirculationEvent.NEW_PATRON)

        # This makes sure the Patron is brought into sync with the
        # other fields of this PatronData object, regardless of
        # whether or not it is newly created.
        if patron:
            self.apply(patron)
        __transaction.commit()

        return patron, is_new

    @property
    def to_response_parameters(self):
        """Return information about this patron which the client might
        find useful.

        This information will be sent to the client immediately after
        a patron's credentials are verified by an OAuth provider.
        """
        if self.personal_name:
            return dict(name=self.personal_name)
        return {}

    @property
    def to_dict(self):
        """Convert the information in this PatronData to a dictionary
        which can be converted to JSON and sent out to a client.
        """

        def scrub(value, default=None):
            if value is self.NO_VALUE:
                return default
            return value

        data = dict(
            permanent_id=self.permanent_id,
            authorization_identifier=self.authorization_identifier,
            username=self.username,
            external_type=self.external_type,
            block_reason=self.block_reason,
            personal_name=self.personal_name,
            email_address=self.email_address,
        )
        data = {k: scrub(v) for k, v in list(data.items())}

        # Handle the data items that aren't just strings.

        # A date
        expires = scrub(self.authorization_expires)
        if expires:
            expires = self.authorization_expires.strftime("%Y-%m-%d")
        data["authorization_expires"] = expires

        # A Money
        fines = scrub(self.fines)
        if fines is not None:
            fines = str(fines)
        data["fines"] = fines

        # A list
        data["authorization_identifiers"] = scrub(self.authorization_identifiers, [])
        return data

    def set_authorization_identifier(self, authorization_identifier):
        """Helper method to set both .authorization_identifier
        and .authorization_identifiers appropriately.
        """
        # The first authorization identifier in the list is the one
        # we should use for Patron.authorization_identifier, assuming
        # Patron.authorization_identifier needs to be updated.
        if isinstance(authorization_identifier, list):
            authorization_identifiers = authorization_identifier
            authorization_identifier = authorization_identifiers[0]
        elif authorization_identifier is None:
            authorization_identifiers = []
            authorization_identifier = None
        elif authorization_identifier is self.NO_VALUE:
            authorization_identifiers = []
            authorization_identifier = self.NO_VALUE
        else:
            authorization_identifiers = [authorization_identifier]
        self.authorization_identifier = authorization_identifier
        self.authorization_identifiers = authorization_identifiers


class CirculationPatronProfileStorage(PatronProfileStorage):
    """A patron profile storage that can also provide short client tokens"""

    @property
    def profile_document(self):
        doc = super().profile_document
        drm = []
        links = []
        device_link = {}

        authdata = AuthdataUtility.from_config(self.patron.library)
        if authdata:
            vendor_id, token = authdata.short_client_token_for_patron(self.patron)
            adobe_drm = {}
            adobe_drm["drm:vendor"] = vendor_id
            adobe_drm["drm:clientToken"] = token
            adobe_drm[
                "drm:scheme"
            ] = "http://librarysimplified.org/terms/drm/scheme/ACS"
            drm.append(adobe_drm)

            device_link["rel"] = "http://librarysimplified.org/terms/drm/rel/devices"
            device_link["href"] = self.url_for(
                "adobe_drm_devices",
                library_short_name=self.patron.library.short_name,
                _external=True,
            )
            links.append(device_link)

            annotations_link = dict(
                rel="http://www.w3.org/ns/oa#annotationService",
                type=AnnotationWriter.CONTENT_TYPE,
                href=self.url_for(
                    "annotations",
                    library_short_name=self.patron.library.short_name,
                    _external=True,
                ),
            )
            links.append(annotations_link)

            doc["links"] = links

        if drm:
            doc["drm"] = drm

        return doc


class Authenticator:
    """Route requests to the appropriate LibraryAuthenticator."""

    def __init__(
        self, _db, libraries: Iterable[Library], analytics: Analytics | None = None
    ):
        # Create authenticators
        self.log = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        self.library_authenticators: dict[str, LibraryAuthenticator] = {}
        self.populate_authenticators(_db, libraries, analytics)

    @property
    def current_library_short_name(self):
        return flask.request.library.short_name

    def populate_authenticators(
        self, _db, libraries: Iterable[Library], analytics: Analytics | None
    ):
        with elapsed_time_logging(
            log_method=self.log.debug, message_prefix="populate_authenticators"
        ):
            for library in libraries:
                self.library_authenticators[
                    library.short_name
                ] = LibraryAuthenticator.from_config(_db, library, analytics)

    def invoke_authenticator_method(self, method_name, *args, **kwargs):
        short_name = self.current_library_short_name
        if short_name not in self.library_authenticators:
            return LIBRARY_NOT_FOUND
        return getattr(self.library_authenticators[short_name], method_name)(
            *args, **kwargs
        )

    def authenticated_patron(self, _db, header):
        return self.invoke_authenticator_method("authenticated_patron", _db, header)

    def create_authentication_document(self):
        return self.invoke_authenticator_method("create_authentication_document")

    def create_authentication_headers(self):
        return self.invoke_authenticator_method("create_authentication_headers")

    def get_credential_from_header(self, auth):
        return self.invoke_authenticator_method("get_credential_from_header", auth)

    def create_bearer_token(self, *args, **kwargs):
        return self.invoke_authenticator_method("create_bearer_token", *args, **kwargs)

    def saml_provider_lookup(self, *args, **kwargs):
        return self.invoke_authenticator_method("saml_provider_lookup", *args, **kwargs)

    def decode_bearer_token(self, *args, **kwargs):
        return self.invoke_authenticator_method("decode_bearer_token", *args, **kwargs)


class LibraryAuthenticator:
    """Use the registered AuthenticationProviders to turn incoming
    credentials into Patron objects.
    """

    @classmethod
    def from_config(
        cls: Type[Self],
        _db: Session,
        library: Library,
        analytics: Optional[Analytics] = None,
        custom_catalog_source: Type[CustomPatronCatalog] = CustomPatronCatalog,
    ) -> Self:
        """Initialize an Authenticator for the given Library based on its
        configured ExternalIntegrations.

        :param custom_catalog_source: The lookup class for CustomPatronCatalogs.
            Intended for mocking during tests.
        """

        custom_catalog = custom_catalog_source.for_library(library)

        # Start with an empty list of authenticators.
        authenticator = cls(
            _db=_db, library=library, authentication_document_annotator=custom_catalog
        )

        # Find all of this library's ExternalIntegrations set up with
        # the goal of authenticating patrons.
        integrations: List[
            IntegrationLibraryConfiguration
        ] = IntegrationLibraryConfiguration.for_library_and_goal(
            _db, library, Goals.PATRON_AUTH_GOAL
        ).all()

        # Turn each such ExternalIntegration into an
        # AuthenticationProvider.
        for integration in integrations:
            try:
                authenticator.register_provider(integration, analytics)
            except CannotLoadConfiguration as e:
                # CannotLoadConfiguration is caused by misconfiguration, as opposed to bad code.
                logging.error(
                    f"Error registering authentication provider {integration.parent.name} "
                    f"({integration.parent.protocol}) for library {library.short_name}: {e}.",
                    exc_info=e,
                )
                authenticator.initialization_exceptions[
                    (integration.parent.id, library.id)
                ] = e

        if authenticator.saml_providers_by_name:
            # NOTE: this will immediately commit the database session,
            # which may not be what you want during a test. To avoid
            # this, you can create the bearer token signing secret as
            # a regular site-wide ConfigurationSetting.
            authenticator.bearer_token_signing_secret = (
                BearerTokenSigner.bearer_token_signing_secret(_db)
            )

        authenticator.assert_ready_for_token_signing()

        return authenticator

    def __init__(
        self,
        _db: Session,
        library: Library,
        basic_auth_provider: Optional[BasicAuthenticationProvider] = None,
        saml_providers: Optional[List[BaseSAMLAuthenticationProvider]] = None,
        bearer_token_signing_secret: Optional[str] = None,
        authentication_document_annotator: Optional[CustomPatronCatalog] = None,
        integration_registry: Optional[
            IntegrationRegistry[AuthenticationProvider]
        ] = None,
    ):
        """Initialize a LibraryAuthenticator from a list of AuthenticationProviders.

        :param _db: A database session (probably a scoped session, which is
            why we can't derive it from `library`)

        :param library: The Library to which this LibraryAuthenticator guards
        access.

        :param basic_auth_provider: The AuthenticatonProvider that handles
        HTTP Basic Auth requests.

        :param saml_providers: A list of AuthenticationProviders that handle
        SAML requests.

        :param bearer_token_signing_secret: The secret to use when
        signing JWTs for use as bearer tokens.

        """
        self._db = _db
        self.library_id = library.id
        self.library_uuid = library.uuid
        self.library_name = library.name
        self.library_short_name = library.short_name
        self.authentication_document_annotator = authentication_document_annotator
        self.integration_registry = (
            PatronAuthRegistry()
            if integration_registry is None
            else integration_registry
        )

        self.basic_auth_provider = basic_auth_provider
        self.saml_providers_by_name = {}
        self.bearer_token_signing_secret = bearer_token_signing_secret
        self.initialization_exceptions: Dict[Tuple[int, int], Exception] = {}

        self.log = logging.getLogger("Authenticator")

        # Make sure there's a public/private key pair for this
        # library. This makes it possible to register the library with
        # discovery services. Store the public key here for
        # convenience; leave the private key in the database.
        self.public_key, ignore = self.key_pair

        if saml_providers:
            for provider in saml_providers:
                self.saml_providers_by_name[provider.label()] = provider

        self.assert_ready_for_token_signing()

    @property
    def supports_patron_authentication(self) -> bool:
        """Does this library have any way of authenticating patrons at all?"""
        if self.basic_auth_provider or self.saml_providers_by_name:
            return True
        return False

    @property
    def identifies_individuals(self) -> bool:
        """Does this library require that individual patrons be identified?

        Most libraries require authentication as an individual. Some
        libraries don't identify patrons at all; others may have a way
        of identifying the patron population without identifying
        individuals, such as an IP gate.

        If some of a library's authentication mechanisms identify individuals,
        and others do not, the library does not identify individuals.
        """
        if not self.supports_patron_authentication:
            return False
        matches = list(self.providers)
        return len(matches) > 0 and all([x.identifies_individuals for x in matches])

    @property
    def library(self) -> Library | None:
        return Library.by_id(self._db, self.library_id)

    def assert_ready_for_token_signing(self):
        """If this LibraryAuthenticator has SAML providers, ensure that it
        also has a secret it can use to sign bearer tokens.
        """
        if self.saml_providers_by_name and not self.bearer_token_signing_secret:
            raise CannotLoadConfiguration(
                _(
                    "SAML providers are configured, but secret for signing bearer tokens is not."
                )
            )

    def register_provider(
        self,
        integration: IntegrationLibraryConfiguration,
        analytics: Analytics | None = None,
    ):
        """Turn an ExternalIntegration object into an AuthenticationProvider
        object, and register it.

        :param integration: An ExternalIntegration that configures
            a way of authenticating patrons.
        """
        if integration.parent.goal != Goals.PATRON_AUTH_GOAL:
            raise CannotLoadConfiguration(
                f"Was asked to register an integration with goal={integration.parent.goal} as though it were a "
                "way of authenticating patrons."
            )

        if self.library_id != integration.library_id:
            raise CannotLoadConfiguration(
                f"Was asked to register an integration with library {self.library_short_name}, which doesn't use it."
            )

        impl_cls = self.integration_registry.get(integration.parent.protocol)
        if not impl_cls:
            raise CannotLoadConfiguration(
                f"Unable to load implementation for external integration: {integration.parent.protocol}."
            )
        if not issubclass(impl_cls, AuthenticationProvider):
            raise CannotLoadConfiguration(
                f"Implementation class {impl_cls} is not an AuthenticationProvider."
            )
        try:
            if not isinstance(integration.parent.settings, dict):
                raise CannotLoadConfiguration(
                    f"Settings for {impl_cls.__name__} authentication provider for "
                    f"library {self.library_short_name} are not a dictionary."
                )
            if not isinstance(integration.settings, dict):
                raise CannotLoadConfiguration(
                    f"Library settings for {impl_cls.__name__} authentication provider for "
                    f"library {self.library_short_name} are not a dictionary."
                )
            settings = impl_cls.settings_class()(**integration.parent.settings)
            library_settings = impl_cls.library_settings_class()(**integration.settings)
            provider = impl_cls(
                self.library_id,
                integration.parent_id,
                settings,
                library_settings,
                analytics,
            )
        except (RemoteIntegrationException, ProblemError):
            raise CannotLoadConfiguration(
                f"Could not instantiate {impl_cls.__name__} authentication provider for "
                f"library {self.library_short_name}."
            )

        if isinstance(provider, BasicAuthenticationProvider):
            self.register_basic_auth_provider(provider)
            # TODO: Run a self-test, or at least check that we have
            # the ability to run one.
        elif isinstance(provider, BaseSAMLAuthenticationProvider):
            self.register_saml_provider(provider)
        else:
            raise CannotLoadConfiguration(
                f"Authentication provider {impl_cls.__name__} is neither a BasicAuthenticationProvider nor a "
                "BaseSAMLAuthenticationProvider. I can create it, but not sure where to put it."
            )

    def register_basic_auth_provider(
        self,
        provider: BasicAuthenticationProvider,
    ):
        if (
            self.basic_auth_provider is not None
            and self.basic_auth_provider != provider
        ):
            raise CannotLoadConfiguration("Two basic auth providers configured")
        self.basic_auth_provider = provider

    def register_saml_provider(
        self,
        provider: BaseSAMLAuthenticationProvider,
    ):
        already_registered = self.saml_providers_by_name.get(provider.label())
        if already_registered and already_registered != provider:
            raise CannotLoadConfiguration(
                'Two different SAML providers claim the name "%s"' % (provider.label())
            )
        self.saml_providers_by_name[provider.label()] = provider

    @property
    def providers(self) -> Iterable[AuthenticationProvider]:
        """An iterator over all registered AuthenticationProviders."""
        if self.basic_auth_provider:
            yield self.basic_auth_provider
        yield from self.saml_providers_by_name.values()

    def authenticated_patron(
        self, _db: Session, auth: Authorization
    ) -> Patron | ProblemDetail | None:
        """Go from an Authorization header value to a Patron object.

        :param auth: A werkzeug.Authorization object

        :return: A Patron, if one can be authenticated. None, if the
            credentials do not authenticate any particular patron. A
            ProblemDetail if an error occurs.
        """
        provider: AuthenticationProvider | None = None
        provider_token: Dict[str, str] | str | None = None
        if self.basic_auth_provider and auth.type.lower() == "basic":
            # The patron wants to authenticate with the
            # BasicAuthenticationProvider.
            provider = self.basic_auth_provider
            provider_token = auth.parameters
        elif self.saml_providers_by_name and auth.type.lower() == "bearer":
            # The patron wants to use an
            # SAMLAuthenticationProvider. Figure out which one.
            if auth.token is None:
                return INVALID_SAML_BEARER_TOKEN
            try:
                provider_name, provider_token = self.decode_bearer_token(auth.token)
            except jwt.exceptions.InvalidTokenError as e:
                return INVALID_SAML_BEARER_TOKEN
            saml_provider = self.saml_provider_lookup(provider_name)
            if isinstance(saml_provider, ProblemDetail):
                # There was a problem turning the provider name into
                # a registered SAMLAuthenticationProvider.
                return saml_provider
            provider = saml_provider

        if provider and provider_token:
            # Turn the token/header into a patron
            return provider.authenticated_patron(_db, provider_token)

        # We were unable to determine what was going on with the
        # Authenticate header.
        return UNSUPPORTED_AUTHENTICATION_MECHANISM

    def get_credential_from_header(self, auth: Authorization) -> str | None:
        """Extract a password credential from a WWW-Authenticate header
        (or equivalent).

        This is used to pass on a patron's credential to a content provider,
        such as Overdrive, which performs independent validation of
        a patron's credentials.

        :return: The patron's password, or None if not available.
        """
        credential = None
        for provider in self.providers:
            credential = provider.get_credential_from_header(auth)
            if credential is not None:
                break

        return credential

    def saml_provider_lookup(
        self, provider_name: str | None
    ) -> BaseSAMLAuthenticationProvider | ProblemDetail:
        """Look up the SAMLAuthenticationProvider with the given name. If that
        doesn't work, return an appropriate ProblemDetail.
        """
        if not self.saml_providers_by_name:
            # We don't support OAuth at all.
            return UNKNOWN_SAML_PROVIDER.detailed(
                _("No SAML providers are configured.")
            )

        if not provider_name or not provider_name in self.saml_providers_by_name:
            # The patron neglected to specify a provider, or specified
            # one we don't support.
            possibilities = ", ".join(list(self.saml_providers_by_name.keys()))
            return UNKNOWN_SAML_PROVIDER.detailed(
                UNKNOWN_SAML_PROVIDER.detail
                + _(" The known providers are: %s") % possibilities
            )
        return self.saml_providers_by_name[provider_name]

    def create_bearer_token(
        self, provider_name: str | None, provider_token: str | None
    ) -> str:
        """Create a JSON web token with the given provider name and access
        token.

        The patron will use this as a bearer token in lieu of the
        token we got from their OAuth provider. The big advantage of
        this token is that it tells us _which_ OAuth provider the
        patron authenticated against.

        When the patron uses the bearer token in the Authenticate header,
        it will be decoded with `decode_bearer_token_from_header`.
        """
        payload = dict(
            token=provider_token,
            # I'm not sure this is the correct way to use an
            # Issuer claim (https://tools.ietf.org/html/rfc7519#section-4.1.1).
            # Maybe we should use something custom instead.
            iss=provider_name,
        )
        return jwt.encode(payload, self.bearer_token_signing_secret, algorithm="HS256")  # type: ignore[arg-type]

    def decode_bearer_token(self, token: str) -> Tuple[str, str]:
        """Extract auth provider name and access token from JSON web token."""
        decoded = jwt.decode(
            token, self.bearer_token_signing_secret, algorithms=["HS256"]  # type: ignore[arg-type]
        )
        provider_name = decoded["iss"]
        token = decoded["token"]
        return (provider_name, token)

    def authentication_document_url(self) -> str:
        """Return the URL of the authentication document for the
        given library.
        """
        return url_for(
            "authentication_document",
            library_short_name=self.library_short_name,
            _external=True,
        )

    def create_authentication_document(self) -> str:
        """Create the Authentication For OPDS document to be used when
        a request comes in with no authentication.
        """
        links = []
        library = self.library

        # Add the same links that we would show in an OPDS feed, plus
        # some extra like 'registration' that are specific to Authentication
        # For OPDS.
        for rel in (
            LibraryAnnotator.CONFIGURATION_LINKS
            + Configuration.AUTHENTICATION_FOR_OPDS_LINKS
        ):
            value = ConfigurationSetting.for_library(rel, library).value
            if not value:
                continue
            link = dict(rel=rel, href=value)
            if any(value.startswith(x) for x in ("http:", "https:")):
                # We assume that HTTP URLs lead to HTML, but we don't
                # assume anything about other URL schemes.
                link["type"] = "text/html"
            links.append(link)

        # Add a rel="start" link pointing to the root OPDS feed.
        index_url = url_for(
            "index", _external=True, library_short_name=self.library_short_name
        )
        loans_url = url_for(
            "active_loans", _external=True, library_short_name=self.library_short_name
        )
        profile_url = url_for(
            "patron_profile", _external=True, library_short_name=self.library_short_name
        )

        links.append(
            dict(rel="start", href=index_url, type=OPDSFeed.ACQUISITION_FEED_TYPE)
        )
        links.append(
            dict(
                rel="http://opds-spec.org/shelf",
                href=loans_url,
                type=OPDSFeed.ACQUISITION_FEED_TYPE,
            )
        )
        links.append(
            dict(
                rel=ProfileController.LINK_RELATION,
                href=profile_url,
                type=ProfileController.MEDIA_TYPE,
            )
        )

        # If there is a Designated Agent email address, add it as a
        # link.
        designated_agent_uri = Configuration.copyright_designated_agent_uri(library)
        if designated_agent_uri:
            links.append(
                dict(
                    rel=Configuration.COPYRIGHT_DESIGNATED_AGENT_REL,
                    href=designated_agent_uri,
                )
            )

        # Add a rel="help" link for every type of URL scheme that
        # leads to library-specific help.
        for type, uri in Configuration.help_uris(library):
            links.append(dict(rel="help", href=uri, type=type))

        # Add a link to the web page of the library itself.
        library_uri = ConfigurationSetting.for_library(
            Configuration.WEBSITE_URL, library
        ).value
        if library_uri:
            links.append(dict(rel="alternate", type="text/html", href=library_uri))

        # Add the library's logo, if it has one.
        logo = ConfigurationSetting.for_library(Configuration.LOGO, library).value
        if logo:
            links.append(dict(rel="logo", type="image/png", href=logo))

        # Add the library's custom CSS file, if it has one.
        css_file = ConfigurationSetting.for_library(
            Configuration.WEB_CSS_FILE, library
        ).value
        if css_file:
            links.append(dict(rel="stylesheet", type="text/css", href=css_file))

        library_name = self.library_name or str(_("Library"))
        auth_doc_url = self.authentication_document_url()
        doc = AuthenticationForOPDSDocument(
            id=auth_doc_url,
            title=library_name,
            authentication_flows=list(self.providers),
            links=links,
        ).to_dict(self._db)

        # Add the library's mobile color scheme, if it has one.
        description = ConfigurationSetting.for_library(
            Configuration.COLOR_SCHEME, library
        ).value
        if description:
            doc["color_scheme"] = description

        # Add the library's web colors, if it has any.
        primary = ConfigurationSetting.for_library(
            Configuration.WEB_PRIMARY_COLOR, library
        ).value
        secondary = ConfigurationSetting.for_library(
            Configuration.WEB_SECONDARY_COLOR, library
        ).value
        if primary or secondary:
            doc["web_color_scheme"] = dict(
                primary=primary,
                secondary=secondary,
                background=primary,
                foreground=secondary,
            )

        # Add the description of the library as the OPDS feed's
        # service_description.
        description = ConfigurationSetting.for_library(
            Configuration.LIBRARY_DESCRIPTION, library
        ).value
        if description:
            doc["service_description"] = description

        # Add the library's focus area and service area, if either is
        # specified.
        focus_area, service_area = self._geographic_areas(library)
        if focus_area:
            doc["focus_area"] = focus_area
        if service_area:
            doc["service_area"] = service_area

        # Add the library's public key.
        doc["public_key"] = dict(type="RSA", value=self.public_key)

        # Add feature flags to signal to clients what features they should
        # offer.
        enabled: List[str] = []
        disabled: List[str] = []
        if library and library.allow_holds:
            bucket = enabled
        else:
            bucket = disabled
        bucket.append(Configuration.RESERVATIONS_FEATURE)
        doc["features"] = dict(enabled=enabled, disabled=disabled)

        # Add any active announcements for the library.
        library_announcements = (
            Announcements.for_library(library).active if library else []
        )
        announcements = [x.for_authentication_document for x in library_announcements]
        # Add any global announcements
        announcements_for_all = [
            x.for_authentication_document
            for x in Announcements.for_all(self._db).active
        ]
        doc["announcements"] = announcements_for_all + announcements

        # Finally, give the active annotator a chance to modify the document.

        if self.authentication_document_annotator:
            doc = (
                self.authentication_document_annotator.annotate_authentication_document(
                    library, doc, url_for
                )
            )

        return json.dumps(doc)

    @property
    def key_pair(self) -> Tuple[str | None, str | None]:
        """Look up or create a public/private key pair for use by this library."""
        setting = ConfigurationSetting.for_library(Configuration.KEY_PAIR, self.library)
        return Configuration.key_pair(setting)

    @classmethod
    def _geographic_areas(
        cls, library: Optional[Library]
    ) -> Tuple[
        Literal["everywhere"] | List[str] | None,
        Literal["everywhere"] | List[str] | None,
    ]:
        """Determine the library's focus area and service area.

        :param library: A Library
        :return: A 2-tuple (focus_area, service_area)
        """
        if not library:
            return None, None

        focus_area = cls._geographic_area(Configuration.LIBRARY_FOCUS_AREA, library)
        service_area = cls._geographic_area(Configuration.LIBRARY_SERVICE_AREA, library)

        # If only one value is provided, both values are considered to
        # be the same.
        if focus_area and not service_area:
            service_area = focus_area
        if service_area and not focus_area:
            focus_area = service_area
        return focus_area, service_area

    @classmethod
    def _geographic_area(
        cls, key: str, library: Library
    ) -> Literal["everywhere"] | List[str] | None:
        """Extract a geographic area from a ConfigurationSetting
        for the given `library`.

        See https://github.com/NYPL-Simplified/Simplified/wiki/Authentication-For-OPDS-Extensions#service_area and #focus_area
        """
        setting = ConfigurationSetting.for_library(key, library).value
        if not setting:
            return setting
        if setting == "everywhere":
            # This literal string may be served as is.
            return setting
        try:
            # If we can load the setting as JSON, it is either a list
            # of place names or a GeoJSON object.
            setting = json.loads(setting)
        except (ValueError, TypeError) as e:
            # The most common outcome -- treat the value as a single place
            # name by turning it into a list.
            setting = [setting]
        return setting

    def create_authentication_headers(self) -> Headers:
        """Create the HTTP headers to return with the OPDS
        authentication document."""
        library = Library.by_id(self._db, self.library_id)
        headers = Headers()
        headers.add("Content-Type", AuthenticationForOPDSDocument.MEDIA_TYPE)
        headers.add(
            "Link",
            "<%s>; rel=%s"
            % (
                self.authentication_document_url(),
                AuthenticationForOPDSDocument.LINK_RELATION,
            ),
        )
        # if requested from a web client, don't include WWW-Authenticate header,
        # which forces the default browser authentication prompt
        if (
            self.basic_auth_provider
            and not flask.request.headers.get("X-Requested-With") == "XMLHttpRequest"
        ):
            headers.add(
                "WWW-Authenticate",
                self.basic_auth_provider.authentication_header,
            )

        # TODO: We're leaving out headers for other providers to avoid breaking iOS
        # clients that don't support multiple auth headers. It's not clear what
        # the header for an oauth provider should look like. This means that there's
        # no auth header for app without a basic auth provider, but we don't have
        # any apps like that yet.

        return headers


class AuthProviderSettings(BaseSettings):
    ...


class AuthProviderLibrarySettings(BaseSettings):
    ...


class AuthenticationProvider(
    OPDSAuthenticationFlow,
    HasLibraryIntegrationConfiguration,
    HasSelfTestsIntegrationConfiguration,
    ABC,
):
    """Handle a specific patron authentication scheme."""

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: AuthProviderSettings,
        library_settings: AuthProviderLibrarySettings,
        analytics: Analytics | None = None,
    ):
        self.library_id = library_id
        self.integration_id = integration_id
        self.analytics = analytics

    @classmethod
    def logger(cls) -> logging.Logger:
        return logging.getLogger(f"{cls.__module__}.{cls.__name__}")

    def library(self, _db: Session) -> Library | None:
        return Library.by_id(_db, self.library_id)

    def integration(self, _db: Session) -> IntegrationConfiguration | None:
        return (
            _db.query(IntegrationConfiguration)
            .filter(IntegrationConfiguration.id == self.integration_id)
            .one_or_none()
        )

    @classmethod
    def settings_class(cls) -> type[AuthProviderSettings]:
        return AuthProviderSettings

    @classmethod
    def library_settings_class(
        cls,
    ) -> type[AuthProviderLibrarySettings]:
        return AuthProviderLibrarySettings

    @property
    @abstractmethod
    def identifies_individuals(self):
        # If an AuthenticationProvider authenticates patrons without identifying
        # then as specific individuals (the way a geographic gate does),
        # it should override this value and set it to False.
        ...

    @abstractmethod
    def authenticated_patron(
        self, _db: Session, header: dict | str
    ) -> Patron | ProblemDetail | None:
        """Go from a WWW-Authenticate header (or equivalent) to a Patron object.

        If the Patron needs to have their metadata updated, it happens
        transparently at this point.

        :return: A Patron if one can be authenticated; a ProblemDetail
            if an error occurs; None if the credentials are missing or wrong.
        """
        ...

    @abstractmethod
    def get_credential_from_header(self, auth: Authorization) -> str | None:
        """Extract a password credential from a werkzeug.Authorization object

        This is used to pass on a patron's credential to a content provider,
        such as Overdrive, which performs independent validation of
        a patron's credentials.

        :return: The patron's password, or None if not available.
        """
        ...


class LibraryIdentifierRestriction(Enum):
    NONE = "none"
    REGEX = "regex"
    PREFIX = "prefix"
    STRING = "string"
    LIST = "list"


class Keyboards(Enum):
    """Used by the mobile app to determine which keyboard to display"""

    DEFAULT = "Default"
    EMAIL_ADDRESS = "Email address"
    NUMBER_PAD = "Number pad"
    NULL = "No input"


class BarcodeFormats(Enum):
    CODABAR = "Codabar"
    NONE = "None"


class IdentifierLabels(Enum):
    BARCODE = "Barcode"
    EMAIL = "Email Address"
    USERNAME = "Username"
    LIBRARY_CARD = "Library Card"
    CARD_NUMBER = "Card Number"


class PasswordLabels(Enum):
    PASSWORD = "Password"
    PIN = "PIN"


class BasicAuthProviderSettings(AuthProviderSettings):
    """Settings for the BasicAuthenticationProvider."""

    # Configuration settings that are common to all Basic Auth-type
    # authentication techniques.
    test_identifier: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Test identifier",
            description="A test identifier to use when testing the authentication provider.",
            weight=10,
            required=True,
        ),
    )
    test_password: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Test password",
            description="A test password to use when testing the authentication provider.",
            weight=10,
        ),
    )
    identifier_barcode_format: BarcodeFormats = FormField(
        BarcodeFormats.NONE,
        form=ConfigurationFormItem(
            label="Patron identifier barcode format",
            description="Many libraries render patron identifiers as barcodes on "
            "physical library cards. If you specify the barcode format, patrons "
            "will be able to scan their library cards with a camera instead of "
            "manually typing in their identifiers.",
            type=ConfigurationFormItemType.SELECT,
            options={
                BarcodeFormats.NONE: "Patron identifiers are not rendered as barcodes",
                BarcodeFormats.CODABAR: "Patron identifiers are rendered as barcodes "
                "in Codabar format",
            },
            required=True,
            weight=10,
        ),
    )
    # By default, patron identifiers can only contain alphanumerics and
    # a few other characters.
    identifier_regular_expression: Pattern = FormField(
        re.compile(r"^[A-Za-z0-9@.-]+$"),
        form=ConfigurationFormItem(
            label="Identifier Regular Expression",
            description="A patron's identifier will be immediately rejected if it doesn't match this "
            "regular expression.",
            weight=10,
        ),
    )
    # By default, there are no restrictions on passwords.
    password_regular_expression: Optional[Pattern] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Password Regular Expression",
            description="A patron's password will be immediately rejected if it doesn't match this "
            "regular expression.",
            weight=10,
        ),
    )
    identifier_keyboard: Keyboards = FormField(
        Keyboards.DEFAULT,
        form=ConfigurationFormItem(
            label="Keyboard for identifier entry",
            type=ConfigurationFormItemType.SELECT,
            options={
                Keyboards.DEFAULT: "System default",
                Keyboards.EMAIL_ADDRESS: "Email address entry",
                Keyboards.NUMBER_PAD: "Number pad",
            },
            required=True,
            weight=10,
        ),
    )
    password_keyboard: Keyboards = FormField(
        Keyboards.DEFAULT,
        form=ConfigurationFormItem(
            label="Keyboard for password entry",
            type=ConfigurationFormItemType.SELECT,
            options={
                Keyboards.DEFAULT: "System default",
                Keyboards.NUMBER_PAD: "Number pad",
                Keyboards.NULL: "Patrons have no password and should not be prompted for one.",
            },
            weight=10,
        ),
    )
    identifier_maximum_length: Optional[PositiveInt] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Maximum identifier length",
            weight=10,
        ),
    )
    password_maximum_length: Optional[PositiveInt] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Maximum password length",
            weight=10,
        ),
    )
    identifier_label: IdentifierLabels = FormField(
        IdentifierLabels.BARCODE,
        form=ConfigurationFormItem(
            label="Label for identifier entry",
            type=ConfigurationFormItemType.SELECT,
            options={
                IdentifierLabels.BARCODE: "Barcode",
                IdentifierLabels.EMAIL: "Email address",
                IdentifierLabels.USERNAME: "Username",
                IdentifierLabels.LIBRARY_CARD: "Library card",
            },
            weight=10,
        ),
    )
    password_label: PasswordLabels = FormField(
        PasswordLabels.PIN,
        form=ConfigurationFormItem(
            label="Label for password entry",
            type=ConfigurationFormItemType.SELECT,
            options={
                PasswordLabels.PASSWORD: "Password",
                PasswordLabels.PIN: "PIN",
            },
            weight=10,
        ),
    )


class BasicAuthProviderLibrarySettings(AuthProviderLibrarySettings):
    # When multiple libraries share an ILS, a person may be able to
    # authenticate with the ILS but not be considered a patron of
    # _this_ library. This setting contains the rule for determining
    # whether an identifier is valid for a specific library.
    library_identifier_restriction_type: LibraryIdentifierRestriction = FormField(
        LibraryIdentifierRestriction.NONE,
        form=ConfigurationFormItem(
            label="Library Identifier Restriction",
            type=ConfigurationFormItemType.SELECT,
            description="When multiple libraries share an ILS, a person may be able to "
            "authenticate with the ILS but not be considered a patron of "
            "<em>this</em> library. This setting contains the rule for determining "
            "whether an identifier is valid for this specific library. <p/> "
            "If this setting it set to 'No Restriction' then the values for "
            "<em>Library Identifier Field</em> and <em>Library Identifier "
            "Restriction</em> will not be used.",
            options={
                LibraryIdentifierRestriction.NONE: "No Restriction",
                LibraryIdentifierRestriction.REGEX: "Regex Match",
                LibraryIdentifierRestriction.PREFIX: "Prefix Match",
                LibraryIdentifierRestriction.STRING: "Exact Match",
                LibraryIdentifierRestriction.LIST: "Exact Match, comma separated list",
            },
        ),
    )

    # This field lets the user choose the data source for the patron match.
    # subclasses can define this field as a more concrete type if they want.
    library_identifier_field: str = FormField(
        "barcode",
        form=ConfigurationFormItem(
            label="Library Identifier Field",
            description="This is the field on the patron record that the <em>Library Identifier Restriction "
            "Type</em> is applied to, different patron authentication methods provide different "
            "values here. This value is not used if <em>Library Identifier Restriction Type</em> "
            "is set to 'No restriction'.",
            options={
                "barcode": "Barcode",
            },
        ),
    )

    # Usually this is a string which is compared against the
    # patron's identifiers using the comparison method chosen in
    # identifier_restriction_type.
    library_identifier_restriction_criteria: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Library Identifier Restriction",
            description="This is the restriction applied to the <em>Library Identifier Field</em> "
            "using the method chosen in <em>Library Identifier Restriction Type</em>. "
            "This value is not used if <em>Library Identifier Restriction Type</em> "
            "is set to 'No restriction'.",
        ),
        alias="library_identifier_restriction",
    )

    @validator("library_identifier_restriction_criteria")
    def validate_restriction_criteria(
        cls, v: Optional[str], values: Dict[str, Any]
    ) -> Optional[str]:
        """Validate the library_identifier_restriction_criteria field."""
        if not v:
            return v

        restriction_type = values.get("library_identifier_restriction_type")
        if restriction_type == LibraryIdentifierRestriction.REGEX:
            try:
                re.compile(v)
            except re.error:
                raise SettingsValidationError(
                    problem_detail=INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION
                )

        return v


class BasicAuthenticationProvider(AuthenticationProvider, ABC):
    """Verify a username/password, obtained through HTTP Basic Auth, with
    a remote source of truth.
    """

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: BasicAuthProviderSettings,
        library_settings: BasicAuthProviderLibrarySettings,
        analytics: Analytics | None = None,
    ):
        """Create a BasicAuthenticationProvider."""
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )

        self.log = logging.getLogger(f"{self.__module__}.{self.__class__.__name__}")

        self.identifier_re = settings.identifier_regular_expression
        self.password_re = settings.password_regular_expression
        self.test_username = settings.test_identifier
        self.test_password = settings.test_password
        self.identifier_maximum_length = settings.identifier_maximum_length
        self.password_maximum_length = settings.password_maximum_length
        self.identifier_keyboard = settings.identifier_keyboard
        self.password_keyboard = settings.password_keyboard

        self.identifier_barcode_format = settings.identifier_barcode_format
        self.identifier_label = settings.identifier_label
        self.password_label = settings.password_label

        self.analytics = analytics

        self.library_identifier_field = library_settings.library_identifier_field
        self.library_identifier_restriction_type = (
            library_settings.library_identifier_restriction_type
        )
        self.library_identifier_restriction_criteria = (
            self.process_library_identifier_restriction_criteria(
                library_settings.library_identifier_restriction_criteria
            )
        )

    def process_library_identifier_restriction_criteria(
        self, criteria: Optional[str]
    ) -> str | List[str] | re.Pattern | None:
        """Process the library identifier restriction criteria."""
        if not criteria:
            return None
        if (
            self.library_identifier_restriction_type
            == LibraryIdentifierRestriction.REGEX
        ):
            return re.compile(criteria)
        elif (
            self.library_identifier_restriction_type
            == LibraryIdentifierRestriction.LIST
        ):
            return [item.strip() for item in criteria.split(",")]
        elif (
            self.library_identifier_restriction_type
            == LibraryIdentifierRestriction.NONE
        ):
            return None
        else:
            return criteria.strip()

    @property
    def authentication_realm(self) -> str:
        # Each subclass MAY override the default value for
        # AUTHENTICATION_REALM. This becomes the name of the HTTP Basic
        # Auth authentication realm.
        return "Library card"

    @property
    def flow_type(self) -> str:
        return "http://opds-spec.org/auth/basic"

    @classmethod
    def settings_class(cls) -> type[BasicAuthProviderSettings]:
        return BasicAuthProviderSettings

    @classmethod
    def library_settings_class(
        cls,
    ) -> type[BasicAuthProviderLibrarySettings]:
        return BasicAuthProviderLibrarySettings

    @abstractmethod
    def remote_patron_lookup(
        self, patron_or_patrondata: PatronData | Patron
    ) -> PatronData | ProblemDetail | None:
        """Ask the remote for detailed information about this patron

        For some authentication providers, this is not necessary. If that is the case,
        this method can just be implemented as `return patron_or_patrondata`.

        If the patron is not found, or an error occurs communicating with the remote,
        return None or a ProblemDetail.

        Otherwise, return a PatronData object with the complete property set to True.
        """
        ...

    @abstractmethod
    def remote_authenticate(
        self, username: str | None, password: str | None
    ) -> PatronData | ProblemDetail | None:
        """Does the source of truth approve of these credentials?

        If the credentials are valid, return a PatronData object. The PatronData object
        has a `complete` field. This field on the returned PatronData object will be used
        to determine if we need to call `remote_patron_lookup` later to get the complete
        information about the patron.

        If the credentials are invalid, return None.

        If there is a problem communicating with the remote, return a ProblemDetail.
        """
        ...

    @property
    def collects_password(self) -> bool:
        """Does this BasicAuthenticationProvider expect a username
        and a password, or just a username?
        """
        return self.password_keyboard != Keyboards.NULL

    def testing_patron(
        self, _db: Session
    ) -> tuple[Patron | ProblemDetail | None, str | None]:
        """Look up a Patron object reserved for testing purposes.

        :return: A 2-tuple (Patron, password)
        """
        if self.test_username is None:
            return self.test_username, self.test_password
        test_password = self.test_password or ""
        header = dict(username=self.test_username, password=test_password)
        return self.authenticated_patron(_db, header), test_password

    def testing_patron_or_bust(self, _db: Session) -> tuple[Patron, str | None]:
        """Look up the Patron object reserved for testing purposes.

        :raise:CannotLoadConfiguration: If no test patron is configured.
        :raise:IntegrationException: If the returned patron is not a Patron object.
        :return: A 2-tuple (Patron, password)
        """
        if self.test_username is None:
            raise CannotLoadConfiguration("No test patron identifier is configured.")

        patron, password = self.testing_patron(_db)
        if isinstance(patron, Patron):
            return patron, password

        if not patron:
            message = (
                "Remote declined to authenticate the test patron. "
                "The patron may not exist or its password may be wrong."
            )
        elif isinstance(patron, ProblemDetail):
            message = (
                "Test patron lookup returned a problem detail - {}: {} ({})".format(
                    patron.title, patron.detail, patron.uri
                )
            )
        else:
            message = (  # type: ignore[unreachable]
                "Test patron lookup returned invalid value for patron: {!r}".format(
                    patron
                )
            )
        raise IntegrationException(message)

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult, None, None]:
        """Verify the credentials of the test patron for this integration,
        and update its metadata.
        """
        patron_test = self.run_test(
            "Authenticating test patron", self.testing_patron_or_bust, _db
        )
        yield patron_test

        if not patron_test.success:
            # We can't run the rest of the tests.
            return

        patron, password = patron_test.result
        yield self.run_test(
            "Syncing patron metadata", self.update_patron_metadata, patron
        )

    def scrub_credential(self, value: str | None) -> str | None:
        """Scrub an incoming value that is part of a patron's set of credentials."""
        if not isinstance(value, str):
            return value
        return value.strip()

    def authenticate(
        self, _db: Session, credentials: dict
    ) -> Patron | ProblemDetail | None:
        """Turn a set of credentials into a Patron object.

        :param credentials: A dictionary with keys `username` and `password`.

        :return: A Patron if one can be authenticated; a ProblemDetail
            if an error occurs; None if the credentials are missing or wrong.
        """
        username = self.scrub_credential(credentials.get("username"))
        password = self.scrub_credential(credentials.get("password"))
        if not self.server_side_validation(username, password):
            return None

        # Check these credentials with the source of truth.
        patrondata = self.remote_authenticate(username, password)
        if patrondata is None or isinstance(patrondata, ProblemDetail):
            # Either an error occurred or the credentials did not correspond to any patron.
            return patrondata

        # Check that the patron belongs to this library.
        patrondata = self.enforce_library_identifier_restriction(patrondata)
        if patrondata is None:
            return PATRON_OF_ANOTHER_LIBRARY

        # At this point we know there is _some_ authenticated patron,
        # but it might not correspond to a Patron in our database, and
        # if it does, that Patron's authorization_identifier might be
        # different from the `username` passed in as part of the
        # credentials.

        # First, try to look up the Patron object in our database.
        patron = self.local_patron_lookup(_db, username, patrondata)
        if patron:
            # We found the patron! Now we need to make sure the patron's
            # information in the database is up-to-date.
            if not patrondata.complete and PatronUtility.needs_external_sync(patron):
                # We found the patron, but we need to sync their information with the
                # remote source of truth.  We do this by calling remote_patron_lookup.
                patrondata = self.remote_patron_lookup(patrondata)
                if not isinstance(patrondata, PatronData):
                    # Something went wrong, we can't get the patron's information.
                    # so we fail the authentication process.
                    return patrondata

            # Apply the information we have to the patron and return it.
            patrondata.apply(patron)
            return patron

        # At this point we didn't find the patron, so we want to look up the patron
        # with the remote, in case this allows us to find an existing patron, based
        # on the information returned by the remote_patron_lookup.
        if not patrondata.complete:
            patrondata = self.remote_patron_lookup(patrondata)
            if not isinstance(patrondata, PatronData):
                # Something went wrong, we can't get the patron's information.
                # so we fail the authentication process.
                return patrondata
            patron = self.local_patron_lookup(_db, username, patrondata)
            if patron:
                # We found the patron, so we apply the information we have to the patron and return it.
                patrondata.apply(patron)
                return patron

        # We didn't find the patron, so we create a new patron with the information we have.
        patron, _ = patrondata.get_or_create_patron(
            _db, self.library_id, analytics=self.analytics
        )
        return patron

    def get_credential_from_header(self, auth: Authorization) -> str | None:
        """Extract a password credential from a WWW-Authenticate header
        (or equivalent).

        This is used to pass on a patron's credential to a content provider,
        such as Overdrive, which performs independent validation of
        a patron's credentials.

        :param header: A dictionary with keys `username` and `password`.
        """
        if auth and auth.type.lower() == "basic":
            return auth.get("password", None)
        return None

    def server_side_validation(
        self, username: str | None, password: str | None
    ) -> bool:
        """Do these credentials even look right?

        Sometimes egregious problems can be caught without needing to
        check with the ILS.
        """
        if username is None or username == "":
            return False

        if self.identifier_re and self.identifier_re.match(username) is None:
            return False

        if (
            self.identifier_maximum_length
            and len(username) > self.identifier_maximum_length
        ):
            return False

        # The only legal password is an empty one.
        if not self.collects_password:
            if password not in (None, ""):
                return False
        else:
            if password is None:
                return False
            if self.password_re and self.password_re.match(password) is None:
                return False
            if (
                self.password_maximum_length
                and len(password) > self.password_maximum_length
            ):
                return False

        return True

    def local_patron_lookup(
        self, _db: Session, username: str | None, patrondata: PatronData | None
    ) -> Patron | None:
        """Try to find a Patron object in the local database.

        :param username: An HTTP Basic Auth username. May or may not
            correspond to the `Patron.username` field.

        :param patrondata: A PatronData object recently obtained from
            the source of truth, possibly as a side effect of validating
            the username and password. This may make it possible to
            identify the patron more precisely. Or it may be None, in
            which case it's no help at all.
        """

        # We're going to try a number of different strategies to look
        # up the appropriate patron based on PatronData. In theory we
        # could employ all these strategies at once (see the code at
        # the end of this method), but if the source of truth is
        # well-behaved, the first available lookup should work, and if
        # it's not, it's better to check the more reliable mechanisms
        # before the less reliable.
        lookups = []
        if patrondata:
            if patrondata.permanent_id:
                # Permanent ID is the most reliable way of identifying
                # a patron, since this is supposed to be an internal
                # ID that never changes.
                lookups.append(dict(external_identifier=patrondata.permanent_id))
            if patrondata.username:
                # Username is fairly reliable, since the patron
                # generally has to decide to change it.
                lookups.append(dict(username=patrondata.username))

            if patrondata.authorization_identifier:
                # Authorization identifiers change all the time so
                # they're not terribly reliable.
                lookups.append(
                    dict(authorization_identifier=patrondata.authorization_identifier)
                )

        patron = None
        for lookup in lookups:
            lookup["library_id"] = self.library_id
            patron = get_one(_db, Patron, **lookup)
            if patron:
                # We found them!
                break

        if not patron and username:
            # This is a Basic Auth username, but it might correspond
            # to either Patron.authorization_identifier or
            # Patron.username.
            #
            # NOTE: If patrons are allowed to choose their own
            # usernames, it's possible that a username and
            # authorization_identifier can conflict. In that case it's
            # undefined which Patron is returned from this query. If
            # this happens, it's a problem with the ILS and needs to
            # be resolved over there.
            clause = or_(
                Patron.authorization_identifier == username, Patron.username == username
            )
            qu = (
                _db.query(Patron)
                .filter(clause)
                .filter(Patron.library_id == self.library_id)
                .limit(1)
            )
            try:
                patron = qu.one()
            except NoResultFound:
                patron = None
        return patron

    @property
    def authentication_header(self) -> str:
        return f'Basic realm="{self.authentication_realm}"'

    def _authentication_flow_document(self, _db: Session) -> dict[str, Any]:
        """Create a Authentication Flow object for use in an Authentication for
        OPDS document.
        """

        login_inputs: Dict[str, Any] = dict(keyboard=self.identifier_keyboard.value)
        if self.identifier_maximum_length:
            login_inputs["maximum_length"] = self.identifier_maximum_length
        if self.identifier_barcode_format != BarcodeFormats.NONE:
            login_inputs["barcode_format"] = self.identifier_barcode_format.value

        password_inputs: Dict[str, Any] = dict(keyboard=self.password_keyboard.value)
        if self.password_maximum_length:
            password_inputs["maximum_length"] = self.password_maximum_length

        flow_doc: dict[str, Any] = dict(
            description=str(self.label()),
            labels=dict(
                login=self.identifier_label.value,
                password=self.password_label.value,
            ),
            inputs=dict(login=login_inputs, password=password_inputs),
        )
        if self.login_button_image:
            # TODO: I'm not sure if logo is appropriate for this, since it's a button
            # with the logo on it rather than a plain logo. Perhaps we should use plain
            # logos instead.
            flow_doc["links"] = [
                dict(
                    rel="logo",
                    href=url_for(
                        "static_image", filename=self.login_button_image, _external=True
                    ),
                )
            ]
        return flow_doc

    @property
    def login_button_image(self) -> str | None:
        # An AuthenticationProvider may define a custom button image for
        # clients to display when letting a user choose between different
        # AuthenticationProviders. Image files MUST be stored in the
        # `resources/images` directory - the value here should be the
        # file name.
        return None

    @property
    def identifies_individuals(self):
        # If an AuthenticationProvider authenticates patrons without identifying
        # then as specific individuals (the way a geographic gate does),
        # it should override this value and set it to False.
        return True

    @classmethod
    def _restriction_matches(
        cls,
        field: str | None,
        restriction: str | List[str] | re.Pattern | None,
        match_type: LibraryIdentifierRestriction,
    ) -> bool:
        """Does the given patron match the given restriction?"""
        if not field:
            # No field -- nothing matches.
            return False

        if not restriction:
            # No restriction -- anything matches.
            return True

        if match_type == LibraryIdentifierRestriction.REGEX:
            if restriction.search(field):  # type: ignore[union-attr]
                return True
        elif match_type == LibraryIdentifierRestriction.PREFIX:
            if field.startswith(restriction):  # type: ignore[arg-type]
                return True
        elif match_type == LibraryIdentifierRestriction.STRING:
            if field == restriction:
                return True
        elif match_type == LibraryIdentifierRestriction.LIST:
            if field in restriction:  # type: ignore[operator]
                return True

        return False

    def get_library_identifier_field_data(
        self, patrondata: PatronData
    ) -> tuple[PatronData, str | None]:
        if self.library_identifier_field.lower() == "barcode":
            return patrondata, patrondata.authorization_identifier

        if not patrondata.complete:
            remote_patrondata = self.remote_patron_lookup(patrondata)
            if not isinstance(remote_patrondata, PatronData):
                # Unable to lookup, just return the original patrondata.
                return patrondata, None
            patrondata = remote_patrondata

        return patrondata, patrondata.library_identifier

    def enforce_library_identifier_restriction(
        self, patrondata: PatronData
    ) -> PatronData | None:
        """Does the given patron match the configured library identifier restriction?"""
        if (
            self.library_identifier_restriction_type
            == LibraryIdentifierRestriction.NONE
        ):
            # No restriction to enforce.
            return patrondata

        if (
            not self.library_identifier_field
            or not self.library_identifier_restriction_type
        ):
            # Restriction field is blank, so everything matches.
            return patrondata

        patrondata, field = self.get_library_identifier_field_data(patrondata)
        if self._restriction_matches(
            field,
            self.library_identifier_restriction_criteria,
            self.library_identifier_restriction_type,
        ):
            return patrondata
        else:
            return None

    def authenticated_patron(
        self, _db: Session, authorization: dict | str
    ) -> Patron | ProblemDetail | None:
        """Go from a werkzeug.Authorization object to a Patron object.

        If the Patron needs to have their metadata updated, it happens
        transparently at this point.

        :return: A Patron if one can be authenticated; a ProblemDetail
            if an error occurs; None if the credentials are missing or wrong.
        """
        if type(authorization) != dict:
            return UNSUPPORTED_AUTHENTICATION_MECHANISM

        with elapsed_time_logging(
            log_method=self.logger().info,
            message_prefix="authenticated_patron - authenticate",
        ):
            patron = self.authenticate(_db, authorization)

        if not isinstance(patron, Patron):
            return patron
        if PatronUtility.needs_external_sync(patron):
            self.update_patron_metadata(patron)
        if patron.cached_neighborhood and not patron.neighborhood:
            # Patron.neighborhood (which is not a model field) was not
            # set, probably because we avoided an expensive metadata
            # update. But we have a cached_neighborhood (which _is_ a
            # model field) to use in situations like this.
            patron.neighborhood = patron.cached_neighborhood
        return patron

    def update_patron_metadata(self, patron: Patron) -> Patron | None:
        """Refresh our local record of this patron's account information.

        :param patron: A Patron object.
        """

        if self.library_id != patron.library_id:
            return None

        with elapsed_time_logging(
            log_method=self.logger().info,
            message_prefix=f"update_patron_metadata - remote_patron_lookup",
        ):
            remote_patron_info = self.remote_patron_lookup(patron)

        if isinstance(remote_patron_info, PatronData):
            remote_patron_info.apply(patron)

        return patron


class BearerTokenSigner:
    """Mixin class used for storing a secret used for signing Bearer tokens"""

    # Name of the site-wide ConfigurationSetting containing the secret
    # used to sign bearer tokens.
    BEARER_TOKEN_SIGNING_SECRET = Configuration.BEARER_TOKEN_SIGNING_SECRET

    @classmethod
    def bearer_token_signing_secret(cls, db):
        """Find or generate the site-wide bearer token signing secret.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: ConfigurationSetting object containing the signing secret
        :rtype: ConfigurationSetting
        """
        return ConfigurationSetting.sitewide_secret(db, cls.BEARER_TOKEN_SIGNING_SECRET)


class BaseSAMLAuthenticationProvider(AuthenticationProvider, BearerTokenSigner, ABC):
    """
    Base class for SAML authentication providers
    """

    @property
    def flow_type(self) -> str:
        return "http://librarysimplified.org/authtype/SAML-2.0"
