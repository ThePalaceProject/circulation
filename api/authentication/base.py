from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypeVar

from money import Money
from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from core.analytics import Analytics
from core.integration.base import HasLibraryIntegrationConfiguration
from core.integration.settings import BaseSettings
from core.model import CirculationEvent, Library, Patron, get_one_or_create
from core.model.hybrid import hybrid_property
from core.model.integration import IntegrationConfiguration
from core.selftest import HasSelfTests
from core.util.authentication_for_opds import OPDSAuthenticationFlow
from core.util.datetime_helpers import utc_now
from core.util.log import LoggerMixin
from core.util.problem_detail import ProblemDetail


class AuthProviderSettings(BaseSettings):
    ...


class AuthProviderLibrarySettings(BaseSettings):
    ...


SettingsType = TypeVar("SettingsType", bound=AuthProviderSettings, covariant=True)
LibrarySettingsType = TypeVar(
    "LibrarySettingsType", bound=AuthProviderLibrarySettings, covariant=True
)


class AuthenticationProvider(
    OPDSAuthenticationFlow,
    HasLibraryIntegrationConfiguration[SettingsType, LibrarySettingsType],
    HasSelfTests,
    LoggerMixin,
    ABC,
):
    """Handle a specific patron authentication scheme."""

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: SettingsType,
        library_settings: LibrarySettingsType,
        analytics: Analytics | None = None,
    ):
        self.library_id = library_id
        self.integration_id = integration_id
        self.analytics = analytics

    def library(self, _db: Session) -> Library | None:
        return Library.by_id(_db, self.library_id)

    def integration(self, _db: Session) -> IntegrationConfiguration | None:
        return (
            _db.query(IntegrationConfiguration)
            .filter(IntegrationConfiguration.id == self.integration_id)
            .one_or_none()
        )

    @property
    @abstractmethod
    def identifies_individuals(self):
        # If an AuthenticationProvider authenticates patrons without identifying
        # then as specific individuals (the way a geographic gate does),
        # it should override this value and set it to False.
        ...

    @property
    def patron_lookup_provider(self):
        """Return the provider responsible for patron lookup.

        By default, we'll put ourself forward for this task.
        """
        return self

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


AuthenticationProviderType = AuthenticationProvider[
    AuthProviderSettings, AuthProviderLibrarySettings
]


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
