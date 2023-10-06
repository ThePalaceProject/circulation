from __future__ import annotations

import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Pattern, TypeVar

from flask import url_for
from pydantic import PositiveInt, validator
from sqlalchemy import or_
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
from werkzeug.datastructures import Authorization

from api.admin.problem_details import (
    INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION,
)
from api.authentication.base import (
    AuthenticationProvider,
    AuthProviderLibrarySettings,
    AuthProviderSettings,
    PatronData,
)
from api.problem_details import (
    PATRON_OF_ANOTHER_LIBRARY,
    UNSUPPORTED_AUTHENTICATION_MECHANISM,
)
from api.util.patron import PatronUtility
from core.analytics import Analytics
from core.config import CannotLoadConfiguration
from core.exceptions import IntegrationException
from core.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
    SettingsValidationError,
)
from core.model import Patron, get_one
from core.selftest import SelfTestResult
from core.util.log import elapsed_time_logging
from core.util.problem_detail import ProblemDetail


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
    identifier_label: str = FormField(
        "Barcode",
        form=ConfigurationFormItem(
            label="Label for identifier entry",
            weight=10,
        ),
    )
    password_label: str = FormField(
        "PIN",
        form=ConfigurationFormItem(
            label="Label for password entry",
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


SettingsType = TypeVar("SettingsType", bound=BasicAuthProviderSettings, covariant=True)
LibrarySettingsType = TypeVar(
    "LibrarySettingsType", bound=BasicAuthProviderLibrarySettings, covariant=True
)


class BasicAuthenticationProvider(
    AuthenticationProvider[SettingsType, LibrarySettingsType], ABC
):
    """Verify a username/password, obtained through HTTP Basic Auth, with
    a remote source of truth.
    """

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: SettingsType,
        library_settings: LibrarySettingsType,
        analytics: Analytics | None = None,
    ):
        """Create a BasicAuthenticationProvider."""
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )

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
                login=self.identifier_label,
                password=self.password_label,
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
