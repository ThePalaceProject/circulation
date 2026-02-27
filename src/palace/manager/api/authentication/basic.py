from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Generator
from enum import Enum
from functools import partial
from re import Pattern
from typing import Annotated, Any, cast

from flask import url_for
from pydantic import PositiveInt, field_validator
from pydantic_core.core_schema import ValidationInfo
from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from palace.manager.api.admin.problem_details import (
    INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION,
)
from palace.manager.api.authentication.base import (
    AuthenticationProvider,
    AuthProviderLibrarySettings,
    AuthProviderSettings,
    PatronAuthResult,
    PatronData,
    PatronLookupNotSupported,
)
from palace.manager.api.problem_details import (
    PATRON_OF_ANOTHER_LIBRARY,
    UNSUPPORTED_AUTHENTICATION_MECHANISM,
)
from palace.manager.api.util.patron import PatronUtility
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.exceptions import IntegrationException
from palace.manager.core.selftest import SelfTestResult
from palace.manager.integration.settings import (
    FormFieldType,
    FormMetadata,
    SettingsValidationError,
)
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.log import elapsed_time_logging
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class LibraryIdentifierRestriction(Enum):
    NONE = "none"
    REGEX = "regex"
    PREFIX = "prefix"
    STRING = "string"
    LIST = "list"
    PREFIX_LIST = "prefix_list"


class LibraryIdenfitierRestrictionField(Enum):
    BARCODE = "barcode"
    PATRON_LIBRARY = "patron location"


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
    test_identifier: Annotated[
        str | None,
        FormMetadata(
            label="Test identifier",
            description="A test identifier to use when testing the authentication provider.",
            weight=10,
            required=True,
        ),
    ] = None
    test_password: Annotated[
        str | None,
        FormMetadata(
            label="Test password",
            description="A test password to use when testing the authentication provider.",
            weight=10,
        ),
    ] = None
    identifier_barcode_format: Annotated[
        BarcodeFormats,
        FormMetadata(
            label="Patron identifier barcode format",
            description="Many libraries render patron identifiers as barcodes on "
            "physical library cards. If you specify the barcode format, patrons "
            "will be able to scan their library cards with a camera instead of "
            "manually typing in their identifiers.",
            type=FormFieldType.SELECT,
            options={
                BarcodeFormats.NONE: "Patron identifiers are not rendered as barcodes",
                BarcodeFormats.CODABAR: "Patron identifiers are rendered as barcodes "
                "in Codabar format",
            },
            required=True,
            weight=10,
        ),
    ] = BarcodeFormats.NONE
    # By default, patron identifiers can only contain alphanumerics and
    # a few other characters.
    identifier_regular_expression: Annotated[
        Pattern,
        FormMetadata(
            label="Identifier Regular Expression",
            description="A patron's identifier will be immediately rejected if it doesn't match this "
            "regular expression.",
            weight=10,
        ),
    ] = re.compile(r"^[A-Za-z0-9@.-]+$")
    # By default, there are no restrictions on passwords.
    password_regular_expression: Annotated[
        Pattern | None,
        FormMetadata(
            label="Password Regular Expression",
            description="A patron's password will be immediately rejected if it doesn't match this "
            "regular expression.",
            weight=10,
        ),
    ] = None
    identifier_keyboard: Annotated[
        Keyboards,
        FormMetadata(
            label="Keyboard for identifier entry",
            type=FormFieldType.SELECT,
            options={
                Keyboards.DEFAULT: "System default",
                Keyboards.EMAIL_ADDRESS: "Email address entry",
                Keyboards.NUMBER_PAD: "Number pad",
            },
            required=True,
            weight=10,
        ),
    ] = Keyboards.DEFAULT
    password_keyboard: Annotated[
        Keyboards,
        FormMetadata(
            label="Keyboard for password entry",
            type=FormFieldType.SELECT,
            options={
                Keyboards.DEFAULT: "System default",
                Keyboards.NUMBER_PAD: "Number pad",
                Keyboards.NULL: "Patrons have no password and should not be prompted for one.",
            },
            weight=10,
        ),
    ] = Keyboards.DEFAULT
    identifier_maximum_length: Annotated[
        PositiveInt | None,
        FormMetadata(
            label="Maximum identifier length",
            weight=10,
        ),
    ] = None
    password_maximum_length: Annotated[
        PositiveInt | None,
        FormMetadata(
            label="Maximum password length",
            weight=10,
        ),
    ] = None
    identifier_label: Annotated[
        str,
        FormMetadata(
            label="Label for identifier entry",
            weight=10,
        ),
    ] = "Barcode"
    password_label: Annotated[
        str,
        FormMetadata(
            label="Label for password entry",
            weight=10,
        ),
    ] = "PIN"


class BasicAuthProviderLibrarySettings(AuthProviderLibrarySettings):
    # When multiple libraries share an ILS, a person may be able to
    # authenticate with the ILS but not be considered a patron of
    # _this_ library. This setting contains the rule for determining
    # whether an identifier is valid for a specific library.
    library_identifier_restriction_type: Annotated[
        LibraryIdentifierRestriction,
        FormMetadata(
            label="Library Identifier Restriction Type",
            type=FormFieldType.SELECT,
            description="When multiple libraries share an ILS, a person may be able to "
            "authenticate with the ILS, but not be considered a patron of "
            "<em>this</em> library. This setting contains the rule for determining "
            "whether an identifier is valid for this specific library. <p/> "
            "If this setting is set to 'No Restriction', then the values for "
            "<em>Library Identifier Field</em> and <em>Library Identifier "
            "Restriction</em> will not be used.",
            options={
                LibraryIdentifierRestriction.NONE: "No Restriction",
                LibraryIdentifierRestriction.REGEX: "Regex Match",
                LibraryIdentifierRestriction.PREFIX: "Prefix Match",
                LibraryIdentifierRestriction.STRING: "Exact Match",
                LibraryIdentifierRestriction.LIST: "Exact Match, comma separated list",
                LibraryIdentifierRestriction.PREFIX_LIST: "Prefix Match, comma separated list",
            },
        ),
    ] = LibraryIdentifierRestriction.NONE

    # This field lets the user choose the data source for the patron match.
    # subclasses can define this field as a more concrete type if they want.
    library_identifier_field: Annotated[
        str,
        FormMetadata(
            label="Library Identifier Field",
            type=FormFieldType.SELECT,
            description="This is the field on the patron record that the <em>Library Identifier Restriction "
            "Type</em> is applied to, different patron authentication methods provide different "
            "values here. This value is not used if <em>Library Identifier Restriction Type</em> "
            "is set to 'No restriction'.",
            options={
                LibraryIdenfitierRestrictionField.BARCODE: "Barcode",
                LibraryIdenfitierRestrictionField.PATRON_LIBRARY: "Patron Location",
            },
        ),
    ] = "barcode"

    # Usually this is a string which is compared against the
    # patron's identifiers using the comparison method chosen in
    # identifier_restriction_type.
    library_identifier_restriction_criteria: Annotated[
        str | None,
        FormMetadata(
            label="Library Identifier Restriction",
            description="This is the restriction applied to the <em>Library Identifier Field</em> "
            "using the method chosen in <em>Library Identifier Restriction Type</em>. "
            "This value is not used if <em>Library Identifier Restriction Type</em> "
            "is set to 'No restriction'.",
        ),
    ] = None

    @field_validator("library_identifier_restriction_criteria")
    @classmethod
    def validate_restriction_criteria(
        cls, restriction_criteria: str | None, info: ValidationInfo
    ) -> str | None:
        """Validate the library_identifier_restriction_criteria field."""
        restriction_type = info.data.get("library_identifier_restriction_type")
        if (
            restriction_criteria
            and restriction_type == LibraryIdentifierRestriction.REGEX
        ):
            try:
                re.compile(restriction_criteria)
            except re.error:
                raise SettingsValidationError(
                    problem_detail=INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION
                )
        return restriction_criteria


class BasicAuthenticationProvider[
    SettingsType: BasicAuthProviderSettings,
    LibrarySettingsType: BasicAuthProviderLibrarySettings,
](AuthenticationProvider[SettingsType, LibrarySettingsType], ABC):
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
        self, criteria: str | None
    ) -> str | list[str] | re.Pattern | None:
        """Process the library identifier restriction criteria."""
        if not criteria:
            return None
        if (
            self.library_identifier_restriction_type
            == LibraryIdentifierRestriction.REGEX
        ):
            return re.compile(criteria)
        elif self.library_identifier_restriction_type in (
            LibraryIdentifierRestriction.LIST,
            LibraryIdentifierRestriction.PREFIX_LIST,
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

        try:
            patron, password = self.testing_patron(_db)
        except ProblemDetailException as e:
            patron = e.problem_detail

        if isinstance(patron, Patron):
            return patron, password

        debug_message = None
        if not patron:
            message = (
                "Remote declined to authenticate the test patron. "
                "The patron may not exist or its password may be wrong."
            )
        elif isinstance(patron, ProblemDetail):
            pd = patron
            message = f"Test patron lookup returned a problem detail - {pd.title}: {pd.detail} ({pd.uri})"
            if pd.debug_message:
                message += f" [{pd.debug_message}]"
                debug_message = pd.debug_message
        else:
            message = (  # type: ignore[unreachable]
                "Test patron lookup returned invalid value for patron: {!r}".format(
                    patron
                )
            )
        raise IntegrationException(message, debug_message=debug_message)

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
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

    def check_library_identifier_restriction(
        self, patrondata: PatronData
    ) -> tuple[PatronData, PatronAuthResult]:
        """Check whether the patron matches the configured library identifier restriction.

        Returns the (possibly updated) *patrondata* together with a
        :class:`PatronAuthResult` carrying diagnostic details.
        :meth:`enforce_library_identifier_restriction` delegates to this method
        and raises on failure.

        :param patrondata: A PatronData object.
        :returns: A 2-tuple of (patrondata, result).
        """
        result = partial(PatronAuthResult, label="Library Identifier Restriction")

        restriction_type = self.library_identifier_restriction_type
        if restriction_type == LibraryIdentifierRestriction.NONE:
            return patrondata, result(success=True, details="No restriction configured")

        field = self.library_identifier_field
        if not field or not restriction_type:
            return patrondata, result(
                success=True, details="No restriction field configured"
            )

        criteria = self.library_identifier_restriction_criteria
        patrondata, field_value = self.get_library_identifier_field_data(patrondata)

        is_valid, reason = self._restriction_matches(
            field_value, criteria, restriction_type
        )

        details: dict[str, str] = {
            "restriction_type": restriction_type.value,
            "field": str(field),
            "criteria": str(criteria),
            "field_value": str(field_value),
            "result": "match" if is_valid else "no match",
        }
        if not is_valid:
            details["failure_reason"] = reason

        return patrondata, result(success=is_valid, details=details)

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
    ) -> PatronAuthResult:
        """Validate credentials locally before checking with the ILS.

        Returns a :class:`PatronAuthResult` with diagnostic details.
        The result is falsy when validation fails (via ``__bool__``),
        so existing call sites like ``if not self.server_side_validation(...)``
        continue to work unchanged.
        """
        result = partial(PatronAuthResult, label="Server-Side Validation")

        if username is None or username == "":
            return result(success=False, details="Username is empty")

        if self.identifier_re and self.identifier_re.match(username) is None:
            return result(
                success=False,
                details=f"Identifier {username!r} does not match pattern {self.identifier_re.pattern!r}",
            )

        if (
            self.identifier_maximum_length
            and len(username) > self.identifier_maximum_length
        ):
            return result(
                success=False,
                details=f"Identifier exceeds maximum length of {self.identifier_maximum_length}",
            )

        if not self.collects_password:
            if password not in (None, ""):
                return result(
                    success=False,
                    details="Password provided but this method does not collect passwords",
                )
        else:
            if password is None:
                return result(
                    success=False,
                    details="Password is required but was not provided",
                )
            if self.password_re and self.password_re.match(password) is None:
                return result(
                    success=False,
                    details=f"Password does not match pattern {self.password_re.pattern!r}",
                )
            if (
                self.password_maximum_length
                and len(password) > self.password_maximum_length
            ):
                return result(
                    success=False,
                    details=f"Password exceeds maximum length of {self.password_maximum_length}",
                )

        return result(
            success=True,
            details={
                "identifier_regular_expression": (
                    self.identifier_re.pattern if self.identifier_re else None
                ),
                "identifier_maximum_length": self.identifier_maximum_length,
                "password_regular_expression": (
                    self.password_re.pattern if self.password_re else None
                ),
                "password_maximum_length": self.password_maximum_length,
                "collects_password": self.collects_password,
            },
        )

    @property
    def authentication_header(self) -> str:
        return f'Basic realm="{self.authentication_realm}"'

    def _authentication_flow_document(self, _db: Session) -> dict[str, Any]:
        """Create a Authentication Flow object for use in an Authentication for
        OPDS document.
        """

        login_inputs: dict[str, Any] = dict(keyboard=self.identifier_keyboard.value)
        if self.identifier_maximum_length:
            login_inputs["maximum_length"] = self.identifier_maximum_length
        if self.identifier_barcode_format != BarcodeFormats.NONE:
            login_inputs["barcode_format"] = self.identifier_barcode_format.value

        password_inputs: dict[str, Any] = dict(keyboard=self.password_keyboard.value)
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
        value: str | None,
        restriction: str | list[str] | re.Pattern | None,
        match_type: LibraryIdentifierRestriction,
    ) -> tuple[bool, str]:
        """Does the given patron match the given restriction?

        :param value: The value from the field we're matching against the restriction.
        :param restriction: The restriction value.
        :param match_type: The type of match we're performing.
        :returns: True if the value matches the restriction, False otherwise.
        """
        value = value or ""

        failure_reason = ""
        match [match_type, restriction, value]:
            case [LibraryIdentifierRestriction.NONE, *_]:
                pass
            case [_, _restriction, _] if not _restriction:
                pass
            case [_, _, _value] if _value is None or _value == "":
                failure_reason = "No value in field"
            case [LibraryIdentifierRestriction.REGEX, *_]:
                if not (_pattern := cast(Pattern, restriction)).search(value):
                    failure_reason = f"{value!r} does not match regular expression {_pattern.pattern!r}"
            case [LibraryIdentifierRestriction.PREFIX, *_]:
                if not value.startswith(_string := cast(str, restriction)):
                    failure_reason = f"{value!r} does not start with {_string!r}"
            case [LibraryIdentifierRestriction.STRING, *_]:
                if value != (_string := cast(str, restriction)):
                    failure_reason = f"{value!r} does not exactly match {_string!r}"
            case [LibraryIdentifierRestriction.LIST, *_]:
                if value not in (_list := cast(list, restriction)):
                    failure_reason = f"{value!r} not in list {restriction!r}"
            case [LibraryIdentifierRestriction.PREFIX_LIST, *_]:
                matches = False
                for prefix in (_list := cast(list, restriction)):
                    if value.startswith(prefix):
                        matches = True
                        break

                if not matches:
                    failure_reason = f"{value!r} does not match any of the prefixes in list {restriction!r}"

        return (False, failure_reason) if failure_reason else (True, "")

    @staticmethod
    def _lookup_in_predefined_field(
        patrondata: PatronData, id_field: str
    ) -> str | None:
        predefined_fields = {
            k.lower(): v
            for k, v in {
                LibraryIdenfitierRestrictionField.BARCODE.value: patrondata.authorization_identifier,
                LibraryIdenfitierRestrictionField.PATRON_LIBRARY.value: patrondata.library_identifier,
            }.items()
        }
        return predefined_fields.get(id_field.lower())

    def get_library_identifier_field_data(
        self, patrondata: PatronData
    ) -> tuple[PatronData, str | None]:
        id_field = self.library_identifier_field
        if (
            field_value := self._lookup_in_predefined_field(patrondata, id_field)
        ) is not None:
            return patrondata, field_value

        if not patrondata.complete:
            try:
                remote_patrondata = self.remote_patron_lookup(patrondata)
            except PatronLookupNotSupported:
                remote_patrondata = patrondata
            if not isinstance(remote_patrondata, PatronData):
                # Unable to lookup, just return the original patrondata.
                return patrondata, None
            patrondata = remote_patrondata

        if (
            field_value := self._lookup_in_predefined_field(patrondata, id_field)
        ) is not None:
            return patrondata, field_value

        # By default, we'll return the `library_identifier` value.
        return patrondata, patrondata.library_identifier

    def enforce_library_identifier_restriction(
        self, patrondata: PatronData
    ) -> PatronData:
        """Ensure the patron matches the configured library identifier restriction.

        Delegates to :meth:`check_library_identifier_restriction` and raises
        when the check fails.

        :param patrondata: A PatronData object.
        :returns: The (possibly updated) PatronData object.
        :raises ProblemDetailException: With `PATRON_OF_ANOTHER_LIBRARY`
            ProblemDetail, if the given patron does not match.
        """
        patrondata, result = self.check_library_identifier_restriction(patrondata)
        if not result:
            details = result.details
            reason = (
                details.get("failure_reason", "")
                if isinstance(details, dict)
                else str(details)
            )
            raise ProblemDetailException(
                PATRON_OF_ANOTHER_LIBRARY.with_debug(
                    f"{self.library_identifier_field!r} does not match library restriction: {reason}."
                )
            )
        return patrondata

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
            try:
                remote_patron_info = self.remote_patron_lookup(patron)
            except PatronLookupNotSupported:
                remote_patron_info = None

        if isinstance(remote_patron_info, PatronData):
            remote_patron_info.apply(patron)

        return patron
