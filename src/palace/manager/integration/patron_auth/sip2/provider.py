from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Any

from annotated_types import Ge, Le
from pydantic import BaseModel, ConfigDict, PositiveInt, field_validator

from palace.manager.api.admin.problem_details import INVALID_CONFIGURATION_OPTION
from palace.manager.api.authentication.base import PatronAuthResult, PatronData
from palace.manager.api.authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)
from palace.manager.api.authentication.patron_debug import HasPatronDebug
from palace.manager.api.problem_details import BLOCKED_CREDENTIALS, INVALID_CREDENTIALS
from palace.manager.integration.patron_auth.sip2.client import Sip2Encoding, SIPClient
from palace.manager.integration.patron_auth.sip2.dialect import Dialect as Sip2Dialect
from palace.manager.integration.settings import (
    FormFieldType,
    FormMetadata,
    SettingsValidationError,
)
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util import MoneyUtility
from palace.manager.util.problem_detail import ProblemDetail


class PatronBlockingRule(BaseModel):
    """A single patron blocking rule stored in SIP2 per-library settings."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    name: str
    rule: str
    message: str | None = None


class SIP2Settings(BasicAuthProviderSettings):
    """Settings for SIP2 authentication providers."""

    # Hostname of the SIP server
    url: Annotated[
        str,
        FormMetadata(label="Server"),
    ]
    # The port number to connect to on the SIP server.
    port: Annotated[
        PositiveInt,
        FormMetadata(
            label="Port",
            required=True,
        ),
    ] = 6001
    # SIP field CN; the user ID to use when initiating a SIP session, if necessary.
    # This is _not_ a patron identifier (SIP field AA); it identifies the SC
    # creating the SIP session. SIP2 defines SC as "...any library automation
    # device dealing with patrons or library materials."
    username: Annotated[
        str | None,
        FormMetadata(
            label="Login User ID",
        ),
    ] = None
    # Sip field CO; the password to use when initiating a SIP session, if necessary.
    password: Annotated[
        str | None,
        FormMetadata(
            label="Login Password",
        ),
    ] = None
    # SIP field CP; the location code to use when initiating a SIP session. A
    # location code supposedly refers to the physical location of a self-checkout
    # machine within a library system. Some libraries require a special location
    # code to be provided when authenticating patrons; others may require the
    # circulation manager to be treated as its own special 'location'.
    location_code: Annotated[
        str | None,
        FormMetadata(
            label="Location Code",
        ),
    ] = None
    encoding: Annotated[
        Sip2Encoding,
        FormMetadata(
            label="Data Encoding",
            type=FormFieldType.SELECT,
            options={
                Sip2Encoding.utf8: "UTF-8",
                Sip2Encoding.cp850: "CP850",
            },
            description=(
                "By default, SIP2 servers encode outgoing data using the Code Page "
                "850 encoding, but some ILSes allow some other encoding to be used, "
                "usually UTF-8."
            ),
        ),
    ] = Sip2Encoding.cp850
    use_ssl: Annotated[
        bool,
        FormMetadata(
            label="Connect over SSL?",
            type=FormFieldType.SELECT,
            options={
                True: "Connect to the SIP2 server over SSL",
                False: "Connect to the SIP2 server over an ordinary socket connection",
            },
            required=True,
        ),
    ] = False
    ssl_verification: Annotated[
        bool,
        FormMetadata(
            label="Perform SSL certificate verification.",
            type=FormFieldType.SELECT,
            options={
                True: "Perform SSL certificate verification.",
                False: "Do not perform SSL certificate verification.",
            },
            description=(
                "Strict certificate verification may be optionally turned off for "
                "hosts that have misconfigured or untrusted certificates."
            ),
        ),
    ] = True
    ils: Annotated[
        Sip2Dialect,
        FormMetadata(
            label="ILS Dialect",
            description=(
                "Some ILS require specific SIP2 settings. If the ILS you are using "
                f"is in the list, please pick it. Otherwise, select '{Sip2Dialect.preferred()}'."
            ),
            type=FormFieldType.SELECT,
            options=Sip2Dialect.form_options(),
            required=True,
        ),
    ] = Sip2Dialect.GENERIC_ILS
    ssl_certificate: Annotated[
        str | None,
        FormMetadata(
            label="SSL Certificate",
            description=(
                "The SSL certificate used to securely connect to an SSL-enabled SIP2 "
                "server. Not all SSL-enabled SIP2 servers require a custom "
                "certificate, but some do. This should be a string beginning with "
                "<code>-----BEGIN CERTIFICATE-----</code> and ending with "
                "<code>-----END CERTIFICATE-----</code>"
            ),
            type=FormFieldType.TEXTAREA,
        ),
    ] = None
    ssl_key: Annotated[
        str | None,
        FormMetadata(
            label="SSL Key",
            description=(
                "The private key, if any, used to sign the SSL certificate above. "
                "If present, this should be a string beginning with "
                "<code>-----BEGIN PRIVATE KEY-----</code> and ending with "
                "<code>-----END PRIVATE KEY-----</code>"
            ),
            type=FormFieldType.TEXTAREA,
        ),
    ] = None
    # The field delimiter (see "Variable-length fields" in the SIP2 spec). If no
    # value is specified, the default (the pipe character) will be used.
    field_separator: Annotated[
        str,
        FormMetadata(
            label="Field Seperator",
            required=True,
        ),
    ] = "|"
    patron_status_block: Annotated[
        bool,
        FormMetadata(
            label="SIP2 Patron Status Block",
            description=(
                "Block patrons from borrowing based on the status of the SIP2 <em>patron status</em> field."
            ),
            type=FormFieldType.SELECT,
            options={
                True: "Block based on patron status field",
                False: "No blocks based on patron status field",
            },
        ),
    ] = True
    timeout: Annotated[
        int,
        FormMetadata(
            label="Timeout",
            description=(
                "The number of seconds to wait for a response from the SIP2 server "
                "before timing out. The default is 3 seconds. <em>Use caution</em> when increasing "
                "this value, as it can slow down the authentication process. Value must be "
                "between 1 and 9 seconds."
            ),
            type=FormFieldType.NUMBER,
        ),
        Ge(1),
        Le(9),
    ] = 3


class SIP2LibrarySettings(BasicAuthProviderLibrarySettings):
    # Used as the SIP2 AO field.
    institution_id: Annotated[
        str | None,
        FormMetadata(
            label="Institution ID",
            description="A specific identifier for the library or branch, if used in patron authentication",
        ),
    ] = None

    patron_blocking_rules: Annotated[
        list[PatronBlockingRule],
        FormMetadata(
            label="Patron Blocking Rules",
            description=(
                "A list of rules that can block patron access. Each rule has a name, "
                "a rule expression, and an optional message shown to the patron when blocked."
            ),
            hidden=True,
        ),
    ] = []

    @field_validator("patron_blocking_rules")
    @classmethod
    def validate_patron_blocking_rules(
        cls, rules: list[PatronBlockingRule]
    ) -> list[PatronBlockingRule]:
        names_seen: set[str] = set()
        for i, rule in enumerate(rules):
            if not rule.name:
                raise SettingsValidationError(
                    INVALID_CONFIGURATION_OPTION.detailed(
                        f"Rule at index {i}: 'name' must not be empty"
                    )
                )
            if not rule.rule:
                raise SettingsValidationError(
                    INVALID_CONFIGURATION_OPTION.detailed(
                        f"Rule at index {i}: 'rule' expression must not be empty"
                    )
                )
            if rule.name in names_seen:
                raise SettingsValidationError(
                    INVALID_CONFIGURATION_OPTION.detailed(
                        f"Rule at index {i}: duplicate rule name '{rule.name}'"
                    )
                )
            names_seen.add(rule.name)
        return rules


def check_patron_blocking_rules(
    rules: list[PatronBlockingRule],
) -> ProblemDetail | None:
    """Check a patron against a list of blocking rules.

    For v1, any rule whose ``rule`` expression equals the literal string
    ``"BLOCK"`` triggers a block. All other expressions are ignored (stub).

    :param rules: The list of PatronBlockingRule objects configured for a library.
    :return: A ProblemDetail if the patron is blocked, else None.
    """
    for rule in rules:
        if rule.rule == "BLOCK":
            detail = rule.message or "Access blocked by library policy."
            return BLOCKED_CREDENTIALS.detailed(detail)
    return None


class SIP2AuthenticationProvider(
    BasicAuthenticationProvider[SIP2Settings, SIP2LibrarySettings],
    HasPatronDebug,
):
    DATE_FORMATS = ["%Y%m%d", "%Y%m%d%Z%H%M%S", "%Y%m%d    %H%M%S"]

    # Map the reasons why SIP2 might report a patron is blocked to the
    # protocol-independent block reason used by PatronData.
    SPECIFIC_BLOCK_REASONS = {
        SIPClient.CARD_REPORTED_LOST: PatronData.CARD_REPORTED_LOST,
        SIPClient.EXCESSIVE_FINES: PatronData.EXCESSIVE_FINES,
        SIPClient.EXCESSIVE_FEES: PatronData.EXCESSIVE_FEES,
        SIPClient.TOO_MANY_ITEMS_BILLED: PatronData.TOO_MANY_ITEMS_BILLED,
        SIPClient.CHARGE_PRIVILEGES_DENIED: PatronData.NO_BORROWING_PRIVILEGES,
        SIPClient.TOO_MANY_ITEMS_CHARGED: PatronData.TOO_MANY_LOANS,
        SIPClient.TOO_MANY_ITEMS_OVERDUE: PatronData.TOO_MANY_OVERDUE,
        SIPClient.TOO_MANY_RENEWALS: PatronData.TOO_MANY_RENEWALS,
        SIPClient.TOO_MANY_LOST: PatronData.TOO_MANY_LOST,
        SIPClient.RECALL_OVERDUE: PatronData.RECALL_OVERDUE,
    }

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: SIP2Settings,
        library_settings: SIP2LibrarySettings,
        analytics: Analytics | None = None,
        client: Callable[..., SIPClient] | None = None,
    ):
        """An object capable of communicating with a SIP server."""
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )

        self.server = settings.url
        self.port = settings.port
        self.login_user_id = settings.username
        self.login_password = settings.password
        self.location_code = settings.location_code
        self.encoding = settings.encoding.value
        self.field_separator = settings.field_separator
        self.use_ssl = settings.use_ssl
        self.ssl_cert = settings.ssl_certificate
        self.ssl_key = settings.ssl_key
        self.ssl_verification = settings.ssl_verification
        self.dialect = settings.ils
        self.institution_id = library_settings.institution_id
        self.patron_blocking_rules = library_settings.patron_blocking_rules
        self.timeout = settings.timeout
        self._client = client

        # Check if patrons should be blocked based on SIP status
        if settings.patron_status_block:
            _deny_fields = SIPClient.PATRON_STATUS_FIELDS_THAT_DENY_BORROWING_PRIVILEGES
            self.patron_status_should_block = True
            self.fields_that_deny_borrowing = _deny_fields
        else:
            self.patron_status_should_block = False
            self.fields_that_deny_borrowing = []

    @property
    def client(self) -> SIPClient:
        """Initialize a SIPClient object using the default settings.

        :return: A SIPClient
        """
        sip_client = self._client or SIPClient
        return sip_client(
            target_server=self.server,
            target_port=self.port,
            login_user_id=self.login_user_id,
            login_password=self.login_password,
            location_code=self.location_code,
            institution_id=self.institution_id or "",
            separator=self.field_separator,
            use_ssl=self.use_ssl,
            ssl_cert=self.ssl_cert,
            ssl_key=self.ssl_key,
            ssl_verification=self.ssl_verification,
            encoding=self.encoding.lower(),
            dialect=self.dialect,
            timeout=self.timeout,
        )

    def authenticate(self, _db, credentials: dict) -> Patron | ProblemDetail | None:
        """Authenticate a patron and apply any configured blocking rules.

        Blocking rules are evaluated after the base authentication succeeds.
        If a rule triggers a block, authentication is stopped and a ProblemDetail
        is returned instead of the Patron object.
        """
        result = super().authenticate(_db, credentials)
        if isinstance(result, Patron):
            blocked = check_patron_blocking_rules(self.patron_blocking_rules)
            if blocked is not None:
                return blocked
        return result

    @classmethod
    def label(cls) -> str:
        return "SIP2"

    @classmethod
    def description(cls) -> str:
        return "SIP2 Patron Authentication"

    @classmethod
    def settings_class(cls) -> type[SIP2Settings]:
        return SIP2Settings

    @classmethod
    def library_settings_class(cls) -> type[SIP2LibrarySettings]:
        return SIP2LibrarySettings

    def patron_information(
        self, username: str | None, password: str | None
    ) -> dict[str, Any] | ProblemDetail:
        try:
            sip = self.client
            sip.connect()
            sip.login()
            sip.sc_status()
            info = sip.patron_information(username, password)
            sip.end_session(username, password)
            sip.disconnect()
            return info

        except OSError as e:
            server_name = self.server or "unknown server"
            self.log.warning(f"SIP2 error ({server_name}): {str(e)}", exc_info=e)
            return INVALID_CREDENTIALS.detailed(
                f"Error contacting authentication server ({server_name}). Please try again later."
            )

    def remote_patron_lookup(
        self, patron_or_patrondata: PatronData | Patron
    ) -> PatronData | None | ProblemDetail:
        info = self.patron_information(
            patron_or_patrondata.authorization_identifier, None
        )
        return self.info_to_patrondata(info, False)

    def remote_authenticate(
        self, username: str | None, password: str | None
    ) -> PatronData | None | ProblemDetail:
        """Authenticate a patron with the SIP2 server.

        :param username: The patron's username/barcode/card
            number/authorization identifier.
        :param password: The patron's password/pin/access code.
        """
        if not self.collects_password:
            # Even if we were somehow given a password, we won't be
            # passing it on.
            password = None
        info = self.patron_information(username, password)
        return self.info_to_patrondata(info)

    def patron_debug(
        self, username: str, password: str | None
    ) -> list[PatronAuthResult]:
        """Run step-by-step diagnostic checks against the SIP2 server."""
        results: list[PatronAuthResult] = []

        validation_result = self.server_side_validation(username, password)
        results.append(validation_result)

        sip = None
        try:
            sip = self.client
            sip.connect()
            results.append(
                PatronAuthResult(
                    label="SIP2 Connection",
                    success=True,
                    details=f"{self.server}:{self.port}",
                )
            )

            results.append(
                PatronAuthResult(
                    label="SIP2 Login",
                    success=True,
                    details=sip.login(),
                )
            )

            results.append(
                PatronAuthResult(
                    label="SC Status",
                    success=True,
                    details=sip.sc_status(),
                )
            )

            info = sip.patron_information(username, password)
            results.append(
                PatronAuthResult(
                    label="Patron Information Request",
                    success=True,
                    details=info,
                )
            )

            valid_password = info.get("valid_patron_password", "")
            valid_patron = info.get("valid_patron", "")
            results.append(
                PatronAuthResult(
                    label="Password Validation",
                    success=valid_password != "N" and valid_patron != "N",
                    details=f"valid_patron={valid_patron}, valid_patron_password={valid_password}",
                )
            )

            patron_status_parsed = info.get("patron_status_parsed", {}).copy()
            blocking_flags = [
                k
                for k in self.fields_that_deny_borrowing
                if patron_status_parsed.get(k) is True
            ]
            status_success = (
                len(blocking_flags) == 0 or not self.patron_status_should_block
            )
            if not status_success:
                patron_status_parsed["blocking_flags"] = ", ".join(blocking_flags)
            results.append(
                PatronAuthResult(
                    label="Patron Status Flags",
                    success=status_success,
                    details=patron_status_parsed,
                )
            )

            patrondata = self.info_to_patrondata(info, validate_password=False)
            if isinstance(patrondata, PatronData):
                patrondata, restriction_result = (
                    self.check_library_identifier_restriction(patrondata)
                )
                results.append(restriction_result)

                results.append(
                    PatronAuthResult(
                        label="Parsed Patron Data",
                        success=True,
                        details=patrondata.to_dict,
                    )
                )

        finally:
            if sip is not None:
                try:
                    sip.end_session(username, password)
                    sip.disconnect()
                except Exception:
                    self.log.exception("Failed to close SIP session")

        return results

    def _run_self_tests(self, _db):
        def makeConnection(sip):
            sip.connect()
            return sip.connection

        sip = self.client
        connection = self.run_test(("Test Connection"), makeConnection, sip)
        yield connection

        if not connection.success:
            return

        login = self.run_test(
            (
                "Test Login with username '%s' and password '%s'"
                % (self.login_user_id, self.login_password)
            ),
            sip.login,
        )
        yield login

        def raw_sc_status_information():
            info = sip.sc_status()
            return json.dumps(info, indent=2)

        yield self.run_test(
            "ILS SIP Service Info (SC Status)", raw_sc_status_information
        )

        # Log in was successful so test patron's test credentials
        if login.success:
            if self.test_username:

                def raw_patron_information():
                    info = sip.patron_information(
                        self.test_username, self.test_password
                    )
                    return json.dumps(info, indent=1)

                yield self.run_test(
                    "Patron information request",
                    sip.patron_information_request,
                    self.test_username,
                    patron_password=self.test_password,
                )

                yield self.run_test(
                    ("Raw test patron information"), raw_patron_information
                )

            yield from super()._run_self_tests(_db)

    def info_to_patrondata(
        self, info: dict[str, Any] | ProblemDetail, validate_password: bool = True
    ) -> PatronData | None | ProblemDetail:
        """Convert the SIP-specific dictionary obtained from
        SIPClient.patron_information() to an abstract,
        authenticator-independent PatronData object.
        """
        if isinstance(info, ProblemDetail):
            return info

        if info.get("valid_patron", "N") == "N":
            # The patron could not be identified as a patron of this
            # library. Don't return any data.
            return None

        if info.get("valid_patron_password") == "N" and validate_password:
            # The patron did not authenticate correctly. Don't
            # return any data.
            return None

        # TODO: I'm not 100% convinced that a missing CQ field
        # always means "we don't have passwords so you're
        # authenticated," rather than "you didn't provide a
        # password so we didn't check."
        patrondata = PatronData()
        if "sipserver_internal_id" in info:
            patrondata.permanent_id = info["sipserver_internal_id"]
        if "patron_identifier" in info:
            patrondata.authorization_identifier = info["patron_identifier"]
        if "email_address" in info:
            patrondata.email_address = info["email_address"]
        if "personal_name" in info:
            patrondata.personal_name = info["personal_name"]
        if "permanent_location" in info:
            patrondata.library_identifier = info["permanent_location"]
        if "fee_amount" in info:
            fines = info["fee_amount"]
        else:
            fines = "0"
        patrondata.fines = MoneyUtility.parse(fines)
        if "sipserver_patron_class" in info:
            patrondata.external_type = info["sipserver_patron_class"]

        # If we don't have any expiry information, we set it to NO_VALUE,
        # so the expiry gets cleared if there once was an expiry, and there
        # no longer is, since this can prevent borrowing otherwise.
        patron_expiry = PatronData.NO_VALUE
        for expire_field in [
            "sipserver_patron_expiration",
            "polaris_patron_expiration",
        ]:
            if expire_field in info:
                if (expiry := self.parse_date(info.get(expire_field))) is not None:
                    patron_expiry = expiry
                    break
        patrondata.authorization_expires = patron_expiry

        if self.patron_status_should_block:
            patrondata.block_reason = self.info_to_patrondata_block_reason(
                info, patrondata
            )
        else:
            patrondata.block_reason = PatronData.NO_VALUE

        return patrondata

    def info_to_patrondata_block_reason(
        self, info, patrondata: PatronData
    ) -> PatronData.NoValue | str:
        # A True value in most (but not all) subfields of the
        # patron_status field will prohibit the patron from borrowing
        # books.
        status = info["patron_status_parsed"]
        block_reason: str | PatronData.NoValue = PatronData.NO_VALUE
        for field in self.fields_that_deny_borrowing:
            if status.get(field) is True:
                block_reason = self.SPECIFIC_BLOCK_REASONS.get(
                    field, PatronData.UNKNOWN_BLOCK
                )
                if block_reason not in (PatronData.NO_VALUE, PatronData.UNKNOWN_BLOCK):
                    # Even if there are multiple problems with this
                    # patron's account, we can now present a specific
                    # error message. There's no need to look through
                    # more fields.
                    break

        # If we can tell by looking at the SIP2 message that the
        # patron has excessive fines, we can use that as the reason
        # they're blocked.
        if "fee_limit" in info:
            fee_limit = MoneyUtility.parse(info["fee_limit"])
            if fee_limit and patrondata.fines > fee_limit:
                block_reason = PatronData.EXCESSIVE_FINES

        return block_reason

    @classmethod
    def parse_date(cls, value):
        """Try to parse `value` using any of several common date formats."""
        date_value = None
        for format in cls.DATE_FORMATS:
            try:
                date_value = datetime.strptime(value, format)
                break
            except ValueError as e:
                continue
        return date_value
