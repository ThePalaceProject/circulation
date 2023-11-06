import json
from datetime import datetime
from typing import Callable, Optional, Type, Union

from pydantic import Field, PositiveInt

from api.authentication.base import PatronData
from api.authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)
from api.sip.client import Sip2Encoding, SIPClient
from api.sip.dialect import Dialect as Sip2Dialect
from core.analytics import Analytics
from core.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.model import Patron
from core.util import MoneyUtility
from core.util.http import RemoteIntegrationException


class SIP2Settings(BasicAuthProviderSettings):
    """Settings for SIP2 authentication providers."""

    # Hostname of the SIP server
    url: str = FormField(..., form=ConfigurationFormItem(label="Server"))
    # The port number to connect to on the SIP server.
    port: PositiveInt = FormField(
        6001,
        form=ConfigurationFormItem(
            label="Port",
            required=True,
        ),
    )
    # SIP field CN; the user ID to use when initiating a SIP session, if necessary.
    # This is _not_ a patron identifier (SIP field AA); it identifies the SC
    # creating the SIP session. SIP2 defines SC as "...any library automation
    # device dealing with patrons or library materials."
    username: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Login User ID",
        ),
    )
    # Sip field CO; the password to use when initiating a SIP session, if necessary.
    password: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Login Password",
        ),
    )
    # SIP field CP; the location code to use when initiating a SIP session. A
    # location code supposedly refers to the physical location of a self-checkout
    # machine within a library system. Some libraries require a special location
    # code to be provided when authenticating patrons; others may require the
    # circulation manager to be treated as its own special 'location'.
    location_code: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Location Code",
        ),
        alias="location code",
    )
    encoding: Sip2Encoding = FormField(
        Sip2Encoding.cp850,
        form=ConfigurationFormItem(
            label="Data Encoding",
            type=ConfigurationFormItemType.SELECT,
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
    )
    use_ssl: bool = FormField(
        False,
        form=ConfigurationFormItem(
            label="Connect over SSL?",
            type=ConfigurationFormItemType.SELECT,
            options={
                "true": "Connect to the SIP2 server over SSL",
                "false": "Connect to the SIP2 server over an ordinary socket connection",
            },
            required=True,
        ),
    )
    ssl_verification: bool = FormField(
        True,
        form=ConfigurationFormItem(
            label="Perform SSL certificate verification.",
            type=ConfigurationFormItemType.SELECT,
            options={
                "true": "Perform SSL certificate verification.",
                "false": "Do not perform SSL certificate verification.",
            },
            description=(
                "Strict certificate verification may be optionally turned off for "
                "hosts that have misconfigured or untrusted certificates."
            ),
        ),
    )
    ils: Sip2Dialect = FormField(
        Sip2Dialect.GENERIC_ILS,
        form=ConfigurationFormItem(
            label="ILS",
            description=(
                "Some ILS require specific SIP2 settings. If the ILS you are using "
                "is in the list please pick it otherwise select 'Generic ILS'."
            ),
            type=ConfigurationFormItemType.SELECT,
            options={
                Sip2Dialect.GENERIC_ILS: "Generic ILS",
                Sip2Dialect.AG_VERSO: "Auto-Graphics VERSO",
                Sip2Dialect.FOLIO: "Folio",
            },
            required=True,
        ),
    )
    ssl_certificate: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="SSL Certificate",
            description=(
                "The SSL certificate used to securely connect to an SSL-enabled SIP2 "
                "server. Not all SSL-enabled SIP2 servers require a custom "
                "certificate, but some do. This should be a string beginning with "
                "<code>-----BEGIN CERTIFICATE-----</code> and ending with "
                "<code>-----END CERTIFICATE-----</code>"
            ),
            type=ConfigurationFormItemType.TEXTAREA,
        ),
    )
    ssl_key: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="SSL Key",
            description=(
                "The private key, if any, used to sign the SSL certificate above. "
                "If present, this should be a string beginning with "
                "<code>-----BEGIN PRIVATE KEY-----</code> and ending with "
                "<code>-----END PRIVATE KEY-----</code>"
            ),
            type=ConfigurationFormItemType.TEXTAREA,
        ),
    )
    # The field delimiter (see "Variable-length fields" in the SIP2 spec). If no
    # value is specified, the default (the pipe character) will be used.
    field_separator: str = FormField(
        "|",
        form=ConfigurationFormItem(
            label="Field Seperator",
            required=True,
        ),
        alias="field seperator",
    )
    patron_status_block: bool = FormField(
        True,
        form=ConfigurationFormItem(
            label="SIP2 Patron Status Block",
            description=(
                "Block patrons from borrowing based on the status of the SIP2 <em>patron status</em> field."
            ),
            type=ConfigurationFormItemType.SELECT,
            options={
                "true": "Block based on patron status field",
                "false": "No blocks based on patron status field",
            },
        ),
        alias="patron status block",
    )


class SIP2LibrarySettings(BasicAuthProviderLibrarySettings):
    # Used as the SIP2 AO field.
    institution_id: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Institution ID",
            description="A specific identifier for the library or branch, if used in patron authentication",
        ),
    )


class SIP2AuthenticationProvider(
    BasicAuthenticationProvider[SIP2Settings, SIP2LibrarySettings]
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
        analytics: Optional[Analytics] = None,
        client: Optional[Callable[..., SIPClient]] = None,
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
        )

    @classmethod
    def label(cls) -> str:
        return "SIP2"

    @classmethod
    def description(cls) -> str:
        return "SIP2 Patron Authentication"

    @classmethod
    def settings_class(cls) -> Type[SIP2Settings]:
        return SIP2Settings

    @classmethod
    def library_settings_class(cls) -> Type[SIP2LibrarySettings]:
        return SIP2LibrarySettings

    def patron_information(self, username, password):
        try:
            sip = self.client
            sip.connect()
            sip.login()
            info = sip.patron_information(username, password)
            sip.end_session(username, password)
            sip.disconnect()
            return info

        except OSError as e:
            raise RemoteIntegrationException(self.server or "unknown server", str(e))

    def remote_patron_lookup(
        self, patron_or_patrondata: Union[PatronData, Patron]
    ) -> Optional[PatronData]:
        info = self.patron_information(
            patron_or_patrondata.authorization_identifier, None
        )
        return self.info_to_patrondata(info, False)

    def remote_authenticate(
        self, username: Optional[str], password: Optional[str]
    ) -> Optional[PatronData]:
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

        # Log in was successful so test patron's test credentials
        if login.success:
            results = [
                r for r in super(SIP2AuthenticationProvider, self)._run_self_tests(_db)
            ]
            yield from results

            if results[0].success:

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

    def info_to_patrondata(self, info, validate_password=True) -> Optional[PatronData]:
        """Convert the SIP-specific dictionary obtained from
        SIPClient.patron_information() to an abstract,
        authenticator-independent PatronData object.
        """
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
        if "fee_amount" in info:
            fines = info["fee_amount"]
        else:
            fines = "0"
        patrondata.fines = MoneyUtility.parse(fines)
        if "sipserver_patron_class" in info:
            patrondata.external_type = info["sipserver_patron_class"]
        for expire_field in [
            "sipserver_patron_expiration",
            "polaris_patron_expiration",
        ]:
            if expire_field in info:
                value = info.get(expire_field)
                value = self.parse_date(value)
                if value:
                    patrondata.authorization_expires = value
                    break

        if self.patron_status_should_block:
            patrondata.block_reason = self.info_to_patrondata_block_reason(
                info, patrondata
            )
        else:
            patrondata.block_reason = PatronData.NO_VALUE

        return patrondata

    def info_to_patrondata_block_reason(
        self, info, patrondata: PatronData
    ) -> Union[PatronData.NoValue, str]:
        # A True value in most (but not all) subfields of the
        # patron_status field will prohibit the patron from borrowing
        # books.
        status = info["patron_status_parsed"]
        block_reason: Union[str, PatronData.NoValue] = PatronData.NO_VALUE
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
            fee_limit = MoneyUtility.parse(info["fee_limit"]).amount
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
