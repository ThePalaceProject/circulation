import json
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from functools import partial
from typing import cast

import pytest

from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authentication.basic import (
    BasicAuthProviderLibrarySettings,
    Keyboards,
    LibraryIdenfitierRestrictionField,
    LibraryIdentifierRestriction,
)
from palace.manager.api.problem_details import (
    INVALID_CREDENTIALS,
    PATRON_OF_ANOTHER_LIBRARY,
)
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.integration.patron_auth.sip2.client import Sip2Encoding
from palace.manager.integration.patron_auth.sip2.dialect import Dialect
from palace.manager.integration.patron_auth.sip2.provider import (
    SIP2AuthenticationProvider,
    SIP2LibrarySettings,
    SIP2Settings,
)
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.sip import MockSIPClient


@pytest.fixture
def mock_sip_client() -> Callable[..., MockSIPClient]:
    sip_client = None

    def mock(client=MockSIPClient, **kwargs):
        nonlocal sip_client
        if sip_client is None:
            sip_client = client(**kwargs)
        return sip_client

    return mock


@pytest.fixture
def mock_library_id() -> int:
    return 20


@pytest.fixture
def mock_integration_id() -> int:
    return 20


@pytest.fixture
def create_library_settings() -> Callable[..., SIP2LibrarySettings]:
    return partial(SIP2LibrarySettings, institution_id="institution_id")


@pytest.fixture
def create_settings() -> Callable[..., SIP2Settings]:
    """
    Return a function that creates a SIP2Settings object.
    With all the mandatory parameters set to default values.
    """
    return partial(SIP2Settings, url="server.com", test_identifier="test")


@pytest.fixture
def create_provider(
    mock_library_id: int,
    mock_integration_id: int,
    create_settings: Callable[..., SIP2Settings],
    create_library_settings: Callable[..., SIP2LibrarySettings],
    mock_sip_client: Callable[..., MockSIPClient],
) -> Callable[..., SIP2AuthenticationProvider]:
    """
    Return a function that creates a SIP2AuthenticationProvider object.

    The function takes the same parameters as the SIP2AuthenticationProvider,
    but gives default values for all the required arguments.
    """
    return partial(
        SIP2AuthenticationProvider,
        library_id=mock_library_id,
        integration_id=mock_integration_id,
        settings=create_settings(),
        library_settings=create_library_settings(),
        client=mock_sip_client,
    )


class TestSIP2AuthenticationProvider:
    # We feed sample data into the MockSIPClient, even though it adds
    # an extra step of indirection, because it lets us use as a
    # starting point the actual (albeit redacted) SIP2 messages we
    # receive from servers.

    sierra_valid_login_unicode = "64              000201610210000142637000000000000000000000000AOnypl |AA12345|AELE CARRÉ, JOHN|BZ0030|CA0050|CB0050|BLY|CQY|BV0|CC15.00|BEfoo@example.com|AY1AZD1B7"
    sierra_valid_login = sierra_valid_login_unicode.encode("cp850")
    sierra_valid_login_utf8 = sierra_valid_login_unicode.encode("utf-8")
    sierra_excessive_fines = b"64              000201610210000142637000000000000000000000000AOnypl |AA12345|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQY|BV20.00|CC15.00|BEfoo@example.com|AY1AZD1B7"
    sierra_invalid_login = b"64Y  YYYYYYYYYYY000201610210000142725000000000000000000000000AOnypl |AA12345|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQN|BV0|CC15.00|BEfoo@example.com|AFInvalid PIN entered.  Please try again or see a staff member for assistance.|AFThere are unresolved issues with your account.  Please see a staff member for assistance.|AY1AZ91A8"

    evergreen_active_user = b"64  Y           00020161021    142851000000000000000000000000AA12345|AEBooth Active Test|BHUSD|BDAdult Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863715|AOBiblioTest|AY2AZ0000"
    evergreen_expired_card = b"64YYYY          00020161021    142937000000000000000000000000AA12345|AEBooth Expired Test|BHUSD|BDAdult Circ Desk #2 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20080907|PCAdult|PIAllowed|XI863716|AFblocked|AOBiblioTest|AY2AZ0000"
    evergreen_excessive_fines = b"64  Y           00020161021    143002000000000000000100000000AA12345|AEBooth Excessive Fines Test|BHUSD|BV100.00|BDChildrens Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863718|AOBiblioTest|AY2AZ0000"
    evergreen_hold_privileges_denied = b"64   Y          00020161021    143002000000000000000100000000AA12345|AEBooth Excessive Fines Test|BHUSD|BV100.00|BDChildrens Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863718|AOBiblioTest|AY2AZ0000"
    evergreen_card_reported_lost = b"64    Y        00020161021    143002000000000000000100000000AA12345|AEBooth Excessive Fines Test|BHUSD|BV100.00|BDChildrens Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863718|AOBiblioTest|AY2AZ0000"
    evergreen_inactive_account = b"64YYYY          00020161021    143028000000000000000000000000AE|AA12345|BLN|AOBiblioTest|AY2AZ0000"
    evergreen_patron_with_location = b"64  Y           00020161021    151441000000000000000000000000AOgapines|AAuser|AEPatron Name|BHUSD|BDCirc Desk, Anytown, Anystate USA 00000|AQTestLoc|BLY|PA20250520|PB19640101|PCDigital Only|PIFiltered|XI5784348|AY2AZ0000"
    evergreen_patron_wo_location = b"64  Y           00020161021    151441000000000000000000000000AOgapines|AAuser|AEPatron Name|BHUSD|BDCirc Desk, Anytown, Anystate USA 00000|BLY|PA20250520|PB19640101|PCDigital Only|PIFiltered|XI5784348|AY2AZ0000"
    evergreen_patron_with_wrong_loc = b"64  Y           00020161021    151441000000000000000000000000AOgapines|AAuser|AEPatron Name|BHUSD|BDCirc Desk, Anytown, Anystate USA 00000|AQOtherLoc|BLY|PA20250520|PB19640101|PCDigital Only|PIFiltered|XI5784348|AY2AZ0000"

    polaris_valid_pin = b"64              00120161121    143327000000000000000000000000AO3|AA25891000331441|AEFalk, Jen|BZ0050|CA0075|CB0075|BLY|CQY|BHUSD|BV9.25|CC9.99|BD123 Charlotte Hall, MD 20622|BEfoo@bar.com|BF501-555-1212|BC19710101    000000|PA1|PEHALL|PSSt. Mary's|U1|U2|U3|U4|U5|PZ20622|PX20180609    235959|PYN|FA0.00|AFPatron status is ok.|AGPatron status is ok.|AY2AZ94F3"

    polaris_wrong_pin = b"64YYYY          00120161121    143157000000000000000000000000AO3|AA25891000331441|AEFalk, Jen|BZ0050|CA0075|CB0075|BLY|CQN|BHUSD|BV9.25|CC9.99|BD123 Charlotte Hall, MD 20622|BEfoo@bar.com|BF501-555-1212|BC19710101    000000|PA1|PEHALL|PSSt. Mary's|U1|U2|U3|U4|U5|PZ20622|PX20180609    235959|PYN|FA0.00|AFInvalid patron password. Passwords do not match.|AGInvalid patron password.|AY2AZ87B4"

    polaris_expired_card = b"64YYYY          00120161121    143430000000000000000000000000AO3|AA25891000224613|AETester, Tess|BZ0050|CA0075|CB0075|BLY|CQY|BHUSD|BV0.00|CC9.99|BD|BEfoo@bar.com|BF|BC19710101    000000|PA1|PELEON|PSSt. Mary's|U1|U2|U3|U4|U5|PZ|PX20161025    235959|PYY|FA0.00|AFPatron has blocks.|AGPatron has blocks.|AY2AZA4F8"

    polaris_excess_fines = b"64YYYY      Y   00120161121    144438000000000000000000000000AO3|AA25891000115879|AEFalk, Jen|BZ0050|CA0075|CB0075|BLY|CQY|BHUSD|BV11.50|CC9.99|BD123, Charlotte Hall, MD 20622|BE|BF501-555-1212|BC20140610    000000|PA1|PEHALL|PS|U1No|U2|U3|U4|U5|PZ20622|PX20170424    235959|PYN|FA0.00|AFPatron has blocks.|AGPatron has blocks.|AY2AZA27B"

    polaris_no_such_patron = b"64YYYY          00120161121    143126000000000000000000000000AO3|AA1112|AE, |BZ0000|CA0000|CB0000|BLN|CQN|BHUSD|BV0.00|CC0.00|BD|BE|BF|BC|PA0|PE|PS|U1|U2|U3|U4|U5|PZ|PX|PYN|FA0.00|AFPatron does not exist.|AGPatron does not exist.|AY2AZBCF2"

    tlc_no_such_patron = b"64YYYY          00020171031    092000000000000000000000000000AOhq|AA2642|AE|BLN|AF#Unknown borrower barcode - please refer to the circulation desk.|AY1AZD46E"

    end_session_response = b"36Y201610210000142637AO3|AA25891000331441|AF|AG"

    def test_initialize_from_settings(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
        create_library_settings: Callable[..., BasicAuthProviderLibrarySettings],
    ) -> None:
        settings = create_settings(
            url="server.com",
            username="user1",
            password="pass1",
            field_separator=">",
            port=1234,
            ils=Dialect.AG_VERSO,
            encoding=Sip2Encoding.utf8,
            patron_status_block=False,
            timeout=9,
        )
        library_settings = create_library_settings(institution_id="MAIN")
        provider = create_provider(settings=settings, library_settings=library_settings)

        # A SIP2AuthenticationProvider was initialized based on the
        # integration values.
        assert "user1" == provider.login_user_id
        assert "pass1" == provider.login_password
        assert ">" == provider.field_separator
        assert "MAIN" == provider.institution_id
        assert "server.com" == provider.server
        assert 1234 == provider.port
        assert Dialect.AG_VERSO == provider.dialect
        assert Sip2Encoding.utf8.value == provider.encoding
        assert provider.patron_status_should_block is False
        assert 9 == provider.timeout

        # And it's possible to get a SIP2Client that's configured
        # based on the same values.
        client = provider.client
        assert "user1" == client.login_user_id
        assert "pass1" == client.login_password
        assert ">" == client.separator
        assert "MAIN" == client.institution_id
        assert "server.com" == client.target_server
        assert 1234 == client.target_port
        assert 9 == client.timeout

    def test_initialize_from_settings_defaults(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
        create_library_settings: Callable[..., BasicAuthProviderLibrarySettings],
    ) -> None:
        provider = create_provider()

        # Test that we get the default values
        assert provider.port == 6001
        assert provider.field_separator == "|"
        assert provider.dialect == Dialect.GENERIC_ILS
        assert provider.encoding == Sip2Encoding.cp850.value
        assert provider.patron_status_should_block is True

    def test_timeout_constraints(
        self,
        create_settings: Callable[..., SIP2Settings],
    ) -> None:
        """Test that timeout enforces ge=1, le=9 constraints."""
        # Valid values should work
        settings = create_settings(timeout=1)
        assert settings.timeout == 1

        settings = create_settings(timeout=5)
        assert settings.timeout == 5

        settings = create_settings(timeout=9)
        assert settings.timeout == 9

        # Invalid: below minimum (ge=1)
        with pytest.raises(ProblemDetailException) as excinfo:
            create_settings(timeout=0)
        assert excinfo.value.problem_detail.detail is not None
        assert "greater than or equal to 1" in excinfo.value.problem_detail.detail

        # Invalid: above maximum (le=9)
        with pytest.raises(ProblemDetailException) as excinfo:
            create_settings(timeout=10)
        assert excinfo.value.problem_detail.detail is not None
        assert "less than or equal to 9" in excinfo.value.problem_detail.detail

    def test_remote_authenticate(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
    ):
        provider = create_provider()
        client = cast(MockSIPClient, provider.client)

        # Some examples taken from a Sierra SIP API.
        client.queue_response(self.sierra_valid_login)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        assert "12345" == patrondata.authorization_identifier
        assert "foo@example.com" == patrondata.email_address
        assert "LE CARRÉ, JOHN" == patrondata.personal_name
        assert 0 == patrondata.fines
        assert PatronData.NO_VALUE == patrondata.authorization_expires
        assert patrondata.external_type is None
        assert PatronData.NO_VALUE == patrondata.block_reason

        client.queue_response(self.sierra_invalid_login)
        client.queue_response(self.end_session_response)
        assert provider.remote_authenticate("user", "pass") is None

        # Since Sierra provides both the patron's fine amount and the
        # maximum allowable amount, we can determine just by looking
        # at the SIP message that this patron is blocked for excessive
        # fines.
        client.queue_response(self.sierra_excessive_fines)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        assert PatronData.EXCESSIVE_FINES == patrondata.block_reason

        # A patron with an expired card.
        client.queue_response(self.evergreen_expired_card)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        assert "12345" == patrondata.authorization_identifier
        # SIP extension field XI becomes sipserver_internal_id which
        # becomes PatronData.permanent_id.
        assert "863716" == patrondata.permanent_id
        assert "Booth Expired Test" == patrondata.personal_name
        assert 0 == patrondata.fines
        assert datetime(2008, 9, 7) == patrondata.authorization_expires
        assert PatronData.NO_BORROWING_PRIVILEGES == patrondata.block_reason

        # A patron with excessive fines
        client.queue_response(self.evergreen_excessive_fines)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        assert "12345" == patrondata.authorization_identifier
        assert "863718" == patrondata.permanent_id
        assert "Booth Excessive Fines Test" == patrondata.personal_name
        assert 100 == patrondata.fines
        assert datetime(2019, 10, 4) == patrondata.authorization_expires

        # We happen to know that this patron can't borrow books due to
        # excessive fines, but that information doesn't show up as a
        # block, because Evergreen doesn't also provide the
        # fine limit. This isn't a big deal -- we'll pick it up later
        # when we apply the site policy.
        #
        # This patron also has "Recall privileges denied" set, but
        # that's not a reason to block them.
        assert PatronData.NO_VALUE == patrondata.block_reason

        # "Hold privileges denied" is not a block because you can
        # still borrow books.
        client.queue_response(self.evergreen_hold_privileges_denied)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        assert PatronData.NO_VALUE == patrondata.block_reason

        client.queue_response(self.evergreen_card_reported_lost)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        assert PatronData.CARD_REPORTED_LOST == patrondata.block_reason

        # Some examples taken from a Polaris instance.
        client.queue_response(self.polaris_valid_pin)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        assert "25891000331441" == patrondata.authorization_identifier
        assert "foo@bar.com" == patrondata.email_address
        assert 9.25 == patrondata.fines
        assert "Falk, Jen" == patrondata.personal_name
        assert datetime(2018, 6, 9, 23, 59, 59) == patrondata.authorization_expires

        client.queue_response(self.polaris_wrong_pin)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert patrondata is None

        client.queue_response(self.polaris_expired_card)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        assert datetime(2016, 10, 25, 23, 59, 59) == patrondata.authorization_expires

        client.queue_response(self.polaris_excess_fines)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        assert 11.50 == patrondata.fines

        # Two cases where the patron's authorization identifier was
        # just not recognized. One on an ILS that sets
        # valid_patron_password='N' when that happens.
        client.queue_response(self.polaris_no_such_patron)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert patrondata is None

        # And once on an ILS that leaves valid_patron_password blank
        # when that happens.
        client.queue_response(self.tlc_no_such_patron)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert patrondata is None

    def test_remote_authenticate_no_password(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
    ):
        settings = create_settings(
            password_keyboard=Keyboards.NULL,
        )
        provider = create_provider(settings=settings)
        client = cast(MockSIPClient, provider.client)
        # This Evergreen instance doesn't use passwords.
        client.queue_response(self.evergreen_active_user)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", None)
        assert isinstance(patrondata, PatronData)
        assert "12345" == patrondata.authorization_identifier
        assert "863715" == patrondata.permanent_id
        assert "Booth Active Test" == patrondata.personal_name
        assert 0 == patrondata.fines
        assert datetime(2019, 10, 4) == patrondata.authorization_expires
        assert "Adult" == patrondata.external_type

        # If a password is specified, it is not sent over the wire.
        client.queue_response(self.evergreen_active_user)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user2", "some password")
        assert isinstance(patrondata, PatronData)
        assert "12345" == patrondata.authorization_identifier
        request = client.requests[-1]
        assert b"user2" in request
        assert b"some password" not in request

    def test_remote_authenticate_location_restriction(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_library_settings: Callable[..., SIP2Settings],
    ):
        # This patron authentication library instance is configured with "TestLoc".
        library_restriction = "TestLoc"
        library_settings = create_library_settings(
            library_identifier_restriction_type=LibraryIdentifierRestriction.STRING,
            library_identifier_field=LibraryIdenfitierRestrictionField.PATRON_LIBRARY.value,
            library_identifier_restriction_criteria=library_restriction,
        )
        provider = create_provider(library_settings=library_settings)
        client = cast(MockSIPClient, provider.client)

        # This patron has the CORRECT location.
        client.queue_response(self.evergreen_patron_with_location)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        patrondata = provider.enforce_library_identifier_restriction(patrondata)
        assert isinstance(patrondata, PatronData)
        assert "Patron Name" == patrondata.personal_name

        # This patron does NOT have an associated location.
        client.queue_response(self.evergreen_patron_wo_location)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        with pytest.raises(ProblemDetailException) as exc:
            provider.enforce_library_identifier_restriction(patrondata)
        assert exc.value.problem_detail == PATRON_OF_ANOTHER_LIBRARY.with_debug(
            "'patron location' does not match library restriction: No value in field."
        )

        # This patron has the WRONG location.
        client.queue_response(self.evergreen_patron_with_wrong_loc)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")
        assert isinstance(patrondata, PatronData)
        with pytest.raises(ProblemDetailException) as exc:
            provider.enforce_library_identifier_restriction(patrondata)
        assert exc.value.problem_detail == PATRON_OF_ANOTHER_LIBRARY.with_debug(
            "'patron location' does not match library restriction: 'OtherLoc' does not exactly match 'TestLoc'."
        )

    def test_encoding(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
    ):
        # It's possible to specify an encoding other than CP850
        # for communication with the SIP2 server.
        #
        # Here, we'll try it with UTF-8.
        settings = create_settings(
            encoding=Sip2Encoding.utf8,
        )
        provider = create_provider(settings=settings)
        client = cast(MockSIPClient, provider.client)

        # Queue the UTF-8 version of the patron information
        # as opposed to the CP850 version.
        client.queue_response(self.sierra_valid_login_utf8)
        client.queue_response(self.end_session_response)
        patrondata = provider.remote_authenticate("user", "pass")

        # We're able to parse the message from the server and parse
        # out patron data, including the É character, with the proper
        # encoding.
        assert isinstance(patrondata, PatronData)
        assert "12345" == patrondata.authorization_identifier
        assert "foo@example.com" == patrondata.email_address
        assert "LE CARRÉ, JOHN" == patrondata.personal_name
        assert 0 == patrondata.fines
        assert PatronData.NO_VALUE == patrondata.authorization_expires
        assert patrondata.external_type is None
        assert PatronData.NO_VALUE == patrondata.block_reason

    def test_ioerror_during_connect_becomes_problemdetail(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
    ):
        """If the IP of the circulation manager has not been whitelisted,
        we generally can't even connect to the server.
        """

        class CannotConnect(MockSIPClient):
            def connect(self):
                raise OSError("Doom!")

        settings = create_settings(
            url="unknown server",
        )
        provider = create_provider(client=CannotConnect, settings=settings)

        response = provider.remote_authenticate(
            "username",
            "password",
        )

        assert isinstance(response, ProblemDetail)
        assert response.status_code == INVALID_CREDENTIALS.status_code
        assert response.uri == INVALID_CREDENTIALS.uri
        assert (
            response.detail
            == "Error contacting authentication server (unknown server). Please try again later."
        )

    def test_ioerror_during_send_becomes_problemdetail(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
    ):
        """If there's an IOError communicating with the server,
        it becomes a ProblemDetail to be sent to the client.
        """

        class CannotSend(MockSIPClient):
            def do_send(self, data):
                raise OSError("Doom!")

        settings = create_settings(
            url="server.local",
        )
        provider = create_provider(client=CannotSend, settings=settings)

        response = provider.remote_authenticate(
            "username",
            "password",
        )

        assert isinstance(response, ProblemDetail)
        assert response.status_code == INVALID_CREDENTIALS.status_code
        assert response.uri == INVALID_CREDENTIALS.uri
        assert (
            response.detail
            == "Error contacting authentication server (server.local). Please try again later."
        )

    def test_parse_date(self):
        parse = SIP2AuthenticationProvider.parse_date
        assert datetime(2011, 1, 2) == parse("20110102")
        assert datetime(2011, 1, 2, 10, 20, 30) == parse("20110102    102030")
        assert datetime(2011, 1, 2, 10, 20, 30) == parse("20110102UTC102030")

    def test_remote_patron_lookup(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
        mock_sip_client: Callable[..., MockSIPClient],
    ):
        # When the SIP authentication provider needs to look up a patron,
        # it calls patron_information on its SIP client and passes in None
        # for the password.
        patron = PatronData()
        patron.authorization_identifier = "1234"

        class Mock(MockSIPClient):
            def patron_information(self, identifier, password):
                self.patron_information = identifier
                self.password = password
                return self.patron_information_parser(
                    TestSIP2AuthenticationProvider.polaris_wrong_pin
                )

        mock = partial(mock_sip_client, client=Mock)
        provider = create_provider(client=mock)
        client = cast(Mock, provider.client)
        client.queue_response(self.end_session_response)

        patron_data = provider.remote_patron_lookup(patron)
        assert patron_data is not None
        assert isinstance(patron_data, PatronData)
        assert "25891000331441" == patron_data.authorization_identifier
        assert "foo@bar.com" == patron_data.email_address
        assert 9.25 == patron_data.fines
        assert "Falk, Jen" == patron_data.personal_name
        assert datetime(2018, 6, 9, 23, 59, 59) == patron_data.authorization_expires
        assert client.patron_information == "1234"
        assert client.password is None

    def test_info_to_patrondata_problemdetail(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
    ):
        # If we get a ProblemDetail we just return it.
        settings = create_settings(
            url="server.local",
        )
        provider = create_provider(settings=settings)
        problem_detail = ProblemDetail("foo")
        patron = provider.info_to_patrondata(problem_detail)
        assert patron is problem_detail

    def test_info_to_patrondata_validate_password(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
    ):
        settings = create_settings(
            url="server.local",
        )
        provider = create_provider(settings=settings)
        client = cast(MockSIPClient, provider.client)

        # Test with valid login, should return PatronData
        info = client.patron_information_parser(
            TestSIP2AuthenticationProvider.sierra_valid_login
        )
        patron = provider.info_to_patrondata(info)
        assert isinstance(patron, PatronData)
        assert "12345" == patron.authorization_identifier
        assert "foo@example.com" == patron.email_address
        assert "LE CARRÉ, JOHN" == patron.personal_name
        assert 0 == patron.fines
        assert PatronData.NO_VALUE == patron.authorization_expires
        assert patron.external_type is None
        assert PatronData.NO_VALUE == patron.block_reason

        # Test with invalid login, should return None
        info = client.patron_information_parser(
            TestSIP2AuthenticationProvider.sierra_invalid_login
        )
        patron = provider.info_to_patrondata(info)
        assert patron is None

    def test_info_to_patrondata_no_validate_password(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
    ):
        settings = create_settings(
            url="server.local",
        )
        provider = create_provider(settings=settings)
        client = cast(MockSIPClient, provider.client)

        # Test with valid login, should return PatronData
        info = client.patron_information_parser(
            TestSIP2AuthenticationProvider.sierra_valid_login
        )
        patron = provider.info_to_patrondata(info, validate_password=False)
        assert isinstance(patron, PatronData)
        assert "12345" == patron.authorization_identifier
        assert "foo@example.com" == patron.email_address
        assert "LE CARRÉ, JOHN" == patron.personal_name
        assert 0 == patron.fines
        assert PatronData.NO_VALUE == patron.authorization_expires
        assert patron.external_type is None
        assert PatronData.NO_VALUE == patron.block_reason

        # Test with invalid login, should return PatronData
        info = client.patron_information_parser(
            TestSIP2AuthenticationProvider.sierra_invalid_login
        )
        patron = provider.info_to_patrondata(info, validate_password=False)
        assert isinstance(patron, PatronData)
        assert "12345" == patron.authorization_identifier
        assert "foo@example.com" == patron.email_address
        assert "SHELDON, ALICE" == patron.personal_name
        assert 0 == patron.fines
        assert PatronData.NO_VALUE == patron.authorization_expires
        assert patron.external_type is None
        assert "no borrowing privileges" == patron.block_reason

    @pytest.mark.parametrize(
        "patron_status_block, expected_block_reason",
        [
            (True, PatronData.NO_BORROWING_PRIVILEGES),
            (False, PatronData.NO_VALUE),
        ],
    )
    def test_patron_block_setting(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
        patron_status_block: bool,
        expected_block_reason: str,
    ):
        settings = create_settings(
            patron_status_block=patron_status_block,
        )
        provider = create_provider(settings=settings)
        client = cast(MockSIPClient, provider.client)

        info = client.patron_information_parser(
            TestSIP2AuthenticationProvider.evergreen_expired_card
        )
        patron = provider.info_to_patrondata(info)
        assert isinstance(patron, PatronData)
        assert "12345" == patron.authorization_identifier
        assert "863716" == patron.permanent_id
        assert "Booth Expired Test" == patron.personal_name
        assert 0 == patron.fines
        assert datetime(2008, 9, 7) == patron.authorization_expires
        assert expected_block_reason == patron.block_reason

    @pytest.mark.parametrize(
        "status_block,expected_block_reason",
        [
            (True, PatronData.EXCESSIVE_FINES),
            (False, PatronData.NO_VALUE),
        ],
    )
    def test_patron_block_setting_with_fines(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
        status_block: bool,
        expected_block_reason: str,
    ):
        # Test with blocked patron, block should be set
        settings = create_settings(
            patron_status_block=status_block,
        )
        provider = create_provider(settings=settings)
        client = cast(MockSIPClient, provider.client)

        info = client.patron_information_parser(
            TestSIP2AuthenticationProvider.evergreen_excessive_fines
        )
        info["fee_limit"] = "10.0"
        patron = provider.info_to_patrondata(info)
        assert isinstance(patron, PatronData)
        assert "12345" == patron.authorization_identifier
        assert "863718" == patron.permanent_id
        assert "Booth Excessive Fines Test" == patron.personal_name
        assert Decimal("100.0") == patron.fines
        assert datetime(2019, 10, 4) == patron.authorization_expires
        assert expected_block_reason == patron.block_reason

    def test_run_self_tests(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
        create_settings: Callable[..., SIP2Settings],
        db: DatabaseTransactionFixture,
    ):
        settings = create_settings(
            url="server.com",
        )

        class MockBadConnection(MockSIPClient):
            def connect(self):
                # probably a timeout if the server or port values are not valid
                raise OSError("Could not connect")

        class MockSIPLogin(MockSIPClient):
            def now(self):
                return datetime(2019, 1, 1).strftime("%Y%m%d0000%H%M%S")

            def login(self):
                if not self.login_user_id and not self.login_password:
                    raise OSError("Error logging in")

            def sc_status(self):
                return {"_status": "98", "online_status": "Y"}

            def patron_information(self, username, password):
                return self.patron_information_parser(
                    TestSIP2AuthenticationProvider.sierra_valid_login
                )

        provider = create_provider(settings=settings, client=MockBadConnection)
        results = [r for r in provider._run_self_tests(db.session)]

        # If the connection doesn't work then don't bother running the other tests
        assert len(results) == 1
        assert results[0].name == "Test Connection"
        assert results[0].success == False
        assert isinstance(results[0].exception, IOError)
        assert results[0].exception.args == ("Could not connect",)

        provider = create_provider(settings=settings, client=MockSIPLogin)
        results = [x for x in provider._run_self_tests(db.session)]

        assert len(results) == 3
        assert results[0].name == "Test Connection"
        assert results[0].success == True

        assert results[1].name == "Test Login with username 'None' and password 'None'"
        assert results[1].success == False
        assert isinstance(results[1].exception, IOError)
        assert results[1].exception.args == ("Error logging in",)

        assert results[2].name == "ILS SIP Service Info (SC Status)"
        assert results[2].success == True
        assert results[2].result == json.dumps(
            {"_status": "98", "online_status": "Y"}, indent=2
        )

        # Set the log in username and password
        settings = create_settings(
            username="user1",
            password="pass1",
            test_identifier="",
            test_password=None,
        )
        provider = create_provider(settings=settings, client=MockSIPLogin)
        results = [x for x in provider._run_self_tests(db.session)]

        assert len(results) == 4
        assert results[0].name == "Test Connection"
        assert results[0].success == True

        assert (
            results[1].name == "Test Login with username 'user1' and password 'pass1'"
        )
        assert results[1].success == True

        assert results[2].name == "ILS SIP Service Info (SC Status)"
        assert results[2].success == True
        assert results[2].result == json.dumps(
            {"_status": "98", "online_status": "Y"}, indent=2
        )

        assert results[3].name == "Authenticating test patron"
        assert results[3].success == False
        assert isinstance(results[3].exception, CannotLoadConfiguration)
        assert results[3].exception.args == (
            "No test patron identifier is configured.",
        )

        # Now add the test patron credentials into the mocked client and SIP2 authenticator provider
        settings = create_settings(
            username="user1",
            password="pass1",
            # The actual test patron credentials
            test_identifier="usertest1",
            test_password="userpassword1",
            # Set verso ILS, since we don't need to send end session message
            ils=Dialect.AG_VERSO,
        )
        library_id = db.default_library().id
        provider = create_provider(
            library_id=library_id, settings=settings, client=MockSIPLogin
        )
        client = cast(MockSIPClient, provider.client)
        results = [x for x in provider._run_self_tests(db.session)]

        assert len(results) == 7
        assert results[0].name == "Test Connection"
        assert results[0].success == True

        assert (
            results[1].name == "Test Login with username 'user1' and password 'pass1'"
        )
        assert results[1].success == True

        assert results[2].name == "ILS SIP Service Info (SC Status)"
        assert results[2].success == True
        assert results[2].result == json.dumps(
            {"_status": "98", "online_status": "Y"}, indent=2
        )

        assert results[3].name == "Patron information request"
        assert results[3].success == True
        assert results[3].result == provider.client.patron_information_request(
            "usertest1", "userpassword1"
        )

        assert results[4].name == "Raw test patron information"
        assert results[4].success == True
        assert results[4].result == json.dumps(
            client.patron_information_parser(
                TestSIP2AuthenticationProvider.sierra_valid_login
            ),
            indent=1,
        )

        assert results[5].name == "Authenticating test patron"
        assert results[5].success == True

        # Since test patron authentication is true, we can now see self
        # test results for syncing metadata and the raw data from `patron_information`
        assert results[6].name == "Syncing patron metadata"
        assert results[6].success == True

    def test_patron_debug_success(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
    ):
        """A successful patron_debug run returns all diagnostic steps in order."""
        provider = create_provider()
        client = cast(MockSIPClient, provider.client)

        client.queue_response(self.sierra_valid_login)
        client.queue_response(self.end_session_response)

        results = provider.patron_debug("user", "pass")

        assert len(results) == 9

        assert results[0].label == "Server-Side Validation"
        assert results[0].success is True

        assert results[1].label == "SIP2 Connection"
        assert results[1].success is True
        assert isinstance(results[1].details, str)
        assert "server.com:6001" == results[1].details

        assert results[2].label == "SIP2 Login"
        assert results[2].success is True

        assert results[3].label == "SC Status"
        assert results[3].success is True

        assert results[4].label == "Patron Information Request"
        assert results[4].success is True
        assert isinstance(results[4].details, dict)
        assert results[4].details["patron_identifier"] == "12345"

        assert results[5].label == "Password Validation"
        assert results[5].success is True
        assert isinstance(results[5].details, str)
        assert "valid_patron=Y" in results[5].details
        assert "valid_patron_password=Y" in results[5].details

        assert results[6].label == "Patron Status Flags"
        assert results[6].success is True

        assert results[7].label == "Library Identifier Restriction"
        assert results[7].success is True

        assert results[8].label == "Parsed Patron Data"
        assert results[8].success is True
        assert isinstance(results[8].details, dict)
        assert results[8].details["authorization_identifier"] == "12345"
        assert results[8].details["personal_name"] == "LE CARRÉ, JOHN"

    def test_patron_debug_invalid_password(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
    ):
        """When the patron's password is invalid, the password validation step
        fails but the flow continues to gather all diagnostic info.
        """
        provider = create_provider()
        client = cast(MockSIPClient, provider.client)

        client.queue_response(self.sierra_invalid_login)
        client.queue_response(self.end_session_response)

        results = provider.patron_debug("user", "wrong_pass")

        assert results[5].label == "Password Validation"
        assert results[5].success is False
        assert isinstance(results[5].details, str)
        assert "valid_patron_password=N" in results[5].details

        # Patron status flags show blocking flags
        assert results[6].label == "Patron Status Flags"
        assert results[6].success is False

        # info_to_patrondata with validate_password=False still returns PatronData
        # so we still get restriction and parsed data
        parsed_result = next(r for r in results if r.label == "Parsed Patron Data")
        assert parsed_result.success is True

    def test_patron_debug_no_such_patron(
        self,
        create_provider: Callable[..., SIP2AuthenticationProvider],
    ):
        """When the patron doesn't exist, info_to_patrondata returns None,
        so library restriction and parsed patron data steps are skipped.
        """
        provider = create_provider()
        client = cast(MockSIPClient, provider.client)

        client.queue_response(self.polaris_no_such_patron)
        client.queue_response(self.end_session_response)

        results = provider.patron_debug("user", "pass")

        # Password validation fails (valid_patron=N)
        password_result = next(r for r in results if r.label == "Password Validation")
        assert password_result.success is False

        # No library restriction or parsed patron data
        labels = [r.label for r in results]
        assert "Library Identifier Restriction" not in labels
        assert "Parsed Patron Data" not in labels
