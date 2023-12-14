from collections.abc import Callable
from copy import deepcopy
from functools import partial
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch

from api.authentication.base import PatronData
from api.authentication.basic import LibraryIdentifierRestriction
from api.config import Configuration
from api.problem_details import PATRON_OF_ANOTHER_LIBRARY
from api.sirsidynix_authentication_provider import (
    SirsiBlockReasons,
    SirsiDynixHorizonAuthenticationProvider,
    SirsiDynixHorizonAuthLibrarySettings,
    SirsiDynixHorizonAuthSettings,
    SirsiDynixPatronData,
)
from core.model import Patron
from tests.core.mock import MockRequestsResponse
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def mock_library_id() -> int:
    return 20


@pytest.fixture
def mock_integration_id() -> int:
    return 20


@pytest.fixture
def create_library_settings() -> Callable[..., SirsiDynixHorizonAuthLibrarySettings]:
    return partial(
        SirsiDynixHorizonAuthLibrarySettings,
        library_id="libraryid",
    )


@pytest.fixture
def create_settings() -> Callable[..., SirsiDynixHorizonAuthSettings]:
    return partial(
        SirsiDynixHorizonAuthSettings,
        url="http://example.org/sirsi/",
        test_identifier="barcode",
        client_id="clientid",
    )


@pytest.fixture
def create_provider(
    mock_library_id: int,
    mock_integration_id: int,
    create_settings: Callable[..., SirsiDynixHorizonAuthSettings],
    create_library_settings: Callable[..., SirsiDynixHorizonAuthLibrarySettings],
    monkeypatch: MonkeyPatch,
) -> Callable[..., SirsiDynixHorizonAuthenticationProvider]:
    monkeypatch.setenv(Configuration.SIRSI_DYNIX_APP_ID, "UNITTEST")
    return partial(
        SirsiDynixHorizonAuthenticationProvider,
        library_id=mock_library_id,
        integration_id=mock_integration_id,
        settings=create_settings(),
        library_settings=create_library_settings(),
    )


class TestSirsiDynixAuthenticationProvider:
    def _headers(self, api):
        return {
            "SD-Originating-App-Id": api.sirsi_app_id,
            "SD-Working-LibraryID": api.sirsi_library_id,
            "x-sirs-clientID": api.sirsi_client_id,
        }

    def test_settings(
        self, create_provider: Callable[..., SirsiDynixHorizonAuthenticationProvider]
    ):
        # trailing slash appended to the preset server url
        provider = create_provider()
        assert provider.server_url == "http://example.org/sirsi/"
        assert provider.sirsi_client_id == "clientid"
        assert provider.sirsi_app_id == "UNITTEST"
        assert provider.sirsi_library_id == "libraryid"

    def test_api_patron_login(
        self, create_provider: Callable[..., SirsiDynixHorizonAuthenticationProvider]
    ):
        provider = create_provider()
        response_dict = {"sessionToken": "xxxx", "patronKey": "test"}
        with patch(
            "api.sirsidynix_authentication_provider.HTTP.request_with_timeout"
        ) as mock_request:
            mock_request.return_value = MockRequestsResponse(200, content=response_dict)
            response = provider.api_patron_login("username", "pwd")

            assert mock_request.call_count == 1
            assert mock_request.call_args == call(
                "POST",
                "http://example.org/sirsi/user/patron/login",
                json=dict(login="username", password="pwd"),
                headers=self._headers(provider),
                max_retry_count=0,
            )
            assert response == response_dict

            mock_request.return_value = MockRequestsResponse(401, content=response_dict)
            assert provider.api_patron_login("username", "pwd") is False

    def test_remote_authenticate(
        self, create_provider: Callable[..., SirsiDynixHorizonAuthenticationProvider]
    ):
        provider = create_provider()
        with patch(
            "api.sirsidynix_authentication_provider.HTTP.request_with_timeout"
        ) as mock_request:
            response_dict = {"sessionToken": "xxxx", "patronKey": "test"}
            mock_request.return_value = MockRequestsResponse(200, content=response_dict)

            response = provider.remote_authenticate("username", "pwd")
            assert type(response) == SirsiDynixPatronData
            assert response.authorization_identifier == "username"
            assert response.username == "username"
            assert response.permanent_id == "test"

            mock_request.return_value = MockRequestsResponse(401, content=response_dict)
            assert provider.remote_authenticate("username", "pwd") is None

    def test_remote_authenticate_username_password_none(
        self, create_provider: Callable[..., SirsiDynixHorizonAuthenticationProvider]
    ):
        provider = create_provider()
        response = provider.remote_authenticate(None, "pwd")
        assert response is None

        response = provider.remote_authenticate("username", None)
        assert response is None

    def test_remote_patron_lookup(
        self, create_provider: Callable[..., SirsiDynixHorizonAuthenticationProvider]
    ):
        provider = create_provider()
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
        provider.api_read_patron_data = MagicMock(return_value=ok_patron_resp)
        provider.api_patron_status_info = MagicMock(return_value=patron_status_resp)
        patrondata = provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )

        assert provider.api_read_patron_data.call_count == 1
        assert provider.api_patron_status_info.call_count == 1
        assert isinstance(patrondata, PatronData)
        assert patrondata.personal_name == "Test User"
        assert patrondata.fines == 50.00
        assert patrondata.block_reason == PatronData.NO_VALUE
        assert patrondata.library_identifier == "testtype"

        # Test the defensive code
        # Test no session token
        patrondata = provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token=None)
        )
        assert patrondata == None

        # Test incorrect patrondata type
        patrondata = provider.remote_patron_lookup(PatronData(permanent_id="xxxx"))
        assert patrondata == None

        # Test bad patron read data
        bad_patron_resp = {"bad": "yes"}
        provider.api_read_patron_data = MagicMock(return_value=bad_patron_resp)
        patrondata = provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata == None

        not_approved_patron_resp = {
            "fields": {"approved": False, "patronType": {"key": "testtype"}}
        }
        provider.api_read_patron_data = MagicMock(return_value=not_approved_patron_resp)
        patrondata = provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert isinstance(patrondata, PatronData)
        assert patrondata.block_reason == SirsiBlockReasons.NOT_APPROVED

        # Test blocked patron types
        bad_prefix_patron_resp = {
            "fields": {"approved": True, "patronType": {"key": "testblocked"}}
        }
        provider.sirsi_disallowed_suffixes = ["blocked"]
        provider.api_read_patron_data = MagicMock(return_value=bad_prefix_patron_resp)
        patrondata = provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert isinstance(patrondata, PatronData)
        assert patrondata.block_reason == SirsiBlockReasons.PATRON_BLOCKED
        assert patrondata.library_identifier == "testblocked"

        # Test bad patron status info
        provider.api_read_patron_data.return_value = ok_patron_resp
        provider.api_patron_status_info.return_value = False
        patrondata = provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata is None

    def test__request(
        self, create_provider: Callable[..., SirsiDynixHorizonAuthenticationProvider]
    ):
        provider = create_provider()
        # Leading slash on the path is not allowed, as it overwrites the urljoin prefix
        with pytest.raises(ValueError):
            provider._request("GET", "/leadingslash")

    @pytest.mark.parametrize(
        "restriction_type, restriction, expected",
        [
            (
                LibraryIdentifierRestriction.NONE,
                "",
                True,
            ),
            (
                LibraryIdentifierRestriction.PREFIX,
                "test",
                True,
            ),
            (
                LibraryIdentifierRestriction.PREFIX,
                "abc",
                PATRON_OF_ANOTHER_LIBRARY,
            ),
        ],
    )
    def test_full_auth_request(
        self,
        db: DatabaseTransactionFixture,
        create_library_settings: Callable[..., SirsiDynixHorizonAuthLibrarySettings],
        create_provider: Callable[..., SirsiDynixHorizonAuthenticationProvider],
        restriction_type,
        restriction,
        expected,
    ):
        library = db.default_library()
        library_settings = create_library_settings(
            library_identifier_field="patronType",
            library_identifier_restriction_type=restriction_type,
            library_identifier_restriction_criteria=restriction,
        )
        provider = create_provider(
            library_id=library.id,
            library_settings=library_settings,
        )
        provider.remote_authenticate = MagicMock(
            return_value=SirsiDynixPatronData(
                permanent_id="xxxx", session_token="xxx", complete=False
            )
        )
        provider.remote_patron_lookup = MagicMock(
            return_value=PatronData(
                permanent_id="xxxx",
                personal_name="Test User",
                fines=50.00,
                library_identifier="testtype",
            )
        )
        patron = provider.authenticated_patron(
            db.session, {"username": "testuser", "password": "testpass"}
        )
        provider.remote_authenticate.assert_called_with("testuser", "testpass")
        provider.remote_patron_lookup.assert_called()
        if expected is True:
            assert isinstance(patron, Patron)
            assert patron.fines == 50.00
            assert patron.block_reason is None
        else:
            assert patron == expected

    def test_blocked_patron_status_info(
        self,
        create_provider: Callable[..., SirsiDynixHorizonAuthenticationProvider],
    ):
        provider = create_provider()
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

        statuses: list[tuple[dict[str, bool], Any]] = [
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

            provider.api_read_patron_data = MagicMock(return_value=ok_patron_resp)
            provider.api_patron_status_info = MagicMock(
                return_value={"fields": info_copy}
            )

            data = provider.remote_patron_lookup(
                SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
            )
            assert isinstance(data, SirsiDynixPatronData)
            assert data.block_reason == reason

    def test_api_methods(
        self, create_provider: Callable[..., SirsiDynixHorizonAuthenticationProvider]
    ):
        """The patron data and patron status methods are almost identical in functionality
        They just hit different APIs, so we only test the difference in endpoints
        """
        provider = create_provider()
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
                test_method = getattr(provider, api_method)

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
