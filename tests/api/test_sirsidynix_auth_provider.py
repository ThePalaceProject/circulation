import os
from copy import deepcopy
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock, call, patch

import pytest

from api.authenticator import PatronData
from api.config import Configuration
from api.problem_details import PATRON_OF_ANOTHER_LIBRARY
from api.sirsidynix_authentication_provider import (
    SirsiBlockReasons,
    SirsiDynixHorizonAuthenticationProvider,
    SirsiDynixPatronData,
)
from core.model import ExternalIntegration, Patron
from tests.core.mock import MockRequestsResponse
from tests.fixtures.database import DatabaseTransactionFixture


class SirsiDynixAuthenticatorFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.integration = db.external_integration(
            "api.sirsidynix",
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            settings={
                ExternalIntegration.URL: "http://example.org/sirsi",
                SirsiDynixHorizonAuthenticationProvider.Keys.CLIENT_ID: "clientid",
                SirsiDynixHorizonAuthenticationProvider.Keys.LIBRARY_ID: "libraryid",
            },
        )

        with patch.dict(os.environ, {Configuration.SIRSI_DYNIX_APP_ID: "UNITTEST"}):
            self.api = SirsiDynixHorizonAuthenticationProvider(
                db.default_library(), self.integration
            )


@pytest.fixture(scope="function")
def sirsi_fixture(db: DatabaseTransactionFixture) -> SirsiDynixAuthenticatorFixture:
    return SirsiDynixAuthenticatorFixture(db)


class TestSirsiDynixAuthenticationProvider:
    def _headers(self, api):
        return {
            "SD-Originating-App-Id": api.sirsi_app_id,
            "SD-Working-LibraryID": api.sirsi_library_id,
            "x-sirs-clientID": api.sirsi_client_id,
        }

    def test_settings(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        # trailing slash appended to the preset server url
        assert sirsi_fixture.api.server_url == "http://example.org/sirsi/"
        assert sirsi_fixture.api.sirsi_client_id == "clientid"
        assert sirsi_fixture.api.sirsi_app_id == "UNITTEST"
        assert sirsi_fixture.api.sirsi_library_id == "libraryid"

    def test_api_patron_login(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        response_dict = {"sessionToken": "xxxx", "patronKey": "test"}
        with patch(
            "api.sirsidynix_authentication_provider.HTTP.request_with_timeout"
        ) as mock_request:
            mock_request.return_value = MockRequestsResponse(200, content=response_dict)
            response = sirsi_fixture.api.api_patron_login("username", "pwd")

            assert mock_request.call_count == 1
            assert mock_request.call_args == call(
                "POST",
                "http://example.org/sirsi/user/patron/login",
                json=dict(login="username", password="pwd"),
                headers=self._headers(sirsi_fixture.api),
            )
            assert response == response_dict

            mock_request.return_value = MockRequestsResponse(401, content=response_dict)
            assert sirsi_fixture.api.api_patron_login("username", "pwd") is False

    def test_remote_authenticate(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        with patch(
            "api.sirsidynix_authentication_provider.HTTP.request_with_timeout"
        ) as mock_request:
            response_dict = {"sessionToken": "xxxx", "patronKey": "test"}
            mock_request.return_value = MockRequestsResponse(200, content=response_dict)

            response = sirsi_fixture.api.remote_authenticate("username", "pwd")
            assert type(response) == SirsiDynixPatronData
            assert response.authorization_identifier == "username"
            assert response.username == "username"
            assert response.permanent_id == "test"

            mock_request.return_value = MockRequestsResponse(401, content=response_dict)
            assert sirsi_fixture.api.remote_authenticate("username", "pwd") is None

    def test_remote_authenticate_username_password_none(
        self, sirsi_fixture: SirsiDynixAuthenticatorFixture
    ):
        response = sirsi_fixture.api.remote_authenticate(None, "pwd")
        assert response is None

        response = sirsi_fixture.api.remote_authenticate("username", None)
        assert response is None

    def test_remote_patron_lookup(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
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
        sirsi_fixture.api.api_read_patron_data = MagicMock(return_value=ok_patron_resp)  # type: ignore[method-assign]
        sirsi_fixture.api.api_patron_status_info = MagicMock(  # type: ignore[method-assign]
            return_value=patron_status_resp
        )
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )

        assert sirsi_fixture.api.api_read_patron_data.call_count == 1
        assert sirsi_fixture.api.api_patron_status_info.call_count == 1
        assert isinstance(patrondata, PatronData)
        assert patrondata.personal_name == "Test User"
        assert patrondata.fines == 50.00
        assert patrondata.block_reason == PatronData.NO_VALUE
        assert patrondata.library_identifier == "testtype"

        # Test the defensive code
        # Test no session token
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token=None)
        )
        assert patrondata == None

        # Test incorrect patrondata type
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            PatronData(permanent_id="xxxx")
        )
        assert patrondata == None

        # Test bad patron read data
        bad_patron_resp = {"bad": "yes"}
        sirsi_fixture.api.api_read_patron_data = MagicMock(return_value=bad_patron_resp)  # type: ignore[method-assign]
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata == None

        not_approved_patron_resp = {
            "fields": {"approved": False, "patronType": {"key": "testtype"}}
        }
        sirsi_fixture.api.api_read_patron_data = MagicMock(  # type: ignore[method-assign]
            return_value=not_approved_patron_resp
        )
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert isinstance(patrondata, PatronData)
        assert patrondata.block_reason == SirsiBlockReasons.NOT_APPROVED

        # Test blocked patron types
        bad_prefix_patron_resp = {
            "fields": {"approved": True, "patronType": {"key": "testblocked"}}
        }
        sirsi_fixture.api.sirsi_disallowed_suffixes = ["blocked"]
        sirsi_fixture.api.api_read_patron_data = MagicMock(  # type: ignore[method-assign]
            return_value=bad_prefix_patron_resp
        )
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert isinstance(patrondata, PatronData)
        assert patrondata.block_reason == SirsiBlockReasons.PATRON_BLOCKED
        assert patrondata.library_identifier == "testblocked"

        # Test bad patron status info
        sirsi_fixture.api.api_read_patron_data.return_value = ok_patron_resp
        sirsi_fixture.api.api_patron_status_info.return_value = False
        patrondata = sirsi_fixture.api.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata is None

    def test__request(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        # Leading slash on the path is not allowed, as it overwrites the urljoin prefix
        with pytest.raises(ValueError):
            sirsi_fixture.api._request("GET", "/leadingslash")

    @pytest.mark.parametrize(
        "restriction_type, restriction, expected",
        [
            (
                SirsiDynixHorizonAuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE,
                "",
                True,
            ),
            (
                SirsiDynixHorizonAuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX,
                "test",
                True,
            ),
            (
                SirsiDynixHorizonAuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX,
                "abc",
                PATRON_OF_ANOTHER_LIBRARY,
            ),
        ],
    )
    def test_full_auth_request(
        self,
        db: DatabaseTransactionFixture,
        sirsi_fixture: SirsiDynixAuthenticatorFixture,
        restriction_type,
        restriction,
        expected,
    ):
        sirsi_fixture.api.remote_authenticate = MagicMock(  # type: ignore[method-assign]
            return_value=SirsiDynixPatronData(
                permanent_id="xxxx", session_token="xxx", complete=False
            )
        )
        sirsi_fixture.api.remote_patron_lookup = MagicMock(  # type: ignore[method-assign]
            return_value=PatronData(
                permanent_id="xxxx",
                personal_name="Test User",
                fines=50.00,
                library_identifier="testtype",
            )
        )
        sirsi_fixture.api.library_identifier_field = "patronType"
        sirsi_fixture.api.library_identifier_restriction_type = restriction_type
        sirsi_fixture.api.library_identifier_restriction = restriction
        patron = sirsi_fixture.api.authenticated_patron(
            db.session, {"username": "testuser", "password": "testpass"}
        )
        sirsi_fixture.api.remote_authenticate.assert_called_with("testuser", "testpass")
        sirsi_fixture.api.remote_patron_lookup.assert_called()
        if expected is True:
            assert isinstance(patron, Patron)
            assert patron.fines == 50.00
            assert patron.block_reason is None
        else:
            assert patron == expected

    def test_blocked_patron_status_info(
        self, sirsi_fixture: SirsiDynixAuthenticatorFixture
    ):
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

        statuses: List[Tuple[Dict[str, bool], Any]] = [
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

            sirsi_fixture.api.api_read_patron_data = MagicMock(  # type: ignore[method-assign]
                return_value=ok_patron_resp
            )
            sirsi_fixture.api.api_patron_status_info = MagicMock(  # type: ignore[method-assign]
                return_value={"fields": info_copy}
            )

            data = sirsi_fixture.api.remote_patron_lookup(
                SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
            )
            assert isinstance(data, SirsiDynixPatronData)
            assert data.block_reason == reason

    def test_api_methods(self, sirsi_fixture: SirsiDynixAuthenticatorFixture):
        """The patron data and patron status methods are almost identical in functionality
        They just hit different APIs, so we only test the difference in endpoints
        """
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
                test_method = getattr(sirsi_fixture.api, api_method)

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
