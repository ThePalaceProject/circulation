import json
from contextlib import nullcontext
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal
from unittest.mock import MagicMock, call, create_autospec

import pytest
from _pytest._code import ExceptionInfo

from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authentication.basic import LibraryIdentifierRestriction
from palace.manager.api.config import Configuration
from palace.manager.api.problem_details import PATRON_OF_ANOTHER_LIBRARY
from palace.manager.core.selftest import SelfTestResult
from palace.manager.integration.patron_auth.sirsidynix_authentication_provider import (
    SirsiBlockReasons,
    SirsiDynixHorizonAuthenticationProvider,
    SirsiDynixHorizonAuthLibrarySettings,
    SirsiDynixHorizonAuthSettings,
    SirsiDynixPatronData,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.http.http import HTTP
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.mock import MockRequestsResponse


@dataclass
class MockedSirsiApi:
    provider: SirsiDynixHorizonAuthenticationProvider
    api_patron_login: MagicMock
    api_read_patron_data: MagicMock
    api_patron_status_info: MagicMock


class SirsiAuthFixture:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.app_id = "UNITTEST"
        monkeypatch.setenv(Configuration.SIRSI_DYNIX_APP_ID, self.app_id)

        self.library_id = "libraryid"
        self.library_settings = partial(
            SirsiDynixHorizonAuthLibrarySettings,
            library_id=self.library_id,
        )

        self.url = "http://example.org/sirsi/"
        self.test_identifier = "barcode"
        self.client_id = "clientid"

        self.settings = partial(
            SirsiDynixHorizonAuthSettings,
            url=self.url,
            test_identifier=self.test_identifier,
            client_id=self.client_id,
        )

        self.mock_library_id = 20
        self.mock_integration_id = 20
        self.provider = partial(
            SirsiDynixHorizonAuthenticationProvider,
            library_id=self.mock_library_id,
            integration_id=self.mock_integration_id,
            settings=self.settings(),
            library_settings=self.library_settings(),
        )

        self.mock_request = create_autospec(HTTP.request_with_timeout)
        monkeypatch.setattr(HTTP, "request_with_timeout", self.mock_request)

        self.mock_session = MagicMock()

    def headers(self, api: SirsiDynixHorizonAuthenticationProvider) -> dict[str, str]:
        return {
            "SD-Originating-App-Id": api.sirsi_app_id,
            "SD-Working-LibraryID": api.sirsi_library_id,
            "x-sirs-clientID": api.sirsi_client_id,
        }

    def provider_mocked_api(
        self,
        provider: SirsiDynixHorizonAuthenticationProvider | None = None,
        patron_status_info: dict[str, Any] | None = None,
    ) -> MockedSirsiApi:
        if provider is None:
            provider = self.provider()

        api_patron_login = create_autospec(
            provider.api_patron_login,
            return_value={"patronKey": "test", "sessionToken": "xxx"},
        )
        api_read_patron_data = create_autospec(
            provider.api_read_patron_data,
            return_value={
                "fields": {
                    "displayName": "Test User",
                    "approved": True,
                    "patronType": {"key": "testtype"},
                }
            },
        )

        if not patron_status_info:
            patron_status_info = {
                "fields": {
                    "estimatedFines": {
                        "amount": "50.00",
                        "currencyCode": "USD",
                    }
                }
            }

        api_patron_status_info = create_autospec(
            provider.api_patron_status_info,
            return_value=patron_status_info,
        )

        provider.api_patron_login = api_patron_login
        provider.api_read_patron_data = api_read_patron_data
        provider.api_patron_status_info = api_patron_status_info

        return MockedSirsiApi(
            provider=provider,
            api_patron_login=api_patron_login,
            api_read_patron_data=api_read_patron_data,
            api_patron_status_info=api_patron_status_info,
        )

    def run_self_tests(
        self, api: SirsiDynixHorizonAuthenticationProvider
    ) -> list[SelfTestResult]:
        return list(api._run_self_tests(self.mock_session))


@pytest.fixture
def sirsi_auth_fixture(monkeypatch: pytest.MonkeyPatch) -> SirsiAuthFixture:
    return SirsiAuthFixture(monkeypatch)


class TestSirsiDynixAuthenticationProvider:
    def test_settings(self, sirsi_auth_fixture: SirsiAuthFixture):
        # trailing slash appended to the preset server url
        provider = sirsi_auth_fixture.provider()
        assert provider.server_url == "http://example.org/sirsi/"
        assert provider.sirsi_client_id == "clientid"
        assert provider.sirsi_app_id == "UNITTEST"
        assert provider.sirsi_library_id == "libraryid"

    def test_api_patron_login(self, sirsi_auth_fixture: SirsiAuthFixture):
        provider = sirsi_auth_fixture.provider()
        response_dict = {"sessionToken": "xxxx", "patronKey": "test"}
        sirsi_auth_fixture.mock_request.return_value = MockRequestsResponse(
            200, content=response_dict
        )
        response = provider.api_patron_login("username", "pwd")

        assert sirsi_auth_fixture.mock_request.call_count == 1
        assert sirsi_auth_fixture.mock_request.call_args == call(
            "POST",
            "http://example.org/sirsi/user/patron/login",
            json=dict(login="username", password="pwd"),
            headers=sirsi_auth_fixture.headers(provider),
            max_retry_count=0,
        )
        assert response == response_dict

        sirsi_auth_fixture.mock_request.return_value = MockRequestsResponse(
            401, content=response_dict
        )
        assert provider.api_patron_login("username", "pwd") is False

    def test_remote_authenticate(self, sirsi_auth_fixture: SirsiAuthFixture):
        provider = sirsi_auth_fixture.provider()
        response_dict = {"sessionToken": "xxxx", "patronKey": "test"}
        sirsi_auth_fixture.mock_request.return_value = MockRequestsResponse(
            200, content=response_dict
        )

        response = provider.remote_authenticate("username", "pwd")
        assert type(response) == SirsiDynixPatronData
        assert response.authorization_identifier == "username"
        assert response.username == "username"
        assert response.permanent_id == "test"

        sirsi_auth_fixture.mock_request.return_value = MockRequestsResponse(
            401, content=response_dict
        )
        assert provider.remote_authenticate("username", "pwd") is None

    def test_remote_authenticate_username_password_none(
        self, sirsi_auth_fixture: SirsiAuthFixture
    ):
        provider = sirsi_auth_fixture.provider()
        response = provider.remote_authenticate(None, "pwd")
        assert response is None

        response = provider.remote_authenticate("username", None)
        assert response is None

    def test_remote_patron_lookup(self, sirsi_auth_fixture: SirsiAuthFixture):
        provider_mock = sirsi_auth_fixture.provider_mocked_api()
        # Test the happy path, patron OK, some fines
        patrondata = provider_mock.provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )

        assert provider_mock.api_read_patron_data.call_count == 1
        assert provider_mock.api_patron_status_info.call_count == 1
        assert isinstance(patrondata, PatronData)
        assert patrondata.personal_name == "Test User"
        assert patrondata.fines == 50.00
        assert patrondata.block_reason == PatronData.NO_VALUE
        assert patrondata.library_identifier == "testtype"

    @pytest.mark.parametrize(
        "patron_data, patron_blocks_enforced, block_reason",
        [
            (
                None,
                False,
                PatronData.NO_VALUE,
            ),
            (
                None,
                True,
                PatronData.NO_VALUE,
            ),
            (
                {"fields": {"hasMaxDaysWithFines": True}},
                True,
                PatronData.EXCESSIVE_FINES,
            ),
            (
                {"fields": {"hasMaxDaysWithFines": True}},
                False,
                PatronData.NO_VALUE,
            ),
            (
                {"fields": {"privilegeExpiresDate": "9999-01-01"}},
                True,
                PatronData.NO_VALUE,
            ),
            (
                {"fields": {"expired": True}},
                True,
                SirsiBlockReasons.EXPIRED,
            ),
            (
                {"fields": {"expired": True}},
                False,
                SirsiBlockReasons.EXPIRED,
            ),
            (
                {"fields": {"expired": False}},
                True,
                PatronData.NO_VALUE,
            ),
            (
                {"fields": {"expired": False}},
                False,
                PatronData.NO_VALUE,
            ),
        ],
    )
    def test_remote_patron_lookup_blocks(
        self,
        sirsi_auth_fixture: SirsiAuthFixture,
        patron_data: dict[Any, Any],
        patron_blocks_enforced: bool,
        block_reason: str,
    ):
        settings = sirsi_auth_fixture.settings(
            patron_blocks_enforced=patron_blocks_enforced
        )
        provider = sirsi_auth_fixture.provider(settings=settings)
        provider_mock = sirsi_auth_fixture.provider_mocked_api(provider)
        if patron_data:
            provider_mock.api_patron_status_info.return_value = patron_data

        patrondata = provider_mock.provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )

        assert provider_mock.api_read_patron_data.call_count == 1
        assert provider_mock.api_patron_status_info.call_count == 1
        assert isinstance(patrondata, PatronData)
        assert patrondata.personal_name == "Test User"
        assert patrondata.block_reason == block_reason

    def test_remote_patron_lookup_bad_patrondata(
        self,
        sirsi_auth_fixture: SirsiAuthFixture,
        db: DatabaseTransactionFixture,
    ):
        # Test no session token
        provider = sirsi_auth_fixture.provider_mocked_api().provider
        patron_data = provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token=None)
        )

        assert patron_data and not patron_data.complete

        # Test incorrect patrondata type
        patron_data = provider.remote_patron_lookup(PatronData(permanent_id="xxxx"))
        assert patron_data and not patron_data.complete

        # Test remote_patron_lookup with Patron object
        patron = db.patron()
        assert provider.remote_patron_lookup(patron) is None

    def test_remote_patron_lookup_bad_patron_read_data(
        self, sirsi_auth_fixture: SirsiAuthFixture
    ):
        # Test bad patron read data
        provider_mock = sirsi_auth_fixture.provider_mocked_api()
        bad_patron_resp = {"bad": "yes"}
        provider_mock.api_read_patron_data.return_value = bad_patron_resp
        patrondata = provider_mock.provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata is None

        not_approved_patron_resp = {
            "fields": {"approved": False, "patronType": {"key": "testtype"}}
        }
        provider_mock.api_read_patron_data.return_value = not_approved_patron_resp
        patrondata = provider_mock.provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert isinstance(patrondata, PatronData)
        assert patrondata.block_reason == SirsiBlockReasons.NOT_APPROVED

        # Test blocked patron types
        bad_prefix_patron_resp = {
            "fields": {"approved": True, "patronType": {"key": "testblocked"}}
        }
        provider_mock.provider.sirsi_disallowed_suffixes = ["blocked"]
        provider_mock.api_read_patron_data.return_value = bad_prefix_patron_resp
        patrondata = provider_mock.provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert isinstance(patrondata, PatronData)
        assert patrondata.block_reason == SirsiBlockReasons.PATRON_BLOCKED
        assert patrondata.library_identifier == "testblocked"

    @pytest.mark.parametrize(
        "patron_type, disallowed_suffixes, expected_blocked",
        [
            pytest.param(
                "testblocked",
                ["blocked"],
                True,
                id="patron_type_ends_with_disallowed_suffix_blocked",
            ),
            pytest.param(
                "testblocked",
                ["ed", "xyz"],
                True,
                id="patron_type_matches_one_of_multiple_suffixes_blocked",
            ),
            pytest.param(
                "testuser",
                ["blocked", "admin"],
                False,
                id="patron_type_no_matching_suffix_not_blocked",
            ),
            pytest.param(
                "blocked",
                ["blocked"],
                True,
                id="patron_type_exact_match_with_suffix_blocked",
            ),
            pytest.param(
                "test",
                [],
                False,
                id="no_disallowed_suffixes_configured_not_blocked",
            ),
            pytest.param(
                "",
                ["test"],
                False,
                id="empty_patron_type_not_blocked",
            ),
        ],
    )
    def test_remote_patron_lookup_disallowed_suffixes(
        self,
        sirsi_auth_fixture: SirsiAuthFixture,
        patron_type: str,
        disallowed_suffixes: list[str],
        expected_blocked: bool,
    ):
        """Test that patron types ending with disallowed suffixes are blocked."""
        library_settings = sirsi_auth_fixture.library_settings(
            library_disallowed_suffixes=disallowed_suffixes
        )
        provider = sirsi_auth_fixture.provider(library_settings=library_settings)
        provider_mock = sirsi_auth_fixture.provider_mocked_api(provider=provider)

        # Set up patron response with specific patron type
        patron_resp = {
            "fields": {
                "displayName": "Test User",
                "approved": True,
                "patronType": {"key": patron_type},
            }
        }
        provider_mock.api_read_patron_data.return_value = patron_resp

        patrondata = provider_mock.provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )

        assert isinstance(patrondata, PatronData)

        if expected_blocked:
            assert patrondata.block_reason == SirsiBlockReasons.PATRON_BLOCKED
        else:
            # If not blocked by suffix, should be NO_VALUE (no blocks)
            assert patrondata.block_reason == PatronData.NO_VALUE

    @pytest.mark.parametrize(
        "standing_key, approved, blocks_enforced, expected_blocked",
        [
            pytest.param(
                "OK",
                True,
                True,
                False,
                id="approved_and_standing_OK_not_blocked",
            ),
            pytest.param(
                "ok",
                True,
                True,
                False,
                id="approved_and_standing_ok_lowercase_not_blocked",
            ),
            pytest.param(
                "Ok",
                True,
                True,
                False,
                id="approved_and_standing_Ok_mixedcase_not_blocked",
            ),
            pytest.param(
                "DELINQUENT",
                True,
                True,
                False,
                id="approved_true_standing_delinquent_not_blocked",
            ),
            pytest.param(
                "",
                True,
                True,
                False,
                id="approved_true_standing_empty_not_blocked",
            ),
            pytest.param(
                "OK",
                False,
                True,
                False,
                id="standing_OK_approved_false_not_blocked",
            ),
            pytest.param(
                None,
                True,
                True,
                False,
                id="approved_true_no_standing_field_not_blocked",
            ),
            pytest.param(
                "ok",
                False,
                True,
                False,
                id="standing_ok_approved_false_not_blocked",
            ),
            pytest.param(
                "DELINQUENT",
                False,
                True,
                True,
                id="not_approved_standing_delinquent_blocked",
            ),
            pytest.param(
                "",
                False,
                True,
                True,
                id="not_approved_standing_empty_blocked",
            ),
            pytest.param(
                "DELINQUENT",
                False,
                False,
                False,
                id="not_approved_standing_delinquent_not_enforced_not_blocked",
            ),
            pytest.param(
                "",
                False,
                False,
                False,
                id="not_approved_standing_empty_not_enforced_not_blocked",
            ),
            pytest.param(
                None,
                False,
                True,
                True,
                id="not_approved_no_standing_field_blocked",
            ),
            pytest.param(
                None,
                False,
                False,
                False,
                id="not_approved_no_standing_field_not_enforced_not_blocked",
            ),
        ],
    )
    def test_remote_patron_lookup_approval_and_standing_fields(
        self,
        sirsi_auth_fixture: SirsiAuthFixture,
        standing_key: str | None,
        approved: bool,
        blocks_enforced: bool,
        expected_blocked: bool,
    ):
        """
        Test patron approval requires EITHER 'approved=True' OR 'standing.key=ok' (case-insensitive) if
        blocks are enforced.
        """
        settings = sirsi_auth_fixture.settings(patron_blocks_enforced=blocks_enforced)
        provider = sirsi_auth_fixture.provider(settings=settings)
        provider_mock = sirsi_auth_fixture.provider_mocked_api(provider=provider)

        # Build patron response with standing field
        patron_fields = {
            "displayName": "Test User",
            "approved": approved,
            "patronType": {"key": "testtype"},
        }

        if standing_key is not None:
            patron_fields["standing"] = {"key": standing_key}

        patron_resp = {"fields": patron_fields}
        provider_mock.api_read_patron_data.return_value = patron_resp

        patrondata = provider_mock.provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )

        assert isinstance(patrondata, PatronData)

        if expected_blocked:
            assert patrondata.block_reason == SirsiBlockReasons.NOT_APPROVED
        else:
            # If not blocked by approval/standing, should be NO_VALUE (no blocks)
            assert patrondata.block_reason == PatronData.NO_VALUE

    @pytest.mark.parametrize(
        "library_identifier_field, expect_error",
        [
            pytest.param(
                "patrontype",
                True,
                id="patrontype_field_missing_patronType_raises_KeyError",
            ),
            pytest.param(
                "barcode",
                False,
                id="barcode_field_missing_patronType_handled_gracefully",
            ),
        ],
    )
    def test_remote_patron_lookup_missing_patron_type_with_disallowed_suffixes(
        self,
        sirsi_auth_fixture: SirsiAuthFixture,
        library_identifier_field: str,
        expect_error: bool,
    ):
        """Test behavior when patronType field is missing from API response.

        When library_identifier_field is 'patrontype', missing patronType raises KeyError.
        When library_identifier_field is 'barcode', missing patronType is handled gracefully.
        """
        library_settings = sirsi_auth_fixture.library_settings(
            library_identifier_field=library_identifier_field,
            library_disallowed_suffixes=["blocked", "test"],
        )
        provider = sirsi_auth_fixture.provider(library_settings=library_settings)
        provider_mock = sirsi_auth_fixture.provider_mocked_api(provider=provider)

        # Return patron data WITHOUT patronType field
        patron_resp_no_type = {
            "fields": {
                "displayName": "Test User",
                "approved": True,
                # patronType is missing
            }
        }
        provider_mock.api_read_patron_data.return_value = patron_resp_no_type

        if expect_error:
            # When library_identifier_field is "patrontype", should raise KeyError
            with pytest.raises(KeyError, match="patronType"):
                provider_mock.provider.remote_patron_lookup(
                    SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
                )
        else:
            # When library_identifier_field is "barcode", should handle gracefully
            patrondata = provider_mock.provider.remote_patron_lookup(
                SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
            )
            assert isinstance(patrondata, PatronData)
            assert patrondata.personal_name == "Test User"
            # Should not be blocked since approved=True
            assert patrondata.block_reason == PatronData.NO_VALUE

    def test_remote_patron_lookup_bad_patron_status_info(
        self, sirsi_auth_fixture: SirsiAuthFixture
    ):
        # Test bad patron status info
        provider_mock = sirsi_auth_fixture.provider_mocked_api()
        provider_mock.api_patron_status_info.return_value = False
        patrondata = provider_mock.provider.remote_patron_lookup(
            SirsiDynixPatronData(permanent_id="xxxx", session_token="xxx")
        )
        assert patrondata is None

    def test__request(self, sirsi_auth_fixture: SirsiAuthFixture):
        provider = sirsi_auth_fixture.provider()
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
                PATRON_OF_ANOTHER_LIBRARY.with_debug(
                    "'patrontype' does not match library restriction: 'testtype' does not start with 'abc'."
                ),
            ),
        ],
    )
    def test_full_auth_request(
        self,
        db: DatabaseTransactionFixture,
        sirsi_auth_fixture: SirsiAuthFixture,
        restriction_type: LibraryIdentifierRestriction,
        restriction: str,
        expected: Literal[True] | ProblemDetail,
    ):
        library = db.default_library()
        library_settings = sirsi_auth_fixture.library_settings(
            library_identifier_field="patrontype",
            library_identifier_restriction_type=restriction_type,
            library_identifier_restriction_criteria=restriction,
        )
        assert library.id is not None
        provider = sirsi_auth_fixture.provider(
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
        context_manager = (
            pytest.raises(ProblemDetailException)
            if isinstance(expected, ProblemDetail)
            else nullcontext()
        )
        with context_manager as ctx:
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
            assert isinstance(ctx, ExceptionInfo)
            problem_detail = ctx.value.problem_detail
            assert problem_detail == expected

    def test_blocked_patron_status_info(self, sirsi_auth_fixture: SirsiAuthFixture):
        provider = sirsi_auth_fixture.provider()
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

    @pytest.mark.parametrize(
        "api_method, uri",
        [
            ("api_read_patron_data", "user/patron/key/patronkey"),
            (
                "api_patron_status_info",
                "user/patronStatusInfo/key/patronkey",
            ),
        ],
    )
    def test_api_methods(
        self, sirsi_auth_fixture: SirsiAuthFixture, api_method: str, uri: str
    ):
        """The patron data and patron status methods are almost identical in functionality
        They just hit different APIs, so we only test the difference in endpoints
        """
        provider = sirsi_auth_fixture.provider()
        test_method = getattr(provider, api_method)

        response_content = {"success": True}

        sirsi_auth_fixture.mock_request.return_value = MockRequestsResponse(
            200, content=response_content
        )
        assert test_method("patronkey", "sessiontoken") == response_content
        assert sirsi_auth_fixture.mock_request.call_count == 1
        assert sirsi_auth_fixture.mock_request.call_args.args == (
            "GET",
            sirsi_auth_fixture.url + uri,
        )

        # Test failure
        sirsi_auth_fixture.mock_request.return_value = MockRequestsResponse(400)
        assert test_method("patronkey", "sessiontoken") is False

    def test__run_self_tests(self, sirsi_auth_fixture: SirsiAuthFixture):
        mocked_provider = sirsi_auth_fixture.provider_mocked_api()
        mocked_provider.provider.testing_patron_or_bust = MagicMock(
            return_value=(MagicMock(), "test")
        )
        [
            login_result,
            patron_data_result,
            patron_status_result,
            auth_result,
            sync_result,
        ] = sirsi_auth_fixture.run_self_tests(mocked_provider.provider)

        # We display a result for login
        assert login_result.name == "Login Patron"
        assert login_result.success is True

        # We display a result for patron data and return the patrons fields as json
        assert patron_data_result.name == "Read Patron Data"
        assert patron_data_result.success is True
        assert json.loads(
            patron_data_result.result
        ) == mocked_provider.api_read_patron_data.return_value.get("fields")

        # We display a result for patron status and return the patrons fields as json
        assert patron_status_result.name == "Patron Status Info"
        assert patron_status_result.success is True
        assert json.loads(
            patron_status_result.result
        ) == mocked_provider.api_patron_status_info.return_value.get("fields")

        # And we return the results from the super class as well
        assert auth_result.name == "Authenticating test patron"
        assert auth_result.success is True

        assert sync_result.name == "Syncing patron metadata"
        assert sync_result.success is True

    def test__run_self_tests_no_barcode(self, sirsi_auth_fixture: SirsiAuthFixture):
        mocked_provider = sirsi_auth_fixture.provider_mocked_api()
        mocked_provider.provider.test_username = None
        [test_result] = sirsi_auth_fixture.run_self_tests(mocked_provider.provider)
        assert test_result.success is False
        assert str(test_result.exception) == "No test patron username configured."

    def test__run_self_tests_patron_login(self, sirsi_auth_fixture: SirsiAuthFixture):
        mocked_provider = sirsi_auth_fixture.provider_mocked_api()
        mocked_provider.api_patron_login.return_value = False
        [test_result] = sirsi_auth_fixture.run_self_tests(mocked_provider.provider)
        assert test_result.success is False
        assert str(test_result.exception) == "Could not authenticate test patron"

    @pytest.mark.parametrize(
        "api_read_patron_data_resp, expected_exception",
        [
            [False, "Could not fetch Patron Data"],
            [{"bad": "data"}, "Field data 'fields' not found in Patron Data."],
            [{"fields": "bad data"}, 'Field data is not a dict (data: "bad data").'],
        ],
    )
    def test__run_self_tests_read_patron_data(
        self,
        sirsi_auth_fixture: SirsiAuthFixture,
        api_read_patron_data_resp: dict[str, Any] | bool,
        expected_exception: str,
    ):
        mocked_provider = sirsi_auth_fixture.provider_mocked_api()
        mocked_provider.api_read_patron_data.return_value = api_read_patron_data_resp
        [login_result, patron_data_result] = sirsi_auth_fixture.run_self_tests(
            mocked_provider.provider
        )
        assert login_result.success is True
        assert patron_data_result.success is False
        assert str(patron_data_result.exception) == expected_exception

    @pytest.mark.parametrize(
        "api_patron_status_info_resp, expected_exception",
        [
            [False, "Could not fetch Patron Status"],
            [{}, "Field data 'fields' not found in Patron Status."],
            [{"fields": ["a", "b"]}, 'Field data is not a dict (data: ["a", "b"]).'],
        ],
    )
    def test__run_self_tests_patron_status_info(
        self,
        sirsi_auth_fixture: SirsiAuthFixture,
        api_patron_status_info_resp: dict[str, Any] | bool,
        expected_exception: str,
    ):
        mocked_provider = sirsi_auth_fixture.provider_mocked_api()
        mocked_provider.api_patron_status_info.return_value = (
            api_patron_status_info_resp
        )
        [
            login_result,
            patron_data_result,
            patron_status_result,
        ] = sirsi_auth_fixture.run_self_tests(mocked_provider.provider)
        assert login_result.success is True
        assert patron_data_result.success is True
        assert patron_status_result.success is False
        assert str(patron_status_result.exception) == expected_exception
