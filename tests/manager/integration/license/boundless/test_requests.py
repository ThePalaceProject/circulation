from __future__ import annotations

import json
from collections.abc import Callable, Generator
from functools import partial
from unittest.mock import MagicMock

import pytest

from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    NotFoundOnRemote,
    PatronAuthorizationFailedException,
    RemoteInitiatedServerError,
)
from palace.manager.core.exceptions import IntegrationException
from palace.manager.integration.license.boundless.constants import (
    API_BASE_URLS,
    LICENSE_SERVER_BASE_URLS,
    ServerNickname,
)
from palace.manager.integration.license.boundless.exception import (
    BoundlessLicenseError,
    BoundlessValidationError,
)
from palace.manager.integration.license.boundless.model.json import (
    AudiobookMetadataResponse,
)
from palace.manager.integration.license.boundless.model.xml import (
    AddHoldResponse,
    EarlyCheckinResponse,
    RemoveHoldResponse,
)
from palace.manager.integration.license.boundless.requests import BoundlessRequests
from palace.manager.integration.license.boundless.settings import BoundlessSettings
from palace.manager.util.datetime_helpers import datetime_utc
from palace.manager.util.http.exception import RemoteIntegrationException
from tests.fixtures.files import BoundlessFilesFixture
from tests.fixtures.http import MockHttpClientFixture


class BoundlessRequestsFixture:
    def __init__(self, http_client: MockHttpClientFixture) -> None:
        self.create_settings = partial(
            BoundlessSettings,
            external_account_id="test_library_id",
            username="test_username",
            password="test_password",
        )
        self.settings = self.create_settings()
        self.requests = BoundlessRequests(self.settings)
        self.client = http_client
        self.request = partial(self.requests._request, "GET", "endpoint")


@pytest.fixture
def boundless_requests(
    boundless_files_fixture: BoundlessFilesFixture,
    http_client: MockHttpClientFixture,
) -> Generator[BoundlessRequestsFixture]:
    fixture = BoundlessRequestsFixture(http_client)
    # Make sure we have a valid token before running tests.
    fixture.client.queue_response(
        200, content=boundless_files_fixture.sample_data("token.json")
    )
    yield fixture


class TestBoundlessRequests:
    def test___init__(self, boundless_requests: BoundlessRequestsFixture) -> None:
        # Test that if timeout is set to 0 in settings, it becomes None in requests.
        settings = boundless_requests.create_settings(timeout=0)
        requests = BoundlessRequests(settings)
        assert requests._timeout is None

        # If its set to a positive value, it remains the same.
        settings = boundless_requests.create_settings(timeout=5)
        requests = BoundlessRequests(settings)
        assert requests._timeout == 5

    @pytest.mark.parametrize(
        "content",
        [
            "",
            "<garbage><foo>bar</ga",
            "<foo>bar</foo>",
            "{}",
        ],
    )
    def test__request_bad_responses(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        content: str,
    ):
        boundless_requests.client.queue_response(200, content=content)
        with pytest.raises(BoundlessValidationError):
            boundless_requests.request(
                AddHoldResponse.from_xml,
            )

    @pytest.mark.parametrize(
        "func",
        [
            pytest.param(AddHoldResponse.from_xml, id="Fails validation"),
            pytest.param(RemoveHoldResponse.from_xml, id="Passes validation"),
        ],
    )
    def test__request_internal_server_error(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        func: Callable,
    ):
        data = boundless_files_fixture.sample_data("internal_server_error.xml")
        boundless_requests.client.queue_response(400, content=data)
        with pytest.raises(RemoteInitiatedServerError, match="Internal Server Error"):
            boundless_requests.request(
                func,
            )

    @pytest.mark.parametrize(
        "filename",
        [
            pytest.param("invalid_error_code.xml", id="Invalid Error Code"),
            pytest.param("missing_error_code.xml", id="Missing Error Code"),
        ],
    )
    def test__request_invalid_response(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        filename: str,
    ):
        data = boundless_files_fixture.sample_data(filename)
        boundless_requests.client.queue_response(400, content=data)
        with pytest.raises(BoundlessValidationError):
            boundless_requests.request(AudiobookMetadataResponse.model_validate_json)

    def test__request_timeout(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ):
        # When no timeout is set, the request goes out with the timeout configured in the settings.
        boundless_requests.client.queue_response(
            200, content=(boundless_files_fixture.sample_data("checkin_success.xml"))
        )
        boundless_requests.request(EarlyCheckinResponse.from_xml)
        assert (
            boundless_requests.client.requests_args[1]["timeout"]
            == boundless_requests.requests._timeout
        )

        # When a timeout is set, the request goes out with that timeout instead
        boundless_requests.client.reset_mock()
        boundless_requests.client.queue_response(
            200, content=(boundless_files_fixture.sample_data("checkin_success.xml"))
        )
        boundless_requests.request(EarlyCheckinResponse.from_xml, timeout=2)
        assert boundless_requests.client.requests_args[0]["timeout"] == 2

        # Even if that timeout is None
        boundless_requests.client.reset_mock()
        boundless_requests.client.queue_response(
            200, content=(boundless_files_fixture.sample_data("checkin_success.xml"))
        )
        boundless_requests.request(EarlyCheckinResponse.from_xml, timeout=None)
        assert boundless_requests.client.requests_args[0]["timeout"] is None

    @pytest.mark.parametrize(
        "filename",
        [
            "checkin_success.xml",
            "checkin_not_checked_out.xml",
        ],
    )
    def test_early_checkin_success(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        filename: str,
    ):
        data = boundless_files_fixture.sample_data(filename)
        boundless_requests.client.queue_response(200, content=data)
        boundless_requests.requests.early_checkin("title_id", "patron_id")

    def test_early_checkin_fail(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ):
        data = boundless_files_fixture.sample_data("checkin_failure.xml")
        boundless_requests.client.queue_response(404, content=data)
        with pytest.raises(NotFoundOnRemote):
            boundless_requests.requests.early_checkin("title_id", "patron_id")

    def test_checkout_success(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ):
        data = boundless_files_fixture.sample_data("checkout_success.xml")
        boundless_requests.client.queue_response(200, content=data)
        response = boundless_requests.requests.checkout(
            "title_id", "patron_id", "format"
        )
        assert response.expiration_date == datetime_utc(2015, 8, 11, 18, 57, 42)

    @pytest.mark.parametrize(
        "filename,exception",
        [
            ("already_checked_out.xml", AlreadyCheckedOut),
            ("not_found_on_remote.xml", NotFoundOnRemote),
        ],
    )
    def test_checkout_failures(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        filename: str,
        exception: type[Exception],
    ):
        data = boundless_files_fixture.sample_data(filename)
        boundless_requests.client.queue_response(400, content=data)
        with pytest.raises(exception):
            boundless_requests.requests.checkout("title_id", "patron_id", "format")

    def test_add_hold_success(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ):
        data = boundless_files_fixture.sample_data("place_hold_success.xml")
        boundless_requests.client.queue_response(200, content=data)
        response = boundless_requests.requests.add_hold("title_id", "patron_id", None)
        assert response.holds_queue_position == 1

    def test_add_hold_fail(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ):
        data = boundless_files_fixture.sample_data("already_on_hold.xml")
        boundless_requests.client.queue_response(400, content=data)
        with pytest.raises(AlreadyOnHold):
            boundless_requests.requests.add_hold("title_id", "patron_id", None)

    @pytest.mark.parametrize(
        "filename",
        [
            "release_hold_success.xml",
            "release_hold_failure.xml",
        ],
    )
    def test_remove_hold_success(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        filename: str,
    ):
        data = boundless_files_fixture.sample_data(filename)
        boundless_requests.client.queue_response(200, content=data)
        boundless_requests.requests.remove_hold("title_id", "patron_id")

    def test_audiobook_metadata_success(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ):
        data = boundless_files_fixture.sample_data("audiobook_metadata.json")
        boundless_requests.client.queue_response(200, content=data)
        response = boundless_requests.requests.audiobook_metadata("content_id")
        assert response.account_id == "BTTest"
        assert boundless_requests.client.requests_methods[1] == "POST"
        assert boundless_requests.client.requests_args[1]["params"] == {
            "fndcontentid": "content_id"
        }

    @pytest.mark.parametrize(
        "filename",
        [
            "ebook_fulfillment_info.json",
            "audiobook_fulfillment_info.json",
        ],
    )
    def test_fulfillment_info_success(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        filename: str,
    ):
        data = boundless_files_fixture.sample_data(filename)
        boundless_requests.client.queue_response(200, content=data)
        boundless_requests.requests.fulfillment_info("transaction_id")
        assert boundless_requests.client.requests_methods[1] == "POST"
        assert boundless_requests.client.requests_args[1]["params"] == {
            "TransactionID": "transaction_id"
        }

    @pytest.mark.parametrize(
        "filename",
        [
            "availability_with_loan_and_hold.xml",
            "availability_with_loans.xml",
            "tiny_collection.xml",
            "availability_patron_not_found.xml",
        ],
    )
    def test_availability_success(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        filename: str,
    ):
        data = boundless_files_fixture.sample_data(filename)
        boundless_requests.client.queue_response(200, content=data)
        boundless_requests.requests.availability()

        # The availability API request defaults to the timeout we give in settings.
        assert (
            boundless_requests.client.requests_args[1]["timeout"]
            == boundless_requests.settings.timeout
        )

    @pytest.mark.parametrize(
        "filename",
        [
            "availability_expired_token.xml",
            "availability_invalid_token.xml",
        ],
    )
    def test_availability_fail(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        filename: str,
    ):
        data = boundless_files_fixture.sample_data(filename)
        boundless_requests.client.queue_response(200, content=data)
        with pytest.raises(PatronAuthorizationFailedException):
            boundless_requests.requests.availability()

    def test_availability_exception(self, boundless_requests: BoundlessRequestsFixture):
        boundless_requests.client.queue_response(500)
        with pytest.raises(
            RemoteIntegrationException,
            match="Got status code 500 from external server, cannot continue.",
        ):
            boundless_requests.requests.availability()

    def test_refresh_bearer_token_after_401(
        self,
        boundless_requests: BoundlessRequestsFixture,
        boundless_files_fixture: BoundlessFilesFixture,
    ):
        # If we get a 401, we will fetch a new bearer token and try the
        # request again.
        boundless_requests.client.queue_response(401)
        boundless_requests.client.queue_response(
            200, content=boundless_files_fixture.sample_data("token.json")
        )
        boundless_requests.client.queue_response(
            200, content=boundless_files_fixture.sample_data("checkout_success.xml")
        )
        boundless_requests.requests.checkout("title_id", "patron_id", "format")

        # We made four requests:
        # 1. The initial request to initialize the token.
        # 2. The checkout request that returned a 401.
        # 3. The request to refresh the bearer token.
        # 4. The request that succeeded after refreshing the token.
        assert len(boundless_requests.client.requests) == 4

    def test_refresh_bearer_token_error(
        self, boundless_requests: BoundlessRequestsFixture
    ):
        # Raise an exception if we don't get a 200 status code when
        # refreshing the bearer token.
        boundless_requests.client.reset_mock()
        boundless_requests.client.queue_response(412)
        with pytest.raises(
            RemoteIntegrationException, match="Got status code 412 from external server"
        ):
            boundless_requests.requests.refresh_bearer_token()

    def test_bearer_token_only_refreshed_once_after_401(
        self,
        boundless_requests: BoundlessRequestsFixture,
        boundless_files_fixture: BoundlessFilesFixture,
    ):
        # If we get a 401 immediately after refreshing the token, we just
        # return the response instead of refreshing the token again.
        boundless_requests.client.queue_response(401)
        boundless_requests.client.queue_response(
            200, content=boundless_files_fixture.sample_data("token.json")
        )
        boundless_requests.client.queue_response(401, content="data")

        mock_response_callable = MagicMock()
        boundless_requests.request(mock_response_callable)
        mock_response_callable.assert_called_once_with(b"data")

    @pytest.mark.parametrize(
        "file, should_refresh",
        [
            pytest.param(None, True, id="no_message"),
            pytest.param("availability_invalid_token.xml", True, id="invalid_token"),
            pytest.param("availability_expired_token.xml", True, id="expired_token"),
            pytest.param(
                "availability_patron_not_found.xml", False, id="patron_not_found"
            ),
        ],
    )
    def test_refresh_bearer_token_based_on_token_status(
        self,
        boundless_requests: BoundlessRequestsFixture,
        boundless_files_fixture: BoundlessFilesFixture,
        file: str | None,
        should_refresh: bool,
    ):
        data = boundless_files_fixture.sample_data(file) if file else b""

        boundless_requests.client.queue_response(401, content=data)
        boundless_requests.client.queue_response(
            200, content=boundless_files_fixture.sample_data("token.json")
        )
        boundless_requests.client.queue_response(200, content="The data")
        mock_response_callable = MagicMock()
        boundless_requests.request(mock_response_callable)

        if should_refresh:
            mock_response_callable.assert_called_once_with(b"The data")
            assert len(boundless_requests.client.requests) == 4
        else:
            mock_response_callable.assert_called_once_with(data)
            assert len(boundless_requests.client.requests) == 2

    @pytest.mark.parametrize(
        "server_nickname,base_url,license_url",
        [
            pytest.param(
                ServerNickname.production,
                API_BASE_URLS[ServerNickname.production],
                LICENSE_SERVER_BASE_URLS[ServerNickname.production],
                id="production",
            ),
            pytest.param(
                ServerNickname.qa,
                API_BASE_URLS[ServerNickname.qa],
                LICENSE_SERVER_BASE_URLS[ServerNickname.qa],
                id="qa",
            ),
        ],
    )
    def test_integration_settings_url(
        self,
        boundless_requests: BoundlessRequestsFixture,
        server_nickname: str,
        base_url: str,
        license_url: str,
    ):
        settings = boundless_requests.create_settings(server_nickname=server_nickname)
        requests = BoundlessRequests(settings)
        assert requests._base_url == base_url
        assert requests._license_server_url == license_url

    def test_license(
        self,
        boundless_requests: BoundlessRequestsFixture,
        boundless_files_fixture: BoundlessFilesFixture,
    ):
        license_request = partial(
            boundless_requests.requests.license,
            "book_vault_uuid",
            "device_id",
            "client_id",
            "isbn",
            "modulus",
            "exponent",
        )

        boundless_requests.client.reset_mock()
        data = boundless_files_fixture.sample_data("license.json")
        boundless_requests.client.queue_response(200, content=data)
        response = license_request()

        assert response == json.loads(data)
        assert boundless_requests.client.requests_methods[0] == "GET"
        assert (
            "license/book_vault_uuid/device_id/client_id/isbn/modulus/exponent"
            in boundless_requests.client.requests[0]
        )

        # Test error handling
        boundless_requests.client.queue_response(500, content=b"")
        with pytest.raises(
            RemoteIntegrationException, match="Got status code 500 from external server"
        ):
            license_request()

        data = boundless_files_fixture.sample_data("license_internal_server_error.json")
        boundless_requests.client.queue_response(500, content=data)
        with pytest.raises(BoundlessLicenseError, match="Internal Server Error"):
            license_request()

        data = boundless_files_fixture.sample_data("license_invalid_isbn.json")
        boundless_requests.client.queue_response(500, content=data)
        with pytest.raises(BoundlessLicenseError, match="Invalid ISBN"):
            license_request()

    def test_title_license_single_item(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ) -> None:
        """Test retrieving a title license response with a single item."""
        data = boundless_files_fixture.sample_data("title_license_single_item.json")
        modified_since = datetime_utc(2024, 1, 1)

        # Queue the response
        boundless_requests.client.queue_response(200, content=data)

        # Make the request
        response = boundless_requests.requests.title_license(
            modified_since=modified_since,
            page=1,
        )

        # Verify request was made with correct parameters (index 1 because token request is 0)
        assert boundless_requests.client.requests_methods[1] == "GET"
        assert (
            boundless_requests.client.requests[1]
            == API_BASE_URLS[ServerNickname.production] + "titleLicense/v3"
        )
        assert boundless_requests.client.requests_args[1]["params"] == {
            "modifiedSince": "01-01-2024 00:00:00",
            "page": "1",
        }

        # Verify response parsing
        assert response.status.code == 0
        assert response.status.message == "Titles Retrieved Successfully."
        assert response.pagination.current_page == 1
        assert response.pagination.page_size == 500
        assert response.pagination.total_count == 1
        assert response.pagination.total_page == 1
        assert len(response.titles) == 1
        assert response.titles[0].title_id == "0009067251"
        assert response.titles[0].active is True

    def test_title_license_full_response(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ) -> None:
        """Test retrieving a full title license response with multiple items."""
        data = boundless_files_fixture.sample_data("title_license_full.json")
        modified_since = datetime_utc(2023, 6, 15, 14, 30, 0)

        # Queue the response
        boundless_requests.client.queue_response(200, content=data)

        # Make the request with custom page
        response = boundless_requests.requests.title_license(
            modified_since=modified_since,
            page=2,
        )

        # Verify request was made with correct parameters (index 1 because token request is 0)
        assert boundless_requests.client.requests_methods[1] == "GET"
        assert (
            boundless_requests.client.requests[1]
            == API_BASE_URLS[ServerNickname.production] + "titleLicense/v3"
        )
        assert boundless_requests.client.requests_args[1]["params"] == {
            "modifiedSince": "06-15-2023 14:30:00",
            "page": "2",
        }

        # Verify response parsing
        assert response.status.code == 0
        assert response.status.message == "Titles Retrieved Successfully."
        assert response.pagination.current_page == 2
        assert response.pagination.page_size == 500
        assert response.pagination.total_count == 133634
        assert response.pagination.total_page == 268
        assert len(response.titles) == 500

        # Verify titles contain both active and inactive
        active_titles = [t for t in response.titles if t.active]
        inactive_titles = [t for t in response.titles if not t.active]
        assert len(active_titles) == 499
        assert len(inactive_titles) == 1

    def test_title_license_no_results(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ) -> None:
        """Test retrieving a title license response with no results."""
        data = boundless_files_fixture.sample_data("title_license_no_results.json")
        modified_since = datetime_utc(2024, 12, 31, 23, 59, 59)

        # Queue the response
        boundless_requests.client.queue_response(200, content=data)

        # Make the request
        response = boundless_requests.requests.title_license(
            modified_since=modified_since,
        )

        # Verify request was made with correct parameters (index 1 because token request is 0)
        assert boundless_requests.client.requests_methods[1] == "GET"
        assert (
            boundless_requests.client.requests[1]
            == API_BASE_URLS[ServerNickname.production] + "titleLicense/v3"
        )
        assert boundless_requests.client.requests_args[1]["params"] == {
            "modifiedSince": "12-31-2024 23:59:59",
            "page": "1",
        }

        # Verify response parsing
        assert response.status.code == 0
        assert response.status.message == "Titles Retrieved Successfully."
        assert response.pagination.current_page == 1
        assert response.pagination.page_size == 500
        assert response.pagination.total_count == 0
        assert response.pagination.total_page == 0
        assert len(response.titles) == 0  # Should be empty list, not None

    @pytest.mark.parametrize(
        "filename, message",
        [
            ("title_license_error_response_pagination.json", "Invalid page number"),
            (
                "title_license_error_response_datetime.json",
                "Invalid inventory delta update datetime format",
            ),
            (
                "title_license_error_response_internal_server.json",
                "Internal Server Error",
            ),
        ],
    )
    def test_title_license_error(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
        filename: str,
        message: str,
    ) -> None:
        """Test retrieving a title license response with an error."""
        data = boundless_files_fixture.sample_data(filename)
        boundless_requests.client.queue_response(200, content=data)

        with pytest.raises(IntegrationException, match=message):
            boundless_requests.requests.title_license(
                modified_since=(datetime_utc(2024, 1, 1)),
            )

    def test_title_license_with_custom_timeout(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        boundless_requests: BoundlessRequestsFixture,
    ) -> None:
        """Test retrieving a title license response with a custom timeout."""
        data = boundless_files_fixture.sample_data("title_license_single_item.json")
        modified_since = datetime_utc(2024, 1, 1)

        # Queue the response
        boundless_requests.client.queue_response(200, content=data)

        # Make the request with custom timeout
        response = boundless_requests.requests.title_license(
            modified_since=modified_since,
            page=1,
            timeout=30,
        )

        # Verify the timeout was used in the request (index 1 because token request is 0)
        assert boundless_requests.client.requests_args[1]["timeout"] == 30

        # Verify response
        assert response.status.code == 0
        assert len(response.titles) == 1
