from __future__ import annotations

from collections.abc import Callable, Generator
from functools import partial
from unittest.mock import MagicMock

import pytest

from palace.manager.api.axis.exception import Axis360ValidationError
from palace.manager.api.axis.models.json import AudiobookMetadataResponse
from palace.manager.api.axis.models.xml import AddHoldResponse, RemoveHoldResponse
from palace.manager.api.axis.requests import Axis360Requests
from palace.manager.api.axis.settings import Axis360Settings
from palace.manager.api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    NotFoundOnRemote,
    PatronAuthorizationFailedException,
    RemoteInitiatedServerError,
)
from palace.manager.util.datetime_helpers import datetime_utc
from palace.manager.util.http import RemoteIntegrationException
from tests.fixtures.files import AxisFilesFixture
from tests.fixtures.http import MockHttpClientFixture


class Axis360RequestsFixture:
    def __init__(self, http_client: MockHttpClientFixture) -> None:
        self.create_settings = partial(
            Axis360Settings,
            external_account_id="test_library_id",
            username="test_username",
            password="test_password",
            url="http://axis360apiqa.baker-taylor.com/Services/VendorAPI/",
        )
        self.settings = self.create_settings()
        self.requests = Axis360Requests(self.settings)
        self.client = http_client
        self.request = partial(self.requests._request, "GET", "endpoint")


@pytest.fixture
def axis360_requests(
    axis_files_fixture: AxisFilesFixture,
    http_client: MockHttpClientFixture,
) -> Generator[Axis360RequestsFixture]:
    fixture = Axis360RequestsFixture(http_client)
    # Make sure we have a valid token before running tests.
    fixture.client.queue_response(
        200, content=axis_files_fixture.sample_data("token.json")
    )
    yield fixture


class TestAxis360Requests:
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
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
        content: str,
    ):
        axis360_requests.client.queue_response(200, content=content)
        with pytest.raises(Axis360ValidationError):
            axis360_requests.request(
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
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
        func: Callable,
    ):
        data = axis_files_fixture.sample_data("internal_server_error.xml")
        axis360_requests.client.queue_response(400, content=data)
        with pytest.raises(RemoteInitiatedServerError, match="Internal Server Error"):
            axis360_requests.request(
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
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
        filename: str,
    ):
        data = axis_files_fixture.sample_data(filename)
        axis360_requests.client.queue_response(400, content=data)
        with pytest.raises(Axis360ValidationError):
            axis360_requests.request(AudiobookMetadataResponse.model_validate_json)

    @pytest.mark.parametrize(
        "filename",
        [
            "checkin_success.xml",
            "checkin_not_checked_out.xml",
        ],
    )
    def test_early_checkin_success(
        self,
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
        filename: str,
    ):
        data = axis_files_fixture.sample_data(filename)
        axis360_requests.client.queue_response(200, content=data)
        axis360_requests.requests.early_checkin("title_id", "patron_id")

    def test_early_checkin_fail(
        self,
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
    ):
        data = axis_files_fixture.sample_data("checkin_failure.xml")
        axis360_requests.client.queue_response(404, content=data)
        with pytest.raises(NotFoundOnRemote):
            axis360_requests.requests.early_checkin("title_id", "patron_id")

    def test_checkout_success(
        self,
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
    ):
        data = axis_files_fixture.sample_data("checkout_success.xml")
        axis360_requests.client.queue_response(200, content=data)
        response = axis360_requests.requests.checkout("title_id", "patron_id", "format")
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
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
        filename: str,
        exception: type[Exception],
    ):
        data = axis_files_fixture.sample_data(filename)
        axis360_requests.client.queue_response(400, content=data)
        with pytest.raises(exception):
            axis360_requests.requests.checkout("title_id", "patron_id", "format")

    def test_add_hold_success(
        self,
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
    ):
        data = axis_files_fixture.sample_data("place_hold_success.xml")
        axis360_requests.client.queue_response(200, content=data)
        response = axis360_requests.requests.add_hold("title_id", "patron_id", None)
        assert response.holds_queue_position == 1

        # Make sure the checkout request doesn't set a timeout
        assert "timeout" not in axis360_requests.client.requests_args[1]

    def test_add_hold_fail(
        self,
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
    ):
        data = axis_files_fixture.sample_data("already_on_hold.xml")
        axis360_requests.client.queue_response(400, content=data)
        with pytest.raises(AlreadyOnHold):
            axis360_requests.requests.add_hold("title_id", "patron_id", None)

    @pytest.mark.parametrize(
        "filename",
        [
            "release_hold_success.xml",
            "release_hold_failure.xml",
        ],
    )
    def test_remove_hold_success(
        self,
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
        filename: str,
    ):
        data = axis_files_fixture.sample_data(filename)
        axis360_requests.client.queue_response(200, content=data)
        axis360_requests.requests.remove_hold("title_id", "patron_id")

    def test_audiobook_metadata_success(
        self,
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
    ):
        data = axis_files_fixture.sample_data("audiobook_metadata.json")
        axis360_requests.client.queue_response(200, content=data)
        response = axis360_requests.requests.audiobook_metadata("content_id")
        assert response.account_id == "BTTest"
        assert axis360_requests.client.requests_methods[1] == "POST"
        assert axis360_requests.client.requests_args[1]["params"] == {
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
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
        filename: str,
    ):
        data = axis_files_fixture.sample_data(filename)
        axis360_requests.client.queue_response(200, content=data)
        axis360_requests.requests.fulfillment_info("transaction_id")
        assert axis360_requests.client.requests_methods[1] == "POST"
        assert axis360_requests.client.requests_args[1]["params"] == {
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
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
        filename: str,
    ):
        data = axis_files_fixture.sample_data(filename)
        axis360_requests.client.queue_response(200, content=data)
        axis360_requests.requests.availability()

        # The availability API request has no timeout set, because it
        # may take time proportionate to the total size of the
        # collection.
        assert axis360_requests.client.requests_args[1]["timeout"] is None

    @pytest.mark.parametrize(
        "filename",
        [
            "availability_expired_token.xml",
            "availability_invalid_token.xml",
        ],
    )
    def test_availability_fail(
        self,
        axis_files_fixture: AxisFilesFixture,
        axis360_requests: Axis360RequestsFixture,
        filename: str,
    ):
        data = axis_files_fixture.sample_data(filename)
        axis360_requests.client.queue_response(200, content=data)
        with pytest.raises(PatronAuthorizationFailedException):
            axis360_requests.requests.availability()

    def test_availability_exception(self, axis360_requests: Axis360RequestsFixture):
        axis360_requests.client.queue_response(500)
        with pytest.raises(
            RemoteIntegrationException,
            match="Got status code 500 from external server, cannot continue.",
        ):
            axis360_requests.requests.availability()

    def test_refresh_bearer_token_after_401(
        self,
        axis360_requests: Axis360RequestsFixture,
        axis_files_fixture: AxisFilesFixture,
    ):
        # If we get a 401, we will fetch a new bearer token and try the
        # request again.
        axis360_requests.client.queue_response(401)
        axis360_requests.client.queue_response(
            200, content=axis_files_fixture.sample_data("token.json")
        )
        axis360_requests.client.queue_response(
            200, content=axis_files_fixture.sample_data("checkout_success.xml")
        )
        axis360_requests.requests.checkout("title_id", "patron_id", "format")

        # We made four requests:
        # 1. The initial request to initialize the token.
        # 2. The checkout request that returned a 401.
        # 3. The request to refresh the bearer token.
        # 4. The request that succeeded after refreshing the token.
        assert len(axis360_requests.client.requests) == 4

    def test_refresh_bearer_token_error(self, axis360_requests: Axis360RequestsFixture):
        # Raise an exception if we don't get a 200 status code when
        # refreshing the bearer token.
        axis360_requests.client.reset_mock()
        axis360_requests.client.queue_response(412)
        with pytest.raises(
            RemoteIntegrationException, match="Got status code 412 from external server"
        ):
            axis360_requests.requests.refresh_bearer_token()

    def test_bearer_token_only_refreshed_once_after_401(
        self,
        axis360_requests: Axis360RequestsFixture,
        axis_files_fixture: AxisFilesFixture,
    ):
        # If we get a 401 immediately after refreshing the token, we just
        # return the response instead of refreshing the token again.
        axis360_requests.client.queue_response(401)
        axis360_requests.client.queue_response(
            200, content=axis_files_fixture.sample_data("token.json")
        )
        axis360_requests.client.queue_response(401, content="data")

        mock_response_callable = MagicMock()
        axis360_requests.request(mock_response_callable)
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
        axis360_requests: Axis360RequestsFixture,
        axis_files_fixture: AxisFilesFixture,
        file: str | None,
        should_refresh: bool,
    ):
        data = axis_files_fixture.sample_data(file) if file else b""

        axis360_requests.client.queue_response(401, content=data)
        axis360_requests.client.queue_response(
            200, content=axis_files_fixture.sample_data("token.json")
        )
        axis360_requests.client.queue_response(200, content="The data")
        mock_response_callable = MagicMock()
        axis360_requests.request(mock_response_callable)

        if should_refresh:
            mock_response_callable.assert_called_once_with(b"The data")
            assert len(axis360_requests.client.requests) == 4
        else:
            mock_response_callable.assert_called_once_with(data)
            assert len(axis360_requests.client.requests) == 2

    @pytest.mark.parametrize(
        ("setting_value", "attribute_value"),
        [
            (None, True),
            (True, True),
            (False, False),
        ],
    )
    def test_integration_settings(
        self,
        axis360_requests: Axis360RequestsFixture,
        setting_value: bool | None,
        attribute_value: bool,
    ):
        settings = axis360_requests.create_settings(verify_certificate=setting_value)
        requests = Axis360Requests(settings)
        assert requests._verify_certificate == attribute_value

    @pytest.mark.parametrize(
        "setting_value,expected",
        [
            (
                "production",
                Axis360Requests.SERVER_NICKNAMES["production"],
            ),
            ("qa", Axis360Requests.SERVER_NICKNAMES["qa"]),
            ("http://any.url.will.do", "http://any.url.will.do/"),
        ],
    )
    def test_integration_settings_url(
        self,
        axis360_requests: Axis360RequestsFixture,
        setting_value: str,
        expected: str | None,
    ):
        settings = axis360_requests.create_settings(url=setting_value)
        requests = Axis360Requests(settings)
        assert requests._base_url == expected
