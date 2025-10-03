import pytest

from palace.manager.api.circulation.exceptions import (
    LibraryInvalidInputException,
    RemoteInitiatedServerError,
)
from palace.manager.integration.license.boundless.model.json import (
    AudiobookMetadataResponse,
    AxisNowFulfillmentInfoResponse,
    FindawayFulfillmentInfoResponse,
    FulfillmentInfoResponse,
    LicenseServerStatus,
    TitleLicenseResponse,
)
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.files import BoundlessFilesFixture


class TestFulfillmentInfoResponse:
    def test_audiobook_fulfillment_info(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        data = boundless_files_fixture.sample_data("audiobook_fulfillment_info.json")
        parsed = FulfillmentInfoResponse.validate_json(data)
        assert isinstance(parsed, FindawayFulfillmentInfoResponse)
        parsed.raise_on_error()

        assert parsed.status.code == 0
        assert parsed.status.message == "SUCCESS"

        assert parsed.content_id == "04960"
        assert parsed.license_id == "58ee81c6d3d8eb3b05597cdc"
        assert parsed.session_key == "0f547af1-38c1-4b1c-8a1a-169d353065d0"
        assert parsed.transaction_id == "5babb89b16a4ed7d8238f498"

        assert parsed.expiration_date == datetime_utc(2018, 9, 29, 18, 34, 0, 139)

    def test_ebook_fulfillment_info(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        data = boundless_files_fixture.sample_data("ebook_fulfillment_info.json")
        parsed = FulfillmentInfoResponse.validate_json(data)
        assert isinstance(parsed, AxisNowFulfillmentInfoResponse)
        parsed.raise_on_error()

        assert parsed.status.code == 0
        assert parsed.status.message == "SUCCESS"

        assert parsed.isbn == "9780547351551"
        assert parsed.expiration_date == datetime_utc(2018, 9, 29, 18, 34, 0, 139)
        assert parsed.book_vault_uuid == "1c11c31f-81c2-41bb-9179-491114c3f121"


class TestAudiobookMetadataResponse:
    def test_audiobook_metadata(self, boundless_files_fixture: BoundlessFilesFixture):
        data = boundless_files_fixture.sample_data("audiobook_metadata.json")
        parsed = AudiobookMetadataResponse.model_validate_json(data)
        parsed.raise_on_error()

        assert parsed.status.code == 0
        assert parsed.status.message == "SUCCESS"

        assert parsed.account_id == "BTTest"
        assert len(parsed.reading_order) == 5

        assert parsed.reading_order[0].title == "Track 0"
        assert parsed.reading_order[0].duration == 2.89
        assert parsed.reading_order[0].part == 0
        assert parsed.reading_order[0].sequence == 0


class TestLicenseServerStatus:
    def test_license_invalid_isbn(self, boundless_files_fixture: BoundlessFilesFixture):
        data = boundless_files_fixture.sample_data("license_invalid_isbn.json")
        parsed = LicenseServerStatus.model_validate_json(data)

        assert parsed.code == 9400
        assert parsed.title == "Invalid ISBN"
        assert parsed.message == "ISBN KeyId association does not exist."

    def test_license_internal_server_error(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        data = boundless_files_fixture.sample_data("license_internal_server_error.json")
        parsed = LicenseServerStatus.model_validate_json(data)

        assert parsed.code == 500
        assert parsed.title == "Internal Server Error"
        assert parsed.message == "Unexpected error occurred."


class TestTitleLicenseResponse:
    def test_parse_single_item(self, boundless_files_fixture: BoundlessFilesFixture):
        """Test parsing a title license response with a single item."""
        data = boundless_files_fixture.sample_data("title_license_single_item.json")
        parsed = TitleLicenseResponse.model_validate_json(data)

        # Test status
        assert parsed.status.code == 0
        assert parsed.status.message == "Titles Retrieved Successfully."

        # Does not raise an error
        parsed.raise_on_error()

        # Test pagination
        assert parsed.pagination.current_page == 1
        assert parsed.pagination.page_size == 500
        assert parsed.pagination.total_count == 1
        assert parsed.pagination.total_page == 1

        # Test titles
        assert parsed.titles is not None
        assert len(parsed.titles) == 1

        title = parsed.titles[0]
        assert title.title_id == "0009067251"
        assert title.active is True

    def test_parse_full_response(self, boundless_files_fixture: BoundlessFilesFixture):
        """Test parsing a full title license response with multiple items."""
        data = boundless_files_fixture.sample_data("title_license_full.json")
        parsed = TitleLicenseResponse.model_validate_json(data)

        # Test status
        assert parsed.status.code == 0
        assert parsed.status.message == "Titles Retrieved Successfully."

        # Test pagination
        assert parsed.pagination.current_page == 2
        assert parsed.pagination.page_size == 500
        assert parsed.pagination.total_count == 133634
        assert parsed.pagination.total_page == 268

        # Test titles
        assert parsed.titles is not None
        assert len(parsed.titles) == 500

        # Verify first title details
        first_title = parsed.titles[0]
        assert first_title.title_id == "0008976000"
        assert first_title.active is True

        # Verify the correct number of active and inactive titles
        active_titles = [title for title in parsed.titles if title.active]
        inactive_titles = [title for title in parsed.titles if not title.active]
        assert len(active_titles) == 499
        assert len(inactive_titles) == 1

    def test_parse_no_results(self, boundless_files_fixture: BoundlessFilesFixture):
        """Test parsing a full title license response with multiple items."""
        data = boundless_files_fixture.sample_data("title_license_no_results.json")
        parsed = TitleLicenseResponse.model_validate_json(data)

        # Test status
        assert parsed.status.code == 0
        assert parsed.status.message == "Titles Retrieved Successfully."

        # Test pagination
        assert parsed.pagination.current_page == 1
        assert parsed.pagination.page_size == 500
        assert parsed.pagination.total_count == 0
        assert parsed.pagination.total_page == 0

        # Test titles
        assert parsed.titles is not None
        assert len(parsed.titles) == 0

    def test_parse_error_response_pagination(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        """Test parsing an error response with null titles."""
        data = boundless_files_fixture.sample_data(
            "title_license_error_response_pagination.json"
        )
        parsed = TitleLicenseResponse.model_validate_json(data)

        # Test error status
        assert parsed.status.code == 3131
        assert parsed.status.message == "Invalid page number"

        # Test pagination still exists
        assert parsed.pagination.current_page == 100
        assert parsed.pagination.page_size == 500
        assert parsed.pagination.total_count == 48418
        assert parsed.pagination.total_page == 97

        # Test titles is empty list
        assert len(parsed.titles) == 0

        # Test error handling
        with pytest.raises(RemoteInitiatedServerError, match="Invalid page number"):
            parsed.raise_on_error()

    def test_parse_error_response_datetime(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        """Test parsing an error response with null pagination."""
        data = boundless_files_fixture.sample_data(
            "title_license_error_response_datetime.json"
        )
        parsed = TitleLicenseResponse.model_validate_json(data)

        # Test error status
        assert parsed.status.code == 3132
        assert parsed.status.message == "Invalid inventory delta update datetime format"

        # Test pagination still exists
        assert parsed.pagination.current_page == 0
        assert parsed.pagination.page_size == 0
        assert parsed.pagination.total_count == 0
        assert parsed.pagination.total_page == 0

        # Test titles is empty list
        assert len(parsed.titles) == 0

        # Test error handling
        with pytest.raises(
            LibraryInvalidInputException,
            match="Invalid inventory delta update datetime format",
        ):
            parsed.raise_on_error()

    def test_parse_error_response_internal_server(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        """Test parsing of an internal server error response."""
        data = boundless_files_fixture.sample_data(
            "title_license_error_response_internal_server.json"
        )
        parsed = TitleLicenseResponse.model_validate_json(data)

        # Test error status
        assert parsed.status.code == 5000
        assert parsed.status.message == "Internal Server Error"

        # Test pagination still exists
        assert parsed.pagination.current_page == 0
        assert parsed.pagination.page_size == 0
        assert parsed.pagination.total_count == 0
        assert parsed.pagination.total_page == 0

        # Test titles is empty list
        assert len(parsed.titles) == 0

        # Test error handling
        with pytest.raises(RemoteInitiatedServerError, match="Internal Server Error"):
            parsed.raise_on_error()
