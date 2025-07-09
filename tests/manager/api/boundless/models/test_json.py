from palace.manager.api.boundless.models.json import (
    AudiobookMetadataResponse,
    AxisNowFulfillmentInfoResponse,
    FindawayFulfillmentInfoResponse,
    FulfillmentInfoResponse,
    LicenseServerStatus,
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
