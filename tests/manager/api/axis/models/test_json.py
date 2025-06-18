import datetime

from freezegun import freeze_time

from palace.manager.api.axis.models.json import (
    AudiobookMetadataResponse,
    AxisNowFulfillmentInfoResponse,
    FindawayFulfillmentInfoResponse,
    FulfillmentInfoResponse,
    LicenseServerStatus,
    Token,
)
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.files import AxisFilesFixture


class TestFulfillmentInfoResponse:
    def test_audiobook_fulfillment_info(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("audiobook_fulfillment_info.json")
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

    def test_ebook_fulfillment_info(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("ebook_fulfillment_info.json")
        parsed = FulfillmentInfoResponse.validate_json(data)
        assert isinstance(parsed, AxisNowFulfillmentInfoResponse)
        parsed.raise_on_error()

        assert parsed.status.code == 0
        assert parsed.status.message == "SUCCESS"

        assert parsed.isbn == "9780547351551"
        assert parsed.expiration_date == datetime_utc(2018, 9, 29, 18, 34, 0, 139)
        assert parsed.book_vault_uuid == "1c11c31f-81c2-41bb-9179-491114c3f121"


class TestAudiobookMetadataResponse:
    def test_audiobook_metadata(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("audiobook_metadata.json")
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


class TestToken:
    def test_token_response(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("token.json")
        parsed = Token.model_validate_json(data)

        assert parsed.access_token == (
            "gAAAACHFFENqnHA709FcDPNQCaaTRE3bJoI-gODKxUm9_LvZd837GmqzgMA4WnLQGI"
            "HunW1PatTuSx3ECPSxidxx3TNtm11a8tsas2S1Qy0hv2QPAYiVLuTQ9u1Wc8f9jL08"
            "nVS5PHPrBZG5zyq9NvkzAch9bCRyEYLDxxZvrK634y1hNAEAAIAAAABSwHdTefBbCP"
            "id1-9RUcDWY3gTNLu-qvOkUnGvAj-0W8aSt84hB2bMFMuB5c7KVR-2j2yM3dW2ICJI"
            "cKP2JRrLdn7k9j6L0z1Ia5lnW2hmKEPoXDF7l8q891JwPk62sVksIupD1kWqpzrbGr"
            "txnZynj2h6-WGueukOLYIqbNPZQkNRf-LsXUGOOzDjVis9WtHNc2QOCxu0YgU6N00Y"
            "R-6j2yoOcQ3qDq_hK_WwZq2S_W_k2UqhpHcWljiOFstqEWzxh44MYOsokQuIcU1TS8"
            "GClyS_YuSrK1SI9tBx1aj9vGq6WzaxTXMaG8Wx_wtm_9MPi743y0Hs5whaBZlYs2-K"
            "SBKbQtiKPx7bbVEjXpfCrXq_eVukYsw6fNjI12B0M8rZm1TB9TDRnOyg1Mkhn0C-"
        )
        assert parsed.token_type == "Bearer"
        assert parsed.expires_in == 600
        assert not parsed.expired

    def test_token_expired(self) -> None:
        with freeze_time() as frozen_time:
            token = Token(access_token="token", token_type="Bearer", expires_in=400)
            assert not token.expired

            frozen_time.tick(delta=datetime.timedelta(seconds=400))
            assert token.expired


class TestLicenseServerStatus:
    def test_license_invalid_isbn(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("license_invalid_isbn.json")
        parsed = LicenseServerStatus.model_validate_json(data)

        assert parsed.code == 9400
        assert parsed.title == "Invalid ISBN"
        assert parsed.message == "ISBN KeyId association does not exist."

    def test_license_internal_server_error(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("license_internal_server_error.json")
        parsed = LicenseServerStatus.model_validate_json(data)

        assert parsed.code == 500
        assert parsed.title == "Internal Server Error"
        assert parsed.message == "Unexpected error occurred."
