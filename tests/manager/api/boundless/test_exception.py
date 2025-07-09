import json

import pytest

from palace.manager.api.boundless.exception import (
    BoundlessLicenseError,
    StatusResponseParser,
)
from palace.manager.api.boundless.models.json import LicenseServerStatus
from palace.manager.api.circulation.exceptions import (
    NotFoundOnRemote,
    RemoteInitiatedServerError,
)
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.files import BoundlessFilesFixture


class TestBoundlessLicenseError:
    @pytest.mark.parametrize(
        "status_code, file_name",
        [
            pytest.param(405, "license_invalid_isbn.json", id="invalid_isbn"),
            pytest.param(
                500, "license_internal_server_error.json", id="internal_server_error"
            ),
        ],
    )
    def test_problem_detail(
        self,
        boundless_files_fixture: BoundlessFilesFixture,
        status_code: int,
        file_name: str,
    ):
        # Test that the Boundless360LicenseError generates a ProblemDetail containing the information
        # from the LicenseServerStatus document.
        status_doc = LicenseServerStatus.model_validate_json(
            boundless_files_fixture.sample_data(file_name)
        )
        error = BoundlessLicenseError(status_doc, status_code)
        assert error.problem_detail == ProblemDetail(
            uri=f"http://palaceproject.io/terms/problem/boundless/{status_doc.code}",
            status_code=status_code,
            title=status_doc.title,
            detail=status_doc.message,
        )


class TestStatusResponseParser:
    def test_parser_xml(self, boundless_files_fixture: BoundlessFilesFixture):
        data = boundless_files_fixture.sample_data("availability_patron_not_found.xml")
        parsed = StatusResponseParser.parse(data)
        assert parsed is not None
        assert parsed.code == 3122
        assert parsed.message == "Patron information is not found."

        data = boundless_files_fixture.sample_data("availability_with_loans.xml")
        parsed = StatusResponseParser.parse(data)
        assert parsed is not None
        assert parsed.code == 0
        assert parsed.message == "Availability Data is Successfully retrieved."

        data = boundless_files_fixture.sample_data(
            "availability_with_ebook_fulfillment.xml"
        )
        parsed = StatusResponseParser.parse(data)
        assert parsed is not None
        assert parsed.code == 0
        assert parsed.message == "Availability Data is Successfully retrieved."

        data = boundless_files_fixture.sample_data("checkin_failure.xml")
        parsed = StatusResponseParser.parse(data)
        assert parsed is not None
        assert parsed.code == 3103
        assert parsed.message == "Invalid Title Id"

        data = boundless_files_fixture.sample_data("invalid_error_code.xml")
        parsed = StatusResponseParser.parse(data)
        assert parsed is None

        data = boundless_files_fixture.sample_data("missing_error_code.xml")
        parsed = StatusResponseParser.parse(data)
        assert parsed is None

        data = boundless_files_fixture.sample_data(
            "checkout_success_no_status_message.xml"
        )
        parsed = StatusResponseParser.parse(data)
        assert parsed is not None
        assert parsed.code == 0
        assert parsed.message is None

    def test_parser_bad_data(self, boundless_files_fixture: BoundlessFilesFixture):
        # Test with None and empty data
        assert StatusResponseParser.parse(None) is None  # type: ignore[arg-type]
        assert StatusResponseParser.parse(b"") is None
        assert StatusResponseParser.parse(b"not xml") is None
        assert StatusResponseParser.parse(b"<bad") is None
        assert StatusResponseParser.parse(b"{") is None
        assert StatusResponseParser.parse("🔥🗑️".encode()) is None

    def test_parser_json(self, boundless_files_fixture: BoundlessFilesFixture):
        data = boundless_files_fixture.sample_data("audiobook_metadata.json")
        parsed = StatusResponseParser.parse(data)
        assert parsed is not None
        assert parsed.code == 0
        assert parsed.message == "SUCCESS"

        data = boundless_files_fixture.sample_data("audiobook_fulfillment_info.json")
        parsed = StatusResponseParser.parse(data)
        assert parsed is not None
        assert parsed.code == 0
        assert parsed.message == "SUCCESS"

        data = boundless_files_fixture.sample_data("ebook_fulfillment_info.json")
        parsed = StatusResponseParser.parse(data)
        assert parsed is not None
        assert parsed.code == 0
        assert parsed.message == "SUCCESS"

        parsed = StatusResponseParser.parse(json.dumps({}).encode())
        assert parsed is None

        parsed = StatusResponseParser.parse(json.dumps({"Status": {}}).encode())
        assert parsed is None

        parsed = StatusResponseParser.parse(
            json.dumps({"Status": {"Code": "Bad Code", "Message": "Wow"}}).encode()
        )
        assert parsed is None

        parsed = StatusResponseParser.parse(
            json.dumps({"Status": {"Code": "123"}}).encode()
        )
        assert parsed is not None
        assert parsed.code == 123
        assert parsed.message is None

        parsed = StatusResponseParser.parse(
            json.dumps({"Status": {"Message": "Wow"}}).encode()
        )
        assert parsed is None

        parsed = StatusResponseParser.parse(
            json.dumps({"Status": {"Code": "123", "Message": "Wow"}}).encode()
        )
        assert parsed is not None
        assert parsed.code == 123
        assert parsed.message == "Wow"

    def test_parse_and_raise(self, boundless_files_fixture: BoundlessFilesFixture):
        assert StatusResponseParser.parse_and_raise(b"") is None

        data = boundless_files_fixture.sample_data("availability_patron_not_found.xml")
        assert (
            3122,
            "Patron information is not found.",
        ) == StatusResponseParser.parse_and_raise(data)

        data = boundless_files_fixture.sample_data("checkin_failure.xml")
        with pytest.raises(NotFoundOnRemote):
            StatusResponseParser.parse_and_raise(data)

        data = boundless_files_fixture.sample_data("internal_server_error.xml")
        with pytest.raises(RemoteInitiatedServerError, match="Internal Server Error"):
            StatusResponseParser.parse_and_raise(data)

    def test_ignore_error_codes(self) -> None:
        # By default, this will raise an exception for error code 5000.
        with pytest.raises(RemoteInitiatedServerError):
            StatusResponseParser.raise_on_error(5000, "Internal Server Error")

        # However if we ignore this error code, no exception is raised.
        StatusResponseParser.raise_on_error(
            5000, "Internal Server Error", ignore_error_codes=[5000]
        )

    def test_custom_error_classes(self) -> None:
        # By default, this will raise an exception for error code 5000.
        class CustomError(RemoteInitiatedServerError): ...

        with pytest.raises(RemoteInitiatedServerError):
            StatusResponseParser.raise_on_error(5000, "Internal Server Error")

        # We can provide a custom error class for this error code.
        with pytest.raises(CustomError):
            StatusResponseParser.raise_on_error(
                5000, "Internal Server Error", custom_error_classes={5000: CustomError}
            )

        # We can also provide a custom error class that only applies to a specific error message.
        with pytest.raises(RemoteInitiatedServerError):
            StatusResponseParser.raise_on_error(
                5000,
                "Internal Server Error",
                custom_error_classes={(5000, "Uh oh"): CustomError},
            )

        with pytest.raises(CustomError):
            StatusResponseParser.raise_on_error(
                5000, "Uh oh", custom_error_classes={(5000, "Uh oh"): CustomError}
            )
