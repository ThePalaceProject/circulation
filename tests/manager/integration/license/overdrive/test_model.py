import json
from datetime import datetime
from functools import partial
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from httpx import Headers

from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotRenew,
    NoAvailableCopies,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.integration.license.overdrive.exception import (
    ExtraFieldsError,
    InvalidFieldOptionError,
    MissingRequiredFieldError,
    MissingSubstitutionsError,
    NotFoundError,
    OverdriveResponseException,
)
from palace.manager.integration.license.overdrive.model import (
    Action,
    ActionField,
    Checkout,
    Checkouts,
    ErrorResponse,
    Format,
    Hold,
    Holds,
    LinkTemplate,
    PatronInformation,
)
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import ResponseData
from tests.fixtures.files import OverdriveFilesFixture


class ErrorResponseFixture:
    def __init__(self) -> None: ...

    def mock_response(self, *, status_code: int = 500, content: str) -> ResponseData:
        return ResponseData(
            status_code,
            url="http://example.com/api/endpoint",
            headers=Headers(),
            text=content,
            content=content.encode("utf-8"),
            extensions={},
        )

    def mock_error(
        self,
        error_code: str,
        error_message: str | None = None,
        token: str | None = None,
    ) -> ResponseData:
        error_response = ErrorResponse(
            error_code=error_code,
            message=error_message or "An error has occurred",
            token=token or str(uuid4()),
        )
        return self.mock_response(content=error_response.model_dump_json())


@pytest.fixture
def error_response_fixture() -> ErrorResponseFixture:
    return ErrorResponseFixture()


class TestErrorResponse:
    def test_bad_data(
        self,
        caplog: pytest.LogCaptureFixture,
        error_response_fixture: ErrorResponseFixture,
    ) -> None:
        # With non-json data, we should just get a generic OverdriveResponseException.
        response = error_response_fixture.mock_response(content="not json data! 💣")
        with pytest.raises(
            OverdriveResponseException, match="default message"
        ) as exc_info:
            ErrorResponse.raise_from_response(response, "default message")
        assert exc_info.value.error_code is None
        assert exc_info.value.error_message == "default message"
        assert exc_info.value.response.status_code == response.status_code
        assert exc_info.value.response.url == response.url
        assert exc_info.value.response.text == response.text

        # The error is logged.
        assert "Error parsing Overdrive response" in caplog.text

        # If no default message is supplied, we should get a generic message.
        with pytest.raises(OverdriveResponseException, match="Unknown Overdrive error"):
            ErrorResponse.raise_from_response(response)

        # Malformed error document should also raise a generic OverdriveResponseException.
        response = error_response_fixture.mock_response(
            content=json.dumps({"errorCode": ["Complete nonsense", 12, 52]})
        )
        with pytest.raises(OverdriveResponseException, match="Unknown Overdrive error"):
            ErrorResponse.raise_from_response(response)

    def test_checkout_errors(
        self, error_response_fixture: ErrorResponseFixture
    ) -> None:
        # Errors not specifically known become generic OverdriveResponseException exceptions.
        with pytest.raises(OverdriveResponseException, match="Weird error") as exc_info:
            ErrorResponse.raise_from_response(
                error_response_fixture.mock_error("WeirdError", "Weird error", "token")
            )
        assert exc_info.value.error_code == "WeirdError"
        assert exc_info.value.error_message == "Weird error"
        assert exc_info.value.token == "token"

        # Some known errors become specific subclasses of CannotLoan.
        with pytest.raises(PatronLoanLimitReached):
            ErrorResponse.raise_from_response(
                error_response_fixture.mock_error("PatronHasExceededCheckoutLimit")
            )

        with pytest.raises(PatronLoanLimitReached):
            ErrorResponse.raise_from_response(
                error_response_fixture.mock_error(
                    "PatronHasExceededCheckoutLimit_ForCPC"
                )
            )

        with pytest.raises(NoAvailableCopies):
            ErrorResponse.raise_from_response(
                error_response_fixture.mock_error("NoCopiesAvailable")
            )

        with pytest.raises(AlreadyCheckedOut):
            ErrorResponse.raise_from_response(
                error_response_fixture.mock_error("TitleAlreadyCheckedOut")
            )

    def test_process_place_hold_response(
        self, error_response_fixture: ErrorResponseFixture
    ):
        # Some error messages result in specific CirculationExceptions.
        with pytest.raises(CannotRenew):
            ErrorResponse.raise_from_response(
                error_response_fixture.mock_error("NotWithinRenewalWindow")
            )
        with pytest.raises(PatronHoldLimitReached):
            ErrorResponse.raise_from_response(
                error_response_fixture.mock_error("PatronExceededHoldLimit")
            )
        with pytest.raises(AlreadyOnHold):
            ErrorResponse.raise_from_response(
                error_response_fixture.mock_error("AlreadyOnWaitList")
            )

    def test_real_errors(self, overdrive_files_fixture: OverdriveFilesFixture) -> None:
        # Test an auth error, which has a slightly different format in some cases.
        response = ErrorResponse.model_validate_json(
            overdrive_files_fixture.sample_data("patron_token_failed.json")
        )
        assert response.error_code == "unauthorized_client"
        assert response.message == "Invalid Library Card: 123456.  Not a valid card."
        assert response.token is None

        response = ErrorResponse.model_validate_json(
            overdrive_files_fixture.sample_data("overdrive_availability_not_found.json")
        )
        assert response.error_code == "NotFound"
        assert response.message == "The requested resource could not be found."
        assert response.token == "60a18218-0d25-42b8-80c3-0bf9df782f1b"

        response = ErrorResponse.model_validate_json(
            overdrive_files_fixture.sample_data("lock_in_format_not_available.json")
        )
        assert response.error_code == "PatronTitleProcessingFailed"
        assert (
            response.message
            == "The selected format may not be available for this title."
        )
        assert response.token == "bf3b1876-20fa-4755-a923-acc809740002"


class TestLinkTemplate:
    def test_template(self) -> None:
        template = LinkTemplate(
            href="http://example.com/{foo}/{bar}", type="application/json"
        )
        assert template.href == "http://example.com/{foo}/{bar}"
        assert template.type == "application/json"
        assert template.substitutions == {"foo", "bar"}
        assert template.template(foo="baz", bar="qux") == "http://example.com/baz/qux"

        # Test templating a string that needs to be URL encoded.
        template = LinkTemplate(
            href="http://example.com/{foo}", type="application/json"
        )
        assert template.template(foo="baz qux:/") == "http://example.com/baz+qux%3A%2F"

        # A URL with no substitutions
        template = LinkTemplate(href="http://example.com/", type="application/json")
        assert template.substitutions == set()
        assert template.template() == "http://example.com/"

        # Test missing substitution
        template = LinkTemplate(
            href="http://example.com/{foo}/{bar}/{baz}", type="application/json"
        )
        with pytest.raises(
            MissingSubstitutionsError, match="Missing substitutions: bar, foo"
        ):
            template.template(baz="qux")

        # Substitution names can be in camelCase or snake_case
        template = LinkTemplate(
            href="http://example.com/{manyParam}/{muchWow}", type="application/json"
        )
        assert template.substitutions == {"manyParam", "muchWow"}
        assert (
            template.template(many_param="abc", much_wow="def")
            == "http://example.com/abc/def"
        )
        assert (
            template.template(manyParam="abc", muchWow="def")
            == "http://example.com/abc/def"
        )


class TestAction:
    def test_get_field(self) -> None:
        action = Action(
            href="http://example.com/action",
            method="get",
            fields=[
                ActionField(name="testField"),
            ],
        )

        # You can get a field by its name either as it is or in snake_case
        assert action.get_field("testField").name == "testField"
        assert action.get_field("test_field").name == "testField"

        # By default, get_field returns None if the field is not found
        assert action.get_field("notFound") is None

        # But you can tell it to raise an error instead
        with pytest.raises(
            NotFoundError,
            match="Field not found: notFound. Available field: testField",
        ):
            action.get_field("not_found", raising=True)

    def test_request(self) -> None:
        action = Action(
            href="http://example.com/action",
            method="put",
            fields=[
                ActionField(name="testField1"),
                ActionField(name="testField2", optional=True),
                ActionField(name="testField3", options={"option1", "option2"}),
                ActionField(name="testField4", value="default"),
            ],
        )

        make_request = MagicMock()
        result: MagicMock = action.request(
            make_request, testField1="value1", testField3="option1"
        )
        make_request.assert_called_once_with(
            method="PUT",
            url="http://example.com/action",
            data=json.dumps(
                {
                    "fields": [
                        {"name": "testField1", "value": "value1"},
                        {"name": "testField3", "value": "option1"},
                        {"name": "testField4", "value": "default"},
                    ]
                }
            ),
            extra_headers={"Content-Type": "application/json"},
        )
        assert result == make_request.return_value

        # You can provide values in snake_case, and override default values
        make_request.reset_mock()
        result = action.request(
            make_request,
            test_field1="value2",
            test_field2="value3",
            test_field3="option2",
            test_field4="value4",
        )
        make_request.assert_called_once_with(
            method="PUT",
            url="http://example.com/action",
            data=json.dumps(
                {
                    "fields": [
                        {"name": "testField1", "value": "value2"},
                        {"name": "testField2", "value": "value3"},
                        {"name": "testField3", "value": "option2"},
                        {"name": "testField4", "value": "value4"},
                    ]
                }
            ),
            extra_headers={"Content-Type": "application/json"},
        )
        assert result == make_request.return_value

        # Test error handling

        # Missing required field
        with pytest.raises(
            MissingRequiredFieldError, match="Action missing required field: testField1"
        ):
            action.request(make_request)

        # Invalid field option
        with pytest.raises(
            InvalidFieldOptionError,
            match="Invalid value for action field testField3: invalid. Valid options: option1, option2",
        ):
            action.request(make_request, test_field1="value1", test_field3="invalid")

        # Extra fields
        action = Action(
            href="http://example.com/action",
            method="delete",
            fields=[],
        )
        with pytest.raises(
            ExtraFieldsError,
            match="Extra fields for action: extraField, otherUnexpected",
        ):
            action.request(make_request, extra_field="value1", other_unexpected="extra")


class TestFormat:
    def test_link_template(self) -> None:
        format = Format(
            format_type="ebook-epub-adobe",
            links={},
            link_templates={
                "aLinkTemplate": LinkTemplate(
                    href="http://example.com/borrow/{templateParam}",
                    type="application/json",
                )
            },
        )

        assert (
            format.template_link("a_link_template", template_param="test")
            == "http://example.com/borrow/test"
        )

        with pytest.raises(
            NotFoundError,
            match="Link template not found: unknownTemplate. Available link template: aLinkTemplate",
        ):
            format.template_link("unknown_template")


class CheckoutsFixture:
    def __init__(self, overdrive_files_fixture: OverdriveFilesFixture) -> None:
        self.create_checkout = partial(
            Checkout,
            reserve_id="reserve_id",
            expires=utc_now(),
            locked_in=False,
        )

        self.files = overdrive_files_fixture


@pytest.fixture
def checkouts_fixture(
    overdrive_files_fixture: OverdriveFilesFixture,
) -> CheckoutsFixture:
    return CheckoutsFixture(overdrive_files_fixture)


class TestCheckout:
    def test_checkout(self, overdrive_files_fixture: OverdriveFilesFixture) -> None:
        checkout = Checkout.model_validate_json(
            overdrive_files_fixture.sample_data(
                "checkout_response_book_fulfilled_on_kindle.json"
            )
        )
        assert checkout.reserve_id == "98EA8135-52C0-4480-9C0E-1D0779670D4A"

        checkout = Checkout.model_validate_json(
            overdrive_files_fixture.sample_data(
                "checkout_response_locked_in_format.json"
            )
        )
        assert checkout.reserve_id == "76C1B7D0-17F4-4C05-8397-C66C17411584"
        assert checkout.locked_in is True
        assert checkout.expires.year == 2013
        assert checkout.expires.month == 10
        assert checkout.expires.day == 4

        assert len(checkout.formats) == 2

        checkout = Checkout.model_validate_json(
            overdrive_files_fixture.sample_data("single_loan.json")
        )
        assert checkout.reserve_id == "2BF132F7-215E-461B-B103-007CCED1915A"

        checkout = Checkout.model_validate_json(
            overdrive_files_fixture.sample_data(
                "checkout_response_bundled_children.json"
            )
        )

    def test_get_format(self, checkouts_fixture: CheckoutsFixture) -> None:
        checkout = Checkout.model_validate_json(
            checkouts_fixture.files.sample_data(
                "checkout_response_locked_in_format.json"
            )
        )

        # Unknown formats return None by default
        assert checkout.get_format("unknown") is None

        # But you can tell it to raise an error instead
        with pytest.raises(
            NotFoundError,
            match="Format not found: unknown. Available formats: ebook-epub-adobe, ebook-overdrive",
        ):
            checkout.get_format("unknown", raising=True)

        epub_format = checkout.get_format("ebook-epub-adobe", raising=True)
        assert epub_format.format_type == "ebook-epub-adobe"
        assert len(epub_format.links) == 1
        assert (
            epub_format.links["self"].href
            == "http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe"
        )
        assert len(epub_format.link_templates) == 1

        ebook_format = checkout.get_format("ebook-overdrive", raising=True)
        assert ebook_format.format_type == "ebook-overdrive"
        assert len(ebook_format.links) == 1
        assert (
            ebook_format.links["self"].href
            == "http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-overdrive"
        )

        # You can also look up a format by its internal format type
        checkout = checkouts_fixture.create_checkout(
            formats=[
                Format(format_type="audiobook-overdrive", links={}, link_templates={}),
            ],
        )
        assert checkout.get_format(
            "audiobook-overdrive", raising=True
        ) is checkout.get_format("audiobook-overdrive-manifest", raising=True)

    def test_available_formats(self, checkouts_fixture: CheckoutsFixture) -> None:
        checkout = Checkout.model_validate_json(
            checkouts_fixture.files.sample_data(
                "checkout_response_no_format_locked_in.json"
            )
        )
        assert checkout.available_formats == {
            "ebook-epub-adobe",
            "ebook-kindle",
            "ebook-overdrive",
        }

        checkout = checkouts_fixture.create_checkout(
            formats=[
                Format(format_type="audiobook-overdrive", links={}, link_templates={}),
            ],
        )
        assert checkout.available_formats == {
            "audiobook-overdrive",
            "audiobook-overdrive-manifest",
        }

    def test_action(self, overdrive_files_fixture: OverdriveFilesFixture) -> None:
        checkout = Checkout.model_validate_json(
            overdrive_files_fixture.sample_data(
                "checkout_response_no_format_locked_in.json"
            )
        )
        make_request = MagicMock()
        action: MagicMock = checkout.action("early_return", make_request)
        assert action == make_request.return_value
        make_request.assert_called_once_with(
            method="DELETE",
            url="http://patron.api.overdrive.com/v1/patrons/me/checkouts/8B0F1552-4677-4FEC-8CE4-8466CFD47E17",
            data=None,
            extra_headers={"Content-Type": "application/json"},
        )

        with pytest.raises(
            NotFoundError,
            match="Action not found: unknownAction. Available actions: earlyReturn, format",
        ):
            checkout.action("unknown_action", make_request)


class TestCheckouts:
    def test_checkouts(self, overdrive_files_fixture: OverdriveFilesFixture) -> None:
        checkouts = Checkouts.model_validate_json(
            overdrive_files_fixture.sample_data("no_loans.json")
        )
        assert checkouts.total_items == 0
        assert checkouts.total_checkouts == 0
        assert checkouts.checkouts == []
        assert len(checkouts.links) == 1
        assert (
            checkouts.links["self"].href
            == "http://patron.api.overdrive.com/v1/patrons/me/checkouts/"
        )

        checkouts = Checkouts.model_validate_json(
            overdrive_files_fixture.sample_data(
                "shelf_with_some_checked_out_books.json"
            )
        )
        assert len(checkouts.links) == 1
        assert (
            checkouts.links["self"].href
            == "http://patron.api.overdrive.com/v1/patrons/me/checkouts/"
        )
        assert checkouts.total_items == 5
        assert checkouts.total_checkouts == 5
        assert len(checkouts.checkouts) == 5

        checkouts = Checkouts.model_validate_json(
            overdrive_files_fixture.sample_data(
                "shelf_with_book_already_fulfilled_on_kindle.json"
            )
        )
        assert len(checkouts.links) == 1
        assert (
            checkouts.links["self"].href
            == "http://patron.api.overdrive.com/v1/patrons/me/checkouts"
        )
        assert checkouts.total_items == 2
        assert checkouts.total_checkouts == 2
        assert len(checkouts.checkouts) == 2


class TestHold:
    def test_hold(self, overdrive_files_fixture: OverdriveFilesFixture) -> None:
        hold = Hold.model_validate_json(
            overdrive_files_fixture.sample_data("successful_hold.json")
        )

        assert hold.reserve_id == "97C2836C-0C2D-4CE4-A1D7-BE3FF15E4E50"
        assert hold.email_address == "leonardrichardson@nypl.org"
        assert hold.hold_list_position == 1
        assert hold.number_of_holds == 1
        assert hold.hold_placed_date == datetime.fromisoformat(
            "2015-03-26T11:30:29+00:00"
        )
        assert "metadata" in hold.links
        assert "removeHold" in hold.actions


class TestHolds:
    def test_holds(self, overdrive_files_fixture: OverdriveFilesFixture) -> None:
        no_holds = Holds.model_validate_json(
            overdrive_files_fixture.sample_data("no_holds.json")
        )
        assert no_holds.total_items == 0
        assert len(no_holds.holds) == 0

        holds = Holds.model_validate_json(
            overdrive_files_fixture.sample_data("holds.json")
        )

        assert holds.total_items == 4
        assert [h.reserve_id for h in holds.holds] == [
            "3BED96C0-36C0-4160-9A23-50B26FAC99B0",
            "539554C4-A01D-4F59-B8E8-602F32AE785B",
            "C823AD40-5B19-4516-93C1-88B5EA1A3E27",
            "97C2836C-0C2D-4CE4-A1D7-BE3FF15E4E50",
        ]


class TestPatronInformation:
    def test_patron_information(
        self, overdrive_files_fixture: OverdriveFilesFixture
    ) -> None:
        patron_information = PatronInformation.model_validate_json(
            overdrive_files_fixture.sample_data("patron_info.json")
        )

        assert patron_information.patron_id == 1810
        assert patron_information.website_id == 37
        assert patron_information.last_hold_email == "foo@bar.com"

        assert "checkouts" in patron_information.links
        assert "search" in patron_information.link_templates
