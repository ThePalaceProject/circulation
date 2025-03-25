import json
from functools import partial
from unittest.mock import MagicMock

import pytest

from palace.manager.api.overdrive.exception import (
    ExtraFieldsError,
    InvalidFieldOptionError,
    MissingRequiredFieldError,
    MissingSubstitutionsError,
    NotFoundError,
)
from palace.manager.api.overdrive.model import (
    Action,
    ActionField,
    Checkout,
    Checkouts,
    Format,
    LinkTemplate,
)
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.files import OverdriveFilesFixture


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
        result = action.request(make_request, testField1="value1", testField3="option1")
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

    def test_supported_formats(self, checkouts_fixture: CheckoutsFixture) -> None:
        checkout = Checkout.model_validate_json(
            checkouts_fixture.files.sample_data(
                "checkout_response_no_format_locked_in.json"
            )
        )
        assert checkout.supported_formats == {
            "ebook-epub-adobe",
            "ebook-kindle",
            "ebook-overdrive",
        }

        checkout = checkouts_fixture.create_checkout(
            formats=[
                Format(format_type="audiobook-overdrive", links={}, link_templates={}),
            ],
        )
        assert checkout.supported_formats == {
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
        action = checkout.action("early_return", make_request)
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
        assert checkouts.total_items == 4
        assert checkouts.total_checkouts == 4
        assert len(checkouts.checkouts) == 4

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
