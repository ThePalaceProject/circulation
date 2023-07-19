import pytest
from werkzeug.datastructures import MultiDict

from api.admin.validator import Validator
from tests.api.admin.dummy_validator.dummy_validator import (
    DummyAuthenticationProviderValidator,
)


class MockValidations:
    LIST_TEST_KEY = "list-test"
    WITH_LIST = [
        {
            "key": LIST_TEST_KEY,
            "label": "I am a list",
            "type": "list",
            "format": "url",
            "required": True,
        }
    ]


class TestValidator:
    def test_validate_email(self):
        settings_form = [
            {
                "key": "help-email",
                "format": "email",
            },
            {
                "key": "configuration_contact_email_address",
                "format": "email",
            },
        ]

        valid = "valid_format@email.com"
        invalid = "invalid_format"

        # One valid input from form
        form = MultiDict([("help-email", valid)])
        response = Validator().validate_email(settings_form, {"form": form})
        assert response == None

        # One invalid input from form
        form = MultiDict([("help-email", invalid)])
        response = Validator().validate_email(settings_form, {"form": form})
        assert response.detail == '"invalid_format" is not a valid email address.'
        assert response.status_code == 400

        # One valid and one invalid input from form
        form = MultiDict(
            [("help-email", valid), ("configuration_contact_email_address", invalid)]
        )
        response = Validator().validate_email(settings_form, {"form": form})
        assert response.detail == '"invalid_format" is not a valid email address.'
        assert response.status_code == 400

        # Valid string
        response = Validator().validate_email(valid, {})
        assert response == None

        # Invalid string
        response = Validator().validate_email(invalid, {})
        assert response.detail == '"invalid_format" is not a valid email address.'
        assert response.status_code == 400

        # Two valid in a list
        form = MultiDict([("help-email", valid), ("help-email", "valid2@email.com")])
        response = Validator().validate_email(settings_form, {"form": form})
        assert response == None

        # One valid and one empty in a list
        form = MultiDict([("help-email", valid), ("help-email", "")])
        response = Validator().validate_email(settings_form, {"form": form})
        assert response == None

        # One valid and one invalid in a list
        form = MultiDict([("help-email", valid), ("help-email", invalid)])
        response = Validator().validate_email(settings_form, {"form": form})
        assert response.detail == '"invalid_format" is not a valid email address.'
        assert response.status_code == 400

    def test_validate_url(self):
        valid = "https://valid_url.com"
        invalid = "invalid_url"

        settings_form = [
            {
                "key": "help-web",
                "format": "url",
            },
            {
                "key": "terms-of-service",
                "format": "url",
            },
        ]

        # Valid
        form = MultiDict([("help-web", valid)])
        response = Validator().validate_url(settings_form, {"form": form})
        assert response == None

        # Invalid
        form = MultiDict([("help-web", invalid)])
        response = Validator().validate_url(settings_form, {"form": form})
        assert response.detail == '"invalid_url" is not a valid URL.'
        assert response.status_code == 400

        # One valid, one invalid
        form = MultiDict([("help-web", valid), ("terms-of-service", invalid)])
        response = Validator().validate_url(settings_form, {"form": form})
        assert response.detail == '"invalid_url" is not a valid URL.'
        assert response.status_code == 400

        # Two valid in a list
        form = MultiDict(
            [
                (MockValidations.LIST_TEST_KEY, "http://library1.com"),
                (MockValidations.LIST_TEST_KEY, "http://library2.com"),
            ]
        )
        response = Validator().validate_url(MockValidations.WITH_LIST, {"form": form})
        assert response == None

        # One valid and one empty in a list
        form = MultiDict(
            [
                (MockValidations.LIST_TEST_KEY, "http://library1.com"),
                (MockValidations.LIST_TEST_KEY, ""),
            ]
        )
        response = Validator().validate_url(MockValidations.WITH_LIST, {"form": form})
        assert response == None

        # One valid and one invalid in a list
        form = MultiDict(
            [
                (MockValidations.LIST_TEST_KEY, "http://library1.com"),
                (MockValidations.LIST_TEST_KEY, invalid),
            ]
        )
        response = Validator().validate_url(MockValidations.WITH_LIST, {"form": form})
        assert response.detail == '"invalid_url" is not a valid URL.'
        assert response.status_code == 400

    def test_validate_number(self):
        settings_form = [
            {
                "key": "hold_limit",
                "type": "number",
            },
            {
                "key": "loan_limit",
                "type": "number",
            },
            {
                "key": "minimum_featured_quality",
                "max": 1,
                "type": "number",
            },
        ]

        valid = "10"
        invalid = "ten"

        # Valid
        form = MultiDict([("hold_limit", valid)])
        response = Validator().validate_number(settings_form, {"form": form})
        assert response == None

        # Invalid
        form = MultiDict([("hold_limit", invalid)])
        response = Validator().validate_number(settings_form, {"form": form})
        assert response.detail == '"ten" is not a number.'
        assert response.status_code == 400

        # One valid, one invalid
        form = MultiDict([("hold_limit", valid), ("loan_limit", invalid)])
        response = Validator().validate_number(settings_form, {"form": form})
        assert response.detail == '"ten" is not a number.'
        assert response.status_code == 400

        # Invalid: below minimum
        form = MultiDict([("hold_limit", -5)])
        response = Validator().validate_number(settings_form, {"form": form})
        assert "must be greater than 0." in response.detail
        assert response.status_code == 400

        # Valid: below maximum
        form = MultiDict([("minimum_featured_quality", ".9")])
        response = Validator().validate_number(settings_form, {"form": form})
        assert response == None

        # Invalid: above maximum
        form = MultiDict([("minimum_featured_quality", "2")])
        response = Validator().validate_number(settings_form, {"form": form})
        assert "cannot be greater than 1." in response.detail
        assert response.status_code == 400

    def test_validate(self):
        called = []

        settings_form = [
            {
                "key": "hold_limit",
                "type": "number",
            },
            {
                "key": "help-web",
                "format": "url",
            },
            {
                "key": "configuration_contact_email_address",
                "format": "email",
            },
        ]

        class Mock(Validator):
            def validate_email(self, settings, content):
                called.append("validate_email")

            def validate_url(self, settings, content):
                called.append("validate_url")

            def validate_number(self, settings, content):
                called.append("validate_number")

        Mock().validate(settings_form, {})
        assert called == [
            "validate_email",
            "validate_url",
            "validate_number",
        ]

    def test__is_url(self):
        m = Validator._is_url

        assert False == m(None, [])
        assert False == m("", [])
        assert False == m("not a url", [])

        # Only HTTP and HTTP URLs are allowed.
        assert True == m("http://server.com/", [])
        assert True == m("https://server.com/", [])
        assert False == m("gopher://server.com/", [])
        assert False == m("http:/server.com/", [])

        # You can make specific URLs go through even if they
        # wouldn't normally pass.
        assert True == m("Not a URL", ["Not a URL", "Also not a URL"])


class PatronAuthenticationValidatorFactoryTest:
    @pytest.mark.parametrize(
        "name,protocol",
        [
            ("validator_using_class_name", "tests.admin.fixtures.dummy_validator"),
            (
                "validator_using_factory_method",
                "tests.admin.fixtures.dummy_validator_factory",
            ),
        ],
    )
    def test_create_can_create(self, name, protocol):
        # Arrange
        factory = PatronAuthenticationValidatorFactory()

        # Act
        result = factory.create(protocol)

        # Assert
        assert isinstance(result, DummyAuthenticationProviderValidator)
