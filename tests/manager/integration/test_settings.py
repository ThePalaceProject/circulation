import logging
from functools import partial
from unittest.mock import MagicMock

import pytest
from pydantic import PositiveInt, ValidationError, field_validator, model_validator
from sqlalchemy.orm import Session
from typing_extensions import Self

from palace.manager.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
    SettingsValidationError,
)
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.problem_detail import raises_problem_detail

mock_problem_detail = ProblemDetail("http://test.com", 400, "test", "testing 123")


class MockSettings(BaseSettings):
    """Mock settings class"""

    @field_validator("test")
    @classmethod
    def test_validation_pd(cls, v: str) -> str:
        if v == "xyz":
            raise SettingsValidationError(mock_problem_detail)
        return v

    @field_validator("with_alias")
    @classmethod
    def alias_validation_no_pd(cls, v: float) -> float:
        assert v != -212.55, "Sorry, -212.55 is a cursed number"
        assert v != 666.0
        return v

    @model_validator(mode="after")
    def secret_number(self) -> Self:
        if self.number == 66:
            raise ValueError("Error! 66 is a secret number")
        return self

    test: str | None = FormField(
        "test",
        form=ConfigurationFormItem(label="Test", description="Test description"),
    )
    number: PositiveInt = FormField(
        ...,
        form=ConfigurationFormItem(label="Number", description="Number description"),
    )
    with_alias: float = FormField(
        -1.1,
        form=ConfigurationFormItem(
            label="With Alias", description="With Alias description"
        ),
        alias="foo",
    )


class BaseSettingsFixture:
    def __init__(self):
        self.test_config_dict = {
            "default": "test",
            "description": "Test description",
            "key": "test",
            "label": "Test",
            "required": False,
            "hidden": False,
        }
        self.number_config_dict = {
            "description": "Number description",
            "key": "number",
            "label": "Number",
            "required": True,
            "hidden": False,
        }
        self.with_alias_config_dict = {
            "default": -1.1,
            "description": "With Alias description",
            "key": "with_alias",
            "label": "With Alias",
            "required": False,
            "hidden": False,
        }
        self.mock_db = MagicMock(spec=Session)
        self.settings = partial(MockSettings, number=1)


@pytest.fixture
def base_settings_fixture():
    fixture = BaseSettingsFixture()
    yield fixture


class TestBaseSettings:
    def test_init(self, base_settings_fixture: BaseSettingsFixture) -> None:
        settings = base_settings_fixture.settings()
        assert settings.test == "test"
        assert settings.number == 1

    def test_init_invalid(self, base_settings_fixture: BaseSettingsFixture) -> None:
        # Make sure that the settings class raises a ProblemError
        # when there is a problem with validation.
        with raises_problem_detail(
            detail="'Number' validation error: Input should be greater than 0."
        ):
            MockSettings(number=-1)

        with raises_problem_detail(detail="Required field 'Number' is missing."):
            MockSettings()

        with raises_problem_detail(detail="Required field 'Number' is missing."):
            MockSettings(number=None)

    def test_settings_validation(
        self, base_settings_fixture: BaseSettingsFixture
    ) -> None:
        # We have a default validation function that replaces emtpy strings
        # with None.
        settings = base_settings_fixture.settings(test="")
        assert settings.model_dump() == {"test": None, "number": 1}

        # We also have a validation function that runs strip() on all strings.
        settings = base_settings_fixture.settings(test=" foo ")
        assert settings.model_dump() == {"test": "foo", "number": 1}

    def test_field_validator_return_pd_exception(
        self, base_settings_fixture: BaseSettingsFixture
    ) -> None:
        # We can also add custom validation functions to the settings class.
        # These functions should raise a ProblemDetailException if there is
        # a problem with validation.
        with raises_problem_detail(pd=mock_problem_detail):
            base_settings_fixture.settings(test="xyz")

    def test_field_validator_not_pd_exception(
        self, base_settings_fixture: BaseSettingsFixture
    ) -> None:
        # Our validation function can also just raise exceptions like a regular
        # pydantic validation function.
        with raises_problem_detail(
            detail="'With Alias' validation error: Sorry, -212.55 is a cursed number."
        ):
            base_settings_fixture.settings(foo=-212.55)

        with raises_problem_detail(
            detail="'With Alias' validation error: assert 666.0 != 666.0."
        ):
            base_settings_fixture.settings(with_alias=666.0)

    def test_model_validator(self, base_settings_fixture: BaseSettingsFixture) -> None:
        # We can also add model validators to the settings class.
        # These functions should raise a ProblemDetailException if there is
        # a problem with validation.
        with pytest.raises(ProblemDetailException) as e:
            base_settings_fixture.settings(number=66)

        problem_detail = e.value.problem_detail
        assert isinstance(problem_detail, ProblemDetail)
        assert (
            problem_detail.detail == "Validation error: Error! 66 is a secret number."
        )

    def test_model_dump(self, base_settings_fixture: BaseSettingsFixture) -> None:
        # When we call model_dump() on a settings class, we get the settings,
        # minus the default values, so that we are not storing defaults
        # in the database, making it easy to change them in the future.

        # Not in model_dump() when using the default
        settings = base_settings_fixture.settings()
        assert settings.model_dump() == {"number": 1}

        # Not in model_dump() when set in constructor to the default either.
        settings = base_settings_fixture.settings(test="test")
        assert settings.model_dump() == {"number": 1}

    def test_settings_no_mutation(
        self, base_settings_fixture: BaseSettingsFixture
    ) -> None:
        # Make sure that we cannot mutate the settings dataclass
        settings = base_settings_fixture.settings()
        with pytest.raises(ValidationError, match="Instance is frozen"):
            settings.number = 125

    def test_settings_extra_args(
        self,
        base_settings_fixture: BaseSettingsFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Set up our log level
        caplog.set_level(logging.INFO)

        # Make sure that we can pass extra args to the settings class
        # and that the extra args get serialized back into the json.
        settings = base_settings_fixture.settings(test="test", extra="extra")
        assert settings.model_dump() == {"number": 1, "extra": "extra"}
        assert settings.extra == "extra"

        # Make sure that we record a log message when encountering an extra arg
        assert len(caplog.records) == 1
        assert "Unexpected extra argument 'extra' for model MockSettings" in caplog.text

        # Exclude extra defaults to False, but we call it explicitly here
        # to make sure it can be explicitly set to False.
        assert settings.model_dump(exclude_extra=False) == {
            "number": 1,
            "extra": "extra",
        }

        # The extra args will be ignored if we call dict with exclude_extra=True
        assert settings.model_dump(exclude_extra=True) == {"number": 1}

    def test_logger(self) -> None:
        log = MockSettings.logger()
        assert isinstance(log, logging.Logger)
        assert log.name == "tests.manager.integration.test_settings.MockSettings"

    def test_configuration_form(
        self, base_settings_fixture: BaseSettingsFixture
    ) -> None:
        # Make sure that we can get the configuration form from the settings class
        form = MockSettings.configuration_form(base_settings_fixture.mock_db)
        assert form == [
            base_settings_fixture.test_config_dict,
            base_settings_fixture.number_config_dict,
            base_settings_fixture.with_alias_config_dict,
        ]

    def test_configuration_form_weights(
        self, base_settings_fixture: BaseSettingsFixture
    ) -> None:
        # Make sure that the configuration form is sorted by weight
        class WeightedMockSettings(BaseSettings):
            string: str | None = FormField(
                "test",
                form=ConfigurationFormItem(
                    label="Test", description="Test description", weight=100
                ),
            )
            number: PositiveInt = FormField(
                12,
                form=ConfigurationFormItem(
                    label="Number", description="Number description", weight=1
                ),
            )

        [item1, item2] = WeightedMockSettings().configuration_form(
            base_settings_fixture.mock_db
        )
        assert item1["key"] == "number"
        assert item2["key"] == "string"

    def test_configuration_form_suppressed(
        self,
        base_settings_fixture: BaseSettingsFixture,
    ) -> None:
        class MockConfigSettings(BaseSettings):
            explicitly_unhidden_field: str = FormField(
                "default",
                form=ConfigurationFormItem(
                    label="Explicitly Unhidden",
                    description="An explicitly unhidden field",
                    hidden=False,
                ),
            )
            implicitly_unhidden_field: str = FormField(
                "default",
                form=ConfigurationFormItem(
                    label="Implicitly Unhidden",
                    description="An implicitly unhidden field",
                ),
            )
            hidden_field: str = FormField(
                "default",
                form=ConfigurationFormItem(
                    label="Hidden",
                    description="An explicitly hidden field",
                    hidden=True,
                ),
            )

        [item1, item2, item3] = MockConfigSettings().configuration_form(
            base_settings_fixture.mock_db
        )

        assert item1["key"] == "explicitly_unhidden_field"
        assert item1["hidden"] is False
        assert item2["key"] == "implicitly_unhidden_field"
        assert item2["hidden"] is False
        assert item3["key"] == "hidden_field"
        assert item3["hidden"] is True

    def test_configuration_form_options(
        self, base_settings_fixture: BaseSettingsFixture
    ) -> None:
        class OptionsMockSettings(BaseSettings):
            test: str = FormField(
                "test",
                form=ConfigurationFormItem(
                    label="Test",
                    options={"option1": "Option 1", "option2": "Option 2"},
                    type=ConfigurationFormItemType.SELECT,
                ),
            )

        form = OptionsMockSettings().configuration_form(base_settings_fixture.mock_db)
        assert form[0]["options"] == [
            {"key": "option1", "label": "Option 1"},
            {"key": "option2", "label": "Option 2"},
        ]

    def test_configuration_form_options_callable(
        self, base_settings_fixture: BaseSettingsFixture
    ) -> None:
        options_callable = MagicMock(return_value={"xyz": "ABC"})

        class OptionsMockSettings(BaseSettings):
            test: str = FormField(
                "test",
                form=ConfigurationFormItem(
                    label="Test",
                    options=options_callable,
                    type=ConfigurationFormItemType.SELECT,
                ),
            )

        options_callable.assert_not_called()
        form = OptionsMockSettings().configuration_form(base_settings_fixture.mock_db)

        options_callable.assert_called_once_with(base_settings_fixture.mock_db)
        assert form[0]["options"] == [
            {"key": "xyz", "label": "ABC"},
        ]

    def test_additional_form_fields(self) -> None:
        class NoAdditionalSettings(BaseSettings): ...

        form = NoAdditionalSettings().configuration_form(MagicMock())
        assert form == []

        class BadAdditionalSettings(BaseSettings):
            # This should be a dict, but we handle it gracefully. We ignore the
            # type error here because we are testing for it.
            _additional_form_fields = 1  # type: ignore[assignment]

        form = BadAdditionalSettings().configuration_form(MagicMock())
        assert form == []

        class AdditionalSettings(BaseSettings):
            _additional_form_fields = {
                "test": ConfigurationFormItem(
                    label="Test",
                    type=ConfigurationFormItemType.TEXT,
                )
            }

        form = AdditionalSettings().configuration_form(MagicMock())
        assert form == [
            {
                "key": "test",
                "label": "Test",
                "required": False,
                "hidden": False,
            }
        ]

    class TestConfigurationFormItem:
        def test_required(self, caplog: pytest.LogCaptureFixture) -> None:
            caplog.set_level(LogLevel.warning)

            # If required isn't specified, we never get a warning and we use the default
            item = ConfigurationFormItem(
                label="Test", type=ConfigurationFormItemType.TEXT
            )
            assert item.to_dict(MagicMock(), "test", True) == (
                0,
                {
                    "key": "test",
                    "label": "Test",
                    "required": True,
                    "hidden": False,
                },
            )
            assert len(caplog.records) == 0
            assert item.to_dict(MagicMock(), "test", False) == (
                0,
                {
                    "key": "test",
                    "label": "Test",
                    "required": False,
                    "hidden": False,
                },
            )
            assert len(caplog.records) == 0

            # If we set required to true, it overrides the default.
            item = ConfigurationFormItem(
                label="Test", type=ConfigurationFormItemType.TEXT, required=True
            )
            assert item.to_dict(MagicMock(), "test", False) == (
                0,
                {
                    "key": "test",
                    "label": "Test",
                    "required": True,
                    "hidden": False,
                },
            )
            assert len(caplog.records) == 0

            # If we set required to false, and the default is True. It doesn't override and we get a warning.
            item = ConfigurationFormItem(
                label="Test", type=ConfigurationFormItemType.TEXT, required=False
            )
            assert item.to_dict(MagicMock(), "test", True) == (
                0,
                {
                    "key": "test",
                    "label": "Test",
                    "required": True,
                    "hidden": False,
                },
            )
            assert len(caplog.records) == 1
            assert (
                'Configuration form item (label="Test", key=test) does not have '
                "a default value or factory and yet its required property is set to False"
            ) in caplog.text
