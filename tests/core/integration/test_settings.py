import dataclasses
import logging
from unittest.mock import MagicMock

import pytest
from pydantic import PositiveInt, validator
from sqlalchemy.orm import Session

from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
    SettingsValidationError,
)
from core.util.problem_detail import ProblemDetail, ProblemError


@pytest.fixture
def mock_problem_detail():
    return ProblemDetail("http://test.com", 400, "test", "testing 123")


@pytest.fixture
def mock_settings(mock_problem_detail):
    class MockSettings(BaseSettings):
        """Mock settings class"""

        @validator("test", allow_reuse=True)
        def custom_validator(cls, v):
            if v == "xyz":
                raise SettingsValidationError(mock_problem_detail)
            return v

        test: str | None = FormField(
            "test",
            form=ConfigurationFormItem(label="Test", description="Test description"),
        )
        number: PositiveInt = FormField(
            ...,
            form=ConfigurationFormItem(
                label="Number", description="Number description"
            ),
        )
        # Intentionally not set with FormField
        missing: bool = False

    return MockSettings


@pytest.fixture
def test_config_dict():
    return {
        "default": "test",
        "description": "Test description",
        "key": "test",
        "label": "Test",
        "required": False,
    }


@pytest.fixture
def number_config_dict():
    return {
        "description": "Number description",
        "key": "number",
        "label": "Number",
        "required": True,
    }


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


def test_settings_init(mock_settings):
    settings = mock_settings(number=1)
    assert settings.test == "test"
    assert settings.number == 1


def test_settings_init_invalid(mock_settings):
    # Make sure that the settings class raises a ProblemError
    # when there is a problem with validation.
    with pytest.raises(ProblemError) as e:
        mock_settings(number=-1)

    problem_detail = e.value.problem_detail
    assert isinstance(problem_detail, ProblemDetail)
    assert (
        problem_detail.detail
        == "'Number' validation error: ensure this value is greater than 0."
    )

    with pytest.raises(ProblemError) as e:
        mock_settings()

    problem_detail = e.value.problem_detail
    assert isinstance(problem_detail, ProblemDetail)
    assert problem_detail.detail == "Required field 'Number' is missing."


def test_settings_validation(mock_settings):
    # We have a default validation function that replaces emtpy strings
    # with None.
    settings = mock_settings(number=1, test="")
    assert settings.dict() == {"test": None, "number": 1}

    # We also have a validation function that runs strip() on all strings.
    settings = mock_settings(number=1, test=" foo ")
    assert settings.dict() == {"test": "foo", "number": 1}


def test_settings_validation_custom(mock_settings, mock_problem_detail):
    # We can also add custom validation functions to the settings class.
    # These functions should raise a ProblemError if there is
    # a problem with validation.
    with pytest.raises(ProblemError) as e:
        mock_settings(number=1, test="xyz")

    problem_detail = e.value.problem_detail
    assert isinstance(problem_detail, ProblemDetail)
    assert problem_detail == mock_problem_detail


def test_settings_dict(mock_settings):
    # When we call to_dict() on a settings class, we get the settings,
    # minus the default values, so that we are not storing defaults
    # in the database, making it easy to change them in the future.

    # Not in dict() when using the default
    settings = mock_settings(number=1)
    assert settings.dict() == {"number": 1}

    # Not in dict() when set in constructor to the default either.
    settings = mock_settings(number=1, test="test")
    assert settings.dict() == {"number": 1}


def test_settings_no_mutation(mock_settings):
    # Make sure that we cannot mutate the settings dataclass
    settings = mock_settings(number=1)
    with pytest.raises(TypeError):
        settings.number = 125


def test_settings_extra_args(mock_settings, caplog):
    # Set up our log level
    caplog.set_level(logging.INFO)

    # Make sure that we can pass extra args to the settings class
    # and that the extra args get serialized back into the json.
    settings = mock_settings(number=1, test="test", extra="extra")
    assert settings.dict() == {"number": 1, "extra": "extra"}
    assert settings.extra == "extra"

    # Make sure that we record a log message when encountering an extra arg
    assert len(caplog.records) == 1
    assert "Unexpected extra argument 'extra' for model MockSettings" in caplog.text

    # Exclude extra defaults to False, but we call it explicitly here
    # to make sure it can be explicitly set to False.
    assert settings.dict(exclude_extra=False) == {"number": 1, "extra": "extra"}

    # The extra args will be ignored if we call dict with exclude_extra=True
    assert settings.dict(exclude_extra=True) == {"number": 1}


def test_settings_logger(mock_settings):
    log = mock_settings.logger()
    assert isinstance(log, logging.Logger)
    assert log.name == "test_settings.MockSettings"


def test_settings_configuration_form(
    mock_settings, test_config_dict, number_config_dict, mock_db
):
    # Make sure that we can get the configuration form from the settings class
    form = mock_settings.configuration_form(mock_db)
    assert form == [test_config_dict, number_config_dict]


def test_settings_configuration_form_weights(
    mock_settings, test_config_dict, number_config_dict, mock_db
):
    # Make sure that the configuration form is sorted by weight
    mock_settings.__fields__["test"].field_info.form = dataclasses.replace(
        mock_settings.__fields__["test"].field_info.form, weight=100
    )
    mock_settings.__fields__["number"].field_info.form = dataclasses.replace(
        mock_settings.__fields__["number"].field_info.form, weight=1
    )
    form = mock_settings.configuration_form(mock_db)
    assert form == [number_config_dict, test_config_dict]


def test_settings_configuration_form_logs_missing(mock_settings, mock_db, caplog):
    caplog.set_level(logging.WARNING)
    _ = mock_settings.configuration_form(mock_db)
    assert len(caplog.records) == 1
    assert "was not initialized with FormField" in caplog.text


def test_settings_configuration_form_options(mock_settings, mock_db):
    mock_settings.__fields__["test"].field_info.form = dataclasses.replace(
        mock_settings.__fields__["test"].field_info.form,
        options={"option1": "Option 1", "option2": "Option 2"},
        type=ConfigurationFormItemType.SELECT,
    )
    form = mock_settings.configuration_form(mock_db)
    assert form[0]["options"] == [
        {"key": "option1", "label": "Option 1"},
        {"key": "option2", "label": "Option 2"},
    ]


def test_settings_configuration_form_options_callable(mock_settings, mock_db):
    called_with = None

    def options_callable(db):
        nonlocal called_with
        called_with = db
        return {"xyz": "ABC"}

    mock_settings.__fields__["test"].field_info.form = dataclasses.replace(
        mock_settings.__fields__["test"].field_info.form,
        options=options_callable,
        type=ConfigurationFormItemType.SELECT,
    )

    form = mock_settings.configuration_form(mock_db)
    assert called_with == mock_db
    assert form[0]["options"] == [
        {"key": "xyz", "label": "ABC"},
    ]


def test_form_field_no_form():
    # Make we cannot create a FormField without a form
    with pytest.raises(ValueError) as e:
        FormField("default value")

    assert str(e.value) == "form parameter is required."
