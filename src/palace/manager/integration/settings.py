from __future__ import annotations

import typing
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from flask_babel import LazyString
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator
from pydantic.fields import FieldInfo
from pydantic_core import ErrorDetails, PydanticUndefined
from sqlalchemy.orm import Session

from palace.manager.api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INVALID_CONFIGURATION_OPTION,
)
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import (
    BaseProblemDetailException,
    ProblemDetail,
    ProblemDetailException,
)
from palace.manager.util.sentinel import SentinelType


def _get_form_metadata(field_info: FieldInfo) -> FormMetadata | None:
    """
    Extract FormMetadata from FieldInfo.metadata.

    Pydantic automatically populates FieldInfo.metadata with items from
    Annotated type hints, so we just iterate through and find our metadata.
    """
    for item in field_info.metadata:
        if isinstance(item, FormMetadata):
            return item
    return None


class FormFieldType(Enum):
    """Enumeration of configuration setting types"""

    TEXT = None
    DATE = "date-picker"
    TEXTAREA = "textarea"
    SELECT = "select"
    LIST = "list"
    MENU = "menu"
    NUMBER = "number"
    ANNOUNCEMENTS = "announcements"
    COLOR = "color-picker"
    IMAGE = "image"


FormOptionsType = Mapping[Enum | str | bool | None, str | LazyString]


@dataclass(frozen=True)
class FormMetadata(LoggerMixin):
    """
    Configuration form metadata

    This is used to generate the configuration form for the admin interface.
    Each FormMetadata corresponds to a field in the Pydantic model
    and is added to the model using Annotated type hints with FormMetadata
    as metadata.
    """

    # The label for the form item, used as the field label in the admin interface.
    label: str | LazyString

    # The type of the form item, used to determine the type of the field displayed
    # in the admin interface.
    type: FormFieldType = FormFieldType.TEXT

    # The description of the form item, displayed below the field in the admin interface.
    description: str | LazyString | None = None

    # The format of the form item, in some cases used to determine the format of the field
    # displayed in the admin interface.
    format: str | None = None

    # When the type is SELECT, LIST, or MENU, the options are used to populate the
    # field in the admin interface. This can either be a callable that returns a
    # dictionary of options or a dictionary of options.
    options: Callable[[Session], FormOptionsType] | FormOptionsType | None = None

    # Required is usually determined by the Pydantic model, but can be overridden
    # here, in the case where a field would not be required in the model, but is
    # required in the admin interface.
    required: bool | typing.Literal[SentinelType.NotGiven] = SentinelType.NotGiven

    # The weight determines the order of the form items in the admin interface.
    # Form items with a lower weight will be displayed first. Items with the same
    # weight will be displayed in the order they were added.
    weight: int = 0

    # If set to True, the Admin UI will be directed to hide this field.
    hidden: bool = False

    @staticmethod
    def get_form_value(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, int):
            return str(value)
        return value

    def to_dict(
        self, db: Session, key: str, required: bool = False, default: Any = None
    ) -> tuple[int, dict[str, Any]]:
        """
        Convert the FormMetadata to a dictionary

        The dictionary is in the format expected by the admin interface.
        """
        form_entry: dict[str, Any] = {
            "label": self.label,
            "key": key,
            "required": required
            or (self.required if self.required is not SentinelType.NotGiven else False),
            "hidden": self.hidden,
        }

        if required is True and self.required is False:
            self.log.warning(
                f'Configuration form item (label="{self.label}", key={key}) does not have a default value or '
                f"factory and yet its required property is set to False.  This condition may indicate a "
                f"programming error. To make this warning go away, either set the configuration form item's default "
                f"value or remove the form item's required property."
            )

        if default is not None and default is not PydanticUndefined:
            form_entry["default"] = self.get_form_value(default)
        if self.type.value is not None:
            form_entry["type"] = self.type.value
        if self.description is not None:
            form_entry["description"] = self.description
        if self.options is not None:
            if not callable(self.options):
                options = self.options
            else:
                options = self.options(db)
            form_entry["options"] = [
                {"key": self.get_form_value(key), "label": value}
                for key, value in options.items()
            ]
        if self.format is not None:
            form_entry["format"] = self.format

        return self.weight, form_entry


class BaseSettings(BaseModel, LoggerMixin):
    """
    Base class for all our database backed pydantic settings classes

    Fields on the model should be defined using Annotated type hints with
    FormMetadata metadata so that we can create a configuration form
    in the admin interface based on the model fields.

    For example:
    class MySettings(BaseSettings):
      my_field: Annotated[
          str,
          FormMetadata(
            label="My Field",
            description="This is my field",
          )
      ] = "default value"
    """

    @model_validator(mode="before")
    @classmethod
    def extra_args(cls, values: dict[str, Any]) -> dict[str, Any]:
        # We log any extra arguments that are passed to the model, but
        # we don't raise an error, these arguments may be old configuration
        # settings that have not been cleaned up by a migration yet.
        model_names_and_aliases = set()
        for field_name, field_info in cls.model_fields.items():
            model_names_and_aliases.add(field_name)
            if field_info.alias is not None:
                model_names_and_aliases.add(field_info.alias)

        for field in values.keys() - model_names_and_aliases:
            msg = f"Unexpected extra argument '{field}' for model {cls.__name__}"
            cls.logger().info(msg)

        # Because the admin interface sends empty strings for all fields
        # we need to convert them to None so that the validators will
        # work correctly.
        for key, value in values.items():
            if isinstance(value, str) and value == "":
                values[key] = None

        return values

    # Custom validation can be done by adding additional validation methods
    # to the model. See the pydantic docs for more information:
    # https://docs.pydantic.dev/usage/validators/
    # If you want to return a ProblemDetail from the validator, you can
    # raise a SettingsValidationError instead of a ValidationError.

    model_config = ConfigDict(
        # See the pydantic docs for information on these settings
        # https://docs.pydantic.dev/usage/model_config/
        # Strip whitespace from all strings
        str_strip_whitespace=True,
        # Make the settings model immutable, so it's clear that settings changes will
        # not automatically be saved to the database.
        frozen=True,
        # Allow extra arguments to be passed to the model. We allow this
        # because we want to preserve old configuration settings that
        # have not been cleaned up by a migration yet.
        extra="allow",
        # Allow population by field name. We store old field names
        # as aliases so that we can properly migrate old settings,
        # but we generally will populate the module using the field name
        # not the alias.
        populate_by_name=True,
    )

    # If your settings class needs additional form fields that are not
    # defined on the model, you can add them here. This is useful if you
    # need to add a custom form field, but don't want the data in the field
    # to be stored on the model in the database. For example, if you want
    # to add a custom form field that allows the user to upload an image, but
    # want to store that image data outside the settings model.
    #
    # The key for the dictionary should be the field name, and the value
    # should be a FormMetadata object that defines the form field.
    _additional_form_fields: dict[str, FormMetadata] = {}

    @classmethod
    def configuration_form(cls, db: Session) -> list[dict[str, Any]]:
        """Get the configuration dictionary for this class"""
        config = []
        for name, field_info in cls.model_fields.items():
            form_item = _get_form_metadata(field_info)
            assert (
                form_item is not None
            ), f"{name} does not have FormMetadata metadata in its Annotated type hint"
            config.append(
                form_item.to_dict(
                    db, name, field_info.is_required(), field_info.default
                )
            )

        additional_fields: Any = cls.__private_attributes__[
            "_additional_form_fields"
        ].default
        if isinstance(additional_fields, dict):
            for key, additional_field in additional_fields.items():
                config.append(additional_field.to_dict(db, key))

        # Sort by weight then return only the settings
        config.sort(key=lambda x: x[0])
        return [item[1] for item in config]

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Override the model_dump method to remove the default values"""

        kwargs.setdefault("exclude_defaults", True)

        # Allow us to exclude extra fields that are not defined on the model
        if "exclude_extra" in kwargs:
            exclude_extra = kwargs.pop("exclude_extra")
            if exclude_extra:
                kwargs["exclude"] = (
                    self.model_fields_set - self.__class__.model_fields.keys()
                )

        return super().model_dump(**kwargs)

    @classmethod
    def get_form_field_label(cls, field_name: str) -> str:
        field_info = cls.model_fields.get(field_name)
        if field_info is None:
            # Try to lookup field_name by alias instead
            for field in cls.model_fields.values():
                if field.alias == field_name:
                    field_info = field
                    break
        if field_info is not None:
            form_item = _get_form_metadata(field_info)
            if form_item is not None:
                return form_item.label

        return field_name

    @classmethod
    def _get_error_label(cls, er: ErrorDetails) -> str:
        error_location = str(er["loc"][0])
        return cls.get_form_field_label(error_location)

    def __init__(self, **data: Any):
        """
        Override the init method to return our custom ProblemError

        This is needed to allow us to include custom ProblemDetail objects
        with information about how to return the error to the front-end in
        our validation functions.
        """
        try:
            super().__init__(**data)
        except ValidationError as e:
            error = e.errors()[0]
            error_exc = error.get("ctx", {}).get("error")
            error_type = error.get("type", "")
            error_input = error.get("input", False)
            error_loc = error.get("loc")
            if error_exc is not None and isinstance(
                error_exc, BaseProblemDetailException
            ):
                # If the exception had a problem detail attached, we want to use that instead
                # of trying to generate an error message.
                problem_detail = error_exc.problem_detail
            elif error_type == "missing" or (
                error_type.endswith("_type") and error_input is None
            ):
                # If the error is a missing field, we return the INCOMPLETE_CONFIGURATION error.
                # The admin UI returns empty strings for all fields, and we have a validator that
                # turns empty strings into None, so we also want to return the INCOMPLETE_CONFIGURATION
                # in the case where the input is None and the error is a type error.
                problem_detail = INCOMPLETE_CONFIGURATION.detailed(
                    f"Required field '{self._get_error_label(error)}' is missing."
                )
            else:
                # Otherwise we create the error message based on Pydantic's error message.

                error_msg = error["msg"]
                if error_type == "assertion_error":
                    # For failed assertions, we do a little editing to make the error message more readable in
                    # the admin UI.
                    error_msg = str.replace(error_msg, "Assertion failed, ", "")
                    split_msg = error_msg.split("\n")
                    if len(split_msg) > 1:
                        error_msg = split_msg[0]
                elif error_type == "value_error":
                    # Same as above, but for value errors.
                    error_msg = str.replace(error_msg, "Value error, ", "")

                # Make sure the error message ends with a period.
                if error_msg and error_msg[-1] not in [".", "!", "?"]:
                    error_msg += "."

                # If the error has a location, we turn that into the Admin UI field label and include it in the
                # error message.
                if error_loc:
                    problem_detail = INVALID_CONFIGURATION_OPTION.detailed(
                        f"'{self._get_error_label(error)}' validation error: {error_msg}"
                    )
                else:
                    problem_detail = INVALID_CONFIGURATION_OPTION.detailed(
                        f"Validation error: {error_msg}"
                    )

            raise ProblemDetailException(problem_detail=problem_detail) from e


class SettingsValidationError(ProblemDetailException, ValueError):
    """
    Raised in a custom pydantic validator when there is a problem
    with the configuration settings. A ProblemDetail should
    be passed to the exception constructor.

    for example:
    raise SettingsValidationError(problem_detail=INVALID_CONFIGURATION_OPTION)
    """

    def __init__(self, problem_detail: ProblemDetail) -> None:
        super().__init__(problem_detail=problem_detail)
