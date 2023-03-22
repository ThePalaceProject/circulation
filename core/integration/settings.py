from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from pydantic import BaseModel, Extra, ValidationError, root_validator
from pydantic.fields import ModelField
from sqlalchemy.orm import Session
from typing_extensions import Self
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INVALID_CONFIGURATION_OPTION,
)
from core.integration.exceptions import ProblemDetailException


class ConfigurationFormItemType(Enum):
    """Enumeration of configuration setting types"""

    TEXT = None
    TEXTAREA = "textarea"
    SELECT = "select"
    LIST = "list"
    MENU = "menu"


@dataclass(frozen=True)
class ConfigurationFormItem:
    """
    Configuration form item

    This is used to generate the configuration form for the admin interface.
    Each ConfigurationFormItem corresponds to a field in the Pydantic model
    and a field in the ConfigurationForm class.
    """

    # The label for the form item, used as the field label in the admin interface.
    label: str

    # The type of the form item, used to determine the type of the field displayed
    # in the admin interface.
    type: ConfigurationFormItemType = ConfigurationFormItemType.TEXT

    # The description of the form item, displayed below the field in the admin interface.
    description: str | None = None

    # The format of the form item, in some cases used to determine the format of the field
    # displayed in the admin interface.
    format: str | None = None

    # When the type is SELECT, LIST, or MENU, the options are used to populate the
    # field in the admin interface. This can either be a callable that returns a
    # dictionary of options or a dictionary of options.
    options: Callable[[Session], Dict[Enum | str, str]] | Dict[
        Enum | str, str
    ] | None = None

    # Required is usually determined by the Pydantic model, but can be overridden
    # here, in the case where a field would not be required in the model, but is
    # required in the admin interface.
    required: bool = False

    # The weight determines the order of the form items in the admin interface.
    # Form items with a lower weight will be displayed first. Items with the same
    # weight will be displayed in the order they were added.
    weight: int = 0

    @staticmethod
    def get_form_value(value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, bool):
            return str(value).lower()
        return value

    def to_dict(self, db: Session, field: ModelField) -> Tuple[int, Dict[str, Any]]:
        """
        Convert the ConfigurationFormItem to a dictionary

        The dictionary is in the format expected by the admin interface.
        """
        form_entry: Dict[str, Any] = {
            "label": self.label,
            "key": field.name,
            "required": field.required or self.required,
        }
        if field.default is not None:
            form_entry["default"] = self.get_form_value(field.default)
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


class BaseSettings(BaseModel):
    """Base class for all our database backed pydantic settings classes"""

    @root_validator(pre=True)
    def extra_args(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # We log any extra arguments that are passed to the model, but
        # we don't raise an error, these arguments may be old configuration
        # settings that have not been cleaned up by a migration yet.
        for field in values.keys() - cls.__fields__.keys():
            msg = f"Unexpected extra argument '{field}' for model {cls.__name__}"  # type: ignore[attr-defined]
            cls.logger().info(msg)

        # Because the admin interface sends empty strings for all fields
        # we need to convert them to None so that the validators will
        # work correctly.
        for key, value in values.items():
            if isinstance(value, str) and value == "":
                values[key] = None

        return values

    # Custom validation can be done by adding additional @validator methods
    # to the model. See the pydantic docs for more information:
    # https://docs.pydantic.dev/usage/validators/
    # If you want to return a ProblemDetail from the validator, you can
    # raise a SettingsValidationError instead of a ValidationError.

    class Config:
        # See the pydantic docs for information on these settings
        # https://docs.pydantic.dev/usage/model_config/

        # Strip whitespace from all strings
        anystr_strip_whitespace = True

        # Forbid mutation, so that its clear that settings changes will
        # not automatically be saved to the database.
        allow_mutation = False

        # Allow extra arguments to be passed to the model. We allow this
        # because we want to preserve old configuration settings that
        # have not been cleaned up by a migration yet.
        extra = Extra.allow

        # Allow population by field name. We store old field names from
        # ConfigurationSettings as aliases so that we can load old settings,
        # but we generally will populate the module using the field name
        # not the alias.
        allow_population_by_field_name = True

    class ConfigurationForm:
        """
        Our Pydantic Models must have a class called ConfigurationForm
        with a ConfigurationFormItem property for each field in the model.
        This is used to generate the form in the admin interface by the
        configuration_form method below.
        """

        ...

    @classmethod
    def logger(cls) -> logging.Logger:
        """Get the logger for this class"""
        return logging.getLogger(f"{cls.__module__}.{cls.__name__}")

    @classmethod
    def configuration_form(cls, db: Session) -> List[Dict[str, Any]]:
        """Get the configuration dictionary for this class"""
        config = []
        for field in cls.__fields__.values():
            config_item: ConfigurationFormItem | None = getattr(
                cls.ConfigurationForm, field.name, None
            )
            if config_item is None:
                cls.logger().warning(
                    f"Missing configuration form item for field {field.name}"
                )
                continue
            config.append(config_item.to_dict(db, field))

        # Sort by weight then return only the settings
        config.sort(key=lambda x: x[0])
        return [item[1] for item in config]

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """Override the dict method to remove the default values"""
        return super().dict(exclude_defaults=True, *args, **kwargs)

    def __init__(self, **data: Any):
        """
        Override the init method to return our custom ProblemDetailException

        This is needed to allow us to include custom ProblemDetail objects
        with information about how to return the error to the front-end in
        our validation functions.
        """
        try:
            super().__init__(**data)
        except ValidationError as e:
            error = e.errors()[0]
            error_location = error["loc"][0]
            item = getattr(self.ConfigurationForm, str(error_location), None)
            item_label = item.label if item else error_location
            if (
                error["type"] == "value_error.problem_detail"
                and "problem_detail" in error["ctx"]
            ):
                # We have a ProblemDetail, so we return that instead of a
                # generic validation error.
                raise ProblemDetailException(
                    problem_detail=error["ctx"]["problem_detail"]
                )
            elif (
                error["type"] == "value_error.missing"
                or error["type"] == "type_error.none.not_allowed"
            ):
                raise ProblemDetailException(
                    problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                        f"Required field '{item_label}' is missing."
                    )
                )
            else:
                raise ProblemDetailException(
                    problem_detail=INVALID_CONFIGURATION_OPTION.detailed(
                        f"'{item_label}' validation error: {error['msg']}."
                    )
                )

    @classmethod
    def from_form_data(cls, form_data: ImmutableMultiDict[str, str]) -> Self:
        """Load this class from form data"""
        data = {}
        for field in cls.__fields__.values():
            if field.name not in form_data:
                cls.logger().warning(f"Missing field {field.name} in form data")
                continue

            value: List[str] | Optional[str]
            if (
                getattr(cls.ConfigurationForm, field.name).type
                == ConfigurationFormItemType.LIST
            ):
                value = form_data.getlist(field.name)
                value = [v for v in value if v != ""]
            else:
                value = form_data.get(field.name)
            data[field.name] = value
        return cls(**data)
