from __future__ import annotations

import typing
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Unpack

import annotated_types
import typing_extensions
from flask_babel import LazyString
from pydantic import (
    AliasChoices,
    AliasPath,
    BaseModel,
    ConfigDict,
    ValidationError,
    model_validator,
    types,
)
from pydantic.config import JsonDict
from pydantic.fields import (
    Deprecated,
    FieldInfo,
    _EmptyKwargs,
    _FieldInfoInputs,
    _Unset,
)
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


class FormFieldInfo(FieldInfo):
    """
    A Pydantic FieldInfo that includes a ConfigurationFormItem

    This is used to store the ConfigurationFormItem for a field, so that
    we can use it to generate a configuration form for the admin interface.

    This class should not be called directly, rather it should be created by
    calling the FormField function below.
    """

    __slots__ = ("form",)

    def __init__(
        self, *, form: ConfigurationFormItem, **kwargs: Unpack[_FieldInfoInputs]
    ) -> None:
        super().__init__(**kwargs)
        self.form = form

    @staticmethod
    def from_field(default: Any = PydanticUndefined, **kwargs: Any) -> FormFieldInfo:
        return FormFieldInfo(default=default, **kwargs)


def FormField(
    default: Any = PydanticUndefined,
    *,
    form: ConfigurationFormItem,
    default_factory: typing.Callable[[], Any] | None = _Unset,
    alias: str | None = _Unset,
    alias_priority: int | None = _Unset,
    validation_alias: str | AliasPath | AliasChoices | None = _Unset,
    serialization_alias: str | None = _Unset,
    title: str | None = _Unset,
    field_title_generator: (
        typing_extensions.Callable[[str, FieldInfo], str] | None
    ) = _Unset,
    description: str | None = _Unset,
    examples: list[Any] | None = _Unset,
    exclude: bool | None = _Unset,
    discriminator: str | types.Discriminator | None = _Unset,
    deprecated: Deprecated | str | bool | None = _Unset,
    json_schema_extra: JsonDict | typing.Callable[[JsonDict], None] | None = _Unset,
    frozen: bool | None = _Unset,
    validate_default: bool | None = _Unset,
    repr: bool = _Unset,
    init: bool | None = _Unset,
    init_var: bool | None = _Unset,
    kw_only: bool | None = _Unset,
    pattern: str | typing.Pattern[str] | None = _Unset,
    strict: bool | None = _Unset,
    coerce_numbers_to_str: bool | None = _Unset,
    gt: annotated_types.SupportsGt | None = _Unset,
    ge: annotated_types.SupportsGe | None = _Unset,
    lt: annotated_types.SupportsLt | None = _Unset,
    le: annotated_types.SupportsLe | None = _Unset,
    multiple_of: float | None = _Unset,
    allow_inf_nan: bool | None = _Unset,
    max_digits: int | None = _Unset,
    decimal_places: int | None = _Unset,
    min_length: int | None = _Unset,
    max_length: int | None = _Unset,
    union_mode: typing.Literal["smart", "left_to_right"] = _Unset,
    fail_fast: bool | None = _Unset,
    **extra: Unpack[_EmptyKwargs],
) -> Any:
    """
    This function is equivalent to the Pydantic Field function, but instead of creating
    a FieldInfo, it creates our FormFieldInfo class.

    When creating a Pydantic model based on the BaseSettings class below, you should
    use this function instead of Field to create fields that will be used to generate
    a configuration form in the admin interface.

    There isn't a great way to override this function so this code is just copied from the
    Pydantic Field function with the FormFieldInfo class used instead of FieldInfo.
    """
    if (
        validation_alias
        and validation_alias is not _Unset
        and not isinstance(validation_alias, (str, AliasChoices, AliasPath))
    ):
        raise TypeError(
            "Invalid `validation_alias` type. it should be `str`, `AliasChoices`, or `AliasPath`"
        )

    if serialization_alias in (_Unset, None) and isinstance(alias, str):
        serialization_alias = alias

    if validation_alias in (_Unset, None):
        validation_alias = alias

    return FormFieldInfo.from_field(
        default,
        form=form,
        default_factory=default_factory,
        alias=alias,
        alias_priority=alias_priority,
        validation_alias=validation_alias,
        serialization_alias=serialization_alias,
        title=title,
        field_title_generator=field_title_generator,
        description=description,
        examples=examples,
        exclude=exclude,
        discriminator=discriminator,
        deprecated=deprecated,
        json_schema_extra=json_schema_extra,
        frozen=frozen,
        pattern=pattern,
        validate_default=validate_default,
        repr=repr,
        init=init,
        init_var=init_var,
        kw_only=kw_only,
        coerce_numbers_to_str=coerce_numbers_to_str,
        strict=strict,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        multiple_of=multiple_of,
        min_length=min_length,
        max_length=max_length,
        allow_inf_nan=allow_inf_nan,
        max_digits=max_digits,
        decimal_places=decimal_places,
        union_mode=union_mode,
        fail_fast=fail_fast,
    )


class ConfigurationFormItemType(Enum):
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


ConfigurationFormOptionsType = Mapping[Enum | str | bool | None, str | LazyString]


@dataclass(frozen=True)
class ConfigurationFormItem(LoggerMixin):
    """
    Configuration form item

    This is used to generate the configuration form for the admin interface.
    Each ConfigurationFormItem corresponds to a field in the Pydantic model
    and is added to the model using the FormField function above.
    """

    # The label for the form item, used as the field label in the admin interface.
    label: str | LazyString

    # The type of the form item, used to determine the type of the field displayed
    # in the admin interface.
    type: ConfigurationFormItemType = ConfigurationFormItemType.TEXT

    # The description of the form item, displayed below the field in the admin interface.
    description: str | LazyString | None = None

    # The format of the form item, in some cases used to determine the format of the field
    # displayed in the admin interface.
    format: str | None = None

    # When the type is SELECT, LIST, or MENU, the options are used to populate the
    # field in the admin interface. This can either be a callable that returns a
    # dictionary of options or a dictionary of options.
    options: (
        Callable[[Session], ConfigurationFormOptionsType]
        | ConfigurationFormOptionsType
        | None
    ) = None

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
        Convert the ConfigurationFormItem to a dictionary

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

    Fields on the model should be defined using the FormField function above so
    that we can create a configuration form in the admin interface based on the
    model fields.

    For example:
    class MySettings(BaseSettings):
      my_field: str = FormField(
        "default value",
        form=ConfigurationFormItem(
            label="My Field",
            description="This is my field",
        ),
      )
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
    # should be a ConfigurationFormItem object that defines the form field.
    _additional_form_fields: dict[str, ConfigurationFormItem] = {}

    @classmethod
    def configuration_form(cls, db: Session) -> list[dict[str, Any]]:
        """Get the configuration dictionary for this class"""
        config = []
        for name, field_info in cls.model_fields.items():
            assert isinstance(
                field_info, FormFieldInfo
            ), f"{name} was not initialized with FormField"
            config.append(
                field_info.form.to_dict(
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
        item = cls.model_fields.get(field_name)
        if item is None:
            # Try to lookup field_name by alias instead
            for field in cls.model_fields.values():
                if field.alias == field_name:
                    item = field
                    break
        if item is not None and isinstance(item, FormFieldInfo):
            return item.form.label

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
