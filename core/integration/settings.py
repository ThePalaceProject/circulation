from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Union

from pydantic import (
    BaseModel,
    Extra,
    PydanticValueError,
    ValidationError,
    root_validator,
)
from pydantic.fields import FieldInfo, NoArgAnyCallable, Undefined
from sqlalchemy.orm import Session

from api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INVALID_CONFIGURATION_OPTION,
)
from core.util.log import LoggerMixin
from core.util.problem_detail import ProblemDetail, ProblemError

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, MappingIntStrAny


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
        self,
        default: Any = Undefined,
        form: ConfigurationFormItem = None,  # type: ignore[assignment]
        **kwargs: Any,
    ) -> None:
        super().__init__(default, **kwargs)
        self.form = form

    def _validate(self) -> None:
        if self.form is None:
            # We do a type ignore above so that we can give form a default of none,
            # since it needs a default value because it comes after other arguments
            # with defaults in the function signature.
            # We know it will never be None in practice because this function
            # is called before the field is used, and it will raise an exception if
            # it is None.

            raise ValueError("form parameter is required.")
        super()._validate()


def FormField(
    default: Any = Undefined,
    *,
    form: ConfigurationFormItem = None,  # type: ignore[assignment]
    default_factory: NoArgAnyCallable | None = None,
    alias: str | None = None,
    title: str | None = None,
    description: str | None = None,
    exclude: AbstractSetIntStr | MappingIntStrAny | Any = None,
    include: AbstractSetIntStr | MappingIntStrAny | Any = None,
    const: bool | None = None,
    gt: float | None = None,
    ge: float | None = None,
    lt: float | None = None,
    le: float | None = None,
    multiple_of: float | None = None,
    allow_inf_nan: bool | None = None,
    max_digits: int | None = None,
    decimal_places: int | None = None,
    min_items: int | None = None,
    max_items: int | None = None,
    unique_items: bool | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    allow_mutation: bool = True,
    regex: str | None = None,
    discriminator: str | None = None,
    repr: bool = True,
    **extra: Any,
) -> Any:
    """
    This function is equivalent to the Pydantic Field function, but instead of creating
    a FieldInfo, it creates our FormFieldInfo class.

    When creating a Pydantic model based on the BaseSettings class below, you should
    use this function instead of Field to create fields that will be used to generate
    a configuration form in the admin interface.
    """
    field_info = FormFieldInfo(
        default,
        form=form,
        default_factory=default_factory,
        alias=alias,
        title=title,
        description=description,
        exclude=exclude,
        include=include,
        const=const,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        multiple_of=multiple_of,
        allow_inf_nan=allow_inf_nan,
        max_digits=max_digits,
        decimal_places=decimal_places,
        min_items=min_items,
        max_items=max_items,
        unique_items=unique_items,
        min_length=min_length,
        max_length=max_length,
        allow_mutation=allow_mutation,
        regex=regex,
        discriminator=discriminator,
        repr=repr,
        **extra,
    )
    field_info._validate()
    return field_info


class ConfigurationFormItemType(Enum):
    """Enumeration of configuration setting types"""

    TEXT = None
    TEXTAREA = "textarea"
    SELECT = "select"
    LIST = "list"
    MENU = "menu"
    NUMBER = "number"
    ANNOUNCEMENTS = "announcements"
    COLOR = "color-picker"
    IMAGE = "image"


ConfigurationFormOptionsType = Mapping[Union[Enum, str, None], str]


@dataclass(frozen=True)
class ConfigurationFormItem:
    """
    Configuration form item

    This is used to generate the configuration form for the admin interface.
    Each ConfigurationFormItem corresponds to a field in the Pydantic model
    and is added to the model using the FormField function above.
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
    options: Callable[
        [Session], ConfigurationFormOptionsType
    ] | ConfigurationFormOptionsType | None = None

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
            "required": required or self.required,
        }
        if default is not None:
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

    @root_validator(pre=True)
    def extra_args(cls, values: dict[str, Any]) -> dict[str, Any]:
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
        # as aliases so that we can properly migrate old settings,
        # but we generally will populate the module using the field name
        # not the alias.
        allow_population_by_field_name = True

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
        for field in cls.__fields__.values():
            if not isinstance(field.field_info, FormFieldInfo):
                cls.logger().warning(
                    f"{field.name} was not initialized with FormField, skipping."
                )
                continue
            required = field.required if isinstance(field.required, bool) else False
            config.append(
                field.field_info.form.to_dict(db, field.name, required, field.default)
            )

        for key, additional_field in cls._additional_form_fields.items():
            config.append(additional_field.to_dict(db, key))

        # Sort by weight then return only the settings
        config.sort(key=lambda x: x[0])
        return [item[1] for item in config]

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Override the dict method to remove the default values"""

        if "exclude_defaults" not in kwargs:
            kwargs["exclude_defaults"] = True

        # Allow us to exclude extra fields that are not defined on the model
        if "exclude_extra" in kwargs:
            exclude_extra = kwargs.pop("exclude_extra")
            if exclude_extra:
                kwargs["exclude"] = self.__fields_set__ - self.__fields__.keys()

        return super().dict(*args, **kwargs)

    @classmethod
    def get_form_field_label(cls, field_name: str) -> str:
        item = cls.__fields__.get(field_name)
        if item is None:
            # Try to lookup field_name by alias instead
            for field in cls.__fields__.values():
                if field.alias == field_name:
                    item = field
                    break
        if item is not None and isinstance(item.field_info, FormFieldInfo):
            return item.field_info.form.label
        else:
            return field_name

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
            error_location = str(error["loc"][0])
            item_label = self.get_form_field_label(error_location)

            if (
                error["type"] == "value_error.problem_detail"
                and "problem_detail" in error["ctx"]
            ):
                # We have a ProblemDetail, so we return that instead of a
                # generic validation error.
                raise ProblemError(problem_detail=error["ctx"]["problem_detail"])
            elif (
                error["type"] == "value_error.missing"
                or error["type"] == "type_error.none.not_allowed"
            ):
                raise ProblemError(
                    problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                        f"Required field '{item_label}' is missing."
                    )
                )
            else:
                raise ProblemError(
                    problem_detail=INVALID_CONFIGURATION_OPTION.detailed(
                        f"'{item_label}' validation error: {error['msg']}."
                    )
                )


class SettingsValidationError(PydanticValueError):
    """
    Raised in a custom pydantic validator when there is a problem
    with the configuration settings. A ProblemDetail should
    be passed to the exception constructor.

    for example:
    raise SettingsValidationError(problem_detail=INVALID_CONFIGURATION_OPTION)
    """

    code = "problem_detail"
    msg_template = "{problem_detail.detail}"

    def __init__(self, problem_detail: ProblemDetail, **kwargs: Any):
        super().__init__(problem_detail=problem_detail, **kwargs)
