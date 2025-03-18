import json
import re
import typing
from functools import cached_property
from typing import Protocol, TypeVar
from urllib.parse import quote_plus

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from palace.manager.api.overdrive.constants import OVERDRIVE_PALACE_MANIFEST_FORMATS
from palace.manager.core.exceptions import PalaceValueError


class BaseOverdriveModel(BaseModel):
    """Base class for Overdrive API models."""

    model_config = ConfigDict(
        populate_by_name=True,
        frozen=True,
    )


class Link(BaseOverdriveModel):
    """Link model."""

    href: str
    type: str


class LinkTemplate(Link):
    """Link template model."""

    _substitution_regex = re.compile(r"{(\w+?)}")

    @cached_property
    def substitutions(self) -> set[str]:
        return set(self._substitution_regex.findall(self.href))

    def template(self, **kwargs: str) -> str:
        href = self.href
        substitutions = self.substitutions

        if missing := (substitutions - kwargs.keys()):
            raise PalaceValueError(
                f"Missing substitutions: {', '.join(sorted(missing))}"
            )

        for substitution in substitutions:
            value = quote_plus(kwargs.pop(substitution))
            href = href.replace(f"{{{substitution}}}", value)

        return href


class ActionField(BaseOverdriveModel):
    name: str
    value: str | None = None
    options: list[str] = Field(default_factory=list)
    optional: bool = False


TOverdriveModel = TypeVar("TOverdriveModel", bound=BaseOverdriveModel)


class MakePatronRequestCallable(Protocol):
    def __call__(
        self,
        *,
        url: str,
        extra_headers: dict[str, str] | None = None,
        data: str | None = None,
        method: str | None = None,
    ) -> dict[str, typing.Any]: ...


class Action(BaseOverdriveModel):
    href: str
    method: str

    type: str | None = None
    fields: list[ActionField] = Field(default_factory=list)

    def get_field(self, name: str) -> ActionField | None:
        for field in self.fields:
            if field.name == name:
                return field
        return None

    def call(
        self, make_request: MakePatronRequestCallable, **kwargs: str
    ) -> dict[str, typing.Any]:
        field_data = {}
        for field in self.fields:
            if field.name in kwargs:
                value = kwargs.pop(field.name)
            elif field.value:
                value = field.value
            elif field.optional:
                continue
            else:
                raise PalaceValueError(f"Missing required field: {field.name}")

            if field.options and value not in field.options:
                raise PalaceValueError(
                    f"Invalid value for field {field.name}: {value}. Valid options: {', '.join(field.options)}"
                )
            field_data[field.name] = value

        if kwargs:
            raise PalaceValueError(f"Unexpected fields: {', '.join(kwargs.keys())}")

        if field_data:
            data = json.dumps(
                {
                    "fields": [
                        {"name": name, "value": value}
                        for name, value in field_data.items()
                    ]
                }
            )
        else:
            data = None

        headers = (
            {"Content-Type": self.type} if self.type and data is not None else None
        )
        return make_request(
            method=self.method, url=self.href, data=data, extra_headers=headers
        )


class Format(BaseOverdriveModel):
    format_type: str = Field(..., alias="formatType")
    links: dict[str, Link]
    link_templates: dict[str, LinkTemplate] = Field(
        default_factory=dict, alias="linkTemplates"
    )

    def template(self, name: str, **kwargs: str) -> str:
        if name not in self.link_templates:
            raise PalaceValueError(
                f"Unknown link template: {name}. "
                f"Available templates: {', '.join(self.link_templates.keys())}"
            )
        return self.link_templates[name].template(**kwargs)


class Checkout(BaseOverdriveModel):
    """
    See: https://developer.overdrive.com/apis/checkouts
    """

    reserve_id: str = Field(..., alias="reserveId")
    cross_ref_id: int | None = Field(None, alias="crossRefId")
    expires: AwareDatetime
    locked_in: bool = Field(..., alias="isFormatLockedIn")
    links: dict[str, Link] = Field(default_factory=dict)
    actions: dict[str, Action] = Field(default_factory=dict)
    checkout_date: AwareDatetime | None = Field(None, alias="checkoutDate")
    formats: list[Format] = Field(default_factory=list)

    def get_format(self, format_type: str) -> Format | None:
        # If the format type is an internal format, we need to map it to the
        # public format type that Overdrive uses.
        if format_type in OVERDRIVE_PALACE_MANIFEST_FORMATS:
            format_type = OVERDRIVE_PALACE_MANIFEST_FORMATS[format_type]

        for format_data in self.formats:
            if format_data.format_type == format_type:
                return format_data
        return None

    @cached_property
    def supported_formats(self) -> set[str]:
        # All the formats listed as available in the checkout
        formats = {f.format_type for f in self.formats}

        # If a format is available that maps to a Palace internal format, we
        # also include the internal format in the set of formats.
        for internal_format, public_format in OVERDRIVE_PALACE_MANIFEST_FORMATS.items():
            if public_format in formats:
                formats.add(internal_format)

        # We also want to include any formats that we can lock in.
        format_action = self.actions.get("format")
        if format_action:
            format_types = format_action.get_field("formatType")
            if format_types:
                formats.update(format_types.options)

        return formats

    def action(
        self, name: str, make_request: MakePatronRequestCallable, **kwargs: str
    ) -> dict[str, typing.Any]:
        if name not in self.actions:
            raise PalaceValueError(
                f"Action {name} is not available for this checkout. "
                f"Available actions: {', '.join(self.actions.keys())}"
            )
        return self.actions[name].call(make_request, **kwargs)


class Checkouts(BaseOverdriveModel):
    """
    See: https://developer.overdrive.com/apis/checkouts
    """

    total_items: int = Field(..., alias="totalItems")
    total_checkouts: int = Field(..., alias="totalCheckouts")
    links: dict[str, Link] = Field(default_factory=dict)
    checkouts: list[Checkout] = Field(default_factory=list)
