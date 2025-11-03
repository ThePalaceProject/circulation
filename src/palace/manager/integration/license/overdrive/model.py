import json
import re
import typing
from functools import cached_property
from typing import Protocol, Self, overload
from urllib.parse import quote_plus

from pydantic import (
    AliasChoices,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
)
from pydantic.alias_generators import to_camel

from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotRenew,
    FormatNotAvailable,
    NoActiveLoan,
    NoAvailableCopies,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.integration.license.overdrive.constants import (
    OVERDRIVE_PALACE_MANIFEST_FORMATS,
)
from palace.manager.integration.license.overdrive.exception import (
    ExtraFieldsError,
    InvalidFieldOptionError,
    MissingRequiredFieldError,
    MissingSubstitutionsError,
    NotFoundError,
    OverdriveResponseException,
)
from palace.manager.util.http.exception import ResponseData
from palace.manager.util.log import LoggerMixin


class BaseOverdriveModel(BaseModel):
    """Base class for Overdrive API models."""

    model_config = ConfigDict(
        populate_by_name=True,
        frozen=True,
    )


class ErrorResponse(BaseOverdriveModel, LoggerMixin):
    """
    Typical error response we see from Overdrive.

    This can take several forms, which is a little annoying, but we
    try to handle them all here.
    """

    error_code: str = Field(
        ..., alias="errorCode", validation_alias=AliasChoices("errorCode", "error")
    )
    message: str | None = Field(
        ..., validation_alias=AliasChoices("message", "error_description")
    )
    token: str | None = None

    @classmethod
    def from_response(cls, response: ResponseData) -> Self | None:
        """
        Parse the error response from the given response object.

        :param response: The response object to parse.
        :return: The ErrorResponse object.
        """
        try:
            error = cls.model_validate_json(response.text)
        except Exception as e:
            cls.logger().exception(
                f"Error parsing Overdrive response. "
                f"Status code: {response.status_code}. Response: {response.text}. Error: {e}"
            )
            error = None
        return error

    @classmethod
    def raise_from_response(
        cls, response: ResponseData, default_message: str | None = None
    ) -> None:
        """
        Raise an appropriate exception based on the Overdrive error code
        and message in the given response.
        """
        if default_message is None:
            default_message = "Unknown Overdrive error"

        error = cls.from_response(response)
        error_code = error.error_code if error else None
        error_message = error.message if error else None
        error_token = error.token if error else None

        if error_code == "TitleNotCheckedOut":
            raise NoActiveLoan(error_message)
        elif error_code == "NoCopiesAvailable":
            raise NoAvailableCopies(error_message)
        elif (
            error_code == "PatronHasExceededCheckoutLimit"
            or error_code == "PatronHasExceededCheckoutLimit_ForCPC"
        ):
            raise PatronLoanLimitReached(error_message)
        elif error_code == "TitleAlreadyCheckedOut":
            raise AlreadyCheckedOut(error_message)
        elif error_code == "AlreadyOnWaitList":
            # The book is already on hold.
            raise AlreadyOnHold()
        elif error_code == "NotWithinRenewalWindow":
            # The patron has this book checked out and cannot yet
            # renew their loan.
            raise CannotRenew()
        elif error_code == "PatronExceededHoldLimit":
            raise PatronHoldLimitReached()
        elif error_code == "PatronTitleProcessingFailed":
            raise FormatNotAvailable()

        if error_message is None:
            error_message = default_message

        raise OverdriveResponseException(
            error_message, error_code, error_token, response
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
        """Return the set of substitutions available in the link."""
        return set(self._substitution_regex.findall(self.href))

    def template(self, **kwargs: str) -> str:
        """
        Template the link with the given substitutions.

        Parameters are provided as keyword arguments, where:
         - Keys correspond to substitution placeholders (without curly braces)
         - Values are the strings to insert (they will be URL-encoded)

        Substitution names can be provided in either:
         - camelCase (Overdrive's format)
         - snake_case (Python's convention)

        All values are automatically converted to camelCase before substitution.

        :raises MissingSubstitutionsError: If any required substitutions are not provided

        :param kwargs: The substitutions to insert into the link
        :return: The templated link
        """
        href = self.href
        substitutions = self.substitutions

        camel_kwargs = {to_camel(k): v for k, v in kwargs.items()}

        if missing := (substitutions - camel_kwargs.keys()):
            raise MissingSubstitutionsError(missing)

        for substitution in substitutions:
            value = quote_plus(camel_kwargs.pop(substitution))
            href = href.replace(f"{{{substitution}}}", value)

        return href


class ActionField(BaseOverdriveModel):
    name: str
    value: str | None = None
    options: set[str] = Field(default_factory=set)
    optional: bool = False


class PatronRequestCallable[T](
    Protocol,
):
    def __call__(
        self,
        *,
        url: str,
        extra_headers: dict[str, str] | None = None,
        data: str | None = None,
        method: str | None = None,
    ) -> T: ...


def _overdrive_field_request[T](
    make_request: PatronRequestCallable[T],
    url: str,
    fields: typing.Mapping[str, str | bool | int],
    *,
    method: str | None = None,
) -> T:
    if fields:
        data = json.dumps(
            {
                "fields": [
                    {"name": name, "value": value} for name, value in fields.items()
                ]
            }
        )
    else:
        data = None

    headers = {"Content-Type": "application/json"}

    return make_request(
        method=method,
        url=url,
        data=data,
        extra_headers=headers,
    )


class Action(BaseOverdriveModel):
    href: str
    method: str

    type: str | None = None
    fields: list[ActionField] = Field(default_factory=list)

    @overload
    def get_field(self, name: str, raising: typing.Literal[True]) -> ActionField: ...

    @overload
    def get_field(self, name: str, raising: bool = False) -> ActionField | None: ...

    def get_field(self, name: str, raising: bool = False) -> ActionField | None:
        """
        Get the field with the given name.

        :param name: The name of the field to get. The name can be in camelCase or snake_case.
        :param raising: If raising is True, raise a NotFoundError exception if the
                        field is not found, otherwise return None.

        :return: The ActionField with the given name, or None if no field is found.
        """
        camel_name = to_camel(name)
        for field in self.fields:
            if field.name == camel_name:
                return field
        if raising:
            raise NotFoundError(camel_name, "field", {f.name for f in self.fields})
        return None

    def request[T](self, make_request: PatronRequestCallable[T], **kwargs: str) -> T:
        """
        Make a HTTP request with the parameters and method specified in the action.

        The request data is constructed from the fields in the action, in the format
        that Overdrive expects.

        :param make_request: The callable used to make the HTTP request.
        :param kwargs: The values to provide in the request for fields in the action.
                       These can be either in camelCase or snake_case. snake_case is
                       converted to camelCase before being used.

        :raises MissingRequiredFieldError: If a required field is missing.
        :raises InvalidFieldOptionError: If a field has a value that is not in its options.

        :return: The response from the HTTP request.
        """

        camel_kwargs = {to_camel(k): v for k, v in kwargs.items()}
        field_data = {}
        for field in self.fields:
            if field.name in camel_kwargs:
                value = camel_kwargs.pop(field.name)
            elif field.value:
                value = field.value
            elif field.optional:
                continue
            else:
                raise MissingRequiredFieldError(field.name)

            if field.options and value not in field.options:
                raise InvalidFieldOptionError(field.name, value, field.options)
            field_data[field.name] = value

        if camel_kwargs:
            raise ExtraFieldsError(camel_kwargs.keys())

        return _overdrive_field_request(
            make_request,
            method=self.method.upper(),
            url=self.href,
            fields=field_data,
        )


class Format(BaseOverdriveModel):
    format_type: str = Field(..., alias="formatType")
    links: dict[str, Link]
    link_templates: dict[str, LinkTemplate] = Field(
        default_factory=dict, alias="linkTemplates"
    )

    def template_link(self, name: str, **kwargs: str) -> str:
        """
        Template the LinkTemplate with the given name.

        :raises LinkTemplateNotFoundError: If the link template is not found.

        :param name: Name of the link template. Can be given as snake_case or camelCase.
        :param kwargs: Substitutions to insert into the link template. These will be passed
                       to the LinkTemplate.template method.

        :return: The templated link.
        """
        camel_name = to_camel(name)
        if camel_name not in self.link_templates:
            raise NotFoundError(camel_name, "link template", self.link_templates.keys())
        return self.link_templates[camel_name].template(**kwargs)


class Checkout(BaseOverdriveModel):
    """
    See: https://developer.overdrive.com/apis/checkouts
    """

    reserve_id: str = Field(..., alias="reserveId")
    cross_ref_id: int | None = Field(None, alias="crossRefId")
    expires: AwareDatetime
    locked_in: bool = Field(..., alias="isFormatLockedIn")
    links: dict[str, Link | list[Link]] = Field(default_factory=dict)
    actions: dict[str, Action] = Field(default_factory=dict)
    checkout_date: AwareDatetime | None = Field(None, alias="checkoutDate")
    formats: list[Format] = Field(default_factory=list)

    @overload
    def get_format(self, format_type: str, raising: typing.Literal[True]) -> Format: ...

    @overload
    def get_format(self, format_type: str, raising: bool = ...) -> Format | None: ...

    def get_format(self, format_type: str, raising: bool = False) -> Format | None:
        """
        Get the format data for the given format type.

        If the format type is an internal format, it will be mapped to the public format type
        before being used to search for the format data.

        :param format_type: The format type to search for.
        :param raising: If raising is True, raise a NotFoundError exception if the
                        format is not found, otherwise return None.

        :return: The Format for the given format type, or None if no format is found.
        """

        # If the format type is an internal format, we need to map it to the
        # public format type that Overdrive uses.
        if format_type in OVERDRIVE_PALACE_MANIFEST_FORMATS:
            format_type = OVERDRIVE_PALACE_MANIFEST_FORMATS[format_type]

        for format_data in self.formats:
            if format_data.format_type == format_type:
                return format_data

        if raising:
            raise NotFoundError(
                format_type, "format", {f.format_type for f in self.formats}
            )

        return None

    @cached_property
    def available_formats(self) -> set[str]:
        """
        Get the set of formats that are available for this checkout.

        This includes internal formats, public formats, and any formats that can be locked in.

        :return: The set of formats available for this checkout.
        """

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

    def action[T](
        self, name: str, make_request: PatronRequestCallable[T], **kwargs: str
    ) -> T:
        """
        Make a HTTP request to the action with the specified name.

        :param name: The name of the action to request, in snake_case or camelCase.
        :param make_request: The callable used to make the HTTP request.
        :param kwargs: The values to provide in the request for fields in the action.

        :return: The response from the HTTP request as returned by make_request.
        """

        camel_name = to_camel(name)
        if camel_name not in self.actions:
            raise NotFoundError(camel_name, "action", self.actions.keys())
        return self.actions[camel_name].request(make_request, **kwargs)


class Checkouts(BaseOverdriveModel):
    """
    See: https://developer.overdrive.com/apis/checkouts
    """

    total_items: int = Field(..., alias="totalItems")
    total_checkouts: int = Field(..., alias="totalCheckouts")
    links: dict[str, Link] = Field(default_factory=dict)
    checkouts: list[Checkout] = Field(default_factory=list)


class Hold(BaseOverdriveModel):
    """
    See: https://developer.overdrive.com/apis/holds
    """

    reserve_id: str = Field(..., alias="reserveId")
    cross_ref_id: int | None = Field(None, alias="crossRefId")
    email_address: str | None = Field(None, alias="emailAddress")
    hold_list_position: NonNegativeInt | None = Field(None, alias="holdListPosition")
    number_of_holds: NonNegativeInt | None = Field(None, alias="numberOfHolds")
    hold_placed_date: AwareDatetime = Field(..., alias="holdPlacedDate")
    links: dict[str, Link] = Field(default_factory=dict)
    actions: dict[str, Action] = Field(default_factory=dict)

    # This field isn't referenced in the API docs, but it is present when a
    # hold is available, and gives the date when the hold will expire if
    # it is not checked out.
    hold_expires: AwareDatetime | None = Field(None, alias="holdExpires")


class Holds(BaseOverdriveModel):
    """
    See: https://developer.overdrive.com/apis/holds
    """

    total_items: int = Field(..., alias="totalItems")
    links: dict[str, Link] = Field(default_factory=dict)
    holds: list[Hold] = Field(default_factory=list)


class LendingPeriod(BaseOverdriveModel):
    """Model for a lending period for a format type."""

    format_type: str = Field(..., alias="formatType")
    lending_period: NonNegativeInt = Field(..., alias="lendingPeriod")
    units: str


class PatronInformation(BaseOverdriveModel):
    """
    See: https://developer.overdrive.com/apis/patron-auth
    """

    patron_id: int = Field(..., alias="patronId")
    website_id: int = Field(..., alias="websiteId")
    existing_patron: bool = Field(..., alias="existingPatron")
    collection_token: str = Field(..., alias="collectionToken")
    hold_limit: NonNegativeInt = Field(..., alias="holdLimit")
    last_hold_email: str | None = Field(None, alias="lastHoldEmail")
    checkout_limit: NonNegativeInt = Field(..., alias="checkoutLimit")
    lending_periods: list[LendingPeriod] = Field(..., alias="lendingPeriods")
    links: dict[str, Link] = Field(default_factory=dict)
    link_templates: dict[str, LinkTemplate] = Field(
        default_factory=dict, alias="linkTemplates"
    )
    actions: list[dict[str, Action]] = Field(default_factory=list)
