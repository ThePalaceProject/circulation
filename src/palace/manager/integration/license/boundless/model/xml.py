from abc import ABC
from functools import cached_property
from typing import Any, Literal, Self

from lxml import etree
from pydantic import ConfigDict, NonNegativeInt, PositiveInt
from pydantic.alias_generators import to_camel, to_pascal
from pydantic_xml import BaseXmlModel, ParsingError, element, wrapped

from palace.manager.api.circulation.exceptions import AlreadyOnHold
from palace.manager.integration.license.boundless.constants import BoundlessFormat
from palace.manager.integration.license.boundless.exception import (
    ErrorLookupType,
    StatusResponseParser,
)
from palace.manager.integration.license.boundless.model.base import (
    BaseBoundlessResponse,
)
from palace.manager.integration.license.boundless.model.validators import (
    BoundlessRuntime,
    BoundlessStringList,
    BoundlessXmlDate,
    BoundlessXmlDateTime,
)


class BaseBoundlessXmlModel(
    BaseXmlModel,
    nsmap={"": "http://axis360api.baker-taylor.com/vendorAPI"},
    search_mode="unordered",
):
    """
    Base for Boundless (Axis 360) XML models.
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        validate_by_name=True,
    )

    @classmethod
    def from_xml(
        cls,
        source: str | bytes,
        context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Self:
        """
        Parse XML source into a Pydantic model.

        We override the `from_xml` method to ensure that we use a lenient XML parser
        that can recover from errors, as Boundless XML responses can be quite messy.
        """

        if "parser" not in kwargs:
            kwargs["parser"] = etree.XMLParser(recover=True)
        try:
            return super().from_xml(source, context, **kwargs)
        except AttributeError:
            # Because we are using a very lenient XML parser, we can end up with
            # None coming back from the XML parser, which causes an AttributeError
            # within the pydantic_xml library. We catch this here and raise a
            # ParsingError instead, which is more appropriate for the situation.
            raise ParsingError()


class BaseBoundlessXmlResponse(BaseBoundlessXmlModel, BaseBoundlessResponse, ABC):
    """
    Base for API XML responses.
    """


class Checkout(BaseBoundlessXmlModel):
    """
    A checkout for a patron.

    Used as part of the Availability model. It is based on "Boundless Vendor API v3.0 - Palace.pdf".
    This model is documented in section 2.14.5.6.
    """

    model_config = ConfigDict(
        # For some reason this part of the response uses PascalCase instead of camelCase
        alias_generator=to_pascal,
    )

    patron: str = element()
    start_date: BoundlessXmlDateTime = element()
    end_date: BoundlessXmlDateTime = element()
    format: str = element()
    active: bool = element()


class Hold(BaseBoundlessXmlModel):
    """
    A hold for a patron.

    Used as part of the Availability model. It is based on "Boundless Vendor API v3.0 - Palace.pdf".
    This model is documented in section 2.14.5.7.
    """

    model_config = ConfigDict(
        # For some reason this part of the response uses PascalCase instead of camelCase
        alias_generator=to_pascal,
    )

    patron: str = element()

    # The documentation says this field is required, but its often missing or empty in the response,
    # so we make it optional.
    email: str | None = element(default=None)

    hold_date: BoundlessXmlDateTime = element()
    reserved: bool = element()


class Availability(BaseBoundlessXmlModel):
    """
    Availability information for a title.

    Used as part of the Title model. It is based on "Boundless Vendor API v3.0 - Palace.pdf".
    This model is documented in section 2.14.5.3.
    """

    available_formats: list[str] = wrapped(
        "availableFormats", element(tag="formatName", default=[])
    )
    available_copies: NonNegativeInt = element()
    total_copies: NonNegativeInt = element()
    holds_queue_size: NonNegativeInt = element()
    holds_queue_position: NonNegativeInt | None = element(default=None)
    is_in_hold_queue: bool = element(default=False)
    is_reserved: bool = element(default=False)
    reserved_end_date: BoundlessXmlDateTime | None = element(default=None)
    is_checked_out: bool = element(default=False, tag="isCheckedout")
    checkout_format: str | None = element(default=None)
    download_url: str | None = element(default=None)
    transaction_id: str | None = element(default=None, tag="transactionID")
    checkout_start_date: BoundlessXmlDateTime | None = element(default=None)
    checkout_end_date: BoundlessXmlDateTime | None = element(default=None)
    update_date: BoundlessXmlDateTime | None = element(default=None)

    # Note: The inconsistency between the `Checkouts` and `checkout` tag. This isn't
    # in the API documentation, but is present in the responses we receive.
    checkouts: list[Checkout] = wrapped(
        "Checkouts", element(tag="checkout", default=[])
    )
    holds: list[Hold] = wrapped("Holds", element(tag="Hold", default=[]))

    @cached_property
    def available_formats_normalized(self) -> list[str]:
        """
        Normalize the available formats to remove the deprecated "Blio" format.
        """
        # We use a dict here as an ordered set, so we can remove duplicates,
        # while also preserving the order of the formats.
        available_formats = dict.fromkeys(self.available_formats)
        if BoundlessFormat.blio in available_formats:
            del available_formats[BoundlessFormat.blio]
            available_formats[BoundlessFormat.axis_now] = None

        return list(available_formats.keys())

    @cached_property
    def checkout_format_normalized(self) -> str | None:
        """
        Normalize the checkout format to remove the deprecated "Blio" format.
        """
        if self.checkout_format is None:
            return None

        # The "Blio" format is deprecated and handled the same way as "AxisNow"
        if self.checkout_format == BoundlessFormat.blio:
            return BoundlessFormat.axis_now

        return self.checkout_format


class Title(
    BaseBoundlessXmlModel,
):
    """
    Information about a title.

    Used as part of the AvailabilityResponse model. It is based on "Boundless Vendor API v3.0 - Palace.pdf".
    This model is documented in section 2.14.5.2.
    """

    title_id: str = element()
    product_title: str = element()

    # This is not documented in the API documentation, but it is present in the response and was
    # used by our previous Axis parser implementation.
    contributors: BoundlessStringList = element(default_factory=list, tag="contributor")

    isbn: str = element()
    subjects: BoundlessStringList = element(default_factory=list, tag="subject")
    series: str | None = element(default=None)
    publisher: str | None = element(default=None)
    language: str = element()
    audience: str | None = element(default=None)
    imprint: str | None = element(default=None)

    # This is the subtitle of the book.
    annotation: str | None = element(default=None)

    # This is not documented in the API documentation, but it is present in the response and was
    # used by our previous Axis parser implementation.
    narrators: BoundlessStringList = element(default_factory=list, tag="narrator")

    runtime: BoundlessRuntime | None = element(default=None)
    publication_date: BoundlessXmlDate | None = element(default=None)
    availability: Availability = element()
    min_loan_period: PositiveInt = element()
    max_loan_period: PositiveInt = element()
    default_loan_period: PositiveInt = element()
    image_url: str | None = element(default=None)

    # The API documentation describes this as: "Is the title an Audio or Video title"
    # However, in practice, this field is used to indicate if the title is an eBook (EBT)
    # or an audiobook (ABT).
    format_type: Literal["EBT", "ABT"] | None = element(default=None)


class Status(BaseBoundlessXmlModel):
    """
    The status of an XML API response.

    This is included in all the API XML responses to indicate the success or failure
    of the request. It contains a status code and a message.

    It is based on "Boundless Vendor API v3.0 - Palace.pdf". This is defined and redefined in
    various sections of the document.
    """

    code: int = element()

    # The status message is required according to the API documentation, but it is
    # missing from some responses we have seen in practice, so we make it optional.
    message: str | None = element(default=None, tag="statusMessage")

    def raise_on_error(
        self,
        *,
        custom_error_classes: ErrorLookupType | None = None,
        ignore_error_codes: list[int] | None = None,
    ) -> None:
        StatusResponseParser.raise_on_error(
            self.code, self.message, custom_error_classes, ignore_error_codes
        )


class AvailabilityResponse(
    BaseBoundlessXmlResponse,
    tag="availabilityResponse",
):
    """
    Response from the Availability Endpoint.

    It is based on "Boundless Vendor API v3.0 - Palace.pdf". This model is documented in
    section 2.14.5.1.

    Note: This model is not complete, we only include the fields that are used in our
    implementation. The API documentation contains more fields that are not included here.
    """

    titles: list[Title] = element(default_factory=list, tag="title")
    status: Status = element()

    def raise_on_error(self) -> None:
        self.status.raise_on_error()


class EarlyCheckinResponse(
    BaseBoundlessXmlResponse,
    tag="EarlyCheckinRestResponse",
):
    """
    Response from the Early Check In Endpoint.

    It is based on "Boundless Vendor API v3.0 - Palace.pdf". This model is documented in
    section 2.18.5.
    """

    status: Status = wrapped("EarlyCheckinRestResult", element())

    def raise_on_error(self) -> None:
        # We ignore error code 4058, which is the error code for "Item not checked out"
        # since for an early checkin, this can be safely ignored.
        self.status.raise_on_error(ignore_error_codes=[4058])


class CheckoutResponse(
    BaseBoundlessXmlResponse,
    tag="checkoutResponse",
):
    """
    Response from the Checkout Endpoint.

    It is based on "Boundless Vendor API v3.0 - Palace.pdf". This model is documented in
    section 2.4.5.
    """

    status: Status = wrapped("checkoutResult", element())
    expiration_date: BoundlessXmlDateTime | None = wrapped(
        "checkoutResult", element(default=None)
    )

    def raise_on_error(self) -> None:
        self.status.raise_on_error()


class AddHoldResponse(
    BaseBoundlessXmlResponse,
    tag="addtoholdResponse",
):
    """
    Response from the Add to Hold Endpoint.

    It is based on "Boundless Vendor API v3.0 - Palace.pdf". This model is documented in
    section 2.5.5.
    """

    status: Status = wrapped("addtoholdResult", element())
    holds_queue_position: NonNegativeInt | None = wrapped(
        "addtoholdResult", element(default=None)
    )

    def raise_on_error(self) -> None:
        self.status.raise_on_error(custom_error_classes={3109: AlreadyOnHold})


class RemoveHoldResponse(
    BaseBoundlessXmlResponse,
    tag="removeholdResponse",
):
    """
    Response from the Remove Hold endpoint.

    It is based on "Boundless Vendor API v3.0 - Palace.pdf". This model is documented in
    section 2.6.5.
    """

    status: Status = wrapped("removeholdResult", element())

    def raise_on_error(self) -> None:
        # We ignore error code 3109, which is the error code for "Item not on hold"
        # since for a remove hold, this can be safely ignored.
        self.status.raise_on_error(ignore_error_codes=[3109])
