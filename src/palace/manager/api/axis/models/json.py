from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    PositiveInt,
    Tag,
    TypeAdapter,
)
from pydantic.alias_generators import to_pascal

from palace.manager.api.axis.exception import ErrorLookupType, StatusResponseParser
from palace.manager.api.axis.models.base import BaseAxisResponse
from palace.manager.api.axis.models.validators import AxisJsonDateTime
from palace.manager.util.datetime_helpers import utc_now


class BaseAxisJsonModel(BaseModel):
    """
    Base model for Axis JSON models.
    """

    model_config = ConfigDict(
        alias_generator=to_pascal,
        validate_by_name=True,
    )


class BaseAxisJsonResponse(BaseAxisJsonModel, BaseAxisResponse):
    """
    Base model for Axis JSON API responses.
    """

    status: Status

    def raise_on_error(self) -> None:
        self.status.raise_on_error()


class Status(BaseAxisJsonModel):
    """
    The JSON version of the Axis status response information.

    This is included with all the Axis API responses, and gives information about
    the success or failure of the request.
    """

    code: int
    message: str

    def raise_on_error(
        self,
        *,
        custom_error_classes: ErrorLookupType | None = None,
        ignore_error_codes: list[int] | None = None,
    ) -> None:
        StatusResponseParser.raise_on_error(
            self.code, self.message, custom_error_classes, ignore_error_codes
        )


class FindawayFulfillmentInfoResponse(BaseAxisJsonResponse):
    """
    The Findaway fulfillment info API response.

    This is entirely undocumented. The fields are based on what we have seen
    from real API responses, and based on the old parser code.
    """

    content_id: str = Field(..., alias="FNDContentID")
    license_id: str = Field(..., alias="FNDLicenseID")
    session_key: str = Field(..., alias="FNDSessionKey")
    transaction_id: str = Field(..., alias="FNDTransactionID")

    expiration_date: AxisJsonDateTime


class AxisNowFulfillmentInfoResponse(BaseAxisJsonResponse):
    """
    The AxisNow fulfillment info API response.

    Like the Findaway response, this is undocumented, and the fields are based
    on observed API responses and the old parser code.
    """

    isbn: str = Field(..., alias="ISBN")
    book_vault_uuid: str = Field(..., alias="BookVaultUUID")

    expiration_date: AxisJsonDateTime


def _fulfillment_info_discriminator(v: Any) -> str | None:
    if isinstance(v, dict):
        return "findaway" if "FNDContentID" in v else "axisnow"
    return None


FulfillmentInfoResponseT = Annotated[
    Annotated[FindawayFulfillmentInfoResponse, Tag("findaway")]
    | Annotated[AxisNowFulfillmentInfoResponse, Tag("axisnow")],
    Discriminator(_fulfillment_info_discriminator),
]
"""
The fulfillment info endpoint can return two different types of responses,
depending on whether the request is for a Findaway audiobook or an AxisNow ebook.

This type is a union of the two possible response types, with a pydantic discriminator
to determine which type to use based on the presence of specific fields in the response.
"""


FulfillmentInfoResponse: TypeAdapter[FulfillmentInfoResponseT] = TypeAdapter(
    FulfillmentInfoResponseT
)
"""
A pydantic TypeAdapter for the FulfillmentInfoResponseT type.
"""


class AudiobookMetadataReadingOrder(BaseAxisJsonModel):
    model_config = ConfigDict(
        alias_generator=None,
    )

    title: str
    duration: float = 0.0
    part: int = Field(0, alias="fndpart")
    sequence: int = Field(0, alias="fndsequence")


class AudiobookMetadataResponse(BaseAxisJsonResponse):
    """
    This response is returned by the Axis audiobook metadata API endpoint.

    It is not documented in any api documentation we have been provided. This
    model is based on the observed API responses and the old parser code.
    """

    account_id: str = Field(..., alias="fndaccountid")
    reading_order: list[AudiobookMetadataReadingOrder] = Field(
        ..., alias="readingOrder"
    )


class Token(BaseAxisJsonModel):
    """
    Represents a bearer token response from the Axis API.

    This model provides some helper methods to check if the token is expired.
    """

    model_config = ConfigDict(
        alias_generator=None,
        frozen=True,
    )

    access_token: str
    expires_in: PositiveInt
    token_type: Literal["Bearer"]

    _expires_at: datetime

    def model_post_init(self, context: Any, /) -> None:
        # We set the expiration time to 95% of the expires_in value
        # to account for any potential delays in processing, so we
        # will get a new token before the current one expires.
        self._expires_at = utc_now() + timedelta(seconds=self.expires_in * 0.95)

    @property
    def expired(self) -> bool:
        """
        Returns True if the token is expired.
        """
        return utc_now() >= self._expires_at
