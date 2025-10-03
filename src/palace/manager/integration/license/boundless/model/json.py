from __future__ import annotations

from functools import cached_property
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    NonNegativeInt,
    Tag,
    TypeAdapter,
    field_validator,
)
from pydantic.alias_generators import to_pascal

from palace.manager.integration.license.boundless.exception import (
    ErrorLookupType,
    StatusResponseParser,
)
from palace.manager.integration.license.boundless.model.base import (
    BaseBoundlessResponse,
)
from palace.manager.integration.license.boundless.model.validators import (
    BoundlessJsonDateTime,
)


class BaseBoundlessJsonModel(BaseModel):
    """
    Base model for Boundless (Axis 360) JSON models.
    """

    model_config = ConfigDict(
        alias_generator=to_pascal,
        validate_by_name=True,
    )


class BaseBoundlessJsonResponse(BaseBoundlessJsonModel, BaseBoundlessResponse):
    """
    Base model for JSON API responses.
    """

    status: Status

    def raise_on_error(self) -> None:
        self.status.raise_on_error()


class Status(BaseBoundlessJsonModel):
    """
    The JSON version of the status response information.

    This is included with all the API responses, and gives information about
    the success or failure of the request.
    """

    code: int
    message: str | None = None

    def raise_on_error(
        self,
        *,
        custom_error_classes: ErrorLookupType | None = None,
        ignore_error_codes: list[int] | None = None,
    ) -> None:
        StatusResponseParser.raise_on_error(
            self.code, self.message, custom_error_classes, ignore_error_codes
        )


class FindawayFulfillmentInfoResponse(BaseBoundlessJsonResponse):
    """
    The Findaway fulfillment info API response.

    This is entirely undocumented. The fields are based on what we have seen
    from real API responses, and based on the old parser code.
    """

    content_id: str = Field(..., alias="FNDContentID")
    license_id: str = Field(..., alias="FNDLicenseID")
    session_key: str = Field(..., alias="FNDSessionKey")
    transaction_id: str = Field(..., alias="FNDTransactionID")

    expiration_date: BoundlessJsonDateTime


class AxisNowFulfillmentInfoResponse(BaseBoundlessJsonResponse):
    """
    The AxisNow fulfillment info API response.

    Like the Findaway response, this is undocumented, and the fields are based
    on observed API responses and the old parser code.
    """

    isbn: str = Field(..., alias="ISBN")
    book_vault_uuid: str = Field(..., alias="BookVaultUUID")

    expiration_date: BoundlessJsonDateTime


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


class AudiobookMetadataReadingOrder(BaseBoundlessJsonModel):
    model_config = ConfigDict(
        alias_generator=None,
    )

    title: str
    duration: float = 0.0
    part: int = Field(0, alias="fndpart")
    sequence: int = Field(0, alias="fndsequence")


class AudiobookMetadataResponse(BaseBoundlessJsonResponse):
    """
    This response is returned by the audiobook metadata API endpoint.

    It is not documented in any api documentation we have been provided. This
    model is based on the observed API responses and the old parser code.
    """

    account_id: str = Field(..., alias="fndaccountid")
    reading_order: list[AudiobookMetadataReadingOrder] = Field(
        ..., alias="readingOrder"
    )


class Title(BaseBoundlessJsonModel):
    """
    A title record in the Title License response.

    This is based on the `Boundless Vendor API- TitlelicenseV3.docx` document.

    There is more data in this record that isn't being parsed, as we don't need
    it currently, and the data is messy and not well-documented. Refer to the
    documentation for more details of the data that is available.
    """

    title_id: str = Field(..., alias="TitleID")
    active: bool


class Pagination(BaseBoundlessJsonModel):
    """
    Pagination information for the Title License response.
    """

    current_page: NonNegativeInt = Field(..., alias="currentPage")
    page_size: NonNegativeInt = Field(..., alias="pageSize")
    total_count: NonNegativeInt = Field(..., alias="totalCount")
    total_page: NonNegativeInt = Field(..., alias="totalPage")


class TitleLicenseResponse(BaseBoundlessJsonResponse):
    """
    The Title License API response.

    This returns title license information based on a modified date,
    allowing retrieval of updated content since the last retrieval.
    """

    pagination: Pagination
    titles: list[Title]

    # A pre-validation step to ensure that titles is always a list, even if the API
    # returns a null.
    @field_validator("titles", mode="before")
    @classmethod
    def _ensure_titles_list(cls, value: Any) -> Any:
        if value is None:
            return []
        return value

    # A pre-validation step to ensure that we always have a pagination object,
    # even if the API returns a null.
    @field_validator("pagination", mode="before")
    @classmethod
    def _ensure_pagination_object(cls, value: Any) -> Any:
        if value is None:
            return Pagination(
                current_page=0,
                page_size=0,
                total_count=0,
                total_page=0,
            )
        return value


class LicenseServerStatus(BaseBoundlessJsonModel):
    """
    A different style of status response, given by the Boundless license server.

    This is semi-documented in "Baker and Taylor KDRM Enhanced Implemtation - AxisNow Node-1.pdf" (sic),
    but the actual response format that is given in that document does not match the actual responses
    that we see coming back.

    This model is based on the observed API responses.
    """

    model_config = ConfigDict(
        frozen=True,
    )

    code: int = Field(..., alias="ReturnCode")
    title: str
    messages: list[str] = Field(default_factory=list)

    @field_validator("messages", mode="after")
    @classmethod
    def _strip_empty_messages(cls, value: list[str]) -> list[str]:
        """
        In most of the responses we have observed, the messages field is a list of strings,
        where some or all of them are empty or whitespace-only. This validator strips those empty strings
        from the list to ensure that we only have meaningful messages.
        """
        return [stripped for message in value if (stripped := message.strip())]

    @cached_property
    def message(self) -> str:
        """
        Returns a single string containing all the messages.
        """
        return " ".join(self.messages)
