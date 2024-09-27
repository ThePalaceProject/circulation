from enum import auto
from functools import cached_property

from backports.strenum import StrEnum
from pydantic import AwareDatetime, Field, HttpUrl, NonNegativeInt

from palace.manager.opds.base import BaseOpdsModel, Price, obj_or_set_to_set


class OdlTerms(BaseOpdsModel):
    checkouts: NonNegativeInt | None = None
    expires: AwareDatetime | None = None
    concurrency: NonNegativeInt | None = None
    length: NonNegativeInt | None = None


class OdlProtection(BaseOpdsModel):
    format: set[str] | str = Field(default_factory=set)
    devices: int | None = None
    # This is aliased because 'copy' is a method on BaseModel, the
    # other fields are aliased for consistency.
    allow_copy: bool = Field(True, alias="copy")
    allow_print: bool = Field(True, alias="print")
    allow_tts: bool = Field(True, alias="tts")

    @cached_property
    def formats(self) -> set[str]:
        return obj_or_set_to_set(self.format)


class LicenseInfoStatus(StrEnum):
    PREORDER = auto()
    AVAILABLE = auto()
    UNAVAILABLE = auto()


class LicenseInfoLoan(BaseOpdsModel):
    href: HttpUrl
    id: str
    # We alias 'patron' here because the ODL documentation
    # https://drafts.opds.io/odl-1.0.html#41-syntax
    # requires the field to be named `patron_id` but
    # DeMarque returns a field named `patron`.
    patron_id: str = Field(validation_alias="patron")
    expires: AwareDatetime


class LicenseInfoCheckouts(BaseOpdsModel):
    left: NonNegativeInt | None = None
    available: NonNegativeInt
    active: list[LicenseInfoLoan] = Field(default_factory=list)

    format: set[str] | str = Field(default_factory=set)
    created: AwareDatetime | None = None
    price: Price | None = None
    source: str | None = None

    @cached_property
    def formats(self) -> set[str]:
        return obj_or_set_to_set(self.format)


class LicenseInfoDocument(BaseOpdsModel):
    """
    This document is defined in the ODL specification:
    https://drafts.opds.io/odl-1.0.html#4-license-info-document
    """

    _content_type: str = "application/vnd.odl.info+json"

    identifier: str
    status: LicenseInfoStatus
    checkouts: LicenseInfoCheckouts
    format: set[str] | str
    created: AwareDatetime | None = None
    terms: OdlTerms = Field(default_factory=OdlTerms)
    protection: OdlProtection = Field(default_factory=OdlProtection)
    price: Price | None = None
    source: str | None = None

    @cached_property
    def formats(self) -> set[str]:
        return obj_or_set_to_set(self.format)
