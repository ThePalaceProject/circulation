import sys
from enum import auto
from functools import cached_property

from pydantic import AwareDatetime, Field, NonNegativeInt

from palace.manager.opds.base import BaseOpdsModel, obj_or_set_to_set
from palace.manager.opds.odl.odl import Protection, Terms
from palace.manager.opds.opds import Price

# TODO: Remove this when we drop support for Python 3.10
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


class Status(StrEnum):
    """
    https://drafts.opds.io/odl-1.0.html#41-syntax
    """

    PREORDER = auto()
    AVAILABLE = auto()
    UNAVAILABLE = auto()


class Loan(BaseOpdsModel):
    """
    https://drafts.opds.io/odl-1.0.html#41-syntax
    """

    href: str
    id: str
    # We alias 'patron' here because the ODL documentation
    # requires the field to be named `patron_id` but
    # DeMarque returns a field named `patron`.
    patron_id: str = Field(validation_alias="patron")
    expires: AwareDatetime


class Checkouts(BaseOpdsModel):
    """
    https://drafts.opds.io/odl-1.0.html#41-syntax
    """

    left: NonNegativeInt | None = None
    available: NonNegativeInt
    active: list[Loan] = Field(default_factory=list)


class LicenseInfo(BaseOpdsModel):
    """
    This document is defined in the ODL specification:
    https://drafts.opds.io/odl-1.0.html#4-license-info-document
    """

    @staticmethod
    def content_type() -> str:
        return "application/vnd.odl.info+json"

    identifier: str
    status: Status
    checkouts: Checkouts
    format: frozenset[str] | str
    created: AwareDatetime | None = None
    terms: Terms = Field(default_factory=Terms)
    protection: Protection = Field(default_factory=Protection)
    price: Price | None = None
    source: str | None = None

    @cached_property
    def formats(self) -> frozenset[str]:
        return obj_or_set_to_set(self.format)

    @cached_property
    def active(self) -> bool:
        return self.status == Status.AVAILABLE
