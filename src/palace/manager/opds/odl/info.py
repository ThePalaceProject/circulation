from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from functools import cached_property

from pydantic import Field, NonNegativeInt

from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.odl.protection import Protection
from palace.manager.opds.odl.terms import Terms
from palace.manager.opds.opds2 import Price
from palace.manager.opds.types.date import Iso8601AwareDatetime
from palace.manager.opds.util import StrOrTuple, obj_or_tuple_to_tuple


class LicenseStatus(Enum):
    preorder = "preorder"
    available = "available"
    unavailable = "unavailable"

    @classmethod
    def get(cls, value: str) -> LicenseStatus:
        return cls.__members__.get(value.lower(), cls.unavailable)


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
    expires: Iso8601AwareDatetime


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
    status: LicenseStatus
    checkouts: Checkouts
    format: StrOrTuple[str] = tuple()

    @cached_property
    def formats(self) -> Sequence[str]:
        return obj_or_tuple_to_tuple(self.format)

    created: Iso8601AwareDatetime | None = None
    terms: Terms = Field(default_factory=Terms)
    protection: Protection = Field(default_factory=Protection)
    price: Price | None = None
    source: str | None = None

    @cached_property
    def active(self) -> bool:
        return self.status == LicenseStatus.available
