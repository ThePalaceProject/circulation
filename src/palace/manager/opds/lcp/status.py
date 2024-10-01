import sys
from enum import auto
from functools import cached_property

from pydantic import AwareDatetime, Field, field_validator

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.opds.base import BaseLink, BaseOpdsModel, ListOfLinks

# TODO: Remove this when we drop support for Python 3.10
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


class Link(BaseLink):
    """
    https://readium.org/lcp-specs/releases/lsd/latest#25-links
    """

    title: str | None = None
    profile: str | None = None


class Status(StrEnum):
    """
    https://readium.org/lcp-specs/releases/lsd/latest.html#23-status-of-a-license
    """

    READY = auto()
    ACTIVE = auto()
    REVOKED = auto()
    RETURNED = auto()
    CANCELLED = auto()
    EXPIRED = auto()


class Updated(BaseOpdsModel):
    """
    https://readium.org/lcp-specs/releases/lsd/latest#24-timestamps
    """

    license: AwareDatetime
    status: AwareDatetime


class PotentialRights(BaseOpdsModel):
    """ "
    https://readium.org/lcp-specs/releases/lsd/latest#26-potential-rights
    """

    end: AwareDatetime | None = None


class EventType(StrEnum):
    """
    https://readium.org/lcp-specs/releases/lsd/latest#27-events
    """

    REGISTER = auto()
    RENEW = auto()
    RETURN = auto()
    REVOKE = auto()
    CANCEL = auto()


class Event(BaseOpdsModel):
    """
    https://readium.org/lcp-specs/releases/lsd/latest#27-events
    """

    type: EventType
    name: str
    id: str
    timestamp: AwareDatetime


class LoanStatus(BaseOpdsModel):
    """
    This document is defined as part of the Readium LCP Specifications.

    Readium calls this the License Status Document (LSD), however, that
    name conflates the concept of License. In the context of ODL and library
    lends, it's really the loan status document, so we use that name here.

    The spec for it is located here:
    https://readium.org/lcp-specs/releases/lsd/latest.html
    """

    @staticmethod
    def content_type() -> str:
        return "application/vnd.readium.license.status.v1.0+json"

    id: str
    status: Status
    message: str
    updated: Updated
    links: ListOfLinks[Link]
    potential_rights: PotentialRights = Field(default_factory=PotentialRights)
    events: list[Event] = Field(default_factory=list)

    @field_validator("links")
    @classmethod
    def _validate_links(cls, value: ListOfLinks[Link]) -> ListOfLinks[Link]:
        if value.get(rel="license") is None:
            raise PalaceValueError("links must contain a link with rel='license'")
        return value

    @cached_property
    def active(self) -> bool:
        return self.status in [Status.READY, Status.ACTIVE]
