import sys
from enum import auto
from functools import cached_property

from pydantic import Field

from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.types.date import Iso8601AwareDatetime
from palace.manager.opds.types.link import BaseLink, CompactCollection

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

    license: Iso8601AwareDatetime
    status: Iso8601AwareDatetime


class PotentialRights(BaseOpdsModel):
    """ "
    https://readium.org/lcp-specs/releases/lsd/latest#26-potential-rights
    """

    end: Iso8601AwareDatetime | None = None


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
    timestamp: Iso8601AwareDatetime

    # The spec isn't clear if these fields are required, but DeMarque does not
    # provide id in their event data.
    id: str | None = None
    device: str | None = None


class LoanStatus(BaseOpdsModel):
    """
    This document is defined as part of the Readium LCP Specifications.

    Readium calls this the License Status Document (LSD), however, that
    name conflates the concept of License. In the context of ODL and library
    lends, it's really the loan status document, so we use that name here.

    The spec for it is located here:
    https://readium.org/lcp-specs/releases/lsd/latest.html

    Technically the spec says that there must be at lease one link
    with rel="license" but this is not always the case in practice,
    especially when the license is returned or revoked. So we don't
    enforce that here.
    """

    @staticmethod
    def content_type() -> str:
        return "application/vnd.readium.license.status.v1.0+json"

    id: str
    status: Status
    message: str
    updated: Updated
    links: CompactCollection[Link]
    potential_rights: PotentialRights = Field(default_factory=PotentialRights)
    events: list[Event] = Field(default_factory=list)

    @cached_property
    def active(self) -> bool:
        return self.status in [Status.READY, Status.ACTIVE]
