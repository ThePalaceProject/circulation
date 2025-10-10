from collections.abc import Sequence
from functools import cached_property
from typing import Annotated, Any

from pydantic import Discriminator, Field, Tag, field_validator

from palace.manager.opds import opds2, rwpm
from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.opds.odl.info import LicenseInfo
from palace.manager.opds.odl.protection import Protection
from palace.manager.opds.odl.terms import Terms
from palace.manager.opds.types.date import Iso8601AwareDatetime
from palace.manager.opds.types.link import CompactCollection
from palace.manager.opds.util import StrOrTuple, obj_or_tuple_to_tuple


class LicenseMetadata(BaseOpdsModel):
    """
    https://drafts.opds.io/odl-1.0#32-describing-a-license
    """

    identifier: str
    format: StrOrTuple[str] = tuple()

    @cached_property
    def formats(self) -> Sequence[str]:
        return obj_or_tuple_to_tuple(self.format)

    created: Iso8601AwareDatetime
    terms: Terms = Field(default_factory=Terms)
    protection: Protection = Field(default_factory=Protection)
    price: opds2.Price | None = None
    source: str | None = None

    # OPDS2 + ODL proposed property. See here for more detail:
    # https://github.com/opds-community/drafts/discussions/63
    availability: opds2.Availability = Field(default_factory=opds2.Availability)


class License(BaseOpdsModel):
    """
    https://drafts.opds.io/odl-1.0#31-serialization
    """

    metadata: LicenseMetadata
    links: Annotated[CompactCollection[opds2.StrictLink], Field(min_length=1)]

    @field_validator("links")
    @classmethod
    def validate_links(
        cls, value: CompactCollection[opds2.StrictLink]
    ) -> CompactCollection[opds2.StrictLink]:
        """
        Must have a self link and at least one acquisition link.

        https://drafts.opds.io/odl-1.0#4-license-info-document
        https://drafts.opds.io/odl-1.0#5-checkouts
        """
        # Make sure we have a self link
        value.get(
            rel=rwpm.LinkRelations.self, type=LicenseInfo.content_type(), raising=True
        )

        # Make sure we have at least one acquisition link
        value.get(
            rel=opds2.AcquisitionLinkRelations.borrow,
            type=LoanStatus.content_type(),
            raising=True,
        )

        return value


class Publication(opds2.BasePublication):
    """
    OPDS2 + ODL publication.

    https://drafts.opds.io/odl-1.0#21-opds-20
    https://drafts.opds.io/odl-1.0#3-licenses
    """

    licenses: Annotated[list[License], Field(min_length=1)]


def _get_publication_type(v: dict[str, Any] | Publication | opds2.Publication) -> str:
    """
    Discriminator function to choose which publication type
    pydantic should parse for the union type. This increases
    our parsing performance.

    See: https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions-with-callable-discriminator
    """
    if isinstance(v, dict) and "licenses" in v or isinstance(v, Publication):
        return "OdlPublication"
    return "Opds2Publication"


Opds2OrOpds2WithOdlPublication = Annotated[
    Annotated[Publication, Tag("OdlPublication")]
    | Annotated[opds2.Publication, Tag("Opds2Publication")],
    Discriminator(_get_publication_type),
]


class Feed(opds2.BasePublicationFeed[Opds2OrOpds2WithOdlPublication]):
    """
    OPDS2 + ODL feed.

    This is a collection of either OPDS2 publications or ODL publications. And
    allows harvesting of both types of publications in a single feed.
    """
