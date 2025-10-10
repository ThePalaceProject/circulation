from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from enum import StrEnum, auto
from functools import cached_property
from typing import Annotated, Any, Generic, Self, TypeVar

from pydantic import (
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from palace.manager.opds import rwpm
from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.palace import PalacePublicationMetadata
from palace.manager.opds.types.currency import CurrencyCode
from palace.manager.opds.types.date import Iso8601AwareDatetime
from palace.manager.opds.types.language import LanguageMap
from palace.manager.opds.types.link import CompactCollection, LinkT
from palace.manager.opds.util import StrOrTuple, obj_or_tuple_to_tuple
from palace.manager.util.datetime_helpers import utc_now


def validate_self_link(value: CompactCollection[LinkT]) -> CompactCollection[LinkT]:
    """
    Must have a self link.
    """
    value.get(rel=rwpm.LinkRelations.self, raising=True)
    return value


def validate_images(value: CompactCollection[LinkT]) -> CompactCollection[LinkT]:
    """
    Must have at least one image link. This is not mentioned in the spec,
    but is enforced in the json schema, so we enforce it here as well.

    https://drafts.opds.io/opds-2.0
    https://github.com/opds-community/drafts/blob/main/schema/publication.schema.json
    """
    image_types = ["image/jpeg", "image/avif", "image/png", "image/gif"]
    if not any([value.get_collection(type=t) for t in image_types]):
        raise ValueError(
            "At least one image resource must use one of the following formats: "
            f"{', '.join(image_types)}"
        )
    return value


class Price(BaseOpdsModel):
    """
    https://drafts.opds.io/opds-2.0#53-acquisition-links

    https://drafts.opds.io/schema/properties.schema.json
    """

    currency: CurrencyCode
    value: NonNegativeFloat


class AcquisitionObject(BaseOpdsModel):
    """
    https://drafts.opds.io/opds-2.0#53-acquisition-links

    https://drafts.opds.io/schema/acquisition-object.schema.json
    """

    type: str
    child: list[AcquisitionObject] | None = None

    @cached_property
    def children(self) -> Sequence[AcquisitionObject]:
        return obj_or_tuple_to_tuple(self.child)


class Holds(BaseOpdsModel):
    """
    https://drafts.opds.io/schema/properties.schema.json
    """

    total: NonNegativeInt | None = None
    position: NonNegativeInt | None = None


class Copies(BaseOpdsModel):
    """
    https://drafts.opds.io/schema/properties.schema.json
    """

    total: NonNegativeInt | None = None
    available: NonNegativeInt | None = None


class AvailabilityState(StrEnum):
    available = auto()
    unavailable = auto()
    reserved = auto()
    ready = auto()


class Availability(BaseOpdsModel):
    """
    https://drafts.opds.io/schema/properties.schema.json
    """

    state: AvailabilityState = AvailabilityState.available
    since: Iso8601AwareDatetime | None = None
    until: Iso8601AwareDatetime | None = None

    @property
    def available(self) -> bool:
        """
        Does the data indicate that this item is available?

        We default being available if no availability information is provided or if the provided
        availability information is past the time specified in its `until` field. The `since` field on the
        availability information is not used, it is assumed to be informational and always in the past if it is
        present. This is based on a discussion with the OPDS 2.0 working group.

        TODO: Update our handling of the `since` field based on the resolution of the discussion here:
          https://github.com/opds-community/drafts/discussions/63#discussioncomment-9806140
        """

        return self.state == AvailabilityState.available or (
            self.until is not None and self.until < utc_now()
        )

    @field_validator("since", mode="after")
    @classmethod
    def _check_past_datetime(cls, value: datetime | None) -> datetime | None:
        """Validate that the datetime is in the past."""
        if value is not None and value > utc_now():
            raise PydanticCustomError(
                "past_datetime",
                "Datetime must be in the past",
            )
        return value


class LinkProperties(rwpm.LinkProperties):
    """
    OPDS2 extensions to the link properties.

    https://drafts.opds.io/schema/properties.schema.json
    """

    number_of_items: NonNegativeInt | None = Field(None, alias="numberOfItems")
    price: Price | None = None
    indirect_acquisition: list[AcquisitionObject] = Field(
        default_factory=list, alias="indirectAcquisition"
    )
    holds: Holds = Field(default_factory=Holds)
    copies: Copies = Field(default_factory=Copies)
    availability: Availability = Field(default_factory=Availability)


class Link(rwpm.Link):
    """
    OPDS2 link.

    https://drafts.opds.io/opds-2.0#53-acquisition-links
    """

    properties: LinkProperties = Field(default_factory=LinkProperties)


class StrictLink(Link):
    """
    OPDS2 link with strict validation.

    These links require that the rel and type fields are present.
    """

    rel: StrOrTuple[str]
    type: str

    alternate: CompactCollection[StrictLink] = Field(default_factory=CompactCollection)
    children: CompactCollection[StrictLink] = Field(default_factory=CompactCollection)


class TitleLink(Link):
    """
    OPDS2 link with title.
    """

    title: str

    alternate: CompactCollection[TitleLink] = Field(default_factory=CompactCollection)
    children: CompactCollection[TitleLink] = Field(default_factory=CompactCollection)


class FeedMetadata(BaseOpdsModel):
    """
    The written specification for OPDS2 seems to indicate that this should be
    RWPM metadata, but the json schema for OPDS2 has it as a separate object,
    so we implement it as a separate object.

    https://github.com/opds-community/drafts/blob/main/schema/feed-metadata.schema.json
    """

    title: LanguageMap
    type: str | None = Field(None, alias="@type")
    subtitle: LanguageMap | None = None
    modified: Iso8601AwareDatetime | None = None
    description: str | None = None

    items_per_page: PositiveInt | None = Field(None, alias="itemsPerPage")
    current_page: PositiveInt | None = Field(None, alias="currentPage")
    number_of_items: NonNegativeInt | None = Field(None, alias="numberOfItems")


class PublicationMetadata(PalacePublicationMetadata, rwpm.Metadata):
    """
    OPDS2 publication metadata.

    This doesn't have an actual specification document, but its a combination
    of the Palace OPDS2 extensions and OPDS2 proposed extensions, along with
    the normal RWPM metadata.
    """

    # OPDS2 proposed property. See here for more detail:
    # https://github.com/opds-community/drafts/discussions/63
    availability: Availability = Field(default_factory=Availability)


class AcquisitionLinkRelations(StrEnum):
    """
    https://drafts.opds.io/opds-2.0#53-acquisition-links
    """

    acquisition = "http://opds-spec.org/acquisition"
    open_access = "http://opds-spec.org/acquisition/open-access"
    borrow = "http://opds-spec.org/acquisition/borrow"
    buy = "http://opds-spec.org/acquisition/buy"
    sample = "http://opds-spec.org/acquisition/sample"
    preview = "preview"
    subscribe = "http://opds-spec.org/acquisition/subscribe"


class BasePublication(BaseOpdsModel):
    """
    Base publication model. This is the base class for both the
    OPDS2 and ODL publications.

    https://drafts.opds.io/opds-2.0#51-opds-publication
    https://github.com/opds-community/drafts/blob/main/schema/publication.schema.json
    """

    @classmethod
    def content_type(cls) -> str:
        return "application/opds-publication+json"

    metadata: PublicationMetadata
    images: CompactCollection[Link]
    links: CompactCollection[StrictLink] = Field(default_factory=CompactCollection)

    _validate_images = field_validator("images")(validate_images)


class Publication(BasePublication):
    """
    OPDS2 publication.

    https://drafts.opds.io/opds-2.0#51-opds-publication
    https://github.com/opds-community/drafts/blob/main/schema/publication.schema.json
    """

    links: Annotated[CompactCollection[StrictLink], Field(min_length=1)]

    @field_validator("links")
    @classmethod
    def validate_acquisition_link(
        cls, value: CompactCollection[StrictLink]
    ) -> CompactCollection[StrictLink]:
        """
        Must have at least one acquisition link.

        https://drafts.opds.io/opds-2.0#51-opds-publication
        """
        if not any([value.get_collection(rel=rel) for rel in AcquisitionLinkRelations]):
            raise ValueError("At least one acquisition link must be present")
        return value


class Facet(BaseOpdsModel):
    """
    OPDS2 facet.

    https://drafts.opds.io/opds-2.0#24-facets
    https://github.com/opds-community/drafts/blob/main/schema/feed.schema.json#L69-L90
    """

    metadata: FeedMetadata
    links: CompactCollection[TitleLink]

    @field_validator("links")
    @classmethod
    def validate_links(
        cls, value: CompactCollection[TitleLink]
    ) -> CompactCollection[TitleLink]:
        """
        Must have at least two links.
        """
        if len(value) < 2:
            raise ValueError("Facet must have at least two links")
        return value


class PublicationsGroup(BaseOpdsModel):
    """
    OPDS2 publications feed group.

    https://drafts.opds.io/opds-2.0#25-groups
    https://github.com/opds-community/drafts/blob/0feb95748db0d71fd1121726b5702725ed828874/schema/feed.schema.json#L91-L129
    """

    metadata: FeedMetadata
    links: CompactCollection[StrictLink] = Field(default_factory=CompactCollection)
    publications: Annotated[list[Publication], Field(min_length=1)]


class NavigationGroup(BaseOpdsModel):
    """
    OPDS2 navigation feed group.

    https://drafts.opds.io/opds-2.0#25-groups
    https://github.com/opds-community/drafts/blob/0feb95748db0d71fd1121726b5702725ed828874/schema/feed.schema.json#L91-L129
    """

    metadata: FeedMetadata
    links: CompactCollection[StrictLink] = Field(default_factory=CompactCollection)
    navigation: CompactCollection[TitleLink] = Field(..., min_length=1)


class Feed(BaseOpdsModel):
    """
    OPDS2 feed.

    https://drafts.opds.io/opds-2.0#2-collections
    https://github.com/opds-community/drafts/blob/main/schema/feed.schema.json
    """

    @classmethod
    def content_type(cls) -> str:
        return "application/opds+json"

    metadata: FeedMetadata
    links: CompactCollection[StrictLink]
    navigation: CompactCollection[TitleLink] = Field(default_factory=CompactCollection)
    publications: list[Publication] = Field(default_factory=list)
    facets: list[Facet] = Field(default_factory=list)
    groups: list[PublicationsGroup | NavigationGroup] = Field(default_factory=list)

    _validate_links = field_validator("links")(validate_self_link)

    @model_validator(mode="after")
    def required_collections(self) -> Self:
        if not self.publications and not self.groups and not self.navigation:
            raise ValueError(
                "Feed must have at least one of: publications, groups, navigation"
            )
        return self


T = TypeVar("T")


class BasePublicationFeed(BaseOpdsModel, Generic[T]):
    """
    Base for OPDS2 and ODL publications feed.

    They only differ in the type of publication they contain.
    """

    @classmethod
    def content_type(cls) -> str:
        return "application/opds+json"

    metadata: FeedMetadata
    links: CompactCollection[StrictLink]
    publications: list[T]

    _validate_links = field_validator("links")(validate_self_link)


class PublicationFeedNoValidation(BasePublicationFeed[dict[str, Any]]):
    """
    A publication feed where the publications themselves are not validated.

    This is useful when we want to parse a feed  but want to ignore the publications
    or parse the publications later, giving us a chance to validate them individually.
    """


class PublicationFeed(BasePublicationFeed[Publication]):
    """
    OPDS2 publication feed.

    This feed type isn't explicitly defined in the OPDS2 spec, but it is the type
    of feed that Palace expects to harvest when fetching an OPDS2 feed from a source.
    """
