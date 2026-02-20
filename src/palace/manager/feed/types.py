from __future__ import annotations

"""Feed model types used to build OPDS 1/2 payloads."""

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum, StrEnum, auto
from typing import Literal, NotRequired, TypedDict, Unpack

from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work


class LinkContentType(Enum):
    """Semantic content types for links that reference OPDS feeds or entries.

    Each serializer maps these to its format-specific content type.
    Links with concrete types (text/html, application/json, etc.) bypass this.
    """

    OPDS_FEED = auto()
    OPDS_ENTRY = auto()


#: Union type for link type fields that accept either concrete MIME types or
#: semantic :class:`LinkContentType` values resolved at serialization time.
LinkType = str | LinkContentType


class LinkKwargs(TypedDict):
    """Typed keyword arguments accepted by FeedData.add_link."""

    rel: NotRequired[str]
    type: NotRequired[LinkType]
    title: NotRequired[str]
    role: NotRequired[str]
    facet_group: NotRequired[str]
    facet_group_type: NotRequired[str]
    active_facet: NotRequired[bool]
    default_facet: NotRequired[bool]
    active_sort: NotRequired[bool]


@dataclass(slots=True)
class RichText:
    """Text content with an optional content type (e.g., HTML)."""

    text: str | None = None
    content_type: Literal["html"] | None = None


@dataclass(slots=True)
class Link:
    """A link with optional facets and display metadata."""

    href: str
    rel: str | None = None
    type: LinkType | None = None

    # Additional types
    role: str | None = None
    title: str | None = None

    # Facet-related attributes
    facet_group: str | None = None
    facet_group_type: str | None = None
    active_facet: bool = False
    default_facet: bool = False
    active_sort: bool = False


@dataclass(slots=True)
class Category:
    """A subject/category tag with an optional rating weight."""

    scheme: str
    term: str
    label: str
    rating_value: str | None = None


@dataclass(slots=True)
class Rating:
    """A schema.org rating for a work entry."""

    rating_value: str
    additional_type: str | None = None


@dataclass(slots=True)
class Series:
    """Series metadata for a work entry."""

    name: str
    position: int | None = None
    link: Link | None = None


@dataclass(slots=True)
class Distribution:
    """Distribution metadata for a work entry."""

    provider_name: str


@dataclass(slots=True)
class PatronData:
    """Patron identifier metadata used in feed-level tags."""

    username: str | None = None
    authorization_identifier: str | None = None


@dataclass(slots=True)
class DRMLicensor:
    """DRM licensor metadata for OPDS DRM extensions."""

    vendor: str | None = None
    client_token: str | None = None
    scheme: str | None = None


@dataclass(slots=True)
class IndirectAcquisition:
    """Tree structure for indirect acquisitions in OPDS1."""

    type: LinkType | None = None
    children: list[IndirectAcquisition] = field(default_factory=list)


@dataclass(slots=True)
class Acquisition(Link):
    """Acquisition link with holds/copies/availability details."""

    holds_position: str | None = None
    holds_total: str | None = None

    copies_available: str | None = None
    copies_total: str | None = None

    availability_status: str | None = None
    availability_since: str | None = None
    availability_until: str | None = None

    rights: str | None = None

    lcp_hashed_passphrase: str | None = None
    drm_licensor: DRMLicensor | None = None

    indirect_acquisitions: list[IndirectAcquisition] = field(default_factory=list)

    # Signal if the acquisition is for a loan or a hold for the patron
    is_loan: bool = False
    is_hold: bool = False

    # Signal if the acquisition link href is templated
    templated: bool = False


@dataclass(slots=True)
class Author:
    """Author or contributor metadata for a work entry."""

    name: str | None = None
    sort_name: str | None = None
    viaf: str | None = None
    role: str | None = None
    family_name: str | None = None
    wikipedia_name: str | None = None
    lc: str | None = None
    link: Link | None = None


@dataclass(slots=True)
class WorkEntryData:
    """Computed metadata used by OPDS serializers for a single work entry."""

    additional_type: str | None = None
    identifier: str | None = None
    pwid: str | None = None
    issued: datetime | date | None = None
    duration: float | None = None

    summary: RichText | None = None
    language: str | None = None
    publisher: str | None = None
    published: str | None = None
    updated: str | None = None
    title: str | None = None
    sort_title: str | None = None
    subtitle: str | None = None
    series: Series | None = None
    imprint: str | None = None

    authors: list[Author] = field(default_factory=list)
    contributors: list[Author] = field(default_factory=list)
    categories: list[Category] = field(default_factory=list)
    ratings: list[Rating] = field(default_factory=list)
    distribution: Distribution | None = None

    # Links
    acquisition_links: list[Acquisition] = field(default_factory=list)
    image_links: list[Link] = field(default_factory=list)
    other_links: list[Link] = field(default_factory=list)


@dataclass(slots=True)
class WorkEntry:
    """Wrapper for a Work and its computed feed representation."""

    work: Work
    edition: Edition
    identifier: Identifier
    license_pool: LicensePool | None = None

    # Actual, computed feed data
    computed: WorkEntryData | None = None


@dataclass(slots=True)
class FeedMetadata:
    """Feed-level metadata used by OPDS serializers."""

    title: str | None = None
    id: str | None = None
    updated: str | None = None
    items_per_page: int | None = None
    patron: PatronData | None = None
    drm_licensor: DRMLicensor | None = None
    lcp_hashed_passphrase: str | None = None


class DataEntryTypes(StrEnum):
    """Known DataEntry.type values."""

    NAVIGATION = "navigation"


@dataclass(slots=True)
class DataEntry:
    """Non-work feed entries (e.g., navigation entries)."""

    type: DataEntryTypes | None = None
    title: str | None = None
    id: str | None = None
    links: list[Link] = field(default_factory=list)


@dataclass(slots=True)
class FeedData:
    """Container for all feed-level data passed to serializers."""

    links: list[Link] = field(default_factory=list)
    breadcrumbs: list[Link] = field(default_factory=list)
    facet_links: list[Link] = field(default_factory=list)
    entries: list[WorkEntry] = field(default_factory=list)
    data_entries: list[DataEntry] = field(default_factory=list)
    metadata: FeedMetadata = field(default_factory=lambda: FeedMetadata())
    entrypoint: str | None = None

    def add_link(self, href: str, **kwargs: Unpack[LinkKwargs]) -> None:
        """Append a Link to the feed's top-level links list.

        :param href: Link URL.
        """
        self.links.append(Link(href=href, **kwargs))
