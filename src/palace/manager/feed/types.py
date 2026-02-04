from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import NotRequired, TypedDict, Unpack, cast

from pydantic import ConfigDict

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work


class LinkAttributes(TypedDict):
    href: str
    rel: NotRequired[str]
    type: NotRequired[str]


class LinkKwargs(TypedDict):
    rel: NotRequired[str]
    type: NotRequired[str]
    title: NotRequired[str]
    role: NotRequired[str]
    facet_group: NotRequired[str]
    facet_group_type: NotRequired[str]
    active_facet: NotRequired[bool]
    default_facet: NotRequired[bool]
    active_sort: NotRequired[bool]


@dataclass
class TextValue:
    text: str | None = None
    type: str | None = None


@dataclass
class Link:
    href: str | None = None
    rel: str | None = None
    type: str | None = None

    # Additional types
    role: str | None = None
    title: str | None = None

    # Facet-related attributes
    facet_group: str | None = None
    facet_group_type: str | None = None
    active_facet: bool = False
    default_facet: bool = False
    active_sort: bool = False

    def link_attribs(self) -> LinkAttributes:
        if self.href is None:
            raise PalaceValueError("Link.href cannot be None for link attributes")
        attrs: LinkAttributes = {"href": self.href}
        if self.rel is not None:
            attrs["rel"] = self.rel
        if self.type is not None:
            attrs["type"] = self.type
        return attrs


@dataclass
class Category:
    scheme: str
    term: str
    label: str
    rating_value: str | None = None


@dataclass
class Rating:
    rating_value: str
    additional_type: str | None = None


@dataclass
class Series:
    name: str
    position: str | None = None
    link: Link | None = None


@dataclass
class Distribution:
    provider_name: str


@dataclass
class PatronData:
    username: str | None = None
    authorization_identifier: str | None = None


@dataclass
class DRMLicensor:
    vendor: str | None = None
    client_token: TextValue | None = None
    scheme: str | None = None


@dataclass
class IndirectAcquisition:
    type: str | None = None
    children: list[IndirectAcquisition] = field(default_factory=list)


@dataclass
class Acquisition(Link):
    holds_position: str | None = None
    holds_total: str | None = None

    copies_available: str | None = None
    copies_total: str | None = None

    availability_status: str | None = None
    availability_since: str | None = None
    availability_until: str | None = None

    rights: str | None = None

    lcp_hashed_passphrase: TextValue | None = None
    drm_licensor: DRMLicensor | None = None

    indirect_acquisitions: list[IndirectAcquisition] = field(default_factory=list)

    # Signal if the acquisition is for a loan or a hold for the patron
    is_loan: bool = False
    is_hold: bool = False

    # Signal if the acquisition link href is templated
    templated: bool = False


@dataclass
class Author:
    name: str | None = None
    sort_name: str | None = None
    viaf: str | None = None
    role: str | None = None
    family_name: str | None = None
    wikipedia_name: str | None = None
    lc: str | None = None
    link: Link | None = None


@dataclass
class WorkEntryData:
    """All the metadata possible for a work. This is not a TextValue because we want strict control."""

    additional_type: str | None = None
    identifier: str | None = None
    pwid: str | None = None
    issued: datetime | date | None = None
    duration: float | None = None

    summary: TextValue | None = None
    language: TextValue | None = None
    publisher: TextValue | None = None
    published: TextValue | None = None
    updated: TextValue | None = None
    title: TextValue | None = None
    sort_title: TextValue | None = None
    subtitle: TextValue | None = None
    series: Series | None = None
    imprint: TextValue | None = None

    authors: list[Author] = field(default_factory=list)
    contributors: list[Author] = field(default_factory=list)
    categories: list[Category] = field(default_factory=list)
    ratings: list[Rating] = field(default_factory=list)
    distribution: Distribution | None = None

    # Links
    acquisition_links: list[Acquisition] = field(default_factory=list)
    image_links: list[Link] = field(default_factory=list)
    other_links: list[Link] = field(default_factory=list)


@dataclass
class WorkEntry:
    work: Work
    edition: Edition
    identifier: Identifier
    license_pool: LicensePool | None = None

    # Actual, computed feed data
    computed: WorkEntryData | None = None

    def __init__(
        self,
        work: Work | None = None,
        edition: Edition | None = None,
        identifier: Identifier | None = None,
        license_pool: LicensePool | None = None,
    ) -> None:
        if None in (work, edition, identifier):
            raise ValueError(
                "Work, Edition or Identifier cannot be None while initializing an entry"
            )
        self.work = cast(Work, work)
        self.edition = cast(Edition, edition)
        self.identifier = cast(Identifier, identifier)
        self.license_pool = license_pool


@dataclass
class FeedMetadata:
    title: str | None = None
    id: str | None = None
    updated: str | None = None
    items_per_page: int | None = None
    patron: PatronData | None = None
    drm_licensor: DRMLicensor | None = None
    lcp_hashed_passphrase: TextValue | None = None


class DataEntryTypes:
    NAVIGATION = "navigation"


@dataclass
class DataEntry:
    """Other kinds of information, like entries of a navigation feed."""

    type: str | None = None
    title: str | None = None
    id: str | None = None
    links: list[Link] = field(default_factory=list)


@dataclass
class FeedData:
    links: list[Link] = field(default_factory=list)
    breadcrumbs: list[Link] = field(default_factory=list)
    facet_links: list[Link] = field(default_factory=list)
    entries: list[WorkEntry] = field(default_factory=list)
    data_entries: list[DataEntry] = field(default_factory=list)
    metadata: FeedMetadata = field(default_factory=lambda: FeedMetadata())
    entrypoint: str | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def add_link(self, href: str, **kwargs: Unpack[LinkKwargs]) -> None:
        self.links.append(Link(href=href, **kwargs))
