from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, cast

from pydantic import ConfigDict

from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work

NO_SUCH_KEY = object()


@dataclass
class BaseModel:
    def _vars(self) -> Generator[tuple[str, Any]]:
        """Yield attributes as a tuple."""
        _attrs = vars(self)
        for name, value in _attrs.items():
            if name.startswith("_"):
                continue
            if callable(value):
                continue
            yield name, value

    def asdict(self) -> dict[str, Any]:
        """Dataclasses do not return undefined attributes via `asdict` so we must implement this ourselves."""
        attrs: dict[str, Any] = {}
        for name, value in self:
            if isinstance(value, BaseModel):
                attrs[name] = value.asdict()
            else:
                attrs[name] = value
        return attrs

    def __iter__(self) -> Generator[tuple[str, Any]]:
        """Allow attribute iteration."""
        yield from self._vars()


@dataclass
class TextValue(BaseModel):
    text: str | None = None
    type: str | None = None


@dataclass
class Link(BaseModel):
    href: str | None = None
    rel: str | None = None
    type: str | None = None

    # Additional types
    role: str | None = None
    title: str | None = None

    # Facet-related attributes
    facetGroup: str | None = None
    facetGroupType: str | None = None
    activeFacet: bool = False
    defaultFacet: bool = False
    activeSort: bool = False

    def asdict(self) -> dict[str, Any]:
        """A dict without None values and without facet-only attributes."""
        sanitized: dict[str, Any] = {}
        for key in ("href", "rel", "type", "role", "title"):
            if (value := getattr(self, key, None)) is not None:
                sanitized[key] = value
        return sanitized

    def link_attribs(self) -> dict[str, Any]:
        d = dict(href=self.href)
        for key in ["rel", "type"]:
            if (value := getattr(self, key, None)) is not None:
                d[key] = value
        return d


@dataclass
class Category(BaseModel):
    scheme: str
    term: str
    label: str
    ratingValue: str | None = None


@dataclass
class Rating(BaseModel):
    ratingValue: str
    additionalType: str | None = None


@dataclass
class Series(BaseModel):
    name: str
    position: str | None = None
    link: Link | None = None


@dataclass
class Distribution(BaseModel):
    provider_name: str


@dataclass
class PatronData(BaseModel):
    username: str | None = None
    authorizationIdentifier: str | None = None


@dataclass
class DRMLicensor(BaseModel):
    vendor: str | None = None
    clientToken: TextValue | None = None
    scheme: str | None = None


@dataclass
class IndirectAcquisition(BaseModel):
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
class Author(BaseModel):
    name: str | None = None
    sort_name: str | None = None
    viaf: str | None = None
    role: str | None = None
    family_name: str | None = None
    wikipedia_name: str | None = None
    lc: str | None = None
    link: Link | None = None


@dataclass
class WorkEntryData(BaseModel):
    """All the metadata possible for a work. This is not a TextValue because we want strict control."""

    additionalType: str | None = None
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
class WorkEntry(BaseModel):
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
class FeedMetadata(BaseModel):
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
class DataEntry(BaseModel):
    """Other kinds of information, like entries of a navigation feed."""

    type: str | None = None
    title: str | None = None
    id: str | None = None
    links: list[Link] = field(default_factory=list)


@dataclass
class FeedData(BaseModel):
    links: list[Link] = field(default_factory=list)
    breadcrumbs: list[Link] = field(default_factory=list)
    facet_links: list[Link] = field(default_factory=list)
    entries: list[WorkEntry] = field(default_factory=list)
    data_entries: list[DataEntry] = field(default_factory=list)
    metadata: FeedMetadata = field(default_factory=lambda: FeedMetadata())
    entrypoint: str | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def add_link(self, href: str, **kwargs: Any) -> None:
        self.links.append(Link(href=href, **kwargs))
