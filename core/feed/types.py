from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, cast

from typing_extensions import Self

from core.model import LicensePool, Work
from core.model.edition import Edition
from core.model.identifier import Identifier

NO_SUCH_KEY = object()


@dataclass
class BaseModel:
    def _vars(self) -> Generator[tuple[str, Any], None, None]:
        """Yield attributes as a tuple"""
        _attrs = vars(self)
        for name, value in _attrs.items():
            if name.startswith("_"):
                continue
            elif callable(value):
                continue
            yield name, value

    def asdict(self) -> dict[str, Any]:
        """Dataclasses do not return undefined attributes via `asdict` so we must implement this ourselves"""
        attrs = {}
        for name, value in self:
            if isinstance(value, BaseModel):
                attrs[name] = value.asdict()
            else:
                attrs[name] = value
        return attrs

    def __iter__(self) -> Generator[tuple[str, Any], None, None]:
        """Allow attribute iteration"""
        yield from self._vars()

    def get(self, name: str, *default: Any) -> Any:
        """Convenience function. Mimics getattr"""
        value = getattr(self, name, NO_SUCH_KEY)
        if value is NO_SUCH_KEY:
            if len(default) > 0:
                return default[0]
            else:
                raise AttributeError(f"No attribute '{name}' found in object {self}")
        return value


@dataclass
class FeedEntryType(BaseModel):
    text: str | None = None

    @classmethod
    def create(cls, **kwargs: Any) -> Self:
        """Create a new object with arbitrary data"""
        obj = cls()
        obj.add_attributes(kwargs)
        return obj

    def add_attributes(self, attrs: dict[str, Any]) -> None:
        for name, data in attrs.items():
            setattr(self, name, data)

    def children(self) -> Generator[tuple[str, FeedEntryType], None, None]:
        """Yield all FeedEntryType attributes"""
        for name, value in self:
            if isinstance(value, self.__class__):
                yield name, value
        return


@dataclass
class Link(FeedEntryType):
    href: str | None = None
    rel: str | None = None
    type: str | None = None

    # Additional types
    role: str | None = None
    title: str | None = None

    def asdict(self) -> dict[str, Any]:
        """A dict without None values"""
        d = super().asdict()
        santized = {}
        for k, v in d.items():
            if v is not None:
                santized[k] = v
        return santized

    def link_attribs(self) -> dict[str, Any]:
        d = dict(href=self.href)
        for key in ["rel", "type"]:
            if (value := getattr(self, key, None)) is not None:
                d[key] = value
        return d


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

    lcp_hashed_passphrase: FeedEntryType | None = None
    drm_licensor: FeedEntryType | None = None

    indirect_acquisitions: list[IndirectAcquisition] = field(default_factory=list)

    # Signal if the acquisition is for a loan or a hold for the patron
    is_loan: bool = False
    is_hold: bool = False


@dataclass
class Author(FeedEntryType):
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
    """All the metadata possible for a work. This is not a FeedEntryType because we want strict control."""

    additionalType: str | None = None
    identifier: str | None = None
    pwid: str | None = None
    issued: datetime | date | None = None
    duration: float | None = None

    summary: FeedEntryType | None = None
    language: FeedEntryType | None = None
    publisher: FeedEntryType | None = None
    published: FeedEntryType | None = None
    updated: FeedEntryType | None = None
    title: FeedEntryType | None = None
    sort_title: FeedEntryType | None = None
    subtitle: FeedEntryType | None = None
    series: FeedEntryType | None = None
    imprint: FeedEntryType | None = None

    authors: list[Author] = field(default_factory=list)
    contributors: list[Author] = field(default_factory=list)
    categories: list[FeedEntryType] = field(default_factory=list)
    ratings: list[FeedEntryType] = field(default_factory=list)
    distribution: FeedEntryType | None = None

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
    patron: FeedEntryType | None = None
    drm_licensor: FeedEntryType | None = None
    lcp_hashed_passphrase: FeedEntryType | None = None


class DataEntryTypes:
    NAVIGATION = "navigation"


@dataclass
class DataEntry(FeedEntryType):
    """Other kinds of information, like entries of a navigation feed"""

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

    class Config:
        arbitrary_types_allowed = True

    def add_link(self, href: str, **kwargs: Any) -> None:
        self.links.append(Link(href=href, **kwargs))
