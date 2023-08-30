from dataclasses import dataclass, field
from typing import Any, Dict, Generator, List, Optional, Tuple

from typing_extensions import Self

from core.model import LicensePool, Work
from core.model.edition import Edition
from core.model.identifier import Identifier

NO_SUCH_KEY = object()


@dataclass
class BaseModel:
    def _vars(self) -> Generator[Tuple[str, Any], None, None]:
        """Yield attributes as a tuple"""
        _attrs = vars(self)
        for name, value in _attrs.items():
            if name.startswith("_"):
                continue
            elif callable(value):
                continue
            yield name, value

    def dict(self) -> Dict[str, Any]:
        """Dataclasses do not return undefined attributes via `asdict` so we must implement this ourselves"""
        attrs = {}
        for name, value in self:
            if isinstance(value, BaseModel):
                attrs[name] = value.dict()
            else:
                attrs[name] = value
        return attrs

    def __iter__(self) -> Generator[Tuple[str, Any], None, None]:
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
    text: Optional[str] = None

    @classmethod
    def create(cls, **kwargs) -> Self:
        """Create a new object with arbitrary data"""
        obj = cls()
        obj.add_attributes(kwargs)
        return obj

    def add_attributes(self, attrs: Dict[str, Any]) -> None:
        for name, data in attrs.items():
            setattr(self, name, data)

    def children(self) -> Generator[Tuple[str, "FeedEntryType"], None, None]:
        """Yield all FeedEntryType attributes"""
        for name, value in self:
            if isinstance(value, self.__class__):
                yield name, value
        return


@dataclass
class Link(FeedEntryType):
    href: str = None
    rel: Optional[str] = None
    type: Optional[str] = None

    # Additional types
    role: Optional[str] = None
    title: Optional[str] = None

    def dict(self) -> Dict[str, Any]:
        """A dict without None values"""
        d = super().dict()
        santized = {}
        for k, v in d.items():
            if v is not None:
                santized[k] = v
        return santized

    def link_attribs(self) -> Dict[str, Any]:
        d = dict(href=self.href)
        for key in ["rel", "type"]:
            if (value := getattr(self, key, None)) is not None:
                d[key] = value
        return d


@dataclass
class IndirectAcquisition(BaseModel):
    type: str = None
    children: List["IndirectAcquisition"] = field(default_factory=list)


@dataclass
class Acquisition(Link):
    holds_position: Optional[str] = None
    holds_total: Optional[str] = None

    copies_available: Optional[str] = None
    copies_total: Optional[str] = None

    availability_status: Optional[str] = None
    availability_since: Optional[str] = None
    availability_until: Optional[str] = None

    rights: Optional[str] = None

    lcp_hashed_passphrase: Optional[FeedEntryType] = None
    drm_licensor: Optional[FeedEntryType] = None

    indirect_acquisitions: List[IndirectAcquisition] = field(default_factory=list)


@dataclass
class Author(FeedEntryType):
    name: Optional[str] = None
    sort_name: Optional[str] = None
    viaf: Optional[str] = None
    role: Optional[str] = None
    family_name: Optional[str] = None
    wikipedia_name: Optional[str] = None
    lc: Optional[str] = None
    link: Optional[Link] = None


@dataclass
class WorkEntryData(BaseModel):
    """All the metadata possible for a work. This is not a FeedEntryType because we want strict control."""

    additionalType: Optional[str] = None
    identifier: Optional[str] = None
    pwid: Optional[str] = None

    summary: Optional[FeedEntryType] = None
    language: Optional[FeedEntryType] = None
    publisher: Optional[FeedEntryType] = None
    issued: Optional[FeedEntryType] = None
    published: Optional[FeedEntryType] = None
    updated: Optional[FeedEntryType] = None
    title: Optional[FeedEntryType] = None
    subtitle: Optional[FeedEntryType] = None
    series: Optional[FeedEntryType] = None
    imprint: Optional[FeedEntryType] = None

    authors: List[Author] = field(default_factory=list)
    contributors: List[Author] = field(default_factory=list)
    categories: List[FeedEntryType] = field(default_factory=list)
    ratings: List[FeedEntryType] = field(default_factory=list)
    distribution: Optional[FeedEntryType] = None

    # Links
    acquisition_links: List[Acquisition] = field(default_factory=list)
    image_links: List[Link] = field(default_factory=list)
    other_links: List[Link] = field(default_factory=list)


@dataclass
class WorkEntry(FeedEntryType):
    work: Optional[Work] = None
    edition: Optional[Edition] = None
    identifier: Optional[Identifier] = None
    license_pool: Optional[LicensePool] = None

    # Actual, computed feed data
    computed: Optional[WorkEntryData] = None

    def __post_init__(self) -> None:
        if None in (self.work, self.edition, self.identifier):
            raise ValueError(
                "Work, Edition or Identifier cannot be None while initializing an entry"
            )


class DataEntryTypes:
    NAVIGATION = "navigation"


@dataclass
class DataEntry(FeedEntryType):
    """Other kinds of information, like entries of a navigation feed"""

    type: Optional[str] = None
    title: Optional[str] = None
    id: Optional[str] = None
    links: List[Link] = field(default_factory=list)


@dataclass
class FeedData(BaseModel):
    links: List[Link] = field(default_factory=list)
    breadcrumbs: List[Link] = field(default_factory=list)
    facet_links: List[Link] = field(default_factory=list)
    entries: List[WorkEntry] = field(default_factory=list)
    data_entries: List[DataEntry] = field(default_factory=list)
    metadata: Dict[str, FeedEntryType] = field(default_factory=dict)
    entrypoint: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True

    def add_link(self, href: str, **kwargs: Any) -> None:
        self.links.append(Link(href=href, **kwargs))

    def add_metadata(
        self, name: str, feed_entry: Optional[FeedEntryType] = None, **kwargs: Any
    ) -> None:
        if not feed_entry:
            self.metadata[name] = FeedEntryType(**kwargs)
        else:
            self.metadata[name] = feed_entry
