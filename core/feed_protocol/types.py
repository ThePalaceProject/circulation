from typing import Any, Dict, Generator, List, Optional, Tuple

from pydantic import BaseModel

from core.model import LicensePool, Work
from core.model.edition import Edition
from core.model.identifier import Identifier


class FeedEntryType(BaseModel):
    text: Optional[str] = None

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True

    def add_attributes(self, attrs: Dict[str, Any]) -> None:
        for name, data in attrs.items():
            setattr(self, name, data)

    def children(self) -> Generator[Tuple[str, "FeedEntryType"], None, None]:
        """Yield all FeedEntryType attributes"""
        for name, value in self:
            if isinstance(value, self.__class__):
                yield name, value
        return


class Link(FeedEntryType):
    href: str
    rel: Optional[str]
    type: Optional[str]

    # Additional types
    role: Optional[str] = None
    title: Optional[str] = None

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        kwargs["exclude_none"] = True
        return super().dict(**kwargs)

    def link_attribs(self) -> Dict[str, Any]:
        d = dict(href=self.href)
        for key in ["rel", "type"]:
            if (value := getattr(self, key, None)) is not None:
                d[key] = value
        return d


class IndirectAcquisition(BaseModel):
    type: str
    children: List["IndirectAcquisition"] = []


class Acquisition(Link):
    holds_position: Optional[str]
    holds_total: Optional[str]

    copies_available: Optional[str]
    copies_total: Optional[str]

    availability_status: Optional[str]
    availability_since: Optional[str]
    availability_until: Optional[str]

    rights: Optional[str]

    lcp_hashed_passphrase: Optional[FeedEntryType]
    drm_licensor: Optional[FeedEntryType]

    indirect_acquisitions: List[IndirectAcquisition] = []


class Author(FeedEntryType):
    name: Optional[str]
    sort_name: Optional[str]
    viaf: Optional[str]
    role: Optional[str]
    family_name: Optional[str]
    wikipedia_name: Optional[str]
    lc: Optional[str]
    link: Optional[Link]


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

    authors: List[Author] = []
    contributors: List[Author] = []
    categories: List[FeedEntryType] = []
    ratings: List[FeedEntryType] = []
    distribution: Optional[FeedEntryType] = None

    # Links
    acquisition_links: List[Acquisition] = []
    image_links: List[Link] = []
    other_links: List[Link] = []


class WorkEntry(FeedEntryType):
    work: Work
    edition: Edition
    identifier: Identifier
    license_pool: Optional[LicensePool]
    cached_entry: Optional[str]

    # Actual, computed feed data
    computed: Optional[WorkEntryData]


class DataEntryTypes:
    NAVIGATION = "navigation"


class DataEntry(FeedEntryType):
    """Other kinds of information, like entries of a navigation feed"""

    type: str
    title: Optional[str]
    id: Optional[str]
    links: List[Link] = []


class FeedData(BaseModel):
    links: List[Link] = []
    breadcrumbs: List[Link] = []
    facet_links: List[Link] = []
    entries: List[WorkEntry] = []
    data_entries: List[DataEntry] = []
    metadata: Dict[str, FeedEntryType] = {}
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
