from typing import Dict, List, Optional

from pydantic import BaseModel

from core.model import LicensePool, Work
from core.model.edition import Edition
from core.model.identifier import Identifier


class FeedEntryType(BaseModel):
    text: Optional[str] = None

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True

    def add_attributes(self, attrs: dict):
        for name, data in attrs.items():
            setattr(self, name, data)

    def children(self):
        """Yield all FeedEntryType attributes"""
        for name, value in self:
            if isinstance(value, self.__class__):
                yield name, value


class Link(FeedEntryType):
    href: str
    rel: Optional[str]
    type: Optional[str]

    # Additional types
    role: Optional[str] = None
    title: Optional[str] = None

    def dict(self, **kwargs):
        kwargs["exclude_none"] = True
        return super().dict(**kwargs)

    def link_attribs(self) -> Dict:
        d = dict(href=self.href)
        for key in ["rel", "type"]:
            if (value := getattr(self, key, None)) is not None:
                d[key] = value
        return d


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
    acquisition_links: List[Link] = []
    image_links: List[Link] = []
    other_links: List[Link] = []


class WorkEntry(FeedEntryType):
    work: Work
    edition: Edition
    identifier: Identifier
    license_pool: LicensePool
    cached_entry: Optional[str] = None

    # Actual, computed feed data
    computed: Optional[WorkEntryData] = None


class FeedData(BaseModel):
    links: List[Link] = []
    breadcrumbs: List[Link] = []
    facet_links: List[Link] = []
    entries: List[WorkEntry] = []
    metadata: Dict[str, FeedEntryType] = {}
    entrypoint: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True

    def add_link(self, href, **kwargs):
        self.links.append(Link(href=href, **kwargs))

    def add_metadata(self, name, feed_entry=None, **kwargs):
        if not feed_entry:
            self.metadata[name] = FeedEntryType(**kwargs)
        else:
            self.metadata[name] = feed_entry
