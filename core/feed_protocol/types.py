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
    rel: str
    type: Optional[str]

    def dict(self):
        d = dict(href=self.href, rel=self.rel)
        if self.type is not None:
            d["type"] = self.type
        return d


class WorkEntryData(BaseModel):
    """All the metadata possible for a work. This is not a FeedEntryType because we want strict control."""

    identifier: str = None
    pwid: str = None

    summary: FeedEntryType = None
    language: FeedEntryType = None
    publisher: FeedEntryType = None
    issued: FeedEntryType = None
    published: FeedEntryType = None
    updated: FeedEntryType = None
    title: FeedEntryType = None
    subtitle: FeedEntryType = None
    series: FeedEntryType = None
    imprint: FeedEntryType = None

    authors: List[FeedEntryType] = []
    contributors: List[FeedEntryType] = []
    categories: List[FeedEntryType] = []
    distribution: FeedEntryType = None

    # Links
    acquisition_links: List[Link] = []
    image_links: List[Link] = []
    other_links: List[Link] = []


class WorkEntry(FeedEntryType):
    work: Work
    edition: Optional[Edition] = None
    identifier: Optional[Identifier] = None
    license_pool: Optional[LicensePool] = None
    cached_entry: Optional[str] = None

    # Actual, computed feed data
    computed: Optional[WorkEntryData] = None


class FeedData(BaseModel):
    links: List[Link] = []
    facet_links: List[Link] = []
    entries: List[WorkEntry] = []
    metadata: Dict[str, FeedEntryType] = {}

    class Config:
        arbitrary_types_allowed = True

    def add_link(self, href, rel, **kwargs):
        self.links.append(Link(href=href, rel=rel, **kwargs))

    def add_metadata(self, name, feed_entry=None, **kwargs):
        if not feed_entry:
            self.metadata[name] = FeedEntryType(**kwargs)
        else:
            self.metadata[name] = feed_entry
