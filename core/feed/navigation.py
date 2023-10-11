from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.orm import Session
from typing_extensions import Self
from werkzeug.datastructures import MIMEAccept

from core.feed.annotator.circulation import CirculationManagerAnnotator
from core.feed.opds import BaseOPDSFeed, NavigationFacets
from core.feed.types import DataEntry, DataEntryTypes, Link
from core.feed.util import strftime
from core.lane import Facets, Pagination, WorkList
from core.util.datetime_helpers import utc_now
from core.util.flask_util import OPDSFeedResponse
from core.util.opds_writer import OPDSFeed


class NavigationFeed(BaseOPDSFeed):
    def __init__(
        self,
        title: str,
        url: str,
        lane: WorkList,
        annotator: CirculationManagerAnnotator,
        facets: Optional[Facets] = None,
        pagination: Optional[Pagination] = None,
    ) -> None:
        self.lane = lane
        self.annotator = annotator
        self._facets = facets
        self._pagination = pagination
        super().__init__(title, url)

    @classmethod
    def navigation(
        cls,
        _db: Session,
        title: str,
        url: str,
        worklist: WorkList,
        annotator: CirculationManagerAnnotator,
        facets: Optional[Facets] = None,
    ) -> Self:
        """The navigation feed with links to a given lane's sublanes."""

        facets = facets or NavigationFacets.default(worklist)
        feed = cls(title, url, worklist, annotator, facets=facets)
        feed.generate_feed()
        return feed

    def generate_feed(self) -> None:
        self._feed.metadata.title = self.title
        self._feed.metadata.id = self.url
        self._feed.metadata.updated = strftime(utc_now())
        self._feed.add_link(href=self.url, rel="self")
        if not self.lane.children:
            # We can't generate links to children, since this Worklist
            # has no children, so we'll generate a link to the
            # Worklist's page-type feed instead.
            title = "All " + self.lane.display_name
            page_url = self.annotator.feed_url(self.lane)
            self.add_entry(page_url, title, OPDSFeed.ACQUISITION_FEED_TYPE)

        for child in self.lane.visible_children:
            title = child.display_name
            if child.children:
                child_url = self.annotator.navigation_url(child)
                self.add_entry(child_url, title, OPDSFeed.NAVIGATION_FEED_TYPE)
            else:
                child_url = self.annotator.feed_url(child)
                self.add_entry(child_url, title, OPDSFeed.ACQUISITION_FEED_TYPE)

        self.annotator.annotate_feed(self._feed)

    def add_entry(
        self, url: str, title: str, type: str = OPDSFeed.NAVIGATION_FEED_TYPE
    ) -> None:
        """Create an OPDS navigation entry for a URL."""
        entry = DataEntry(type=DataEntryTypes.NAVIGATION, title=title, id=url)
        entry.links.append(Link(rel="subsection", href=url, type=type))
        self._feed.data_entries.append(entry)

    def as_response(
        self,
        mime_types: Optional[MIMEAccept] = None,
        **kwargs: Any,
    ) -> OPDSFeedResponse:
        response = super().as_response(mime_types=mime_types, **kwargs)
        response.content_type = OPDSFeed.NAVIGATION_FEED_TYPE
        return response
