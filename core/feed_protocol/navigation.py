from typing import Optional

from flask import Response

from core.feed_protocol.opds import OPDSFeedProtocol
from core.feed_protocol.types import DataEntry, DataEntryTypes, Link
from core.lane import Facets, Pagination
from core.opds import NavigationFacets
from core.util.datetime_helpers import utc_now
from core.util.opds_writer import AtomFeed, OPDSFeed


class NavigationFeed(OPDSFeedProtocol):
    def __init__(
        self,
        title,
        url,
        lane,
        annotator,
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
        _db,
        title,
        url,
        worklist,
        annotator,
        facets=None,
    ):
        """The navigation feed with links to a given lane's sublanes."""

        facets = facets or NavigationFacets.default(worklist)
        feed = NavigationFeed(title, url, worklist, annotator, facets=facets)
        feed.generate_feed()
        return feed

    def generate_feed(self):
        self._feed.add_metadata("title", text=self.title)
        self._feed.add_metadata("id", text=self.url)
        self._feed.add_metadata("updated", text=AtomFeed._strftime(utc_now()))
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

    def add_entry(self, url, title, type=OPDSFeed.NAVIGATION_FEED_TYPE):
        """Create an OPDS navigation entry for a URL."""
        entry = DataEntry(type=DataEntryTypes.NAVIGATION, title=title, id=url)
        entry.links.append(Link(rel="subsection", href=url, type=type))
        self._feed.data_entries.append(entry)

    def as_response(self, **kwargs) -> Response:
        response = super().as_response(**kwargs)
        response.content_type = OPDSFeed.NAVIGATION_FEED_TYPE
        return response