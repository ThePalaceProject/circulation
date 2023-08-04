from __future__ import annotations

from typing import TYPE_CHECKING

from flask import Response

from core.entrypoint import EntryPoint
from core.facets import FacetConstants
from core.feed_protocol.base import FeedProtocol
from core.feed_protocol.serializer.opds import OPDS1Serializer
from core.feed_protocol.types import FeedData, Link
from core.lane import Lane
from core.opds import AcquisitionFeed

if TYPE_CHECKING:
    from core.external_search import ExternalSearchIndex
    from core.lane import Facets, Pagination


class OPDSFeedProtocol(FeedProtocol):
    def __init__(
        self,
        search_engine: ExternalSearchIndex,
        facets: Facets,
        pagination: Pagination,
    ) -> None:
        self._search_engine = search_engine
        self._facets = facets
        self._pagination = pagination
        self._feed = FeedData()
        self._serializer = OPDS1Serializer()

    def serialize(self):
        return self._serializer.serialize_feed(self._feed)

    ## OPDS1 specifics
    @classmethod
    def add_entrypoint_links(
        cls, feed, url_generator, entrypoints, selected_entrypoint, group_name="Formats"
    ):
        """Add links to a feed forming an OPDS facet group for a set of
        EntryPoints.

        :param feed: A FeedData object.
        :param url_generator: A callable that returns the entry point
            URL when passed an EntryPoint.
        :param entrypoints: A list of all EntryPoints in the facet group.
        :param selected_entrypoint: The current EntryPoint, if selected.
        """
        if len(entrypoints) == 1 and selected_entrypoint in (None, entrypoints[0]):
            # There is only one entry point. Unless the currently
            # selected entry point is somehow different, there's no
            # need to put any links at all here -- a facet group with
            # one one facet might as well not be there.
            return

        is_default = True
        for entrypoint in entrypoints:
            link = cls._entrypoint_link(
                url_generator, entrypoint, selected_entrypoint, is_default, group_name
            )
            if link is not None:
                feed.add_link(**link)
                is_default = False

    @classmethod
    def _entrypoint_link(
        cls, url_generator, entrypoint, selected_entrypoint, is_default, group_name
    ):
        """Create arguments for add_link_to_feed for a link that navigates
        between EntryPoints.
        """
        display_title = EntryPoint.DISPLAY_TITLES.get(entrypoint)
        if not display_title:
            # Shouldn't happen.
            return

        url = url_generator(entrypoint)
        is_selected = entrypoint is selected_entrypoint
        link = AcquisitionFeed.facet_link(url, display_title, group_name, is_selected)

        # Unlike a normal facet group, every link in this facet
        # group has an additional attribute marking it as an entry
        # point.
        #
        # In OPDS 2 this can become an additional rel value,
        # removing the need for a custom attribute.
        link["facetGroupType"] = FacetConstants.ENTRY_POINT_REL
        return link

    def add_breadcrumb_links(self, lane, entrypoint=None):
        """TODO: Rewrite"""
        """Add information necessary to find your current place in the
        site's navigation.

        A link with rel="start" points to the start of the site

        A <simplified:entrypoint> section describes the current entry point.

        A <simplified:breadcrumbs> section contains a sequence of
        breadcrumb links.
        """
        # Add the top-level link with rel='start'
        xml = self.feed
        annotator = self.annotator
        top_level_title = annotator.top_level_title() or "Collection Home"
        self._feed.links.append(
            Link(rel="start", href=annotator.default_lane_url(), title=top_level_title)
        )

        # Add a link to the direct parent with rel="up".
        #
        # TODO: the 'direct parent' may be the same lane but without
        # the entry point specified. Fixing this would also be a good
        # opportunity to refactor the code for figuring out parent and
        # parent_title.
        parent = None
        if isinstance(lane, Lane):
            parent = lane.parent
        if parent and parent.display_name:
            parent_title = parent.display_name
        else:
            parent_title = top_level_title

        if parent:
            up_uri = annotator.lane_url(parent)
            self._feed.links.append(Link(href=up_uri, rel="up", title=parent_title))
        self.add_breadcrumbs(lane, entrypoint=entrypoint)

        # Annotate the feed with a simplified:entryPoint for the
        # current EntryPoint.
        self.show_current_entrypoint(entrypoint)

    def as_response(self) -> Response:
        feed = self.serialize()
        return Response(feed, content_type="application/atom+xml;profile=opds-catalog")
