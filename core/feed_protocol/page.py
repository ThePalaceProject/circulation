"""OPDS 1 paged feed"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from core.feed_protocol.opds import OPDSFeedProtocol
from core.feed_protocol.types import Link, WorkEntry
from core.feed_protocol.utils import active_loans_and_holds
from core.opds import AcquisitionFeed

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from core.lane import WorkList
    from core.model import Patron


class OPDSPageFeed(OPDSFeedProtocol):
    def __init__(
        self, url, title, search_engine, facets, pagination, annotator
    ) -> None:
        self.annotator = annotator
        self.url = url
        self.title = title
        super().__init__(search_engine, facets, pagination)

    def generate_feed(
        self, _db: Session, lane: WorkList, patron: Optional[Patron] = None
    ):
        works = lane.works(_db, self._facets, self._pagination, self._search_engine)

        # "works" MAY be a generator, we want a list
        if not isinstance(works, list):
            works = list(works)

        loans_and_holds = active_loans_and_holds(patron)

        for work in works:
            if work in loans_and_holds["loans_by_work"]:
                pool = loans_and_holds["loans_by_work"][work].license_pool
            elif work in loans_and_holds["holds_by_work"]:
                pool = loans_and_holds["holds_by_work"][work].license_pool
            else:
                pool = work.active_license_pool()
            # TODO: use the entry cache (is this still relevant?)
            self._feed.entries.append(WorkEntry(work=work, license_pool=pool))

        # TODO: All the metadata
        self._feed.add_metadata("id", text=self.url)
        self._feed.add_metadata("title", text=self.title)

        # Links
        entrypoints = self._facets.selectable_entrypoints(lane)
        if entrypoints:
            # A paginated feed may have multiple entry points into the
            # same dataset.
            def make_link(ep):
                return self.annotator.feed_url(
                    lane, facets=self._facets.navigate(entrypoint=ep)
                )

            self.add_entrypoint_links(
                self._feed, make_link, entrypoints, self._facets.entrypoint
            )

        # Facet links
        facet_links = AcquisitionFeed.facet_links(self.annotator, self._facets)
        for linkdata in facet_links:
            self._feed.links.append(Link(**linkdata))

        if len(works) and self._pagination.has_next_page:
            self._feed.add_link(
                href=self.annotator.feed_url(
                    lane, self._facets, self._pagination.next_page
                ),
                rel="next",
            )

        if self._pagination.offset > 0:
            self._feed.add_link(
                href=self.annotator.feed_url(
                    lane, self._facets, self._pagination.first_page
                ),
                rel="first",
            )

        if self._pagination.previous_page:
            self._feed.add_link(
                href=self.annotator.feed_url(
                    lane, self._facets, self._pagination.previous_page
                ),
                rel="previous",
            )

        for entry in self._feed.entries:
            self.annotator.annotate_work_entry(entry)

        # TODO: Breadcrumb links

        # TODO: Annotator.annotate_feed
