from datetime import datetime
from typing import Optional

from api.circulation import CirculationAPI
from core.feed.annotator.circulation import LibraryAnnotator
from core.feed.annotator.verbose import VerboseAnnotator
from core.feed.types import FeedData, Link, WorkEntry
from core.lane import Pagination
from core.model import DataSource
from core.model.library import Library


class AdminAnnotator(LibraryAnnotator):
    def __init__(self, circulation: Optional[CirculationAPI], library: Library) -> None:
        super().__init__(circulation, None, library)

    def annotate_work_entry(
        self, entry: WorkEntry, updated: Optional[datetime] = None
    ) -> None:
        super().annotate_work_entry(entry)
        if not entry.computed:
            return
        VerboseAnnotator.add_ratings(entry)

        identifier = entry.identifier
        active_license_pool = entry.license_pool

        # Find staff rating and add a tag for it.
        for measurement in identifier.measurements:
            if (
                measurement.data_source.name == DataSource.LIBRARY_STAFF
                and measurement.is_most_recent
                and measurement.value is not None
            ):
                entry.computed.ratings.append(
                    self.rating(measurement.quantity_measured, measurement.value)
                )

        if active_license_pool and active_license_pool.suppressed:
            entry.computed.other_links.append(
                Link(
                    href=self.url_for(
                        "unsuppress",
                        identifier_type=identifier.type,
                        identifier=identifier.identifier,
                        _external=True,
                    ),
                    rel="http://librarysimplified.org/terms/rel/restore",
                )
            )
        else:
            entry.computed.other_links.append(
                Link(
                    href=self.url_for(
                        "suppress",
                        identifier_type=identifier.type,
                        identifier=identifier.identifier,
                        _external=True,
                    ),
                    rel="http://librarysimplified.org/terms/rel/hide",
                )
            )

        entry.computed.other_links.append(
            Link(
                href=self.url_for(
                    "edit",
                    identifier_type=identifier.type,
                    identifier=identifier.identifier,
                    _external=True,
                ),
                rel="edit",
            )
        )

    def suppressed_url(self, pagination: Pagination) -> str:
        kwargs = dict(list(pagination.items()))
        return self.url_for("suppressed", _external=True, **kwargs)

    def annotate_feed(self, feed: FeedData) -> None:
        # Add a 'search' link.
        search_url = self.url_for("lane_search", languages=None, _external=True)
        feed.add_link(
            search_url, rel="search", type="application/opensearchdescription+xml"
        )
