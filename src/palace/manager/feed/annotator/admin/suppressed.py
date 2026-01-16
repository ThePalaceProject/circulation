from datetime import datetime
from typing import Any

from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.feed.annotator.circulation import LibraryAnnotator
from palace.manager.feed.annotator.verbose import VerboseAnnotator
from palace.manager.feed.types import FeedData, Link, WorkEntry
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.lane import Pagination
from palace.manager.sqlalchemy.model.library import Library


class AdminSuppressedAnnotator(LibraryAnnotator):
    REL_SUPPRESS_FOR_LIBRARY = "http://palaceproject.io/terms/rel/suppress-for-library"
    REL_UNSUPPRESS_FOR_LIBRARY = (
        "http://palaceproject.io/terms/rel/unsuppress-for-library"
    )

    # We do not currently support un/suppressing at the collection level via the API.
    # These are the link `rels` that we used, in case we want to support them in the future.
    # REL_SUPPRESS_FOR_COLLECTION = "http://librarysimplified.org/terms/rel/hide"
    # REL_UNSUPPRESS_FOR_COLLECTION = "http://librarysimplified.org/terms/rel/restore"

    def __init__(
        self, circulation: CirculationApiDispatcher | None, library: Library
    ) -> None:
        super().__init__(circulation, None, library)

    def annotate_work_entry(
        self, entry: WorkEntry, updated: datetime | None = None
    ) -> None:
        """Annotate a work entry for the admin client feed.

        This annotator supports links for un/suppressing works at the
        library level, but not at the collection level. If a work is
        already suppressed at the collection level, we don't add any
        per-library un/suppression links to the feed.
        """
        super().annotate_work_entry(entry)
        if not entry.computed:
            return
        VerboseAnnotator.add_ratings(entry)

        identifier = entry.identifier
        active_license_pool = entry.license_pool
        work = entry.work

        # Find staff rating and add a tag for it.
        for measurement in identifier.measurements:
            if (
                measurement.data_source
                and measurement.data_source.name == DataSource.LIBRARY_STAFF
                and measurement.is_most_recent
                and measurement.value is not None
            ):
                entry.computed.ratings.append(
                    self.rating(measurement.quantity_measured, measurement.value)
                )

        if active_license_pool and not active_license_pool.suppressed:
            if self.library in work.suppressed_for:
                entry.computed.other_links.append(
                    Link(
                        href=self.url_for(
                            "unsuppress_for_library",
                            identifier_type=identifier.type,
                            identifier=identifier.identifier,
                            library_short_name=self.library.short_name,
                            _external=True,
                        ),
                        rel=self.REL_UNSUPPRESS_FOR_LIBRARY,
                    )
                )
            else:
                entry.computed.other_links.append(
                    Link(
                        href=self.url_for(
                            "suppress_for_library",
                            identifier_type=identifier.type,
                            identifier=identifier.identifier,
                            library_short_name=self.library.short_name,
                            _external=True,
                        ),
                        rel=self.REL_SUPPRESS_FOR_LIBRARY,
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

    def suppressed_url(self, **kwargs: dict[str, Any]) -> str:
        return self.url_for(
            "suppressed",
            library_short_name=self.library.short_name,
            _external=True,
            **kwargs,
        )

    def suppressed_url_with_pagination(self, pagination: Pagination) -> str:
        kwargs = dict(list(pagination.items()))
        return self.suppressed_url(**kwargs)

    def annotate_feed(self, feed: FeedData) -> None:
        # Add a 'search' link.
        search_url = self.url_for("lane_search", languages=None, _external=True)
        feed.add_link(
            search_url, rel="search", type="application/opensearchdescription+xml"
        )
