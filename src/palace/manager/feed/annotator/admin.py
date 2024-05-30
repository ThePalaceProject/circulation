from datetime import datetime

from palace.manager.api.circulation import CirculationAPI
from palace.manager.feed.annotator.circulation import LibraryAnnotator
from palace.manager.feed.annotator.verbose import VerboseAnnotator
from palace.manager.feed.types import FeedData, Link, WorkEntry
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.lane import Pagination
from palace.manager.sqlalchemy.model.library import Library


class AdminAnnotator(LibraryAnnotator):
    def __init__(self, circulation: CirculationAPI | None, library: Library) -> None:
        super().__init__(circulation, None, library)

    def annotate_work_entry(
        self, entry: WorkEntry, updated: datetime | None = None
    ) -> None:
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
                measurement.data_source.name == DataSource.LIBRARY_STAFF
                and measurement.is_most_recent
                and measurement.value is not None
            ):
                entry.computed.ratings.append(
                    self.rating(measurement.quantity_measured, measurement.value)
                )

        if active_license_pool and not active_license_pool.suppressed:
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
                        rel="http://palaceproject.io/terms/rel/unsuppress-for-library",
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
                        rel="http://palaceproject.io/terms/rel/suppress-for-library",
                    )
                )
        elif active_license_pool and active_license_pool.suppressed:
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
