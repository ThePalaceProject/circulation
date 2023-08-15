from core.feed_protocol.annotator.circulation import LibraryAnnotator
from core.feed_protocol.annotator.verbose import VerboseAnnotator
from core.feed_protocol.types import FeedData, Link, WorkEntry
from core.mirror import MirrorUploader
from core.model import DataSource
from core.model.configuration import ExternalIntegrationLink


class AdminAnnotator(LibraryAnnotator):
    def __init__(self, circulation, library, test_mode=False):
        super().__init__(circulation, None, library, test_mode=test_mode)

    def annotate_work_entry(self, entry: WorkEntry):
        super().annotate_work_entry(entry)
        VerboseAnnotator.add_ratings(entry)

        identifier = entry.identifier
        active_license_pool = entry.license_pool

        # Find staff rating and add a tag for it.
        for measurement in identifier.measurements:
            if (
                measurement.data_source.name == DataSource.LIBRARY_STAFF
                and measurement.is_most_recent
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

        # If there is a storage integration for the collection, changing the cover is allowed.
        mirror = MirrorUploader.for_collection(
            active_license_pool.collection, ExternalIntegrationLink.COVERS
        )
        if mirror:
            entry.computed.other_links.append(
                Link(
                    href=self.url_for(
                        "work_change_book_cover",
                        identifier_type=identifier.type,
                        identifier=identifier.identifier,
                        _external=True,
                    ),
                    rel="http://librarysimplified.org/terms/rel/change_cover",
                )
            )

    def suppressed_url(self, pagination):
        kwargs = dict(list(pagination.items()))
        return self.url_for("suppressed", _external=True, **kwargs)

    def annotate_feed(self, feed: FeedData):
        # Add a 'search' link.
        search_url = self.url_for("lane_search", languages=None, _external=True)
        feed.add_link(
            search_url, rel="search", type="application/opensearchdescription+xml"
        )
