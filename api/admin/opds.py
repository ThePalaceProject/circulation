from sqlalchemy import and_

from api.opds import LibraryAnnotator
from core.lane import Pagination
from core.model import DataSource, LicensePool
from core.opds import AcquisitionFeed, VerboseAnnotator


class AdminAnnotator(LibraryAnnotator):
    def __init__(self, circulation, library, test_mode=False):
        super().__init__(circulation, None, library, test_mode=test_mode)
        self.opds_cache_field = None

    def annotate_work_entry(
        self, work, active_license_pool, edition, identifier, feed, entry
    ):
        super().annotate_work_entry(
            work, active_license_pool, edition, identifier, feed, entry
        )
        VerboseAnnotator.add_ratings(work, entry)

        # Find staff rating and add a tag for it.
        for measurement in identifier.measurements:
            if (
                measurement.data_source.name == DataSource.LIBRARY_STAFF
                and measurement.is_most_recent
            ):
                entry.append(
                    self.rating_tag(measurement.quantity_measured, measurement.value)
                )

        if active_license_pool and active_license_pool.suppressed:
            feed.add_link_to_entry(
                entry,
                rel="http://librarysimplified.org/terms/rel/restore",
                href=self.url_for(
                    "unsuppress",
                    identifier_type=identifier.type,
                    identifier=identifier.identifier,
                    _external=True,
                ),
            )
        else:
            feed.add_link_to_entry(
                entry,
                rel="http://librarysimplified.org/terms/rel/hide",
                href=self.url_for(
                    "suppress",
                    identifier_type=identifier.type,
                    identifier=identifier.identifier,
                    _external=True,
                ),
            )

        feed.add_link_to_entry(
            entry,
            rel="edit",
            href=self.url_for(
                "edit",
                identifier_type=identifier.type,
                identifier=identifier.identifier,
                _external=True,
            ),
        )

    def suppressed_url(self, pagination):
        kwargs = dict(list(pagination.items()))
        return self.url_for("suppressed", _external=True, **kwargs)

    def annotate_feed(self, feed):
        # Add a 'search' link.
        search_url = self.url_for("lane_search", languages=None, _external=True)
        search_link = dict(
            rel="search", type="application/opensearchdescription+xml", href=search_url
        )
        feed.add_link_to_feed(feed.feed, **search_link)


class AdminFeed(AcquisitionFeed):
    @classmethod
    def suppressed(cls, _db, title, url, annotator, pagination=None):
        pagination = pagination or Pagination.default()

        q = (
            _db.query(LicensePool)
            .filter(
                and_(
                    LicensePool.suppressed == True,
                    LicensePool.superceded == False,
                )
            )
            .order_by(LicensePool.id)
        )
        pools = pagination.modify_database_query(_db, q).all()

        works = [pool.work for pool in pools]
        feed = cls(_db, title, url, works, annotator)

        # Render a 'start' link
        top_level_title = annotator.top_level_title()
        start_uri = annotator.groups_url(None)
        AdminFeed.add_link_to_feed(
            feed.feed, href=start_uri, rel="start", title=top_level_title
        )

        # Render an 'up' link, same as the 'start' link to indicate top-level feed
        AdminFeed.add_link_to_feed(
            feed.feed, href=start_uri, rel="up", title=top_level_title
        )

        if len(works) > 0:
            # There are works in this list. Add a 'next' link.
            AdminFeed.add_link_to_feed(
                feed.feed,
                rel="next",
                href=annotator.suppressed_url(pagination.next_page),
            )

        if pagination.offset > 0:
            AdminFeed.add_link_to_feed(
                feed.feed,
                rel="first",
                href=annotator.suppressed_url(pagination.first_page),
            )

        previous_page = pagination.previous_page
        if previous_page:
            AdminFeed.add_link_to_feed(
                feed.feed, rel="previous", href=annotator.suppressed_url(previous_page)
            )

        annotator.annotate_feed(feed)
        return str(feed)
