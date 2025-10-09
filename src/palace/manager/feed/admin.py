from typing import Self

from sqlalchemy import and_, false
from sqlalchemy.orm import Session

from palace.manager.feed.acquisition import OPDSAcquisitionFeed
from palace.manager.feed.annotator.admin import AdminAnnotator
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.lane import Pagination
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work


class AdminFeed(OPDSAcquisitionFeed):
    @classmethod
    def suppressed(
        cls,
        _db: Session,
        title: str,
        annotator: AdminAnnotator,
        pagination: Pagination | None = None,
    ) -> Self:
        _pagination = pagination or Pagination.default()
        start_url = annotator.suppressed_url()
        library = annotator.library

        q = (
            _db.query(Work)
            .join(LicensePool)
            .join(Edition)
            .filter(
                and_(
                    LicensePool.suppressed == false(),
                    Work.suppressed_for.contains(library),
                )
            )
            .order_by(Edition.sort_title)
        )
        works = _pagination.modify_database_query(_db, q).all()
        next_page_item_count = (
            _pagination.next_page.modify_database_query(_db, q).count()
            if _pagination.next_page
            else 0
        )

        feed = cls(title, start_url, works, annotator, pagination=_pagination)
        feed.generate_feed()

        # Render a 'start' link
        top_level_title = annotator.top_level_title()
        feed.add_link(start_url, rel="start", title=top_level_title)

        # Link to next page only if there are more entries than current page size.
        if next_page_item_count > 0:
            feed.add_link(
                href=annotator.suppressed_url_with_pagination(_pagination.next_page),
                rel="next",
            )

        # Link back to first page only if we're not the first page.
        if _pagination.offset > 0:
            feed.add_link(
                annotator.suppressed_url_with_pagination(_pagination.first_page),
                rel="first",
            )

        # Link back to previous page only if there is one.
        if (previous_page := _pagination.previous_page) is not None:
            feed.add_link(
                annotator.suppressed_url_with_pagination(previous_page),
                rel="previous",
            )

        return feed
