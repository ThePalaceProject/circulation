from typing import Optional

from sqlalchemy import and_
from sqlalchemy.orm import Session
from typing_extensions import Self

from core.feed.acquisition import OPDSAcquisitionFeed
from core.feed.annotator.admin import AdminAnnotator
from core.lane import Pagination
from core.model.licensing import LicensePool


class AdminFeed(OPDSAcquisitionFeed):
    @classmethod
    def suppressed(
        cls,
        _db: Session,
        title: str,
        url: str,
        annotator: AdminAnnotator,
        pagination: Optional[Pagination] = None,
    ) -> Self:
        _pagination = pagination or Pagination.default()

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
        pools = _pagination.modify_database_query(_db, q).all()

        works = [pool.work for pool in pools]
        feed = cls(title, url, works, annotator, pagination=_pagination)
        feed.generate_feed()

        # Render a 'start' link
        top_level_title = annotator.top_level_title()
        start_uri = annotator.groups_url(None)

        feed.add_link(start_uri, rel="start", title=top_level_title)

        # Render an 'up' link, same as the 'start' link to indicate top-level feed
        feed.add_link(start_uri, rel="up", title=top_level_title)

        if len(works) > 0:
            # There are works in this list. Add a 'next' link.
            feed.add_link(
                href=annotator.suppressed_url(_pagination.next_page),
                rel="next",
            )

        if _pagination.offset > 0:
            feed.add_link(
                annotator.suppressed_url(_pagination.first_page),
                rel="first",
            )

        previous_page = _pagination.previous_page
        if previous_page:
            feed.add_link(
                annotator.suppressed_url(previous_page),
                rel="previous",
            )

        return feed
