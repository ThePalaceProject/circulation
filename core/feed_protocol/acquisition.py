"""OPDS 1 paged feed"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from flask import Response
from sqlalchemy.orm import Session

from api.circulation import FulfillmentInfo
from api.problem_details import NOT_FOUND_ON_REMOTE
from core.feed_protocol.annotator.base import Annotator
from core.feed_protocol.annotator.circulation import LibraryAnnotator
from core.feed_protocol.annotator.loan_and_hold import LibraryLoanAndHoldAnnotator
from core.feed_protocol.opds import OPDSFeedProtocol
from core.feed_protocol.types import Link, WorkEntry
from core.feed_protocol.utils import serializer_for
from core.lane import FacetsWithEntryPoint
from core.model.edition import Edition
from core.model.licensing import LicensePool
from core.model.patron import Hold, Loan
from core.model.work import Work
from core.opds import AcquisitionFeed, UnfulfillableWork
from core.util.datetime_helpers import utc_now
from core.util.flask_util import OPDSFeedResponse
from core.util.opds_writer import AtomFeed, OPDSMessage

if TYPE_CHECKING:
    from core.lane import WorkList


class OPDSAcquisitionFeed(OPDSFeedProtocol):
    """An Acquisition Feed which is not tied to any particular format.
    It is simply responsible for creating different types of feeds."""

    def __init__(self, title, url, facets, pagination, annotator) -> None:
        self.annotator = annotator
        self.url = url
        self.title = title
        super().__init__(facets, pagination)

    def generate_feed(
        self,
        work_entries: List[WorkEntry],
    ):
        """Generate the feed metadata and links.
        We assume the entries have already been annotated."""
        self._feed.add_metadata("id", text=self.url)
        self._feed.add_metadata("title", text=self.title)
        self._feed.add_metadata("updated", text=AtomFeed._strftime(utc_now()))
        self._feed.add_link(href=self.url, rel="self")

        # TODO: use the entry cache (is this still relevant?)
        for entry in work_entries:
            self._feed.entries.append(entry)

        self.annotator.annotate_feed(self._feed)

    def add_pagination_links(self, works, lane):
        """Add pagination links to the feed"""
        if not self._pagination:
            return
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

    def add_facet_links(self, lane):
        """Add facet links to the feed"""
        if not self._facets:
            return
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

    def as_response(self, **kwargs) -> Response:
        """Serialize the feed using the serializer protocol"""
        return OPDSFeedResponse(self._serializer.serialize_feed(self._feed), **kwargs)

    @classmethod
    def page(
        cls,
        _db: Session,
        url: str,
        lane: WorkList,
        annotator: LibraryAnnotator,
        facets,
        pagination,
        search_engine,
    ):
        works = lane.works(_db, facets, pagination, search_engine)
        """A basic paged feed"""
        # "works" MAY be a generator, we want a list
        if not isinstance(works, list):
            works = list(works)

        entries = []
        feed = OPDSAcquisitionFeed(
            lane.display_name, url, facets, pagination, annotator
        )
        for work in works:
            entry = cls.single_entry(work, annotator)
            if entry:
                entries.append(entry)

        feed.generate_feed(entries)
        feed.add_pagination_links(works, lane)
        feed.add_facet_links(lane)

        if isinstance(facets, FacetsWithEntryPoint):
            feed.add_breadcrumb_links(lane, facets.entrypoint)

        return feed

    @classmethod
    def active_loans_for(
        cls,
        circulation,
        patron,
        annotator: Optional[LibraryAnnotator] = None,
        **response_kwargs,
    ):
        """A patron specific feed that only contains the loans and holds of a patron"""
        db = Session.object_session(patron)
        active_loans_by_work = {}
        for loan in patron.loans:
            work = loan.work
            if work:
                active_loans_by_work[work] = loan

        # There might be multiple holds for the same work so we gather all of them and choose the best one.
        all_holds_by_work: Dict[Work, List[Hold]] = {}
        for hold in patron.holds:
            work = hold.work
            if not work:
                continue

            if work not in all_holds_by_work:
                all_holds_by_work[work] = []

            all_holds_by_work[work].append(hold)

        active_holds_by_work: Dict[Work, List[Hold]] = {}
        for work, list_of_holds in all_holds_by_work.items():
            active_holds_by_work[
                work
            ] = LibraryLoanAndHoldAnnotator.choose_best_hold_for_work(list_of_holds)

        if not annotator:
            annotator = LibraryLoanAndHoldAnnotator(
                circulation, None, patron.library, patron
            )

        annotator.active_holds_by_work = active_holds_by_work
        annotator.active_loans_by_work = active_loans_by_work
        url = annotator.url_for(
            "active_loans", library_short_name=patron.library.short_name, _external=True
        )
        works = patron.works_on_loan_or_on_hold()

        _work_entries = [cls.single_entry(work, annotator) for work in works]
        work_entries = [entry for entry in _work_entries if entry is not None]

        feed = OPDSAcquisitionFeed("Active loans and holds", url, None, None, annotator)
        feed.generate_feed(work_entries)
        response = feed.as_response(max_age=0, private=True)

        last_modified = patron.last_loan_activity_sync
        if last_modified:
            response.last_modified = last_modified
        return response

    @classmethod
    def single_entry_loans_feed(
        cls,
        _db: Session,
        circulation: Any,
        item: LicensePool | Loan,
        annotator: LibraryAnnotator,
        fulfillment: FulfillmentInfo | None = None,
        **response_kwargs,
    ):
        """A single entry as a standalone feed specific to a patron"""
        if not item:
            raise ValueError("Argument 'item' must be non-empty")

        if isinstance(item, LicensePool):
            license_pool = item
            library = circulation.library
        elif isinstance(item, (Loan, Hold)):
            license_pool = item.license_pool
            library = item.library
        else:
            raise ValueError(
                "Argument 'item' must be an instance of {}, {}, or {} classes".format(
                    Loan, Hold, LicensePool
                )
            )

        log = logging.getLogger(cls.__name__)

        # Sometimes the pool or work may be None
        # In those cases we have to protect against the exceptions
        try:
            work = license_pool.work or license_pool.presentation_edition.work
        except AttributeError as ex:
            log.error(f"Error retrieving a Work Object {ex}")
            log.error(
                f"Error Data: {license_pool} | {license_pool and license_pool.presentation_edition}"
            )
            return NOT_FOUND_ON_REMOTE

        if not work:
            return NOT_FOUND_ON_REMOTE

        _db = Session.object_session(item)
        active_loans_by_work: Any = {}
        active_holds_by_work: Any = {}
        active_fulfillments_by_work = {}
        item_dictionary = None

        if isinstance(item, Loan):
            item_dictionary = active_loans_by_work
        elif isinstance(item, Hold):
            item_dictionary = active_holds_by_work

        if item_dictionary is not None:
            item_dictionary[work] = item

        if fulfillment:
            active_fulfillments_by_work[work] = fulfillment

        annotator.active_loans_by_work = active_loans_by_work
        annotator.active_holds_by_work = active_holds_by_work
        annotator.active_fulfillments_by_work = active_fulfillments_by_work
        identifier = license_pool.identifier

        # TODO: Error response as is done in _single_entry_response
        url = annotator.url_for(
            "loan_or_hold_detail",
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            library_short_name=library.short_name,
            _external=True,
        )

        entry = cls.single_entry(work, annotator, even_if_no_license_pool=True)

        # TODO: max_age and private response kwargs
        if entry and entry.computed:
            serializer = serializer_for("OPDS1")()
            return serializer.to_string(serializer.serialize_work_entry(entry.computed))

    @classmethod
    def single_entry(
        cls,
        work: Work | Edition | None,
        annotator: Annotator,
        even_if_no_license_pool=False,
        force_create=False,
        use_cache=True,
    ) -> Optional[WorkEntry]:
        """Turn a work into an annotated work entry for an acquisition feed."""
        identifier = None
        if isinstance(work, Edition):
            active_edition = work
            identifier = active_edition.primary_identifier
            active_license_pool = None
            work = None
        else:
            if not work:
                # We have a license pool but no work. Most likely we don't have
                # metadata for this work yet.
                return None
            active_license_pool = annotator.active_licensepool_for(work)
            if active_license_pool:
                identifier = active_license_pool.identifier
                active_edition = active_license_pool.presentation_edition
            elif work.presentation_edition:
                active_edition = work.presentation_edition
                identifier = active_edition.primary_identifier

        # There's no reason to present a book that has no active license pool.
        if not identifier:
            logging.warning("%r HAS NO IDENTIFIER", work)
            return None

        if not active_license_pool and not even_if_no_license_pool:
            logging.warning("NO ACTIVE LICENSE POOL FOR %r", work)
            return cls.error_message(
                identifier,
                403,
                "I've heard about this work but have no active licenses for it.",
            )

        if not active_edition:
            logging.warning("NO ACTIVE EDITION FOR %r", active_license_pool)
            return cls.error_message(
                identifier,
                403,
                "I've heard about this work but have no metadata for it.",
            )

        try:
            return cls._create_entry(
                work, active_license_pool, active_edition, identifier, annotator
            )
        except UnfulfillableWork as e:
            logging.info(
                "Work %r is not fulfillable, refusing to create an <entry>.",
                work,
            )
            return cls.error_message(
                identifier,
                403,
                "I know about this work but can offer no way of fulfilling it.",
            )
        except Exception as e:
            logging.error("Exception generating OPDS entry for %r", work, exc_info=e)
            return None

    @classmethod
    def _create_entry(
        cls, work, active_licensepool, edition, identifier, annotator
    ) -> WorkEntry:
        entry = WorkEntry(
            work=work,
            license_pool=active_licensepool,
            edition=edition,
            identifier=identifier,
        )
        annotator.annotate_work_entry(entry)
        return entry

    @classmethod
    def error_message(cls, identifier, error_status, error_message):
        """Turn an error result into an OPDSMessage suitable for
        adding to a feed.
        """
        return OPDSMessage(identifier.urn, error_status, error_message)
