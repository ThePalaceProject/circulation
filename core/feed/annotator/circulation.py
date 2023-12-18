from __future__ import annotations

import copy
import datetime
import logging
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from typing import Any

from dependency_injector.wiring import Provide, inject
from flask import url_for
from sqlalchemy.orm import Session

from api.adobe_vendor_id import AuthdataUtility
from api.annotations import AnnotationWriter
from api.circulation import BaseCirculationAPI, CirculationAPI
from api.config import Configuration
from api.lanes import DynamicLane
from api.novelist import NoveListAPI
from core.analytics import Analytics
from core.classifier import Classifier
from core.config import CannotLoadConfiguration
from core.entrypoint import EverythingEntryPoint
from core.external_search import WorkSearchResult
from core.feed.annotator.base import Annotator
from core.feed.opds import UnfulfillableWork
from core.feed.types import (
    Acquisition,
    FeedData,
    FeedEntryType,
    IndirectAcquisition,
    Link,
    WorkEntry,
)
from core.feed.util import strftime
from core.lane import Facets, FacetsWithEntryPoint, Lane, Pagination, WorkList
from core.lcp.credential import LCPCredentialFactory, LCPHashedPassphrase
from core.lcp.exceptions import LCPError
from core.model.circulationevent import CirculationEvent
from core.model.collection import Collection
from core.model.edition import Edition
from core.model.formats import FormatPriorities
from core.model.identifier import Identifier
from core.model.integration import IntegrationConfiguration
from core.model.library import Library
from core.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from core.model.patron import Hold, Loan, Patron
from core.model.work import Work
from core.service.container import Services
from core.util.datetime_helpers import from_timestamp
from core.util.opds_writer import OPDSFeed


class AcquisitionHelper:
    @classmethod
    def license_tags(
        cls,
        license_pool: LicensePool | None,
        loan: Loan | None,
        hold: Hold | None,
    ) -> dict[str, Any] | None:
        acquisition = {}
        # Generate a list of licensing tags. These should be inserted
        # into a <link> tag.
        status = None
        since = None
        until = None

        if not license_pool:
            return None
        default_loan_period = default_reservation_period = None
        collection = license_pool.collection
        obj: Loan | Hold
        if (loan or hold) and not license_pool.open_access:
            if loan:
                obj = loan
            elif hold:
                obj = hold
            default_loan_period = datetime.timedelta(
                collection.default_loan_period(obj.library)
            )
        if loan:
            status = "available"
            since = loan.start
            if not loan.license_pool.unlimited_access:
                until = loan.until(default_loan_period)
        elif hold:
            if not license_pool.open_access:
                default_reservation_period = datetime.timedelta(
                    collection.default_reservation_period
                )
            until = hold.until(default_loan_period, default_reservation_period)
            if hold.position == 0:
                status = "ready"
                since = None
            else:
                status = "reserved"
                since = hold.start
        elif (
            license_pool.open_access
            or license_pool.unlimited_access
            or (license_pool.licenses_available > 0 and license_pool.licenses_owned > 0)
        ):
            status = "available"
        else:
            status = "unavailable"

        acquisition["availability_status"] = status
        if since:
            acquisition["availability_since"] = strftime(since)
        if until:
            acquisition["availability_until"] = strftime(until)

        # Open-access pools do not need to display <opds:holds> or <opds:copies>.
        if license_pool.open_access or license_pool.unlimited_access:
            return acquisition

        total = license_pool.patrons_in_hold_queue or 0

        if hold:
            if hold.position is None:
                # This shouldn't happen, but if it does, assume we're last
                # in the list.
                position = total
            else:
                position = hold.position

            if position > 0:
                acquisition["holds_position"] = str(position)
            if position > total:
                # The patron's hold position appears larger than the total
                # number of holds. This happens frequently because the
                # number of holds and a given patron's hold position are
                # updated by different processes. Don't propagate this
                # appearance to the client.
                total = position
            elif position == 0 and total == 0:
                # The book is reserved for this patron but they're not
                # counted as having it on hold. This is the only case
                # where we know that the total number of holds is
                # *greater* than the hold position.
                total = 1
        acquisition["holds_total"] = str(total)

        acquisition["copies_total"] = str(license_pool.licenses_owned or 0)
        acquisition["copies_available"] = str(license_pool.licenses_available or 0)

        return acquisition

    @classmethod
    def format_types(cls, delivery_mechanism: DeliveryMechanism) -> list[str]:
        """Generate a set of types suitable for passing into
        acquisition_link().
        """
        types = []
        # If this is a streaming book, you have to get an OPDS entry, then
        # get a direct link to the streaming reader from that.
        if delivery_mechanism.is_streaming:
            types.append(OPDSFeed.ENTRY_TYPE)

        # If this is a DRM-encrypted book, you have to get through the DRM
        # to get the goodies inside.
        drm = delivery_mechanism.drm_scheme_media_type
        if drm:
            types.append(drm)

        # Finally, you get the goodies.
        media = delivery_mechanism.content_type_media_type
        if media:
            types.append(media)

        return types


class CirculationManagerAnnotator(Annotator):
    hidden_content_types: list[str]

    @inject
    def __init__(
        self,
        lane: WorkList | None,
        active_loans_by_work: dict[Work, Loan] | None = None,
        active_holds_by_work: dict[Work, Hold] | None = None,
        active_fulfillments_by_work: dict[Work, Any] | None = None,
        hidden_content_types: list[str] | None = None,
        analytics: Analytics = Provide[Services.analytics.analytics],
    ) -> None:
        if lane:
            logger_name = "Circulation Manager Annotator for %s" % lane.display_name
        else:
            logger_name = "Circulation Manager Annotator"
        self.log = logging.getLogger(logger_name)
        self.lane = lane
        self.active_loans_by_work = active_loans_by_work or {}
        self.active_holds_by_work = active_holds_by_work or {}
        self.active_fulfillments_by_work = active_fulfillments_by_work or {}
        self.hidden_content_types = hidden_content_types or []
        self.facet_view = "feed"
        self.analytics = analytics

    def is_work_entry_solo(self, work: Work) -> bool:
        """Return a boolean value indicating whether the work's OPDS catalog entry is served by itself,
            rather than as a part of the feed.

        :param work: Work object
        :type work: core.model.work.Work

        :return: Boolean value indicating whether the work's OPDS catalog entry is served by itself,
            rather than as a part of the feed
        :rtype: bool
        """
        return any(
            work in x  # type: ignore[operator] # Mypy gets confused with complex "in" statements
            for x in (
                self.active_loans_by_work,
                self.active_holds_by_work,
                self.active_fulfillments_by_work,
            )
        )

    def _lane_identifier(self, lane: WorkList | None) -> int | None:
        if isinstance(lane, Lane):
            return lane.id
        return None

    def top_level_title(self) -> str:
        return ""

    def default_lane_url(self) -> str:
        return self.feed_url(None)

    def lane_url(self, lane: WorkList) -> str:
        return self.feed_url(lane)

    def url_for(self, *args: Any, **kwargs: Any) -> str:
        return url_for(*args, **kwargs)

    def facet_url(self, facets: Facets) -> str:
        return self.feed_url(self.lane, facets=facets, default_route=self.facet_view)

    def feed_url(
        self,
        lane: WorkList | None,
        facets: FacetsWithEntryPoint | None = None,
        pagination: Pagination | None = None,
        default_route: str = "feed",
        extra_kwargs: dict[str, Any] | None = None,
    ) -> str:
        if isinstance(lane, WorkList) and hasattr(lane, "url_arguments"):
            route, kwargs = lane.url_arguments
        else:
            route = default_route
            lane_identifier = self._lane_identifier(lane)
            kwargs = dict(lane_identifier=lane_identifier)
        if facets is not None:
            kwargs.update(dict(list(facets.items())))
        if pagination is not None:
            kwargs.update(dict(list(pagination.items())))
        if extra_kwargs:
            kwargs.update(extra_kwargs)
        return self.url_for(route, _external=True, **kwargs)

    def navigation_url(self, lane: Lane) -> str:
        return self.url_for(
            "navigation_feed",
            lane_identifier=self._lane_identifier(lane),
            library_short_name=lane.library.short_name,
            _external=True,
        )

    def active_licensepool_for(
        self, work: Work, library: Library | None = None
    ) -> LicensePool | None:
        loan = self.active_loans_by_work.get(work) or self.active_holds_by_work.get(
            work
        )
        if loan:
            # The active license pool is the one associated with
            # the loan/hold.
            return loan.license_pool
        else:
            # There is no active loan. Use the default logic for
            # determining the active license pool.
            return super().active_licensepool_for(work, library=library)

    @staticmethod
    def _prioritized_formats_for_pool(
        licensepool: LicensePool,
    ) -> tuple[list[str], list[str]]:
        collection: Collection = licensepool.collection
        config: IntegrationConfiguration = collection.integration_configuration

        # Consult the configuration information for the integration configuration
        # that underlies the license pool's collection. The configuration
        # information _might_ contain a set of prioritized DRM schemes and
        # content types.
        prioritized_drm_schemes: list[str] = (
            config.settings_dict.get(FormatPriorities.PRIORITIZED_DRM_SCHEMES_KEY) or []
        )

        content_setting: list[str] = (
            config.settings_dict.get(FormatPriorities.PRIORITIZED_CONTENT_TYPES_KEY)
            or []
        )
        return prioritized_drm_schemes, content_setting

    @staticmethod
    def _deprioritized_lcp_content(
        licensepool: LicensePool,
    ) -> bool:
        collection: Collection = licensepool.collection
        config: IntegrationConfiguration = collection.integration_configuration

        # Consult the configuration information for the integration configuration
        # that underlies the license pool's collection. The configuration
        # information _might_ contain a flag that indicates whether to deprioritize
        # LCP content. By default, if no configuration value is specified, then
        # the priority of LCP content will be left completely unchanged.

        _prioritize: bool = config.settings_dict.get(
            FormatPriorities.DEPRIORITIZE_LCP_NON_EPUBS_KEY, False
        )
        return _prioritize

    def visible_delivery_mechanisms(
        self, licensepool: LicensePool | None
    ) -> list[LicensePoolDeliveryMechanism]:
        if not licensepool:
            return []

        (
            prioritized_drm_schemes,
            prioritized_content_types,
        ) = CirculationManagerAnnotator._prioritized_formats_for_pool(licensepool)

        return FormatPriorities(
            prioritized_drm_schemes=prioritized_drm_schemes,
            prioritized_content_types=prioritized_content_types,
            hidden_content_types=self.hidden_content_types,
            deprioritize_lcp_non_epubs=CirculationManagerAnnotator._deprioritized_lcp_content(
                licensepool
            ),
        ).prioritize_for_pool(licensepool)

    def annotate_work_entry(
        self,
        entry: WorkEntry,
        updated: datetime.datetime | None = None,
    ) -> None:
        work = entry.work
        identifier = entry.identifier or work.presentation_edition.primary_identifier
        active_license_pool = entry.license_pool or self.active_licensepool_for(work)
        # If OpenSearch included a more accurate last_update_time,
        # use it instead of Work.last_update_time
        updated = entry.work.last_update_time
        if isinstance(work, WorkSearchResult):
            # Opensearch puts this field in a list, but we've set it up
            # so there will be at most one value.
            last_updates = getattr(work._hit, "last_update", [])
            if last_updates:
                # last_update is seconds-since epoch; convert to UTC datetime.
                updated = from_timestamp(last_updates[0])

                # There's a chance that work.last_updated has been
                # modified but the change hasn't made it to the search
                # engine yet. Even then, we stick with the search
                # engine value, because a sorted list is more
                # important to the import process than an up-to-date
                # 'last update' value.

        super().annotate_work_entry(entry, updated=updated)
        active_loan = self.active_loans_by_work.get(work)
        active_hold = self.active_holds_by_work.get(work)
        active_fulfillment = self.active_fulfillments_by_work.get(work)

        # Now we need to generate a <link> tag for every delivery mechanism
        # that has well-defined media types.
        link_tags = self.acquisition_links(
            active_license_pool,
            active_loan,
            active_hold,
            active_fulfillment,
            identifier,
        )
        if entry.computed:
            for tag in link_tags:
                entry.computed.acquisition_links.append(tag)

    def acquisition_links(
        self,
        active_license_pool: LicensePool | None,
        active_loan: Loan | None,
        active_hold: Hold | None,
        active_fulfillment: Any | None,
        identifier: Identifier,
        can_hold: bool = True,
        can_revoke_hold: bool = True,
        set_mechanism_at_borrow: bool = False,
        direct_fulfillment_delivery_mechanisms: None
        | (list[LicensePoolDeliveryMechanism]) = None,
        add_open_access_links: bool = True,
    ) -> list[Acquisition]:
        """Generate a number of <link> tags that enumerate all acquisition
        methods.

        :param direct_fulfillment_delivery_mechanisms: A way to
            fulfill each LicensePoolDeliveryMechanism in this list will be
            presented as a link with
            rel="http://opds-spec.org/acquisition/open-access", indicating
            that it can be downloaded with no intermediate steps such as
            authentication.
        """
        can_borrow = False
        can_fulfill = False
        can_revoke = False

        if active_loan:
            can_fulfill = True
            can_revoke = True
        elif active_hold:
            # We display the borrow link even if the patron can't
            # borrow the book right this minute.
            can_borrow = True

            can_revoke = can_revoke_hold
        elif active_fulfillment:
            can_fulfill = True
            can_revoke = True
        else:
            # The patron has no existing relationship with this
            # work. Give them the opportunity to check out the work
            # or put it on hold.
            can_borrow = True

        # If there is something to be revoked for this book,
        # add a link to revoke it.
        revoke_links = []
        if active_license_pool and can_revoke:
            revoke_links.append(
                self.revoke_link(active_license_pool, active_loan, active_hold)
            )

        # Add next-step information for every useful delivery
        # mechanism.
        borrow_links = []
        if can_borrow:
            # Borrowing a book gives you an OPDS entry that gives you
            # fulfillment links for every visible delivery mechanism.
            visible_mechanisms = self.visible_delivery_mechanisms(active_license_pool)
            if set_mechanism_at_borrow and active_license_pool:
                # The ebook distributor requires that the delivery
                # mechanism be set at the point of checkout. This means
                # a separate borrow link for each mechanism.
                for mechanism in visible_mechanisms:
                    borrow_links.append(
                        self.borrow_link(
                            active_license_pool, mechanism, [mechanism], active_hold
                        )
                    )
            elif active_license_pool:
                # The ebook distributor does not require that the
                # delivery mechanism be set at the point of
                # checkout. This means a single borrow link with
                # indirectAcquisition tags for every visible delivery
                # mechanism. If a delivery mechanism must be set, it
                # will be set at the point of fulfillment.
                borrow_links.append(
                    self.borrow_link(
                        active_license_pool, None, visible_mechanisms, active_hold
                    )
                )

            # Generate the licensing tags that tell you whether the book
            # is available.
            for link in borrow_links:
                if link is not None:
                    license_tags = AcquisitionHelper.license_tags(
                        active_license_pool, active_loan, active_hold
                    )
                    if license_tags is not None:
                        link.add_attributes(license_tags)

        # Add links for fulfilling an active loan.
        fulfill_links: list[Acquisition | None] = []
        if can_fulfill:
            if active_fulfillment:
                # We're making an entry for a specific fulfill link.
                type = active_fulfillment.content_type
                url = active_fulfillment.content_link
                rel = OPDSFeed.ACQUISITION_REL
                link_tag = self.acquisition_link(
                    rel=rel, href=url, types=[type], active_loan=active_loan
                )
                fulfill_links.append(link_tag)

            elif active_loan and active_loan.fulfillment and active_license_pool:
                # The delivery mechanism for this loan has been
                # set. There is one link for the delivery mechanism
                # that was locked in, and links for any streaming
                # delivery mechanisms.
                #
                # Since the delivery mechanism has already been locked in,
                # we choose not to use visible_delivery_mechanisms --
                # they already chose it and they're stuck with it.
                for lpdm in active_license_pool.delivery_mechanisms:
                    if (
                        lpdm is active_loan.fulfillment
                        or lpdm.delivery_mechanism.is_streaming
                    ):
                        fulfill_links.append(
                            self.fulfill_link(
                                active_license_pool,
                                active_loan,
                                lpdm.delivery_mechanism,
                            )
                        )
            elif active_license_pool is not None:
                # The delivery mechanism for this loan has not been
                # set. There is one fulfill link for every visible
                # delivery mechanism.
                for lpdm in self.visible_delivery_mechanisms(active_license_pool):
                    fulfill_links.append(
                        self.fulfill_link(
                            active_license_pool, active_loan, lpdm.delivery_mechanism
                        )
                    )

        open_access_links: list[Acquisition | None] = []
        if (
            active_license_pool is not None
            and direct_fulfillment_delivery_mechanisms is not None
        ):
            for lpdm in direct_fulfillment_delivery_mechanisms:
                # These links use the OPDS 'open-access' link relation not
                # because they are open access in the licensing sense, but
                # because they are ways to download the book "without any
                # requirement, which includes payment and registration."
                #
                # To avoid confusion, we explicitly add a dc:rights
                # statement to each link explaining what the rights are to
                # this title.
                direct_fulfill = self.fulfill_link(
                    active_license_pool,
                    active_loan,
                    lpdm.delivery_mechanism,
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                )
                if direct_fulfill:
                    direct_fulfill.add_attributes(self.rights_attributes(lpdm))
                    open_access_links.append(direct_fulfill)

        # If this is an open-access book, add an open-access link for
        # every delivery mechanism with an associated resource.
        # But only if this library allows it, generally this is if
        # a library has no patron authentication attached to it
        if (
            add_open_access_links
            and active_license_pool
            and active_license_pool.open_access
        ):
            for lpdm in active_license_pool.delivery_mechanisms:
                if lpdm.resource:
                    open_access_links.append(
                        self.open_access_link(active_license_pool, lpdm)
                    )

        return [
            x
            for x in borrow_links + fulfill_links + open_access_links + revoke_links
            if x is not None
        ]

    def revoke_link(
        self,
        active_license_pool: LicensePool,
        active_loan: Loan | None,
        active_hold: Hold | None,
    ) -> Acquisition | None:
        return None

    def borrow_link(
        self,
        active_license_pool: LicensePool,
        borrow_mechanism: LicensePoolDeliveryMechanism | None,
        fulfillment_mechanisms: list[LicensePoolDeliveryMechanism],
        active_hold: Hold | None = None,
    ) -> Acquisition | None:
        return None

    def fulfill_link(
        self,
        license_pool: LicensePool,
        active_loan: Loan | None,
        delivery_mechanism: DeliveryMechanism,
        rel: str = OPDSFeed.ACQUISITION_REL,
    ) -> Acquisition | None:
        return None

    def open_access_link(
        self, pool: LicensePool, lpdm: LicensePoolDeliveryMechanism
    ) -> Acquisition:
        kw: dict[str, Any] = dict(rel=OPDSFeed.OPEN_ACCESS_REL, type="")

        # Start off assuming that the URL associated with the
        # LicensePoolDeliveryMechanism's Resource is the URL we should
        # send for download purposes. This will be the case unless we
        # previously mirrored that URL somewhere else.
        href = lpdm.resource.url

        rep = lpdm.resource.representation
        if rep:
            if rep.media_type:
                kw["type"] = rep.media_type
            href = rep.public_url
        kw["href"] = href
        kw.update(self.rights_attributes(lpdm))
        link = Acquisition(**kw)
        link.availability_status = "available"
        return link

    def rights_attributes(
        self, lpdm: LicensePoolDeliveryMechanism | None
    ) -> dict[str, str]:
        """Create a dictionary of tag attributes that explain the
        rights status of a LicensePoolDeliveryMechanism.

        If nothing is known, the dictionary will be empty.
        """
        if not lpdm or not lpdm.rights_status or not lpdm.rights_status.uri:
            return {}
        rights_attr = "rights"
        return {rights_attr: lpdm.rights_status.uri}

    @classmethod
    def acquisition_link(
        cls,
        rel: str,
        href: str,
        types: list[str] | None,
        active_loan: Loan | None = None,
    ) -> Acquisition:
        if types:
            initial_type = types[0]
            indirect_types = types[1:]
        else:
            initial_type = None
            indirect_types = []
        link = Acquisition(
            href=href,
            rel=rel,
            type=initial_type,
            is_loan=True if active_loan else False,
        )
        indirect = cls.indirect_acquisition(indirect_types)

        if indirect is not None:
            link.indirect_acquisitions = [indirect]
        return link

    @classmethod
    def indirect_acquisition(
        cls, indirect_types: list[str]
    ) -> IndirectAcquisition | None:
        top_level_parent: IndirectAcquisition | None = None
        parent: IndirectAcquisition | None = None
        for t in indirect_types:
            indirect_link = IndirectAcquisition(type=t)
            if parent is not None:
                parent.children = [indirect_link]
            parent = indirect_link
            if top_level_parent is None:
                top_level_parent = indirect_link
        return top_level_parent


class LibraryAnnotator(CirculationManagerAnnotator):
    def __init__(
        self,
        circulation: CirculationAPI | None,
        lane: WorkList | None,
        library: Library,
        patron: Patron | None = None,
        active_loans_by_work: dict[Work, Loan] | None = None,
        active_holds_by_work: dict[Work, Hold] | None = None,
        active_fulfillments_by_work: dict[Work, Any] | None = None,
        facet_view: str = "feed",
        top_level_title: str = "All Books",
        library_identifies_patrons: bool = True,
        facets: FacetsWithEntryPoint | None = None,
    ) -> None:
        """Constructor.

        :param library_identifies_patrons: A boolean indicating
          whether or not this library can distinguish between its
          patrons. A library might not authenticate patrons at
          all, or it might distinguish patrons from non-patrons in a
          way that does not allow it to keep track of individuals.

          If this is false, links that imply the library can
          distinguish between patrons will not be included. Depending
          on the configured collections, some extra links may be
          added, for direct acquisition of titles that would normally
          require a loan.
        """
        super().__init__(
            lane,
            active_loans_by_work=active_loans_by_work,
            active_holds_by_work=active_holds_by_work,
            active_fulfillments_by_work=active_fulfillments_by_work,
            hidden_content_types=library.settings.hidden_content_types,
        )
        self.circulation = circulation
        self.library: Library = library
        self.patron = patron
        self.lanes_by_work: dict[Work, list[Any]] = defaultdict(list)
        self.facet_view = facet_view
        self._adobe_id_cache: dict[str, Any] = {}
        self._top_level_title = top_level_title
        self.identifies_patrons = library_identifies_patrons
        self.facets = facets or None

    def top_level_title(self) -> str:
        return self._top_level_title

    def permalink_for(self, identifier: Identifier) -> tuple[str, str]:
        # TODO: Do not force OPDS types
        url = self.url_for(
            "permalink",
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            library_short_name=self.library.short_name,
            _external=True,
        )
        return url, OPDSFeed.ENTRY_TYPE

    def groups_url(
        self, lane: WorkList | None, facets: FacetsWithEntryPoint | None = None
    ) -> str:
        lane_identifier = self._lane_identifier(lane)
        if facets:
            kwargs = dict(list(facets.items()))
        else:
            kwargs = {}

        return self.url_for(
            "acquisition_groups",
            lane_identifier=lane_identifier,
            library_short_name=self.library.short_name,
            _external=True,
            **kwargs,
        )

    def default_lane_url(self, facets: FacetsWithEntryPoint | None = None) -> str:
        return self.groups_url(None, facets=facets)

    def feed_url(  # type: ignore [override]
        self,
        lane: WorkList | None,
        facets: FacetsWithEntryPoint | None = None,
        pagination: Pagination | None = None,
        default_route: str = "feed",
    ) -> str:
        extra_kwargs = dict()
        if self.library:
            extra_kwargs["library_short_name"] = self.library.short_name
        return super().feed_url(lane, facets, pagination, default_route, extra_kwargs)

    def search_url(
        self,
        lane: WorkList | None,
        query: str,
        pagination: Pagination | None,
        facets: FacetsWithEntryPoint | None = None,
    ) -> str:
        lane_identifier = self._lane_identifier(lane)
        kwargs = dict(q=query)
        if facets:
            kwargs.update(dict(list(facets.items())))
        if pagination:
            kwargs.update(dict(list(pagination.items())))
        return self.url_for(
            "lane_search",
            lane_identifier=lane_identifier,
            library_short_name=self.library.short_name,
            _external=True,
            **kwargs,
        )

    def group_uri(
        self, work: Work, license_pool: LicensePool | None, identifier: Identifier
    ) -> tuple[str | None, str]:
        if not work in self.lanes_by_work:
            return None, ""

        lanes = self.lanes_by_work[work]
        if not lanes:
            # I don't think this should ever happen?
            lane_name = None
            url = self.url_for(
                "acquisition_groups",
                lane_identifier=None,
                library_short_name=self.library.short_name,
                _external=True,
            )
            title = "All Books"
            return url, title

        lane = lanes[0]
        self.lanes_by_work[work] = lanes[1:]
        lane_name = ""
        show_feed = False

        if isinstance(lane, dict):
            show_feed = lane.get("link_to_list_feed", show_feed)
            title = lane.get("label", lane_name)
            lane = lane["lane"]

        if isinstance(lane, str):
            return lane, lane_name

        if hasattr(lane, "display_name") and not title:
            title = lane.display_name

        if show_feed:
            return self.feed_url(lane, self.facets), title

        return self.lane_url(lane, self.facets), title

    def lane_url(
        self, lane: WorkList | None, facets: FacetsWithEntryPoint | None = None
    ) -> str:
        # If the lane has sublanes, the URL identifying the group will
        # take the user to another set of groups for the
        # sublanes. Otherwise it will take the user to a list of the
        # books in the lane by author.

        if lane and isinstance(lane, Lane) and lane.sublanes:
            url = self.groups_url(lane, facets=facets)
        elif lane and (isinstance(lane, Lane) or isinstance(lane, DynamicLane)):
            url = self.feed_url(lane, facets)
        else:
            # This lane isn't part of our lane hierarchy. It's probably
            # a WorkList created to represent the top-level. Use the top-level
            # url for it.
            url = self.default_lane_url(facets=facets)
        return url

    def annotate_work_entry(
        self, entry: WorkEntry, updated: datetime.datetime | None = None
    ) -> None:
        super().annotate_work_entry(entry, updated=updated)

        if not entry.computed:
            return

        work = entry.work
        identifier = entry.identifier or work.presentation_edition.primary_identifier

        permalink_uri, permalink_type = self.permalink_for(identifier)
        # TODO: Do not force OPDS types
        if permalink_uri:
            entry.computed.other_links.append(
                Link(href=permalink_uri, rel="alternate", type=permalink_type)
            )
            if self.is_work_entry_solo(work):
                entry.computed.other_links.append(
                    Link(rel="self", href=permalink_uri, type=permalink_type)
                )

        # Add a link to each author tag.
        self.add_author_links(entry)

        # And a series, if there is one.
        if work.series:
            self.add_series_link(entry)

        if NoveListAPI.is_configured(self.library):
            # If NoveList Select is configured, there might be
            # recommendations, too.
            entry.computed.other_links.append(
                Link(
                    rel="recommendations",
                    type=OPDSFeed.ACQUISITION_FEED_TYPE,
                    title="Recommended Works",
                    href=self.url_for(
                        "recommendations",
                        identifier_type=identifier.type,
                        identifier=identifier.identifier,
                        library_short_name=self.library.short_name,
                        _external=True,
                    ),
                )
            )

        # Add a link for related books if available.
        if self.related_books_available(work, self.library):
            entry.computed.other_links.append(
                Link(
                    rel="related",
                    type=OPDSFeed.ACQUISITION_FEED_TYPE,
                    title="Recommended Works",
                    href=self.url_for(
                        "related_books",
                        identifier_type=identifier.type,
                        identifier=identifier.identifier,
                        library_short_name=self.library.short_name,
                        _external=True,
                    ),
                )
            )

        # Add a link to get a patron's annotations for this book.
        if self.identifies_patrons:
            entry.computed.other_links.append(
                Link(
                    rel="http://www.w3.org/ns/oa#annotationService",
                    type=AnnotationWriter.CONTENT_TYPE,
                    href=self.url_for(
                        "annotations_for_work",
                        identifier_type=identifier.type,
                        identifier=identifier.identifier,
                        library_short_name=self.library.short_name,
                        _external=True,
                    ),
                )
            )

        if self.analytics.is_configured():
            entry.computed.other_links.append(
                Link(
                    rel="http://librarysimplified.org/terms/rel/analytics/open-book",
                    href=self.url_for(
                        "track_analytics_event",
                        identifier_type=identifier.type,
                        identifier=identifier.identifier,
                        event_type=CirculationEvent.OPEN_BOOK,
                        library_short_name=self.library.short_name,
                        _external=True,
                    ),
                )
            )

        # Groups is only from the library annotator
        group_uri, group_title = self.group_uri(
            entry.work, entry.license_pool, entry.identifier
        )
        if group_uri:
            entry.computed.other_links.append(
                Link(href=group_uri, rel=OPDSFeed.GROUP_REL, title=str(group_title))
            )

    @classmethod
    def related_books_available(cls, work: Work, library: Library) -> bool:
        """:return: bool asserting whether related books might exist for a particular Work"""
        contributions = work.sort_author and work.sort_author != Edition.UNKNOWN_AUTHOR

        return bool(contributions or work.series or NoveListAPI.is_configured(library))

    def language_and_audience_key_from_work(
        self, work: Work
    ) -> tuple[str | None, str | None]:
        language_key = work.language

        audiences = None
        if work.audience == Classifier.AUDIENCE_CHILDREN:
            audiences = [Classifier.AUDIENCE_CHILDREN]
        elif work.audience == Classifier.AUDIENCE_YOUNG_ADULT:
            audiences = Classifier.AUDIENCES_JUVENILE
        elif work.audience == Classifier.AUDIENCE_ALL_AGES:
            audiences = [Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_ALL_AGES]
        elif work.audience in Classifier.AUDIENCES_ADULT:
            audiences = list(Classifier.AUDIENCES_NO_RESEARCH)
        elif work.audience == Classifier.AUDIENCE_RESEARCH:
            audiences = list(Classifier.AUDIENCES)
        else:
            audiences = []

        audience_key = None
        if audiences:
            audience_strings = [urllib.parse.quote_plus(a) for a in sorted(audiences)]
            audience_key = ",".join(audience_strings)

        return language_key, audience_key

    def add_author_links(self, entry: WorkEntry) -> None:
        """Add a link to all authors"""
        if not entry.computed:
            return None

        languages, audiences = self.language_and_audience_key_from_work(entry.work)
        for author_entry in entry.computed.authors:
            if not (name := getattr(author_entry, "name", None)):
                continue

            author_entry.add_attributes(
                {
                    "link": Link(
                        rel="contributor",
                        type=OPDSFeed.ACQUISITION_FEED_TYPE,
                        title=name,
                        href=self.url_for(
                            "contributor",
                            contributor_name=name,
                            languages=languages,
                            audiences=audiences,
                            library_short_name=self.library.short_name,
                            _external=True,
                        ),
                    ),
                }
            )

    def add_series_link(self, entry: WorkEntry) -> None:
        if not entry.computed:
            return None

        series_entry = entry.computed.series
        work = entry.work

        if series_entry is None:
            # There is no series, and thus nothing to annotate.
            # This probably indicates an out-of-date OPDS entry.
            work_id = work.id
            work_title = work.title
            self.log.error(
                'add_series_link() called on work %s ("%s"), which has no Series data in its OPDS WorkEntry.',
                work_id,
                work_title,
            )
            return

        series_name = work.series
        languages, audiences = self.language_and_audience_key_from_work(work)
        href = self.url_for(
            "series",
            series_name=series_name,
            languages=languages,
            audiences=audiences,
            library_short_name=self.library.short_name,
            _external=True,
        )
        series_entry.add_attributes(
            {
                "link": Link(
                    rel="series",
                    type=OPDSFeed.ACQUISITION_FEED_TYPE,
                    title=series_name,
                    href=href,
                ),
            }
        )

    def annotate_feed(self, feed: FeedData) -> None:
        if self.patron:
            # A patron is authenticated.
            self.add_patron(feed)
        else:
            # No patron is authenticated. Show them how to
            # authenticate (or that authentication is not supported).
            self.add_authentication_document_link(feed)

        # Add a 'search' link if the lane is searchable.
        if self.lane and self.lane.search_target:
            search_facet_kwargs = {}
            if self.facets is not None:
                if self.facets.entrypoint_is_default:
                    # The currently selected entry point is a default.
                    # Rather than using it, we want the 'default' behavior
                    # for search, which is to search everything.
                    search_facets = self.facets.navigate(
                        entrypoint=EverythingEntryPoint
                    )
                else:
                    search_facets = self.facets
                search_facet_kwargs.update(dict(list(search_facets.items())))

            lane_identifier = self._lane_identifier(self.lane)
            search_url = self.url_for(
                "lane_search",
                lane_identifier=lane_identifier,
                library_short_name=self.library.short_name,
                _external=True,
                **search_facet_kwargs,
            )
            search_link = dict(
                rel="search",
                type="application/opensearchdescription+xml",
                href=search_url,
            )
            feed.add_link(**search_link)

        if self.identifies_patrons:
            # Since this library authenticates patrons it can offer
            # a bookshelf and an annotation service.
            shelf_link = dict(
                rel="http://opds-spec.org/shelf",
                type=OPDSFeed.ACQUISITION_FEED_TYPE,
                href=self.url_for(
                    "active_loans",
                    library_short_name=self.library.short_name,
                    _external=True,
                ),
            )
            feed.add_link(**shelf_link)

            annotations_link = dict(
                rel="http://www.w3.org/ns/oa#annotationService",
                type=AnnotationWriter.CONTENT_TYPE,
                href=self.url_for(
                    "annotations",
                    library_short_name=self.library.short_name,
                    _external=True,
                ),
            )
            feed.add_link(**annotations_link)

        if self.lane and self.lane.uses_customlists:
            name = None
            if hasattr(self.lane, "customlists") and len(self.lane.customlists) == 1:
                name = self.lane.customlists[0].name
            else:
                _db = Session.object_session(self.library)
                customlist = self.lane.get_customlists(_db)
                if customlist:
                    name = customlist[0].name

            if name:
                crawlable_url = self.url_for(
                    "crawlable_list_feed",
                    list_name=name,
                    library_short_name=self.library.short_name,
                    _external=True,
                )
                crawlable_link = dict(
                    rel="http://opds-spec.org/crawlable",
                    type=OPDSFeed.ACQUISITION_FEED_TYPE,
                    href=crawlable_url,
                )
                feed.add_link(**crawlable_link)

        self.add_configuration_links(feed)

    def add_configuration_links(self, feed: FeedData) -> None:
        _db = Session.object_session(self.library)

        def _add_link(l: dict[str, Any]) -> None:
            feed.add_link(**l)

        library = self.library
        if library.settings.terms_of_service:
            _add_link(
                dict(
                    rel="terms-of-service",
                    href=library.settings.terms_of_service,
                    type="text/html",
                )
            )

        if library.settings.privacy_policy:
            _add_link(
                dict(
                    rel="privacy-policy",
                    href=library.settings.privacy_policy,
                    type="text/html",
                )
            )

        if library.settings.copyright:
            _add_link(
                dict(
                    rel="copyright",
                    href=library.settings.copyright,
                    type="text/html",
                )
            )

        if library.settings.about:
            _add_link(
                dict(
                    rel="about",
                    href=library.settings.about,
                    type="text/html",
                )
            )

        if library.settings.license:
            _add_link(
                dict(
                    rel="license",
                    href=library.settings.license,
                    type="text/html",
                )
            )

        navigation_urls = self.library.settings.web_header_links
        navigation_labels = self.library.settings.web_header_labels
        for url, label in zip(navigation_urls, navigation_labels):
            d = dict(
                href=url,
                title=label,
                type="text/html",
                rel="related",
                role="navigation",
            )
            _add_link(d)

        for type, value in Configuration.help_uris(self.library):
            d = dict(href=value, rel="help")
            if type:
                d["type"] = type
            _add_link(d)

    def acquisition_links(  # type: ignore [override]
        self,
        active_license_pool: LicensePool | None,
        active_loan: Loan | None,
        active_hold: Hold | None,
        active_fulfillment: Any | None,
        identifier: Identifier,
        direct_fulfillment_delivery_mechanisms: None
        | (list[LicensePoolDeliveryMechanism]) = None,
        mock_api: Any | None = None,
    ) -> list[Acquisition]:
        """Generate one or more <link> tags that can be used to borrow,
        reserve, or fulfill a book, depending on the state of the book
        and the current patron.

        :param active_license_pool: The LicensePool for which we're trying to
           generate <link> tags.
        :param active_loan: A Loan object representing the current patron's
           existing loan for this title, if any.
        :param active_hold: A Hold object representing the current patron's
           existing hold on this title, if any.
        :param active_fulfillment: A LicensePoolDeliveryMechanism object
           representing the mechanism, if any, which the patron has chosen
           to fulfill this work.
        :param feed: The OPDSFeed that will eventually contain these <link>
           tags.
        :param identifier: The Identifier of the title for which we're
           trying to generate <link> tags.
        :param direct_fulfillment_delivery_mechanisms: A list of
           LicensePoolDeliveryMechanisms for the given LicensePool
           that should have fulfillment-type <link> tags generated for
           them, even if this method wouldn't normally think that
           makes sense.
        :param mock_api: A mock object to stand in for the API to the
           vendor who provided this LicensePool. If this is not provided, a
           live API for that vendor will be used.
        """
        direct_fulfillment_delivery_mechanisms = (
            direct_fulfillment_delivery_mechanisms or []
        )
        api = mock_api
        if not api and self.circulation and active_license_pool:
            api = self.circulation.api_for_license_pool(active_license_pool)
        if api:
            set_mechanism_at_borrow = (
                api.SET_DELIVERY_MECHANISM_AT == BaseCirculationAPI.BORROW_STEP
            )
            if active_license_pool and not self.identifies_patrons and not active_loan:
                for lpdm in active_license_pool.delivery_mechanisms:
                    if api.can_fulfill_without_loan(None, active_license_pool, lpdm):
                        # This title can be fulfilled without an
                        # active loan, so we're going to add an acquisition
                        # link that goes directly to the fulfillment step
                        # without the 'borrow' step.
                        direct_fulfillment_delivery_mechanisms.append(lpdm)
        else:
            # This is most likely an open-access book. Just put one
            # borrow link and figure out the rest later.
            set_mechanism_at_borrow = False

        return super().acquisition_links(
            active_license_pool,
            active_loan,
            active_hold,
            active_fulfillment,
            identifier,
            can_hold=self.library.settings.allow_holds,
            can_revoke_hold=bool(
                active_hold
                and (
                    not self.circulation
                    or (
                        active_license_pool
                        and self.circulation.can_revoke_hold(
                            active_license_pool, active_hold
                        )
                    )
                )
            ),
            set_mechanism_at_borrow=set_mechanism_at_borrow,
            direct_fulfillment_delivery_mechanisms=direct_fulfillment_delivery_mechanisms,
            add_open_access_links=(not self.identifies_patrons),
        )

    def revoke_link(
        self,
        active_license_pool: LicensePool,
        active_loan: Loan | None,
        active_hold: Hold | None,
    ) -> Acquisition | None:
        if not self.identifies_patrons:
            return None
        url = self.url_for(
            "revoke_loan_or_hold",
            license_pool_id=active_license_pool.id,
            library_short_name=self.library.short_name,
            _external=True,
        )
        kw: dict[str, Any] = dict(href=url, rel=OPDSFeed.REVOKE_LOAN_REL)
        revoke_link_tag = Acquisition(**kw)
        return revoke_link_tag

    def borrow_link(
        self,
        active_license_pool: LicensePool,
        borrow_mechanism: LicensePoolDeliveryMechanism | None,
        fulfillment_mechanisms: list[LicensePoolDeliveryMechanism],
        active_hold: Hold | None = None,
    ) -> Acquisition | None:
        if not self.identifies_patrons:
            return None
        identifier = active_license_pool.identifier
        if borrow_mechanism:
            # Following this link will both borrow the book and set
            # its delivery mechanism.
            mechanism_id = borrow_mechanism.delivery_mechanism.id
        else:
            # Following this link will borrow the book but not set
            # its delivery mechanism.
            mechanism_id = None
        borrow_url = self.url_for(
            "borrow",
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            mechanism_id=mechanism_id,
            library_short_name=self.library.short_name,
            _external=True,
        )
        rel = OPDSFeed.BORROW_REL
        borrow_link = Acquisition(
            rel=rel,
            href=borrow_url,
            type=OPDSFeed.ENTRY_TYPE,
            is_hold=True if active_hold else False,
        )

        indirect_acquisitions: list[IndirectAcquisition] = []
        for lpdm in fulfillment_mechanisms:
            # We have information about one or more delivery
            # mechanisms that will be available at the point of
            # fulfillment. To the extent possible, put information
            # about these mechanisms into the <link> tag as
            # <opds:indirectAcquisition> tags.

            # These are the formats mentioned in the indirect
            # acquisition.
            format_types = AcquisitionHelper.format_types(lpdm.delivery_mechanism)

            # If we can borrow this book, add this delivery mechanism
            # to the borrow link as an <opds:indirectAcquisition>.
            if format_types:
                indirect_acquisition = self.indirect_acquisition(format_types)
                if indirect_acquisition:
                    indirect_acquisitions.append(indirect_acquisition)

        if not indirect_acquisitions:
            # If there's no way to actually get the book, cancel the creation
            # of an OPDS entry altogether.
            raise UnfulfillableWork()

        borrow_link.indirect_acquisitions = indirect_acquisitions
        return borrow_link

    def fulfill_link(
        self,
        license_pool: LicensePool,
        active_loan: Loan | None,
        delivery_mechanism: DeliveryMechanism,
        rel: str = OPDSFeed.ACQUISITION_REL,
    ) -> Acquisition | None:
        """Create a new fulfillment link.

        This link may include tags from the OPDS Extensions for DRM.
        """
        if not self.identifies_patrons and rel != OPDSFeed.OPEN_ACCESS_REL:
            return None
        if isinstance(delivery_mechanism, LicensePoolDeliveryMechanism):
            logging.warning(
                "LicensePoolDeliveryMechanism passed into fulfill_link instead of DeliveryMechanism!"
            )
            delivery_mechanism = delivery_mechanism.delivery_mechanism
        format_types = AcquisitionHelper.format_types(delivery_mechanism)
        if not format_types:
            return None

        fulfill_url = self.url_for(
            "fulfill",
            license_pool_id=license_pool.id,
            mechanism_id=delivery_mechanism.id,
            library_short_name=self.library.short_name,
            _external=True,
        )

        link_tag = self.acquisition_link(
            rel=rel, href=fulfill_url, types=format_types, active_loan=active_loan
        )

        children = AcquisitionHelper.license_tags(license_pool, active_loan, None)
        if children:
            link_tag.add_attributes(children)

        drm_tags = self.drm_extension_tags(
            license_pool, active_loan, delivery_mechanism
        )
        link_tag.add_attributes(drm_tags)
        return link_tag

    def open_access_link(
        self, pool: LicensePool, lpdm: LicensePoolDeliveryMechanism
    ) -> Acquisition:
        link_tag = super().open_access_link(pool, lpdm)
        fulfill_url = self.url_for(
            "fulfill",
            license_pool_id=pool.id,
            mechanism_id=lpdm.delivery_mechanism.id,
            library_short_name=self.library.short_name,
            _external=True,
        )
        link_tag.href = fulfill_url
        return link_tag

    def drm_extension_tags(
        self,
        license_pool: LicensePool,
        active_loan: Loan | None,
        delivery_mechanism: DeliveryMechanism | None,
    ) -> dict[str, Any]:
        """Construct OPDS Extensions for DRM tags that explain how to
        register a device with the DRM server that manages this loan.
        :param delivery_mechanism: A DeliveryMechanism
        """
        if not active_loan or not delivery_mechanism or not self.identifies_patrons:
            return {}

        if delivery_mechanism.drm_scheme == DeliveryMechanism.ADOBE_DRM:
            # Get an identifier for the patron that will be registered
            # with the DRM server.
            patron = active_loan.patron

            # Generate a <drm:licensor> tag that can feed into the
            # Vendor ID service.
            return self.adobe_id_tags(patron)

        if delivery_mechanism.drm_scheme == DeliveryMechanism.LCP_DRM:
            # Generate a <lcp:hashed_passphrase> tag that can be used for the loan
            # in the mobile apps.

            return self.lcp_key_retrieval_tags(active_loan)

        return {}

    def adobe_id_tags(
        self, patron_identifier: str | Patron
    ) -> dict[str, FeedEntryType]:
        """Construct tags using the DRM Extensions for OPDS standard that
        explain how to get an Adobe ID for this patron, and how to
        manage their list of device IDs.
        :param delivery_mechanism: A DeliveryMechanism
        :return: If Adobe Vendor ID delegation is configured, a list
        containing a <drm:licensor> tag. If not, an empty list.
        """
        # CirculationManagerAnnotators are created per request.
        # Within the context of a single request, we can cache the
        # tags that explain how the patron can get an Adobe ID, and
        # reuse them across <entry> tags. This saves a little time,
        # makes tests more reliable, and stops us from providing a
        # different Short Client Token for every <entry> tag.
        if isinstance(patron_identifier, Patron):
            cache_key = str(patron_identifier.id)
        else:
            cache_key = patron_identifier
        cached = self._adobe_id_cache.get(cache_key)
        if cached is None:
            cached = {}
            authdata = None
            try:
                authdata = AuthdataUtility.from_config(self.library)
            except CannotLoadConfiguration as e:
                logging.error(
                    "Cannot load Short Client Token configuration; outgoing OPDS entries will not have DRM autodiscovery support",
                    exc_info=e,
                )
                return {}
            if authdata:
                vendor_id, token = authdata.short_client_token_for_patron(
                    patron_identifier
                )
                drm_licensor = FeedEntryType.create(
                    vendor=vendor_id, clientToken=FeedEntryType(text=token)
                )
                cached = {"drm_licensor": drm_licensor}

            self._adobe_id_cache[cache_key] = cached
        else:
            cached = copy.deepcopy(cached)
        return cached

    def lcp_key_retrieval_tags(self, active_loan: Loan) -> dict[str, FeedEntryType]:
        # In the case of LCP we have to include a patron's hashed passphrase
        # inside the acquisition link so client applications can use it to open the LCP license
        # without having to ask the user to enter their password
        # https://readium.org/lcp-specs/notes/lcp-key-retrieval.html#including-a-hashed-passphrase-in-an-opds-1-catalog

        db = Session.object_session(active_loan)
        lcp_credential_factory = LCPCredentialFactory()

        response = {}

        try:
            hashed_passphrase: LCPHashedPassphrase = (
                lcp_credential_factory.get_hashed_passphrase(db, active_loan.patron)
            )
            response["lcp_hashed_passphrase"] = FeedEntryType(
                text=hashed_passphrase.hashed
            )
        except LCPError:
            # The patron's passphrase wasn't generated yet and not present in the database.
            pass

        return response

    def add_patron(self, feed: FeedData) -> None:
        if not self.patron or not self.identifies_patrons:
            return None
        patron_details = {}
        if self.patron.username:
            patron_details["username"] = self.patron.username
        if self.patron.authorization_identifier:
            patron_details[
                "authorizationIdentifier"
            ] = self.patron.authorization_identifier

        patron_tag = FeedEntryType.create(**patron_details)
        feed.metadata.patron = patron_tag

    def add_authentication_document_link(self, feed_obj: FeedData) -> None:
        """Create a <link> tag that points to the circulation
        manager's Authentication for OPDS document
        for the current library.
        """
        # Even if self.identifies_patrons is false, we include this link,
        # because this document is the one that explains there is no
        # patron authentication at this library.
        feed_obj.add_link(
            rel="http://opds-spec.org/auth/document",
            href=self.url_for(
                "authentication_document",
                library_short_name=self.library.short_name,
                _external=True,
            ),
        )
