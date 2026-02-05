"""OPDS feeds, they can be serialized to either OPDS 1 or OPDS 2"""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterable
from typing import TYPE_CHECKING, Any

from dependency_injector.wiring import Provide, inject
from sqlalchemy.orm import Query, Session

from palace.manager.api.problem_details import NOT_FOUND_ON_REMOTE
from palace.manager.core.entrypoint import EntryPoint
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.feed.annotator.base import Annotator
from palace.manager.feed.annotator.circulation import (
    CirculationManagerAnnotator,
    LibraryAnnotator,
)
from palace.manager.feed.annotator.loan_and_hold import LibraryLoanAndHoldAnnotator
from palace.manager.feed.facets.base import FacetsWithEntryPoint
from palace.manager.feed.facets.constants import FacetConstants
from palace.manager.feed.facets.feed import Facets
from palace.manager.feed.facets.search import SearchFacets
from palace.manager.feed.opds import BaseOPDSFeed, UnfulfillableWork
from palace.manager.feed.types import FeedData, Link, WorkEntry
from palace.manager.feed.util import strftime
from palace.manager.search.external_search import (
    ExternalSearchIndex,
)
from palace.manager.search.pagination import Pagination
from palace.manager.search.query import QueryParseException
from palace.manager.sqlalchemy.constants import LinkRelations
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.lane import (
    Lane,
)
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.flask_util import OPDSEntryResponse, OPDSFeedResponse
from palace.manager.util.opds_writer import OPDSMessage
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
    from palace.manager.api.circulation.fulfillment import UrlFulfillment
    from palace.manager.feed.worklist.base import WorkList


class OPDSAcquisitionFeed(BaseOPDSFeed):
    """An Acquisition Feed which is not tied to any particular format.
    It is simply responsible for creating different types of feeds."""

    def __init__(
        self,
        title: str,
        url: str,
        works: Iterable[Work],
        annotator: CirculationManagerAnnotator,
        facets: FacetsWithEntryPoint | None = None,
        pagination: Pagination | None = None,
        precomposed_entries: list[OPDSMessage] | None = None,
    ) -> None:
        self.annotator = annotator
        self._facets = facets
        self._pagination = pagination
        super().__init__(title, url, precomposed_entries=precomposed_entries)
        for work in works:
            try:
                entry = self.single_entry(work, self.annotator)
            except PalaceValueError:
                self.log.exception("Error creating entry for %r", work)
                continue
            if isinstance(entry, WorkEntry):
                self._feed.entries.append(entry)

    def generate_feed(self, annotate: bool = True) -> None:
        """Generate the feed metadata and links.
        We assume the entries have already been annotated."""
        self._feed.metadata.id = self.url
        self._feed.metadata.title = self.title
        self._feed.metadata.updated = strftime(utc_now())
        self._feed.add_link(href=self.url, rel="self")
        if annotate:
            self.annotator.annotate_feed(self._feed)

    def add_pagination_links(self, works: list[Work], lane: WorkList) -> None:
        """Add pagination links to the feed"""
        if not self._pagination:
            return None
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

    def add_facet_links(self, lane: WorkList) -> None:
        """Add facet links to the feed"""
        if self._facets is None:
            return None
        else:
            facets = self._facets
        entrypoints = facets.selectable_entrypoints(lane)
        if entrypoints:
            # A paginated feed may have multiple entry points into the
            # same dataset.
            def make_link(ep: type[EntryPoint]) -> str:
                return self.annotator.feed_url(
                    lane, facets=facets.navigate(entrypoint=ep)
                )

            self.add_entrypoint_links(
                self._feed, make_link, entrypoints, facets.entrypoint
            )

        # Facet links
        facet_links = self.facet_links(self.annotator, self._facets)
        for linkdata in facet_links:
            self._feed.facet_links.append(linkdata)

    @classmethod
    def facet_links(
        cls, annotator: CirculationManagerAnnotator, facets: FacetsWithEntryPoint
    ) -> Generator[Link]:
        """Create links for this feed's navigational facet groups.

        This does not create links for the entry point facet group,
        because those links should only be present in certain
        circumstances, and this method doesn't know if those
        circumstances apply. You need to decide whether to call
        add_entrypoint_links in addition to calling this method.
        """
        for facet_group in facets.facet_groups:
            url = annotator.facet_url(facet_group.facets)
            if not url:
                continue
            group_title = Facets.GROUP_DISPLAY_TITLES.get(facet_group.group)
            facet_title = Facets.FACET_DISPLAY_TITLES.get(facet_group.value)
            if not facet_title:
                display_lambda = Facets.FACET_DISPLAY_TITLES_DYNAMIC.get(
                    facet_group.group
                )
                facet_title = (
                    display_lambda(facet_group.facets) if display_lambda else None
                )
            if not (group_title and facet_title):
                # This facet group or facet, is not recognized by the
                # system. It may be left over from an earlier version,
                # or just weird junk data.
                continue
            yield cls.facet_link(
                url,
                str(facet_title),
                str(group_title),
                facet_group.is_selected,
                facet_group.is_default,
            )

    @classmethod
    def facet_link(
        cls,
        href: str,
        title: str,
        facet_group_name: str,
        is_active: bool,
        is_default: bool,
    ) -> Link:
        """Build a set of attributes for a facet link.

        :param href: Destination of the link.
        :param title: Human-readable description of the facet.
        :param facet_group_name: The facet group to which the facet belongs,
           e.g. "Sort By".
        :param is_active: True if this is the client's currently
           selected facet.
        :param is_default: True if this is the default facet
        :return: A dictionary of attributes, suitable for passing as
            keyword arguments into OPDSFeed.add_link_to_feed.
        """
        return Link(
            href=href,
            title=title,
            rel=LinkRelations.FACET_REL,
            facet_group=facet_group_name,
            active_facet=is_active,
            default_facet=is_default,
        )

    def as_error_response(self, **kwargs: Any) -> OPDSFeedResponse:
        """Convert this feed into an OPDSFeedResponse that should be treated
        by intermediaries as an error -- that is, treated as private
        and not cached.
        """
        kwargs["max_age"] = 0
        kwargs["private"] = True
        return self.as_response(**kwargs)

    @classmethod
    def _create_entry(
        cls,
        work: Work,
        active_licensepool: LicensePool | None,
        edition: Edition,
        identifier: Identifier,
        annotator: Annotator,
    ) -> WorkEntry:
        entry = WorkEntry(
            work=work,
            edition=edition,
            identifier=identifier,
            license_pool=active_licensepool,
        )
        annotator.annotate_work_entry(entry)
        return entry

    ## OPDS1 specifics
    @classmethod
    def add_entrypoint_links(
        cls,
        feed: FeedData,
        url_generator: Callable[[type[EntryPoint]], str],
        entrypoints: Iterable[type[EntryPoint]],
        selected_entrypoint: type[EntryPoint] | None,
        group_name: str = "Formats",
    ) -> None:
        """Add links to a feed forming an OPDS facet group for a set of
        EntryPoints.

        :param feed: A FeedData object.
        :param url_generator: A callable that returns the entry point
            URL when passed an EntryPoint.
        :param entrypoints: An iterable of all EntryPoints in the facet group.
        :param selected_entrypoint: The current EntryPoint, if selected.
        """
        entrypoints = list(entrypoints)

        if len(entrypoints) == 1 and selected_entrypoint in (None, entrypoints[0]):
            # There is only one entry point. Unless the currently
            # selected entry point is somehow different, there's no
            # need to put any links at all here -- a facet group with
            # one one facet might as well not be there.
            return

        is_default = True
        for entrypoint in entrypoints:
            link = cls._entrypoint_link(
                url_generator, entrypoint, selected_entrypoint, is_default, group_name
            )
            if link is not None:
                feed.links.append(link)
                is_default = False

    @classmethod
    def _entrypoint_link(
        cls,
        url_generator: Callable[[type[EntryPoint]], str],
        entrypoint: type[EntryPoint],
        selected_entrypoint: type[EntryPoint] | None,
        is_default: bool,
        group_name: str,
    ) -> Link | None:
        """Create arguments for add_link_to_feed for a link that navigates
        between EntryPoints.
        """
        display_title = EntryPoint.DISPLAY_TITLES.get(entrypoint)
        if not display_title:
            # Shouldn't happen.
            return None

        url = url_generator(entrypoint)
        is_selected = entrypoint is selected_entrypoint
        link = cls.facet_link(url, display_title, group_name, is_selected, is_default)

        # Unlike a normal facet group, every link in this facet
        # group has an additional attribute marking it as an entry
        # point.
        #
        # In OPDS 2 this can become an additional rel value,
        # removing the need for a custom attribute.
        link.facet_group_type = FacetConstants.ENTRY_POINT_REL
        return link

    def add_breadcrumb_links(
        self, lane: WorkList, entrypoint: type[EntryPoint] | None = None
    ) -> None:
        """Add information necessary to find your current place in the
        site's navigation.

        A link with rel="start" points to the start of the site

        An Entrypoint section describes the current entry point.

        A breadcrumbs section contains a sequence of breadcrumb links.
        """
        # Add the top-level link with rel='start'
        annotator = self.annotator
        top_level_title = annotator.top_level_title() or "Collection Home"
        self.add_link(annotator.default_lane_url(), rel="start", title=top_level_title)

        # Add a link to the direct parent with rel="up".
        #
        # TODO: the 'direct parent' may be the same lane but without
        # the entry point specified. Fixing this would also be a good
        # opportunity to refactor the code for figuring out parent and
        # parent_title.
        parent = None
        if isinstance(lane, Lane):
            parent = lane.parent
        if parent and parent.display_name:
            parent_title = parent.display_name
        else:
            parent_title = top_level_title

        if parent:
            up_uri = annotator.lane_url(parent)
            self.add_link(up_uri, rel="up", title=parent_title)
        self.add_breadcrumbs(lane, entrypoint=entrypoint)

        # Annotate the feed with a simplified:entryPoint for the
        # current EntryPoint.
        self.show_current_entrypoint(entrypoint)

    def add_breadcrumbs(
        self,
        lane: WorkList,
        include_lane: bool = False,
        entrypoint: type[EntryPoint] | None = None,
    ) -> None:
        """Add list of ancestor links in a breadcrumbs element.

        :param lane: Add breadcrumbs from up to this lane.
        :param include_lane: Include `lane` itself in the breadcrumbs.
        :param entrypoint: The currently selected entrypoint, if any.

        TODO: The switchover from "no entry point" to "entry point" needs
        its own breadcrumb link.
        """
        if entrypoint is None:
            entrypoint_query = ""
        else:
            entrypoint_query = "?entrypoint=" + entrypoint.INTERNAL_NAME

        # Breadcrumbs for lanes may be end up being cut off by a
        # patron-type-specific root lane. If so, that lane -- not the
        # site root -- should become the first breadcrumb.
        site_root_lane = None
        usable_parentage = []
        if lane is not None:
            for ancestor in [lane] + list(lane.parentage):
                if isinstance(ancestor, Lane) and ancestor.root_for_patron_type:
                    # Root lane for a specific patron type. The root is
                    # treated specially, so it should not be added to
                    # usable_parentage. Any lanes between this lane and the
                    # library root should not be included at all.
                    site_root_lane = ancestor
                    break

                if ancestor != lane or include_lane:
                    # A lane may appear in its own breadcrumbs
                    # only if include_lane is True.
                    usable_parentage.append(ancestor)

        annotator = self.annotator
        if lane == site_root_lane or (
            site_root_lane is None
            and annotator.lane_url(lane) == annotator.default_lane_url()
        ):
            # There are no extra breadcrumbs: either we are at the
            # site root, or we are at a lane that is the root for a
            # specific patron type.
            return

        breadcrumbs = []

        # Add root link. This is either the link to the site root
        # or to the root lane for some patron type.
        if site_root_lane is None:
            root_url = annotator.default_lane_url()
            root_title = annotator.top_level_title()
        else:
            root_url = annotator.lane_url(site_root_lane)
            root_title = site_root_lane.display_name
        root_link = Link(href=root_url, title=root_title)
        breadcrumbs.append(root_link)

        # Add entrypoint selection link
        if entrypoint:
            breadcrumbs.append(
                Link(
                    href=root_url + entrypoint_query,
                    title=entrypoint.INTERNAL_NAME,
                )
            )

        # Add links for all usable lanes between `lane` and `site_root_lane`
        # (possibly including `lane` itself).
        for ancestor in reversed(usable_parentage):
            lane_url = annotator.lane_url(ancestor)
            if lane_url == root_url:
                # Root lane for the entire site.
                break

            breadcrumbs.append(
                Link(
                    href=lane_url + entrypoint_query,
                    title=ancestor.display_name,
                )
            )

        # Append the breadcrumbs to the feed.
        self._feed.breadcrumbs = breadcrumbs

    def show_current_entrypoint(self, entrypoint: type[EntryPoint] | None) -> None:
        """Annotate this given feed with a simplified:entryPoint
        attribute pointing to the current entrypoint's TYPE_URI.

        This gives clients an overall picture of the type of works in
        the feed, and a way to distinguish between one EntryPoint
        and another.

        :param entrypoint: An EntryPoint.
        """
        if not entrypoint:
            return None

        if not entrypoint.URI:
            return None
        self._feed.entrypoint = entrypoint.URI

    @classmethod
    def error_message(
        cls, identifier: Identifier, error_status: int, error_message: str
    ) -> OPDSMessage:
        """Turn an error result into an OPDSMessage suitable for
        adding to a feed.
        """
        return OPDSMessage(identifier.urn, error_status, error_message)

    # All feed generating classmethods below
    # Each classmethod creates a different kind of feed

    @classmethod
    @inject
    def page(
        cls,
        _db: Session,
        title: str,
        url: str,
        worklist: WorkList,
        annotator: CirculationManagerAnnotator,
        facets: FacetsWithEntryPoint | None,
        pagination: Pagination | None,
        *,
        search_engine: ExternalSearchIndex = Provide["search.index"],
    ) -> OPDSAcquisitionFeed:
        works = worklist.works(
            _db, facets=facets, pagination=pagination, search_engine=search_engine
        )
        """A basic paged feed"""
        # "works" MAY be a generator, we want a list
        if not isinstance(works, list):
            works = list(works)

        feed = OPDSAcquisitionFeed(
            title, url, works, annotator, facets=facets, pagination=pagination
        )

        feed.generate_feed()
        feed.add_pagination_links(works, worklist)
        feed.add_facet_links(worklist)

        if isinstance(facets, FacetsWithEntryPoint):
            feed.add_breadcrumb_links(worklist, facets.entrypoint)

        return feed

    @classmethod
    def active_loans_for(
        cls,
        circulation: CirculationApiDispatcher | None,
        patron: Patron,
        annotator: LibraryAnnotator | None = None,
        **response_kwargs: Any,
    ) -> OPDSAcquisitionFeed:
        """A patron specific feed that only contains the loans and holds of a patron"""
        db = Session.object_session(patron)
        active_loans_by_work = {}
        for loan in patron.loans:
            work = loan.work
            if work:
                active_loans_by_work[work] = loan

        # There might be multiple holds for the same work so we gather all of them and choose the best one.
        all_holds_by_work: dict[Work, list[Hold]] = {}
        for hold in patron.holds:
            work = hold.work
            if not work:
                continue

            if work not in all_holds_by_work:
                all_holds_by_work[work] = []

            all_holds_by_work[work].append(hold)

        active_holds_by_work: dict[Work, Hold] = {}
        for work, list_of_holds in all_holds_by_work.items():
            active_holds_by_work[work] = (
                LibraryLoanAndHoldAnnotator.choose_best_hold_for_work(list_of_holds)
            )

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

        feed = OPDSAcquisitionFeed("Active loans and holds", url, works, annotator)
        feed.generate_feed()
        return feed

    @classmethod
    def single_entry_loans_feed(
        cls,
        circulation: Any,
        item: LicensePool | Loan | Hold | None,
        annotator: LibraryAnnotator | None = None,
        fulfillment: UrlFulfillment | None = None,
        **response_kwargs: Any,
    ) -> OPDSEntryResponse | ProblemDetail:
        """A single entry as a standalone feed specific to a patron"""
        if not item:
            raise PalaceValueError("Argument 'item' must be non-empty")

        if isinstance(item, LicensePool):
            license_pool = item
            library = circulation.library
        elif isinstance(item, (Loan, Hold)):
            license_pool = item.license_pool
            library = item.library
        else:
            raise PalaceValueError(
                "Argument 'item' must be an instance of {}, {}, or {} classes".format(
                    Loan, Hold, LicensePool
                )
            )

        if not annotator:
            annotator = LibraryLoanAndHoldAnnotator(circulation, None, library)

        # Sometimes the pool or work may be None
        # In those cases we have to protect against the exceptions
        work: Work | None = None
        if license_pool.work:
            work = license_pool.work
        elif license_pool.presentation_edition:
            work = license_pool.presentation_edition.work

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

        entry = cls.single_entry(work, annotator, even_if_no_license_pool=True)

        if isinstance(entry, OPDSMessage):
            response_kwargs["max_age"] = 0

        return cls.entry_as_response(entry, **response_kwargs)

    @classmethod
    def single_entry(
        cls,
        work: Work | Edition,
        annotator: Annotator,
        even_if_no_license_pool: bool = False,
    ) -> WorkEntry | OPDSMessage:
        """Turn a work into an annotated work entry for an acquisition feed."""
        identifier = None
        _work: Work
        active_edition: Edition | None
        if isinstance(work, Edition):
            active_edition = work
            identifier = active_edition.primary_identifier
            active_license_pool = None
            if active_edition.work is None:
                # We always need a work for an entry
                raise PalaceValueError(
                    f"Edition has no associated work: {active_edition!r}"
                )
            _work = active_edition.work
        else:
            _work = work
            active_license_pool = annotator.active_licensepool_for(work)
            if active_license_pool:
                identifier = active_license_pool.identifier
                active_edition = active_license_pool.presentation_edition
            elif work.presentation_edition:
                active_edition = work.presentation_edition
                identifier = active_edition.primary_identifier
            else:
                active_edition = None

        # There's no reason to present a book that has no active license pool.
        if not identifier:
            raise PalaceValueError(f"Work has no associated identifier: {work!r}")

        if not active_license_pool and not even_if_no_license_pool:
            cls.logger().warning("NO ACTIVE LICENSE POOL FOR %r", work)
            return cls.error_message(
                identifier,
                403,
                "I've heard about this work but have no active licenses for it.",
            )

        if not active_edition:
            cls.logger().warning("NO ACTIVE EDITION FOR %r", active_license_pool)
            return cls.error_message(
                identifier,
                403,
                "I've heard about this work but have no metadata for it.",
            )

        try:
            return cls._create_entry(
                _work, active_license_pool, active_edition, identifier, annotator
            )
        except UnfulfillableWork as e:
            cls.logger().info(
                "Work %r is not fulfillable, refusing to create an <entry>.",
                work,
                exc_info=e,
            )
            return cls.error_message(
                identifier,
                403,
                "I know about this work but can offer no way of fulfilling it.",
            )

    @classmethod
    @inject
    def groups(
        cls,
        _db: Session,
        title: str,
        url: str,
        worklist: WorkList,
        annotator: LibraryAnnotator,
        pagination: Pagination | None = None,
        facets: FacetsWithEntryPoint | None = None,
        *,
        search_engine: ExternalSearchIndex = Provide["search.index"],
        search_debug: bool = False,
        work_ids_to_exclude: list[int] | None = None,
    ) -> OPDSAcquisitionFeed:
        """Internal method called by groups() when a grouped feed
        must be regenerated.
        """

        # Try to get a set of (Work, WorkList) 2-tuples
        # to make a normal grouped feed.
        works_and_lanes = [
            x
            for x in worklist.groups(
                _db=_db,
                pagination=pagination,
                facets=facets,
                search_engine=search_engine,
                debug=search_debug,
            )
        ]
        # Make a typical grouped feed.
        all_works = []
        for work, sublane in works_and_lanes:

            # skip works in the exclusion list
            if work_ids_to_exclude and work.id in work_ids_to_exclude:
                continue

            if sublane == worklist:
                # We are looking at the groups feed for (e.g.)
                # "Science Fiction", and we're seeing a book
                # that is featured within "Science Fiction" itself
                # rather than one of the sublanes.
                #
                # We want to assign this work to a group called "All
                # Science Fiction" and point its 'group URI' to
                # the linear feed of the "Science Fiction" lane
                # (as opposed to the groups feed, which is where we
                # are now).
                v = dict(
                    lane=worklist,
                    label=worklist.display_name_for_all,
                    link_to_list_feed=True,
                )
            else:
                # We are looking at the groups feed for (e.g.)
                # "Science Fiction", and we're seeing a book
                # that is featured within one of its sublanes,
                # such as "Space Opera".
                #
                # We want to assign this work to a group derived
                # from the sublane.
                v = dict(lane=sublane)

            annotator.lanes_by_work[work].append(v)
            all_works.append(work)

        feed = OPDSAcquisitionFeed(
            title, url, all_works, annotator, facets=facets, pagination=pagination
        )
        feed.generate_feed()

        # Regardless of whether or not the entries in feed can be
        # grouped together, we want to apply certain feed-level
        # annotations.

        # A grouped feed may link to alternate entry points into
        # the data.
        if facets:
            entrypoints = facets.selectable_entrypoints(worklist)
            if entrypoints:

                def make_link(ep: type[EntryPoint]) -> str:
                    return annotator.groups_url(
                        worklist, facets=facets.navigate(entrypoint=ep)
                    )

                cls.add_entrypoint_links(
                    feed._feed, make_link, entrypoints, facets.entrypoint
                )

            # A grouped feed may have breadcrumb links.
            feed.add_breadcrumb_links(worklist, facets.entrypoint)

        return feed

    @classmethod
    def search(
        cls,
        _db: Session,
        title: str,
        url: str,
        lane: WorkList,
        search_engine: ExternalSearchIndex,
        query: str,
        annotator: LibraryAnnotator,
        pagination: Pagination | None = None,
        facets: FacetsWithEntryPoint | None = None,
        **response_kwargs: Any,
    ) -> OPDSAcquisitionFeed | ProblemDetail:
        """Run a search against the given search engine and return
        the results as a Flask Response.

        :param _db: A database connection
        :param title: The title of the resulting OPDS feed.
        :param url: The URL from which the feed will be served.
        :param search_engine: An ExternalSearchIndex.
        :param query: The search query
        :param pagination: A Pagination
        :param facets: A Facets
        :param annotator: An Annotator
        :param response_kwargs: Keyword arguments to pass into the OPDSFeedResponse
            constructor.
        :return: An ODPSFeedResponse
        """
        facets = facets or SearchFacets()
        pagination = pagination or Pagination.default()

        try:
            results = lane.search(
                _db, query, search_engine, pagination=pagination, facets=facets
            )
        except QueryParseException as e:
            return INVALID_INPUT.detailed(e.detail)

        feed = OPDSAcquisitionFeed(
            title, url, results, annotator, facets=facets, pagination=pagination
        )
        feed.generate_feed()
        feed.add_link(
            annotator.default_lane_url(), rel="start", title=annotator.top_level_title()
        )

        # A feed of search results may link to alternate entry points
        # into those results.
        entrypoints = facets.selectable_entrypoints(lane)
        if entrypoints:

            def make_link(ep: type[EntryPoint]) -> str:
                return annotator.search_url(
                    lane, query, pagination=None, facets=facets.navigate(entrypoint=ep)
                )

            cls.add_entrypoint_links(
                feed._feed,
                make_link,
                entrypoints,
                facets.entrypoint,
            )

        if len(results) > 0:
            # There are works in this list. Add a 'next' link.
            next_url = annotator.search_url(lane, query, pagination.next_page, facets)
            feed.add_link(href=next_url, rel="next")

        if pagination.offset > 0:
            first_url = annotator.search_url(lane, query, pagination.first_page, facets)
            feed.add_link(rel="first", href=first_url)

        previous_page = pagination.previous_page
        if previous_page:
            previous_url = annotator.search_url(lane, query, previous_page, facets)
            feed.add_link(rel="previous", href=previous_url)

        # Add "up" link.
        feed.add_link(
            annotator.lane_url(lane),
            rel="up",
            title=str(lane.display_name),
        )

        # We do not add breadcrumbs to this feed since you're not
        # technically searching the this lane; you are searching the
        # library's entire collection, using _some_ of the constraints
        # imposed by this lane (notably language and audience).
        return feed

    @classmethod
    def from_query(
        cls,
        query: Query[Work],
        _db: Session,
        feed_name: str,
        url: str,
        pagination: Pagination,
        url_fn: Callable[[int], str],
        annotator: CirculationManagerAnnotator,
    ) -> OPDSAcquisitionFeed:
        """Build  a feed representing one page of a given list. Currently used for
        creating an OPDS feed for a custom list and not cached.

        TODO: This is used by the circulation manager admin interface.
        Investigate changing the code that uses this to use the search
        index -- this is inefficient and creates an alternate code path
        that may harbor bugs.

        TODO: This cannot currently return OPDSFeedResponse because the
        admin interface modifies the feed after it's generated.

        """
        page_of_works = pagination.modify_database_query(_db, query)
        pagination.total_size = int(query.count())

        feed = OPDSAcquisitionFeed(
            feed_name, url, page_of_works, annotator, pagination=pagination
        )
        feed.generate_feed(annotate=False)

        if pagination.total_size > 0 and pagination.has_next_page:
            feed.add_link(url_fn(pagination.next_page.offset), rel="next")
        if pagination.offset > 0:
            feed.add_link(url_fn(pagination.first_page.offset), rel="first")
        if pagination.previous_page:
            feed.add_link(
                url_fn(pagination.previous_page.offset),
                rel="previous",
            )

        return feed


class LookupAcquisitionFeed(OPDSAcquisitionFeed):
    """Used when the user has requested a lookup of a specific identifier,
    which may be different from the identifier used by the Work's
    default LicensePool.
    """

    @classmethod
    def single_entry(cls, work: tuple[Identifier, Work], annotator: Annotator) -> WorkEntry | OPDSMessage:  # type: ignore[override]
        # This comes in as a tuple, which deviates from the typical behaviour
        identifier, _work = work

        active_licensepool: LicensePool | None
        if identifier.licensed_through:
            active_licensepool = identifier.licensed_through[0]
        else:
            # Use the default active LicensePool for the Work.
            active_licensepool = annotator.active_licensepool_for(_work)

        error_status = error_message = None
        if not active_licensepool:
            error_status = 404
            error_message = "Identifier not found in collection"
        elif identifier.work != _work:
            error_status = 500
            error_message = (
                'I tried to generate an OPDS entry for the identifier "%s" using a Work not associated with that identifier.'
                % identifier.urn
            )

        if error_status:
            return cls.error_message(identifier, error_status, error_message or "")

        edition = (
            active_licensepool.presentation_edition
            if active_licensepool
            else _work.presentation_edition
        )
        if edition is not None:
            try:
                return cls._create_entry(
                    _work, active_licensepool, edition, identifier, annotator
                )
            except UnfulfillableWork:
                pass
        cls.logger().info(
            "Work %r is not fulfillable, refusing to create an <entry>.", _work
        )
        return cls.error_message(
            identifier,
            403,
            "I know about this work but can offer no way of fulfilling it.",
        )
