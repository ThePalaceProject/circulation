from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Self

from sqlalchemy import Boolean, and_, exists, false, or_, select
from sqlalchemy.orm import Query, Session
from sqlalchemy.sql import ColumnElement

from palace.manager.feed.acquisition import OPDSAcquisitionFeed
from palace.manager.feed.annotator.admin.suppressed import AdminSuppressedAnnotator
from palace.manager.search.external_search import (
    QueryParseException,
    SuppressedWorkFilter,
)
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.lane import Pagination
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work, WorkGenre
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from palace.manager.search.external_search import ExternalSearchIndex


class VisibilityFilter(StrEnum):
    """Visibility filter values for the suppressed works feed."""

    ALL = "all"
    MANUALLY_SUPPRESSED = "manually-suppressed"
    POLICY_FILTERED = "policy-filtered"

    @property
    def display_title(self) -> str:
        """Return the human-readable display title for this filter."""
        titles = {
            VisibilityFilter.ALL: "All",
            VisibilityFilter.MANUALLY_SUPPRESSED: "Manually Hidden",
            VisibilityFilter.POLICY_FILTERED: "Policy Filtered",
        }
        return titles[self]


@dataclass(frozen=True)
class FacetGroup:
    """Represents a single facet option for OPDS facet links."""

    group_name: str
    filter_value: VisibilityFilter
    facets: SuppressedFacets
    is_selected: bool
    is_default: bool


class SuppressedFacets:
    """Facets for filtering the suppressed works feed by visibility status."""

    VISIBILITY_FACET_GROUP_NAME = "Visibility"

    def __init__(self, visibility: VisibilityFilter | None = None) -> None:
        self.visibility: VisibilityFilter = visibility or VisibilityFilter.ALL

    @classmethod
    def from_request(cls, get_argument: Callable[[str, str], str]) -> SuppressedFacets:
        """Load visibility filter from request args.

        :param get_argument: A function that retrieves request arguments.
        :return: A SuppressedFacets instance with the requested visibility filter.
        """
        visibility_str = get_argument("visibility", VisibilityFilter.ALL)
        try:
            visibility = VisibilityFilter(visibility_str)
        except ValueError:
            visibility = VisibilityFilter.ALL
        return cls(visibility=visibility)

    def items(self) -> Iterable[tuple[str, str]]:
        """Yield (key, value) tuples for query string parameters."""
        if self.visibility != VisibilityFilter.ALL:
            yield ("visibility", self.visibility.value)

    def navigate(self, visibility: VisibilityFilter | None = None) -> SuppressedFacets:
        """Create a new SuppressedFacets with a different visibility filter."""
        return SuppressedFacets(visibility=visibility or self.visibility)

    @property
    def facet_groups(self) -> Iterable[FacetGroup]:
        """Yield FacetGroup objects for OPDS facet links."""
        for filter_value in VisibilityFilter:
            yield FacetGroup(
                group_name=self.VISIBILITY_FACET_GROUP_NAME,
                filter_value=filter_value,
                facets=self.navigate(visibility=filter_value),
                is_selected=self.visibility == filter_value,
                is_default=filter_value == VisibilityFilter.ALL,
            )


class AdminSuppressedFeed(OPDSAcquisitionFeed):
    @classmethod
    def suppressed_query(
        cls,
        _db: Session,
        library: Library,
        visibility_filter: VisibilityFilter,
    ) -> Query[Work]:
        """Build the query for works in the suppressed feed.

        :param _db: Database session.
        :param library: The library that requested the suppressed feed.
        :param visibility_filter: Filter by visibility status.
        """
        settings = library.settings

        is_manually_suppressed = Work.suppressed_for.any(Library.id == library.id)

        # Build policy filter conditions for audience and genre filtering
        policy_filter_conditions: list[ColumnElement[Boolean]] = []

        if settings.filtered_audiences:
            policy_filter_conditions.append(
                Work.audience.in_(settings.filtered_audiences)
            )

        if settings.filtered_genres:
            genre_filter = (
                select(1)
                .select_from(WorkGenre)
                .join(Genre, WorkGenre.genre_id == Genre.id)
                .where(
                    WorkGenre.work_id == Work.id,
                    Genre.name.in_(settings.filtered_genres),
                )
            )
            policy_filter_conditions.append(exists(genre_filter))

        # Build the policy-filtered condition
        is_policy_filtered = (
            or_(*policy_filter_conditions) if policy_filter_conditions else false()
        )

        # Apply visibility filter
        if visibility_filter == VisibilityFilter.MANUALLY_SUPPRESSED:
            visibility_condition = is_manually_suppressed
        elif visibility_filter == VisibilityFilter.POLICY_FILTERED:
            # Policy filtered but NOT manually suppressed
            visibility_condition = and_(~is_manually_suppressed, is_policy_filtered)
        else:
            # Default: all hidden works (manually suppressed OR policy-filtered)
            visibility_condition = or_(is_manually_suppressed, is_policy_filtered)

        collection_ids = [
            collection.id for collection in library.associated_collections
        ]

        return (
            _db.query(Work)
            .join(LicensePool)
            .join(Edition)
            .filter(
                and_(
                    LicensePool.suppressed == false(),
                    LicensePool.collection_id.in_(collection_ids),
                    visibility_condition,
                )
            )
            .order_by(Edition.sort_title)
        )

    @classmethod
    def suppressed(
        cls,
        _db: Session,
        title: str,
        annotator: AdminSuppressedAnnotator,
        pagination: Pagination | None = None,
        facets: SuppressedFacets | None = None,
    ) -> Self:
        pagination = pagination or Pagination.default()
        facets = facets or SuppressedFacets()

        # Build query params for URLs (includes facet params)
        facet_params = dict(facets.items())

        start_url = annotator.suppressed_url(**facet_params)
        library = annotator.library
        q = cls.suppressed_query(_db, library, facets.visibility)
        works = pagination.modify_database_query(_db, q).all()
        next_page_item_count = (
            pagination.next_page.modify_database_query(_db, q).count()
            if pagination.next_page
            else 0
        )

        feed = cls(title, start_url, works, annotator, pagination=pagination)
        feed.generate_feed()

        # Render a 'start' link
        top_level_title = annotator.top_level_title()
        feed.add_link(start_url, rel="start", title=top_level_title)

        # Add facet links for visibility filtering
        for facet_group in facets.facet_groups:
            facet_url = annotator.suppressed_url(**dict(facet_group.facets.items()))
            facet_link = cls.facet_link(
                href=facet_url,
                title=facet_group.filter_value.display_title,
                facet_group_name=facet_group.group_name,
                is_active=facet_group.is_selected,
                is_default=facet_group.is_default,
            )
            feed._feed.facet_links.append(facet_link)

        # Link to next page only if there are more entries than current page size.
        if next_page_item_count > 0:
            feed.add_link(
                href=annotator.suppressed_url_with_pagination(
                    pagination.next_page, **facet_params
                ),
                rel="next",
            )

        # Link back to first page only if we're not the first page.
        if pagination.offset > 0:
            feed.add_link(
                annotator.suppressed_url_with_pagination(
                    pagination.first_page, **facet_params
                ),
                rel="first",
            )

        # Link back to previous page only if there is one.
        if (previous_page := pagination.previous_page) is not None:
            feed.add_link(
                annotator.suppressed_url_with_pagination(previous_page, **facet_params),
                rel="previous",
            )

        return feed

    @classmethod
    def suppressed_search(
        cls,
        _db: Session,
        title: str,
        url: str,
        annotator: AdminSuppressedAnnotator,
        search_engine: ExternalSearchIndex,
        query: str,
        pagination: Pagination | None = None,
    ) -> Self | ProblemDetail:
        """Search within suppressed/hidden works.

        :param _db: Database session.
        :param title: Title for the search results feed.
        :param url: Base URL for the search endpoint.
        :param annotator: AdminAnnotator for generating links and annotations.
        :param search_engine: Search engine for executing the search.
        :param query: The search query string.
        :param pagination: Optional pagination settings.
        :return: An SuppressedFeed with search results, or a ProblemDetail on error.
        """
        from palace.manager.core.problem_details import INVALID_INPUT

        _pagination = pagination or Pagination.default()
        library = annotator.library

        # Create filter that matches only suppressed/filtered works
        search_filter = SuppressedWorkFilter(collections=library, library=library)

        # Execute search
        try:
            results = search_engine.query_works(
                query, search_filter, _pagination, debug=False
            )
        except QueryParseException as e:
            return INVALID_INPUT.detailed(str(e))

        # Convert search results to Work objects
        work_ids = [result.work_id for result in results]
        if work_ids:
            works = _db.query(Work).filter(Work.id.in_(work_ids)).all()
            # Maintain search result order
            works_by_id = {w.id: w for w in works}
            works = [works_by_id[wid] for wid in work_ids if wid in works_by_id]
        else:
            works = []

        # Build feed
        feed = cls(title, url, works, annotator, pagination=_pagination)
        feed.generate_feed()

        # Add navigation links
        start_url = annotator.suppressed_url()
        feed.add_link(start_url, rel="start", title=annotator.top_level_title())
        feed.add_link(start_url, rel="up", title="Hidden Books")

        # Pagination links
        if len(results) >= _pagination.size:
            # There might be more results
            next_page = _pagination.next_page
            if next_page:
                feed.add_link(
                    href=annotator.suppressed_search_url(query, next_page),
                    rel="next",
                )

        if _pagination.offset > 0:
            feed.add_link(
                annotator.suppressed_search_url(query, _pagination.first_page),
                rel="first",
            )

        if (previous_page := _pagination.previous_page) is not None:
            feed.add_link(
                annotator.suppressed_search_url(query, previous_page),
                rel="previous",
            )

        return feed
