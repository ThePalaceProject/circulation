from datetime import datetime
from typing import Any

from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.feed.annotator.circulation import LibraryAnnotator
from palace.manager.feed.annotator.verbose import VerboseAnnotator
from palace.manager.feed.types import Category, FeedData, Link, WorkEntry
from palace.manager.search.pagination import Pagination
from palace.manager.sqlalchemy.model.datasource import DataSource
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

    # Visibility status categories for distinguishing manually suppressed vs policy-filtered works
    VISIBILITY_STATUS_SCHEME = "http://palaceproject.io/terms/visibility-status"
    VISIBILITY_MANUALLY_SUPPRESSED = "manually-suppressed"
    VISIBILITY_POLICY_FILTERED = "policy-filtered"

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

        Works filtered by library policy (audience/genre) get a visibility
        category but no suppress/unsuppress links since they are controlled
        by library settings.
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

        # Determine visibility status
        is_manually_suppressed = self.library in work.suppressed_for
        is_policy_filtered = work.is_filtered_for_library(self.library)

        # Add visibility status category for hidden works
        # Manual suppression takes precedence over policy filtering
        if is_manually_suppressed:
            entry.computed.categories.append(
                Category(
                    scheme=self.VISIBILITY_STATUS_SCHEME,
                    term=self.VISIBILITY_MANUALLY_SUPPRESSED,
                    label="Manually Suppressed",
                )
            )
        elif is_policy_filtered:
            entry.computed.categories.append(
                Category(
                    scheme=self.VISIBILITY_STATUS_SCHEME,
                    term=self.VISIBILITY_POLICY_FILTERED,
                    label="Policy Filtered",
                )
            )

        # Add suppress/unsuppress links based on visibility status
        # Policy-filtered works don't get these links (controlled by settings)
        if active_license_pool and not active_license_pool.suppressed:
            if is_manually_suppressed:
                # Manually suppressed works get an unsuppress link
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
            elif not is_policy_filtered:
                # Works that are visible (not hidden) get a suppress link
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
            # Policy-filtered only: no suppress/unsuppress links

        # Edit link is always present
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

    def suppressed_url(self, **kwargs: str) -> str:
        return self.url_for(
            "suppressed",
            library_short_name=self.library.short_name,
            _external=True,
            **kwargs,
        )

    def suppressed_url_with_pagination(
        self, pagination: Pagination, **kwargs: str
    ) -> str:
        url_kwargs = dict(list(pagination.items()))
        url_kwargs.update(kwargs)
        return self.suppressed_url(**url_kwargs)

    def suppressed_search_url(
        self, query: str, pagination: Pagination | None = None
    ) -> str:
        """Generate URL for suppressed work search results.

        :param query: The search query string.
        :param pagination: Optional pagination for the search results.
        """
        kwargs: dict[str, Any] = {"q": query}
        if pagination:
            kwargs.update(dict(list(pagination.items())))
        return self.url_for(
            "suppressed_search",
            library_short_name=self.library.short_name,
            _external=True,
            **kwargs,
        )

    def annotate_feed(self, feed: FeedData) -> None:
        # Add a 'search' link that searches within suppressed/hidden works.
        search_url = self.url_for(
            "suppressed_search",
            library_short_name=self.library.short_name,
            _external=True,
        )
        feed.add_link(
            search_url, rel="search", type="application/opensearchdescription+xml"
        )
