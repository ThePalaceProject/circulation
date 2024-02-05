from __future__ import annotations

import datetime
import json
from typing import TYPE_CHECKING

from dependency_injector.wiring import Provide, inject

from api.admin.problem_details import (
    CUSTOMLIST_ENTRY_NOT_VALID_FOR_LIBRARY,
    CUSTOMLIST_SOURCE_COLLECTION_MISSING,
)
from core.external_search import ExternalSearchIndex, SortKeyPagination
from core.lane import SearchFacets, WorkList
from core.model.customlist import CustomList, CustomListEntry
from core.model.library import Library
from core.model.licensing import LicensePool
from core.service.container import Services
from core.util.log import LoggerMixin
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class CustomListQueries(LoggerMixin):
    @classmethod
    def share_locally_with_library(
        cls, _db, customlist: CustomList, library: Library
    ) -> ProblemDetail | bool:
        # All customlist collections must be present in the library
        log = cls.logger()
        log.info(
            f"Attempting to share customlist '{customlist.name}' with library '{library.name}'."
        )
        for collection in customlist.collections:
            if collection not in library.collections:
                log.info(
                    f"Unable to share customlist: Collection '{collection.name}' is missing from the library."
                )
                return CUSTOMLIST_SOURCE_COLLECTION_MISSING

        # All entries must be valid for the library
        library_collection_ids = [c.id for c in library.collections]
        entry: CustomListEntry
        missing_work_id_count = 0
        for entry in customlist.entries:
            # It appears that many many lists have entries without works.
            # see https://ebce-lyrasis.atlassian.net/browse/PP-708 for the full story.
            # Because of this frequently occurring condition, lists are quietly not shared
            # with the majority of libraries causing confusion for our users.  As it stands
            # there is nothing that prevents lists with work-less entries that have already been
            # shared from being unshared.  So for the time being the least intrusive intervention
            # for enabling sharing to work again for many existing lists would be to relax the
            # validation when an entry does not have an associated work.
            if not entry.work:
                missing_work_id_count += 1
                continue

            valid_license = (
                _db.query(LicensePool)
                .filter(
                    LicensePool.work_id == entry.work_id,
                    LicensePool.collection_id.in_(library_collection_ids),
                )
                .first()
            )
            if valid_license is None:
                log.info(
                    f"Unable to share customlist: No license for work '{entry.work.title}'."
                )

                return CUSTOMLIST_ENTRY_NOT_VALID_FOR_LIBRARY

        if missing_work_id_count > 0:
            log.warning(
                f"This list contains {missing_work_id_count} {'entries' if missing_work_id_count > 1 else 'entry'} "
                f"without an associated work. "
            )
        customlist.shared_locally_with_libraries.append(library)
        log.info(
            f"Successfully shared customlist '{customlist.name}' with library '{library.name}'."
        )
        return True

    @classmethod
    @inject
    def populate_query_pages(
        cls,
        _db: Session,
        custom_list: CustomList,
        start_page: int = 1,
        max_pages: int = 100000,
        page_size: int = 100,
        json_query: dict | None = None,
        search: ExternalSearchIndex = Provide[Services.search.index],
    ) -> int:
        """Populate the custom list while paging through the search query results
        :param _db: The database connection
        :param custom_list: The list to be populated
        :param start_page: Offset of the search will be used from here (based on page_size)
        :param max_pages: Maximum number of pages to search through
        :param page_size: Page size to use for the search iteration
        :param json_query: If provided, use this json query rather than that of the custom list
        """

        if not custom_list.auto_update_query:
            cls.logger().info(
                f"Cannot populate entries: Custom list {custom_list.name} is missing an auto update query"
            )
            return 0

        if not json_query:
            json_query = json.loads(custom_list.auto_update_query)

        if custom_list.auto_update_facets:
            facet_data = json.loads(custom_list.auto_update_facets)
            facet_data.setdefault("order", SearchFacets.ORDER_TITLE)
            facets = SearchFacets(
                search_type="json",
                **facet_data,
            )
        else:
            facets = SearchFacets(search_type="json", order=SearchFacets.ORDER_TITLE)

        total_works_updated = 0
        start_page -= 1  # 0 based offset, so page 1 == 0
        pagination = SortKeyPagination(size=page_size)
        wl = WorkList()
        wl.initialize(custom_list.library)
        for page_num in range(0, start_page + max_pages):
            ## Query for the works with the search query
            works = wl.search(
                _db, json_query, search, pagination=pagination, facets=facets
            )

            ## No more works
            if not len(works):
                cls.logger().info(
                    f"{custom_list.name} customlist updated with {total_works_updated} works, moving on..."
                )
                break

            # Set the next page
            pagination = pagination.next_page
            # If we have not yet arrived at the page we need to start from, we should skip the update
            # and just page again
            if page_num < start_page:
                continue

            total_works_updated += len(works)

            ## Now update works into the list
            for work in works:
                custom_list.add_entry(work, update_external_index=True)

            cls.logger().info(
                f"Updated customlist {custom_list.name} with {total_works_updated} works"
            )

        # update this lists last updated time
        custom_list.auto_update_last_update = datetime.datetime.now()
        # update the list size
        custom_list.size = len(custom_list.entries)

        return total_works_updated
