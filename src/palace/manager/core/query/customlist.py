from __future__ import annotations

import datetime
import json
from typing import TYPE_CHECKING, Any

from palace.util.log import LoggerMixin

from palace.manager.api.admin.problem_details import (
    CUSTOMLIST_ENTRY_NOT_VALID_FOR_LIBRARY,
    CUSTOMLIST_SOURCE_COLLECTION_MISSING,
)
from palace.manager.feed.facets.search import SearchFacets
from palace.manager.feed.worklist.base import WorkList
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.search.pagination import SortKeyPagination
from palace.manager.sqlalchemy.model.customlist import CustomList, CustomListEntry
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class CustomListQueries(LoggerMixin):
    @classmethod
    def share_locally_with_library(
        cls, _db: Session, customlist: CustomList, library: Library
    ) -> ProblemDetail | bool:
        # All customlist collections must be present in the library
        log = cls.logger()
        log.info(
            f"Attempting to share customlist '{customlist.name}' with library '{library.name}'."
        )
        for collection in customlist.collections:
            if collection not in library.active_collections:
                log.info(
                    f"Unable to share customlist: Collection '{collection.name}'"
                    " is missing from or inactive for the library."
                )
                return CUSTOMLIST_SOURCE_COLLECTION_MISSING

        # All entries must be valid for the library
        library_collection_ids = [c.id for c in library.active_collections]
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
    def populate_query_pages(
        cls,
        _db: Session,
        search: ExternalSearchIndex,
        custom_list: CustomList,
        start_page: int = 1,
        max_pages: int = 100000,
        page_size: int = 100,
        json_query: dict[str, Any] | None = None,
        pagination_key: list[Any] | None = None,
        update_metadata: bool = True,
    ) -> tuple[int, list[Any] | None]:
        """Populate the custom list while paging through the search query results.

        :param _db: The database connection
        :param search: The search index to use
        :param custom_list: The list to be populated
        :param start_page: Offset of the search will be used from here (based on page_size).
            Ignored when ``pagination_key`` is provided.
        :param max_pages: Maximum number of pages to process in this call. When reached without
            exhausting results, the returned ``next_pagination_key`` is non-None. Note that a
            value of 100000 is effectively unlimited for any real-world list; lists anywhere near
            that size (10M+ entries) are not expected in practice.
        :param page_size: Page size to use for the search iteration
        :param json_query: If provided, use this json query rather than that of the custom list
        :param pagination_key: Cursor returned by a previous call; resumes from this position and
            ignores ``start_page``. Use this for paginating across multiple calls (e.g. Celery
            ``task.replace()`` continuations).
        :param update_metadata: When True (the default), update ``auto_update_last_update`` and
            ``size`` after processing. Pass False when paginating across multiple calls so that
            metadata is only written once, on the final invocation.
        :return: A ``(total_works_updated, next_pagination_key)`` tuple. ``next_pagination_key``
            is non-None only when ``max_pages`` was reached before results were exhausted, and
            should be passed as ``pagination_key`` on the next call to continue pagination.
        """

        if not custom_list.auto_update_query:
            cls.logger().info(
                f"Cannot populate entries: Custom list {custom_list.name} is missing an auto update query"
            )
            return 0, None

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
        wl = WorkList()
        wl.initialize(custom_list.library)

        if pagination_key is not None:
            # Resume from a cursor returned by a previous call.
            pagination = SortKeyPagination(
                last_item_on_previous_page=pagination_key, size=page_size
            )
        else:
            # Start from the beginning, optionally fast-forwarding past pages already processed.
            # The fast-forward mechanism is kept for backward compatibility with callers that
            # use start_page (e.g. the existing script-based INIT mode that skips page 1).
            pagination = SortKeyPagination(size=page_size)
            fast_forward = max(0, start_page - 1)
            for _ in range(fast_forward):
                works = wl.search(
                    _db, json_query, search, pagination=pagination, facets=facets
                )
                if not works:
                    if update_metadata:
                        custom_list.auto_update_last_update = datetime.datetime.now()
                        custom_list.size = custom_list.get_entry_count(_db)
                    return 0, None
                next_page = pagination.next_page
                if next_page is None:
                    if update_metadata:
                        custom_list.auto_update_last_update = datetime.datetime.now()
                        custom_list.size = custom_list.get_entry_count(_db)
                    return 0, None
                pagination = next_page

        next_pagination_key: list[Any] | None = None
        for _ in range(max_pages):
            works = wl.search(
                _db, json_query, search, pagination=pagination, facets=facets
            )

            if not works:
                cls.logger().info(
                    f"{custom_list.name} customlist updated with {total_works_updated} works, moving on..."
                )
                break

            total_works_updated += len(works)
            for work in works:
                custom_list.add_entry(work, update_external_index=True)

            cls.logger().info(
                f"Updated customlist {custom_list.name} with {total_works_updated} works"
            )

            next_page = pagination.next_page
            if next_page is None:
                break
            pagination = next_page
        else:
            # The for loop ran to completion without a break, meaning max_pages pages were
            # processed without exhausting results. Return a cursor so the caller can continue.
            # Note: the cursor points to immediately after the last processed page so the next
            # call can resume without re-fetching any already-processed results.
            next_pagination_key = pagination.last_item_on_previous_page

        if update_metadata:
            custom_list.auto_update_last_update = datetime.datetime.now()
            custom_list.size = custom_list.get_entry_count(_db)

        return total_works_updated, next_pagination_key
