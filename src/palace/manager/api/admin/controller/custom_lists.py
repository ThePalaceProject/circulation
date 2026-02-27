from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from typing import Any, cast

import flask
from flask import Response, url_for
from flask_babel import lazy_gettext as _

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.model.custom_lists import (
    CustomListPostRequest,
    CustomListSharePostResponse,
)
from palace.manager.api.admin.problem_details import (
    ADMIN_NOT_AUTHORIZED,
    AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES,
    CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST,
    COLLECTION_NOT_ACTIVE_FOR_LIST_LIBRARY,
    CUSTOM_LIST_NAME_ALREADY_IN_USE,
    CUSTOMLIST_CANNOT_DELETE_SHARE,
    MISSING_COLLECTION,
    MISSING_CUSTOM_LIST,
)
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.problem_details import CANNOT_DELETE_SHARED_LIST
from palace.manager.api.util.flask import get_request_library
from palace.manager.core.app_server import load_pagination_from_request
from palace.manager.core.problem_details import INVALID_INPUT, METHOD_NOT_ALLOWED
from palace.manager.core.query.customlist import CustomListQueries
from palace.manager.feed.acquisition import OPDSAcquisitionFeed
from palace.manager.feed.worklist.base import WorkList
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import create, get_one
from palace.manager.util.flask_util import parse_multi_dict
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class CustomListsController(
    CirculationManagerController, AdminPermissionsControllerMixin
):
    def _list_as_json(self, list: CustomList, is_owner: bool = True) -> dict[str, Any]:
        """Transform a CustomList object into a response ready dict"""
        collections = []
        for collection in list.collections:
            collections.append(
                dict(
                    id=collection.id,
                    name=collection.name,
                    protocol=collection.protocol,
                )
            )
        return dict(
            id=list.id,
            name=list.name,
            collections=collections,
            entry_count=list.size,
            auto_update=list.auto_update_enabled,
            auto_update_query=list.auto_update_query,
            auto_update_facets=list.auto_update_facets,
            auto_update_status=list.auto_update_status,
            is_owner=is_owner,
            is_shared=len(list.shared_locally_with_libraries) > 0,
        )

    def custom_lists(self) -> dict[str, Any] | ProblemDetail | Response | None:
        library = get_request_library()
        self.require_librarian(library)

        if flask.request.method == "GET":
            custom_lists = []
            for list in library.custom_lists:
                custom_lists.append(self._list_as_json(list))

            for list in library.shared_custom_lists:
                custom_lists.append(self._list_as_json(list, is_owner=False))

            return dict(custom_lists=sorted(custom_lists, key=lambda x: x["name"]))

        if flask.request.method == "POST":
            list_ = CustomListPostRequest.model_validate(
                parse_multi_dict(flask.request.form)
            )
            return self._create_or_update_list(
                library,
                list_.name,
                list_.entries,
                list_.collections,
                id=list_.id,
                auto_update=list_.auto_update,
                auto_update_facets=list_.auto_update_facets,
                auto_update_query=list_.auto_update_query,
            )

        return None

    def _get_work_from_urn(self, library: Library, urn: str | None) -> Work | None:
        identifier, ignore = Identifier.parse_urn(self._db, urn)

        if identifier is None:
            return None

        query = (
            self._db.query(Work)
            .join(LicensePool, LicensePool.work_id == Work.id)
            .join(Collection, LicensePool.collection_id == Collection.id)
            .filter(LicensePool.identifier_id == identifier.id)
            .filter(Collection.id.in_([c.id for c in library.associated_collections]))
        )
        return cast(Work, query.one())

    def _create_or_update_list(
        self,
        library: Library,
        name: str,
        entries: list[dict[str, Any]],
        collections: list[int],
        deleted_entries: list[dict[str, Any]] | None = None,
        id: int | None = None,
        auto_update: bool | None = None,
        auto_update_query: dict[str, str] | None = None,
        auto_update_facets: dict[str, str] | None = None,
    ) -> ProblemDetail | Response:
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        old_list_with_name = CustomList.find(self._db, name, library=library)

        list: CustomList | None
        if id:
            is_new = False
            list = get_one(self._db, CustomList, id=int(id), data_source=data_source)
            if list is None:
                return MISSING_CUSTOM_LIST
            if list.library != library:
                return CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST
            if old_list_with_name and old_list_with_name != list:
                return CUSTOM_LIST_NAME_ALREADY_IN_USE
        elif old_list_with_name:
            return CUSTOM_LIST_NAME_ALREADY_IN_USE
        else:
            new_list, is_new = create(
                self._db, CustomList, name=name, data_source=data_source
            )
            new_list.created = datetime.now()
            new_list.library = library
            list = new_list

        # Test JSON viability of auto update data
        try:
            auto_update_query_str = None
            auto_update_facets_str = None
            if auto_update_query is not None:
                try:
                    auto_update_query_str = json.dumps(auto_update_query)
                except TypeError:
                    raise ProblemDetailException(
                        INVALID_INPUT.detailed(
                            "auto_update_query is not JSON serializable"
                        )
                    )

                if entries and len(entries) > 0:
                    raise ProblemDetailException(
                        AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES
                    )
                if deleted_entries and len(deleted_entries) > 0:
                    raise ProblemDetailException(
                        AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES
                    )

            if auto_update_facets is not None:
                try:
                    auto_update_facets_str = json.dumps(auto_update_facets)
                except TypeError:
                    raise ProblemDetailException(
                        INVALID_INPUT.detailed(
                            "auto_update_facets is not JSON serializable"
                        )
                    )
            if auto_update is True and auto_update_query is None:
                raise ProblemDetailException(
                    INVALID_INPUT.detailed(
                        "auto_update_query must be present when auto_update is enabled"
                    )
                )
        except ProblemDetailException as e:
            # Rollback if we have a problem detail to return
            self._db.rollback()
            return e.problem_detail

        list.updated = datetime.now()
        list.name = name
        previous_auto_update_query = list.auto_update_query
        # Record the time the auto_update was toggled "on"
        if auto_update is True and list.auto_update_enabled is False:
            list.auto_update_last_update = datetime.now()
        if auto_update is not None:
            list.auto_update_enabled = auto_update
        if auto_update_query is not None:
            list.auto_update_query = auto_update_query_str
        if auto_update_facets is not None:
            list.auto_update_facets = auto_update_facets_str

        # In case this is a new list with no entries, populate the first page
        if (
            is_new
            and list.auto_update_enabled
            and list.auto_update_status == CustomList.INIT
        ):
            if isinstance(self.search_engine, ProblemDetail):
                return self.search_engine
            CustomListQueries.populate_query_pages(
                self._db, self.search_engine, list, max_pages=1
            )
        elif (
            not is_new
            and list.auto_update_enabled
            and auto_update_query
            and previous_auto_update_query
        ):
            # In case this is a previous auto update list, we must check if the
            # query has been updated
            # JSON maps are unordered by definition, so we must deserialize and compare dicts
            try:
                prev_query_dict = json.loads(previous_auto_update_query)
                if prev_query_dict != auto_update_query:
                    list.auto_update_status = CustomList.REPOPULATE
            except json.JSONDecodeError:
                # Do nothing if the previous query was not valid
                pass

        membership_change = False

        works_to_update_in_search = set()

        for entry in entries:
            urn = entry.get("id")
            work = self._get_work_from_urn(library, urn)

            if work:
                entry, entry_is_new = list.add_entry(work, featured=True)
                if entry_is_new:
                    works_to_update_in_search.add(work)
                    membership_change = True

        if deleted_entries:
            for entry in deleted_entries:
                urn = entry.get("id")
                work = self._get_work_from_urn(library, urn)

                if work:
                    list.remove_entry(work)
                    works_to_update_in_search.add(work)
                    membership_change = True

        if membership_change:
            # We need to update the search index entries for works that caused a membership change,
            # so the upstream counts can be calculated correctly.
            documents = Work.to_search_documents(
                self._db,
                [w.id for w in works_to_update_in_search if w.id is not None],
            )
            # TODO: Does this need to be done here, or can this be done asynchronously?
            if isinstance(self.search_engine, ProblemDetail):
                return self.search_engine
            self.search_engine.add_documents(documents)
            self.search_engine.search_service().refresh()

            # If this list was used to populate any lanes, those lanes need to have their counts updated.
            for lane in Lane.affected_by_customlist(list):
                lane.update_size(self._db, search_engine=self.search_engine)

        new_collections = []
        for collection_id in collections:
            collection = get_one(self._db, Collection, id=collection_id)
            if not collection:
                self._db.rollback()
                return MISSING_COLLECTION
            if list.library not in collection.active_libraries:
                self._db.rollback()
                return COLLECTION_NOT_ACTIVE_FOR_LIST_LIBRARY
            new_collections.append(collection)
        list.collections = new_collections

        if is_new:
            return Response(str(list.id), 201)
        else:
            return Response(str(list.id), 200)

    def url_for_custom_list(
        self, library: Library, list: CustomList
    ) -> Callable[[int], str]:
        def url_fn(after: int) -> str:
            return url_for(
                "custom_list_get",
                after=after,
                library_short_name=library.short_name,
                list_id=list.id,
                _external=True,
            )

        return url_fn

    def custom_list(
        self, list_id: int | str
    ) -> Response | dict[str, Any] | ProblemDetail | None:
        try:
            list_id = int(list_id) if isinstance(list_id, str) else list_id
        except ValueError:
            return MISSING_CUSTOM_LIST
        library = get_request_library()
        self.require_librarian(library)
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        list = get_one(self._db, CustomList, id=list_id, data_source=data_source)
        if not list:
            return MISSING_CUSTOM_LIST

        if flask.request.method == "GET":
            pagination = load_pagination_from_request()
            if isinstance(pagination, ProblemDetail):
                return pagination

            query = CustomList.entries_having_works(self._db, list_id)
            url = url_for(
                "custom_list_get",
                list_name=list.name,
                library_short_name=library.short_name,
                list_id=list_id,
                _external=True,
            )

            worklist = WorkList()
            worklist.initialize(library, customlists=[list])

            annotator = self.manager.annotator(worklist)
            url_fn = self.url_for_custom_list(library, list)
            feed = OPDSAcquisitionFeed.from_query(
                query, self._db, list.name or "", url, pagination, url_fn, annotator
            )
            annotator.annotate_feed(feed)
            return feed.as_response(
                max_age=0, mime_types=flask.request.accept_mimetypes
            )

        elif flask.request.method == "POST":
            list_ = CustomListPostRequest.model_validate(
                parse_multi_dict(flask.request.form)
            )
            return self._create_or_update_list(
                library,
                list_.name,
                list_.entries,
                list_.collections,
                deleted_entries=list_.deletedEntries,
                id=list_id,
                auto_update=list_.auto_update,
                auto_update_query=list_.auto_update_query,
                auto_update_facets=list_.auto_update_facets,
            )

        elif flask.request.method == "DELETE":
            # Deleting requires a library manager.
            self.require_library_manager(get_request_library())

            if len(list.shared_locally_with_libraries) > 0:
                return CANNOT_DELETE_SHARED_LIST

            # Build the list of affected lanes before modifying the
            # CustomList.
            affected_lanes = Lane.affected_by_customlist(list)
            surviving_lanes = []
            for lane in affected_lanes:
                if lane.list_datasource == None and len(lane.customlist_ids) == 1:
                    # This Lane is based solely upon this custom list,
                    # which is about to be deleted. Delete the Lane
                    # itself.
                    self._db.delete(lane)
                else:
                    surviving_lanes.append(lane)
            for entry in list.entries:
                self._db.delete(entry)
            self._db.delete(list)
            self._db.flush()
            # Update the size for any lanes affected by this
            # CustomList which _weren't_ deleted.
            for lane in surviving_lanes:
                lane.update_size(self._db, search_engine=self.search_engine)
            return Response(str(_("Deleted")), 200)

        return None

    def share_locally(
        self, customlist_id: int | str
    ) -> ProblemDetail | dict[str, int] | Response:
        """Share this customlist with all libraries on this local CM"""
        try:
            customlist_id = (
                int(customlist_id) if isinstance(customlist_id, str) else customlist_id
            )
        except ValueError:
            return MISSING_CUSTOM_LIST
        if not customlist_id:
            return INVALID_INPUT
        customlist = get_one(self._db, CustomList, id=customlist_id)
        if not customlist:
            return MISSING_CUSTOM_LIST
        if customlist.library != get_request_library():
            return ADMIN_NOT_AUTHORIZED.detailed(
                _("This library does not have permissions on this customlist.")
            )

        if flask.request.method == "POST":
            return self.share_locally_POST(customlist)
        elif flask.request.method == "DELETE":
            return self.share_locally_DELETE(customlist)
        else:
            return METHOD_NOT_ALLOWED

    def share_locally_POST(
        self, customlist: CustomList
    ) -> ProblemDetail | dict[str, int]:
        successes = []
        failures = []
        self.log.info(f"Begin sharing customlist '{customlist.name}'")
        for library in self._db.query(Library).all():
            # Do not share with self
            if library == customlist.library:
                continue

            # Do not attempt to re-share
            if library in customlist.shared_locally_with_libraries:
                self.log.info(
                    f"Customlist '{customlist.name}' is already shared with library '{library.name}'"
                )
                continue

            # Attempt to share the list
            response = CustomListQueries.share_locally_with_library(
                self._db, customlist, library
            )

            if response is not True:
                failures.append(library)
            else:
                successes.append(library)

        self._db.commit()
        self.log.info(f"Done sharing customlist {customlist.name}")
        return CustomListSharePostResponse(
            successes=len(successes), failures=len(failures)
        ).model_dump()

    def share_locally_DELETE(self, customlist: CustomList) -> ProblemDetail | Response:
        """Delete the shared status of a custom list
        If a customlist is actively in use by another library, then disallow the unshare
        """
        if not customlist.shared_locally_with_libraries:
            return Response("", 204)

        shared_list_lanes = (
            self._db.query(Lane)
            .filter(
                Lane.customlists.contains(customlist),
                Lane.library_id != customlist.library_id,
            )
            .count()
        )

        if shared_list_lanes > 0:
            return CUSTOMLIST_CANNOT_DELETE_SHARE.detailed(
                _(
                    "This list cannot be unshared because it is currently being used by one or more libraries on this Palace Manager."
                )
            )

        # This list is not in use by any other libraries, we can delete the share
        # by simply emptying the list of shared libraries
        customlist.shared_locally_with_libraries = []

        return Response("", status=204)
