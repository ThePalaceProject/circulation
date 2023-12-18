from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime

import flask
from flask import Response, url_for
from flask_babel import lazy_gettext as _
from flask_pydantic_spec.flask_backend import Context
from pydantic import BaseModel

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.problem_details import (
    ADMIN_NOT_AUTHORIZED,
    AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES,
    CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST,
    COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY,
    CUSTOM_LIST_NAME_ALREADY_IN_USE,
    CUSTOMLIST_CANNOT_DELETE_SHARE,
    MISSING_COLLECTION,
    MISSING_CUSTOM_LIST,
)
from api.controller.circulation_manager import CirculationManagerController
from api.problem_details import CANNOT_DELETE_SHARED_LIST
from core.app_server import load_pagination_from_request
from core.feed.acquisition import OPDSAcquisitionFeed
from core.lane import Lane, WorkList
from core.model import (
    Collection,
    CustomList,
    DataSource,
    Identifier,
    Library,
    LicensePool,
    Work,
    create,
    get_one,
)
from core.problem_details import INVALID_INPUT, METHOD_NOT_ALLOWED
from core.query.customlist import CustomListQueries
from core.util.problem_detail import ProblemDetail


class CustomListsController(
    CirculationManagerController, AdminPermissionsControllerMixin
):
    class CustomListSharePostResponse(BaseModel):
        successes: int = 0
        failures: int = 0

    class CustomListPostRequest(BaseModel):
        name: str
        id: int | None = None
        entries: list[dict] = []
        collections: list[int] = []
        deletedEntries: list[dict] = []
        # For auto updating lists
        auto_update: bool = False
        auto_update_query: dict | None = None
        auto_update_facets: dict | None = None

    def _list_as_json(self, list: CustomList, is_owner=True) -> dict:
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

    def custom_lists(self) -> dict | ProblemDetail | Response | None:
        library: Library = flask.request.library  # type: ignore  # "Request" has no attribute "library"
        self.require_librarian(library)

        if flask.request.method == "GET":
            custom_lists = []
            for list in library.custom_lists:
                custom_lists.append(self._list_as_json(list))

            for list in library.shared_custom_lists:
                custom_lists.append(self._list_as_json(list, is_owner=False))

            return dict(custom_lists=custom_lists)

        if flask.request.method == "POST":
            ctx: Context = flask.request.context.body  # type: ignore
            return self._create_or_update_list(
                library,
                ctx.name,
                ctx.entries,
                ctx.collections,
                id=ctx.id,
                auto_update=ctx.auto_update,
                auto_update_facets=ctx.auto_update_facets,
                auto_update_query=ctx.auto_update_query,
            )

        return None

    def _getJSONFromRequest(self, values: str | None) -> list:
        if values:
            return_values = json.loads(values)
        else:
            return_values = []

        return return_values

    def _get_work_from_urn(self, library: Library, urn: str | None) -> Work | None:
        identifier, ignore = Identifier.parse_urn(self._db, urn)

        if identifier is None:
            return None

        query = (
            self._db.query(Work)
            .join(LicensePool, LicensePool.work_id == Work.id)
            .join(Collection, LicensePool.collection_id == Collection.id)
            .filter(LicensePool.identifier_id == identifier.id)
            .filter(Collection.id.in_([c.id for c in library.all_collections]))
        )
        work = query.one()
        return work

    def _create_or_update_list(
        self,
        library: Library,
        name: str,
        entries: list[dict],
        collections: list[int],
        deleted_entries: list[dict] | None = None,
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
                except json.JSONDecodeError:
                    raise Exception(
                        INVALID_INPUT.detailed(
                            "auto_update_query is not JSON serializable"
                        )
                    )

                if entries and len(entries) > 0:
                    raise Exception(AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES)
                if deleted_entries and len(deleted_entries) > 0:
                    raise Exception(AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES)

            if auto_update_facets is not None:
                try:
                    auto_update_facets_str = json.dumps(auto_update_facets)
                except json.JSONDecodeError:
                    raise Exception(
                        INVALID_INPUT.detailed(
                            "auto_update_facets is not JSON serializable"
                        )
                    )
            if auto_update is True and auto_update_query is None:
                raise Exception(
                    INVALID_INPUT.detailed(
                        "auto_update_query must be present when auto_update is enabled"
                    )
                )
        except Exception as e:
            auto_update_error = e.args[0] if len(e.args) else None

            if not auto_update_error or type(auto_update_error) != ProblemDetail:
                raise

            # Rollback if this was a deliberate error
            self._db.rollback()
            return auto_update_error

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
            CustomListQueries.populate_query_pages(self._db, list, max_pages=1)
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
            documents = self.search_engine.create_search_documents_from_works(
                works_to_update_in_search
            )
            index = self.search_engine.start_updating_search_documents()
            index.add_documents(documents)
            index.finish()

            # If this list was used to populate any lanes, those lanes need to have their counts updated.
            for lane in Lane.affected_by_customlist(list):
                lane.update_size(self._db, self.search_engine)

        new_collections = []
        for collection_id in collections:
            collection = get_one(self._db, Collection, id=collection_id)
            if not collection:
                self._db.rollback()
                return MISSING_COLLECTION
            if list.library not in collection.libraries:
                self._db.rollback()
                return COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY
            new_collections.append(collection)
        list.collections = new_collections

        if is_new:
            return Response(str(list.id), 201)
        else:
            return Response(str(list.id), 200)

    def url_for_custom_list(
        self, library: Library, list: CustomList
    ) -> Callable[[int], str]:
        def url_fn(after):
            return url_for(
                "custom_list_get",
                after=after,
                library_short_name=library.short_name,
                list_id=list.id,
                _external=True,
            )

        return url_fn

    def custom_list(self, list_id: int) -> Response | dict | ProblemDetail | None:
        library: Library = flask.request.library  # type: ignore
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
            ctx: Context = flask.request.context.body  # type: ignore
            return self._create_or_update_list(
                library,
                ctx.name,
                ctx.entries,
                ctx.collections,
                deleted_entries=ctx.deletedEntries,
                id=list_id,
                auto_update=ctx.auto_update,
                auto_update_query=ctx.auto_update_query,
                auto_update_facets=ctx.auto_update_facets,
            )

        elif flask.request.method == "DELETE":
            # Deleting requires a library manager.
            self.require_library_manager(flask.request.library)  # type: ignore

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
                lane.update_size(self._db, self.search_engine)
            return Response(str(_("Deleted")), 200)

        return None

    def share_locally(
        self, customlist_id: int
    ) -> ProblemDetail | dict[str, int] | Response:
        """Share this customlist with all libraries on this local CM"""
        if not customlist_id:
            return INVALID_INPUT
        customlist = get_one(self._db, CustomList, id=customlist_id)
        if not customlist:
            return MISSING_CUSTOM_LIST
        if customlist.library != flask.request.library:  # type: ignore
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
        return self.CustomListSharePostResponse(
            successes=len(successes), failures=len(failures)
        ).dict()

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
