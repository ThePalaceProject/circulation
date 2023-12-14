from __future__ import annotations

import json

import flask
from flask import Response
from flask_babel import lazy_gettext as _

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.problem_details import (
    CANNOT_EDIT_DEFAULT_LANE,
    CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT,
    LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS,
    MISSING_CUSTOM_LIST,
    MISSING_LANE,
    NO_CUSTOM_LISTS_FOR_LANE,
    NO_DISPLAY_NAME_FOR_LANE,
)
from api.controller.circulation_manager import CirculationManagerController
from api.lanes import create_default_lanes
from core.lane import Lane
from core.model import CustomList, Library, create, get_one


class LanesController(CirculationManagerController, AdminPermissionsControllerMixin):
    def lanes(self):
        library = flask.request.library
        self.require_librarian(library)

        if flask.request.method == "GET":

            def lanes_for_parent(parent):
                lanes = (
                    self._db.query(Lane)
                    .filter(Lane.library == library)
                    .filter(Lane.parent == parent)
                    .order_by(Lane.priority)
                )
                return [
                    {
                        "id": lane.id,
                        "display_name": lane.display_name,
                        "visible": lane.visible,
                        "count": lane.size,
                        "sublanes": lanes_for_parent(lane),
                        "custom_list_ids": [list.id for list in lane.customlists],
                        "inherit_parent_restrictions": lane.inherit_parent_restrictions,
                    }
                    for lane in lanes
                ]

            return dict(lanes=lanes_for_parent(None))

        if flask.request.method == "POST":
            self.require_library_manager(flask.request.library)

            id = flask.request.form.get("id")
            parent_id = flask.request.form.get("parent_id")
            display_name = flask.request.form.get("display_name")
            custom_list_ids = json.loads(
                flask.request.form.get("custom_list_ids", "[]")
            )
            inherit_parent_restrictions = flask.request.form.get(
                "inherit_parent_restrictions"
            )
            if inherit_parent_restrictions == "true":
                inherit_parent_restrictions = True
            else:
                inherit_parent_restrictions = False

            if not display_name:
                return NO_DISPLAY_NAME_FOR_LANE

            if id:
                is_new = False
                lane = get_one(self._db, Lane, id=id, library=library)
                if not lane:
                    return MISSING_LANE

                if not lane.customlists:
                    # just update what is allowed for default lane, and exit out
                    lane.display_name = display_name
                    return Response(str(lane.id), 200)
                else:
                    # In case we are not a default lane, the lane MUST have custom lists
                    if not custom_list_ids or len(custom_list_ids) == 0:
                        return NO_CUSTOM_LISTS_FOR_LANE

                if display_name != lane.display_name:
                    old_lane = get_one(
                        self._db, Lane, display_name=display_name, parent=lane.parent
                    )
                    if old_lane:
                        return LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS
                lane.display_name = display_name
            else:
                if not custom_list_ids or len(custom_list_ids) == 0:
                    return NO_CUSTOM_LISTS_FOR_LANE

                parent = None
                if parent_id:
                    parent = get_one(self._db, Lane, id=parent_id, library=library)
                    if not parent:
                        return MISSING_LANE.detailed(
                            _(
                                "The specified parent lane does not exist, or is associated with a different library."
                            )
                        )
                old_lane = get_one(
                    self._db,
                    Lane,
                    display_name=display_name,
                    parent=parent,
                    library=library,
                )
                if old_lane:
                    return LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS

                lane, is_new = create(
                    self._db,
                    Lane,
                    display_name=display_name,
                    parent=parent,
                    library=library,
                )

                # Make a new lane the first child of its parent and bump all the siblings down in priority.
                siblings = (
                    self._db.query(Lane)
                    .filter(Lane.library == library)
                    .filter(Lane.parent == lane.parent)
                    .filter(Lane.id != lane.id)
                )
                for sibling in siblings:
                    sibling.priority += 1
                lane.priority = 0

            lane.inherit_parent_restrictions = inherit_parent_restrictions

            for list_id in custom_list_ids:
                list = get_one(self._db, CustomList, library=library, id=list_id)
                if not list:
                    # We did not find a list, is this a shared list?
                    list = (
                        self._db.query(CustomList)
                        .join(CustomList.shared_locally_with_libraries)
                        .filter(CustomList.id == list_id, Library.id == library.id)
                        .first()
                    )
                if not list:
                    self._db.rollback()
                    return MISSING_CUSTOM_LIST.detailed(
                        _(
                            "The list with id %(list_id)s does not exist or is associated with a different library.",
                            list_id=list_id,
                        )
                    )
                lane.customlists.append(list)

            for list in lane.customlists:
                if list.id not in custom_list_ids:
                    lane.customlists.remove(list)
            lane.update_size(self._db, self.search_engine)

            if is_new:
                return Response(str(lane.id), 201)
            else:
                return Response(str(lane.id), 200)

    def lane(self, lane_identifier):
        if flask.request.method == "DELETE":
            library = flask.request.library
            self.require_library_manager(library)

            lane = get_one(self._db, Lane, id=lane_identifier, library=library)
            if not lane:
                return MISSING_LANE
            if not lane.customlists:
                return CANNOT_EDIT_DEFAULT_LANE

            # Recursively delete all the lane's sublanes.
            def delete_lane_and_sublanes(lane):
                for sublane in lane.sublanes:
                    delete_lane_and_sublanes(sublane)
                self._db.delete(lane)

            delete_lane_and_sublanes(lane)
            return Response(str(_("Deleted")), 200)

    def show_lane(self, lane_identifier):
        library = flask.request.library
        self.require_library_manager(library)

        lane = get_one(self._db, Lane, id=lane_identifier, library=library)
        if not lane:
            return MISSING_LANE
        if lane.parent and not lane.parent.visible:
            return CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT
        lane.visible = True
        return Response(str(_("Success")), 200)

    def hide_lane(self, lane_identifier):
        library = flask.request.library
        self.require_library_manager(library)

        lane = get_one(self._db, Lane, id=lane_identifier, library=library)
        if not lane:
            return MISSING_LANE
        lane.visible = False
        return Response(str(_("Success")), 200)

    def reset(self):
        self.require_library_manager(flask.request.library)

        create_default_lanes(self._db, flask.request.library)
        return Response(str(_("Success")), 200)

    def change_order(self):
        self.require_library_manager(flask.request.library)

        submitted_lanes = json.loads(flask.request.data)

        def update_lane_order(lanes):
            for index, lane_data in enumerate(lanes):
                lane_id = lane_data.get("id")
                lane = self._db.query(Lane).filter(Lane.id == lane_id).one()
                lane.priority = index
                update_lane_order(lane_data.get("sublanes", []))

        update_lane_order(submitted_lanes)

        return Response(str(_("Success")), 200)
