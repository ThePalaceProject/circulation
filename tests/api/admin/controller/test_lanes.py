import json

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_EDIT_DEFAULT_LANE,
    CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT,
    LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS,
    MISSING_CUSTOM_LIST,
    MISSING_LANE,
    NO_CUSTOM_LISTS_FOR_LANE,
    NO_DISPLAY_NAME_FOR_LANE,
)
from core.lane import Lane
from core.model import AdminRole, CustomList, DataSource, get_one
from core.query.customlist import CustomListQueries
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture


class AdminLibraryManagerFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)
        self.admin.add_role(
            AdminRole.LIBRARY_MANAGER, controller_fixture.db.default_library()
        )


@pytest.fixture(scope="function")
def alm_fixture(controller_fixture: ControllerFixture) -> AdminLibraryManagerFixture:
    return AdminLibraryManagerFixture(controller_fixture)


class TestLanesController:
    def test_lanes_get(self, alm_fixture: AdminLibraryManagerFixture):
        library = alm_fixture.ctrl.db.library()
        collection = alm_fixture.ctrl.db.collection()
        library.collections += [collection]

        english = alm_fixture.ctrl.db.lane(
            "English", library=library, languages=["eng"]
        )
        english.priority = 0
        english.size = 44
        english_fiction = alm_fixture.ctrl.db.lane(
            "Fiction", library=library, parent=english, fiction=True
        )
        english_fiction.visible = False
        english_fiction.size = 33
        english_sf = alm_fixture.ctrl.db.lane(
            "Science Fiction", library=library, parent=english_fiction
        )
        english_sf.add_genre("Science Fiction")
        english_sf.inherit_parent_restrictions = True
        english_sf.size = 22
        spanish = alm_fixture.ctrl.db.lane(
            "Spanish", library=library, languages=["spa"]
        )
        spanish.priority = 1
        spanish.size = 11

        w1 = alm_fixture.ctrl.db.work(
            with_license_pool=True,
            language="eng",
            genre="Science Fiction",
            collection=collection,
        )
        w2 = alm_fixture.ctrl.db.work(
            with_license_pool=True, language="eng", fiction=False, collection=collection
        )

        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = library
        lane_for_list = alm_fixture.ctrl.db.lane("List Lane", library=library)
        lane_for_list.customlists += [list]
        lane_for_list.priority = 2
        lane_for_list.size = 1

        with alm_fixture.request_context_with_library_and_admin("/"):
            flask.request.library = library  # type: ignore[attr-defined]
            # The admin is not a librarian for this library.
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.lanes,
            )
            alm_fixture.admin.add_role(AdminRole.LIBRARIAN, library)
            response = alm_fixture.manager.admin_lanes_controller.lanes()

            assert 3 == len(response.get("lanes"))
            [english_info, spanish_info, list_info] = response.get("lanes")

            assert english.id == english_info.get("id")
            assert english.display_name == english_info.get("display_name")
            assert english.visible == english_info.get("visible")
            assert 44 == english_info.get("count")
            assert [] == english_info.get("custom_list_ids")
            assert True == english_info.get("inherit_parent_restrictions")

            [fiction_info] = english_info.get("sublanes")
            assert english_fiction.id == fiction_info.get("id")
            assert english_fiction.display_name == fiction_info.get("display_name")
            assert english_fiction.visible == fiction_info.get("visible")
            assert 33 == fiction_info.get("count")
            assert [] == fiction_info.get("custom_list_ids")
            assert True == fiction_info.get("inherit_parent_restrictions")

            [sf_info] = fiction_info.get("sublanes")
            assert english_sf.id == sf_info.get("id")
            assert english_sf.display_name == sf_info.get("display_name")
            assert english_sf.visible == sf_info.get("visible")
            assert 22 == sf_info.get("count")
            assert [] == sf_info.get("custom_list_ids")
            assert True == sf_info.get("inherit_parent_restrictions")

            assert spanish.id == spanish_info.get("id")
            assert spanish.display_name == spanish_info.get("display_name")
            assert spanish.visible == spanish_info.get("visible")
            assert 11 == spanish_info.get("count")
            assert [] == spanish_info.get("custom_list_ids")
            assert True == spanish_info.get("inherit_parent_restrictions")

            assert lane_for_list.id == list_info.get("id")
            assert lane_for_list.display_name == list_info.get("display_name")
            assert lane_for_list.visible == list_info.get("visible")
            assert 1 == list_info.get("count")
            assert [list.id] == list_info.get("custom_list_ids")
            assert True == list_info.get("inherit_parent_restrictions")

    def test_lanes_post_errors(self, alm_fixture: AdminLibraryManagerFixture):
        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict([])
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert NO_DISPLAY_NAME_FOR_LANE == response

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("display_name", "lane"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert NO_CUSTOM_LISTS_FOR_LANE == response

        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = alm_fixture.ctrl.db.default_library()

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", "12345"),
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert MISSING_LANE == response

        library = alm_fixture.ctrl.db.library()
        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.library = library  # type: ignore[attr-defined]
            flask.request.form = ImmutableMultiDict(
                [
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.lanes,
            )

        lane1 = alm_fixture.ctrl.db.lane("lane1")
        lane2 = alm_fixture.ctrl.db.lane("lane2")
        lane1.customlists += [list]

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", lane1.id),
                    ("display_name", "lane2"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS == response

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("display_name", "lane2"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS == response

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("parent_id", "12345"),
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert MISSING_LANE.uri == response.uri

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("parent_id", lane1.id),
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps(["12345"])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert MISSING_CUSTOM_LIST.uri == response.uri

    def test_lanes_create(self, alm_fixture: AdminLibraryManagerFixture):
        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = alm_fixture.ctrl.db.default_library()

        # The new lane's parent has a sublane already.
        parent = alm_fixture.ctrl.db.lane("parent")
        sibling = alm_fixture.ctrl.db.lane("sibling", parent=parent)

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("parent_id", parent.id),
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                    ("inherit_parent_restrictions", "false"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert 201 == response.status_code

            [lane] = alm_fixture.ctrl.db.session.query(Lane).filter(
                Lane.display_name == "lane"
            )
            assert lane.id == int(response.get_data(as_text=True))
            assert alm_fixture.ctrl.db.default_library() == lane.library
            assert "lane" == lane.display_name
            assert parent == lane.parent
            assert None == lane.media
            assert 1 == len(lane.customlists)
            assert list == lane.customlists[0]
            assert False == lane.inherit_parent_restrictions
            assert 0 == lane.priority

            # The sibling's priority has been shifted down to put the new lane at the top.
            assert 1 == sibling.priority

    def test_lanes_create_shared_list(self, alm_fixture: AdminLibraryManagerFixture):
        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = alm_fixture.ctrl.db.default_library()
        library = alm_fixture.ctrl.db.library()
        alm_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library=library)

        with alm_fixture.request_context_with_library_and_admin(
            "/", method="POST", library=library
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                    ("inherit_parent_restrictions", "false"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert 404 == response.status_code

        success = CustomListQueries.share_locally_with_library(
            alm_fixture.ctrl.db.session, list, library
        )
        assert success == True

        with alm_fixture.request_context_with_library_and_admin(
            "/", method="POST", library=library
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                    ("inherit_parent_restrictions", "false"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert 201 == response.status_code
            lane_id = int(response.data)

        lane = get_one(alm_fixture.ctrl.db.session, Lane, id=lane_id)
        assert isinstance(lane, Lane)
        assert lane.customlists == [list]
        assert lane.library == library

    def test_lanes_edit(self, alm_fixture: AdminLibraryManagerFixture):
        work = alm_fixture.ctrl.db.work(with_license_pool=True)

        list1, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list1.library = alm_fixture.ctrl.db.default_library()
        list2, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list2.library = alm_fixture.ctrl.db.default_library()
        list2.add_entry(work)

        lane = alm_fixture.ctrl.db.lane("old name")
        lane.customlists += [list1]

        # When we add a list to the lane, the controller will ask the
        # search engine to update lane.size, and it will think there
        # are two works in the lane.
        assert 0 == lane.size
        alm_fixture.ctrl.controller.search_engine.docs = dict(
            id1="value1", id2="value2"
        )

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(lane.id)),
                    ("display_name", "new name"),
                    ("custom_list_ids", json.dumps([list2.id])),
                    ("inherit_parent_restrictions", "true"),
                ]
            )

            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert 200 == response.status_code
            assert lane.id == int(response.get_data(as_text=True))

            assert "new name" == lane.display_name
            assert [list2] == lane.customlists
            assert True == lane.inherit_parent_restrictions
            assert None == lane.media

    def test_default_lane_edit(self, alm_fixture: AdminLibraryManagerFixture):
        """Default lanes only allow the display_name to be edited"""
        lane: Lane = alm_fixture.ctrl.db.lane("default")
        customlist, _ = alm_fixture.ctrl.db.customlist()
        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(lane.id)),
                    ("parent_id", "12345"),
                    ("display_name", "new name"),
                    ("custom_list_ids", json.dumps([customlist.id])),
                    ("inherit_parent_restrictions", "false"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()

        assert 200 == response.status_code
        assert lane.id == int(response.get_data(as_text=True))

        assert "new name" == lane.display_name
        # Nothing else changes
        assert [] == lane.customlists
        assert True == lane.inherit_parent_restrictions
        assert None == lane.parent_id

    def test_lane_delete_success(self, alm_fixture: AdminLibraryManagerFixture):
        library = alm_fixture.ctrl.db.library()
        alm_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
        lane = alm_fixture.ctrl.db.lane("lane", library=library)
        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = library
        lane.customlists += [list]
        assert (
            1
            == alm_fixture.ctrl.db.session.query(Lane)
            .filter(Lane.library == library)
            .count()
        )

        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library  # type: ignore[attr-defined]
            response = alm_fixture.manager.admin_lanes_controller.lane(lane.id)
            assert 200 == response.status_code

            # The lane has been deleted.
            assert (
                0
                == alm_fixture.ctrl.db.session.query(Lane)
                .filter(Lane.library == library)
                .count()
            )

            # The custom list still exists though.
            assert (
                1
                == alm_fixture.ctrl.db.session.query(CustomList)
                .filter(CustomList.library == library)
                .count()
            )

        lane = alm_fixture.ctrl.db.lane("lane", library=library)
        lane.customlists += [list]
        child = alm_fixture.ctrl.db.lane("child", parent=lane, library=library)
        child.customlists += [list]
        grandchild = alm_fixture.ctrl.db.lane(
            "grandchild", parent=child, library=library
        )
        grandchild.customlists += [list]
        assert (
            3
            == alm_fixture.ctrl.db.session.query(Lane)
            .filter(Lane.library == library)
            .count()
        )

        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library  # type: ignore[attr-defined]
            response = alm_fixture.manager.admin_lanes_controller.lane(lane.id)
            assert 200 == response.status_code

            # The lanes have all been deleted.
            assert (
                0
                == alm_fixture.ctrl.db.session.query(Lane)
                .filter(Lane.library == library)
                .count()
            )

            # The custom list still exists though.
            assert (
                1
                == alm_fixture.ctrl.db.session.query(CustomList)
                .filter(CustomList.library == library)
                .count()
            )

    def test_lane_delete_errors(self, alm_fixture: AdminLibraryManagerFixture):
        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            response = alm_fixture.manager.admin_lanes_controller.lane(123)
            assert MISSING_LANE == response

        lane = alm_fixture.ctrl.db.lane("lane")
        library = alm_fixture.ctrl.db.library()
        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library  # type: ignore[attr-defined]
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.lane,
                lane.id,
            )

        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            response = alm_fixture.manager.admin_lanes_controller.lane(lane.id)
            assert CANNOT_EDIT_DEFAULT_LANE == response

    def test_show_lane_success(self, alm_fixture: AdminLibraryManagerFixture):
        lane = alm_fixture.ctrl.db.lane("lane")
        lane.visible = False
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.show_lane(lane.id)
            assert 200 == response.status_code
            assert True == lane.visible

    def test_show_lane_errors(self, alm_fixture: AdminLibraryManagerFixture):
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.show_lane(123)
            assert MISSING_LANE == response

        parent = alm_fixture.ctrl.db.lane("parent")
        parent.visible = False
        child = alm_fixture.ctrl.db.lane("lane")
        child.visible = False
        child.parent = parent
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.show_lane(child.id)
            assert CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT == response

        alm_fixture.admin.remove_role(
            AdminRole.LIBRARY_MANAGER, alm_fixture.ctrl.db.default_library()
        )
        with alm_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.show_lane,
                parent.id,
            )

    def test_hide_lane_success(self, alm_fixture: AdminLibraryManagerFixture):
        lane = alm_fixture.ctrl.db.lane("lane")
        lane.visible = True
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.hide_lane(lane.id)
            assert 200 == response.status_code
            assert False == lane.visible

    def test_hide_lane_errors(self, alm_fixture: AdminLibraryManagerFixture):
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.hide_lane(123456789)
            assert MISSING_LANE == response

        lane = alm_fixture.ctrl.db.lane()
        alm_fixture.admin.remove_role(
            AdminRole.LIBRARY_MANAGER, alm_fixture.ctrl.db.default_library()
        )
        with alm_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.show_lane,
                lane.id,
            )

    def test_reset(self, alm_fixture: AdminLibraryManagerFixture):
        library = alm_fixture.ctrl.db.library()
        old_lane = alm_fixture.ctrl.db.lane("old lane", library=library)

        with alm_fixture.request_context_with_library_and_admin("/"):
            flask.request.library = library  # type: ignore[attr-defined]
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.reset,
            )

            alm_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
            response = alm_fixture.manager.admin_lanes_controller.reset()
            assert 200 == response.status_code

            # The old lane is gone.
            assert (
                0
                == alm_fixture.ctrl.db.session.query(Lane)
                .filter(Lane.library == library)
                .filter(Lane.id == old_lane.id)
                .count()
            )
            # tests/test_lanes.py tests the default lane creation, but make sure some
            # lanes were created.
            assert (
                0
                < alm_fixture.ctrl.db.session.query(Lane)
                .filter(Lane.library == library)
                .count()
            )

    def test_change_order(self, alm_fixture: AdminLibraryManagerFixture):
        library = alm_fixture.ctrl.db.library()
        parent1 = alm_fixture.ctrl.db.lane("parent1", library=library)
        parent2 = alm_fixture.ctrl.db.lane("parent2", library=library)
        child1 = alm_fixture.ctrl.db.lane("child1", parent=parent2)
        child2 = alm_fixture.ctrl.db.lane("child2", parent=parent2)
        parent1.priority = 0
        parent2.priority = 1
        child1.priority = 0
        child2.priority = 1

        new_order = [
            {"id": parent2.id, "sublanes": [{"id": child2.id}, {"id": child1.id}]},
            {"id": parent1.id},
        ]

        with alm_fixture.request_context_with_library_and_admin("/"):
            flask.request.library = library  # type: ignore[attr-defined]
            flask.request.data = json.dumps(new_order).encode()

            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.change_order,
            )

            alm_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
            response = alm_fixture.manager.admin_lanes_controller.change_order()
            assert 200 == response.status_code

            assert 0 == parent2.priority
            assert 1 == parent1.priority
            assert 0 == child2.priority
            assert 1 == child1.priority
