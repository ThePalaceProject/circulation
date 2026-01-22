from __future__ import annotations

from unittest.mock import patch

from palace.manager.api.lanes import create_default_lanes
from palace.manager.feed.worklist.base import WorkList
from palace.manager.scripts.lane import (
    DeleteInvisibleLanesScript,
    LaneSweeperScript,
    UpdateLaneSizeScript,
)
from palace.manager.sqlalchemy.model.lane import Lane
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixtureFake


class TestLaneSweeperScript:
    def test_process_library(self, db: DatabaseTransactionFixture):
        class Mock(LaneSweeperScript):
            def __init__(self, _db):
                super().__init__(_db)
                self.considered = []
                self.processed = []

            def should_process_lane(self, lane):
                self.considered.append(lane)
                return lane.display_name == "process me"

            def process_lane(self, lane):
                self.processed.append(lane)

        good = db.lane(display_name="process me")
        bad = db.lane(display_name="don't process me")
        good_child = db.lane(display_name="process me", parent=bad)

        script = Mock(db.session)
        script.do_run(cmd_args=[])

        # The first item considered for processing was an ad hoc
        # WorkList representing the library's entire collection.
        worklist = script.considered.pop(0)
        assert db.default_library() == worklist.get_library(db.session)
        assert db.default_library().name == worklist.display_name
        assert {good, bad} == set(worklist.children)

        # After that, every lane was considered for processing, with
        # top-level lanes considered first.
        assert {good, bad, good_child} == set(script.considered)

        # But a lane was processed only if should_process_lane
        # returned True.
        assert {good, good_child} == set(script.processed)


class TestUpdateLaneSizeScript:
    def test_do_run(self, db, end_to_end_search_fixture: EndToEndSearchFixture):
        end_to_end_search_fixture.populate_search_index()

        lane = db.lane()
        lane.size = 100
        UpdateLaneSizeScript(
            db.session,
            search_index_client=end_to_end_search_fixture.external_search_index,
        ).do_run(cmd_args=[])
        assert 0 == lane.size

    def test_should_process_lane(
        self,
        db: DatabaseTransactionFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        """Only Lane objects can have their size updated."""
        lane = db.lane()
        script = UpdateLaneSizeScript(
            db.session, search_index_client=external_search_fake_fixture.external_search
        )
        assert True == script.should_process_lane(lane)

        worklist = WorkList()
        assert False == script.should_process_lane(worklist)

    def test_site_configuration_has_changed(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        end_to_end_search_fixture.populate_search_index()

        library = db.default_library()
        lane1 = db.lane()
        lane2 = db.lane()

        # Run the script to create all the default config settings.
        UpdateLaneSizeScript(
            db.session,
            search_index_client=end_to_end_search_fixture.external_search_index,
        ).do_run(cmd_args=[])

        # Set the lane sizes
        lane1.size = 100
        lane2.size = 50

        # Commit changes to the DB so the lane update listeners are fired
        db.session.flush()

        with (
            patch(
                "palace.manager.sqlalchemy.listeners.site_configuration_has_changed"
            ) as listeners_changed,
            patch(
                "palace.manager.scripts.lane.site_configuration_has_changed"
            ) as scripts_changed,
        ):
            UpdateLaneSizeScript(db.session).do_run(cmd_args=[])

        assert 0 == lane1.size
        assert 0 == lane2.size

        # The listeners in lane.py shouldn't call site_configuration_has_changed
        listeners_changed.assert_not_called()

        # The script should call site_configuration_has_changed once when it is done
        scripts_changed.assert_called_once()


class TestDeleteInvisibleLanesScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        """Test that invisible lanes and their visible children are deleted."""
        # create a library
        short_name = "TESTLIB"
        l1 = db.library("test library", short_name=short_name)
        # with a set of default lanes
        create_default_lanes(db.session, l1)

        # verify there is a top level visible Fiction lane
        top_level_fiction_lane: Lane = (
            db.session.query(Lane)
            .filter(Lane.library == l1)
            .filter(Lane.parent == None)
            .filter(Lane.display_name == "Fiction")
            .order_by(Lane.priority)
            .one()
        )

        first_child_id = top_level_fiction_lane.children[0].id

        assert top_level_fiction_lane is not None
        assert top_level_fiction_lane.visible == True
        assert first_child_id is not None

        # run script and verify that it had no effect:
        DeleteInvisibleLanesScript(_db=db.session).do_run([short_name])
        top_level_fiction_lane = (
            db.session.query(Lane)
            .filter(Lane.library == l1)
            .filter(Lane.parent == None)
            .filter(Lane.display_name == "Fiction")
            .order_by(Lane.priority)
            .one()
        )
        assert top_level_fiction_lane is not None

        # flag as deleted
        top_level_fiction_lane.visible = False

        # and now run script.
        DeleteInvisibleLanesScript(_db=db.session).do_run([short_name])

        # verify the lane has now been deleted.
        deleted_lane = (
            db.session.query(Lane)
            .filter(Lane.library == l1)
            .filter(Lane.parent == None)
            .filter(Lane.display_name == "Fiction")
            .order_by(Lane.priority)
            .all()
        )

        assert deleted_lane == []

        # verify the first child was also deleted:

        first_child_lane = (
            db.session.query(Lane).filter(Lane.id == first_child_id).all()
        )

        assert first_child_lane == []
