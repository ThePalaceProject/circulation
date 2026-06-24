from __future__ import annotations

from palace.manager.api.lanes import create_default_lanes
from palace.manager.scripts.lane import DeleteInvisibleLanesScript
from palace.manager.sqlalchemy.model.lane import Lane
from tests.fixtures.database import DatabaseTransactionFixture


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
