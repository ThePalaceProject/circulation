from unittest.mock import MagicMock

from palace.manager.core.classifier import Classifier
from palace.manager.feed.worklist.base import WorkList
from palace.manager.feed.worklist.dynamic import WorkBasedLane
from tests.fixtures.database import DatabaseTransactionFixture


class TestWorkBasedLane:
    def test_initialization_sets_appropriate_audiences(
        self, db: DatabaseTransactionFixture
    ):
        work = db.work(with_license_pool=True)

        work.audience = Classifier.AUDIENCE_CHILDREN
        children_lane = WorkBasedLane(db.default_library(), work, "")
        assert [Classifier.AUDIENCE_CHILDREN] == children_lane.audiences

        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        ya_lane = WorkBasedLane(db.default_library(), work, "")
        assert sorted(Classifier.AUDIENCES_JUVENILE) == sorted(ya_lane.audiences)

        work.audience = Classifier.AUDIENCE_ADULT
        adult_lane = WorkBasedLane(db.default_library(), work, "")
        assert sorted(Classifier.AUDIENCES) == sorted(adult_lane.audiences)

        work.audience = Classifier.AUDIENCE_ADULTS_ONLY
        adults_only_lane = WorkBasedLane(db.default_library(), work, "")
        assert sorted(Classifier.AUDIENCES) == sorted(adults_only_lane.audiences)

    def test_append_child(self, db: DatabaseTransactionFixture):
        """When a WorkBasedLane gets a child, its language and audience
        restrictions are propagated to the child.
        """
        work = db.work(
            with_license_pool=True,
            audience=Classifier.AUDIENCE_CHILDREN,
            language="spa",
        )

        def make_child():
            # Set up a WorkList with settings that contradict the
            # settings of the work we'll be using as the basis for our
            # WorkBasedLane.
            child = WorkList()
            child.initialize(
                db.default_library(),
                "sublane",
                languages=["eng"],
                audiences=[Classifier.AUDIENCE_ADULT],
            )
            return child

        child1, child2 = (make_child() for i in range(2))

        # The WorkBasedLane's restrictions are propagated to children
        # passed in to the constructor.
        lane = WorkBasedLane(
            db.default_library(), work, "parent lane", children=[child1]
        )

        assert ["spa"] == child1.languages
        assert [Classifier.AUDIENCE_CHILDREN] == child1.audiences

        # It also happens when .append_child is called after the
        # constructor.
        lane.append_child(child2)
        assert ["spa"] == child2.languages
        assert [Classifier.AUDIENCE_CHILDREN] == child2.audiences

    def test_default_children_list_not_reused(self, db: DatabaseTransactionFixture):
        work = db.work()

        # By default, a WorkBasedLane has no children.
        lane1 = WorkBasedLane(db.default_library(), work)
        assert [] == lane1.children

        # Add a child...
        lane1.children.append(object)

        # Another lane for the same work gets a different, empty list
        # of children. It doesn't reuse the first lane's list.
        lane2 = WorkBasedLane(db.default_library(), work)
        assert [] == lane2.children

    def test_accessible_to(self, db: DatabaseTransactionFixture):
        # A lane based on a Work is accessible to a patron only if
        # the Work is age-appropriate for the patron.
        work = db.work()
        patron = db.patron()
        lane = WorkBasedLane(db.default_library(), work)

        work.age_appropriate_for_patron = MagicMock(return_value=False)
        assert False == lane.accessible_to(patron)
        work.age_appropriate_for_patron.assert_called_once_with(patron)

        # If for whatever reason Work is not set, we just we say the Lane is
        # accessible -- but things probably won't work.
        lane.work = None
        assert True == lane.accessible_to(patron)

        # age_appropriate_for_patron wasn't called, since there was no
        # work.
        work.age_appropriate_for_patron.assert_called_once_with(patron)

        lane.work = work
        work.age_appropriate_for_patron = MagicMock(return_value=True)
        lane = WorkBasedLane(db.default_library(), work)
        assert True == lane.accessible_to(patron)
        work.age_appropriate_for_patron.assert_called_once_with(patron)

        # The WorkList rules are still enforced -- for instance, a
        # patron from library B can't access any kind of WorkList from
        # library A.
        other_library_patron = db.patron(library=db.library())
        assert False == lane.accessible_to(other_library_patron)

        # age_appropriate_for_patron was never called with the new
        # patron -- the WorkList rules answered the question before we
        # got to that point.
        work.age_appropriate_for_patron.assert_called_once_with(patron)
