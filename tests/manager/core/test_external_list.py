from palace.manager.core.external_list import (
    ClassificationBasedMembershipManager,
    MembershipManager,
)
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.database import DatabaseTransactionFixture


class BooksInSeries(MembershipManager):
    """A sample implementation of MembershipManager that makes a CustomList
    out of all books that are in some series.
    """

    @property
    def new_membership(self):
        """Only books that are part of a series should be in this list."""
        return self._db.query(Edition).filter(Edition.series != None)


class TestMembershipManager:
    def test_update(self, db: DatabaseTransactionFixture):
        # Create two books that are part of series, and one book that
        # is not.
        series1 = db.edition()
        series1.series = "Series 1"

        series2 = db.edition()
        series2.series = "Series Two"

        no_series = db.edition()
        assert None == no_series.series

        update_time = datetime_utc(2015, 1, 1)

        # To create necessary mocked objects,
        # _customlist calls _work
        #    which calls _edition, which makes an edition and a pool (through _licensepool)
        #    then makes work through get_one_or_create
        custom_list, ignore = db.customlist()
        manager = BooksInSeries(custom_list)
        manager.update(update_time)

        [entry1] = [x for x in custom_list.entries if x.edition.series == "Series 1"]
        [entry2] = [x for x in custom_list.entries if x.edition.series == "Series Two"]

        assert update_time == entry1.first_appearance
        assert update_time == entry1.most_recent_appearance

        # In a shocking twist, one of the entries turns out not to
        # have a series, while the entry previously thought not to
        # have a series actually does.
        series2.series = None
        no_series.series = "Actually I do have a series."
        db.session.commit()

        new_update_time = datetime_utc(2016, 1, 1)

        manager.update(new_update_time)

        # Entry #2 has been removed from the list, and a new entry added.
        [old_entry] = [x for x in custom_list.entries if x.edition.series == "Series 1"]
        [new_entry] = [
            x
            for x in custom_list.entries
            if x.edition.series == "Actually I do have a series."
        ]
        assert update_time == old_entry.first_appearance
        assert new_update_time == old_entry.most_recent_appearance
        assert new_update_time == new_entry.first_appearance
        assert new_update_time == new_entry.most_recent_appearance

    def test_classification_based_membership_manager(
        self, db: DatabaseTransactionFixture
    ):
        e1 = db.edition()
        e2 = db.edition()
        e3 = db.edition()
        source = e1.data_source
        e1.primary_identifier.classify(source, Subject.TAG, "GOOD FOOD")
        e2.primary_identifier.classify(source, Subject.TAG, "barflies")
        e3.primary_identifier.classify(source, Subject.TAG, "irrelevant")

        custom_list, ignore = db.customlist()
        fragments = ["foo", "bar"]
        manager = ClassificationBasedMembershipManager(custom_list, fragments)
        members = list(manager.new_membership)
        assert 2 == len(members)

        # e1 is a member of the list because its primary identifier is
        # classified under a subject that matches %foo%.
        #
        # e2 is a member of the list because its primary identifier is
        # classified under a subject that matches %bar%.
        #
        # e3 is not a member of the list.
        assert e1 in members
        assert e2 in members
