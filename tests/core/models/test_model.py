import pytest
from psycopg2.extras import NumericRange
from sqlalchemy import not_
from sqlalchemy.orm.exc import MultipleResultsFound

from core.config import Configuration
from core.model import (
    Edition,
    SessionManager,
    Timestamp,
    get_one,
    numericrange_to_tuple,
    pg_advisory_lock,
    tuple_to_numericrange,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestDatabaseInterface:
    def test_get_one(self, db: DatabaseTransactionFixture):
        # When a matching object isn't found, None is returned.
        result = get_one(db.session, Edition)
        assert None == result

        # When a single item is found, it is returned.
        edition = db.edition()
        result = get_one(db.session, Edition)
        assert edition == result

        # When multiple items are found, an error is raised.
        other_edition = db.edition()
        pytest.raises(MultipleResultsFound, get_one, db.session, Edition)

        # Unless they're interchangeable.
        result = get_one(db.session, Edition, on_multiple="interchangeable")
        assert result in db.session.query(Edition)

        # Or specific attributes are passed that limit the results to one.
        result = get_one(
            db.session, Edition, title=other_edition.title, author=other_edition.author
        )
        assert other_edition == result

        # A particular constraint clause can also be passed in.
        titles = [ed.title for ed in (edition, other_edition)]
        constraint = not_(Edition.title.in_(titles))
        result = get_one(db.session, Edition, constraint=constraint)
        assert None == result

    def test_initialize_data_does_not_reset_timestamp(
        self, db: DatabaseTransactionFixture
    ):
        # initialize_data() has already been called, so the database is
        # initialized and the 'site configuration changed' Timestamp has
        # been set. Calling initialize_data() again won't change the
        # date on the timestamp.
        timestamp = get_one(
            db.session,
            Timestamp,
            collection=None,
            service=Configuration.SITE_CONFIGURATION_CHANGED,
        )
        old_timestamp = timestamp.finish
        SessionManager.initialize_data(db.session)
        assert old_timestamp == timestamp.finish


class TestNumericRangeConversion:
    """Test the helper functions that convert between tuples and NumericRange
    objects.
    """

    def test_tuple_to_numericrange(self):
        f = tuple_to_numericrange
        assert None == f(None)

        one_to_ten = f((1, 10))
        assert isinstance(one_to_ten, NumericRange)
        assert 1 == one_to_ten.lower
        assert 10 == one_to_ten.upper
        assert True == one_to_ten.upper_inc

        up_to_ten = f((None, 10))
        assert isinstance(up_to_ten, NumericRange)
        assert None == up_to_ten.lower
        assert 10 == up_to_ten.upper
        assert True == up_to_ten.upper_inc

        ten_and_up = f((10, None))
        assert isinstance(ten_and_up, NumericRange)
        assert 10 == ten_and_up.lower
        assert None == ten_and_up.upper
        assert False == ten_and_up.upper_inc

    def test_numericrange_to_tuple(self):
        m = numericrange_to_tuple
        two_to_six_inclusive = NumericRange(2, 6, "[]")
        assert (2, 6) == m(two_to_six_inclusive)
        two_to_six_exclusive = NumericRange(2, 6, "()")
        assert (3, 5) == m(two_to_six_exclusive)


class TestAdvisoryLock:
    TEST_LOCK_ID = 999999

    def _lock_exists(self, session, lock_id):
        result = list(session.execute(f"SELECT * from pg_locks where objid={lock_id}"))
        return len(result) != 0

    def test_lock_unlock(self, db: DatabaseTransactionFixture):
        with pg_advisory_lock(db.session, self.TEST_LOCK_ID):
            assert self._lock_exists(db.session, self.TEST_LOCK_ID) == True
        assert self._lock_exists(db.session, self.TEST_LOCK_ID) == False

    def test_exception_case(self, db: DatabaseTransactionFixture):
        try:
            with pg_advisory_lock(db.session, self.TEST_LOCK_ID):
                assert self._lock_exists(db.session, self.TEST_LOCK_ID) == True
                raise Exception("Lock should open!!")
        except:
            assert self._lock_exists(db.session, self.TEST_LOCK_ID) == False

    def test_no_lock_id(self, db: DatabaseTransactionFixture):
        with pg_advisory_lock(db.session, None):
            assert self._lock_exists(db.session, self.TEST_LOCK_ID) == False
