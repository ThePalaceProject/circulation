import functools
from unittest.mock import patch

import pytest
from psycopg2.extras import NumericRange
from sqlalchemy import not_
from sqlalchemy.orm.exc import MultipleResultsFound

from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy import util
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.util import (
    get_one,
    numericrange_to_string,
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

    def test_get_one_or_create(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        data_source = DataSource.lookup(db.session, "Test", autocreate=True)
        patron = db.patron()
        collection = db.collection()

        get_one_or_create = functools.partial(
            util.get_one_or_create,
            db.session,
            Credential,
            data_source=data_source,
            type="Test token",
            patron=patron,
            collection=collection,
        )

        # If it doesn't exist... the function will create it
        credential, is_new = get_one_or_create()
        assert is_new
        assert isinstance(credential, Credential)

        # If it already exists, that gets returned instead
        credential_existing, is_new = get_one_or_create()
        assert not is_new
        assert isinstance(credential_existing, Credential)
        assert credential_existing == credential

        # If there is a race condition, and an IntegrityError happens
        # it will be handled, an error message logged, and the existing
        # object returned
        caplog.clear()
        caplog.set_level(LogLevel.debug)
        with patch.object(util, "get_one", return_value=None):
            credential_existing, is_new = get_one_or_create()
        assert not is_new
        assert isinstance(credential_existing, Credential)
        assert credential_existing == credential
        assert "INTEGRITY ERROR" in caplog.text


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
            assert self._lock_exists(db.session, self.TEST_LOCK_ID) is True
        assert self._lock_exists(db.session, self.TEST_LOCK_ID) is False

    def test_exception_case(self, db: DatabaseTransactionFixture):
        try:
            with pg_advisory_lock(db.session, self.TEST_LOCK_ID):
                assert self._lock_exists(db.session, self.TEST_LOCK_ID) is True
                raise Exception("Lock should open!!")
        except:
            assert self._lock_exists(db.session, self.TEST_LOCK_ID) is False

    def test_no_lock_id(self, db: DatabaseTransactionFixture):
        with pg_advisory_lock(db.session, None):
            assert self._lock_exists(db.session, self.TEST_LOCK_ID) is False


class TestNumericRangeToString:
    def test_numericrange_to_string_float(self):
        with pytest.raises(AssertionError):
            numericrange_to_string(NumericRange(1.1, 1.8, "[]"))
