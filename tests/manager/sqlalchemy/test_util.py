import functools
from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest
from psycopg2.extras import NumericRange
from sqlalchemy import not_, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError
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
from tests.fixtures.database import (
    DatabaseFixture,
    DatabaseTransactionFixture,
    IdFixture,
)


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
        with patch.object(util, "get_one", side_effect=[None, credential]):
            credential_existing, is_new = get_one_or_create()
        assert not is_new
        assert isinstance(credential_existing, Credential)
        assert credential_existing == credential
        assert "INTEGRITY ERROR" in caplog.text

        # If there is a race condition, and we cannot create or find an existing object,
        # we just raise the IntegrityError.
        caplog.clear()
        caplog.set_level(LogLevel.error)
        with (
            patch.object(util, "get_one", return_value=None),
            pytest.raises(IntegrityError),
        ):
            get_one_or_create()
        assert "Unable to retrieve with get_one after IntegrityError" in caplog.text

        # Test that all the methods get called with the correct parameters
        with (
            patch.object(util, "get_one", side_effect=[None, None]) as mock_get_one,
            patch.object(
                util, "create", side_effect=IntegrityError(None, None, None)
            ) as mock_create,
            pytest.raises(IntegrityError),
        ):
            obj_type = MagicMock()
            create_method = "create_method"
            create_method_kwargs = {"test": "value"}
            constraint = MagicMock()
            util.get_one_or_create(
                db.session,
                obj_type,
                "create_method",
                create_method_kwargs,
                id=123,
                on_multiple="interchangeable",
                constraint=constraint,
            )
        mock_create.assert_called_once_with(
            db.session,
            obj_type,
            create_method,
            create_method_kwargs,
            id=123,
        )
        assert mock_get_one.call_count == 2
        mock_get_one.assert_has_calls(
            [
                call(
                    db.session,
                    obj_type,
                    id=123,
                    on_multiple="interchangeable",
                    constraint=constraint,
                ),
                call(
                    db.session,
                    obj_type,
                    id=123,
                    on_multiple="interchangeable",
                    constraint=constraint,
                ),
            ]
        )


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
    def _lock_exists(self, connection: Connection, lock_id: int) -> bool:
        result = list(
            connection.execute(text(f"SELECT * from pg_locks where objid={lock_id}"))
        )
        return len(result) != 0

    @staticmethod
    @contextmanager
    def _connectable(engine: Engine, use_engine: bool):
        """Yield either the engine itself or a connection from it."""
        if use_engine:
            yield engine
        else:
            with engine.connect() as connection:
                yield connection

    @pytest.mark.parametrize(
        "use_engine",
        [
            pytest.param(True, id="engine"),
            pytest.param(False, id="connection"),
        ],
    )
    def test_lock_unlock(
        self,
        function_database: DatabaseFixture,
        function_test_id: IdFixture,
        use_engine: bool,
    ) -> None:
        lock_id = function_test_id.int_id
        engine = function_database.engine
        with self._connectable(engine, use_engine) as connectable:
            with pg_advisory_lock(connectable, lock_id):
                with engine.connect() as check_conn:
                    assert self._lock_exists(check_conn, lock_id) is True
        with engine.connect() as check_conn:
            assert self._lock_exists(check_conn, lock_id) is False

    @pytest.mark.parametrize(
        "use_engine",
        [
            pytest.param(True, id="engine"),
            pytest.param(False, id="connection"),
        ],
    )
    def test_exception_case(
        self,
        function_database: DatabaseFixture,
        function_test_id: IdFixture,
        use_engine: bool,
    ):
        lock_id = function_test_id.int_id
        engine = function_database.engine
        with (
            self._connectable(engine, use_engine) as connectable,
            pytest.raises(Exception, match="Lock should open"),
        ):
            with pg_advisory_lock(connectable, lock_id):
                with engine.connect() as check_conn:
                    assert self._lock_exists(check_conn, lock_id) is True
                raise Exception("Lock should open!!")

        # The lock should be released
        with engine.connect() as check_conn:
            assert self._lock_exists(check_conn, lock_id) is False

    @pytest.mark.parametrize(
        "use_engine",
        [
            pytest.param(True, id="engine"),
            pytest.param(False, id="connection"),
        ],
    )
    def test_integrity_error(
        self,
        function_database: DatabaseFixture,
        function_test_id: IdFixture,
        use_engine: bool,
    ) -> None:
        """The lock is released even when an IntegrityError occurs mid-transaction."""
        lock_id = function_test_id.int_id
        engine = function_database.engine
        with (
            self._connectable(engine, use_engine) as connectable,
            pytest.raises(IntegrityError),
        ):
            with pg_advisory_lock(connectable, lock_id) as conn:
                # Create a temp table with a unique constraint, then violate
                # it to trigger a real IntegrityError.
                conn.execute(text("CREATE TEMP TABLE _test_integrity (id INT UNIQUE)"))
                conn.execute(text("INSERT INTO _test_integrity VALUES (1)"))
                conn.execute(text("INSERT INTO _test_integrity VALUES (1)"))

        # The lock should be released despite the IntegrityError
        with engine.connect() as check_conn:
            assert self._lock_exists(check_conn, lock_id) is False


class TestNumericRangeToString:
    def test_numericrange_to_string_float(self):
        with pytest.raises(AssertionError):
            numericrange_to_string(NumericRange(1.1, 1.8, "[]"))

    def test_numericrange_to_string_no_upper_bound(self):
        assert numericrange_to_string(NumericRange(18, None)) == "18-"

    def test_numericrange_to_string_no_lower_bound(self):
        assert numericrange_to_string(NumericRange(None, 12)) == "12"

    def test_numericrange_to_string_range(self):
        assert numericrange_to_string(NumericRange(8, 12, "[]")) == "8-12"

    def test_numericrange_to_string_single_value(self):
        assert numericrange_to_string(NumericRange(5, 6, "[)")) == "5"

    def test_numericrange_to_string_none(self):
        assert numericrange_to_string(None) == ""

    def test_numericrange_to_string_empty(self):
        assert numericrange_to_string(NumericRange(None, None)) == ""
