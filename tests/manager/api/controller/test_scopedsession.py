from collections.abc import Generator
from contextlib import contextmanager
from typing import Self
from unittest.mock import create_autospec

import pytest
from flask.ctx import RequestContext
from sqlalchemy.orm import Session

from palace.manager.api.app import app, initialize_database
from palace.manager.sqlalchemy.flask_sqlalchemy_session import current_session
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from tests.fixtures.database import DatabaseFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.circulation import MockCirculationManager


class ScopedSessionFixture:
    def __init__(
        self, db_fixture: DatabaseFixture, services: ServicesFixture, session: Session
    ):
        self.db_fixture = db_fixture
        self.session = session
        self.services = services
        self.app = app
        with db_fixture.patch_engine():
            initialize_database()
        self.app.manager = MockCirculationManager(app._db, services.services)
        self.mock_library = create_autospec(Library)
        self.mock_library.has_root_lanes = False

    def _cleanup(self) -> None:
        delattr(self.app, "manager")
        delattr(self.app, "_db")

    @classmethod
    @contextmanager
    def fixture(
        cls, db_fixture: DatabaseFixture, services_fixture: ServicesFixture
    ) -> Generator[Self, None, None]:
        with db_fixture.engine.connect() as connection, Session(connection) as session:
            fixture = cls(db_fixture, services_fixture, session)
            yield fixture
            fixture._cleanup()

    @contextmanager
    def request_context(self, path: str) -> Generator[RequestContext]:
        with self.app.test_request_context(path) as ctx:
            setattr(ctx.request, "library", self.mock_library)
            yield ctx


@pytest.fixture
def scoped_session_fixture(
    function_database: DatabaseFixture, services_fixture: ServicesFixture
) -> Generator[ScopedSessionFixture, None, None]:
    with ScopedSessionFixture.fixture(function_database, services_fixture) as fixture:
        yield fixture


class TestScopedSession:
    """Test that in production scenarios (as opposed to normal unit tests)
    the app server runs each incoming request in a separate database
    session.

    Compare to TestBaseController.test_unscoped_session, which tests
    the corresponding behavior in unit tests.
    """

    def test_scoped_session(
        self,
        scoped_session_fixture: ScopedSessionFixture,
    ):
        # Start a simulated request to the Flask app server.
        with scoped_session_fixture.request_context("/"):
            # Each request is given its own database session distinct
            # from the one used by most unit tests and the one created
            # outside of this context.
            session1 = current_session()
            assert session1 != scoped_session_fixture.session

            # Add an Identifier to the database.
            identifier = Identifier(type=DataSource.GUTENBERG, identifier="1024")
            session1.add(identifier)
            session1.flush()

            # The Identifier immediately shows up in the session that
            # created it.
            [identifier] = session1.query(Identifier).all()
            assert "1024" == identifier.identifier

            # It doesn't show up in a different session because
            # the request is still in progress so its transaction
            # hasn't been committed.
            assert [] == scoped_session_fixture.session.query(Identifier).all()

            # It shows up in the flask_scoped_session object that
            # created the request-scoped session, because within the
            # context of a request, running database queries on that object
            # actually runs them against your request-scoped session.
            [identifier] = app.manager._db.query(Identifier).all()
            assert "1024" == identifier.identifier

            # We use the session context manager here to make sure
            # we don't keep a transaction open for this new session
            # once we are done with it.
            with app.manager._db.session_factory() as new_session:
                # But if we were to use flask_scoped_session to create a
                # brand new session, it would not see the Identifier,
                # because it's running in a different database session.
                assert [] == new_session.query(Identifier).all()

            # When the index controller runs in the request context,
            # it doesn't store anything that's associated with the
            # scoped session.
            response = app.manager.index_controller()
            assert 302 == response.status_code

        # Once we exit the context of the Flask request, the
        # transaction is committed and the Identifier is written to the
        # database. That is why we run this test with the function_database
        # fixture, which gives us a function scoped database to work with.
        # This database is removed after the test completes, so we don't
        # have to worry about cleaning up the database after ourselves.
        [identifier] = scoped_session_fixture.session.query(Identifier).all()
        assert "1024" == identifier.identifier

        # Now create a different simulated Flask request
        with scoped_session_fixture.request_context("/"):
            session2 = current_session()
            assert session2 != scoped_session_fixture.session
            assert session2 != app.manager._db

            # The controller still works in the new request context -
            # nothing it needs is associated with the previous scoped
            # session.
            response = app.manager.index_controller()
            assert 302 == response.status_code

        # The two Flask requests got different sessions, neither of
        # which is the same as self._db, the unscoped database session
        # used by most other unit tests.
        assert session1 != session2
