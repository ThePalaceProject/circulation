from contextlib import contextmanager

import flask
from flask_sqlalchemy_session import current_session

from core.model import (
    Collection,
    DataSource,
    ExternalIntegration,
    Identifier,
    Library,
    create,
)
from tests.fixtures.api_controller import (
    ControllerFixture,
    ControllerFixtureSetupOverrides,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestScopedSession:
    """Test that in production scenarios (as opposed to normal unit tests)
    the app server runs each incoming request in a separate database
    session.

    Compare to TestBaseController.test_unscoped_session, which tests
    the corresponding behavior in unit tests.
    """

    @staticmethod
    def make_default_libraries(_db: DatabaseTransactionFixture):
        assert _db is DatabaseTransactionFixture

        libraries = []
        for i in range(2):
            name = _db.fresh_str() + " (library for scoped session)"
            library, ignore = create(_db.session, Library, short_name=name)
            libraries.append(library)
        return libraries

    @staticmethod
    def make_default_collection(_db: DatabaseTransactionFixture, library):
        """We need to create a test collection that
        uses the scoped session.
        """
        assert _db is DatabaseTransactionFixture
        collection, ignore = create(
            _db.session,
            Collection,
            name=_db.fresh_str() + " (collection for scoped session)",
        )
        collection.create_external_integration(ExternalIntegration.OPDS_IMPORT)
        library.collections.append(collection)
        return collection

    @contextmanager
    def test_request_context_and_transaction(
        self, controller_fixture_without_cm: ControllerFixture, *args
    ):
        """Run a simulated Flask request in a transaction that gets rolled
        back at the end of the request.
        """
        fixture = controller_fixture_without_cm
        with fixture.app.test_request_context(*args) as ctx:
            transaction = current_session.begin_nested()
            fixture.app.manager = fixture.circulation_manager_setup(
                overrides=ControllerFixtureSetupOverrides(
                    make_default_libraries=self.make_default_libraries,
                    make_default_collection=self.make_default_collection,
                )
            )
            yield ctx
            transaction.rollback()

    def test_scoped_session(self, controller_fixture_without_cm: ControllerFixture):
        controller_fixture_without_cm.set_base_url()

        # Start a simulated request to the Flask app server.
        fixture = controller_fixture_without_cm
        with self.test_request_context_and_transaction(fixture, "/"):
            # Each request is given its own database session distinct
            # from the one used by most unit tests or the one
            # associated with the CirculationManager object.
            session1 = current_session()
            assert session1 != fixture.db
            assert session1 != fixture.app.manager._db

            # Add an Identifier to the database.
            identifier = Identifier(type=DataSource.GUTENBERG, identifier="1024")
            session1.add(identifier)
            session1.flush()

            # The Identifier immediately shows up in the session that
            # created it.
            [identifier] = session1.query(Identifier).all()
            assert "1024" == identifier.identifier

            # It doesn't show up in self._db, the database session
            # used by most other unit tests, because it was created
            # within the (still-active) context of a Flask request,
            # which happens within a nested database transaction.
            assert [] == fixture.db.session.query(Identifier).all()

            # It shows up in the flask_scoped_session object that
            # created the request-scoped session, because within the
            # context of a request, running database queries on that object
            # actually runs them against your request-scoped session.
            [identifier] = fixture.app.manager._db.query(Identifier).all()
            assert "1024" == identifier.identifier

            # But if we were to use flask_scoped_session to create a
            # brand new session, it would not see the Identifier,
            # because it's running in a different database session.
            new_session = fixture.app.manager._db.session_factory()
            assert [] == new_session.query(Identifier).all()

            # When the index controller runs in the request context,
            # it doesn't store anything that's associated with the
            # scoped session.
            flask.request.library = fixture.library  # type: ignore
            response = fixture.app.manager.index_controller()
            assert 302 == response.status_code

        # Once we exit the context of the Flask request, the
        # transaction is rolled back. The Identifier never actually
        # enters the database.
        #
        # If it did enter the database, it would never leave.  Changes
        # that happen through self._db happen inside a nested
        # transaction which is rolled back after the test is over.
        # But changes that happen through a session-scoped database
        # connection are actually written to the database when we
        # leave the scope of the request.
        #
        # To avoid this, we use test_request_context_and_transaction
        # to create a nested transaction that's rolled back just
        # before we leave the scope of the request.
        assert [] == fixture.db.session.query(Identifier).all()

        # Now create a different simulated Flask request
        with self.test_request_context_and_transaction(fixture, "/"):
            session2 = current_session()
            assert session2 != fixture.db
            assert session2 != fixture.app.manager._db

            # The controller still works in the new request context -
            # nothing it needs is associated with the previous scoped
            # session.
            flask.request.library = fixture.library  # type: ignore
            response = fixture.app.manager.index_controller()
            assert 302 == response.status_code

        # The two Flask requests got different sessions, neither of
        # which is the same as self._db, the unscoped database session
        # used by most other unit tests.
        assert session1 != session2
