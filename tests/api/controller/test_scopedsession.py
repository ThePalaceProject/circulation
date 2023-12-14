from contextlib import contextmanager

import flask
from sqlalchemy.orm import Session

import api
from core.flask_sqlalchemy_session import current_session
from core.model import (
    Collection,
    DataSource,
    ExternalIntegration,
    Identifier,
    Library,
    create,
)
from core.opds_import import OPDSAPI
from tests.fixtures.api_controller import (
    ControllerFixture,
    ControllerFixtureSetupOverrides,
)


class ScopedHolder:
    """A scoped holder used to store some state in the test. This is necessary because
    we want to do some unusual things with scoped sessions, and don't necessary have access
    to a database transaction fixture in all of the various methods that will be called.
    """

    def __init__(self):
        self.identifiers = 0

    def fresh_id(self) -> str:
        self.identifiers = self.identifiers + 1
        return str(self.identifiers)

    def make_default_libraries(self, session: Session):
        libraries = []
        for i in range(2):
            name = self.fresh_id() + " (library for scoped session)"
            library, ignore = create(
                session,
                Library,
                short_name=name,
                public_key="x",
                private_key=b"y",
                settings_dict={
                    "website": "https://library.com",
                    "help_web": "https://library.com/help",
                },
            )
            libraries.append(library)
        return libraries

    def make_default_collection(self, session: Session, library):
        """We need to create a test collection that
        uses the scoped session.
        """
        collection, _ = Collection.by_name_and_protocol(
            session,
            self.fresh_id() + " (collection for scoped session)",
            ExternalIntegration.OPDS_IMPORT,
        )
        settings = OPDSAPI.settings_class()(
            external_account_id="http://url.com", data_source="OPDS"
        )
        OPDSAPI.settings_update(collection.integration_configuration, settings)
        library.collections.append(collection)
        return collection


class TestScopedSession:
    """Test that in production scenarios (as opposed to normal unit tests)
    the app server runs each incoming request in a separate database
    session.

    Compare to TestBaseController.test_unscoped_session, which tests
    the corresponding behavior in unit tests.
    """

    @contextmanager
    def request_context_and_transaction(
        self,
        scoped: ScopedHolder,
        controller_fixture_without_cm: ControllerFixture,
        *args
    ):
        """Run a simulated Flask request in a transaction that gets rolled
        back at the end of the request.
        """

        fixture = controller_fixture_without_cm
        with fixture.app.test_request_context(*args) as ctx:
            transaction = current_session.begin_nested()  # type: ignore[attr-defined]
            fixture.app.manager = fixture.circulation_manager_setup_with_session(
                session=current_session,  # type: ignore[arg-type]
                overrides=ControllerFixtureSetupOverrides(
                    make_default_libraries=scoped.make_default_libraries,
                    make_default_collection=scoped.make_default_collection,
                ),
            )
            yield ctx
            transaction.rollback()

    def test_scoped_session(self, controller_fixture_without_cm: ControllerFixture):
        fixture = controller_fixture_without_cm
        fixture.set_base_url()
        api.app.initialize_database()

        # Create a holder that carries some state for the purposes of testing
        scoped = ScopedHolder()

        # Start a simulated request to the Flask app server.
        with self.request_context_and_transaction(scoped, fixture, "/"):
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

            # It doesn't show up in fixture.db.session, the database session
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

            # We use the session context manager here to make sure
            # we don't keep a transaction open for this new session
            # once we are done with it.
            with fixture.app.manager._db.session_factory() as new_session:
                # But if we were to use flask_scoped_session to create a
                # brand new session, it would not see the Identifier,
                # because it's running in a different database session.
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
        with self.request_context_and_transaction(scoped, fixture, "/"):
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

        # Make sure that we close the connections for the scoped sessions.
        session1.bind.dispose()
        session2.bind.dispose()
