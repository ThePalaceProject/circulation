from __future__ import annotations

import logging

from sqlalchemy import create_engine, event, literal_column, select, table, text
from sqlalchemy.engine import Connection, Engine, make_url
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import Pool

from palace.manager.core import classifier
from palace.manager.core.config import Configuration
from palace.manager.sqlalchemy.before_flush_decorator import Listener
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.key import Key
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.json import json_serializer
from palace.manager.util.log import LoggerMixin
from palace.manager.util.resources import resources_dir

DEBUG = False


class SessionManager(LoggerMixin):
    # A function that calculates recursively equivalent identifiers
    # is also defined in SQL.
    RECURSIVE_EQUIVALENTS_FUNCTION = "recursive_equivalents.sql"

    @classmethod
    def engine(
        cls,
        url: str | None = None,
        poolclass: type[Pool] | None = None,
        application_name: str | None = None,
    ) -> Engine:
        url = url or Configuration.database_url()
        url_obj = make_url(url)
        if application_name is not None:
            if "application_name" in url_obj.query.keys():
                cls.logger().warning(
                    "Overwriting existing application_name in database URL "
                    f"({url_obj.render_as_string(hide_password=True)}) with {application_name}"
                )
            url = url_obj.set(
                query={**url_obj.query, "application_name": application_name}
            ).render_as_string(hide_password=False)

        return create_engine(
            url,
            echo=DEBUG,
            json_serializer=json_serializer,
            pool_pre_ping=True,
            poolclass=poolclass,
        )

    @classmethod
    def setup_event_listener(
        cls, session: Session | sessionmaker
    ) -> Session | sessionmaker:
        event.listen(session, "before_flush", Listener.before_flush_event_listener)
        return session

    @classmethod
    def sessionmaker(cls, application_name: str | None = None):
        bind_obj = cls.engine(application_name=application_name)
        session_factory = sessionmaker(bind=bind_obj)
        cls.setup_event_listener(session_factory)
        return session_factory

    @classmethod
    def initialize_schema(cls, engine):
        """Initialize the database schema."""
        # Use SQLAlchemy to create all the tables.
        Base.metadata.create_all(engine)

    @classmethod
    def session(cls, url: str | None = None, application_name: str | None = None):
        engine = cls.engine(url, application_name=application_name)
        connection = engine.connect()
        return cls.session_from_connection(connection)

    @classmethod
    def session_from_connection(cls, connection: Connection) -> Session:
        session = Session(connection)
        cls.setup_event_listener(session)
        return session

    @classmethod
    def initialize_data(cls, session: Session):
        # Check if the recursive equivalents function exists already.
        query = (
            select([literal_column("proname")])
            .select_from(table("pg_proc"))
            .where(literal_column("proname") == "fn_recursive_equivalents")
        )
        result = session.execute(query).all()

        # If it doesn't, create it.
        if not result:
            resource_file = (
                resources_dir("sqlalchemy") / cls.RECURSIVE_EQUIVALENTS_FUNCTION
            )
            if not resource_file.is_file():
                raise OSError(
                    "Could not load recursive equivalents function from %s: file does not exist."
                    % resource_file
                )
            sql = resource_file.read_text()
            session.execute(text(sql))

        # Create initial content.
        from palace.manager.sqlalchemy.model.classification import Genre
        from palace.manager.sqlalchemy.model.datasource import DataSource
        from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism

        list(DataSource.well_known_sources(session))

        # Create any genres not in the database.
        for g in list(classifier.genres.values()):
            # TODO: On the very first startup this is rather expensive
            # because the cache is invalidated every time a Genre is
            # created, then populated the next time a Genre is looked
            # up. This wouldn't be a big problem, but this also happens
            # on setup for the unit tests.
            Genre.lookup(session, g, autocreate=True)

        # Make sure that the mechanisms fulfillable by the default
        # client are marked as such.
        for (
            content_type,
            drm_scheme,
        ) in DeliveryMechanism.default_client_can_fulfill_lookup:
            mechanism, is_new = DeliveryMechanism.lookup(
                session, content_type, drm_scheme
            )
            mechanism.default_client_can_fulfill = True

        from palace.manager.api.authentication.access_token import (
            PatronJWEAccessTokenProvider,
        )

        # Create our secret keys
        Key.create_admin_secret_key(session)
        Key.create_bearer_token_signing_key(session)
        PatronJWEAccessTokenProvider.create_key(session)

        # If there is currently no 'site configuration change'
        # Timestamp in the database, create one.
        timestamp, is_new = get_one_or_create(
            session,
            Timestamp,
            collection=None,
            service=Configuration.SITE_CONFIGURATION_CHANGED,
            create_method_kwargs=dict(finish=utc_now()),
        )
        if is_new:
            site_configuration_has_changed(session)
        session.commit()

        # Return a potentially-new Session object in case
        # it was updated by cls.update_timestamps_table
        return session


def production_session(application_name: type[object] | str) -> Session:
    if isinstance(application_name, str):
        application_name = application_name
    else:
        application_name = f"{application_name.__module__}.{application_name.__name__}"
    url = Configuration.database_url()
    if url.startswith('"'):
        url = url[1:]
    logging.debug("Database url: %s", url)
    _db = SessionManager.session(url, application_name=application_name)
    return _db
