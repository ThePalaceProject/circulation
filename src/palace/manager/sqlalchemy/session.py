from __future__ import annotations

import json
import logging
import os
from typing import Any

from pydantic.json import pydantic_encoder
from sqlalchemy import create_engine, event, literal_column, select, table, text
from sqlalchemy.engine import Connection
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
from palace.manager.util.resources import resources_dir

DEBUG = False


def json_encoder(obj: Any) -> Any:
    # Handle Flask Babel LazyString objects.
    if hasattr(obj, "__html__"):
        return str(obj.__html__())

    # Pass everything else off to Pydantic JSON encoder.
    return pydantic_encoder(obj)


def json_serializer(*args, **kwargs) -> str:
    return json.dumps(*args, default=json_encoder, **kwargs)


class SessionManager:
    # A function that calculates recursively equivalent identifiers
    # is also defined in SQL.
    RECURSIVE_EQUIVALENTS_FUNCTION = "recursive_equivalents.sql"

    @classmethod
    def engine(cls, url: str | None = None, poolclass: type[Pool] | None = None):
        url = url or Configuration.database_url()
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
    def sessionmaker(cls, url=None, session=None):
        if not (url or session):
            url = Configuration.database_url()
        if url:
            bind_obj = cls.engine(url)
        elif session:
            bind_obj = session.get_bind()
            if not os.environ.get("TESTING"):
                # If a factory is being created from a session in test mode,
                # use the same Connection for all of the tests so objects can
                # be accessed. Otherwise, bind against an Engine object.
                bind_obj = bind_obj.engine
        session_factory = sessionmaker(bind=bind_obj)
        cls.setup_event_listener(session_factory)
        return session_factory

    @classmethod
    def initialize_schema(cls, engine):
        """Initialize the database schema."""
        # Use SQLAlchemy to create all the tables.
        Base.metadata.create_all(engine)

    @classmethod
    def session(cls, url, initialize_data=True, initialize_schema=True):
        engine = cls.engine(url)
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


def production_session(initialize_data=True) -> Session:
    url = Configuration.database_url()
    if url.startswith('"'):
        url = url[1:]
    logging.debug("Database url: %s", url)
    _db = SessionManager.session(url, initialize_data=initialize_data)
    return _db
