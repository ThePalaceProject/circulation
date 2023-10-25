from __future__ import annotations

import json
import logging
import os
from typing import Any, Generator, List, Literal, Tuple, Type, TypeVar

from contextlib2 import contextmanager
from psycopg2.extensions import adapt as sqlescape
from psycopg2.extras import NumericRange
from pydantic.json import pydantic_encoder
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.sql import compiler, select
from sqlalchemy.sql.expression import literal_column, table

Base = declarative_base()

from core import classifier
from core.model.constants import (
    DataSourceConstants,
    EditionConstants,
    IdentifierConstants,
    LinkRelations,
    MediaTypes,
)

# This is the lock ID used to ensure that only one circulation manager
# initializes or migrates the database at a time.
LOCK_ID_DB_INIT = 1000000001

# This is the lock ID used to ensure that only one circulation manager
# initializes an application instance at a time.
LOCK_ID_APP_INIT = 1000000002


@contextmanager
def pg_advisory_lock(
    connection: Connection | Session, lock_id: int | None
) -> Generator[None, None, None]:
    """
    Application wide locking based on Lock IDs

    If lock_id is None, no lock is acquired.
    """
    if lock_id is None:
        yield
    else:
        # Create the lock
        connection.execute(text(f"SELECT pg_advisory_lock({lock_id});"))
        try:
            yield
        finally:
            # Close the lock
            connection.execute(text(f"SELECT pg_advisory_unlock({lock_id});"))


def flush(db):
    """Flush the database connection unless it's known to already be flushing."""
    is_flushing = False
    if hasattr(db, "_flushing"):
        # This is a regular database session.
        is_flushing = db._flushing
    elif hasattr(db, "registry"):
        # This is a flask_scoped_session scoped session.
        is_flushing = db.registry()._flushing
    else:
        logging.error("Unknown database connection type: %r", db)
    if not is_flushing:
        db.flush()


T = TypeVar("T")


def create(
    db: Session, model: Type[T], create_method="", create_method_kwargs=None, **kwargs
) -> Tuple[T, Literal[True]]:
    kwargs.update(create_method_kwargs or {})
    created = getattr(model, create_method, model)(**kwargs)
    db.add(created)
    flush(db)
    return created, True


def get_one(
    db: Session, model: Type[T], on_multiple="error", constraint=None, **kwargs
) -> T | None:
    """Gets an object from the database based on its attributes.

    :param constraint: A single clause that can be passed into
        `sqlalchemy.Query.filter` to limit the object that is returned.
    :return: object or None
    """
    constraint = constraint
    if "constraint" in kwargs:
        constraint = kwargs["constraint"]
        del kwargs["constraint"]

    q = db.query(model).filter_by(**kwargs)
    if constraint is not None:
        q = q.filter(constraint)

    try:
        return q.one()
    except MultipleResultsFound:
        if on_multiple == "error":
            raise
        elif on_multiple == "interchangeable":
            # These records are interchangeable so we can use
            # whichever one we want.
            #
            # This may be a sign of a problem somewhere else. A
            # database-level constraint might be useful.
            q = q.limit(1)
            return q.one()
    except NoResultFound:
        return None
    return None


def get_one_or_create(
    db: Session, model: Type[T], create_method="", create_method_kwargs=None, **kwargs
) -> Tuple[T, bool]:
    one = get_one(db, model, **kwargs)
    if one:
        return one, False
    else:
        __transaction = db.begin_nested()
        try:
            # These kwargs are supported by get_one() but not by create().
            get_one_keys = ["on_multiple", "constraint"]
            for key in get_one_keys:
                if key in kwargs:
                    del kwargs[key]
            obj = create(db, model, create_method, create_method_kwargs, **kwargs)
            __transaction.commit()
            return obj
        except IntegrityError as e:
            logging.info(
                "INTEGRITY ERROR on %r %r, %r: %r",
                model,
                create_method_kwargs,
                kwargs,
                e,
            )
            __transaction.rollback()
            return db.query(model).filter_by(**kwargs).one(), False


def numericrange_to_string(r):
    """Helper method to convert a NumericRange to a human-readable string."""
    if not r:
        return ""
    lower = r.lower
    upper = r.upper
    if upper is None and lower is None:
        return ""
    if lower and upper is None:
        return str(lower)
    if upper and lower is None:
        return str(upper)
    if not r.upper_inc:
        upper -= 1
    if not r.lower_inc:
        lower += 1
    if upper == lower:
        return str(lower)
    return f"{lower}-{upper}"


def numericrange_to_tuple(r):
    """Helper method to normalize NumericRange into a tuple."""
    if r is None:
        return (None, None)
    lower = r.lower
    upper = r.upper
    if lower and not r.lower_inc:
        lower += 1
    if upper and not r.upper_inc:
        upper -= 1
    return lower, upper


def tuple_to_numericrange(t):
    """Helper method to convert a tuple to an inclusive NumericRange."""
    if not t:
        return None
    return NumericRange(t[0], t[1], "[]")


class PresentationCalculationPolicy:
    """Which parts of the Work or Edition's presentation
    are we actually looking to update?
    """

    DEFAULT_LEVELS = 3
    DEFAULT_THRESHOLD = 0.5
    DEFAULT_CUTOFF = 1000

    def __init__(
        self,
        choose_edition=True,
        set_edition_metadata=True,
        classify=True,
        choose_summary=True,
        calculate_quality=True,
        choose_cover=True,
        regenerate_marc_record=False,
        update_search_index=False,
        verbose=True,
        equivalent_identifier_levels=DEFAULT_LEVELS,
        equivalent_identifier_threshold=DEFAULT_THRESHOLD,
        equivalent_identifier_cutoff=DEFAULT_CUTOFF,
    ):
        """Constructor.

        :param choose_edition: Should a new presentation edition be
           chosen/created, or should we assume the old one is fine?
        :param set_edition_metadata: Should we set new values for
           basic metadata such as title?
        :param classify: Should we reconsider which Genres under which
           a Work should be filed?
        :param choose_summary: Should we reconsider which of the
           available summaries is the best?
        :param calculate_quality: Should we recalculate the overall
           quality of the Work?
        :param choose_cover: Should we reconsider which of the
           available cover images is the best?
        :param regenerate_marc_record: Should we regenerate the MARC record
           for this Work?
        :param update_search_index: Should we reindex this Work's
           entry in the search index?
        :param verbose: Should we print out information about the work we're
           doing?
        :param equivalent_identifier_levels: When determining which
           identifiers refer to this Work (used when gathering
           classifications, cover images, etc.), how many levels of
           equivalency should we go down? E.g. for one level of
           equivalency we will go from a proprietary vendor ID to the
           equivalent ISBN.
        :param equivalent_identifier_threshold: When determining which
           identifiers refer to this Work, what is the probability
           threshold for 'equivalency'? E.g. a value of 1 means that
           we will not count two identifiers as equivalent unless we
           are absolutely certain.
        :param equivalent_identifier_cutoff: When determining which
           identifiers refer to this work, how many Identifiers are
           enough? Gathering _all_ the identifiers that identify an
           extremely popular work can take an extraordinarily long time
           for very little payoff, so it's useful to have a cutoff.

           The cutoff is applied _per level_, so the total maximum
           number of equivalent identifiers is
           equivalent_identifier_cutoff * equivalent_identifier_levels.
        """
        self.choose_edition = choose_edition
        self.set_edition_metadata = set_edition_metadata
        self.classify = classify
        self.choose_summary = choose_summary
        self.calculate_quality = calculate_quality
        self.choose_cover = choose_cover

        # Regenerate MARC records, except that they will
        # never be generated unless a MARC organization code is set
        # in a sitewide configuration setting.
        self.regenerate_marc_record = regenerate_marc_record

        # Similarly for update_search_index.
        self.update_search_index = update_search_index

        self.verbose = verbose

        self.equivalent_identifier_levels = equivalent_identifier_levels
        self.equivalent_identifier_threshold = equivalent_identifier_threshold
        self.equivalent_identifier_cutoff = equivalent_identifier_cutoff

    @classmethod
    def recalculate_everything(cls):
        """A PresentationCalculationPolicy that always recalculates
        everything, even when it doesn't seem necessary.
        """
        return PresentationCalculationPolicy(
            regenerate_marc_record=True,
            update_search_index=True,
        )

    @classmethod
    def reset_cover(cls):
        """A PresentationCalculationPolicy that only resets covers
        (including updating cached entries, if necessary) without
        impacting any other metadata.
        """
        return cls(
            choose_cover=True,
            choose_edition=False,
            set_edition_metadata=False,
            classify=False,
            choose_summary=False,
            calculate_quality=False,
        )


def dump_query(query):
    dialect = query.session.bind.dialect
    statement = query.statement
    comp = compiler.SQLCompiler(dialect, statement)
    enc = dialect.encoding
    params = {}
    for k, v in list(comp.params.items()):
        if isinstance(v, str):
            v = v.encode(enc)
        params[k] = sqlescape(v)
    return (comp.string.encode(enc) % params).decode(enc)


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
    def engine(cls, url=None):
        url = url or Configuration.database_url()
        return create_engine(url, echo=DEBUG, json_serializer=json_serializer)

    @classmethod
    def setup_event_listener(
        cls, session: Union[Session, sessionmaker]
    ) -> Union[Session, sessionmaker]:
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
    def resource_directory(cls):
        """The directory containing SQL files used in database setup."""
        base_path = os.path.split(__file__)[0]
        return os.path.join(base_path, "files")

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
            resource_file = os.path.join(
                cls.resource_directory(), cls.RECURSIVE_EQUIVALENTS_FUNCTION
            )
            if not os.path.exists(resource_file):
                raise OSError(
                    "Could not load recursive equivalents function from %s: file does not exist."
                    % resource_file
                )
            sql = open(resource_file).read()
            session.execute(text(sql))

        # Create initial content.
        from core.model.classification import Genre
        from core.model.datasource import DataSource
        from core.model.licensing import DeliveryMechanism

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


class SessionBulkOperation:
    """Bulk insert/update/operate on a session"""

    def __init__(
        self,
        session,
        batch_size,
        bulk_method: str = "bulk_save_objects",
        bulk_method_kwargs=None,
    ) -> None:
        self.session = session
        self.bulk_method = bulk_method
        self.bulk_method_kwargs = bulk_method_kwargs or {}
        self.batch_size = batch_size
        self._objects: List[Base] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._bulk_operation()

    def add(self, object):
        self._objects.append(object)
        if len(self._objects) == self.batch_size:
            self._bulk_operation()

    def _bulk_operation(self):
        self.bulk_method, getattr(
            self.session,
            self.bulk_method,
        )(self._objects, **self.bulk_method_kwargs)
        self.session.commit()
        self._objects = []


# We rely on all of our sqlalchemy models being imported here, so that they are
# registered with the declarative base. This is necessary to make sure that all
# of our models are properly reflected in the database when we run migrations or
# create a new database.

from api.saml.metadata.federations.model import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from core.model.admin import Admin, AdminRole
from core.model.cachedfeed import CachedMARCFile
from core.model.circulationevent import CirculationEvent
from core.model.classification import Classification, Genre, Subject
from core.model.collection import (
    Collection,
    CollectionIdentifier,
    CollectionMissing,
    collections_identifiers,
)
from core.model.configuration import ConfigurationSetting, ExternalIntegration
from core.model.contributor import Contribution, Contributor
from core.model.coverage import (
    BaseCoverageRecord,
    CoverageRecord,
    Timestamp,
    WorkCoverageRecord,
)
from core.model.credential import Credential
from core.model.customlist import CustomList, CustomListEntry
from core.model.datasource import DataSource
from core.model.devicetokens import DeviceToken
from core.model.discovery_service_registration import DiscoveryServiceRegistration
from core.model.edition import Edition
from core.model.hassessioncache import HasSessionCache
from core.model.identifier import Equivalency, Identifier
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.model.library import Library
from core.model.licensing import (
    DeliveryMechanism,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
    PolicyException,
    RightsStatus,
)
from core.model.listeners import *
from core.model.measurement import Measurement
from core.model.patron import (
    Annotation,
    Hold,
    Loan,
    LoanAndHoldMixin,
    Patron,
    PatronProfileStorage,
)
from core.model.resource import (
    Hyperlink,
    Representation,
    Resource,
    ResourceTransformation,
)
from core.model.time_tracking import PlaytimeEntry, PlaytimeSummary
from core.model.work import Work, WorkGenre

# Import order important here to avoid an import cycle.
from core.lane import Lane, LaneGenre  # isort:skip
