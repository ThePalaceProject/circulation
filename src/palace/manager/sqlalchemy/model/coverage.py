# BaseCoverageRecord, Timestamp, CoverageRecord
from __future__ import annotations

import datetime
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Literal, Self

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm.session import Session

from palace.util.datetime_helpers import utc_now

from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.sentinel import SentinelType

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.collection import Collection


class BaseCoverageRecord:
    """Holds the ``coverage_status`` enum shared by the two dormant coverage models.

    Everything else on this mixin went with the queries it supported; it is
    removed along with those models and their tables in the follow-up PR.
    """

    SUCCESS = "success"
    TRANSIENT_FAILURE = "transient failure"
    PERSISTENT_FAILURE = "persistent failure"
    REGISTERED = "registered"

    status_enum = Enum(
        SUCCESS,
        TRANSIENT_FAILURE,
        PERSISTENT_FAILURE,
        REGISTERED,
        name="coverage_status",
    )


class Timestamp(Base):
    """Tracks the activities of Monitors, CoverageProviders,
    and general scripts.
    """

    __tablename__ = "timestamps"

    MONITOR_TYPE = "monitor"
    TASK_TYPE = "task"
    COVERAGE_PROVIDER_TYPE = "coverage_provider"
    SCRIPT_TYPE = "script"

    # We use SentinelType.ClearValue as a stand-in value used to indicate that a field in the timestamps
    # table should be explicitly set to None. Passing in None for most fields will use default values.

    service_type_enum = Enum(
        MONITOR_TYPE,
        COVERAGE_PROVIDER_TYPE,
        SCRIPT_TYPE,
        TASK_TYPE,
        name="service_type",
    )

    # Unique ID
    id: Mapped[int] = Column(Integer, primary_key=True)

    # Name of the service.
    service: Mapped[str] = Column(String(255), index=True, nullable=False)

    # Type of the service -- monitor, coverage provider, or script.
    # If the service type does not fit into these categories, this field
    # can be left null.
    service_type = Column(service_type_enum, index=True, default=None)

    # The collection, if any, associated with this service -- some services
    # run separately on a number of collections.
    collection_id = Column(
        Integer, ForeignKey("collections.id"), index=True, nullable=True
    )
    collection: Mapped[Collection | None] = relationship(
        "Collection", back_populates="timestamps"
    )

    # The last time the service _started_ running.
    start = Column(DateTime(timezone=True), nullable=True)

    # The last time the service _finished_ running. In most cases this
    # is the 'timestamp' proper.
    finish = Column(DateTime(timezone=True))

    # A description of the things the service achieved during its last
    # run. Each service may decide for itself what counts as an
    # 'achievement'; this is just a way to distinguish services that
    # do a lot of things from services that do a few things, or to see
    # services that run to completion but don't actually do anything.
    achievements = Column(Unicode, nullable=True)

    # This column allows a service to keep one item of state between
    # runs. For example, a monitor that iterates over a database table
    # needs to keep track of the last database ID it processed.
    counter = Column(Integer, nullable=True)

    # The exception, if any, that stopped the service from running
    # during its previous run.
    exception = Column(Unicode, nullable=True)

    def __repr__(self):
        format = "%b %d, %Y at %H:%M"
        if self.finish:
            finish = self.finish.strftime(format)
        else:
            finish = None
        if self.start:
            start = self.start.strftime(format)
        else:
            start = None
        if self.collection:
            collection = self.collection.name
        else:
            collection = None

        message = "<Timestamp {}: collection={}, start={} finish={} counter={}>".format(
            self.service,
            collection,
            start,
            finish,
            self.counter,
        )
        return message

    @classmethod
    def lookup(cls, _db, service, service_type, collection):
        return get_one(
            _db,
            Timestamp,
            service=service,
            service_type=service_type,
            collection=collection,
        )

    @classmethod
    def value(cls, _db, service, service_type, collection):
        """Return the current value of the given Timestamp, if it exists."""
        stamp = cls.lookup(_db, service, service_type, collection)
        if not stamp:
            return None
        return stamp.finish

    @classmethod
    def stamp(
        cls,
        _db: Session,
        service: str,
        service_type: str | None,
        collection: Collection | None = None,
        start: datetime.datetime | None | Literal[SentinelType.ClearValue] = None,
        finish: datetime.datetime | None | Literal[SentinelType.ClearValue] = None,
        achievements: str | None | Literal[SentinelType.ClearValue] = None,
        counter: int | None | Literal[SentinelType.ClearValue] = None,
        exception: str | None | Literal[SentinelType.ClearValue] = None,
    ) -> Timestamp:
        """Set a Timestamp, creating it if necessary.

        This should be called once a service has stopped running,
        whether or not it was able to complete its task.

        :param _db: A database connection.
        :param service: The name of the service associated with the Timestamp.

        :param service_type: The type of the service associated with
            the Timestamp. This must be one of the values in
            Timestmap.service_type_enum.
        :param collection: The Collection, if any, on which this service
            just ran.
        :param start: The time at which this service started running.
            Defaults to now.
        :param finish: The time at which this service stopped running.
            Defaults to now.
        :param achievements: A human-readable description of what the service
            did during its run.
        :param counter: An integer item of state that the service may use
            to track its progress between runs.
        :param exception: A stack trace for the exception, if any, which
            stopped the service from running.
        """
        if start is None and finish is None:
            start = finish = utc_now()
        elif start is None:
            start = finish
        elif finish is None:
            finish = start
        stamp, was_new = get_one_or_create(
            _db,
            Timestamp,
            service=service,
            service_type=service_type,
            collection=collection,
        )
        stamp.update(start, finish, achievements, counter, exception)

        # Committing immediately reduces the risk of contention.
        _db.commit()
        return stamp

    def update(
        self,
        start: datetime.datetime | None | Literal[SentinelType.ClearValue] = None,
        finish: datetime.datetime | None | Literal[SentinelType.ClearValue] = None,
        achievements: str | None | Literal[SentinelType.ClearValue] = None,
        counter: int | None | Literal[SentinelType.ClearValue] = None,
        exception: str | None | Literal[SentinelType.ClearValue] = None,
    ) -> None:
        """Use a single method to update all the fields that aren't
        used to identify a Timestamp.
        """

        if start is not None:
            if start is SentinelType.ClearValue:
                # In most cases, None is not a valid value for
                # Timestamp.start, but this can be overridden.
                start = None
            self.start = start
        if finish is not None:
            if finish is SentinelType.ClearValue:
                # In most cases, None is not a valid value for
                # Timestamp.finish, but this can be overridden.
                finish = None
            self.finish = finish
        if achievements is not None:
            if achievements is SentinelType.ClearValue:
                achievements = None
            self.achievements = achievements
        if counter is not None:
            if counter is SentinelType.ClearValue:
                counter = None
            self.counter = counter

        # Unlike the other fields, None is the default value for
        # .exception, so passing in None to mean "use the default" and
        # None to mean "no exception" mean the same thing. But we'll
        # support SentinelType.ClearValue anyway.
        if exception is SentinelType.ClearValue:
            exception = None
        self.exception = exception

    def to_data(self):
        """Convert this Timestamp to an unfinalized TimestampData."""
        from palace.manager.core.monitor import TimestampData

        return TimestampData(
            start=self.start,
            finish=self.finish,
            achievements=self.achievements,
            counter=self.counter,
        )

    @property
    def elapsed(self) -> datetime.timedelta | None:
        """The amount of time that elapsed between the start and finish of the
        service's last run, if both are known.
        """
        if self.start is None:
            return None

        finish = utc_now() if self.finish is None else self.finish
        return finish - self.start

    @property
    def elapsed_seconds(self) -> float | None:
        """
        The amount of time that elapsed between the start and finish of the
        service's last run, if both are known.

        This is a float value measured in seconds. If possible we retain
        microsecond precision.
        """
        elapsed = self.elapsed
        if elapsed is None:
            return None

        return elapsed / datetime.timedelta(microseconds=1) / 1_000_000

    @contextmanager
    def recording(self) -> Generator[Self]:
        """Context manager that records the start and finish times of a
        service's run, and captures any exception that occurs.
        """
        self.start = utc_now()
        self.finish = None
        self.exception = None
        try:
            yield self
        except Exception as e:
            self.exception = str(e)
            raise
        finally:
            self.finish = utc_now()

    __table_args__ = (UniqueConstraint("service", "collection_id"),)


class CoverageRecord(Base, BaseCoverageRecord):
    """A record of a Identifier being used as input into some process.

    Dormant model retained only so the ``coveragerecords`` table stays in the
    schema for one more release. The CoverageProvider machinery that read and
    wrote these records has been retired, and the ``coverage_records``
    relationships on Identifier, DataSource and Collection have been removed, so
    nothing in the current code reads or writes this table.

    Removing those relationships is what actually stops the reads: a mapped
    relationship is loaded by SQLAlchemy whenever its parent is deleted (to
    cascade the delete, or to null the child's foreign key), so while they
    existed every ``session.delete(collection)`` still SELECTed from this table.

    The model is kept for one more release because the schema of a freshly
    initialized database is built with ``create_all`` from these models, not by
    replaying migrations -- dropping the class would remove the table from new
    installs immediately, while N-1 app servers still expect it. The model and
    the table are removed together in a follow-up PR that ships after this
    release.

    Only the columns remain. The query and write helpers (``lookup``, ``add_for``,
    ``bulk_add`` and friends) are gone: nothing called them, and with the parent
    relationships removed a row they wrote could no longer be cleaned up when its
    Identifier, DataSource or Collection is deleted -- it would just make that
    delete fail on a foreign key. Keeping the class a bare table definition makes
    it impossible to write such a row.
    """

    __tablename__ = "coveragerecords"

    id: Mapped[int] = Column(Integer, primary_key=True)
    identifier_id = Column(Integer, ForeignKey("identifiers.id"), index=True)

    # If applicable, this is the ID of the data source that took the
    # Identifier as input.
    data_source_id = Column(Integer, ForeignKey("datasources.id"))
    operation = Column(String(255), default=None)

    timestamp = Column(DateTime(timezone=True), index=True)

    status = Column(BaseCoverageRecord.status_enum, index=True)
    exception = Column(Unicode, index=True)

    # If applicable, this is the ID of the collection for which
    # coverage has taken place. This is currently only applicable
    # for Metadata Wrangler coverage.
    collection_id = Column(Integer, ForeignKey("collections.id"), nullable=True)

    __table_args__ = (
        Index(
            "ix_identifier_id_data_source_id_operation",
            identifier_id,
            data_source_id,
            operation,
            unique=True,
            postgresql_where=collection_id.is_(None),
        ),
        Index(
            "ix_identifier_id_data_source_id_operation_collection_id",
            identifier_id,
            data_source_id,
            operation,
            collection_id,
            unique=True,
        ),
    )


Index(
    "ix_coveragerecords_data_source_id_operation_identifier_id",
    CoverageRecord.data_source_id,
    CoverageRecord.operation,
    CoverageRecord.identifier_id,
)


class EquivalencyCoverageRecord(Base, BaseCoverageRecord):
    """Dormant model retained only so the ``equivalentscoveragerecords`` table
    stays in the schema for one more release.

    The equivalent-identifiers refresh no longer reads or writes this table — it
    was replaced by the Redis dirty-set queue and the ``equivalent_identifiers_refresh``
    Celery task. But per our online-migration convention the table cannot be dropped
    in the same release that stops using it: during a rolling deploy, N-1 app servers
    still run the old listener that writes here, so dropping the table now would make
    them error. The table and this model will be removed in a follow-up PR that ships
    after this release. See https://github.com/ThePalaceProject/circulation/pull/3459.
    """

    __tablename__ = "equivalentscoveragerecords"

    id: Mapped[int] = Column(Integer, primary_key=True)

    equivalency_id: Mapped[int] = Column(
        Integer,
        ForeignKey("equivalents.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    operation = Column(String(255), index=True, default=None)
    timestamp = Column(DateTime(timezone=True), index=True)
    status = Column(BaseCoverageRecord.status_enum, index=True)
    exception = Column(Unicode)

    __table_args__ = (UniqueConstraint(equivalency_id, operation),)
