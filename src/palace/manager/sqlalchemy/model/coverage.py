# BaseCoverageRecord, Timestamp
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
    Integer,
    String,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import or_

from palace.util.datetime_helpers import utc_now

from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.sentinel import SentinelType

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.collection import Collection


class BaseCoverageRecord:
    """Contains useful constants used by CoverageRecord."""

    SUCCESS = "success"
    TRANSIENT_FAILURE = "transient failure"
    PERSISTENT_FAILURE = "persistent failure"
    REGISTERED = "registered"

    ALL_STATUSES = [REGISTERED, SUCCESS, TRANSIENT_FAILURE, PERSISTENT_FAILURE]

    # Count coverage as attempted if the record is not 'registered'.
    PREVIOUSLY_ATTEMPTED = [SUCCESS, TRANSIENT_FAILURE, PERSISTENT_FAILURE]

    # By default, count coverage as present if it ended in
    # success or in persistent failure. Do not count coverage
    # as present if it ended in transient failure.
    DEFAULT_COUNT_AS_COVERED = [SUCCESS, PERSISTENT_FAILURE]

    status_enum = Enum(
        SUCCESS,
        TRANSIENT_FAILURE,
        PERSISTENT_FAILURE,
        REGISTERED,
        name="coverage_status",
    )

    @classmethod
    def not_covered(
        cls, count_as_covered=None, count_as_not_covered_if_covered_before=None
    ):
        """Filter a query to find only items without coverage records.

        :param count_as_covered: A list of constants that indicate
            types of coverage records that should count as 'coverage'
            for purposes of this query.
        :param count_as_not_covered_if_covered_before: If a coverage record
            exists, but is older than the given date, do not count it as
            covered.
        :return: A clause that can be passed in to Query.filter().
        """

        if not count_as_covered:
            count_as_covered = cls.DEFAULT_COUNT_AS_COVERED
        elif isinstance(count_as_covered, (bytes, str)):
            count_as_covered = [count_as_covered]

        # If there is no coverage record, then of course the item is
        # not covered.
        missing = cls.id == None

        # If we're looking for specific coverage statuses, then a
        # record does not count if it has some other status.
        missing = or_(missing, ~cls.status.in_(count_as_covered))

        # If the record's timestamp is before the cutoff time, we
        # don't count it as covered, regardless of which status it
        # has.
        if count_as_not_covered_if_covered_before:
            missing = or_(
                missing, cls.timestamp < count_as_not_covered_if_covered_before
            )

        return missing


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
