from __future__ import annotations

import datetime
import logging
import traceback
from typing import TYPE_CHECKING, Literal

from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError
from sqlalchemy.sql.expression import and_, or_
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from palace.manager.service.container import container_instance
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.collection import Collection, CollectionMissing
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.sentinel import SentinelType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class CollectionMonitorLogger(logging.LoggerAdapter):
    """Prefix log messages with a collection, if one is present."""

    def __init__(self, logger, extra):
        self.logger = logger
        self.extra = extra
        collection = self.extra.get("collection", None)
        self.log_prefix = f"[{collection.name}] " if collection else ""
        # TODO: Remove the next line once all uses have adopted `warning`.
        self.warn = self.warning

    def process(self, msg, kwargs):
        return f"{self.log_prefix}{msg}", kwargs


class Monitor:
    """A Monitor is responsible for running some piece of code on a
    regular basis. A Monitor has an associated Timestamp that tracks
    the last time it successfully ran; it may use this information on
    its next run to cover the intervening span of time.

    A Monitor will run to completion and then stop. To repeatedly run
    a Monitor, you'll need to repeatedly invoke it from some external
    source such as a cron job.

    This class is designed to be subclassed rather than instantiated
    directly. Subclasses must define SERVICE_NAME. Subclasses may
    define replacement values for DEFAULT_START_TIME and
    DEFAULT_COUNTER.

    Although any Monitor may be associated with a Collection, it's
    most useful to subclass CollectionMonitor if you're writing code
    that needs to be run on every Collection of a certain type.

    """

    # In your subclass, set this to the name of the service,
    # e.g. "Overdrive Circulation Monitor". All instances of your
    # subclass will give this as their service name and track their
    # Timestamps under this name.
    SERVICE_NAME: str | None = None

    # Some useful relative constants for DEFAULT_START_TIME (below).
    ONE_MINUTE_AGO = datetime.timedelta(seconds=60)
    ONE_YEAR_AGO = datetime.timedelta(seconds=60 * 60 * 24 * 365)
    NEVER = object()

    # If there is no Timestamp for this Monitor, this time will be
    # passed into `run_once()` as the `start_time` parameter.
    DEFAULT_START_TIME: object | datetime.timedelta = ONE_MINUTE_AGO

    # When the Timestamp for this Monitor is created, this value will
    # be set for `Timestamp.counter`.
    #
    # This is only used by the SweepMonitor subclass.
    DEFAULT_COUNTER: int | None = None

    def __init__(self, _db, collection=None):
        self._db = _db
        cls = self.__class__
        if not self.SERVICE_NAME and not cls.SERVICE_NAME:
            raise ValueError("%s must define SERVICE_NAME." % cls.__name__)
        self.service_name = self.SERVICE_NAME
        default_start_time = cls.DEFAULT_START_TIME
        if isinstance(default_start_time, datetime.timedelta):
            default_start_time = utc_now() - default_start_time
        self.default_start_time = default_start_time
        self.default_counter = cls.DEFAULT_COUNTER

        # We store the collection ID rather than the Collection to
        # avoid breakage in case an app server with a scoped session
        # ever uses a Monitor.
        self.collection_id = None
        if collection:
            self.collection_id = collection.id

        # Make sure that logging is configured.
        self.services = container_instance()
        self.services.init_resources()

        self.exception = None

    @property
    def log(self):
        if not hasattr(self, "_log"):
            self._log = CollectionMonitorLogger(
                logging.getLogger(self.service_name),
                {"collection": self.collection},
            )
        return self._log

    @property
    def collection(self):
        """Retrieve the Collection object associated with this
        Monitor.
        """
        if not self.collection_id:
            return None
        return get_one(self._db, Collection, id=self.collection_id)

    @property
    def initial_start_time(self):
        """The time that should be used as the 'start time' the first
        time this Monitor is run.
        """
        if self.default_start_time is self.NEVER:
            return None
        if self.default_start_time:
            return self.default_start_time
        return utc_now()

    def timestamp(self):
        """Find or create a Timestamp for this Monitor.

        This does not use TimestampData because it relies on checking
        whether a Timestamp already exists in the database.

        A new timestamp will have .finish set to None, since the first
        run is presumably in progress.
        """
        initial_timestamp = self.initial_start_time
        timestamp, new = get_one_or_create(
            self._db,
            Timestamp,
            service=self.service_name,
            service_type=Timestamp.MONITOR_TYPE,
            collection=self.collection,
            create_method_kwargs=dict(
                start=initial_timestamp,
                finish=None,
                counter=self.default_counter,
            ),
        )
        return timestamp

    def run(self):
        """Do all the work that has piled up since the
        last time the Monitor ran to completion.
        """
        # Use the existing Timestamp to determine the progress made up
        # to this point. It's the job of the subclass to decide where
        # to go from here.
        timestamp_obj = self.timestamp()
        progress = timestamp_obj.to_data()

        this_run_start = utc_now()
        exception = None

        ignorable = (None, SentinelType.ClearValue)
        try:
            new_timestamp = self.run_once(progress)
            this_run_finish = utc_now()
            if new_timestamp is None:
                # Assume this Monitor has no special needs surrounding
                # its timestamp.
                new_timestamp = TimestampData()
            if new_timestamp.achievements not in ignorable:
                # This eliminates the need to create similar-looking
                # strings for TimestampData.achievements and for the log.
                self.log.info(new_timestamp.achievements)
            if new_timestamp.exception in ignorable:
                # run_once() completed with no exceptions being raised.
                # We can run the cleanup code and finalize the timestamp.
                self.cleanup()
                new_timestamp.finalize(
                    service=self.service_name,
                    service_type=Timestamp.MONITOR_TYPE,
                    collection=self.collection,
                    start=this_run_start,
                    finish=this_run_finish,
                    exception=None,
                )
                new_timestamp.apply(self._db)
            else:
                # This will be treated the same as an unhandled
                # exception, below.
                exception = new_timestamp.exception
        except Exception:
            this_run_finish = utc_now()
            self.log.exception(
                "Error running %s monitor. Timestamp will not be updated.",
                self.service_name,
            )
            exception = traceback.format_exc()
        if exception is not None:
            # We will update Timestamp.exception but not go through
            # the whole TimestampData.apply() process, which might
            # erase the information the Monitor needs to recover from
            # this failure.
            timestamp_obj.exception = exception

        try:
            self._db.commit()
        except Exception:
            self._db.rollback()
            # save the exception to the time after commit in case the db commit fails.
            timestamp_obj = self.timestamp()
            timestamp_obj.finish = this_run_finish
            timestamp_obj.exception = exception
            self._db.commit()

        duration = this_run_finish - this_run_start
        self.log.info(
            "Ran %s monitor in %.2f sec.",
            self.service_name,
            duration.total_seconds(),
        )

    def run_once(self, progress):
        """Do the actual work of the Monitor.

        :param progress: A TimestampData representing the
           work done by the Monitor up to this point.

        :return: A TimestampData representing how you want the
            Monitor's entry in the `timestamps` table to look like
            from this point on.  NOTE: Modifying the incoming
            `progress` and returning it is generally a bad idea,
            because the incoming `progress` is full of old
            data. Instead, return a new TimestampData containing data
            for only the fields you want to set.
        """
        raise NotImplementedError()

    def cleanup(self):
        """Do any work that needs to be done at the end, once the main work
        has completed successfully.
        """


class TimelineMonitor(Monitor):
    """A monitor that needs to process everything that happened between
    two specific times.

    This Monitor uses `Timestamp.start` and `Timestamp.finish` to describe
    the span of time covered in the most recent run, not the time it
    actually took to run.
    """

    OVERLAP = datetime.timedelta(minutes=5)

    def run_once(self, progress):
        if progress.finish is None:
            # This monitor has never run before. Use the default
            # start time for this monitor.
            start = self.initial_start_time
        else:
            start = progress.finish - self.OVERLAP
        cutoff = utc_now()
        self.catch_up_from(start, cutoff, progress)

        if progress.is_failure:
            # Something has gone wrong. Stop immediately.
            #
            # TODO: Ideally we would undo any other changes made to
            # the TimestampData, but most Monitors don't set
            # .exception directly so it's not a big deal.
            return progress

        # We set `finish` to the time at which we _started_
        # running this process, to reduce the risk that we miss
        # events that happened while the process was running.
        progress.start = start
        progress.finish = cutoff
        return progress

    def catch_up_from(self, start, cutoff, progress):
        """Make sure all events between `start` and `cutoff` are covered.

        :param start: Start looking for events that happened at this
            time.
        :param cutoff: You're not responsible for events that happened
            after this time.
        :param progress: A TimestampData representing the progress so
            far. Unlike with run_once(), you are encouraged to can
            modify this in place, for instance to set .achievements.
            However, you cannot change .start and .finish -- any
            changes will be overwritten by run_once().
        """
        raise NotImplementedError()

    @classmethod
    def slice_timespan(cls, start, cutoff, increment):
        """Slice a span of time into segments no large than [increment].

        This lets you divide up a task like "gather the entire
        circulation history for a collection" into chunks of one day.

        :param start: A datetime.
        :param cutoff: A datetime.
        :param increment: A timedelta.
        """
        slice_start = start
        while slice_start < cutoff:
            full_slice = True
            slice_cutoff = slice_start + increment
            if slice_cutoff > cutoff:
                slice_cutoff = cutoff
                full_slice = False
            yield slice_start, slice_cutoff, full_slice
            slice_start = slice_start + increment


class CollectionMonitor(Monitor):
    """A Monitor that does something for all Collections that come
    from a certain provider.

    This class is designed to be subclassed rather than instantiated
    directly. Subclasses must define SERVICE_NAME and
    PROTOCOL. Subclasses may define replacement values for
    DEFAULT_START_TIME and DEFAULT_COUNTER.
    """

    # Set this to the name of the license provider managed by this
    # Monitor. If this value is set, the CollectionMonitor can only be
    # instantiated with Collections that get their licenses from this
    # provider. If this is unset, the CollectionMonitor can be
    # instantiated with any Collection, or with no Collection at all.
    PROTOCOL: str | None = None

    def __init__(self, _db, collection):
        cls = self.__class__
        self.protocol = cls.PROTOCOL
        if self.protocol:
            if collection is None:
                raise CollectionMissing()
        cls._validate_collection(collection, protocol=self.protocol)
        super().__init__(_db, collection)

    @classmethod
    def _validate_collection(cls, collection, protocol=None):
        protocol = protocol or cls.PROTOCOL
        if protocol and collection.protocol != protocol:
            raise ValueError(
                "Collection protocol (%s) does not match Monitor protocol (%s)"
                % (collection.protocol, protocol)
            )

    @classmethod
    def all(cls, _db, collections=None, **constructor_kwargs):
        """Yield a sequence of CollectionMonitor objects: one for every
        Collection associated with cls.PROTOCOL.

        If `collections` is specified, then there must be a Monitor
        for each one and Monitors will be yielded in the same order
        that the collections are specified. Otherwise, Monitors will
        be yielded as follows...

        Monitors that have no Timestamp will be yielded first. After
        that, Monitors with older values for Timestamp.start will be
        yielded before Monitors with newer values.

        :param _db: Database session object.
        :param collections: An optional list of collections. If None,
            we'll process all collections.
        :type collections: List[Collection]
        :param constructor_kwargs: These keyword arguments will be passed
            into the CollectionMonitor constructor.

        """
        service_match = or_(
            Timestamp.service == cls.SERVICE_NAME, Timestamp.service == None
        )
        collections_for_protocol = Collection.by_protocol(_db, cls.PROTOCOL).outerjoin(
            Timestamp,
            and_(
                Timestamp.collection_id == Collection.id,
                service_match,
            ),
        )

        if collections:
            # verify that each specified collection exists in this context
            for coll in collections:  # type: str
                try:
                    cls._validate_collection(coll, cls.PROTOCOL)
                except ValueError as e:
                    additional_info = (
                        "Only the following collections are available: {!r}".format(
                            [c.name for c in collections_for_protocol]
                        )
                    )
                    e.args += (additional_info,)
                    raise ValueError(str(e) + "\n" + additional_info)
        else:
            collections = collections_for_protocol.order_by(
                Timestamp.start.asc().nullsfirst()
            )
        for collection in collections:
            yield cls(_db=_db, collection=collection, **constructor_kwargs)


class SweepMonitor(CollectionMonitor):
    """A monitor that does some work for every item in a database table,
    then stops.

    Progress through the table is stored in the Timestamp, so that if
    the Monitor crashes, the next time the Monitor is run, it starts
    at the item that caused the crash, rather than starting from the
    beginning of the table.
    """

    # The completion of each individual item should be logged at
    # this log level.
    COMPLETION_LOG_LEVEL = logging.INFO

    # Items will be processed in batches of this size.
    DEFAULT_BATCH_SIZE = 100

    MAXIMUM_BATCH_RETRIES = 10

    DEFAULT_COUNTER = 0

    # The model class corresponding to the database table that this
    # Monitor sweeps over. This class must keep its primary key in the
    # `id` field.
    MODEL_CLASS: type[Base] | None = None

    def __init__(self, _db, collection=None, batch_size=None):
        cls = self.__class__
        if not batch_size or batch_size < 0:
            batch_size = cls.DEFAULT_BATCH_SIZE
        self.batch_size = batch_size
        if not cls.MODEL_CLASS:
            raise ValueError("%s must define MODEL_CLASS" % cls.__name__)
        self.model_class = cls.MODEL_CLASS
        super().__init__(_db, collection=collection)

    def run_once(self, *ignore):
        timestamp = self.timestamp()
        offset = timestamp.counter
        new_offset = offset
        exception = None

        # The timestamp for a SweepMonitor is purely informative --
        # we're not trying to capture all the events that happened
        # since a certain time -- so we're going to make sure the
        # timestamp is set from the start of the run to the end of the
        # last _successful_ batch.
        run_started_at = utc_now()
        timestamp.start = run_started_at
        total_processed = 0
        while True:
            batch_started_at = utc_now()
            new_offset, batch_size = self.process_batch(offset)
            total_processed += batch_size
            batch_ended_at = utc_now()

            self.log.debug(
                "%s monitor went from offset %s to %s in %.2f sec",
                self.service_name,
                offset,
                new_offset,
                (batch_ended_at - batch_started_at).total_seconds(),
            )
            achievements = "Records processed: %d." % total_processed

            offset = new_offset
            if offset == 0:
                # We completed a sweep. We're done.
                break

            # We need to do another batch. If it should raise an exception,
            # we don't want to lose the progress we've already made.
            timestamp.update(
                counter=new_offset, finish=batch_ended_at, achievements=achievements
            )
            self._db.commit()

        # We're done with this run. The run() method will do the final
        # update.
        return TimestampData(counter=offset, achievements=achievements)

    @retry(
        retry=(
            retry_if_exception_type(StaleDataError)
            | retry_if_exception_type(ObjectDeletedError)
            | retry_if_exception_type(InvalidRequestError)
        ),
        stop=stop_after_attempt(MAXIMUM_BATCH_RETRIES),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        reraise=True,
    )
    def process_batch(self, offset):
        """Process one batch of work."""
        with self._db.begin_nested():
            offset = offset or 0
            items = self.fetch_batch(offset).all()
            if items:
                self.process_items(items)
                # We've completed a batch. Return the ID of the last item
                # in the batch so we don't do this work again.
                result = (items[-1].id, len(items))
            else:
                # There are no more items in this database table, so we
                # are done with the sweep. Reset the counter.
                result = (0, 0)

            return result

    def process_items(self, items):
        """Process a list of items."""
        for item in items:
            self.process_item(item)
            self.log.log(self.COMPLETION_LOG_LEVEL, "Completed %r", item)

    def fetch_batch(self, offset):
        """Retrieve one batch of work from the database."""
        q = (
            self.item_query()
            .filter(self.model_class.id > offset)
            .order_by(self.model_class.id)
            .limit(self.batch_size)
        )
        return q

    def item_query(self):
        """Find the items that need to be processed in the sweep.

        :return: A query object.
        """
        # Start by getting everything in the table.
        qu = self._db.query(self.model_class)
        if self.collection:
            # Restrict to only those items associated with self.collection
            # somehow.
            qu = self.scope_to_collection(qu, self.collection)
        qu = qu.order_by(self.model_class.id)
        return qu

    def scope_to_collection(self, qu, collection):
        """Restrict the given query so that it only finds items
        associated with the given collection.

        :param qu: A query object.
        :param collection: A Collection object, presumed to not be None.
        """
        raise NotImplementedError()

    def process_item(self, item):
        """Do the work that needs to be done for a given item."""
        raise NotImplementedError()


class IdentifierSweepMonitor(SweepMonitor):
    """A Monitor that does some work for every Identifier."""

    MODEL_CLASS = Identifier

    def scope_to_collection(self, qu, collection):
        """Only find Identifiers licensed through the given Collection."""
        return qu.join(Identifier.licensed_through).filter(
            LicensePool.collection == collection
        )


class TimestampData:
    def __init__(
        self,
        start: datetime.datetime | None | Literal[SentinelType.ClearValue] = None,
        finish: datetime.datetime | None | Literal[SentinelType.ClearValue] = None,
        achievements: str | None | Literal[SentinelType.ClearValue] = None,
        counter: int | None | Literal[SentinelType.ClearValue] = None,
        exception: str | None | Literal[SentinelType.ClearValue] = None,
    ) -> None:
        """A constructor intended to be used by a service to customize its
        eventual Timestamp.

        service, service_type, and collection cannot be set through
        this constructor, because they are generally not under the
        control of the code that runs the service. They are set
        afterwards, in finalize().

        :param start: The time that the service should be considered to
           have started running.
        :param finish: The time that the service should be considered
           to have stopped running.
        :param achievements: A string describing what was achieved by the
           service.
        :param counter: A single integer item of state representing the point
           at which the service left off.
        :param exception: A traceback representing an exception that stopped
           the progress of the service.
        """

        # These are set by finalize().
        self.service: str | None = None
        self.service_type: str | None = None
        self.collection_id: int | None = None

        self.start = start
        self.finish = finish
        self.achievements = achievements
        self.counter = counter
        self.exception = exception

    @property
    def is_failure(self) -> bool:
        """Does this TimestampData represent an unrecoverable failure?"""
        return self.exception not in (None, SentinelType.ClearValue)

    @property
    def is_complete(self) -> bool:
        """Does this TimestampData represent an operation that has
        completed?

        An operation is completed if it has failed, or if the time of its
        completion is known.
        """
        return self.is_failure or self.finish not in (None, SentinelType.ClearValue)

    def finalize(
        self,
        service: str,
        service_type: str,
        collection: Collection | None,
        start: datetime.datetime | None = None,
        finish: datetime.datetime | None = None,
        counter: int | None = None,
        exception: str | None = None,
    ) -> None:
        """Finalize any values that were not set during the constructor.

        This is intended to be run by the code that originally ran the
        service.

        The given values for `start`, `finish`,
        `counter`, and `exception` will be used only if the service
        did not specify its own values for those fields.
        """
        self.service = service
        self.service_type = service_type
        if collection is None:
            self.collection_id = None
        else:
            self.collection_id = collection.id
        if self.start is None:
            self.start = start
        if self.finish is None:
            if finish is None:
                finish = utc_now()
            self.finish = finish
        if self.start is None:
            self.start = self.finish
        if self.counter is None:
            self.counter = counter
        if self.exception is None:
            self.exception = exception

    def collection(self, _db: Session) -> Collection | None:
        return get_one(_db, Collection, id=self.collection_id)

    def apply(self, _db: Session) -> Timestamp:
        if self.service is None or self.service_type is None:
            raise ValueError(
                "Not enough information to write TimestampData to the database."
            )

        return Timestamp.stamp(
            _db,
            self.service,
            self.service_type,
            self.collection(_db),
            self.start,
            self.finish,
            self.achievements,
            self.counter,
            self.exception,
        )
