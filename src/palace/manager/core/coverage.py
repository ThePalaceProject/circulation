import logging
import traceback

from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import func

from palace.manager.core.monitor import TimestampData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.service.container import container_instance
from palace.manager.sqlalchemy.model.collection import Collection, CollectionMissing
from palace.manager.sqlalchemy.model.coverage import (
    BaseCoverageRecord,
    CoverageRecord,
    EquivalencyCoverageRecord,
    Timestamp,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now


class CoverageFailure:
    """Object representing the failure to provide coverage."""

    def __init__(
        self, obj, exception, data_source=None, transient=True, collection=None
    ):
        self.obj = obj
        self.data_source = data_source
        self.exception = exception
        self.transient = transient
        self.collection = collection

    def __repr__(self):
        if self.data_source:
            data_source = self.data_source.name
        else:
            data_source = None
        return "<CoverageFailure: obj={!r} data_source={!r} transient={!r} exception={!r}>".format(
            self.obj,
            data_source,
            self.transient,
            self.exception,
        )

    def to_coverage_record(self, operation=None):
        """Convert this failure into a CoverageRecord."""
        if not self.data_source:
            raise Exception(
                "Cannot convert coverage failure to CoverageRecord because it has no output source."
            )

        record, ignore = CoverageRecord.add_for(
            self.obj, self.data_source, operation=operation, collection=self.collection
        )
        record.exception = self.exception
        if self.transient:
            record.status = CoverageRecord.TRANSIENT_FAILURE
        else:
            record.status = CoverageRecord.PERSISTENT_FAILURE
        return record

    def to_equivalency_coverage_record(
        self, operation: str
    ) -> EquivalencyCoverageRecord:
        """Convert this failure into a EquivalencyCoverageRecord."""
        record, ignore = EquivalencyCoverageRecord.add_for(
            self.obj, operation=operation
        )
        record.exception = self.exception
        if self.transient:
            record.status = CoverageRecord.TRANSIENT_FAILURE
        else:
            record.status = CoverageRecord.PERSISTENT_FAILURE
        return record


class CoverageProviderProgress(TimestampData):
    """A TimestampData optimized for the special needs of
    CoverageProviders.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # The offset is distinct from the counter, in that it's not written
        # to the database -- it's used to track state that's necessary within
        # a single run of the CoverageProvider.
        self.offset = 0

        self.successes = 0
        self.transient_failures = 0
        self.persistent_failures = 0

    @property
    def achievements(self):
        """Represent the achievements of a CoverageProvider as a
        human-readable string.
        """
        template = "Items processed: %d. Successes: %d, transient failures: %d, persistent failures: %d"
        total = self.successes + self.transient_failures + self.persistent_failures
        return template % (
            total,
            self.successes,
            self.transient_failures,
            self.persistent_failures,
        )

    @achievements.setter
    def achievements(self, value):
        # It's not possible to set .achievements directly. Do nothing.
        pass


class BaseCoverageProvider:
    """Run certain objects through an algorithm. If the algorithm returns
    success, add a coverage record for that object, so the object
    doesn't need to be processed again. If the algorithm returns a
    CoverageFailure, that failure may itself be memorialized as a
    coverage record.

    Instead of instantiating this class directly, subclass one of its
    subclasses, e.g. IdentifierCoverageProvider.

    In IdentifierCoverageProvider the 'objects' are Identifier objects
    and the coverage records are CoverageRecord objects.
    """

    # In your subclass, set this to the name of the service,
    # e.g. "Overdrive Bibliographic Coverage Provider".
    SERVICE_NAME: str | None = None

    # In your subclass, you _may_ set this to a string that distinguishes
    # two different CoverageProviders from the same data source.
    # (You may also override the operation method, if you need
    # database access to determine which operation to use.)
    OPERATION: str | None = None

    # The database session will be committed each time the
    # BaseCoverageProvider has (attempted to) provide coverage to this
    # number of Identifiers. You may change this in your subclass.
    # It's also possible to change it by passing in a value for
    # `batch_size` in the constructor, but generally nobody bothers
    # doing this.
    DEFAULT_BATCH_SIZE: int = 100

    def __init__(
        self,
        _db,
        batch_size=None,
        cutoff_time=None,
        registered_only=False,
    ):
        """Constructor.

        :param batch_size: The maximum number of objects that will be processed
        at once.

        :param cutoff_time: Coverage records created before this time
        will be treated as though they did not exist.

        :param registered_only: Optional. Determines whether this
        CoverageProvider will only cover items that already have been
        "preregistered" with a CoverageRecord with a registered or failing
        status. This option is only used on the Metadata Wrangler.
        """
        self._db = _db
        if not self.__class__.SERVICE_NAME:
            raise ValueError("%s must define SERVICE_NAME." % self.__class__.__name__)
        service_name = self.__class__.SERVICE_NAME
        operation = self.operation
        if operation:
            service_name += " (%s)" % operation
        self.service_name = service_name
        if not batch_size or batch_size < 0:
            batch_size = self.DEFAULT_BATCH_SIZE
        self.batch_size = batch_size
        self.cutoff_time = cutoff_time
        self.registered_only = registered_only
        self.collection_id = None

        # Call init_resources() to initialize the logging configuration.
        self.services = container_instance()
        self.services.init_resources()

    @property
    def log(self):
        if not hasattr(self, "_log"):
            self._log = logging.getLogger(self.service_name)
        return self._log

    @property
    def collection(self):
        """Retrieve the Collection object associated with this
        CoverageProvider.
        """
        if not self.collection_id:
            return None
        return get_one(self._db, Collection, id=self.collection_id)

    @property
    def operation(self):
        """Which operation should this CoverageProvider use to
        distinguish between multiple CoverageRecords from the same data
        source?
        """
        return self.OPERATION

    def run(self):
        start = utc_now()
        result = self.run_once_and_update_timestamp()
        result = result or CoverageProviderProgress()
        self.finalize_timestampdata(result, start=start)
        return result

    def run_once_and_update_timestamp(self):
        # First prioritize items that have never had a coverage attempt before.
        # Then cover items that failed with a transient failure on a
        # previous attempt.
        covered_status_lists = [
            BaseCoverageRecord.PREVIOUSLY_ATTEMPTED,
            BaseCoverageRecord.DEFAULT_COUNT_AS_COVERED,
        ]
        start_time = utc_now()
        timestamp = self.timestamp

        # We'll use this TimestampData object to track our progress
        # as we grant coverage to items.
        progress = CoverageProviderProgress(start=start_time)

        for covered_statuses in covered_status_lists:
            # We may have completed our work for the previous value of
            # covered_statuses, but there's more work to do. Unset the
            # 'finish' date to guarantee that progress.is_complete
            # starts out False.
            #
            # Also set the offset to zero to ensure that we always start
            # at the start of the database table.
            original_finish = progress.finish = None
            progress.offset = 0

            # Call run_once() until we get an exception or
            # progress.finish is set.
            while not progress.is_complete:
                try:
                    new_progress = self.run_once(
                        progress, count_as_covered=covered_statuses
                    )
                    # run_once can either return a new
                    # CoverageProviderProgress object, or modify
                    # in-place the one it was passed.
                    if new_progress is not None:
                        progress = new_progress
                except Exception as e:
                    logging.error(
                        "CoverageProvider %s raised uncaught exception.",
                        self.service_name,
                        exc_info=e,
                    )
                    progress.exception = traceback.format_exc()
                    progress.finish = utc_now()

                # The next run_once() call might raise an exception,
                # so let's write the work to the database as it's
                # done.
                original_finish = progress.finish
                self.finalize_timestampdata(progress)

                # That wrote a value for progress.finish to the
                # database, which is fine, but we don't necessarily
                # want that value for progress.finish to stand. It
                # might incorrectly make progress.is_complete appear
                # to be True, making us exit the loop before we mean
                # to.
                if not progress.exception:
                    progress.finish = original_finish

        # TODO: We should be able to return a list of progress objects,
        # not just one.
        return progress

    @property
    def timestamp(self):
        """Look up the Timestamp object for this CoverageProvider."""
        return Timestamp.lookup(
            self._db,
            self.service_name,
            Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=self.collection,
        )

    def finalize_timestampdata(self, timestamp, **kwargs):
        """Finalize the given TimestampData and write it to the
        database.
        """
        timestamp.finalize(
            self.service_name,
            Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=self.collection,
            **kwargs
        )
        timestamp.apply(self._db)
        self._db.commit()

    def run_once(self, progress, count_as_covered=None):
        """Try to grant coverage to a number of uncovered items.

        NOTE: If you override this method, it's very important that
        your implementation eventually do one of the following:
        * Set progress.finish
        * Set progress.exception
        * Raise an exception

        If you don't do any of these things, run() will assume you still
        have work to do, and will keep calling run_once() forever.

        :param progress: A CoverageProviderProgress representing the
           progress made so far, and the number of records that
           need to be ignored for the rest of the run.

        :param count_as_covered: Which values for CoverageRecord.status
           should count as meaning 'already covered'.

        :return: A CoverageProviderProgress representing whatever
           additional progress has been made.
        """
        count_as_covered = (
            count_as_covered or BaseCoverageRecord.DEFAULT_COUNT_AS_COVERED
        )
        # Make it clear which class of items we're covering on this
        # run.
        count_as_covered_message = " (counting %s as covered)" % (
            ", ".join(count_as_covered)
        )

        qu = self.items_that_need_coverage(count_as_covered=count_as_covered)

        # Running this statement only ONCE as it adds an unbounded query, very bad for the DB cluster
        if progress.successes == 0:
            self.log.info(
                "%d items need coverage%s", qu.count(), count_as_covered_message
            )
        else:
            self.log.info(
                "Covering items after %d successes: %s",
                progress.successes,
                count_as_covered_message,
            )

        qu = qu.offset(progress.offset)
        batch = qu.limit(self.batch_size)
        batch_results = batch.all()
        batch_count = len(batch_results)

        if not batch_count:
            # The batch is empty. We're done.
            progress.finish = utc_now()
            return progress

        (
            successes,
            transient_failures,
            persistent_failures,
        ), results = self.process_batch_and_handle_results(batch_results)
        self.finalize_batch()
        # Update the running totals so that the service's eventual timestamp
        # will have a useful .achievements.
        progress.successes += successes
        progress.transient_failures += transient_failures
        progress.persistent_failures += persistent_failures

        if BaseCoverageRecord.SUCCESS not in count_as_covered:
            # If any successes happened in this batch, increase the
            # offset to ignore them, or they will just show up again
            # the next time we run this batch.
            progress.offset += successes

        if BaseCoverageRecord.TRANSIENT_FAILURE not in count_as_covered:
            # If any transient failures happened in this batch,
            # increase the offset to ignore them, or they will
            # just show up again the next time we run this batch.
            progress.offset += transient_failures

        if BaseCoverageRecord.PERSISTENT_FAILURE not in count_as_covered:
            # If any persistent failures happened in this batch,
            # increase the offset to ignore them, or they will
            # just show up again the next time we run this batch.
            progress.offset += persistent_failures

        return progress

    def process_batch_and_handle_results(self, batch):
        """:return: A 2-tuple (counts, records).

        `counts` is a 3-tuple (successes, transient failures,
        persistent_failures).

        `records` is a mixed list of coverage record objects (for
        successes and persistent failures) and CoverageFailure objects
        (for transient failures).
        """

        # Batch is a query that may not be ordered, so it may return
        # different results when executed multiple times. Converting to
        # a list ensures that all subsequent code will run on the same items.
        batch = list(batch)

        offset_increment = 0
        results = self.process_batch(batch)
        successes = 0
        transient_failures = 0
        persistent_failures = 0
        num_ignored = 0
        records = []

        unhandled_items = set(batch)
        success_items = []
        for item in results:
            if isinstance(item, CoverageFailure):
                if item.obj in unhandled_items:
                    unhandled_items.remove(item.obj)
                record = self.record_failure_as_coverage_record(item)
                if item.transient:
                    self.log.warning(
                        "Transient failure covering %r: %s", item.obj, item.exception
                    )
                    record.status = BaseCoverageRecord.TRANSIENT_FAILURE
                    transient_failures += 1
                else:
                    self.log.error(
                        "Persistent failure covering %r: %s", item.obj, item.exception
                    )
                    record.status = BaseCoverageRecord.PERSISTENT_FAILURE
                    persistent_failures += 1
                records.append(record)
            else:
                # Count this as a success and prepare to add a
                # coverage record for it. It won't show up anymore, on
                # this run or subsequent runs.
                if item in unhandled_items:
                    unhandled_items.remove(item)
                successes += 1
                success_items.append(item)

        records.extend(self.add_coverage_records_for(success_items))

        # Perhaps some records were ignored--they neither succeeded nor
        # failed. Treat them as transient failures.
        for item in unhandled_items:
            self.log.warning(
                "%r was ignored by a coverage provider that was supposed to cover it.",
                item,
            )
            failure = self.failure_for_ignored_item(item)
            record = self.record_failure_as_coverage_record(failure)
            record.status = BaseCoverageRecord.TRANSIENT_FAILURE
            records.append(record)
            num_ignored += 1

        self.log.info(
            "Batch processed with %d successes, %d transient failures, %d persistent failures, %d ignored.",
            successes,
            transient_failures,
            persistent_failures,
            num_ignored,
        )

        # For all purposes outside this method, treat an ignored identifier
        # as a transient failure.
        transient_failures += num_ignored

        return (successes, transient_failures, persistent_failures), records

    def process_batch(self, batch):
        """Do what it takes to give coverage records to a batch of
        items.

        :return: A mixed list of coverage records and CoverageFailures.
        """
        results = []
        for item in batch:
            result = self.process_item(item)
            if not isinstance(result, CoverageFailure):
                self.handle_success(item)
            results.append(result)
        return results

    def add_coverage_records_for(self, items):
        """Add CoverageRecords for a group of items from a batch,
        each of which was successful.
        """
        return [self.add_coverage_record_for(item) for item in items]

    def handle_success(self, item):
        """Do something special to mark the successful coverage of the
        given item.
        """

    def should_update(self, coverage_record):
        """Should we do the work to update the given coverage record?"""
        if coverage_record is None:
            # An easy decision -- there is no existing coverage record,
            # so we need to do the work.
            return True

        if coverage_record.status == BaseCoverageRecord.REGISTERED:
            # There's a CoverageRecord, but coverage hasn't actually
            # been attempted. Try to get covered.
            return True

        if self.cutoff_time is None:
            # An easy decision -- without a cutoff_time, once we
            # create a coverage record we never update it.
            return False

        # We update a coverage record if it was last updated before
        # cutoff_time.
        return coverage_record.timestamp < self.cutoff_time

    def finalize_batch(self):
        """Do whatever is necessary to complete this batch before moving on to
        the next one.

        e.g. committing the database session or uploading a bunch of
        assets to S3.
        """
        self._db.commit()

    #
    # Subclasses must implement these virtual methods.
    #

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Create a database query returning only those items that
        need coverage.

        :param subset: A list of Identifier objects. If present, return
            only items that need coverage *and* are associated with one
            of these identifiers.

        Implemented in CoverageProvider.
        """
        raise NotImplementedError()

    def add_coverage_record_for(self, item):
        """Add a coverage record for the given item.

        Implemented in IdentifierCoverageProvider
        """
        raise NotImplementedError()

    def record_failure_as_coverage_record(self, failure):
        """Convert the given CoverageFailure to a coverage record.

        Implemented in IdentifierCoverageProvider
        """
        raise NotImplementedError()

    def failure_for_ignored_item(self, work):
        """Create a CoverageFailure recording the coverage provider's
        failure to even try to process an item.

        Implemented in IdentifierCoverageProvider.
        """
        raise NotImplementedError()

    def process_item(self, item):
        """Do the work necessary to give coverage to one specific item.

        Since this is where the actual work happens, this is not
        implemented in IdentifierCoverageProvider, and must be handled in a subclass.
        """
        raise NotImplementedError()


class IdentifierCoverageProvider(BaseCoverageProvider):
    """Run Identifiers of certain types (ISBN, Overdrive, OCLC Number,
    etc.) through an algorithm associated with a certain DataSource.

    This class is designed to be subclassed rather than instantiated
    directly. Subclasses should define SERVICE_NAME, OPERATION
    (optional), DATA_SOURCE_NAME, and
    INPUT_IDENTIFIER_TYPES. SERVICE_NAME and OPERATION are described
    in BaseCoverageProvider; the rest are described in appropriate
    comments in this class.
    """

    # In your subclass, set this to the name of the data source you
    # consult when providing coverage, e.g. DataSource.OVERDRIVE.
    DATA_SOURCE_NAME: str

    # In your subclass, set this to a single identifier type, or a list
    # of identifier types. The CoverageProvider will attempt to give
    # coverage to every Identifier of this type.
    #
    # Setting this to None will attempt to give coverage to every single
    # Identifier in the system, which is probably not what you want.
    NO_SPECIFIED_TYPES = object()
    INPUT_IDENTIFIER_TYPES: None | str | object = NO_SPECIFIED_TYPES

    # Set this to False if a given Identifier needs to be run through
    # this CoverageProvider once for every Collection that has this
    # Identifier in its catalog. If this is set to True, a given
    # Identifier will be considered completely covered the first time
    # it's run through this CoverageProvider, no matter how many
    # Collections the Identifier belongs to.
    COVERAGE_COUNTS_FOR_EVERY_COLLECTION = True

    def __init__(
        self,
        _db,
        collection=None,
        input_identifiers=None,
        replacement_policy=None,
        **kwargs
    ):
        """Constructor.

        :param collection: Optional. If information comes in from a
           third party about a license pool associated with an
           Identifier, the LicensePool that belongs to this Collection
           will be used to contain that data. You may pass in None for
           this value, but that means that no circulation information
           (such as the formats in which a book is available) will be
           stored as a result of running this CoverageProvider. Only
           bibliographic information will be stored.
        :param input_identifiers: Optional. This CoverageProvider is
           requested to provide coverage for these specific
           Identifiers.
        :param replacement_policy: Optional. A ReplacementPolicy to use
           when updating local data with data from the third party.
        """
        super().__init__(_db, **kwargs)

        # We store the collection ID rather than the Collection to
        # avoid breakage if an app server with a scoped session ever
        # uses a IdentifierCoverageProvider.
        self.collection_id = None
        if collection:
            self.collection_id = collection.id
        self.input_identifiers = input_identifiers
        self.replacement_policy = (
            replacement_policy or self._default_replacement_policy(_db)
        )

        if not self.DATA_SOURCE_NAME:
            raise ValueError(
                "%s must define DATA_SOURCE_NAME" % self.__class__.__name__
            )

        # Get this information immediately so that an error happens immediately
        # if INPUT_IDENTIFIER_TYPES is not set properly.
        self.input_identifier_types = self._input_identifier_types()

    def _default_replacement_policy(self, _db):
        """Unless told otherwise, assume that we are getting
        this data from a reliable metadata source.
        """
        return ReplacementPolicy.from_metadata_source()

    @property
    def collection_or_not(self):
        """If this CoverageProvider needs to be run multiple times on
        the same identifier in different collections, this
        returns the collection. Otherwise, this returns None.
        """
        if self.COVERAGE_COUNTS_FOR_EVERY_COLLECTION:
            return None
        return self.collection

    @classmethod
    def _input_identifier_types(cls):
        """Create a normalized value for `input_identifier_types`
        based on the INPUT_IDENTIFIER_TYPES class variable.
        """
        value = cls.INPUT_IDENTIFIER_TYPES

        # Nip in the bud a situation where someone subclassed this
        # class without thinking about a value for
        # INPUT_IDENTIFIER_TYPES.
        if value is cls.NO_SPECIFIED_TYPES:
            raise ValueError(
                "%s must define INPUT_IDENTIFIER_TYPES, even if the value is None."
                % (cls.__name__)
            )

        if not value:
            # We will be processing every single type of identifier in
            # the system. This (hopefully) means that the identifiers
            # are restricted in some other way, such as being licensed
            # to a specific Collection.
            return None
        elif not isinstance(value, list):
            # We will be processing every identifier of a given type.
            return [value]
        else:
            # We will be processing every identify whose type belongs to
            # a list of types.
            return value

    @classmethod
    def register(
        cls,
        identifier,
        data_source=None,
        collection=None,
        force=False,
        autocreate=False,
    ):
        """Registers an identifier for future coverage.

        See `CoverageProvider.bulk_register` for more information about using
        this method.
        """
        name = cls.SERVICE_NAME or cls.__name__
        log = logging.getLogger(name)

        new_records, ignored_identifiers = cls.bulk_register(
            [identifier],
            data_source=data_source,
            collection=collection,
            force=force,
            autocreate=autocreate,
        )
        was_registered = identifier not in ignored_identifiers

        new_record = None
        if new_records:
            [new_record] = new_records

        if was_registered and new_record:
            log.info("CREATED %r" % new_record)
            return new_record, was_registered

        _db = Session.object_session(identifier)
        data_source = cls._data_source_for_registration(
            _db, data_source, autocreate=autocreate
        )

        if collection and cls.COVERAGE_COUNTS_FOR_EVERY_COLLECTION:
            # There's no need for a collection when registering this
            # Identifier, even if it provided the DataSource.
            collection = None

        existing_record = CoverageRecord.lookup(
            identifier, data_source, cls.OPERATION, collection=collection
        )
        log.info("FOUND %r" % existing_record)
        return existing_record, was_registered

    @classmethod
    def bulk_register(
        cls,
        identifiers,
        data_source=None,
        collection=None,
        force=False,
        autocreate=False,
    ):
        """Registers identifiers for future coverage.

        This method is primarily for use with CoverageProviders that use the
        `registered_only` flag to process items. It's currently only in use
        on the Metadata Wrangler.

        :param data_source: DataSource object or basestring representing a
            DataSource name.
        :param collection: Collection object to be associated with the
            CoverageRecords.
        :param force: When True, even existing CoverageRecords will have
            their status reset to CoverageRecord.REGISTERED.
        :param autocreate: When True, a basestring provided by data_source will
            be autocreated in the database if it didn't previously exist.

        :return: A tuple of two lists: the first has fresh new REGISTERED
            CoverageRecords and the second list already has Identifiers that
            were ignored because they already had coverage.

        TODO: Take identifier eligibility into account when registering.
        """
        if not identifiers:
            return list(), list()

        _db = Session.object_session(identifiers[0])
        data_source = cls._data_source_for_registration(
            _db, data_source, autocreate=autocreate
        )

        if collection and cls.COVERAGE_COUNTS_FOR_EVERY_COLLECTION:
            # There's no need for a collection on this CoverageRecord.
            collection = None

        new_records, ignored_identifiers = CoverageRecord.bulk_add(
            identifiers,
            data_source,
            operation=cls.OPERATION,
            status=CoverageRecord.REGISTERED,
            collection=collection,
            force=force,
        )

        return new_records, ignored_identifiers

    @classmethod
    def _data_source_for_registration(cls, _db, data_source, autocreate=False):
        """Finds or creates a DataSource for the registration methods
        `cls.register` and `cls.bulk_register`.
        """
        if not data_source:
            return DataSource.lookup(_db, cls.DATA_SOURCE_NAME)
        if isinstance(data_source, DataSource):
            return data_source
        if isinstance(data_source, (bytes, str)):
            return DataSource.lookup(_db, data_source, autocreate=autocreate)

    @property
    def data_source(self):
        """Look up the DataSource object corresponding to the
        service we're running this data through.

        Out of an excess of caution, we look up the DataSource every
        time, rather than storing it, in case a CoverageProvider is
        ever used in an environment where the database session is
        scoped (e.g. the circulation manager).
        """
        return DataSource.lookup(self._db, self.DATA_SOURCE_NAME)

    def failure(self, identifier, error, transient=True):
        """Create a CoverageFailure object to memorialize an error."""
        return CoverageFailure(
            identifier,
            error,
            data_source=self.data_source,
            transient=transient,
            collection=self.collection_or_not,
        )

    def can_cover(self, identifier):
        """Can this IdentifierCoverageProvider do anything with the given
        Identifier?

        This is not needed in the normal course of events, but a
        caller may need to decide whether to pass an Identifier
        into ensure_coverage() or register().
        """
        return (
            not self.input_identifier_types
            or identifier.type in self.input_identifier_types
        )

    def run_on_specific_identifiers(self, identifiers):
        """Split a specific set of Identifiers into batches and process one
        batch at a time.

        This is for use by IdentifierInputScript.

        :return: The same (counts, records) 2-tuple as
            process_batch_and_handle_results.
        """
        index = 0
        successes = 0
        transient_failures = 0
        persistent_failures = 0
        records = []

        # Of all the items that need coverage, find the intersection
        # with the given list of items.
        need_coverage = self.items_that_need_coverage(identifiers).all()

        # Treat any items with up-to-date coverage records as
        # automatic successes.
        #
        # NOTE: We won't actually be returning those coverage records
        # in `records`, since items_that_need_coverage() filters them
        # out, but nobody who calls this method really needs those
        # records.
        automatic_successes = len(identifiers) - len(need_coverage)
        successes += automatic_successes
        self.log.info("%d automatic successes.", successes)

        # Iterate over any items that were not automatic
        # successes.
        while index < len(need_coverage):
            batch = need_coverage[index : index + self.batch_size]
            (s, t, p), r = self.process_batch_and_handle_results(batch)
            self.finalize_batch()
            successes += s
            transient_failures += t
            persistent_failures += p
            records += r
            index += self.batch_size
        return (successes, transient_failures, persistent_failures), records

    def ensure_coverage(self, item, force=False):
        """Ensure coverage for one specific item.

        :param item: This should always be an Identifier, but this
            code will also work if it's an Edition. (The Edition's
            .primary_identifier will be covered.)
        :param force: Run the coverage code even if an existing
            coverage record for this item was created after `self.cutoff_time`.
        :return: Either a coverage record or a CoverageFailure.

        TODO: This could be abstracted and moved to BaseCoverageProvider.

        """
        if isinstance(item, Identifier):
            identifier = item
        else:
            identifier = item.primary_identifier

        if self.COVERAGE_COUNTS_FOR_EVERY_COLLECTION:
            # We need to cover this Identifier once, and then we're
            # done, for all collections.
            collection = None
        else:
            # We need separate coverage for the specific Collection
            # associated with this CoverageProvider.
            collection = self.collection

        coverage_record = get_one(
            self._db,
            CoverageRecord,
            identifier=identifier,
            collection=collection,
            data_source=self.data_source,
            operation=self.operation,
            on_multiple="interchangeable",
        )
        if not force and not self.should_update(coverage_record):
            return coverage_record

        counts, records = self.process_batch_and_handle_results([identifier])
        if records:
            coverage_record = records[0]
        else:
            coverage_record = None
        return coverage_record

    def edition(self, identifier):
        """Finds or creates an Edition representing this coverage provider's
        view of a given Identifier.
        """
        edition, ignore = Edition.for_foreign_id(
            self._db, self.data_source, identifier.type, identifier.identifier
        )
        return edition

    def set_bibliographic(self, identifier, bibliographic):
        """Finds or creates the Edition for an Identifier, updates it
        with the given bibliographic data.

        :return: The Identifier (if successful) or an appropriate
            CoverageFailure (if not).
        """
        edition = self.edition(identifier)
        if isinstance(edition, CoverageFailure):
            return edition

        if not bibliographic:
            e = "Did not receive bibliographic data from input source"
            return self.failure(identifier, e, transient=True)

        try:
            # We're passing in the Collection even if this
            # CoverageProvider has
            # COVERAGE_COUNTS_FOR_EVERY_COLLECTION set to False. If
            # we did happen to get some circulation information while
            # we were at it, we might as well store it properly.
            # The data layer will not use the collection when creating
            # CoverageRecords for the bibliographic actions.
            bibliographic.apply(
                self._db,
                edition,
                collection=self.collection,
                replace=self.replacement_policy,
            )
        except Exception as e:
            self.log.warning(
                "Error applying bibliographic data to edition %d: %s",
                edition.id,
                e,
                exc_info=e,
            )
            return self.failure(identifier, repr(e), transient=True)

        return identifier

    #
    # Implementation of BaseCoverageProvider virtual methods.
    #

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Find all items lacking coverage from this CoverageProvider.

        Items should be Identifiers, though Editions should also work.

        By default, all identifiers of the `INPUT_IDENTIFIER_TYPES` which
        don't already have coverage are chosen.

        :param identifiers: The batch of identifier objects to test
            for coverage. identifiers and self.input_identifiers can
            intersect -- if this provider was created for the purpose
            of running specific Identifiers, and within those
            Identifiers you want to batch, you can use both
            parameters.
        """
        qu = Identifier.missing_coverage_from(
            self._db,
            self.input_identifier_types,
            self.data_source,
            count_as_missing_before=self.cutoff_time,
            operation=self.operation,
            identifiers=self.input_identifiers,
            collection=self.collection_or_not,
            **kwargs
        )

        if identifiers:
            qu = qu.filter(Identifier.id.in_([x.id for x in identifiers]))
        if not identifiers and identifiers != None:
            # An empty list was provided. The returned query should be empty.
            qu = qu.filter(Identifier.id == None)

        if self.registered_only:
            # Return Identifiers that have been "registered" for coverage
            # or already have a failure from previous coverage attempts.
            qu = qu.filter(CoverageRecord.id != None)

        return qu

    def add_coverage_record_for(self, item):
        """Record this CoverageProvider's coverage for the given
        Edition/Identifier, as a CoverageRecord.
        """
        record, is_new = CoverageRecord.add_for(
            item,
            data_source=self.data_source,
            operation=self.operation,
            collection=self.collection_or_not,
        )
        record.status = CoverageRecord.SUCCESS
        record.exception = None
        return record

    def record_failure_as_coverage_record(self, failure):
        """Turn a CoverageFailure into a CoverageRecord object."""
        return failure.to_coverage_record(operation=self.operation)

    def failure_for_ignored_item(self, item):
        """Create a CoverageFailure recording the CoverageProvider's
        failure to even try to process an item.
        """
        return self.failure(item, "Was ignored by CoverageProvider.", transient=True)


class CollectionCoverageProvider(IdentifierCoverageProvider):
    """A CoverageProvider that covers all the Identifiers currently
    licensed to a given Collection.

    You should subclass this CoverageProvider if you want to create
    Works (as opposed to operating on existing Works) or update the
    circulation information for LicensePools. You can't use it to
    create new LicensePools, since it only operates on Identifiers
    that already have a LicencePool in the given Collection.

    If a book shows up in multiple Collections, the first Collection
    to process it takes care of it for the others. Any books that were
    processed through their membership in another Collection will be
    left alone.

    For this reason it's important that subclasses of this
    CoverageProvider only deal with bibliographic information and
    format availability information (such as links to open-access
    downloads). You'll have problems if you try to use
    CollectionCoverageProvider to keep track of information like the
    number of licenses available for a book.

    In addition to defining the class variables defined by
    CoverageProvider, you must define the class variable PROTOCOL when
    subclassing this class. This is the entity that provides the
    licenses for this Collection. It should be a valid protocol
    in the LICENSE_GOAL registry.
    """

    # By default, this type of CoverageProvider will provide coverage to
    # all Identifiers in the given Collection, regardless of their type.
    INPUT_IDENTIFIER_TYPES: None | str | object = None

    DEFAULT_BATCH_SIZE = 10

    # Set this to the name of the protocol managed by this type of
    # CoverageProvider. If this CoverageProvider can manage collections
    # for any protocol, leave this as None.
    PROTOCOL: str | None = None

    # By default, Works calculated by a CollectionCoverageProvider update
    # the ExternalSearchIndex. Set this value to True for applications that
    # don't use external search, such as the Metadata Wrangler.
    EXCLUDE_SEARCH_INDEX = False

    def __init__(self, collection, **kwargs):
        """Constructor.

        :param collection: Will provide coverage to all Identifiers with
            a LicensePool licensed to the given Collection.
        """
        if not isinstance(collection, Collection):
            raise CollectionMissing(
                "%s must be instantiated with a Collection." % (self.__class__.__name__)
            )

        if self.PROTOCOL and collection.protocol != self.PROTOCOL:
            raise ValueError(
                "Collection protocol (%s) does not match CoverageProvider protocol (%s)"
                % (collection.protocol, self.PROTOCOL)
            )
        _db = Session.object_session(collection)
        super().__init__(_db, collection, **kwargs)

    def _default_replacement_policy(self, _db):
        """Unless told otherwise, assume that we are getting
        this data from a reliable source of both bibliographic and circulation
        information.
        """
        return ReplacementPolicy.from_license_source(_db)

    @classmethod
    def collections(cls, _db):
        """Returns a list of randomly sorted list of collections covered by the
        provider.
        """
        if cls.PROTOCOL:
            collections = Collection.by_protocol(_db, cls.PROTOCOL)
        else:
            collections = _db.query(Collection)
        return collections.order_by(func.random()).all()

    @classmethod
    def all(cls, _db, **kwargs):
        """Yield a sequence of CollectionCoverageProvider instances, one for
        every Collection that gets its licenses from cls.PROTOCOL.

        CollectionCoverageProviders will be yielded in a random order.

        :param kwargs: Keyword arguments passed into the constructor for
            CollectionCoverageProvider (or, more likely, one of its subclasses).

        """
        for collection in cls.collections(_db):
            yield cls(collection, **kwargs)

    def run_once(self, *args, **kwargs):
        self.log.info("Considering collection %s", self.collection.name)
        return super().run_once(*args, **kwargs)

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Find all Identifiers associated with this Collection but lacking
        coverage through this CoverageProvider.
        """
        qu = super().items_that_need_coverage(identifiers, **kwargs)
        qu = qu.join(Identifier.licensed_through).filter(
            LicensePool.collection_id == self.collection_id
        )
        return qu

    def license_pool(self, identifier, data_source=None):
        """Finds this Collection's LicensePool for the given Identifier,
        creating one if necessary.

        :param data_source: If it's necessary to create a LicensePool,
            the new LicensePool will have this DataSource. The default is to
            use the DataSource associated with the CoverageProvider. This
            should only be needed by the metadata wrangler.
        """
        license_pools = [
            p for p in identifier.licensed_through if self.collection == p.collection
        ]

        if license_pools:
            # A given Collection may have at most one LicensePool for
            # a given identifier.
            return license_pools[0]

        data_source = data_source or self.data_source
        if isinstance(data_source, (bytes, str)):
            data_source = DataSource.lookup(self._db, data_source)

        # This Collection has no LicensePool for the given Identifier.
        # Create one.
        #
        # Under normal circumstances, this will never happen, because
        # CollectionCoverageProvider only operates on Identifiers that
        # already have a LicensePool in this Collection.
        #
        # However, this does happen in the metadata wrangler,
        # which typically has to manage information about books it has no
        # rights to.
        license_pool, ignore = LicensePool.for_foreign_id(
            self._db,
            data_source,
            identifier.type,
            identifier.identifier,
            collection=self.collection,
        )
        return license_pool

    def work(self, identifier, license_pool=None, **calculate_work_kwargs):
        """Finds or creates a Work for this Identifier as licensed through
        this Collection.

        If the given Identifier already has a Work associated with it,
        that Work will always be used, since an Identifier can only have one
        Work associated with it.

        However, if there is no current Work, a Work will only be
        created if the given Identifier already has a LicensePool in
        the Collection associated with this CoverageProvider (or if a
        LicensePool to use is provided.) This method will not create
        new LicensePools.

        If the work is newly created or an existing work is not
        presentation-ready, a new Work will be created by calling
        LicensePool.calculate_work(). If there is an existing
        presentation-ready work, calculate_work() will not be called;
        instead, the work will be slated for recalculation when its
        metadata changes through BibliographicData.apply().

        :param calculate_work_kwargs: Keyword arguments to pass into
            calculate_work() if and when it is called.

        :return: A Work, if possible. Otherwise, a CoverageFailure explaining
            why no Work could be created.

        """
        work = identifier.work
        if work and work.presentation_ready:
            # There is already a presentation-ready Work associated
            # with this Identifier.  Return it.
            return work

        # There is no presentation-ready Work associated with this
        # Identifier. We need to create one, if possible.
        error = None
        if not license_pool:
            license_pool, ignore = LicensePool.for_foreign_id(
                self._db,
                self.data_source,
                identifier.type,
                identifier.identifier,
                collection=self.collection,
                autocreate=False,
            )

        if license_pool:
            if not license_pool.work or not license_pool.work.presentation_ready:
                for v, default in (("exclude_search", self.EXCLUDE_SEARCH_INDEX),):
                    if not v in calculate_work_kwargs:
                        calculate_work_kwargs[v] = default

                # Calling calculate_work will calculate the work's
                # presentation and make it presentation-ready if
                # possible.
                work, created = license_pool.calculate_work(**calculate_work_kwargs)
            if not work:
                error = "Work could not be calculated"
        else:
            error = "Cannot locate LicensePool"

        if error:
            return self.failure(identifier, error, transient=True)
        return work

    def set_bibliographic_and_circulation_data(
        self,
        identifier,
        bibliographicdata,
        circulationdata,
    ):
        """Makes sure that the given Identifier has a Work, Edition (in the
        context of this Collection), and LicensePool (ditto), and that
        all the information is up to date.

        :return: The Identifier (if successful) or an appropriate
            CoverageFailure (if not).
        """

        if not bibliographicdata and not circulationdata:
            e = "Received neither bibliographic data nor circulation data from input source"
            return self.failure(identifier, e, transient=True)

        if bibliographicdata:
            result = self.set_bibliographic(identifier, bibliographicdata)
            if isinstance(result, CoverageFailure):
                return result

        if circulationdata:
            result = self._set_circulationdata(identifier, circulationdata)
            if isinstance(result, CoverageFailure):
                return result

        # By this point the Identifier should have an appropriate
        # Edition and LicensePool. We should now be able to make a
        # Work.
        work = self.work(identifier)
        if isinstance(work, CoverageFailure):
            return work

        return identifier

    def _set_circulationdata(self, identifier, circulationdata):
        """Finds or creates a LicensePool for an Identifier, updates it
        with the given circulationdata, then creates a Work for the book.

        :return: The Identifier (if successful) or an appropriate
        CoverageFailure (if not).
        """
        error = None
        if circulationdata:
            try:
                primary_identifier = circulationdata.load_primary_identifier(self._db)
            except ValueError:
                primary_identifier = None
            if identifier != primary_identifier:
                error = "Identifier did not match CirculationData's primary identifier."
        else:
            error = "Did not receive circulationdata from input source"

        if error:
            return self.failure(identifier, error, transient=True)

        try:
            circulationdata.apply(
                self._db, self.collection, replace=self.replacement_policy
            )
        except Exception as e:
            if self.collection:
                collection_name = " to collection %s" % self.collection.name
            else:
                collection_name = ""
            self.log.warning(
                "Error applying circulationdata%s: %s", collection_name, e, exc_info=e
            )
            return self.failure(identifier, repr(e), transient=True)

        return identifier

    def set_presentation_ready(self, identifier):
        """Set a Work presentation-ready."""
        work = self.work(identifier)
        if isinstance(work, CoverageFailure):
            return work
        work.set_presentation_ready(exclude_search=self.EXCLUDE_SEARCH_INDEX)
        return identifier


class BibliographicCoverageProvider(CollectionCoverageProvider):
    """Fill in bibliographic metadata for all books in a Collection.

    e.g. ensures that we get Overdrive coverage for all Overdrive IDs
    in a collection.

    Although a BibliographicCoverageProvider may gather
    CirculationData for a book, it cannot guarantee equal coverage for
    all Collections that contain that book. CirculationData should be
    limited to things like formats that don't vary between
    Collections, and you should use a CollectionMonitor to make sure
    your circulation information is up-to-date for each Collection.
    """

    def handle_success(self, identifier):
        """Once a book has bibliographic coverage, it can be given a
        work and made presentation ready.
        """
        self.set_presentation_ready(identifier)
