import argparse
import csv
import datetime
import json
import logging
import os
import random
import sys
import traceback
import unicodedata
import uuid
from enum import Enum
from pathlib import Path
from typing import Generator, List, Optional, Type

from sqlalchemy import and_, exists, tuple_
from sqlalchemy.orm import Query, Session, defer
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from alembic.command import downgrade, upgrade
from alembic.config import Config as AlembicConfig
from alembic.util import CommandError
from core.model.classification import Classification
from core.query.customlist import CustomListQueries

from .config import CannotLoadConfiguration, Configuration
from .coverage import CollectionCoverageProviderJob, CoverageProviderProgress
from .external_search import ExternalSearchIndex, Filter, SearchIndexCoverageProvider
from .lane import Lane
from .metadata_layer import (
    LinkData,
    MetaToModelUtility,
    ReplacementPolicy,
    TimestampData,
)
from .mirror import MirrorUploader
from .model import (
    BaseCoverageRecord,
    CachedFeed,
    Collection,
    ConfigurationSetting,
    Contributor,
    CustomList,
    DataSource,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Library,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Patron,
    PresentationCalculationPolicy,
    Representation,
    SessionManager,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
    create,
    get_one,
    get_one_or_create,
    production_session,
)
from .model.configuration import ExternalIntegrationLink
from .model.listeners import site_configuration_has_changed
from .monitor import CollectionMonitor, ReaperMonitor
from .opds_import import OPDSImporter, OPDSImportMonitor
from .overdrive import OverdriveCoreAPI
from .util import fast_query_count
from .util.datetime_helpers import strptime_utc, utc_now
from .util.personal_names import contributor_name_match_ratio, display_name_to_sort_name
from .util.worker_pools import DatabasePool


class Script:
    @property
    def _db(self) -> Session:
        if not hasattr(self, "_session"):
            self._session = production_session()
        return self._session

    @property
    def script_name(self):
        """Find or guess the name of the script.

        This is either the .name of the Script object or the name of
        the class.
        """
        return getattr(self, "name", self.__class__.__name__)

    @property
    def log(self):
        if not hasattr(self, "_log"):
            self._log = logging.getLogger(self.script_name)
        return self._log

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None):
        parser = cls.arg_parser()
        return parser.parse_known_args(cmd_args)[0]

    @classmethod
    def arg_parser(cls):
        raise NotImplementedError()

    @classmethod
    def parse_time(cls, time_string):
        """Try to pass the given string as a time."""
        if not time_string:
            return None
        for format in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
            for hours in ("", " %H:%M:%S"):
                full_format = format + hours
                try:
                    parsed = strptime_utc(time_string, full_format)
                    return parsed
                except ValueError as e:
                    continue
        raise ValueError("Could not parse time: %s" % time_string)

    def __init__(self, _db=None, *args, **kwargs):
        """Basic constructor.

        :_db: A database session to be used instead of
        creating a new one. Useful in tests.
        """
        if _db:
            self._session = _db

    def run(self):
        DataSource.well_known_sources(self._db)
        start_time = utc_now()
        try:
            timestamp_data = self.do_run()
            if not isinstance(timestamp_data, TimestampData):
                # Ignore any nonstandard return value from do_run().
                timestamp_data = None
            self.update_timestamp(timestamp_data, start_time, None)
        except Exception as e:
            logging.error("Fatal exception while running script: %s", e, exc_info=e)
            stack_trace = traceback.format_exc()
            self.update_timestamp(None, start_time, stack_trace)
            raise

    def update_timestamp(self, timestamp_data, start_time, exception):
        """By default scripts have no timestamp of their own.

        Most scripts either work through Monitors or CoverageProviders,
        which have their own logic for creating timestamps, or they
        are designed to be run interactively from the command-line, so
        facts about when they last ran are not relevant.

        :param start_time: The time the script started running.
        :param exception: A stack trace for the exception, if any,
           that stopped the script from running.
        """


class TimestampScript(Script):
    """A script that automatically records a timestamp whenever it runs."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp_collection = None

    def update_timestamp(self, timestamp_data, start, exception):
        """Update the appropriate Timestamp for this script.

        :param timestamp_data: A TimestampData representing what the script
          itself thinks its timestamp should look like. Data will be filled in
          where it is missing, but it will not be modified if present.

        :param start: The time at which this script believes the
          service started running. The script itself may change this
          value for its own purposes.

        :param exception: The exception with which this script
          believes the service stopped running. The script itself may
          change this value for its own purposes.
        """
        if timestamp_data is None:
            timestamp_data = TimestampData()
        timestamp_data.finalize(
            self.script_name,
            Timestamp.SCRIPT_TYPE,
            self.timestamp_collection,
            start=start,
            exception=exception,
        )
        timestamp_data.apply(self._db)


class RunMonitorScript(Script):
    def __init__(self, monitor, _db=None, **kwargs):
        super().__init__(_db)
        if issubclass(monitor, CollectionMonitor):
            self.collection_monitor = monitor
            self.collection_monitor_kwargs = kwargs
            self.monitor = None
            self.name = self.collection_monitor.SERVICE_NAME
        else:
            self.collection_monitor = None
            if callable(monitor):
                monitor = monitor(self._db, **kwargs)
            self.monitor = monitor
            self.name = self.monitor.service_name

    def do_run(self):
        if self.monitor:
            self.monitor.run()
        elif self.collection_monitor:
            logging.warning(
                "Running a CollectionMonitor by delegating to RunCollectionMonitorScript. "
                "It would be better if you used RunCollectionMonitorScript directly."
            )
            RunCollectionMonitorScript(
                self.collection_monitor, self._db, **self.collection_monitor_kwargs
            ).run()


class RunMultipleMonitorsScript(Script):
    """Run a number of monitors in sequence.

    Currently the Monitors are run one at a time. It should be
    possible to take a command-line argument that runs all the
    Monitors in batches, each in its own thread. Unfortunately, it's
    tough to know in a given situation that this won't overload the
    system.
    """

    def __init__(self, _db=None, **kwargs):
        """Constructor.

        :param kwargs: Keyword arguments to pass into the `monitors` method
            when building the Monitor objects.
        """
        super().__init__(_db)
        self.kwargs = kwargs

    def monitors(self, **kwargs):
        """Find all the Monitors that need to be run.

        :return: A list of Monitor objects.
        """
        raise NotImplementedError()

    def do_run(self):
        for monitor in self.monitors(**self.kwargs):
            try:
                monitor.run()
            except Exception as e:
                # This is bad, but not so bad that we should give up trying
                # to run the other Monitors.
                if monitor.collection:
                    collection_name = monitor.collection.name
                else:
                    collection_name = None
                monitor.exception = e
                self.log.error(
                    "Error running monitor %s for collection %s: %s",
                    self.name,
                    collection_name,
                    e,
                    exc_info=e,
                )


class RunReaperMonitorsScript(RunMultipleMonitorsScript):
    """Run all the monitors found in ReaperMonitor.REGISTRY"""

    name = "Run all reaper monitors"

    def monitors(self, **kwargs):
        return [cls(self._db, **kwargs) for cls in ReaperMonitor.REGISTRY]


class RunCoverageProvidersScript(Script):
    """Alternate between multiple coverage providers."""

    def __init__(self, providers, _db=None):
        super().__init__(_db=_db)
        self.providers = []
        for i in providers:
            if callable(i):
                i = i(self._db)
            self.providers.append(i)

    def do_run(self):
        providers = list(self.providers)
        if not providers:
            self.log.info("No CoverageProviders to run.")

        progress = []
        while providers:
            random.shuffle(providers)
            for provider in providers:
                self.log.debug("Running %s", provider.service_name)

                try:
                    provider_progress = provider.run_once_and_update_timestamp()
                    progress.append(provider_progress)
                except Exception as e:
                    self.log.error(
                        "Error in %r, moving on to next CoverageProvider.",
                        provider,
                        exc_info=e,
                    )

                self.log.debug("Completed %s", provider.service_name)
                providers.remove(provider)
        return progress


class RunCollectionCoverageProviderScript(RunCoverageProvidersScript):
    """Run the same CoverageProvider code for all Collections that
    get their licenses from the appropriate place.
    """

    def __init__(self, provider_class, _db=None, providers=None, **kwargs):
        _db = _db or self._db
        providers = providers or list()
        if provider_class:
            providers += self.get_providers(_db, provider_class, **kwargs)
        super().__init__(providers, _db=_db)

    def get_providers(self, _db, provider_class, **kwargs):
        return list(provider_class.all(_db, **kwargs))


class RunThreadedCollectionCoverageProviderScript(Script):
    """Run coverage providers in multiple threads."""

    DEFAULT_WORKER_SIZE = 5

    def __init__(self, provider_class, worker_size=None, _db=None, **provider_kwargs):
        super().__init__(_db)

        self.worker_size = worker_size or self.DEFAULT_WORKER_SIZE
        self.session_factory = SessionManager.sessionmaker(session=self._db)

        # Use a database from the factory.
        if not _db:
            # Close the new, autogenerated database session.
            self._session.close()
        self._session = self.session_factory()

        self.provider_class = provider_class
        self.provider_kwargs = provider_kwargs

    def run(self, pool=None):
        """Runs a CollectionCoverageProvider with multiple threads and
        updates the timestamp accordingly.

        :param pool: A DatabasePool (or other) object for use in testing
            environments.
        """
        collections = self.provider_class.collections(self._db)
        if not collections:
            return

        for collection in collections:
            provider = self.provider_class(collection, **self.provider_kwargs)
            with (
                pool or DatabasePool(self.worker_size, self.session_factory)
            ) as job_queue:
                query_size, batch_size = self.get_query_and_batch_sizes(provider)
                # Without a commit, the query to count which items need
                # coverage hangs in the database, blocking the threads.
                self._db.commit()

                offset = 0
                # TODO: We create a separate 'progress' object
                # for each job, and each will overwrite the timestamp
                # value as its complets. It woudl be better if all the
                # jobs could share a single 'progress' object.
                while offset < query_size:
                    progress = CoverageProviderProgress(start=utc_now())
                    progress.offset = offset
                    job = CollectionCoverageProviderJob(
                        collection,
                        self.provider_class,
                        progress,
                        **self.provider_kwargs,
                    )
                    job_queue.put(job)
                    offset += batch_size

    def get_query_and_batch_sizes(self, provider):
        qu = provider.items_that_need_coverage(
            count_as_covered=BaseCoverageRecord.DEFAULT_COUNT_AS_COVERED
        )
        return fast_query_count(qu), provider.batch_size


class RunWorkCoverageProviderScript(RunCollectionCoverageProviderScript):
    """Run a WorkCoverageProvider on every relevant Work in the system."""

    # This class overrides RunCollectionCoverageProviderScript just to
    # take advantage of the constructor; it doesn't actually use the
    # concept of 'collections' at all.

    def get_providers(self, _db, provider_class, **kwargs):
        return [provider_class(_db, **kwargs)]


class InputScript(Script):
    @classmethod
    def read_stdin_lines(self, stdin):
        """Read lines from a (possibly mocked, possibly empty) standard input."""
        if stdin is not sys.stdin or not os.isatty(0):
            # A file has been redirected into standard input. Grab its
            # lines.
            lines = [x.strip() for x in stdin.readlines()]
        else:
            lines = []
        return lines


class IdentifierInputScript(InputScript):
    """A script that takes identifiers as command line inputs."""

    DATABASE_ID = "Database ID"

    @classmethod
    def parse_command_line(
        cls, _db=None, cmd_args=None, stdin=sys.stdin, *args, **kwargs
    ):
        parser = cls.arg_parser()
        parsed = parser.parse_args(cmd_args)
        stdin = cls.read_stdin_lines(stdin)
        return cls.look_up_identifiers(_db, parsed, stdin, *args, **kwargs)

    @classmethod
    def look_up_identifiers(
        cls, _db, parsed, stdin_identifier_strings, *args, **kwargs
    ):
        """Turn identifiers as specified on the command line into
        real database Identifier objects.
        """
        data_source = None
        if parsed.identifier_data_source:
            data_source = DataSource.lookup(_db, parsed.identifier_data_source)
        if _db and parsed.identifier_type:
            # We can also call parse_identifier_list.
            identifier_strings = parsed.identifier_strings
            if stdin_identifier_strings:
                identifier_strings = identifier_strings + stdin_identifier_strings
            parsed.identifiers = cls.parse_identifier_list(
                _db,
                parsed.identifier_type,
                data_source,
                identifier_strings,
                *args,
                **kwargs,
            )
        else:
            # The script can call parse_identifier_list later if it
            # wants to.
            parsed.identifiers = None
        return parsed

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--identifier-type",
            help='Process identifiers of this type. If IDENTIFIER is not specified, all identifiers of this type will be processed. To name identifiers by their database ID, use --identifier-type="Database ID"',
        )
        parser.add_argument(
            "--identifier-data-source",
            help="Process only identifiers which have a LicensePool associated with this DataSource",
        )
        parser.add_argument(
            "identifier_strings",
            help="A specific identifier to process.",
            metavar="IDENTIFIER",
            nargs="*",
        )
        return parser

    @classmethod
    def parse_identifier_list(
        cls, _db, identifier_type, data_source, arguments, autocreate=False
    ):
        """Turn a list of identifiers into a list of Identifier objects.

        The list of arguments is probably derived from a command-line
        parser such as the one defined in
        IdentifierInputScript.arg_parser().

        This makes it easy to identify specific identifiers on the
        command line. Examples:

        1 2

        a b c
        """
        identifiers = []

        if not identifier_type:
            raise ValueError(
                "No identifier type specified! Use '--identifier-type=\"Database ID\"' to name identifiers by database ID."
            )

        if len(arguments) == 0:
            if data_source:
                identifiers = (
                    _db.query(Identifier)
                    .join(Identifier.licensed_through)
                    .filter(
                        Identifier.type == identifier_type,
                        LicensePool.data_source == data_source,
                    )
                    .all()
                )
            return identifiers

        for arg in arguments:
            if identifier_type == cls.DATABASE_ID:
                try:
                    arg = int(arg)
                except ValueError as e:
                    # We'll print out a warning later.
                    arg = None
                if arg:
                    identifier = get_one(_db, Identifier, id=arg)
            else:
                identifier, ignore = Identifier.for_foreign_id(
                    _db, identifier_type, arg, autocreate=autocreate
                )
            if not identifier:
                logging.warning("Could not load identifier %s/%s", identifier_type, arg)
            if identifier:
                identifiers.append(identifier)
        return identifiers


class LibraryInputScript(InputScript):
    """A script that operates on one or more Libraries."""

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None, *args, **kwargs):
        parser = cls.arg_parser(_db)
        parsed = parser.parse_args(cmd_args)
        return cls.look_up_libraries(_db, parsed, *args, **kwargs)

    @classmethod
    def arg_parser(cls, _db, multiple_libraries=True):
        parser = argparse.ArgumentParser()
        library_names = sorted(l.short_name for l in _db.query(Library))
        library_names = '"' + '", "'.join(library_names) + '"'
        parser.add_argument(
            "libraries",
            help="Name of a specific library to process. Libraries on this system: %s"
            % library_names,
            metavar="SHORT_NAME",
            nargs="*" if multiple_libraries else 1,
        )
        return parser

    @classmethod
    def look_up_libraries(cls, _db, parsed, *args, **kwargs):
        """Turn library names as specified on the command line into real
        Library objects.
        """
        if _db:
            library_strings = parsed.libraries
            if library_strings:
                parsed.libraries = cls.parse_library_list(
                    _db, library_strings, *args, **kwargs
                )
            else:
                # No libraries are specified. We will be processing
                # every library.
                parsed.libraries = _db.query(Library).all()
        else:
            # Database is not active yet. The script can call
            # parse_library_list later if it wants to.
            parsed.libraries = None
        return parsed

    @classmethod
    def parse_library_list(cls, _db, arguments):
        """Turn a list of library short names into a list of Library objects.

        The list of arguments is probably derived from a command-line
        parser such as the one defined in
        LibraryInputScript.arg_parser().
        """
        if len(arguments) == 0:
            return []
        libraries = []
        for arg in arguments:
            if not arg:
                continue
            for field in (Library.short_name, Library.name):
                try:
                    library = _db.query(Library).filter(field == arg).one()
                except NoResultFound:
                    continue
                except MultipleResultsFound:
                    continue
                if library:
                    libraries.append(library)
                    break
            else:
                logging.warning("Could not find library %s", arg)
        return libraries

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        self.process_libraries(parsed.libraries)

    def process_libraries(self, libraries):
        for library in libraries:
            self.process_library(library)

    def process_library(self, library):
        raise NotImplementedError()


class PatronInputScript(LibraryInputScript):
    """A script that operates on one or more Patrons."""

    @classmethod
    def parse_command_line(
        cls, _db=None, cmd_args=None, stdin=sys.stdin, *args, **kwargs
    ):
        parser = cls.arg_parser(_db)
        parsed = parser.parse_args(cmd_args)
        if stdin:
            stdin = cls.read_stdin_lines(stdin)
        parsed = super().look_up_libraries(_db, parsed, *args, **kwargs)
        return cls.look_up_patrons(_db, parsed, stdin, *args, **kwargs)

    @classmethod
    def arg_parser(cls, _db):
        parser = super().arg_parser(_db, multiple_libraries=False)
        parser.add_argument(
            "identifiers",
            help="A specific patron identifier to process.",
            metavar="IDENTIFIER",
            nargs="+",
        )
        return parser

    @classmethod
    def look_up_patrons(cls, _db, parsed, stdin_patron_strings, *args, **kwargs):
        """Turn patron identifiers as specified on the command line into real
        Patron objects.
        """
        if _db:
            patron_strings = parsed.identifiers
            library = parsed.libraries[0]
            if stdin_patron_strings:
                patron_strings = patron_strings + stdin_patron_strings
            parsed.patrons = cls.parse_patron_list(
                _db, library, patron_strings, *args, **kwargs
            )
        else:
            # Database is not active yet. The script can call
            # parse_patron_list later if it wants to.
            parsed.patrons = None
        return parsed

    @classmethod
    def parse_patron_list(cls, _db, library, arguments):
        """Turn a list of patron identifiers into a list of Patron objects.

        The list of arguments is probably derived from a command-line
        parser such as the one defined in
        PatronInputScript.arg_parser().
        """
        if len(arguments) == 0:
            return []
        patrons = []
        for arg in arguments:
            if not arg:
                continue
            for field in (
                Patron.authorization_identifier,
                Patron.username,
                Patron.external_identifier,
            ):
                try:
                    patron = (
                        _db.query(Patron)
                        .filter(field == arg)
                        .filter(Patron.library_id == library.id)
                        .one()
                    )
                except NoResultFound:
                    continue
                except MultipleResultsFound:
                    continue
                if patron:
                    patrons.append(patron)
                    break
            else:
                logging.warning("Could not find patron %s", arg)
        return patrons

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        self.process_patrons(parsed.patrons)

    def process_patrons(self, patrons):
        for patron in patrons:
            self.process_patron(patron)

    def process_patron(self, patron):
        raise NotImplementedError()


class LaneSweeperScript(LibraryInputScript):
    """Do something to each lane in a library."""

    def process_library(self, library):
        from .lane import WorkList

        top_level = WorkList.top_level_for_library(self._db, library)
        queue = [top_level]
        while queue:
            new_queue = []
            for l in queue:
                if isinstance(l, Lane):
                    l = self._db.merge(l)
                if self.should_process_lane(l):
                    self.process_lane(l)
                    self._db.commit()
                for sublane in l.children:
                    new_queue.append(sublane)
            queue = new_queue

    def should_process_lane(self, lane):
        return True

    def process_lane(self, lane):
        pass


class CustomListSweeperScript(LibraryInputScript):
    """Do something to each custom list in a library."""

    def process_library(self, library):
        lists = self._db.query(CustomList).filter(CustomList.library_id == library.id)
        for l in lists:
            self.process_custom_list(l)
        self._db.commit()

    def process_custom_list(self, custom_list):
        pass


class SubjectInputScript(Script):
    """A script whose command line filters the set of Subjects.

    :return: a 2-tuple (subject type, subject filter) that can be
        passed into the SubjectSweepMonitor constructor.

    """

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument("--subject-type", help="Process subjects of this type")
        parser.add_argument(
            "--subject-filter",
            help="Process subjects whose names or identifiers match this substring",
        )
        return parser


class RunCoverageProviderScript(IdentifierInputScript):
    """Run a single coverage provider."""

    @classmethod
    def arg_parser(cls):
        parser = IdentifierInputScript.arg_parser()
        parser.add_argument(
            "--cutoff-time",
            help="Update existing coverage records if they were originally created after this time.",
        )
        return parser

    @classmethod
    def parse_command_line(cls, _db, cmd_args=None, stdin=sys.stdin, *args, **kwargs):
        parser = cls.arg_parser()
        parsed = parser.parse_args(cmd_args)
        stdin = cls.read_stdin_lines(stdin)
        parsed = cls.look_up_identifiers(_db, parsed, stdin, *args, **kwargs)
        if parsed.cutoff_time:
            parsed.cutoff_time = cls.parse_time(parsed.cutoff_time)
        return parsed

    def __init__(
        self, provider, _db=None, cmd_args=None, *provider_args, **provider_kwargs
    ):

        super().__init__(_db)
        parsed_args = self.parse_command_line(self._db, cmd_args)
        if parsed_args.identifier_type:
            self.identifier_type = parsed_args.identifier_type
            self.identifier_types = [self.identifier_type]
        else:
            self.identifier_type = None
            self.identifier_types = []

        if parsed_args.identifiers:
            self.identifiers = parsed_args.identifiers
        else:
            self.identifiers = []

        if callable(provider):
            kwargs = self.extract_additional_command_line_arguments()
            kwargs.update(provider_kwargs)

            provider = provider(
                self._db, *provider_args, cutoff_time=parsed_args.cutoff_time, **kwargs
            )
        self.provider = provider
        self.name = self.provider.service_name

    def extract_additional_command_line_arguments(self):
        """A hook method for subclasses.

        Turns command-line arguments into additional keyword arguments
        to the CoverageProvider constructor.

        By default, pass in a value used only by CoverageProvider
        (as opposed to WorkCoverageProvider).
        """
        return {
            "input_identifiers": self.identifiers,
        }

    def do_run(self):
        if self.identifiers:
            self.provider.run_on_specific_identifiers(self.identifiers)
        else:
            self.provider.run()


class ShowLibrariesScript(Script):
    """Show information about the libraries on a server."""

    name = "List the libraries on this server."

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--short-name",
            help="Only display information for the library with the given short name",
        )
        parser.add_argument(
            "--show-secrets",
            help="Print out secrets associated with the library.",
            action="store_true",
        )
        return parser

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if args.short_name:
            library = get_one(_db, Library, short_name=args.short_name)
            libraries = [library]
        else:
            libraries = _db.query(Library).order_by(Library.name).all()
        if not libraries:
            output.write("No libraries found.\n")
        for library in libraries:
            output.write("\n".join(library.explain(include_secrets=args.show_secrets)))
            output.write("\n")


class ConfigurationSettingScript(Script):
    @classmethod
    def _parse_setting(self, setting):
        """Parse a command-line setting option into a key-value pair."""
        if not "=" in setting:
            raise ValueError(
                'Incorrect format for setting: "%s". Should be "key=value"' % setting
            )
        return setting.split("=", 1)

    @classmethod
    def add_setting_argument(self, parser, help):
        """Modify an ArgumentParser to indicate that the script takes
        command-line settings.
        """
        parser.add_argument("--setting", help=help, action="append")

    def apply_settings(self, settings, obj):
        """Treat `settings` as a list of command-line argument settings,
        and apply each one to `obj`.
        """
        if not settings:
            return None
        for setting in settings:
            key, value = self._parse_setting(setting)
            obj.setting(key).value = value


class ConfigureSiteScript(ConfigurationSettingScript):
    """View or update site-wide configuration."""

    def __init__(self, _db=None, config=Configuration):
        self.config = config
        super().__init__(_db=_db)

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--show-secrets",
            help="Include secrets when displaying site settings.",
            action="store_true",
            default=False,
        )

        cls.add_setting_argument(
            parser,
            'Set a site-wide setting, such as default_nongrouped_feed_max_age. Format: --setting="default_nongrouped_feed_max_age=1200"',
        )

        parser.add_argument(
            "--force",
            help="Set a site-wide setting even if the key isn't a known setting.",
            dest="force",
            action="store_true",
        )

        return parser

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if args.setting:
            for setting in args.setting:
                key, value = self._parse_setting(setting)
                if not args.force and not key in [
                    s.get("key") for s in self.config.SITEWIDE_SETTINGS
                ]:
                    raise ValueError(
                        "'%s' is not a known site-wide setting. Use --force to set it anyway."
                        % key
                    )
                else:
                    ConfigurationSetting.sitewide(_db, key).value = value
        output.write(
            "\n".join(
                ConfigurationSetting.explain(_db, include_secrets=args.show_secrets)
            )
        )
        site_configuration_has_changed(_db)
        _db.commit()


class ConfigureLibraryScript(ConfigurationSettingScript):
    """Create a library or change its settings."""

    name = "Change a library's settings"

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--name",
            help="Official name of the library",
        )
        parser.add_argument(
            "--short-name",
            help="Short name of the library",
        )
        cls.add_setting_argument(
            parser,
            'Set a per-library setting, such as terms-of-service. Format: --setting="terms-of-service=https://example.library/tos"',
        )
        return parser

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if not args.short_name:
            raise ValueError("You must identify the library by its short name.")

        # Are we talking about an existing library?
        libraries = _db.query(Library).all()

        if libraries:
            # Currently there can only be one library, and one already exists.
            [library] = libraries
            if args.short_name and library.short_name != args.short_name:
                raise ValueError("Could not locate library '%s'" % args.short_name)
        else:
            # No existing library. Make one.
            library, ignore = get_one_or_create(
                _db,
                Library,
                create_method_kwargs=dict(
                    uuid=str(uuid.uuid4()),
                    short_name=args.short_name,
                ),
            )

        if args.name:
            library.name = args.name
        if args.short_name:
            library.short_name = args.short_name
        self.apply_settings(args.setting, library)
        site_configuration_has_changed(_db)
        _db.commit()
        output.write("Configuration settings stored.\n")
        output.write("\n".join(library.explain()))
        output.write("\n")


class ShowCollectionsScript(Script):
    """Show information about the collections on a server."""

    name = "List the collections on this server."

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--name",
            help="Only display information for the collection with the given name",
        )
        parser.add_argument(
            "--show-secrets",
            help="Display secret values such as passwords.",
            action="store_true",
        )
        return parser

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if args.name:
            name = args.name
            collection = get_one(_db, Collection, name=name)
            if collection:
                collections = [collection]
            else:
                output.write("Could not locate collection by name: %s" % name)
                collections = []
        else:
            collections = _db.query(Collection).order_by(Collection.name).all()
        if not collections:
            output.write("No collections found.\n")
        for collection in collections:
            output.write(
                "\n".join(collection.explain(include_secrets=args.show_secrets))
            )
            output.write("\n")


class ShowIntegrationsScript(Script):
    """Show information about the external integrations on a server."""

    name = "List the external integrations on this server."

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--name",
            help="Only display information for the integration with the given name or ID",
        )
        parser.add_argument(
            "--show-secrets",
            help="Display secret values such as passwords.",
            action="store_true",
        )
        return parser

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if args.name:
            name = args.name
            integration = get_one(_db, ExternalIntegration, name=name)
            if not integration:
                integration = get_one(_db, ExternalIntegration, id=name)
            if integration:
                integrations = [integration]
            else:
                output.write("Could not locate integration by name or ID: %s\n" % args)
                integrations = []
        else:
            integrations = (
                _db.query(ExternalIntegration)
                .order_by(ExternalIntegration.name, ExternalIntegration.id)
                .all()
            )
        if not integrations:
            output.write("No integrations found.\n")
        for integration in integrations:
            output.write(
                "\n".join(integration.explain(include_secrets=args.show_secrets))
            )
            output.write("\n")


class ConfigureCollectionScript(ConfigurationSettingScript):
    """Create a collection or change its settings."""

    name = "Change a collection's settings"

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None):
        parser = cls.arg_parser(_db)
        return parser.parse_known_args(cmd_args)[0]

    @classmethod
    def arg_parser(cls, _db):
        parser = argparse.ArgumentParser()
        parser.add_argument("--name", help="Name of the collection", required=True)
        parser.add_argument(
            "--protocol",
            help='Protocol to use to get the licenses. Possible values: "%s"'
            % ('", "'.join(ExternalIntegration.LICENSE_PROTOCOLS)),
        )
        parser.add_argument(
            "--external-account-id",
            help='The ID of this collection according to the license source. Sometimes called a "library ID".',
        )
        parser.add_argument(
            "--url",
            help="Run the acquisition protocol against this URL.",
        )
        parser.add_argument(
            "--username",
            help='Use this username to authenticate with the license protocol. Sometimes called a "key".',
        )
        parser.add_argument(
            "--password",
            help='Use this password to authenticate with the license protocol. Sometimes called a "secret".',
        )
        cls.add_setting_argument(
            parser,
            'Set a protocol-specific setting on the collection, such as Overdrive\'s "website_id". Format: --setting="website_id=89"',
        )
        library_names = cls._library_names(_db)
        if library_names:
            parser.add_argument(
                "--library",
                help="Associate this collection with the given library. Possible libraries: %s"
                % library_names,
                action="append",
            )

        return parser

    @classmethod
    def _library_names(self, _db):
        """Return a string that lists known library names."""
        library_names = [
            x.short_name for x in _db.query(Library).order_by(Library.short_name)
        ]
        if library_names:
            return '"' + '", "'.join(library_names) + '"'
        return ""

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)

        # Find or create the collection
        protocol = None
        name = args.name
        protocol = args.protocol
        collection = get_one(_db, Collection, Collection.name == name)
        if not collection:
            if protocol:
                collection, is_new = Collection.by_name_and_protocol(
                    _db, name, protocol
                )
            else:
                # We didn't find a Collection, and we don't have a protocol,
                # so we can't create a new Collection.
                raise ValueError(
                    'No collection called "%s". You can create it, but you must specify a protocol.'
                    % name
                )
        integration = collection.external_integration
        if protocol:
            integration.protocol = protocol
        if args.external_account_id:
            collection.external_account_id = args.external_account_id

        if args.url:
            integration.url = args.url
        if args.username:
            integration.username = args.username
        if args.password:
            integration.password = args.password
        self.apply_settings(args.setting, integration)

        if hasattr(args, "library"):
            for name in args.library:
                library = get_one(_db, Library, short_name=name)
                if not library:
                    library_names = self._library_names(_db)
                    message = 'No such library: "%s".' % name
                    if library_names:
                        message += " I only know about: %s" % library_names
                    raise ValueError(message)
                if collection not in library.collections:
                    library.collections.append(collection)
        site_configuration_has_changed(_db)
        _db.commit()
        output.write("Configuration settings stored.\n")
        output.write("\n".join(collection.explain()))
        output.write("\n")


class ConfigureIntegrationScript(ConfigurationSettingScript):
    """Create a integration or change its settings."""

    name = "Create a site-wide integration or change an integration's settings"

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None):
        parser = cls.arg_parser(_db)
        return parser.parse_known_args(cmd_args)[0]

    @classmethod
    def arg_parser(cls, _db):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--name",
            help="Name of the integration",
        )
        parser.add_argument(
            "--id",
            help="ID of the integration, if it has no name",
        )
        parser.add_argument(
            "--protocol",
            help="Protocol used by the integration.",
        )
        parser.add_argument(
            "--goal",
            help="Goal of the integration",
        )
        cls.add_setting_argument(
            parser,
            'Set a configuration value on the integration. Format: --setting="key=value"',
        )
        return parser

    @classmethod
    def _integration(self, _db, id, name, protocol, goal):
        """Find or create the ExternalIntegration referred to."""
        if not id and not name and not (protocol and goal):
            raise ValueError(
                "An integration must by identified by either ID, name, or the combination of protocol and goal."
            )
        integration = None
        if id:
            integration = get_one(
                _db, ExternalIntegration, ExternalIntegration.id == id
            )
            if not integration:
                raise ValueError("No integration with ID %s." % id)
        if name:
            integration = get_one(_db, ExternalIntegration, name=name)
            if not integration and not (protocol and goal):
                raise ValueError(
                    'No integration with name "%s". To create it, you must also provide protocol and goal.'
                    % name
                )
        if not integration and (protocol and goal):
            integration, is_new = get_one_or_create(
                _db, ExternalIntegration, protocol=protocol, goal=goal
            )
        if name:
            integration.name = name
        return integration

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)

        # Find or create the integration
        protocol = None
        id = args.id
        name = args.name
        protocol = args.protocol
        goal = args.goal
        integration = self._integration(_db, id, name, protocol, goal)
        self.apply_settings(args.setting, integration)
        site_configuration_has_changed(_db)
        _db.commit()
        output.write("Configuration settings stored.\n")
        output.write("\n".join(integration.explain()))
        output.write("\n")


class ShowLanesScript(Script):
    """Show information about the lanes on a server."""

    name = "List the lanes on this server."

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--id",
            help="Only display information for the lane with the given ID",
        )
        return parser

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if args.id:
            id = args.id
            lane = get_one(_db, Lane, id=id)
            if lane:
                lanes = [lane]
            else:
                output.write("Could not locate lane with id: %s" % id)
                lanes = []
        else:
            lanes = _db.query(Lane).order_by(Lane.id).all()
        if not lanes:
            output.write("No lanes found.\n")
        for lane in lanes:
            output.write("\n".join(lane.explain()))
            output.write("\n\n")


class ConfigureLaneScript(ConfigurationSettingScript):
    """Create a lane or change its settings."""

    name = "Change a lane's settings"

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None):
        parser = cls.arg_parser(_db)
        return parser.parse_known_args(cmd_args)[0]

    @classmethod
    def arg_parser(cls, _db):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--id",
            help="ID of the lane, if editing an existing lane.",
        )
        parser.add_argument(
            "--library-short-name",
            help="Short name of the library for this lane. Possible values: %s"
            % cls._library_names(_db),
        )
        parser.add_argument(
            "--parent-id",
            help="The ID of this lane's parent lane",
        )
        parser.add_argument(
            "--priority",
            help="The lane's priority",
        )
        parser.add_argument(
            "--display-name",
            help="The lane name that will be displayed to patrons.",
        )
        return parser

    @classmethod
    def _library_names(self, _db):
        """Return a string that lists known library names."""
        library_names = [
            x.short_name for x in _db.query(Library).order_by(Library.short_name)
        ]
        if library_names:
            return '"' + '", "'.join(library_names) + '"'
        return ""

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)

        # Find or create the lane
        id = args.id
        lane = get_one(_db, Lane, id=id)
        if not lane:
            if args.library_short_name:
                library = get_one(_db, Library, short_name=args.library_short_name)
                if not library:
                    raise ValueError('No such library: "%s".' % args.library_short_name)
                lane, is_new = create(_db, Lane, library=library)
            else:
                raise ValueError("Library short name is required to create a new lane.")

        if args.parent_id:
            lane.parent_id = args.parent_id
        if args.priority:
            lane.priority = args.priority
        if args.display_name:
            lane.display_name = args.display_name
        site_configuration_has_changed(_db)
        _db.commit()
        output.write("Lane settings stored.\n")
        output.write("\n".join(lane.explain()))
        output.write("\n")


class AddClassificationScript(IdentifierInputScript):
    name = "Add a classification to an identifier"

    @classmethod
    def arg_parser(cls):
        parser = IdentifierInputScript.arg_parser()
        parser.add_argument(
            "--subject-type",
            help="The type of the subject to add to each identifier.",
            required=True,
        )
        parser.add_argument(
            "--subject-identifier",
            help="The identifier of the subject to add to each identifier.",
        )
        parser.add_argument(
            "--subject-name", help="The name of the subject to add to each identifier."
        )
        parser.add_argument(
            "--data-source",
            help="The data source to use when classifying.",
            default=DataSource.MANUAL,
        )
        parser.add_argument(
            "--weight",
            help="The weight to use when classifying.",
            type=int,
            default=1000,
        )
        parser.add_argument(
            "--create-subject",
            help="Add the subject to the database if it doesn't already exist",
            action="store_const",
            const=True,
        )
        return parser

    def __init__(self, _db=None, cmd_args=None, stdin=sys.stdin):
        super().__init__(_db=_db)
        args = self.parse_command_line(self._db, cmd_args=cmd_args, stdin=stdin)
        self.identifier_type = args.identifier_type
        self.identifiers = args.identifiers
        subject_type = args.subject_type
        subject_identifier = args.subject_identifier
        subject_name = args.subject_name
        if not subject_name and not subject_identifier:
            raise ValueError(
                "Either subject-name or subject-identifier must be provided."
            )
        self.data_source = DataSource.lookup(self._db, args.data_source)
        self.weight = args.weight
        self.subject, ignore = Subject.lookup(
            self._db,
            subject_type,
            subject_identifier,
            subject_name,
            autocreate=args.create_subject,
        )

    def do_run(self):
        policy = PresentationCalculationPolicy(
            choose_edition=False,
            set_edition_metadata=False,
            classify=True,
            choose_summary=False,
            calculate_quality=False,
            choose_cover=False,
            regenerate_opds_entries=True,
            regenerate_marc_record=True,
            update_search_index=True,
            verbose=True,
        )
        if self.subject:
            for identifier in self.identifiers:
                identifier.classify(
                    self.data_source,
                    self.subject.type,
                    self.subject.identifier,
                    self.subject.name,
                    self.weight,
                )
                work = identifier.work
                if work:
                    work.calculate_presentation(policy=policy)
        else:
            self.log.warning("Could not locate subject, doing nothing.")


class WorkProcessingScript(IdentifierInputScript):

    name = "Work processing script"

    def __init__(
        self, force=False, batch_size=10, _db=None, cmd_args=None, stdin=sys.stdin
    ):
        super().__init__(_db=_db)

        args = self.parse_command_line(self._db, cmd_args=cmd_args, stdin=stdin)
        self.identifier_type = args.identifier_type
        self.data_source = args.identifier_data_source

        self.identifiers = self.parse_identifier_list(
            self._db, self.identifier_type, self.data_source, args.identifier_strings
        )

        self.batch_size = batch_size
        self.query = self.make_query(
            self._db,
            self.identifier_type,
            self.identifiers,
            self.data_source,
            log=self.log,
        )
        self.force = force

    def paginate_query(self, query):
        raise NotImplementedError()

    @classmethod
    def make_query(cls, _db, identifier_type, identifiers, data_source, log=None):
        query = _db.query(Work)
        if identifiers or identifier_type:
            query = query.join(Work.license_pools).join(LicensePool.identifier)

        if identifiers:
            if log:
                log.info("Restricted to %d specific identifiers." % len(identifiers))
            query = query.filter(
                LicensePool.identifier_id.in_([x.id for x in identifiers])
            )
        elif data_source:
            if log:
                log.info('Restricted to identifiers from DataSource "%s".', data_source)
            source = DataSource.lookup(_db, data_source)
            query = query.filter(LicensePool.data_source == source)

        if identifier_type:
            if log:
                log.info('Restricted to identifier type "%s".' % identifier_type)
            query = query.filter(Identifier.type == identifier_type)

        if log:
            log.info("Processing %d works.", query.count())
        return query.order_by(Work.id)

    def do_run(self):
        works = True
        offset = 0

        # Does this script class allow uniquely paged queries
        # If not we will default to OFFSET paging
        try:
            paged_query = self.paginate_query(self.query)
        except NotImplementedError:
            paged_query = None

        while works:
            if not paged_query:
                works = self.query.offset(offset).limit(self.batch_size).all()
            else:
                works = next(paged_query, [])

            for work in works:
                self.process_work(work)
            offset += self.batch_size
            self._db.commit()
        self._db.commit()

    def process_work(self, work):
        raise NotImplementedError()


class WorkConsolidationScript(WorkProcessingScript):
    """Given an Identifier, make sure all the LicensePools for that
    Identifier are in Works that follow these rules:

    a) For a given permanent work ID, there may be at most one Work
    containing open-access LicensePools.

    b) Each non-open-access LicensePool has its own individual Work.
    """

    name = "Work consolidation script"

    def make_query(self, _db, identifier_type, identifiers, data_source, log=None):
        # We actually process LicensePools, not Works.
        qu = _db.query(LicensePool).join(LicensePool.identifier)
        if identifier_type:
            qu = qu.filter(Identifier.type == identifier_type)
        if identifiers:
            qu = qu.filter(
                Identifier.identifier.in_([x.identifier for x in identifiers])
            )
        return qu

    def process_work(self, work):
        # We call it 'work' for signature compatibility with the superclass,
        # but it's actually a LicensePool.
        licensepool = work
        licensepool.calculate_work()

    def do_run(self):
        super().do_run()
        qu = (
            self._db.query(Work)
            .outerjoin(Work.license_pools)
            .filter(LicensePool.id == None)
        )
        self.log.info("Deleting %d Works that have no LicensePools." % qu.count())
        for i in qu:
            self._db.delete(i)
        self._db.commit()


class WorkPresentationScript(TimestampScript, WorkProcessingScript):
    """Calculate the presentation for Work objects."""

    name = "Recalculate the presentation for works that need it."

    # Do a complete recalculation of the presentation.
    policy = PresentationCalculationPolicy()

    def process_work(self, work):
        work.calculate_presentation(policy=self.policy)


class WorkClassificationScript(WorkPresentationScript):
    """Recalculate the classification--and nothing else--for Work objects."""

    name = "Recalculate the classification for works that need it." ""

    policy = PresentationCalculationPolicy(
        choose_edition=False,
        set_edition_metadata=False,
        classify=True,
        choose_summary=False,
        calculate_quality=False,
        choose_cover=False,
        regenerate_opds_entries=False,
        regenerate_marc_record=False,
        update_search_index=False,
    )


class ReclassifyWorksForUncheckedSubjectsScript(WorkClassificationScript):
    """Reclassify all Works whose current classifications appear to
    depend on Subjects in the 'unchecked' state.

    This generally means that some migration script reset those
    Subjects because the rules for processing them changed.
    """

    name = "Reclassify works that use unchecked subjects." ""

    policy = WorkClassificationScript.policy

    batch_size = 100

    def __init__(self, _db=None):
        self.timestamp_collection = None
        if _db:
            self._session = _db
        self.query = self._optimized_query()

    def _optimized_query(self):
        """Optimizations include
        - Order by each joined table's PK, so that paging is consistent
        - Deferred loading of large text columns"""

        # No filter clause yet, we will filter this PER SUBJECT ID
        # in the paginate query
        query = (
            self._db.query(Work)
            .join(Work.license_pools)
            .join(LicensePool.identifier)
            .join(Identifier.classifications)
            .join(Classification.subject)
        )

        # Must order by all joined attributes
        query = (
            query.order_by(None)
            .order_by(
                Subject.id, Work.id, LicensePool.id, Identifier.id, Classification.id
            )
            .options(
                defer(Work.summary_text),
                defer(Work.simple_opds_entry),
                defer(Work.verbose_opds_entry),
            )
        )

        return query

    def _unchecked_subjects(self):
        """Yield one unchecked subject at a time"""
        query = (
            self._db.query(Subject)
            .filter(Subject.checked == False)
            .order_by(Subject.id)
        )
        last_id = None
        while True:
            qu = query
            if last_id:
                qu = qu.filter(Subject.id > last_id)
            subject = qu.first()

            if not subject:
                return

            last_id = subject.id
            yield subject

    def paginate_query(self, query) -> Generator:
        """Page this query using the row-wise comparison
        technique unique to this job. We have already ensured
        the ordering of the rows follows all the joined tables"""

        for subject in self._unchecked_subjects():

            last_work: Optional[Work] = None  # Last work object of the previous page
            # IDs of the last work, for paging
            work_id, license_id, iden_id, classn_id = (
                None,
                None,
                None,
                None,
            )

            while True:
                # We are a "per subject" filter, this is the MOST efficient method
                qu: Query = query.filter(Subject.id == subject.id)
                # Add the columns we need to page with explicitly in the query
                qu = qu.add_columns(LicensePool.id, Identifier.id, Classification.id)
                # We're not on the first page, add the row-wise comparison
                if last_work is not None:
                    qu = qu.filter(
                        tuple_(
                            Work.id,
                            LicensePool.id,
                            Identifier.id,
                            Classification.id,
                        )
                        > (work_id, license_id, iden_id, classn_id)
                    )

                qu = qu.limit(self.batch_size)
                works = qu.all()
                if not len(works):
                    break

                last_work_row = works[-1]
                last_work = last_work_row[0]
                # set comprehension ensures we get unique works per loop
                # Works will get duplicated in the query because of the addition
                # of the ID columns in the select, it is possible and expected
                # that works will get duplicated across loops. It is not a desired
                # outcome to duplicate works across loops, but the alternative is to maintain
                # the IDs in memory and add a NOT IN operator in the query
                # which would grow quite large, quite fast
                only_works = list({w[0] for w in works})

                yield only_works

                work_id, license_id, iden_id, classn_id = (
                    last_work_row[0].id,
                    last_work_row[1],
                    last_work_row[2],
                    last_work_row[3],
                )


class WorkOPDSScript(WorkPresentationScript):
    """Recalculate the OPDS entries, MARC record, and search index entries
    for Work objects.

    This is intended to verify that a problem has already been resolved and just
    needs to be propagated to these three 'caches'.
    """

    name = "Recalculate OPDS entries, MARC record, and search index entries for works that need it."

    policy = PresentationCalculationPolicy(
        choose_edition=False,
        set_edition_metadata=False,
        classify=True,
        choose_summary=False,
        calculate_quality=False,
        choose_cover=False,
        regenerate_opds_entries=True,
        regenerate_marc_record=True,
        update_search_index=True,
    )


class CustomListManagementScript(Script):
    """Maintain a CustomList whose membership is determined by a
    MembershipManager.
    """

    def __init__(
        self,
        manager_class,
        data_source_name,
        list_identifier,
        list_name,
        primary_language,
        description,
        **manager_kwargs,
    ):
        data_source = DataSource.lookup(self._db, data_source_name)
        self.custom_list, is_new = get_one_or_create(
            self._db,
            CustomList,
            data_source_id=data_source.id,
            foreign_identifier=list_identifier,
        )
        self.custom_list.primary_language = primary_language
        self.custom_list.description = description
        self.membership_manager = manager_class(self.custom_list, **manager_kwargs)

    def run(self):
        self.membership_manager.update()
        self._db.commit()


class CollectionType(Enum):
    OPEN_ACCESS = "OPEN_ACCESS"
    PROTECTED_ACCESS = "PROTECTED_ACCESS"
    LCP = "LCP"

    def __str__(self):
        return self.name


class CollectionInputScript(Script):
    """A script that takes collection names as command line inputs."""

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None, *args, **kwargs):
        parser = cls.arg_parser()
        parsed = parser.parse_args(cmd_args)
        return cls.look_up_collections(_db, parsed, *args, **kwargs)

    @classmethod
    def look_up_collections(cls, _db, parsed, *args, **kwargs):
        """Turn collection names as specified on the command line into
        real database Collection objects.
        """
        parsed.collections = []
        for name in parsed.collection_names:
            collection = get_one(_db, Collection, name=name)
            if not collection:
                raise ValueError("Unknown collection: %s" % name)
            parsed.collections.append(collection)
        return parsed

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--collection",
            help="Collection to use",
            dest="collection_names",
            metavar="NAME",
            action="append",
            default=[],
        )
        return parser


class CollectionArgumentsScript(CollectionInputScript):
    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "collection_names",
            help="One or more collection names.",
            metavar="COLLECTION",
            nargs="*",
        )
        return parser


class RunCollectionMonitorScript(RunMultipleMonitorsScript, CollectionArgumentsScript):
    """Run a CollectionMonitor on every Collection that comes through a
    certain protocol.
    """

    def __init__(self, monitor_class, _db=None, cmd_args=None, **kwargs):
        """Constructor.

        :param monitor_class: A class object that derives from
            CollectionMonitor.
        :type monitor_class: CollectionMonitor

        :param cmd_args: Optional command line arguments. These will be
            passed on to the command line parser.
        :type cmd_args: Optional[List[str]]

        :param kwargs: Keyword arguments to pass into the `monitor_class`
            constructor each time it's called.

        """
        super().__init__(_db, **kwargs)
        self.monitor_class = monitor_class
        self.name = self.monitor_class.SERVICE_NAME
        parsed = vars(self.parse_command_line(self._db, cmd_args=cmd_args))
        parsed.pop("collection_names", None)
        self.collections = parsed.pop("collections", None)
        self.kwargs.update(parsed)

    def monitors(self, **kwargs):
        return self.monitor_class.all(self._db, collections=self.collections, **kwargs)


class OPDSImportScript(CollectionInputScript):
    """Import all books from the OPDS feed associated with a collection."""

    name = "Import all books from the OPDS feed associated with a collection."

    IMPORTER_CLASS = OPDSImporter
    MONITOR_CLASS: Type[OPDSImportMonitor] = OPDSImportMonitor
    PROTOCOL = ExternalIntegration.OPDS_IMPORT

    def __init__(
        self,
        _db=None,
        importer_class=None,
        monitor_class=None,
        protocol=None,
        *args,
        **kwargs,
    ):
        super().__init__(_db, *args, **kwargs)
        self.importer_class = importer_class or self.IMPORTER_CLASS
        self.monitor_class = monitor_class or self.MONITOR_CLASS
        self.protocol = protocol or self.PROTOCOL
        self.importer_kwargs = kwargs

    @classmethod
    def arg_parser(cls):
        parser = CollectionInputScript.arg_parser()
        parser.add_argument(
            "--force",
            help="Import the feed from scratch, even if it seems like it was already imported.",
            dest="force",
            action="store_true",
        )
        return parser

    def do_run(self, cmd_args=None):
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        collections = parsed.collections or Collection.by_protocol(
            self._db, self.protocol
        )
        for collection in collections:
            self.run_monitor(collection, force=parsed.force)

    def run_monitor(self, collection, force=None):
        monitor = self.monitor_class(
            self._db,
            collection,
            import_class=self.importer_class,
            force_reimport=force,
            **self.importer_kwargs,
        )
        monitor.run()


class MirrorResourcesScript(CollectionInputScript):
    """Make sure that all mirrorable resources in a collection have
    in fact been mirrored.
    """

    # This object contains the actual logic of mirroring.
    MIRROR_UTILITY = MetaToModelUtility()

    @classmethod
    def arg_parser(cls):
        parser = super().arg_parser()
        parser.add_argument(
            "--collection-type",
            help="Collection type. Valid values are: OPEN_ACCESS (default), PROTECTED_ACCESS.",
            type=CollectionType,
            choices=list(CollectionType),
            default=CollectionType.OPEN_ACCESS,
        )
        return parser

    def do_run(self, cmd_args=None):
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        collections = parsed.collections
        collection_type = parsed.collection_type
        if not collections:
            # Assume they mean all collections.
            collections = self._db.query(Collection).all()

        # But only process collections that have an associated MirrorUploader.
        for collection, policy in self.collections_with_uploader(
            collections, collection_type
        ):
            self.process_collection(collection, policy)

    def collections_with_uploader(
        self, collections, collection_type=CollectionType.OPEN_ACCESS
    ):
        """Filter out collections that have no MirrorUploader.

        :yield: 2-tuples (Collection, ReplacementPolicy). The
            ReplacementPolicy is the appropriate one for this script
            to use for that Collection.
        """
        for collection in collections:
            covers = MirrorUploader.for_collection(
                collection, ExternalIntegrationLink.COVERS
            )
            books_mirror_type = (
                ExternalIntegrationLink.OPEN_ACCESS_BOOKS
                if collection_type == CollectionType.OPEN_ACCESS
                else ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS
            )
            books = MirrorUploader.for_collection(collection, books_mirror_type)
            if covers or books:
                mirrors = {
                    ExternalIntegrationLink.COVERS: covers,
                    books_mirror_type: books,
                }
                policy = self.replacement_policy(mirrors)
                yield collection, policy
            else:
                self.log.info("Skipping %r as it has no MirrorUploader.", collection)

    @classmethod
    def replacement_policy(cls, mirrors):
        """Create a ReplacementPolicy for this script that uses the
        given mirrors.
        """
        return ReplacementPolicy(
            mirrors=mirrors,
            link_content=True,
            even_if_not_apparently_updated=True,
            http_get=Representation.cautious_http_get,
        )

    def process_collection(self, collection, policy, unmirrored=None):
        """Make sure every mirrorable resource in this collection has
        been mirrored.

        :param unmirrored: A replacement for Hyperlink.unmirrored,
            for use in tests.

        """
        unmirrored = unmirrored or Hyperlink.unmirrored
        for link in unmirrored(collection):
            self.process_item(collection, link, policy)
            self._db.commit()

    @classmethod
    def derive_rights_status(cls, license_pool, resource):
        """Make a best guess about the rights status for the given
        resource.

        This relies on the information having been available at one point,
        but having been stored in the database at a slight remove.
        """
        rights_status = None
        if not license_pool:
            return None
        if resource:
            lpdm = resource.as_delivery_mechanism_for(license_pool)
            # When this Resource was associated with this LicensePool,
            # the rights information was recorded in its
            # LicensePoolDeliveryMechanism.
            if lpdm:
                rights_status = lpdm.rights_status
        if not rights_status:
            # We could not find a LicensePoolDeliveryMechanism for
            # this particular resource, but if every
            # LicensePoolDeliveryMechanism has the same rights
            # status, we can assume it's that one.
            statuses = list({x.rights_status for x in license_pool.delivery_mechanisms})
            if len(statuses) == 1:
                [rights_status] = statuses
        if rights_status:
            rights_status = rights_status.uri
        return rights_status

    def process_item(self, collection, link_obj, policy):
        """Determine the URL that needs to be mirrored and (for books)
        the rationale that lets us mirror that URL. Then mirror it.
        """
        identifier = link_obj.identifier
        license_pool, ignore = LicensePool.for_foreign_id(
            self._db,
            collection.data_source,
            identifier.type,
            identifier.identifier,
            collection=collection,
            autocreate=False,
        )
        if not license_pool:
            # This shouldn't happen.
            self.log.warning(
                "Could not find LicensePool for %r, skipping it rather than mirroring something we shouldn't."
            )
            return
        resource = link_obj.resource

        if link_obj.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD:
            rights_status = self.derive_rights_status(license_pool, resource)
            if not rights_status:
                self.log.warning(
                    "Could not unambiguously determine rights status for %r, skipping.",
                    link_obj,
                )
                return
        else:
            # For resources like book covers, the rights status is
            # irrelevant -- we rely on fair use.
            rights_status = None

        # Mock up a LinkData that MetaToModelUtility can use to
        # mirror this link (or decide not to mirror it).
        linkdata = LinkData(
            rel=link_obj.rel, href=resource.url, rights_uri=rights_status
        )

        # Mirror the link (or not).
        self.MIRROR_UTILITY.mirror_link(
            model_object=license_pool,
            data_source=collection.data_source,
            link=linkdata,
            link_obj=link_obj,
            policy=policy,
        )


class CheckContributorNamesInDB(IdentifierInputScript):
    """Checks that contributor sort_names are display_names in
    "last name, comma, other names" format.

    Read contributors edition by edition, so that can, if necessary,
    restrict db query by passed-in identifiers, and so can find associated
    license pools to register author complaints to.

    NOTE:  There's also CheckContributorNamesOnWeb in metadata,
    it's a child of this script.  Use it to check our knowledge against
    viaf, with the newer better sort_name selection and formatting.

    TODO: make sure don't start at beginning again when interrupt while batch job is running.
    """

    COMPLAINT_SOURCE = "CheckContributorNamesInDB"
    COMPLAINT_TYPE = "http://librarysimplified.org/terms/problem/wrong-author"

    def __init__(self, _db=None, cmd_args=None, stdin=sys.stdin):
        super().__init__(_db=_db)

        self.parsed_args = self.parse_command_line(
            _db=self._db, cmd_args=cmd_args, stdin=stdin
        )

    @classmethod
    def make_query(self, _db, identifier_type, identifiers, log=None):
        query = _db.query(Edition)
        if identifiers or identifier_type:
            query = query.join(Edition.primary_identifier)

        # we only want to look at editions with license pools, in case we want to make a Complaint
        query = query.join(Edition.is_presentation_for)

        if identifiers:
            if log:
                log.info("Restricted to %d specific identifiers." % len(identifiers))
            query = query.filter(
                Edition.primary_identifier_id.in_([x.id for x in identifiers])
            )
        if identifier_type:
            if log:
                log.info('Restricted to identifier type "%s".' % identifier_type)
            query = query.filter(Identifier.type == identifier_type)

        if log:
            log.info("Processing %d editions.", query.count())

        return query.order_by(Edition.id)

    def do_run(self, batch_size=10):

        self.query = self.make_query(
            self._db,
            self.parsed_args.identifier_type,
            self.parsed_args.identifiers,
            self.log,
        )

        editions = True
        offset = 0
        output = "ContributorID|\tSortName|\tDisplayName|\tComputedSortName|\tResolution|\tComplaintSource"
        print(output.encode("utf8"))

        while editions:
            my_query = self.query.offset(offset).limit(batch_size)
            editions = my_query.all()

            for edition in editions:
                if edition.contributions:
                    for contribution in edition.contributions:
                        self.process_contribution_local(
                            self._db, contribution, self.log
                        )
            offset += batch_size

            self._db.commit()
        self._db.commit()

    def process_local_mismatch(self, **kwargs):
        """XXX: This used to produce a Complaint, but the complaint system no longer exists..."""
        return None

    def process_contribution_local(self, _db, contribution, log=None):
        if not contribution or not contribution.edition:
            return

        contributor = contribution.contributor

        identifier = contribution.edition.primary_identifier

        if contributor.sort_name and contributor.display_name:
            computed_sort_name_local_new = unicodedata.normalize(
                "NFKD", str(display_name_to_sort_name(contributor.display_name))
            )
            # Did HumanName parser produce a differet result from the plain comma replacement?
            if (
                contributor.sort_name.strip().lower()
                != computed_sort_name_local_new.strip().lower()
            ):
                error_message_detail = (
                    "Contributor[id=%s].sort_name is oddly different from computed_sort_name, human intervention required."
                    % contributor.id
                )

                # computed names don't match.  by how much?  if it's a matter of a comma or a misplaced
                # suffix, we can fix without asking for human intervention.  if the names are very different,
                # there's a chance the sort and display names are different on purpose, s.a. when foreign names
                # are passed as translated into only one of the fields, or when the author has a popular pseudonym.
                # best ask a human.

                # if the relative lengths are off by more than a stray space or comma, ask a human
                # it probably means that a human metadata professional had added an explanation/expansion to the
                # sort_name, s.a. "Bob A. Jones" --> "Bob A. (Allan) Jones", and we'd rather not replace this data
                # with the "Jones, Bob A." that the auto-algorigthm would generate.
                length_difference = len(contributor.sort_name.strip()) - len(
                    computed_sort_name_local_new.strip()
                )
                if abs(length_difference) > 3:
                    return self.process_local_mismatch(
                        _db=_db,
                        contribution=contribution,
                        computed_sort_name=computed_sort_name_local_new,
                        error_message_detail=error_message_detail,
                        log=log,
                    )

                match_ratio = contributor_name_match_ratio(
                    contributor.sort_name,
                    computed_sort_name_local_new,
                    normalize_names=False,
                )

                if match_ratio < 40:
                    # ask a human.  this kind of score can happen when the sort_name is a transliteration of the display_name,
                    # and is non-trivial to fix.
                    self.process_local_mismatch(
                        _db=_db,
                        contribution=contribution,
                        computed_sort_name=computed_sort_name_local_new,
                        error_message_detail=error_message_detail,
                        log=log,
                    )
                else:
                    # we can fix it!
                    output = "{}|\t{}|\t{}|\t{}|\tlocal_fix".format(
                        contributor.id,
                        contributor.sort_name,
                        contributor.display_name,
                        computed_sort_name_local_new,
                    )
                    print(output.encode("utf8"))
                    self.set_contributor_sort_name(
                        computed_sort_name_local_new, contribution
                    )

    @classmethod
    def set_contributor_sort_name(cls, sort_name, contribution):
        """Sets the contributor.sort_name and associated edition.author_name to the passed-in value."""
        contribution.contributor.sort_name = sort_name

        # also change edition.sort_author, if the author was primary
        # Note: I considered using contribution.edition.author_contributors, but
        # found that it's not impossible to have a messy dataset that doesn't work on.
        # For our purpose here, the following logic is cleaner-acting:
        # If this author appears as Primary Author anywhere on the edition, then change edition.sort_author.
        edition_contributions = contribution.edition.contributions
        for edition_contribution in edition_contributions:
            if (edition_contribution.role == Contributor.PRIMARY_AUTHOR_ROLE) and (
                edition_contribution.contributor.display_name
                == contribution.contributor.display_name
            ):
                contribution.edition.sort_author = sort_name


class Explain(IdentifierInputScript):
    """Explain everything known about a given work."""

    name = "Explain everything known about a given work"

    # Where to go to get best available metadata about a work.
    METADATA_URL_TEMPLATE = "http://metadata.librarysimplified.org/lookup?urn=%s"
    TIME_FORMAT = "%Y-%m-%d %H:%M"

    def do_run(self, cmd_args=None, stdin=sys.stdin, stdout=sys.stdout):
        param_args = self.parse_command_line(self._db, cmd_args=cmd_args, stdin=stdin)
        identifier_ids = [x.id for x in param_args.identifiers]
        editions = self._db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_ids)
        )
        self.stdout = stdout

        policy = None
        for edition in editions:
            self.explain(self._db, edition, policy)
            self.write("-" * 80)

    def write(self, s):
        """Write a string to self.stdout."""
        if not s.endswith("\n"):
            s += "\n"
        self.stdout.write(s)

    def explain(self, _db, edition, presentation_calculation_policy=None):
        if edition.medium not in ("Book", "Audio"):
            # we haven't yet decided what to display for you
            return

        # Tell about the Edition record.
        output = "{} ({}, {}) according to {}".format(
            edition.title,
            edition.author,
            edition.medium,
            edition.data_source.name,
        )
        self.write(output)
        self.write(" Permanent work ID: %s" % edition.permanent_work_id)
        self.write(
            " Metadata URL: %s "
            % (self.METADATA_URL_TEMPLATE % edition.primary_identifier.urn)
        )

        seen = set()
        self.explain_identifier(edition.primary_identifier, True, seen, 1, 0)

        # Find all contributions, and tell about the contributors.
        if edition.contributions:
            for contribution in edition.contributions:
                self.explain_contribution(contribution)

        # Tell about the LicensePool.
        lps = edition.license_pools
        if lps:
            for lp in lps:
                self.explain_license_pool(lp)
        else:
            self.write(" No associated license pools.")

        # Tell about the Work.
        work = edition.work
        if work:
            self.explain_work(work)
        else:
            self.write(" No associated work.")

        # Note:  Can change DB state.
        if work and presentation_calculation_policy is not None:
            print("!!! About to calculate presentation!")
            work.calculate_presentation(policy=presentation_calculation_policy)
            print("!!! All done!")
            print()
            print("After recalculating presentation:")
            self.explain_work(work)

    def explain_contribution(self, contribution):
        contributor_id = contribution.contributor.id
        contributor_sort_name = contribution.contributor.sort_name
        contributor_display_name = contribution.contributor.display_name
        self.write(
            " Contributor[%s]: contributor_sort_name=%s, contributor_display_name=%s, "
            % (contributor_id, contributor_sort_name, contributor_display_name)
        )

    def explain_identifier(self, identifier, primary, seen, strength, level):
        indent = "  " * level
        if primary:
            ident = "Primary identifier"
        else:
            ident = "Identifier"
        if primary:
            strength = 1
        self.write(
            "%s %s: %s/%s (q=%s)"
            % (indent, ident, identifier.type, identifier.identifier, strength)
        )

        _db = Session.object_session(identifier)
        classifications = Identifier.classifications_for_identifier_ids(
            _db, [identifier.id]
        )
        for classification in classifications:
            subject = classification.subject
            genre = subject.genre
            if genre:
                genre = genre.name
            else:
                genre = "(!genre)"
            # print("%s  %s says: %s/%s %s w=%s" % (
            #    indent, classification.data_source.name,
            #    subject.identifier, subject.name, genre, classification.weight
            # ))
        seen.add(identifier)
        for equivalency in identifier.equivalencies:
            if equivalency.id in seen:
                continue
            seen.add(equivalency.id)
            output = equivalency.output
            self.explain_identifier(
                output, False, seen, equivalency.strength, level + 1
            )
        if primary:
            crs = identifier.coverage_records
            if crs:
                self.write("  %d coverage records:" % len(crs))
                for cr in sorted(crs, key=lambda x: x.timestamp):
                    self.explain_coverage_record(cr)

    def explain_license_pool(self, pool):
        self.write("Licensepool info:")
        if pool.collection:
            self.write(" Collection: %r" % pool.collection)
            libraries = [library.name for library in pool.collection.libraries]
            if libraries:
                self.write(" Available to libraries: %s" % ", ".join(libraries))
            else:
                self.write("Not available to any libraries!")
        else:
            self.write(" Not in any collection!")
        self.write(" Delivery mechanisms:")
        if pool.delivery_mechanisms:
            for lpdm in pool.delivery_mechanisms:
                dm = lpdm.delivery_mechanism
                if dm.default_client_can_fulfill:
                    fulfillable = "Fulfillable"
                else:
                    fulfillable = "Unfulfillable"
                self.write(f"  {fulfillable} {dm.content_type}/{dm.drm_scheme}")
        else:
            self.write(" No delivery mechanisms.")
        self.write(
            " %s owned, %d available, %d holds, %d reserves"
            % (
                pool.licenses_owned,
                pool.licenses_available,
                pool.patrons_in_hold_queue,
                pool.licenses_reserved,
            )
        )

    def explain_work(self, work):
        self.write("Work info:")
        if work.presentation_edition:
            self.write(
                " Identifier of presentation edition: %r"
                % work.presentation_edition.primary_identifier
            )
        else:
            self.write(" No presentation edition.")
        self.write(" Fiction: %s" % work.fiction)
        self.write(" Audience: %s" % work.audience)
        self.write(" Target age: %r" % work.target_age)
        self.write(" %s genres." % (len(work.genres)))
        for genre in work.genres:
            self.write("  %s" % genre)
        self.write(" License pools:")
        for pool in work.license_pools:
            active = "SUPERCEDED"
            if not pool.superceded:
                active = "ACTIVE"
            if pool.collection:
                collection = pool.collection.name
            else:
                collection = "!collection"
            self.write(f"  {active}: {pool.identifier!r} {collection}")
        wcrs = sorted(work.coverage_records, key=lambda x: x.timestamp)
        if wcrs:
            self.write(" %s work coverage records" % len(wcrs))
            for wcr in wcrs:
                self.explain_work_coverage_record(wcr)

    def explain_coverage_record(self, cr):
        self._explain_coverage_record(
            cr.timestamp, cr.data_source, cr.operation, cr.status, cr.exception
        )

    def explain_work_coverage_record(self, cr):
        self._explain_coverage_record(
            cr.timestamp, None, cr.operation, cr.status, cr.exception
        )

    def _explain_coverage_record(
        self, timestamp, data_source, operation, status, exception
    ):
        timestamp = timestamp.strftime(self.TIME_FORMAT)
        if data_source:
            data_source = data_source.name + " | "
        else:
            data_source = ""
        if operation:
            operation = operation + " | "
        else:
            operation = ""
        if exception:
            exception = " | " + exception
        else:
            exception = ""
        self.write(
            "   {} | {}{}{}{}".format(
                timestamp, data_source, operation, status, exception
            )
        )


class WhereAreMyBooksScript(CollectionInputScript):
    """Try to figure out why Works aren't showing up.

    This is a common problem on a new installation or when a new collection
    is being configured.
    """

    def __init__(self, _db=None, output=None, search=None):
        _db = _db or self._db
        super().__init__(_db)
        self.output = output or sys.stdout
        try:
            self.search = search or ExternalSearchIndex(_db)
        except CannotLoadConfiguration:
            self.out(
                "Here's your problem: the search integration is missing or misconfigured."
            )
            raise

    def out(self, s, *args):
        if not s.endswith("\n"):
            s += "\n"
        self.output.write(s % args)

    def run(self, cmd_args=None):
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args or [])

        # Check each library.
        libraries = self._db.query(Library).all()
        if libraries:
            for library in libraries:
                self.check_library(library)
                self.out("\n")
        else:
            self.out("There are no libraries in the system -- that's a problem.")
        self.delete_cached_feeds()
        self.out("\n")
        collections = parsed.collections or self._db.query(Collection)
        for collection in collections:
            self.explain_collection(collection)
            self.out("\n")

    def check_library(self, library):
        """Make sure a library is properly set up to show works."""
        self.out("Checking library %s", library.name)

        # Make sure it has collections.
        if not library.collections:
            self.out(" This library has no collections -- that's a problem.")
        else:
            for collection in library.collections:
                self.out(" Associated with collection %s.", collection.name)

        # Make sure it has lanes.
        if not library.lanes:
            self.out(" This library has no lanes -- that's a problem.")
        else:
            self.out(" Associated with %s lanes.", len(library.lanes))

    def delete_cached_feeds(self):
        page_feeds = self._db.query(CachedFeed).filter(
            CachedFeed.type != CachedFeed.GROUPS_TYPE
        )
        page_feeds_count = page_feeds.count()
        self.out(
            "%d feeds in cachedfeeds table, not counting grouped feeds.",
            page_feeds_count,
        )
        if page_feeds_count:
            self.out(" Deleting them all.")
            page_feeds.delete()
            self._db.commit()

    def explain_collection(self, collection):
        self.out('Examining collection "%s"', collection.name)

        base = (
            self._db.query(Work)
            .join(LicensePool)
            .filter(LicensePool.collection == collection)
        )

        ready = base.filter(Work.presentation_ready == True)
        unready = base.filter(Work.presentation_ready == False)

        ready_count = ready.count()
        unready_count = unready.count()
        self.out(" %d presentation-ready works.", ready_count)
        self.out(" %d works not presentation-ready.", unready_count)

        # Check if the works have delivery mechanisms.
        LPDM = LicensePoolDeliveryMechanism
        no_delivery_mechanisms = base.filter(
            ~exists().where(
                and_(
                    LicensePool.data_source_id == LPDM.data_source_id,
                    LicensePool.identifier_id == LPDM.identifier_id,
                )
            )
        ).count()
        if no_delivery_mechanisms > 0:
            self.out(
                " %d works are missing delivery mechanisms and won't show up.",
                no_delivery_mechanisms,
            )

        # Check if the license pools are suppressed.
        suppressed = base.filter(LicensePool.suppressed == True).count()
        if suppressed > 0:
            self.out(
                " %d works have suppressed LicensePools and won't show up.", suppressed
            )

        # Check if the pools have available licenses.
        not_owned = base.filter(
            and_(LicensePool.licenses_owned == 0, ~LicensePool.open_access)
        ).count()
        if not_owned > 0:
            self.out(
                " %d non-open-access works have no owned licenses and won't show up.",
                not_owned,
            )

        filter = Filter(collections=[collection])
        count = self.search.count_works(filter)
        self.out(
            " %d works in the search index, expected around %d.", count, ready_count
        )


class ListCollectionMetadataIdentifiersScript(CollectionInputScript):
    """List the metadata identifiers for Collections in the database.

    This script is helpful for accounting for and tracking collections on
    the metadata wrangler.
    """

    def __init__(self, _db=None, output=None):
        _db = _db or self._db
        super().__init__(_db)
        self.output = output or sys.stdout

    def run(self, cmd_args=None):
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        self.do_run(parsed.collections)

    def do_run(self, collections=None):
        collection_ids = list()
        if collections:
            collection_ids = [c.id for c in collections]

        collections = self._db.query(Collection).order_by(Collection.id)
        if collection_ids:
            collections = collections.filter(Collection.id.in_(collection_ids))

        self.output.write("COLLECTIONS\n")
        self.output.write("=" * 50 + "\n")

        def add_line(id, name, protocol, metadata_identifier):
            line = f"({id}) {name}/{protocol} => {metadata_identifier}\n"
            self.output.write(line)

        count = 0
        for collection in collections:
            if not count:
                # Add a format line.
                add_line("id", "name", "protocol", "metadata_identifier")

            count += 1
            add_line(
                str(collection.id),
                collection.name,
                collection.protocol,
                collection.metadata_identifier,
            )

        self.output.write("\n%d collections found.\n" % count)


class UpdateLaneSizeScript(LaneSweeperScript):
    def should_process_lane(self, lane):
        """We don't want to process generic WorkLists -- there's nowhere
        to store the data.
        """
        return isinstance(lane, Lane)

    def process_lane(self, lane):
        """Update the estimated size of a Lane."""

        # We supress the configuration changes updates, as each lane is updated
        # and call the site_configuration_has_changed function once after this
        # script has finished running.
        #
        # This is done because calling site_configuration_has_changed repeatedly
        # was causing performance problems, when we have lots of lanes to update.
        lane._suppress_before_flush_listeners = True
        lane.update_size(self._db)
        self.log.info("%s: %d", lane.full_identifier, lane.size)

    def do_run(self, *args, **kwargs):
        super().do_run(*args, **kwargs)
        site_configuration_has_changed(self._db)


class UpdateCustomListSizeScript(CustomListSweeperScript):
    def process_custom_list(self, custom_list):
        custom_list.update_size(self._db)


class RemovesSearchCoverage:
    """Mix-in class for a script that might remove all coverage records
    for the search engine.
    """

    def remove_search_coverage_records(self):
        """Delete all search coverage records from the database.

        :return: The number of records deleted.
        """
        wcr = WorkCoverageRecord
        clause = wcr.operation == wcr.UPDATE_SEARCH_INDEX_OPERATION
        count = self._db.query(wcr).filter(clause).count()

        # We want records to be updated in ascending order in order to avoid deadlocks.
        # To guarantee lock order, we explicitly acquire locks by using a subquery with FOR UPDATE (with_for_update).
        # Please refer for my details to this SO article:
        # https://stackoverflow.com/questions/44660368/postgres-update-with-order-by-how-to-do-it
        self._db.execute(
            wcr.__table__.delete().where(
                wcr.id.in_(
                    self._db.query(wcr.id)
                    .with_for_update()
                    .filter(clause)
                    .order_by(WorkCoverageRecord.id)
                )
            )
        )

        return count


class RebuildSearchIndexScript(RunWorkCoverageProviderScript, RemovesSearchCoverage):
    """Completely delete the search index and recreate it."""

    def __init__(self, *args, **kwargs):
        search = kwargs.get("search_index_client", None)
        self.search = search or ExternalSearchIndex(self._db)
        super().__init__(SearchIndexCoverageProvider, *args, **kwargs)

    def do_run(self):
        # Calling setup_index will destroy the index and recreate it
        # empty.
        self.search.setup_index()

        # Remove all search coverage records so the
        # SearchIndexCoverageProvider will start from scratch.
        count = self.remove_search_coverage_records()
        self.log.info("Deleted %d search coverage records.", count)

        # Now let the SearchIndexCoverageProvider do its thing.
        return super().do_run()


class SearchIndexCoverageRemover(TimestampScript, RemovesSearchCoverage):
    """Script that removes search index coverage for all works.

    This guarantees the SearchIndexCoverageProvider will add
    fresh coverage for every Work the next time it runs.
    """

    def do_run(self):
        count = self.remove_search_coverage_records()
        return TimestampData(
            achievements="Coverage records deleted: %(deleted)d" % dict(deleted=count)
        )


class GenerateOverdriveAdvantageAccountList(InputScript):
    """Generates a CSV containing the following fields:
    circulation manager
    collection
    client_key
    external_account_id
    library_token
    advantage_name
    advantage_id
    advantage_token
    already_configured
    """

    def __init__(self, _db=None, *args, **kwargs):
        super().__init__(_db, args, kwargs)
        self._data: List[List[str]] = list()

    def _create_overdrive_api(self, c: Collection):
        return OverdriveCoreAPI(_db=self._db, collection=c)

    def do_run(self, *args, **kwargs):
        parsed = GenerateOverdriveAdvantageAccountList.parse_command_line(
            _db=self._db, *args, **kwargs
        )
        query: Query = Collection.by_protocol(
            self._db, protocol=ExternalIntegration.OVERDRIVE
        )
        for c in query.filter(Collection.parent_id == None):
            collection: Collection = c
            api = self._create_overdrive_api(collection=collection)
            client_key = api.client_key().decode()
            client_secret = api.client_secret().decode()

            try:
                library_token = api.collection_token
                advantage_accounts = api.get_advantage_accounts()

                for aa in advantage_accounts:
                    existing_child_collections = query.filter(
                        Collection.parent_id == collection.id
                    )
                    already_configured_aa_libraries = [
                        e.external_account_id for e in existing_child_collections
                    ]
                    self._data.append(
                        [
                            collection.name,
                            collection.external_account_id,
                            client_key,
                            client_secret,
                            library_token,
                            aa.name,
                            aa.library_id,
                            aa.token,
                            aa.library_id in already_configured_aa_libraries,
                        ]
                    )
            except Exception as e:
                logging.error(
                    f"Could not connect to collection {c.name}: reason: {str(e)}."
                )

        file_path = parsed.output_file_path[0]
        circ_manager_name = parsed.circulation_manager_name[0]
        self.write_csv(output_file_path=file_path, circ_manager_name=circ_manager_name)

    def write_csv(self, output_file_path: str, circ_manager_name: str):
        with open(output_file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    "cm",
                    "collection",
                    "overdrive_library_id",
                    "client_key",
                    "client_secret",
                    "library_token",
                    "advantage_name",
                    "advantage_id",
                    "advantage_token",
                    "already_configured",
                ]
            )
            for i in self._data:
                i.insert(0, circ_manager_name)
                writer.writerow(i)

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--output-file-path",
            help="The path of an output file",
            metavar="o",
            nargs=1,
        )

        parser.add_argument(
            "--circulation-manager-name",
            help="The name of the circulation-manager",
            metavar="c",
            nargs=1,
            required=True,
        )

        parser.add_argument(
            "--file-format",
            help="The file format of the output file",
            metavar="f",
            nargs=1,
            default="csv",
        )

        return parser


class CustomListUpdateEntriesScript(CustomListSweeperScript):
    """Traverse all entries and update lists if they have auto_update_enabled"""

    def process_custom_list(self, custom_list: CustomList):
        if not custom_list.auto_update_enabled:
            return
        try:
            self.log.info(f"Auto updating list entries for: {custom_list.name}")
            self._update_list_with_new_entries(custom_list)
        except Exception:
            self.log.exception(f"Could not auto update {custom_list.name}")

    def _update_list_with_new_entries(self, custom_list: CustomList):
        """Run a search on a custom list, assuming we have auto_update_enabled with a valid query
        Only json type queries are supported right now, without any support for additional facets"""

        start_page = 1
        json_query = None
        if custom_list.auto_update_status == CustomList.INIT:
            # We're in the init phase, we need to back-populate all titles
            # starting from page 2, since page 1 should be already populated
            start_page = 2
        elif custom_list.auto_update_status == CustomList.REPOPULATE:
            # During a repopulate phase we must empty the list
            # and start population from page 1
            for entry in custom_list.entries:
                self._db.delete(entry)
            custom_list.entries = []
        else:
            # Otherwise we are in an update type process, which means we only search for
            # "newer" books from the last time we updated the list
            try:
                if custom_list.auto_update_query:
                    json_query = json.loads(custom_list.auto_update_query)
                else:
                    return
            except json.JSONDecodeError as e:
                self.log.error(
                    f"Could not decode custom list({custom_list.id}) saved query: {e}"
                )
                return
            # Update availability time as a query part that allows us to filter for new licenses
            # Although the last_update should never be null, we're failsafing
            availability_time = (
                custom_list.auto_update_last_update or datetime.datetime.now()
            )
            query_part = json_query["query"]
            query_part = {
                "and": [
                    {
                        "key": "licensepools.availability_time",
                        "op": "gte",
                        "value": availability_time.timestamp(),
                    },
                    query_part,
                ]
            }
            # Update the query as such
            json_query["query"] = query_part

        CustomListQueries.populate_query_pages(
            self._db, custom_list, json_query=json_query, start_page=start_page
        )
        custom_list.auto_update_status = CustomList.UPDATED


class AlembicMigrateVersion(Script):
    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser(
            prog="Alembic Database Migration",
            description="By default, running this script without any arguments "
            "will run an 'upgrade head' command from alembic",
        )
        parser.add_argument(
            "-d",
            "--downgrade",
            help="Downgrade to a specific version.",
            required=False,
            default=None,
        )
        parser.add_argument(
            "-u",
            "--upgrade",
            help="Upgrade to a specific version.",
            required=False,
            default="head",
        )
        return parser

    def do_run(self, cmd_args=None):
        args = self.parse_command_line(cmd_args=cmd_args)
        config = AlembicConfig(
            str(Path(__file__).parent.parent.absolute() / "alembic.ini")
        )
        try:
            if args.downgrade is not None:
                downgrade(config, args.downgrade)
            elif args.upgrade is not None:
                upgrade(config, args.upgrade)
        except CommandError as e:
            print(f"Error: {e}. No migrations performed.")


class DeleteInvisibleLanesScript(LibraryInputScript):
    """Delete lanes that are flagged as invisible"""

    def process_library(self, library):

        try:
            for lane in self._db.query(Lane).filter(Lane.library_id == library.id):
                if not lane.visible:
                    self._db.delete(lane)
            self._db.commit()
            logging.info(f"Completed hidden lane deletion for {library.short_name}")
        except Exception as e:
            try:
                logging.exception(
                    f"hidden lane deletion failed for {library.short_name}. "
                    f"Attempting to rollback updates",
                    e,
                )
                self._db.rollback()
            except Exception as e:
                logging.exception(
                    f"hidden lane deletion rollback for {library.short_name} failed", e
                )


class MockStdin:
    """Mock a list of identifiers passed in on standard input."""

    def __init__(self, *lines):
        self.lines = lines

    def readlines(self):
        lines = self.lines
        self.lines = []
        return lines
