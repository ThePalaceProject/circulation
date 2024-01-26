import argparse
import datetime
import logging
import os
import sys
import time
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import inspect, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from alembic import command, config
from alembic.util import CommandError
from api.adobe_vendor_id import AuthdataUtility
from api.authenticator import LibraryAuthenticator
from api.axis import Axis360BibliographicCoverageProvider
from api.bibliotheca import BibliothecaCirculationSweep
from api.config import CannotLoadConfiguration, Configuration
from api.lanes import create_default_lanes
from api.local_analytics_exporter import LocalAnalyticsExporter
from api.metadata.novelist import NoveListAPI
from api.metadata.nyt import NYTBestSellerAPI
from api.opds_for_distributors import (
    OPDSForDistributorsImporter,
    OPDSForDistributorsImportMonitor,
    OPDSForDistributorsReaperMonitor,
)
from api.overdrive import OverdriveAPI
from core.integration.goals import Goals
from core.lane import Lane
from core.marc import Annotator as MarcAnnotator
from core.marc import MARCExporter, MarcExporterLibrarySettings, MarcExporterSettings
from core.model import (
    LOCK_ID_DB_INIT,
    CirculationEvent,
    Collection,
    ConfigurationSetting,
    Contribution,
    DataSource,
    DiscoveryServiceRegistration,
    Edition,
    Hold,
    Identifier,
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    Library,
    LicensePool,
    Loan,
    MarcFile,
    Patron,
    SessionManager,
    get_one,
    pg_advisory_lock,
)
from core.scripts import (
    IdentifierInputScript,
    LibraryInputScript,
    OPDSImportScript,
    PatronInputScript,
)
from core.scripts import Script as CoreScript
from core.scripts import TimestampScript
from core.service.container import container_instance
from core.util import LanguageCodes
from core.util.datetime_helpers import utc_now


class Script(CoreScript):
    ...


class MetadataCalculationScript(Script):

    """Force calculate_presentation() to be called on some set of Editions.

    This assumes that the metadata is in already in the database and
    will fall into place if we just call
    Edition.calculate_presentation() and Edition.calculate_work() and
    Work.calculate_presentation().

    Most of these will be data repair scripts that do not need to be run
    regularly.

    """

    name = "Metadata calculation script"

    def q(self):
        raise NotImplementedError()

    def run(self):
        q = self.q()
        search_index_client = self.services.search.index()
        self.log.info("Attempting to repair metadata for %d works" % q.count())

        success = 0
        failure = 0
        also_created_work = 0

        def checkpoint():
            self._db.commit()
            self.log.info(
                "%d successes, %d failures, %d new works.",
                success,
                failure,
                also_created_work,
            )

        i = 0
        for edition in q:
            edition.calculate_presentation()
            if edition.sort_author:
                success += 1
                work, is_new = edition.license_pool.calculate_work(
                    search_index_client=search_index_client
                )
                if work:
                    work.calculate_presentation()
                    if is_new:
                        also_created_work += 1
            else:
                failure += 1
            i += 1
            if not i % 1000:
                checkpoint()
        checkpoint()


class FillInAuthorScript(MetadataCalculationScript):
    """Fill in Edition.sort_author for Editions that have a list of
    Contributors, but no .sort_author.

    This is a data repair script that should not need to be run
    regularly.
    """

    name = "Fill in missing authors"

    def q(self):
        return (
            self._db.query(Edition)
            .join(Edition.contributions)
            .join(Contribution.contributor)
            .filter(Edition.sort_author == None)
        )


class CacheMARCFiles(LibraryInputScript):
    """Generate and cache MARC files for each input library."""

    name = "Cache MARC files"

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:  # type: ignore[override]
        parser = super().arg_parser(_db)
        parser.add_argument(
            "--force",
            help="Generate new MARC files even if MARC files have already been generated recently enough",
            dest="force",
            action="store_true",
        )
        return parser

    def __init__(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str] | None = None,
        exporter: MARCExporter | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(_db, *args, **kwargs)
        self.force = False
        self.parse_args(cmd_args)
        self.storage_service = self.services.storage.public()

        self.cm_base_url = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY
        ).value

        self.exporter = exporter or MARCExporter(self._db, self.storage_service)

    def parse_args(self, cmd_args: Sequence[str] | None = None) -> argparse.Namespace:
        parser = self.arg_parser(self._db)
        parsed = parser.parse_args(cmd_args)
        self.force = parsed.force
        return parsed

    def settings(
        self, library: Library
    ) -> tuple[MarcExporterSettings, MarcExporterLibrarySettings]:
        integration_query = (
            select(IntegrationLibraryConfiguration)
            .join(IntegrationConfiguration)
            .where(
                IntegrationConfiguration.goal == Goals.CATALOG_GOAL,
                IntegrationConfiguration.protocol == MARCExporter.__name__,
                IntegrationLibraryConfiguration.library == library,
            )
        )
        integration = self._db.execute(integration_query).scalar_one()

        library_settings = MARCExporter.library_settings_load(integration)
        settings = MARCExporter.settings_load(integration.parent)

        return settings, library_settings

    def process_libraries(self, libraries: Sequence[Library]) -> None:
        if not self.storage_service:
            self.log.info("No storage service was found.")
            return

        super().process_libraries(libraries)

    def get_collections(self, library: Library) -> Sequence[Collection]:
        return self._db.scalars(
            select(Collection).where(
                Collection.libraries.contains(library),
                Collection.export_marc_records == True,
            )
        ).all()

    def get_web_client_urls(
        self, library: Library, url: str | None = None
    ) -> list[str]:
        """Find web client URLs configured by the registry for this library."""
        urls = [
            s.web_client
            for s in self._db.execute(
                select(DiscoveryServiceRegistration.web_client).where(
                    DiscoveryServiceRegistration.library == library,
                    DiscoveryServiceRegistration.web_client != None,
                )
            ).all()
        ]

        if url:
            urls.append(url)

        return urls

    def process_library(
        self, library: Library, annotator_cls: type[MarcAnnotator] = MarcAnnotator
    ) -> None:
        try:
            settings, library_settings = self.settings(library)
        except NoResultFound:
            return

        self.log.info("Processing library %s" % library.name)

        update_frequency = int(settings.update_frequency)

        # Find the collections for this library.
        collections = self.get_collections(library)

        # Find web client URLs configured by the registry for this library.
        web_client_urls = self.get_web_client_urls(
            library, library_settings.web_client_url
        )

        annotator = annotator_cls(
            self.cm_base_url,
            library.short_name or "",
            web_client_urls,
            library_settings.organization_code,
            library_settings.include_summary,
            library_settings.include_genres,
        )

        # We set the creation time to be the start of the batch. Any updates that happen during the batch will be
        # included in the next batch.
        creation_time = utc_now()

        for collection in collections:
            self.process_collection(
                library,
                collection,
                annotator,
                update_frequency,
                creation_time,
            )

    def last_updated(
        self, library: Library, collection: Collection
    ) -> datetime.datetime | None:
        """Find the most recent MarcFile creation time."""
        last_updated_file = self._db.execute(
            select(MarcFile.created)
            .where(
                MarcFile.library == library,
                MarcFile.collection == collection,
            )
            .order_by(MarcFile.created.desc())
        ).first()

        return last_updated_file.created if last_updated_file else None

    def process_collection(
        self,
        library: Library,
        collection: Collection,
        annotator: MarcAnnotator,
        update_frequency: int,
        creation_time: datetime.datetime,
    ) -> None:
        last_update = self.last_updated(library, collection)

        if (
            not self.force
            and last_update
            and (last_update > creation_time - timedelta(days=update_frequency))
        ):
            self.log.info(
                f"Skipping collection {collection.name} because last update was less than {update_frequency} days ago"
            )
            return

        # First update the file with ALL the records.
        self.exporter.records(
            library, collection, annotator, creation_time=creation_time
        )

        # Then create a new file with changes since the last update.
        if last_update:
            self.exporter.records(
                library,
                collection,
                annotator,
                creation_time=creation_time,
                since_time=last_update,
            )

        self._db.commit()
        self.log.info("Processed collection %s" % collection.name)


class AdobeAccountIDResetScript(PatronInputScript):
    @classmethod
    def arg_parser(cls, _db):
        parser = super().arg_parser(_db)
        parser.add_argument(
            "--delete",
            help="Actually delete credentials as opposed to showing what would happen.",
            action="store_true",
        )
        return parser

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        patrons = parsed.patrons
        self.delete = parsed.delete
        if not self.delete:
            self.log.info(
                "This is a dry run. Nothing will actually change in the database."
            )
            self.log.info("Run with --delete to change the database.")

        if patrons and self.delete:
            self.log.warn(
                """This is not a drill.
Running this script will permanently disconnect %d patron(s) from their Adobe account IDs.
They will be unable to fulfill any existing loans that involve Adobe-encrypted files.
Sleeping for five seconds to give you a chance to back out.
You'll get another chance to back out before the database session is committed.""",
                len(patrons),
            )
            time.sleep(5)
        self.process_patrons(patrons)
        if self.delete:
            self.log.warn("All done. Sleeping for five seconds before committing.")
            time.sleep(5)
            self._db.commit()

    def process_patron(self, patron):
        """Delete all of a patron's Credentials that contain an Adobe account
        ID _or_ connect the patron to a DelegatedPatronIdentifier that
        contains an Adobe account ID.
        """
        self.log.info(
            'Processing patron "%s"',
            patron.authorization_identifier
            or patron.username
            or patron.external_identifier,
        )
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self.log.info(
                ' Deleting "%s" credential "%s"', credential.type, credential.credential
            )
            if self.delete:
                self._db.delete(credential)


class AvailabilityRefreshScript(IdentifierInputScript):
    """Refresh the availability information for a LicensePool, direct from the
    license source.
    """

    def do_run(self):
        args = self.parse_command_line(self._db)
        if not args.identifiers:
            raise Exception("You must specify at least one identifier to refresh.")

        # We don't know exactly how big to make these batches, but 10 is
        # always safe.
        start = 0
        size = 10
        while start < len(args.identifiers):
            batch = args.identifiers[start : start + size]
            self.refresh_availability(batch)
            self._db.commit()
            start += size

    def refresh_availability(self, identifiers):
        provider = None
        identifier = identifiers[0]
        if identifier.type == Identifier.THREEM_ID:
            sweeper = BibliothecaCirculationSweep(self._db)
            sweeper.process_batch(identifiers)
        elif identifier.type == Identifier.OVERDRIVE_ID:
            api = OverdriveAPI(self._db)
            for identifier in identifiers:
                api.update_licensepool(identifier.identifier)
        elif identifier.type == Identifier.AXIS_360_ID:
            provider = Axis360BibliographicCoverageProvider(self._db)
            provider.process_batch(identifiers)
        else:
            self.log.warn("Cannot update coverage for %r" % identifier.type)


class LanguageListScript(LibraryInputScript):
    """List all the languages with at least one non-open access work
    in the collection.
    """

    def process_library(self, library):
        print(library.short_name)
        for item in self.languages(library):
            print(item)

    def languages(self, library):
        ":yield: A list of output lines, one per language."
        for abbreviation, count in library.estimated_holdings_by_language(
            include_open_access=False
        ).most_common():
            display_name = LanguageCodes.name_for_languageset(abbreviation)
            yield "%s %i (%s)" % (abbreviation, count, display_name)


class CompileTranslationsScript(Script):
    """A script to combine translation files for circulation, core
    and the admin interface, and compile the result to be used by the
    app. The combination step is necessary because Flask-Babel does not
    support multiple domains yet.
    """

    def run(self):
        languages = Configuration.localization_languages()
        for language in languages:
            base_path = "translations/%s/LC_MESSAGES" % language
            if not os.path.exists(base_path):
                logging.warn("No translations for configured language %s" % language)
                continue

            os.system("rm %(path)s/messages.po" % dict(path=base_path))
            os.system("cat %(path)s/*.po > %(path)s/messages.po" % dict(path=base_path))

        os.system("pybabel compile -f -d translations")


class InstanceInitializationScript:
    """An idempotent script to initialize an instance of the Circulation Manager.

    This script is intended for use in servers, Docker containers, etc,
    when the Circulation Manager app is being installed. It initializes
    the database and sets an appropriate alias on the OpenSearch index.

    Because it's currently run every time a container is started, it must
    remain idempotent.
    """

    def __init__(self) -> None:
        self._log: logging.Logger | None = None
        self._container = container_instance()

        # Call init_resources() to initialize the logging configuration.
        self._container.init_resources()

    @property
    def log(self) -> logging.Logger:
        if self._log is None:
            self._log = logging.getLogger(
                f"{self.__module__}.{self.__class__.__name__}"
            )
        return self._log

    @staticmethod
    def _get_alembic_config(connection: Connection) -> config.Config:
        """Get the Alembic config object for the current app."""
        conf = config.Config(str(Path(__file__).parent.absolute() / "alembic.ini"))
        conf.attributes["configure_logger"] = False
        conf.attributes["connection"] = connection.engine
        conf.attributes["need_lock"] = False
        return conf

    def migrate_database(self, connection: Connection) -> None:
        """Run our database migrations to make sure the database is up-to-date."""
        alembic_conf = self._get_alembic_config(connection)
        command.upgrade(alembic_conf, "head")

    def initialize_database(self, connection: Connection) -> None:
        """
        Initialize the database, creating tables, loading default data and then
        stamping the most recent migration as the current state of the DB.
        """
        SessionManager.initialize_schema(connection)

        with Session(connection) as session:
            # Initialize the database with default data
            SessionManager.initialize_data(session)

            # Create a secret key if one doesn't already exist.
            ConfigurationSetting.sitewide_secret(session, Configuration.SECRET_KEY)

        # Stamp the most recent migration as the current state of the DB
        alembic_conf = self._get_alembic_config(connection)
        command.stamp(alembic_conf, "head")

    def initialize_search_indexes(self) -> bool:
        search = self._container.search.index()
        return search.initialize_indices()

    def initialize(self, connection: Connection):
        """Initialize the database if necessary."""
        inspector = inspect(connection)
        if inspector.has_table("alembic_version"):
            self.log.info("Database schema already exists. Running migrations.")
            try:
                self.migrate_database(connection)
                self.log.info("Migrations complete.")
            except CommandError as e:
                self.log.error(
                    f"Error running database migrations: {str(e)}. This "
                    f"is possibly because you are running a old version "
                    f"of the application against a new database."
                )
        else:
            self.log.info("Database schema does not exist. Initializing.")
            self.initialize_database(connection)
            self.log.info("Initialization complete.")

        self.initialize_search_indexes()

    def run(self) -> None:
        """
        Initialize the database if necessary. This script is idempotent, so it
        can be run every time the app starts.

        The script uses a PostgreSQL advisory lock to ensure that only one
        instance of the script is running at a time. This prevents multiple
        instances from trying to initialize the database at the same time.
        """
        engine = SessionManager.engine()
        with engine.begin() as connection:
            with pg_advisory_lock(connection, LOCK_ID_DB_INIT):
                self.initialize(connection)

        engine.dispose()


class LoanReaperScript(TimestampScript):
    """Remove expired loans and holds whose owners have not yet synced
    with the loan providers.

    This stops the library from keeping a record of the final loans and
    holds of a patron who stopped using the circulation manager.

    If a loan or (more likely) hold is removed incorrectly, it will be
    restored the next time the patron syncs their loans feed.
    """

    name = "Remove expired loans and holds from local database"

    def do_run(self):
        now = utc_now()

        # Reap loans and holds that we know have expired.
        for obj, what in ((Loan, "loans"), (Hold, "holds")):
            qu = self._db.query(obj).filter(obj.end < now)
            self._reap(qu, "expired %s" % what)

        for obj, what, max_age in (
            (Loan, "loans", timedelta(days=90)),
            (Hold, "holds", timedelta(days=365)),
        ):
            # Reap loans and holds which have no end date and are very
            # old. It's very likely these loans and holds have expired
            # and we simply don't have the information.
            older_than = now - max_age
            qu = (
                self._db.query(obj)
                .join(obj.license_pool)
                .filter(obj.end == None)
                .filter(obj.start < older_than)
                .filter(LicensePool.open_access == False)
            )
            explain = "{} older than {}".format(what, older_than.strftime("%Y-%m-%d"))
            self._reap(qu, explain)

    def _reap(self, qu, what):
        """Delete every database object that matches the given query.

        :param qu: The query that yields objects to delete.
        :param what: A human-readable explanation of what's being
                     deleted.
        """
        counter = 0
        print("Reaping %d %s." % (qu.count(), what))
        for o in qu:
            self._db.delete(o)
            counter += 1
            if not counter % 100:
                print(counter)
                self._db.commit()
        self._db.commit()


class DisappearingBookReportScript(Script):

    """Print a TSV-format report on books that used to be in the
    collection, or should be in the collection, but aren't.
    """

    def do_run(self):
        qu = (
            self._db.query(LicensePool)
            .filter(LicensePool.open_access == False)
            .filter(LicensePool.suppressed == False)
            .filter(LicensePool.licenses_owned <= 0)
            .order_by(LicensePool.availability_time.desc())
        )
        first_row = [
            "Identifier",
            "Title",
            "Author",
            "First seen",
            "Last seen (best guess)",
            "Current licenses owned",
            "Current licenses available",
            "Changes in number of licenses",
            "Changes in title availability",
        ]
        print("\t".join(first_row))

        for pool in qu:
            self.explain(pool)

    def investigate(self, licensepool):
        """Find when the given LicensePool might have disappeared from the
        collection.

        :param licensepool: A LicensePool.

        :return: a 3-tuple (last_seen, title_removal_events,
            license_removal_events).

        `last_seen` is the latest point at which we knew the book was
        circulating. If we never knew the book to be circulating, this
        is the first time we ever saw the LicensePool.

        `title_removal_events` is a query that returns CirculationEvents
        in which this LicensePool was removed from the remote collection.

        `license_removal_events` is a query that returns
        CirculationEvents in which LicensePool.licenses_owned went
        from having a positive number to being zero or a negative
        number.
        """
        first_activity = None
        most_recent_activity = None

        # If we have absolutely no information about the book ever
        # circulating, we act like we lost track of the book
        # immediately after seeing it for the first time.
        last_seen = licensepool.availability_time

        # If there's a recorded loan or hold on the book, that can
        # push up the last time the book was known to be circulating.
        for l in (licensepool.loans, licensepool.holds):
            for item in l:
                if not last_seen or item.start > last_seen:
                    last_seen = item.start

        # Now we look for relevant circulation events. First, an event
        # where the title was explicitly removed is pretty clearly
        # a 'last seen'.
        base_query = (
            self._db.query(CirculationEvent)
            .filter(CirculationEvent.license_pool == licensepool)
            .order_by(CirculationEvent.start.desc())
        )
        title_removal_events = base_query.filter(
            CirculationEvent.type == CirculationEvent.DISTRIBUTOR_TITLE_REMOVE
        )
        if title_removal_events.count():
            candidate = title_removal_events[-1].start
            if not last_seen or candidate > last_seen:
                last_seen = candidate

        # Also look for an event where the title went from a nonzero
        # number of licenses to a zero number of licenses. That's a
        # good 'last seen'.
        license_removal_events = (
            base_query.filter(
                CirculationEvent.type == CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE,
            )
            .filter(CirculationEvent.old_value > 0)
            .filter(CirculationEvent.new_value <= 0)
        )
        if license_removal_events.count():
            candidate = license_removal_events[-1].start
            if not last_seen or candidate > last_seen:
                last_seen = candidate

        return last_seen, title_removal_events, license_removal_events

    format = "%Y-%m-%d"

    def explain(self, licensepool):
        edition = licensepool.presentation_edition
        identifier = licensepool.identifier
        last_seen, title_removal_events, license_removal_events = self.investigate(
            licensepool
        )

        data = [f"{identifier.type} {identifier.identifier}"]
        if edition:
            data.extend([edition.title, edition.author])
        if licensepool.availability_time:
            first_seen = licensepool.availability_time.strftime(self.format)
        else:
            first_seen = ""
        data.append(first_seen)
        if last_seen:
            last_seen = last_seen.strftime(self.format)
        else:
            last_seen = ""
        data.append(last_seen)
        data.append(licensepool.licenses_owned)
        data.append(licensepool.licenses_available)

        license_removals = []
        for event in license_removal_events:
            description = "{}: {}→{}".format(
                event.start.strftime(self.format),
                event.old_value,
                event.new_value,
            )
            license_removals.append(description)
        data.append(", ".join(license_removals))

        title_removals = [
            event.start.strftime(self.format) for event in title_removal_events
        ]
        data.append(", ".join(title_removals))

        print("\t".join([str(x) for x in data]))


class NYTBestSellerListsScript(TimestampScript):
    name = "Update New York Times best-seller lists"

    def __init__(self, include_history=False):
        super().__init__()
        self.include_history = include_history

    def do_run(self):
        self.api = NYTBestSellerAPI.from_config(self._db)
        self.data_source = DataSource.lookup(self._db, DataSource.NYT)
        # For every best-seller list...
        names = self.api.list_of_lists()
        for l in sorted(names["results"], key=lambda x: x["list_name_encoded"]):
            name = l["list_name_encoded"]
            self.log.info("Handling list %s" % name)
            best = self.api.best_seller_list(l)

            if self.include_history:
                self.api.fill_in_history(best)
            else:
                self.api.update(best)

            # Mirror the list to the database.
            customlist = best.to_customlist(self._db)
            self.log.info("Now %s entries in the list.", len(customlist.entries))
            self._db.commit()


class OPDSForDistributorsImportScript(OPDSImportScript):
    """Import all books from the OPDS feed associated with a collection
    that requires authentication."""

    IMPORTER_CLASS = OPDSForDistributorsImporter
    MONITOR_CLASS = OPDSForDistributorsImportMonitor
    PROTOCOL = OPDSForDistributorsImporter.NAME


class OPDSForDistributorsReaperScript(OPDSImportScript):
    """Get all books from the OPDS feed associated with a collection
    to find out if any have been removed."""

    IMPORTER_CLASS = OPDSForDistributorsImporter
    MONITOR_CLASS = OPDSForDistributorsReaperMonitor
    PROTOCOL = OPDSForDistributorsImporter.NAME


class LaneResetScript(LibraryInputScript):
    """Reset a library's lanes based on language configuration or estimates
    of the library's current collection."""

    @classmethod
    def arg_parser(cls, _db):
        parser = LibraryInputScript.arg_parser(_db)
        parser.add_argument(
            "--reset",
            help="Actually reset the lanes as opposed to showing what would happen.",
            action="store_true",
        )
        return parser

    def do_run(self, output=sys.stdout, **kwargs):
        parsed = self.parse_command_line(self._db, **kwargs)
        libraries = parsed.libraries
        self.reset = parsed.reset
        if not self.reset:
            self.log.info(
                "This is a dry run. Nothing will actually change in the database."
            )
            self.log.info("Run with --reset to change the database.")

        if libraries and self.reset:
            self.log.warn(
                """This is not a drill.
Running this script will permanently reset the lanes for %d libraries. Any lanes created from
custom lists will be deleted (though the lists themselves will be preserved).
Sleeping for five seconds to give you a chance to back out.
You'll get another chance to back out before the database session is committed.""",
                len(libraries),
            )
            time.sleep(5)
        self.process_libraries(libraries)

        new_lane_output = "New Lane Configuration:"
        for library in libraries:
            new_lane_output += "\n\nLibrary '%s':\n" % library.name

            def print_lanes_for_parent(parent):
                lanes = (
                    self._db.query(Lane)
                    .filter(Lane.library == library)
                    .filter(Lane.parent == parent)
                    .order_by(Lane.priority)
                )
                lane_output = ""
                for lane in lanes:
                    lane_output += (
                        "  "
                        + ("  " * len(list(lane.parentage)))
                        + lane.display_name
                        + "\n"
                    )
                    lane_output += print_lanes_for_parent(lane)
                return lane_output

            new_lane_output += print_lanes_for_parent(None)

        output.write(new_lane_output)

        if self.reset:
            self.log.warn("All done. Sleeping for five seconds before committing.")
            time.sleep(5)
            self._db.commit()

    def process_library(self, library):
        create_default_lanes(self._db, library)


class NovelistSnapshotScript(TimestampScript, LibraryInputScript):
    def do_run(self, output=sys.stdout, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        for library in parsed.libraries:
            try:
                api = NoveListAPI.from_config(library)
            except CannotLoadConfiguration as e:
                self.log.info(str(e))
                continue
            if api:
                response = api.put_items_novelist(library)

                if response:
                    result = "NoveList API Response\n"
                    result += str(response)

                    output.write(result)


class LocalAnalyticsExportScript(Script):
    """Export circulation events for a date range to a CSV file."""

    @classmethod
    def arg_parser(cls, _db):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--start",
            help="Include circulation events that happened at or after this time.",
            required=True,
        )
        parser.add_argument(
            "--end",
            help="Include circulation events that happened before this time.",
            required=True,
        )
        return parser

    def do_run(self, output=sys.stdout, cmd_args=None, exporter=None):
        parser = self.arg_parser(self._db)
        parsed = parser.parse_args(cmd_args)
        start = parsed.start
        end = parsed.end

        exporter = exporter or LocalAnalyticsExporter()
        output.write(exporter.export(self._db, start, end))


class GenerateShortTokenScript(LibraryInputScript):
    """
    Generate a short client token of the specified duration that can be used for testing that
    involves the Adobe Vendor ID API implementation.
    """

    @classmethod
    def arg_parser(cls, _db):
        parser = super().arg_parser(_db, multiple_libraries=False)
        parser.add_argument(
            "--barcode",
            help="The patron barcode.",
            required=True,
        )
        parser.add_argument("--pin", help="The patron pin.")
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--days",
            help="Token expiry in days.",
            type=int,
        )
        group.add_argument(
            "--hours",
            help="Token expiry in hours.",
            type=int,
        )
        group.add_argument(
            "--minutes",
            help="Token expiry in minutes.",
            type=int,
        )
        return parser

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout, authdata=None):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)

        if len(args.libraries) != 1:
            output.write("Library not found!\n")
            sys.exit(-1)
        library = args.libraries[0]

        # First try to shortcut full authentication, by just looking up patron directly
        patron = get_one(_db, Patron, authorization_identifier=args.barcode)
        if patron is None:
            # Fall back to a full patron lookup
            auth = LibraryAuthenticator.from_config(
                _db, args.libraries[0]
            ).basic_auth_provider
            if auth is None:
                output.write("No methods to authenticate patron found!\n")
                sys.exit(-1)
            patron = auth.authenticate(
                _db, credentials={"username": args.barcode, "password": args.pin}
            )
            if not isinstance(patron, Patron):
                output.write(f"Patron not found {args.barcode}!\n")
                sys.exit(-1)

        authdata = authdata or AuthdataUtility.from_config(library, _db)
        if authdata is None:
            output.write(
                "Library not registered with library registry! Please register and try again."
            )
            sys.exit(-1)

        patron_identifier = authdata._adobe_patron_identifier(patron)
        expires = {
            k: v
            for (k, v) in vars(args).items()
            if k in ["days", "hours", "minutes"] and v is not None
        }
        vendor_id, token = authdata.encode_short_client_token(
            patron_identifier, expires=expires
        )
        username, password = token.rsplit("|", 1)

        output.write(f"Vendor ID: {vendor_id}\n")
        output.write(f"Token: {token}\n")
        output.write(f"Username: {username}\n")
        output.write(f"Password: {password}\n")
