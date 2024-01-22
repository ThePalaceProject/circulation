from __future__ import annotations

import datetime
import json
import random
from io import StringIO
from unittest.mock import MagicMock, call, patch

import pytest
from freezegun import freeze_time
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import Session

from api.lanes import create_default_lanes
from core.classifier import Classifier
from core.config import Configuration, ConfigurationConstants
from core.external_search import ExternalSearchIndex, Filter
from core.lane import Lane, WorkList
from core.metadata_layer import TimestampData
from core.model import (
    Collection,
    ConfigurationSetting,
    Contributor,
    CoverageRecord,
    DataSource,
    ExternalIntegration,
    Identifier,
    Library,
    LicensePool,
    Timestamp,
    Work,
    WorkCoverageRecord,
    get_one,
    get_one_or_create,
)
from core.model.classification import Classification, Subject
from core.model.customlist import CustomList
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from core.model.patron import Patron
from core.monitor import CollectionMonitor, Monitor, ReaperMonitor
from core.opds_import import OPDSImportMonitor
from core.scripts import (
    AddClassificationScript,
    CheckContributorNamesInDB,
    CollectionArgumentsScript,
    CollectionInputScript,
    ConfigureCollectionScript,
    ConfigureIntegrationScript,
    ConfigureLaneScript,
    ConfigureLibraryScript,
    ConfigureSiteScript,
    CustomListUpdateEntriesScript,
    DeleteInvisibleLanesScript,
    Explain,
    IdentifierInputScript,
    LaneSweeperScript,
    LibraryInputScript,
    LoanNotificationsScript,
    MockStdin,
    OPDSImportScript,
    PatronInputScript,
    RebuildSearchIndexScript,
    ReclassifyWorksForUncheckedSubjectsScript,
    RunCollectionMonitorScript,
    RunCoverageProviderScript,
    RunMonitorScript,
    RunMultipleMonitorsScript,
    RunReaperMonitorsScript,
    RunThreadedCollectionCoverageProviderScript,
    RunWorkCoverageProviderScript,
    Script,
    SearchIndexCoverageRemover,
    ShowCollectionsScript,
    ShowIntegrationsScript,
    ShowLanesScript,
    ShowLibrariesScript,
    TimestampScript,
    UpdateCustomListSizeScript,
    UpdateLaneSizeScript,
    WhereAreMyBooksScript,
    WorkClassificationScript,
    WorkProcessingScript,
)
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.notifications import PushNotifications
from core.util.worker_pools import DatabasePool
from tests.core.mock import (
    AlwaysSuccessfulCollectionCoverageProvider,
    AlwaysSuccessfulWorkCoverageProvider,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixtureFake
from tests.fixtures.services import ServicesFixture


class TestScript:
    def test_parse_time(self):
        reference_date = datetime_utc(2016, 1, 1)

        assert Script.parse_time("2016-01-01") == reference_date
        assert Script.parse_time("2016-1-1") == reference_date
        assert Script.parse_time("1/1/2016") == reference_date
        assert Script.parse_time("20160101") == reference_date

        pytest.raises(ValueError, Script.parse_time, "201601-01")

    def test_script_name(self, db: DatabaseTransactionFixture):
        session = db.session

        class Sample(Script):
            pass

        # If a script does not define .name, its class name
        # is treated as the script name.
        script = Sample(session)
        assert "Sample" == script.script_name

        # If a script does define .name, that's used instead.
        script.name = "I'm a script"
        assert script.name == script.script_name


class TestTimestampScript:
    @staticmethod
    def _ts(session: Session, script):
        """Convenience method to look up the Timestamp for a script.

        We don't use Timestamp.stamp() because we want to make sure
        that Timestamps are being created by the actual code, not test
        code.
        """
        return get_one(session, Timestamp, service=script.script_name)

    def test_update_timestamp(self, db: DatabaseTransactionFixture):
        # Test the Script subclass that sets a timestamp after a
        # script is run.
        class Noisy(TimestampScript):
            def do_run(self):
                pass

        script = Noisy(db.session)
        script.run()

        timestamp = self._ts(db.session, script)

        # The start and end points of do_run() have become
        # Timestamp.start and Timestamp.finish.
        now = utc_now()
        assert (now - timestamp.start).total_seconds() < 5
        assert (now - timestamp.finish).total_seconds() < 5
        assert timestamp.start < timestamp.finish
        assert None == timestamp.collection

    def test_update_timestamp_with_collection(self, db: DatabaseTransactionFixture):
        # A script can indicate that it is operating on a specific
        # collection.
        class MyCollection(TimestampScript):
            def do_run(self):
                pass

        script = MyCollection(db.session)
        script.timestamp_collection = db.default_collection()
        script.run()
        timestamp = self._ts(db.session, script)
        assert db.default_collection() == timestamp.collection

    def test_update_timestamp_on_failure(self, db: DatabaseTransactionFixture):
        # A TimestampScript that fails to complete still has its
        # Timestamp set -- the timestamp just records the time that
        # the script stopped running.
        #
        # This is different from Monitors, where the timestamp
        # is only updated when the Monitor runs to completion.
        # The difference is that Monitors are frequently responsible for
        # keeping track of everything that happened since a certain
        # time, and Scripts generally aren't.
        class Broken(TimestampScript):
            def do_run(self):
                raise Exception("i'm broken")

        script = Broken(db.session)
        with pytest.raises(Exception) as excinfo:
            script.run()
        assert "i'm broken" in str(excinfo.value)
        timestamp = self._ts(db.session, script)

        now = utc_now()
        assert (now - timestamp.finish).total_seconds() < 5

        # A stack trace for the exception has been recorded in the
        # Timestamp object.
        assert "Exception: i'm broken" in timestamp.exception

    def test_normal_script_has_no_timestamp(self, db: DatabaseTransactionFixture):
        # Running a normal script does _not_ set a Timestamp.
        class Silent(Script):
            def do_run(self):
                pass

        script = Silent(db.session)
        script.run()
        assert None == self._ts(db.session, script)


class TestCheckContributorNamesInDB:
    def test_process_contribution_local(self, db: DatabaseTransactionFixture):
        stdin = MockStdin()
        cmd_args = []

        edition_alice, pool_alice = db.edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="1",
            with_open_access_download=True,
            title="Alice Writes Books",
        )

        alice, new = db.contributor(sort_name="Alice Alrighty")
        alice._sort_name = "Alice Alrighty"
        alice.display_name = "Alice Alrighty"

        edition_alice.add_contributor(alice, [Contributor.PRIMARY_AUTHOR_ROLE])
        edition_alice.sort_author = "Alice Rocks"

        # everything is set up as we expect
        assert "Alice Alrighty" == alice.sort_name
        assert "Alice Alrighty" == alice.display_name
        assert "Alice Rocks" == edition_alice.sort_author

        edition_bob, pool_bob = db.edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="2",
            with_open_access_download=True,
            title="Bob Writes Books",
        )

        bob, new = db.contributor(sort_name="Bob")
        bob.display_name = "Bob Bitshifter"

        edition_bob.add_contributor(bob, [Contributor.PRIMARY_AUTHOR_ROLE])
        edition_bob.sort_author = "Bob Rocks"

        assert "Bob" == bob.sort_name
        assert "Bob Bitshifter" == bob.display_name
        assert "Bob Rocks" == edition_bob.sort_author

        contributor_fixer = CheckContributorNamesInDB(
            _db=db.session, cmd_args=cmd_args, stdin=stdin
        )
        contributor_fixer.do_run()

        # Alice got fixed up.
        assert "Alrighty, Alice" == alice.sort_name
        assert "Alice Alrighty" == alice.display_name
        assert "Alrighty, Alice" == edition_alice.sort_author

        # Bob's repairs were too extensive to make.
        assert "Bob" == bob.sort_name
        assert "Bob Bitshifter" == bob.display_name
        assert "Bob Rocks" == edition_bob.sort_author


class TestIdentifierInputScript:
    def test_parse_list_as_identifiers(self, db: DatabaseTransactionFixture):
        i1 = db.identifier()
        i2 = db.identifier()
        args = [i1.identifier, "no-such-identifier", i2.identifier]
        identifiers = IdentifierInputScript.parse_identifier_list(
            db.session, i1.type, None, args
        )
        assert [i1, i2] == identifiers

        assert [] == IdentifierInputScript.parse_identifier_list(
            db.session, i1.type, None, []
        )

    def test_parse_list_as_identifiers_with_autocreate(
        self, db: DatabaseTransactionFixture
    ):
        type = Identifier.OVERDRIVE_ID
        args = ["brand-new-identifier"]
        [i] = IdentifierInputScript.parse_identifier_list(
            db.session, type, None, args, autocreate=True
        )
        assert type == i.type
        assert "brand-new-identifier" == i.identifier

    def test_parse_list_as_identifiers_with_data_source(
        self, db: DatabaseTransactionFixture
    ):
        lp1 = db.licensepool(None, data_source_name=DataSource.UNGLUE_IT)
        lp2 = db.licensepool(None, data_source_name=DataSource.FEEDBOOKS)
        lp3 = db.licensepool(None, data_source_name=DataSource.FEEDBOOKS)

        i1, i2, i3 = (lp.identifier for lp in [lp1, lp2, lp3])
        i1.type = i2.type = Identifier.URI
        source = DataSource.lookup(db.session, DataSource.FEEDBOOKS)

        # Only URIs with a FeedBooks LicensePool are selected.
        identifiers = IdentifierInputScript.parse_identifier_list(
            db.session, Identifier.URI, source, []
        )
        assert [i2] == identifiers

    def test_parse_list_as_identifiers_by_database_id(
        self, db: DatabaseTransactionFixture
    ):
        id1 = db.identifier()
        id2 = db.identifier()

        # Make a list containing two Identifier database IDs,
        # as well as two strings which are not existing Identifier database
        # IDs.
        ids = [id1.id, "10000000", "abcde", id2.id]

        identifiers = IdentifierInputScript.parse_identifier_list(
            db.session, IdentifierInputScript.DATABASE_ID, None, ids
        )
        assert [id1, id2] == identifiers

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        i1 = db.identifier()
        i2 = db.identifier()
        # We pass in one identifier on the command line...
        cmd_args = ["--identifier-type", i1.type, i1.identifier]
        # ...and another one into standard input.
        stdin = MockStdin(i2.identifier)
        parsed = IdentifierInputScript.parse_command_line(db.session, cmd_args, stdin)
        assert [i1, i2] == parsed.identifiers
        assert i1.type == parsed.identifier_type

    def test_parse_command_line_no_identifiers(self, db: DatabaseTransactionFixture):
        cmd_args = [
            "--identifier-type",
            Identifier.OVERDRIVE_ID,
            "--identifier-data-source",
            DataSource.STANDARD_EBOOKS,
        ]
        parsed = IdentifierInputScript.parse_command_line(
            db.session, cmd_args, MockStdin()
        )
        assert [] == parsed.identifiers
        assert Identifier.OVERDRIVE_ID == parsed.identifier_type
        assert DataSource.STANDARD_EBOOKS == parsed.identifier_data_source


class SuccessMonitor(Monitor):
    """A simple Monitor that alway succeeds."""

    SERVICE_NAME = "Success"

    def run(self):
        self.ran = True


class OPDSCollectionMonitor(CollectionMonitor):
    """Mock Monitor for use in tests of Run*MonitorScript."""

    SERVICE_NAME = "Test Monitor"
    PROTOCOL = ExternalIntegration.OPDS_IMPORT

    def __init__(self, _db, test_argument=None, **kwargs):
        self.test_argument = test_argument
        super().__init__(_db, **kwargs)

    def run_once(self, progress):
        self.collection.ran_with_argument = self.test_argument


class DoomedCollectionMonitor(CollectionMonitor):
    """Mock CollectionMonitor that always raises an exception."""

    SERVICE_NAME = "Doomed Monitor"
    PROTOCOL = ExternalIntegration.OPDS_IMPORT

    def run(self, *args, **kwargs):
        self.ran = True
        self.collection.doomed = True
        raise Exception("Doomed!")


class TestCollectionMonitorWithDifferentRunners:
    """CollectionMonitors are usually run by a RunCollectionMonitorScript.
    It's not ideal, but you can also run a CollectionMonitor script from a
    RunMonitorScript. In either case, if no collection argument is specified,
    the monitor will run on every appropriate Collection. If any collection
    names are specified, then the monitor will be run only on the ones specified.
    """

    @pytest.mark.parametrize(
        "name,script_runner",
        [
            ("run CollectionMonitor from RunMonitorScript", RunMonitorScript),
            (
                "run CollectionMonitor from RunCollectionMonitorScript",
                RunCollectionMonitorScript,
            ),
        ],
    )
    def test_run_collection_monitor_with_no_args(self, db, name, script_runner):
        # Run CollectionMonitor via RunMonitor for all applicable collections.
        c1 = db.collection()
        c2 = db.collection()
        script = script_runner(
            OPDSCollectionMonitor, db.session, cmd_args=[], test_argument="test value"
        )
        script.run()
        for c in [c1, c2]:
            assert "test value" == c.ran_with_argument

    @pytest.mark.parametrize(
        "name,script_runner",
        [
            (
                "run CollectionMonitor with collection args from RunMonitorScript",
                RunMonitorScript,
            ),
            (
                "run CollectionMonitor with collection args from RunCollectionMonitorScript",
                RunCollectionMonitorScript,
            ),
        ],
    )
    def test_run_collection_monitor_with_collection_args(self, db, name, script_runner):
        # Run CollectionMonitor via RunMonitor for only specified collections.
        c1 = db.collection(name="Collection 1")
        c2 = db.collection(name="Collection 2")
        c3 = db.collection(name="Collection 3")

        all_collections = [c1, c2, c3]
        monitored_collections = [c1, c3]
        monitored_names = [c.name for c in monitored_collections]
        script = script_runner(
            OPDSCollectionMonitor,
            db.session,
            cmd_args=monitored_names,
            test_argument="test value",
        )
        script.run()
        for c in monitored_collections:
            assert hasattr(c, "ran_with_argument")
            assert "test value" == c.ran_with_argument
        for c in [
            collection
            for collection in all_collections
            if collection not in monitored_collections
        ]:
            assert not hasattr(c, "ran_with_argument")


class TestRunMultipleMonitorsScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        m1 = SuccessMonitor(db.session)
        m2 = DoomedCollectionMonitor(db.session, db.default_collection())
        m3 = SuccessMonitor(db.session)

        class MockScript(RunMultipleMonitorsScript):
            name = "Run three monitors"

            def monitors(self, **kwargs):
                self.kwargs = kwargs
                return [m1, m2, m3]

        # Run the script.
        script = MockScript(db.session, kwarg="value")
        script.do_run()

        # The kwarg we passed in to the MockScript constructor was
        # propagated into the monitors() method.
        assert dict(kwarg="value") == script.kwargs

        # All three monitors were run, even though the
        # second one raised an exception.
        assert True == m1.ran
        assert True == m2.ran
        assert True == m3.ran

        # The exception that crashed the second monitor was stored as
        # .exception, in case we want to look at it.
        assert "Doomed!" == str(m2.exception)
        assert None == getattr(m1, "exception", None)


class TestRunCollectionMonitorScript:
    def test_monitors(self, db: DatabaseTransactionFixture):
        # Here we have three OPDS import Collections...
        o1 = db.collection()
        o2 = db.collection()
        o3 = db.collection()

        # ...and a Bibliotheca collection.
        b1 = db.collection(protocol=ExternalIntegration.BIBLIOTHECA)

        script = RunCollectionMonitorScript(
            OPDSCollectionMonitor, db.session, cmd_args=[]
        )

        # Calling monitors() instantiates an OPDSCollectionMonitor
        # for every OPDS import collection. The Bibliotheca collection
        # is unaffected.
        monitors = script.monitors()
        collections = [x.collection for x in monitors]
        assert set(collections) == {o1, o2, o3}
        for monitor in monitors:
            assert isinstance(monitor, OPDSCollectionMonitor)


class TestRunReaperMonitorsScript:
    def test_monitors(self, db: DatabaseTransactionFixture):
        """This script instantiates a Monitor for every class in
        ReaperMonitor.REGISTRY.
        """
        old_registry = ReaperMonitor.REGISTRY
        ReaperMonitor.REGISTRY = [SuccessMonitor]
        script = RunReaperMonitorsScript(db.session)
        [monitor] = script.monitors()
        assert isinstance(monitor, SuccessMonitor)
        ReaperMonitor.REGISTRY = old_registry


class TestPatronInputScript:
    def test_parse_patron_list(self, db: DatabaseTransactionFixture):
        """Test that patrons can be identified with any unique identifier."""
        l1 = db.library()
        l2 = db.library()
        p1 = db.patron()
        p1.authorization_identifier = db.fresh_str()
        p1.library_id = l1.id
        p2 = db.patron()
        p2.username = db.fresh_str()
        p2.library_id = l1.id
        p3 = db.patron()
        p3.external_identifier = db.fresh_str()
        p3.library_id = l1.id
        p4 = db.patron()
        p4.external_identifier = db.fresh_str()
        p4.library_id = l2.id
        args = [
            p1.authorization_identifier,
            "no-such-patron",
            "",
            p2.username,
            p3.external_identifier,
        ]
        patrons = PatronInputScript.parse_patron_list(db.session, l1, args)
        assert [p1, p2, p3] == patrons
        assert [] == PatronInputScript.parse_patron_list(db.session, l1, [])
        assert [p1] == PatronInputScript.parse_patron_list(
            db.session, l1, [p1.external_identifier, p4.external_identifier]
        )
        assert [p4] == PatronInputScript.parse_patron_list(
            db.session, l2, [p1.external_identifier, p4.external_identifier]
        )

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        l1 = db.library()
        p1 = db.patron()
        p2 = db.patron()
        p1.authorization_identifier = db.fresh_str()
        p2.authorization_identifier = db.fresh_str()
        p1.library_id = l1.id
        p2.library_id = l1.id
        # We pass in one patron identifier on the command line...
        cmd_args = [l1.short_name, p1.authorization_identifier]
        # ...and another one into standard input.
        stdin = MockStdin(p2.authorization_identifier)
        parsed = PatronInputScript.parse_command_line(db.session, cmd_args, stdin)
        assert [p1, p2] == parsed.patrons

    def test_patron_different_library(self, db: DatabaseTransactionFixture):
        l1 = db.library()
        l2 = db.library()
        p1 = db.patron()
        p2 = db.patron()
        p1.authorization_identifier = db.fresh_str()
        p2.authorization_identifier = p1.authorization_identifier
        p1.library_id = l1.id
        p2.library_id = l2.id
        cmd_args = [l1.short_name, p1.authorization_identifier]
        parsed = PatronInputScript.parse_command_line(db.session, cmd_args, None)
        assert [p1] == parsed.patrons
        cmd_args = [l2.short_name, p2.authorization_identifier]
        parsed = PatronInputScript.parse_command_line(db.session, cmd_args, None)
        assert [p2] == parsed.patrons

    def test_do_run(self, db: DatabaseTransactionFixture):
        """Test that PatronInputScript.do_run() calls process_patron()
        for every patron designated by the command-line arguments.
        """

        class MockPatronInputScript(PatronInputScript):
            def process_patron(self, patron):
                patron.processed = True

        l1 = db.library()
        p1 = db.patron()
        p2 = db.patron()
        p3 = db.patron()
        p1.library_id = l1.id
        p2.library_id = l1.id
        p3.library_id = l1.id
        p1.processed = False
        p2.processed = False
        p3.processed = False
        p1.authorization_identifier = db.fresh_str()
        p2.authorization_identifier = db.fresh_str()
        cmd_args = [l1.short_name, p1.authorization_identifier]
        stdin = MockStdin(p2.authorization_identifier)
        script = MockPatronInputScript(db.session)
        script.do_run(cmd_args=cmd_args, stdin=stdin)
        assert True == p1.processed
        assert True == p2.processed
        assert False == p3.processed


class TestLibraryInputScript:
    def test_parse_library_list(self, db: DatabaseTransactionFixture):
        """Test that libraries can be identified with their full name or short name."""
        l1 = db.library()
        l2 = db.library()
        args = [l1.name, "no-such-library", "", l2.short_name]
        libraries = LibraryInputScript.parse_library_list(db.session, args)
        assert [l1, l2] == libraries

        assert [] == LibraryInputScript.parse_library_list(db.session, [])

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        l1 = db.library()
        # We pass in one library identifier on the command line...
        cmd_args = [l1.name]
        parsed = LibraryInputScript.parse_command_line(db.session, cmd_args)

        # And here it is.
        assert [l1] == parsed.libraries

    def test_parse_command_line_no_identifiers(self, db: DatabaseTransactionFixture):
        """If you don't specify any libraries on the command
        line, we will process all libraries in the system.
        """
        parsed = LibraryInputScript.parse_command_line(db.session, [])
        assert db.session.query(Library).all() == parsed.libraries

    def test_do_run(self, db: DatabaseTransactionFixture):
        """Test that LibraryInputScript.do_run() calls process_library()
        for every library designated by the command-line arguments.
        """

        class MockLibraryInputScript(LibraryInputScript):
            def process_library(self, library):
                library.processed = True

        l1 = db.library()
        l2 = db.library()
        l2.processed = False
        cmd_args = [l1.name]
        script = MockLibraryInputScript(db.session)
        script.do_run(cmd_args=cmd_args)
        assert True == l1.processed
        assert False == l2.processed


class TestLaneSweeperScript:
    def test_process_library(self, db: DatabaseTransactionFixture):
        class Mock(LaneSweeperScript):
            def __init__(self, _db):
                super().__init__(_db)
                self.considered = []
                self.processed = []

            def should_process_lane(self, lane):
                self.considered.append(lane)
                return lane.display_name == "process me"

            def process_lane(self, lane):
                self.processed.append(lane)

        good = db.lane(display_name="process me")
        bad = db.lane(display_name="don't process me")
        good_child = db.lane(display_name="process me", parent=bad)

        script = Mock(db.session)
        script.do_run(cmd_args=[])

        # The first item considered for processing was an ad hoc
        # WorkList representing the library's entire collection.
        worklist = script.considered.pop(0)
        assert db.default_library() == worklist.get_library(db.session)
        assert db.default_library().name == worklist.display_name
        assert {good, bad} == set(worklist.children)

        # After that, every lane was considered for processing, with
        # top-level lanes considered first.
        assert {good, bad, good_child} == set(script.considered)

        # But a lane was processed only if should_process_lane
        # returned True.
        assert {good, good_child} == set(script.processed)


class TestRunCoverageProviderScript:
    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        cmd_args = [
            "--cutoff-time",
            "2016-05-01",
            "--identifier-type",
            identifier.type,
            identifier.identifier,
        ]
        parsed = RunCoverageProviderScript.parse_command_line(
            db.session, cmd_args, MockStdin()
        )
        assert datetime_utc(2016, 5, 1) == parsed.cutoff_time
        assert [identifier] == parsed.identifiers
        assert identifier.type == parsed.identifier_type


class TestRunThreadedCollectionCoverageProviderScript:
    def test_run(self, db: DatabaseTransactionFixture):
        provider = AlwaysSuccessfulCollectionCoverageProvider
        script = RunThreadedCollectionCoverageProviderScript(
            provider, worker_size=2, _db=db.session
        )

        # If there are no collections for the provider, run does nothing.
        # Pass a mock pool that will raise an error if it's used.
        pool = object()
        collection = db.collection(protocol=ExternalIntegration.ENKI)

        # Run exits without a problem because the pool is never touched.
        script.run(pool=pool)

        # Create some identifiers that need coverage.
        collection = db.collection()
        ed1, lp1 = db.edition(collection=collection, with_license_pool=True)
        ed2, lp2 = db.edition(collection=collection, with_license_pool=True)
        ed3 = db.edition()

        [id1, id2, id3] = [e.primary_identifier for e in (ed1, ed2, ed3)]

        # Set a timestamp for the provider.
        timestamp = Timestamp.stamp(
            db.session,
            provider.SERVICE_NAME,
            Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=collection,
        )
        original_timestamp = timestamp.finish
        db.session.commit()

        pool = DatabasePool(2, script.session_factory)
        script.run(pool=pool)
        db.session.commit()

        # The expected number of workers and jobs have been created.
        assert 2 == len(pool.workers)
        assert 1 == pool.job_total

        # All relevant identifiers have been given coverage.
        source = DataSource.lookup(db.session, provider.DATA_SOURCE_NAME)
        identifiers_missing_coverage = Identifier.missing_coverage_from(
            db.session,
            provider.INPUT_IDENTIFIER_TYPES,
            source,
        )
        assert [id3] == identifiers_missing_coverage.all()

        record1, was_registered1 = provider.register(id1)
        record2, was_registered2 = provider.register(id2)
        assert CoverageRecord.SUCCESS == record1.status
        assert CoverageRecord.SUCCESS == record2.status
        assert (False, False) == (was_registered1, was_registered2)

        # The timestamp for the provider has been updated.
        new_timestamp = Timestamp.value(
            db.session,
            provider.SERVICE_NAME,
            Timestamp.COVERAGE_PROVIDER_TYPE,
            collection,
        )
        assert new_timestamp != original_timestamp
        assert new_timestamp > original_timestamp


class TestRunWorkCoverageProviderScript:
    def test_constructor(self, db: DatabaseTransactionFixture):
        script = RunWorkCoverageProviderScript(
            AlwaysSuccessfulWorkCoverageProvider, _db=db.session, batch_size=123
        )
        [provider] = script.providers
        assert isinstance(provider, AlwaysSuccessfulWorkCoverageProvider)
        assert 123 == provider.batch_size


class TestWorkProcessingScript:
    def test_make_query(self, db: DatabaseTransactionFixture):
        # Create two Gutenberg works and one Overdrive work
        g1 = db.work(with_license_pool=True, with_open_access_download=True)
        g2 = db.work(with_license_pool=True, with_open_access_download=True)

        overdrive_edition = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )[0]
        overdrive_work = db.work(presentation_edition=overdrive_edition)

        ugi_edition = db.edition(
            data_source_name=DataSource.UNGLUE_IT,
            identifier_type=Identifier.URI,
            with_license_pool=True,
        )[0]
        unglue_it = db.work(presentation_edition=ugi_edition)

        se_edition = db.edition(
            data_source_name=DataSource.STANDARD_EBOOKS,
            identifier_type=Identifier.URI,
            with_license_pool=True,
        )[0]
        standard_ebooks = db.work(presentation_edition=se_edition)

        everything = WorkProcessingScript.make_query(db.session, None, None, None)
        assert {g1, g2, overdrive_work, unglue_it, standard_ebooks} == set(
            everything.all()
        )

        all_gutenberg = WorkProcessingScript.make_query(
            db.session, Identifier.GUTENBERG_ID, [], None
        )
        assert {g1, g2} == set(all_gutenberg.all())

        one_gutenberg = WorkProcessingScript.make_query(
            db.session, Identifier.GUTENBERG_ID, [g1.license_pools[0].identifier], None
        )
        assert [g1] == one_gutenberg.all()

        one_standard_ebook = WorkProcessingScript.make_query(
            db.session, Identifier.URI, [], DataSource.STANDARD_EBOOKS
        )
        assert [standard_ebooks] == one_standard_ebook.all()


class TestAddClassificationScript:
    def test_end_to_end(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        stdin = MockStdin(identifier.identifier)
        assert Classifier.AUDIENCE_ADULT == work.audience

        cmd_args = [
            "--identifier-type",
            identifier.type,
            "--subject-type",
            Classifier.FREEFORM_AUDIENCE,
            "--subject-identifier",
            Classifier.AUDIENCE_CHILDREN,
            "--weight",
            "42",
            "--create-subject",
        ]
        script = AddClassificationScript(_db=db.session, cmd_args=cmd_args, stdin=stdin)
        script.do_run()

        # The identifier has been classified under 'children'.
        [classification] = identifier.classifications
        assert 42 == classification.weight
        subject = classification.subject
        assert Classifier.FREEFORM_AUDIENCE == subject.type
        assert Classifier.AUDIENCE_CHILDREN == subject.identifier

        # The work has been reclassified and is now known as a
        # children's book.
        assert Classifier.AUDIENCE_CHILDREN == work.audience

    def test_autocreate(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        stdin = MockStdin(identifier.identifier)
        assert Classifier.AUDIENCE_ADULT == work.audience

        cmd_args = [
            "--identifier-type",
            identifier.type,
            "--subject-type",
            Classifier.TAG,
            "--subject-identifier",
            "some random tag",
        ]
        script = AddClassificationScript(_db=db.session, cmd_args=cmd_args, stdin=stdin)
        script.do_run()

        # Nothing has happened. There was no Subject with that
        # identifier, so we assumed there was a typo and did nothing.
        assert [] == identifier.classifications

        # If we stick the 'create-subject' onto the end of the
        # command-line arguments, the Subject is created and the
        # classification happens.
        stdin = MockStdin(identifier.identifier)
        cmd_args.append("--create-subject")
        script = AddClassificationScript(_db=db.session, cmd_args=cmd_args, stdin=stdin)
        script.do_run()

        [classification] = identifier.classifications
        subject = classification.subject
        assert "some random tag" == subject.identifier


class TestShowLibrariesScript:
    def test_with_no_libraries(self, db: DatabaseTransactionFixture):
        output = StringIO()
        ShowLibrariesScript().do_run(db.session, output=output)
        assert "No libraries found.\n" == output.getvalue()

    def test_with_multiple_libraries(self, db: DatabaseTransactionFixture):
        l1 = db.library(name="Library 1", short_name="L1")
        l1.library_registry_shared_secret = "a"
        l2 = db.library(
            name="Library 2",
            short_name="L2",
        )
        l2.library_registry_shared_secret = "b"

        # The output of this script is the result of running explain()
        # on both libraries.
        output = StringIO()
        ShowLibrariesScript().do_run(db.session, output=output)
        expect_1 = "\n".join(l1.explain(include_secrets=False))
        expect_2 = "\n".join(l2.explain(include_secrets=False))

        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()

        # We can tell the script to only list a single library.
        output = StringIO()
        ShowLibrariesScript().do_run(
            db.session, cmd_args=["--short-name=L2"], output=output
        )
        assert expect_2 + "\n" == output.getvalue()

        # We can tell the script to include the library registry
        # shared secret.
        output = StringIO()
        ShowLibrariesScript().do_run(
            db.session, cmd_args=["--show-secrets"], output=output
        )
        expect_1 = "\n".join(l1.explain(include_secrets=True))
        expect_2 = "\n".join(l2.explain(include_secrets=True))
        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()


class TestConfigureSiteScript:
    def test_unknown_setting(self, db: DatabaseTransactionFixture):
        script = ConfigureSiteScript()
        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, ["--setting=setting1=value1"])
        assert (
            "'setting1' is not a known site-wide setting. Use --force to set it anyway."
            in str(excinfo.value)
        )

        assert None == ConfigurationSetting.sitewide(db.session, "setting1").value

        # Running with --force sets the setting.
        script.do_run(
            db.session,
            [
                "--setting=setting1=value1",
                "--force",
            ],
        )

        assert "value1" == ConfigurationSetting.sitewide(db.session, "setting1").value

    def test_settings(self, db: DatabaseTransactionFixture):
        class TestConfig:
            SITEWIDE_SETTINGS = [
                {"key": "setting1"},
                {"key": "setting2"},
                {"key": "setting_secret"},
            ]

        script = ConfigureSiteScript(config=TestConfig)
        output = StringIO()
        script.do_run(
            db.session,
            [
                "--setting=setting1=value1",
                '--setting=setting2=[1,2,"3"]',
                "--setting=setting_secret=secretvalue",
            ],
            output,
        )
        # The secret was set, but is not shown.
        expect = "\n".join(
            ConfigurationSetting.explain(db.session, include_secrets=False)
        )
        assert expect == output.getvalue()
        assert "setting_secret" not in expect
        assert "value1" == ConfigurationSetting.sitewide(db.session, "setting1").value
        assert (
            '[1,2,"3"]' == ConfigurationSetting.sitewide(db.session, "setting2").value
        )
        assert (
            "secretvalue"
            == ConfigurationSetting.sitewide(db.session, "setting_secret").value
        )

        # If we run again with --show-secrets, the secret is shown.
        output = StringIO()
        script.do_run(db.session, ["--show-secrets"], output)
        expect = "\n".join(
            ConfigurationSetting.explain(db.session, include_secrets=True)
        )
        assert expect == output.getvalue()
        assert "setting_secret" in expect


class TestConfigureLibraryScript:
    def test_bad_arguments(self, db: DatabaseTransactionFixture):
        script = ConfigureLibraryScript()
        library = db.library(
            name="Library 1",
            short_name="L1",
        )
        library.library_registry_shared_secret = "secret"
        db.session.commit()
        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, [])
        assert "You must identify the library by its short name." in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, ["--short-name=foo"])
        assert "Could not locate library 'foo'" in str(excinfo.value)

    def test_create_library(self, db: DatabaseTransactionFixture):
        # There is no library.
        assert [] == db.session.query(Library).all()

        script = ConfigureLibraryScript()
        output = StringIO()
        script.do_run(
            db.session,
            [
                "--short-name=L1",
                "--name=Library 1",
                "--setting=customkey=value",
                "--setting=website=http://library.org",
                "--setting=help_email=support@library.org",
            ],
            output,
        )

        # Now there is one library.
        [library] = db.session.query(Library).all()
        assert "Library 1" == library.name
        assert "L1" == library.short_name
        assert "http://library.org" == library.settings.website
        assert "support@library.org" == library.settings.help_email
        assert "value" == library.settings_dict.get("customkey")
        expect_output = (
            "Configuration settings stored.\n" + "\n".join(library.explain()) + "\n"
        )
        assert expect_output == output.getvalue()

    def test_reconfigure_library(self, db: DatabaseTransactionFixture):
        # The library exists.
        library = db.library(
            name="Library 1",
            short_name="L1",
        )
        script = ConfigureLibraryScript()
        output = StringIO()

        # We're going to change one value and add a setting.
        script.do_run(
            db.session,
            [
                "--short-name=L1",
                "--name=Library 1 New Name",
                "--setting=customkey=value",
            ],
            output,
        )

        assert "Library 1 New Name" == library.name
        assert "value" == library.settings_dict.get("customkey")

        expect_output = (
            "Configuration settings stored.\n" + "\n".join(library.explain()) + "\n"
        )
        assert expect_output == output.getvalue()


class TestShowCollectionsScript:
    def test_with_no_collections(self, db: DatabaseTransactionFixture):
        output = StringIO()
        ShowCollectionsScript().do_run(db.session, output=output)
        assert "No collections found.\n" == output.getvalue()

    def test_with_multiple_collections(self, db: DatabaseTransactionFixture):
        c1 = db.collection(name="Collection 1", protocol=ExternalIntegration.OVERDRIVE)
        c1.collection_password = "a"
        c2 = db.collection(
            name="Collection 2", protocol=ExternalIntegration.BIBLIOTHECA
        )
        c2.collection_password = "b"

        # The output of this script is the result of running explain()
        # on both collections.
        output = StringIO()
        ShowCollectionsScript().do_run(db.session, output=output)
        expect_1 = "\n".join(c1.explain(include_secrets=False))
        expect_2 = "\n".join(c2.explain(include_secrets=False))

        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()

        # We can tell the script to only list a single collection.
        output = StringIO()
        ShowCollectionsScript().do_run(
            db.session, cmd_args=["--name=Collection 2"], output=output
        )
        assert expect_2 + "\n" == output.getvalue()

        # We can tell the script to include the collection password
        output = StringIO()
        ShowCollectionsScript().do_run(
            db.session, cmd_args=["--show-secrets"], output=output
        )
        expect_1 = "\n".join(c1.explain(include_secrets=True))
        expect_2 = "\n".join(c2.explain(include_secrets=True))
        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()


class TestConfigureCollectionScript:
    def test_bad_arguments(self, db: DatabaseTransactionFixture):
        script = ConfigureCollectionScript()
        db.library(
            name="Library 1",
            short_name="L1",
        )
        db.session.commit()

        # Reference to a nonexistent collection without the information
        # necessary to create it.
        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, ["--name=collection"])
        assert (
            'No collection called "collection". You can create it, but you must specify a protocol.'
            in str(excinfo.value)
        )

        # Incorrect format for the 'setting' argument.
        with pytest.raises(ValueError) as excinfo:
            script.do_run(
                db.session,
                ["--name=collection", "--protocol=Overdrive", "--setting=key"],
            )
        assert 'Incorrect format for setting: "key". Should be "key=value"' in str(
            excinfo.value
        )

        # Try to add the collection to a nonexistent library.
        with pytest.raises(ValueError) as excinfo:
            script.do_run(
                db.session,
                [
                    "--name=collection",
                    "--protocol=Overdrive",
                    "--library=nosuchlibrary",
                ],
            )
        assert 'No such library: "nosuchlibrary". I only know about: "L1"' in str(
            excinfo.value
        )

    def test_success(self, db: DatabaseTransactionFixture):
        script = ConfigureCollectionScript()
        l1 = db.library(name="Library 1", short_name="L1")
        l2 = db.library(name="Library 2", short_name="L2")
        l3 = db.library(name="Library 3", short_name="L3")

        # Create a collection, set all its attributes, set a custom
        # setting, and associate it with two libraries.
        output = StringIO()
        script.do_run(
            db.session,
            [
                "--name=New Collection",
                "--protocol=Overdrive",
                "--library=L2",
                "--library=L1",
                "--setting=library_id=1234",
                "--external-account-id=acctid",
                "--url=url",
                "--username=username",
                "--password=password",
            ],
            output,
        )

        db.session.commit()

        # The collection was created and configured properly.
        collection = get_one(db.session, Collection)
        assert "New Collection" == collection.name
        assert "url" == collection.integration_configuration.settings_dict["url"]
        assert (
            "acctid"
            == collection.integration_configuration.settings_dict["external_account_id"]
        )
        assert (
            "username" == collection.integration_configuration.settings_dict["username"]
        )
        assert (
            "password" == collection.integration_configuration.settings_dict["password"]
        )

        # Two libraries now have access to the collection.
        assert [collection] == l1.collections
        assert [collection] == l2.collections
        assert [] == l3.collections

        # One CollectionSetting was set on the collection, in addition
        # to url, username, and password.
        setting = collection.integration_configuration.settings_dict.get("library_id")
        assert "1234" == setting

        # The output explains the collection settings.
        expect = (
            "Configuration settings stored.\n" + "\n".join(collection.explain()) + "\n"
        )
        assert expect == output.getvalue()

    def test_reconfigure_collection(self, db: DatabaseTransactionFixture):
        # The collection exists.
        collection = db.collection(
            name="Collection 1", protocol=ExternalIntegration.OVERDRIVE
        )
        script = ConfigureCollectionScript()
        output = StringIO()

        # We're going to change one value and add a new one.
        script.do_run(
            db.session,
            [
                "--name=Collection 1",
                "--url=foo",
                "--protocol=%s" % ExternalIntegration.BIBLIOTHECA,
            ],
            output,
        )

        # The collection has been changed.
        db.session.refresh(collection.integration_configuration)
        assert "foo" == collection.integration_configuration.settings_dict.get("url")
        assert ExternalIntegration.BIBLIOTHECA == collection.protocol

        expect = (
            "Configuration settings stored.\n" + "\n".join(collection.explain()) + "\n"
        )

        assert expect == output.getvalue()


class TestShowIntegrationsScript:
    def test_with_no_integrations(self, db: DatabaseTransactionFixture):
        output = StringIO()
        ShowIntegrationsScript().do_run(db.session, output=output)
        assert "No integrations found.\n" == output.getvalue()

    def test_with_multiple_integrations(self, db: DatabaseTransactionFixture):
        i1 = db.external_integration(
            name="Integration 1", goal="Goal", protocol=ExternalIntegration.OVERDRIVE
        )
        i1.password = "a"
        i2 = db.external_integration(
            name="Integration 2", goal="Goal", protocol=ExternalIntegration.BIBLIOTHECA
        )
        i2.password = "b"

        # The output of this script is the result of running explain()
        # on both integrations.
        output = StringIO()
        ShowIntegrationsScript().do_run(db.session, output=output)
        expect_1 = "\n".join(i1.explain(include_secrets=False))
        expect_2 = "\n".join(i2.explain(include_secrets=False))

        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()

        # We can tell the script to only list a single integration.
        output = StringIO()
        ShowIntegrationsScript().do_run(
            db.session, cmd_args=["--name=Integration 2"], output=output
        )
        assert expect_2 + "\n" == output.getvalue()

        # We can tell the script to include the integration secrets
        output = StringIO()
        ShowIntegrationsScript().do_run(
            db.session, cmd_args=["--show-secrets"], output=output
        )
        expect_1 = "\n".join(i1.explain(include_secrets=True))
        expect_2 = "\n".join(i2.explain(include_secrets=True))
        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()


class TestConfigureIntegrationScript:
    def test_load_integration(self, db: DatabaseTransactionFixture):
        m = ConfigureIntegrationScript._integration

        with pytest.raises(ValueError) as excinfo:
            m(db.session, None, None, "protocol", None)
        assert (
            "An integration must by identified by either ID, name, or the combination of protocol and goal."
            in str(excinfo.value)
        )

        with pytest.raises(ValueError) as excinfo:
            m(db.session, "notanid", None, None, None)
        assert "No integration with ID notanid." in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            m(db.session, None, "Unknown integration", None, None)
        assert (
            'No integration with name "Unknown integration". To create it, you must also provide protocol and goal.'
            in str(excinfo.value)
        )

        integration = db.external_integration(protocol="Protocol", goal="Goal")
        integration.name = "An integration"
        assert integration == m(db.session, integration.id, None, None, None)

        assert integration == m(db.session, None, integration.name, None, None)

        assert integration == m(db.session, None, None, "Protocol", "Goal")

        # An integration may be created given a protocol and goal.
        integration2 = m(db.session, None, "I exist now", "Protocol", "Goal2")
        assert integration2 != integration
        assert "Protocol" == integration2.protocol
        assert "Goal2" == integration2.goal
        assert "I exist now" == integration2.name

    def test_add_settings(self, db: DatabaseTransactionFixture):
        script = ConfigureIntegrationScript()
        output = StringIO()

        script.do_run(
            db.session,
            [
                "--protocol=aprotocol",
                "--goal=agoal",
                "--setting=akey=avalue",
            ],
            output,
        )

        # An ExternalIntegration was created and configured.
        integration = get_one(
            db.session, ExternalIntegration, protocol="aprotocol", goal="agoal"
        )

        expect_output = (
            "Configuration settings stored.\n" + "\n".join(integration.explain()) + "\n"
        )
        assert expect_output == output.getvalue()


class TestShowLanesScript:
    def test_with_no_lanes(self, db: DatabaseTransactionFixture):
        output = StringIO()
        ShowLanesScript().do_run(db.session, output=output)
        assert "No lanes found.\n" == output.getvalue()

    def test_with_multiple_lanes(self, db: DatabaseTransactionFixture):
        l1 = db.lane()
        l2 = db.lane()

        # The output of this script is the result of running explain()
        # on both lanes.
        output = StringIO()
        ShowLanesScript().do_run(db.session, output=output)
        expect_1 = "\n".join(l1.explain())
        expect_2 = "\n".join(l2.explain())

        assert expect_1 + "\n\n" + expect_2 + "\n\n" == output.getvalue()

        # We can tell the script to only list a single lane.
        output = StringIO()
        ShowLanesScript().do_run(
            db.session, cmd_args=["--id=%s" % l2.id], output=output
        )
        assert expect_2 + "\n\n" == output.getvalue()


class TestConfigureLaneScript:
    def test_bad_arguments(self, db: DatabaseTransactionFixture):
        script = ConfigureLaneScript()

        # No lane id but no library short name for creating it either.
        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, [])
        assert "Library short name is required to create a new lane" in str(
            excinfo.value
        )

        # Try to create a lane for a nonexistent library.
        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, ["--library-short-name=nosuchlibrary"])
        assert 'No such library: "nosuchlibrary".' in str(excinfo.value)

    def test_create_lane(self, db: DatabaseTransactionFixture):
        script = ConfigureLaneScript()
        parent = db.lane()

        # Create a lane and set its attributes.
        output = StringIO()
        script.do_run(
            db.session,
            [
                "--library-short-name=%s" % db.default_library().short_name,
                "--parent-id=%s" % parent.id,
                "--priority=3",
                "--display-name=NewLane",
            ],
            output,
        )

        # The lane was created and configured properly.
        lane = get_one(db.session, Lane, display_name="NewLane")
        assert db.default_library() == lane.library
        assert parent == lane.parent
        assert 3 == lane.priority

        # The output explains the lane settings.
        expect = "Lane settings stored.\n" + "\n".join(lane.explain()) + "\n"
        assert expect == output.getvalue()

    def test_reconfigure_lane(self, db: DatabaseTransactionFixture):
        # The lane exists.
        lane = db.lane(display_name="Name")
        lane.priority = 3

        parent = db.lane()

        script = ConfigureLaneScript()
        output = StringIO()

        script.do_run(
            db.session,
            [
                "--id=%s" % lane.id,
                "--priority=1",
                "--parent-id=%s" % parent.id,
            ],
            output,
        )

        # The lane has been changed.
        assert 1 == lane.priority
        assert parent == lane.parent
        expect = "Lane settings stored.\n" + "\n".join(lane.explain()) + "\n"

        assert expect == output.getvalue()


class TestCollectionInputScript:
    """Test the ability to name collections on the command line."""

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        def collections(cmd_args):
            parsed = CollectionInputScript.parse_command_line(db.session, cmd_args)
            return parsed.collections

        # No collections named on command line -> no collections
        assert [] == collections([])

        # Nonexistent collection -> ValueError
        with pytest.raises(ValueError) as excinfo:
            collections(['--collection="no such collection"'])
        assert 'Unknown collection: "no such collection"' in str(excinfo.value)

        # Collections are presented in the order they were encountered
        # on the command line.
        c2 = db.collection()
        expect = [c2, db.default_collection()]
        args = ["--collection=" + c.name for c in expect]
        actual = collections(args)
        assert expect == actual


class TestCollectionArgumentsScript:
    """Test the ability to take collection arguments on the command line."""

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        def collections(cmd_args):
            parsed = CollectionArgumentsScript.parse_command_line(db.session, cmd_args)
            return parsed.collections

        # No collections named on command line -> no collections
        assert [] == collections([])

        # Nonexistent collection -> ValueError
        with pytest.raises(ValueError) as excinfo:
            collections(["no such collection"])
        assert "Unknown collection: no such collection" in str(excinfo.value)

        # Collections are presented in the order they were encountered
        # on the command line.
        c2 = db.collection()
        expect = [c2, db.default_collection()]
        args = [c.name for c in expect]
        actual = collections(args)
        assert expect == actual

        # It is okay to not specify any collections.
        expect = []
        args = [c.name for c in expect]
        actual = collections(args)
        assert expect == actual


# Mock classes used by TestOPDSImportScript
class MockOPDSImportMonitor:
    """Pretend to monitor an OPDS feed for new titles."""

    INSTANCES: list[MockOPDSImportMonitor] = []

    def __init__(self, _db, collection, *args, **kwargs):
        self.collection = collection
        self.args = args
        self.kwargs = kwargs
        self.INSTANCES.append(self)
        self.was_run = False

    def run(self):
        self.was_run = True


class MockOPDSImporter:
    """Pretend to import titles from an OPDS feed."""


class MockOPDSImportScript(OPDSImportScript):
    """Actually instantiate a monitor that will pretend to do something."""

    MONITOR_CLASS: type[OPDSImportMonitor] = MockOPDSImportMonitor  # type: ignore
    IMPORTER_CLASS = MockOPDSImporter  # type: ignore


class TestOPDSImportScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        DatabaseTransactionFixture.set_settings(
            db.default_collection().integration_configuration,
            Collection.DATA_SOURCE_NAME_SETTING,
            DataSource.OA_CONTENT_SERVER,
        )

        script = MockOPDSImportScript(db.session)
        script.do_run([])

        # Since we provided no collection, a MockOPDSImportMonitor
        # was instantiated for each OPDS Import collection in the database.
        monitor = MockOPDSImportMonitor.INSTANCES.pop()
        assert db.default_collection() == monitor.collection

        args = ["--collection=%s" % db.default_collection().name]
        script.do_run(args)

        # If we provide the collection name, a MockOPDSImportMonitor is
        # also instantiated.
        monitor = MockOPDSImportMonitor.INSTANCES.pop()
        assert db.default_collection() == monitor.collection
        assert True == monitor.was_run

        # Our replacement OPDS importer class was passed in to the
        # monitor constructor. If this had been a real monitor, that's the
        # code we would have used to import OPDS feeds.
        assert MockOPDSImporter == monitor.kwargs["import_class"]
        assert False == monitor.kwargs["force_reimport"]

        # Setting --force changes the 'force_reimport' argument
        # passed to the monitor constructor.
        args.append("--force")
        script.do_run(args)
        monitor = MockOPDSImportMonitor.INSTANCES.pop()
        assert db.default_collection() == monitor.collection
        assert True == monitor.kwargs["force_reimport"]


class MockWhereAreMyBooks(WhereAreMyBooksScript):
    """A mock script that keeps track of its output in an easy-to-test
    form, so we don't have to mess around with StringIO.
    """

    def __init__(self, search: ExternalSearchIndex, _db=None, output=None):
        # In most cases a list will do fine for `output`.
        output = output or []

        super().__init__(_db, output, search)
        self.output = []

    def out(self, s, *args):
        if args:
            self.output.append((s, list(args)))
        else:
            self.output.append(s)


class TestWhereAreMyBooksScript:
    @pytest.mark.skip(
        reason="This test currently freezes inside pytest and has to be killed with SIGKILL."
    )
    def test_overall_structure(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # Verify that run() calls the methods we expect.

        class Mock(MockWhereAreMyBooks):
            """Used to verify that the correct methods are called."""

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.delete_cached_feeds_called = False
                self.checked_libraries = []
                self.explained_collections = []

            def check_library(self, library):
                self.checked_libraries.append(library)

            def delete_cached_feeds(self):
                self.delete_cached_feeds_called = True

            def explain_collection(self, collection):
                self.explained_collections.append(collection)

        # If there are no libraries in the system, that's a big problem.
        script = Mock(db.session)
        script.run()
        assert [
            "There are no libraries in the system -- that's a problem.",
            "\n",
        ] == script.output

        # We still run the other checks, though.
        assert True == script.delete_cached_feeds_called

        # Make some libraries and some collections, and try again.
        library1 = db.default_library()
        library2 = db.library()

        collection1 = db.default_collection()
        collection2 = db.collection()

        script = Mock(db.session)
        script.run()

        # Every library in the collection was checked.
        assert {library1, library2} == set(script.checked_libraries)

        # delete_cached_feeds() was called.
        assert True == script.delete_cached_feeds_called

        # Every collection in the database was explained.
        assert {collection1, collection2} == set(script.explained_collections)

        # There only output were the newlines after the five method
        # calls. All other output happened inside the methods we
        # mocked.
        assert ["\n"] * 5 == script.output

        # Finally, verify the ability to use the command line to limit
        # the check to specific collections. (This isn't terribly useful
        # since checks now run very quickly.)
        script = Mock(db.session)
        script.run(cmd_args=["--collection=%s" % collection2.name])
        assert [collection2] == script.explained_collections

    def test_check_library(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # Give the default library a collection and a lane.
        library = db.default_library()
        collection = db.default_collection()
        lane = db.lane(library=library)

        script = MockWhereAreMyBooks(
            _db=db.session, search=end_to_end_search_fixture.external_search_index
        )
        script.check_library(library)

        checking, has_collection, has_lanes = script.output
        assert ("Checking library %s", [library.name]) == checking
        assert (" Associated with collection %s.", [collection.name]) == has_collection
        assert (" Associated with %s lanes.", [1]) == has_lanes

        # This library has no collections and no lanes.
        library2 = db.library()
        script.output = []
        script.check_library(library2)
        checking, no_collection, no_lanes = script.output
        assert ("Checking library %s", [library2.name]) == checking
        assert " This library has no collections -- that's a problem." == no_collection
        assert " This library has no lanes -- that's a problem." == no_lanes

    @staticmethod
    def check_explanation(
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
        presentation_ready=1,
        not_presentation_ready=0,
        no_delivery_mechanisms=0,
        suppressed=0,
        not_owned=0,
        in_search_index=0,
        **kwargs,
    ):
        """Runs explain_collection() and verifies expected output."""
        script = MockWhereAreMyBooks(
            _db=db.session,
            search=end_to_end_search_fixture.external_search_index,
            **kwargs,
        )
        script.explain_collection(db.default_collection())
        out = script.output

        # This always happens.
        assert (
            'Examining collection "%s"',
            [db.default_collection().name],
        ) == out.pop(0)
        assert (" %d presentation-ready works.", [presentation_ready]) == out.pop(0)
        assert (
            " %d works not presentation-ready.",
            [not_presentation_ready],
        ) == out.pop(0)

        # These totals are only given if the numbers are nonzero.
        #
        if no_delivery_mechanisms:
            assert (
                " %d works are missing delivery mechanisms and won't show up.",
                [no_delivery_mechanisms],
            ) == out.pop(0)

        if suppressed:
            assert (
                " %d works have suppressed LicensePools and won't show up.",
                [suppressed],
            ) == out.pop(0)

        if not_owned:
            assert (
                " %d non-open-access works have no owned licenses and won't show up.",
                [not_owned],
            ) == out.pop(0)

        # Search engine statistics are always shown.
        assert (
            " %d works in the search index, expected around %d.",
            [in_search_index, presentation_ready],
        ) == out.pop(0)

    def test_no_presentation_ready_works(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # This work is not presentation-ready.
        work = db.work(with_license_pool=True)
        end_to_end_search_fixture.external_search_index.initialize_indices()
        work.presentation_ready = False
        script = MockWhereAreMyBooks(
            _db=db.session, search=end_to_end_search_fixture.external_search_index
        )
        self.check_explanation(
            end_to_end_search_fixture=end_to_end_search_fixture,
            presentation_ready=0,
            not_presentation_ready=1,
            db=db,
        )

    def test_no_delivery_mechanisms(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # This work has a license pool, but no delivery mechanisms.
        work = db.work(with_license_pool=True)
        end_to_end_search_fixture.external_search_index.initialize_indices()
        for lpdm in work.license_pools[0].delivery_mechanisms:
            db.session.delete(lpdm)
        self.check_explanation(
            no_delivery_mechanisms=1,
            db=db,
            end_to_end_search_fixture=end_to_end_search_fixture,
        )

    def test_suppressed_pool(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # This work has a license pool, but it's suppressed.
        work = db.work(with_license_pool=True)
        end_to_end_search_fixture.external_search_index.initialize_indices()
        work.license_pools[0].suppressed = True
        self.check_explanation(
            suppressed=1,
            db=db,
            end_to_end_search_fixture=end_to_end_search_fixture,
        )

    def test_no_licenses(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # This work has a license pool, but no licenses owned.
        work = db.work(with_license_pool=True)
        end_to_end_search_fixture.external_search_index.initialize_indices()
        work.license_pools[0].licenses_owned = 0
        self.check_explanation(
            not_owned=1,
            db=db,
            end_to_end_search_fixture=end_to_end_search_fixture,
        )

    def test_search_engine(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        search = end_to_end_search_fixture.external_search_index
        work = db.work(with_license_pool=True)
        work.presentation_ready = True

        docs = search.start_migration()
        docs.add_documents(search.create_search_documents_from_works([work]))
        docs.finish()

        # This search index will always claim there is one result.
        self.check_explanation(
            in_search_index=1,
            db=db,
            end_to_end_search_fixture=end_to_end_search_fixture,
        )


class TestExplain:
    def test_explain(self, db: DatabaseTransactionFixture):
        """Make sure the Explain script runs without crashing."""
        work = db.work(with_license_pool=True, genre="Science Fiction")
        [pool] = work.license_pools
        edition = work.presentation_edition
        identifier = pool.identifier
        source = DataSource.lookup(db.session, DataSource.OCLC_LINKED_DATA)
        CoverageRecord.add_for(identifier, source, "an operation")
        input = StringIO()
        output = StringIO()
        args = ["--identifier-type", "Database ID", str(identifier.id)]
        Explain(db.session).do_run(cmd_args=args, stdin=input, stdout=output)
        output = output.getvalue()

        # The script ran. Spot-check that it provided various
        # information about the work, without testing the exact
        # output.
        assert pool.collection.name in output
        assert "Available to libraries: default" in output
        assert work.title in output
        assert "Science Fiction" in output
        for contributor in edition.contributors:
            assert contributor.sort_name in output

        # CoverageRecords associated with the primary identifier were
        # printed out.
        assert "OCLC Linked Data | an operation | success" in output

        # There is an active LicensePool that is fulfillable and has
        # copies owned.
        assert "%s owned" % pool.licenses_owned in output
        assert "Fulfillable" in output
        assert "ACTIVE" in output


class TestReclassifyWorksForUncheckedSubjectsScript:
    def test_constructor(self, db: DatabaseTransactionFixture):
        """Make sure that we're only going to classify works
        with unchecked subjects.
        """
        script = ReclassifyWorksForUncheckedSubjectsScript(db.session)
        assert (
            WorkClassificationScript.policy
            == ReclassifyWorksForUncheckedSubjectsScript.policy
        )
        assert 100 == script.batch_size

        # Assert all joins have been included in the Order By
        ordered_by = script.query._order_by_clauses
        for join in [Work, LicensePool, Identifier, Classification]:
            assert join.id in ordered_by

        assert Work.id in ordered_by

    def test_paginate(self, db: DatabaseTransactionFixture):
        """Pagination is changed to be row-wise comparison
        Ensure we are paginating correctly within the same Subject page"""
        subject = db.subject(Subject.AXIS_360_AUDIENCE, "Any")
        works = []
        for i in range(20):
            work: Work = db.work(with_license_pool=True)
            db.classification(
                work.presentation_edition.primary_identifier,
                subject,
                work.license_pools[0].data_source,
            )
            works.append(work)

        script = ReclassifyWorksForUncheckedSubjectsScript(db.session)
        script.batch_size = 1
        for ix, [work] in enumerate(script.paginate_query(script.query)):
            # We are coming in via "id" order
            assert work == works[ix]
        assert ix == 19

        other_subject = db.subject(Subject.BISAC, "Any")
        last_work = works[-1]
        db.classification(
            last_work.presentation_edition.primary_identifier,
            other_subject,
            last_work.license_pools[0].data_source,
        )
        script.batch_size = 100
        next_works = next(script.paginate_query(script.query))
        # Works are only iterated over ONCE per loop
        assert len(next_works) == 20

        # A checked subjects work is not included
        not_work = db.work(with_license_pool=True)
        another_subject = db.subject(Subject.DDC, "Any")
        db.classification(
            not_work.presentation_edition.primary_identifier,
            another_subject,
            not_work.license_pools[0].data_source,
        )
        another_subject.checked = True
        db.session.commit()
        next_works = next(script.paginate_query(script.query))
        assert len(next_works) == 20
        assert not_work not in next_works

    def test_subject_checked(self, db: DatabaseTransactionFixture):
        subject = db.subject(Subject.AXIS_360_AUDIENCE, "Any")
        assert subject.checked == False

        works = []
        for i in range(10):
            work: Work = db.work(with_license_pool=True)
            db.classification(
                work.presentation_edition.primary_identifier,
                subject,
                work.license_pools[0].data_source,
            )
            works.append(work)

        script = ReclassifyWorksForUncheckedSubjectsScript(db.session)
        script.run()
        db.session.refresh(subject)
        assert subject.checked == True


class TestRebuildSearchIndexScript:
    def test_do_run(
        self,
        db: DatabaseTransactionFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        index = external_search_fake_fixture.external_search
        work = db.work(with_license_pool=True)
        work2 = db.work(with_license_pool=True)
        wcr = WorkCoverageRecord
        decoys = [wcr.QUALITY_OPERATION, wcr.SUMMARY_OPERATION]

        # Set up some coverage records.
        for operation in decoys + [wcr.UPDATE_SEARCH_INDEX_OPERATION]:
            for w in (work, work2):
                wcr.add_for(w, operation, status=random.choice(wcr.ALL_STATUSES))

        coverage_qu = db.session.query(wcr).filter(
            wcr.operation == wcr.UPDATE_SEARCH_INDEX_OPERATION
        )
        original_coverage = [x.id for x in coverage_qu]

        # Run the script.
        script = RebuildSearchIndexScript(db.session, search_index_client=index)
        [progress] = script.do_run()

        # The mock methods were called with the values we expect.
        assert {work.id, work2.id} == set(
            map(
                lambda d: d["_id"], external_search_fake_fixture.service.documents_all()
            )
        )

        # The script returned a list containing a single
        # CoverageProviderProgress object containing accurate
        # information about what happened (from the CoverageProvider's
        # point of view).
        assert (
            "Items processed: 2. Successes: 2, transient failures: 0, persistent failures: 0"
            == progress.achievements
        )

        # The old WorkCoverageRecords for the works were deleted. Then
        # the CoverageProvider did its job and new ones were added.
        new_coverage = [x.id for x in coverage_qu]
        assert 2 == len(new_coverage)
        assert set(new_coverage) != set(original_coverage)


class TestSearchIndexCoverageRemover:
    SERVICE_NAME = "Search Index Coverage Remover"

    def test_do_run(self, db: DatabaseTransactionFixture):
        work = db.work()
        work2 = db.work()
        wcr = WorkCoverageRecord
        decoys = [wcr.QUALITY_OPERATION, wcr.SUMMARY_OPERATION]

        # Set up some coverage records.
        for operation in decoys + [wcr.UPDATE_SEARCH_INDEX_OPERATION]:
            for w in (work, work2):
                wcr.add_for(w, operation, status=random.choice(wcr.ALL_STATUSES))

        # Run the script.
        script = SearchIndexCoverageRemover(db.session)
        result = script.do_run()
        assert isinstance(result, TimestampData)
        assert "Coverage records deleted: 2" == result.achievements

        # UPDATE_SEARCH_INDEX_OPERATION records have been removed.
        # No other records are affected.
        for w in (work, work2):
            remaining = [x.operation for x in w.coverage_records]
            assert sorted(remaining) == sorted(decoys)


class TestUpdateLaneSizeScript:
    def test_do_run(self, db, end_to_end_search_fixture: EndToEndSearchFixture):
        end_to_end_search_fixture.external_search_index.start_migration().finish()

        lane = db.lane()
        lane.size = 100
        UpdateLaneSizeScript(
            db.session,
            search_index_client=end_to_end_search_fixture.external_search_index,
        ).do_run(cmd_args=[])
        assert 0 == lane.size

    def test_should_process_lane(
        self,
        db: DatabaseTransactionFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        """Only Lane objects can have their size updated."""
        lane = db.lane()
        script = UpdateLaneSizeScript(
            db.session, search_index_client=external_search_fake_fixture.external_search
        )
        assert True == script.should_process_lane(lane)

        worklist = WorkList()
        assert False == script.should_process_lane(worklist)

    def test_site_configuration_has_changed(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        end_to_end_search_fixture.external_search_index.start_migration().finish()

        library = db.default_library()
        lane1 = db.lane()
        lane2 = db.lane()

        # Run the script to create all the default config settings.
        UpdateLaneSizeScript(
            db.session,
            search_index_client=end_to_end_search_fixture.external_search_index,
        ).do_run(cmd_args=[])

        # Set the lane sizes
        lane1.size = 100
        lane2.size = 50

        # Commit changes to the DB so the lane update listeners are fired
        db.session.flush()

        with patch(
            "core.model.listeners.site_configuration_has_changed"
        ) as listeners_changed:
            with patch(
                "core.scripts.site_configuration_has_changed"
            ) as scripts_changed:
                UpdateLaneSizeScript(db.session).do_run(cmd_args=[])

        assert 0 == lane1.size
        assert 0 == lane2.size

        # The listeners in lane.py shouldn't call site_configuration_has_changed
        listeners_changed.assert_not_called()

        # The script should call site_configuration_has_changed once when it is done
        scripts_changed.assert_called_once()


class TestUpdateCustomListSizeScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        customlist, ignore = db.customlist(num_entries=1)
        customlist.library = db.default_library()
        customlist.size = 100
        UpdateCustomListSizeScript(db.session).do_run(cmd_args=[])
        assert 1 == customlist.size


class TestDeleteInvisibleLanesScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        """Test that invisible lanes and their visible children are deleted."""
        # create a library
        short_name = "TESTLIB"
        l1 = db.library("test library", short_name=short_name)
        # with a set of default lanes
        create_default_lanes(db.session, l1)

        # verify there is a top level visible Fiction lane
        top_level_fiction_lane: Lane = (
            db.session.query(Lane)
            .filter(Lane.library == l1)
            .filter(Lane.parent == None)
            .filter(Lane.display_name == "Fiction")
            .order_by(Lane.priority)
            .one()
        )

        first_child_id = top_level_fiction_lane.children[0].id

        assert top_level_fiction_lane is not None
        assert top_level_fiction_lane.visible == True
        assert first_child_id is not None

        # run script and verify that it had no effect:
        DeleteInvisibleLanesScript(_db=db.session).do_run([short_name])
        top_level_fiction_lane: Lane = (
            db.session.query(Lane)
            .filter(Lane.library == l1)
            .filter(Lane.parent == None)
            .filter(Lane.display_name == "Fiction")
            .order_by(Lane.priority)
            .one()
        )
        assert top_level_fiction_lane is not None

        # flag as deleted
        top_level_fiction_lane.visible = False

        # and now run script.
        DeleteInvisibleLanesScript(_db=db.session).do_run([short_name])

        # verify the lane has now been deleted.
        deleted_lane = (
            db.session.query(Lane)
            .filter(Lane.library == l1)
            .filter(Lane.parent == None)
            .filter(Lane.display_name == "Fiction")
            .order_by(Lane.priority)
            .all()
        )

        assert deleted_lane == []

        # verify the first child was also deleted:

        first_child_lane = (
            db.session.query(Lane).filter(Lane.id == first_child_id).all()
        )

        assert first_child_lane == []


class TestCustomListUpdateEntriesScriptData:
    populated_books: list[Work]
    unpopular_books: list[Work]


class TestCustomListUpdateEntriesScript:
    @staticmethod
    def _populate_works(
        data: EndToEndSearchFixture,
    ) -> TestCustomListUpdateEntriesScriptData:
        db = data.external_search.db

        result = TestCustomListUpdateEntriesScriptData()
        result.populated_books = [
            db.work(with_license_pool=True, title="Populated Book") for _ in range(5)
        ]
        result.unpopular_books = [
            db.work(with_license_pool=True, title="Unpopular Book") for _ in range(3)
        ]
        # This is for back population only
        result.populated_books[0].license_pools[
            0
        ].availability_time = datetime.datetime(1900, 1, 1)
        db.session.commit()
        return result

    def test_process_custom_list(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        fixture = end_to_end_search_fixture
        db, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        last_updated = datetime.datetime.now() - datetime.timedelta(hours=1)
        custom_list, _ = db.customlist()
        custom_list.library = db.default_library()
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Populated Book"))
        )
        custom_list.auto_update_last_update = last_updated
        custom_list.auto_update_status = CustomList.UPDATED

        custom_list1, _ = db.customlist()
        custom_list1.library = db.default_library()
        custom_list1.auto_update_enabled = True
        custom_list1.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Unpopular Book"))
        )
        custom_list1.auto_update_last_update = last_updated
        custom_list1.auto_update_status = CustomList.UPDATED

        # Do the process
        script = CustomListUpdateEntriesScript(session)
        mock_parse = MagicMock()
        mock_parse.return_value.libraries = [db.default_library()]
        script.parse_command_line = mock_parse

        with freeze_time("2022-01-01") as frozen_time:
            script.run()

        session.refresh(custom_list)
        session.refresh(custom_list1)
        assert (
            len(custom_list.entries) == 1 + len(data.populated_books) - 1
        )  # default + new - one past availability time
        assert custom_list.size == 1 + len(data.populated_books) - 1
        assert len(custom_list1.entries) == 1 + len(
            data.unpopular_books
        )  # default + new
        assert custom_list1.size == 1 + len(data.unpopular_books)
        # last updated time has updated correctly
        assert custom_list.auto_update_last_update == frozen_time.time_to_freeze
        assert custom_list1.auto_update_last_update == frozen_time.time_to_freeze

    def test_search_facets(
        self, db: DatabaseTransactionFixture, services_fixture_wired: ServicesFixture
    ):
        mock_index = services_fixture_wired.search_fixture.index_mock

        last_updated = datetime.datetime.now() - datetime.timedelta(hours=1)
        custom_list, _ = db.customlist()
        custom_list.library = db.default_library()
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Populated Book"))
        )
        custom_list.auto_update_facets = json.dumps(
            dict(order="title", languages="fr", media=["book", "audio"])
        )
        custom_list.auto_update_last_update = last_updated

        script = CustomListUpdateEntriesScript(db.session)
        script.process_custom_list(custom_list)

        assert mock_index.query_works.call_count == 1
        filter: Filter = mock_index.query_works.call_args_list[0][0][1]
        assert filter.sort_order[0] == {
            "sort_title": "asc"
        }  # since we asked for title ordering this should come up first
        assert filter.languages == ["fr"]
        assert filter.media == ["book", "audio"]

    @freeze_time("2022-01-01", as_kwarg="frozen_time")
    def test_no_last_update(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        frozen_time=None,
    ):
        fixture = end_to_end_search_fixture
        db, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        # No previous timestamp
        custom_list, _ = db.customlist()
        custom_list.library = db.default_library()
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Populated Book"))
        )
        script = CustomListUpdateEntriesScript(session)
        script.process_custom_list(custom_list)
        assert custom_list.auto_update_last_update == frozen_time.time_to_freeze

    def test_init_backpopulates(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        with patch("core.scripts.CustomListQueries") as mock_queries:
            fixture = end_to_end_search_fixture
            db, session = (
                fixture.external_search.db,
                fixture.external_search.db.session,
            )
            data = self._populate_works(fixture)
            fixture.populate_search_index()

            custom_list, _ = db.customlist()
            custom_list.library = db.default_library()
            custom_list.auto_update_enabled = True
            custom_list.auto_update_query = json.dumps(
                dict(query=dict(key="title", value="Populated Book"))
            )
            script = CustomListUpdateEntriesScript(session)
            script.process_custom_list(custom_list)

            args = mock_queries.populate_query_pages.call_args_list[0]
            assert args[1]["json_query"] == None
            assert args[1]["start_page"] == 2
            assert custom_list.auto_update_status == CustomList.UPDATED

    def test_repopulate_state(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        """The repopulate deletes all entries and runs the query again"""
        fixture = end_to_end_search_fixture
        db, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        custom_list, _ = db.customlist()
        custom_list.library = db.default_library()
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = json.dumps(
            dict(query=dict(key="title", value="Populated Book"))
        )
        custom_list.auto_update_status = CustomList.REPOPULATE

        # Previously the list would have had Unpopular books
        for w in data.unpopular_books:
            custom_list.add_entry(w)
        prev_entry = custom_list.entries[0]

        script = CustomListUpdateEntriesScript(session)
        script.process_custom_list(custom_list)
        # Commit the process changes and refresh the list
        session.commit()
        session.refresh(custom_list)

        # Now the entries are only the Popular books
        assert {e.work_id for e in custom_list.entries} == {
            w.id for w in data.populated_books
        }
        # The previous entries should have been deleted, not just un-related
        with pytest.raises(InvalidRequestError):
            session.refresh(prev_entry)
        assert custom_list.auto_update_status == CustomList.UPDATED
        assert custom_list.size == len(data.populated_books)


class TestLoanNotificationsScript:
    def _setup_method(self, db: DatabaseTransactionFixture):
        self.script = LoanNotificationsScript(_db=db.session)
        self.patron: Patron = db.patron()
        self.work: Work = db.work(with_license_pool=True)
        self.device_token, _ = get_one_or_create(
            db.session,
            DeviceToken,
            patron=self.patron,
            token_type=DeviceTokenTypes.FCM_ANDROID,
            device_token="atesttoken",
        )
        PushNotifications.TESTING_MODE = True

    def test_loan_notification(self, db: DatabaseTransactionFixture):
        self._setup_method(db)
        p = self.work.active_license_pool()
        if p:  # mypy complains if we don't do this
            loan, _ = p.loan_to(
                self.patron,
                utc_now(),
                utc_now() + datetime.timedelta(days=1, hours=1),
            )

        work2 = db.work(with_license_pool=True)
        loan2, _ = work2.active_license_pool().loan_to(
            self.patron,
            utc_now(),
            utc_now() + datetime.timedelta(days=2, hours=1),
        )  # Should not get notified

        with patch("core.scripts.PushNotifications") as mock_notf:
            self.script.process_loan(loan)
            self.script.process_loan(loan2)

        assert mock_notf.send_loan_expiry_message.call_count == 1
        assert mock_notf.send_loan_expiry_message.call_args[0] == (
            loan,
            1,
            [self.device_token],
        )

    def test_do_run(self, db: DatabaseTransactionFixture):
        now = utc_now()
        self._setup_method(db)
        loan, _ = self.work.active_license_pool().loan_to(
            self.patron,
            now,
            now + datetime.timedelta(days=1, hours=1),
        )

        work2 = db.work(with_license_pool=True)
        loan2, _ = work2.active_license_pool().loan_to(
            self.patron,
            now,
            now + datetime.timedelta(days=2, hours=1),
        )

        work3 = db.work(with_license_pool=True)
        p = work3.active_license_pool()
        loan3, _ = p.loan_to(
            self.patron,
            now,
            now + datetime.timedelta(days=1, hours=1),
        )
        # loan 3 was notified today already, so should get skipped
        loan3.patron_last_notified = now.date()

        work4 = db.work(with_license_pool=True)
        p = work4.active_license_pool()
        loan4, _ = p.loan_to(
            self.patron,
            now,
            now + datetime.timedelta(days=1, hours=1),
        )
        # loan 4 was notified yesterday, so should NOT get skipped
        loan4.patron_last_notified = now.date() - datetime.timedelta(days=1)

        self.script.process_loan = MagicMock()
        self.script.BATCH_SIZE = 1
        self.script.do_run()

        assert self.script.process_loan.call_count == 3
        assert self.script.process_loan.call_args_list == [
            call(loan),
            call(loan2),
            call(loan4),
        ]

        # Sitewide notifications are turned off
        self.script.process_loan.reset_mock()
        ConfigurationSetting.sitewide(
            db.session, Configuration.PUSH_NOTIFICATIONS_STATUS
        ).value = ConfigurationConstants.FALSE
        self.script.do_run()
        assert self.script.process_loan.call_count == 0


class TestWorkConsolidationScript:
    """TODO"""


class TestWorkPresentationScript:
    """TODO"""


class TestWorkClassificationScript:
    """TODO"""


class TestWorkOPDSScript:
    """TODO"""


class TestCustomListManagementScript:
    """TODO"""


class TestNYTBestSellerListsScript:
    """TODO"""
