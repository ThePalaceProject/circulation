from __future__ import annotations

import datetime
import logging
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional
from unittest.mock import MagicMock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from alembic.util import CommandError
from api.adobe_vendor_id import AuthdataUtility
from api.config import Configuration
from api.marc import LibraryAnnotator as MARCLibraryAnnotator
from api.novelist import NoveListAPI
from core.entrypoint import AudiobooksEntryPoint, EbooksEntryPoint
from core.external_search import ExternalSearchIndex, mock_search_index
from core.lane import Facets, FeaturedFacets, Pagination, WorkList
from core.marc import MARCExporter
from core.model import (
    LOCK_ID_DB_INIT,
    CachedMARCFile,
    ConfigurationSetting,
    Credential,
    DataSource,
    ExternalIntegration,
    SessionManager,
    create,
)
from core.opds import AcquisitionFeed
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.flask_util import OPDSFeedResponse, Response
from scripts import (
    AdobeAccountIDResetScript,
    CacheFacetListsPerLane,
    CacheMARCFiles,
    CacheOPDSGroupFeedPerLane,
    CacheRepresentationPerLane,
    GenerateShortTokenScript,
    InstanceInitializationScript,
    LanguageListScript,
    LocalAnalyticsExportScript,
    NovelistSnapshotScript,
)
from tests.api.mockapi.circulation import MockCirculationManager
from tests.fixtures.library import LibraryFixture
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixtureFake
from tests.mocks.search import fake_hits

if TYPE_CHECKING:
    from tests.fixtures.authenticator import SimpleAuthIntegrationFixture
    from tests.fixtures.database import DatabaseTransactionFixture
    from tests.fixtures.search import ExternalSearchFixture


class TestAdobeAccountIDResetScript:
    def test_process_patron(self, db: DatabaseTransactionFixture):
        patron = db.patron()

        # This patron has a credential that links them to a Adobe account ID
        def set_value(credential):
            credential.value = "a credential"

        # Data source doesn't matter -- even if it's incorrect, a Credential
        # of the appropriate type will be deleted.
        data_source = DataSource.lookup(db.session, DataSource.OVERDRIVE)

        # Create one Credential that will be deleted and one that will be
        # left alone.
        for type in (
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            "Some other type",
        ):
            credential = Credential.lookup(
                db.session, data_source, type, patron, set_value, True
            )

        assert 2 == len(patron.credentials)

        # Run the patron through the script.
        script = AdobeAccountIDResetScript(db.session)

        # A dry run does nothing.
        script.delete = False
        script.process_patron(patron)
        db.session.commit()
        assert 2 == len(patron.credentials)

        # Now try it for real.
        script.delete = True
        script.process_patron(patron)
        db.session.commit()

        # The Adobe-related credential is gone. The other one remains.
        [credential] = patron.credentials
        assert "Some other type" == credential.type


class LaneScriptFixture:
    def __init__(self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture):
        self.db = db
        base_url_setting = ConfigurationSetting.sitewide(
            self.db.session, Configuration.BASE_URL_KEY
        )
        base_url_setting.value = "http://test-circulation-manager/"
        library = db.default_library()
        settings = library_fixture.mock_settings()
        settings.large_collection_languages = []
        settings.small_collection_languages = []
        settings.tiny_collection_languages = ["eng", "fre"]
        library.update_settings(settings)


@pytest.fixture(scope="function")
def lane_script_fixture(
    db: DatabaseTransactionFixture, library_fixture: LibraryFixture
) -> LaneScriptFixture:
    return LaneScriptFixture(db, library_fixture)


class TestCacheRepresentationPerLane:
    def test_should_process_lane(self, lane_script_fixture: LaneScriptFixture):
        db = lane_script_fixture.db

        # Test that should_process_lane respects any specified
        # language restrictions.
        script = CacheRepresentationPerLane(
            db.session,
            [
                "--language=fre",
                "--language=English",
                "--language=none",
                "--min-depth=0",
            ],
            manager=object(),
        )
        assert ["fre", "eng"] == script.languages

        english_lane = db.lane(languages=["eng"])
        assert True == script.should_process_lane(english_lane)

        no_english_lane = db.lane(languages=["spa", "fre"])
        assert True == script.should_process_lane(no_english_lane)

        no_english_or_french_lane = db.lane(languages=["spa"])
        assert False == script.should_process_lane(no_english_or_french_lane)

        # Test that should_process_lane respects maximum depth
        # restrictions.
        script = CacheRepresentationPerLane(
            db.session, ["--max-depth=0", "--min-depth=0"], manager=object()
        )
        assert 0 == script.max_depth

        child = db.lane(display_name="sublane")
        parent = db.lane(display_name="parent")
        parent.sublanes = [child]
        assert True == script.should_process_lane(parent)
        assert False == script.should_process_lane(child)

        script = CacheRepresentationPerLane(
            db.session, ["--min-depth=1"], manager=MockCirculationManager(db.session)
        )
        assert 1 == script.min_depth
        assert False == script.should_process_lane(parent)
        assert True == script.should_process_lane(child)

    def test_process_lane(self, lane_script_fixture: LaneScriptFixture):
        db = lane_script_fixture.db
        # process_lane() calls do_generate() once for every
        # combination of items yielded by facets() and pagination().

        class MockFacets:
            def __init__(self, query):
                self.query = query

            @property
            def query_string(self):
                return self.query

        facets1 = MockFacets("facets1")
        facets2 = MockFacets("facets2")
        page1 = Pagination.default()
        page2 = page1.next_page

        class Mock(CacheRepresentationPerLane):
            generated = []

            def do_generate(self, lane, facets, pagination):
                value = (lane, facets, pagination)
                response = Response("mock response")
                response.value = value
                self.generated.append(response)
                return response

            def facets(self, lane):
                yield facets1
                yield facets2

            def pagination(self, lane):
                yield page1
                yield page2

        lane = db.lane()
        script = Mock(db.session, manager=object(), cmd_args=[])
        generated = script.process_lane(lane)
        assert generated == script.generated

        c1, c2, c3, c4 = (x.value for x in script.generated)
        assert (lane, facets1, page1) == c1
        assert (lane, facets1, page2) == c2
        assert (lane, facets2, page1) == c3
        assert (lane, facets2, page2) == c4

    def test_default_facets(self, lane_script_fixture: LaneScriptFixture):
        db = lane_script_fixture.db
        # By default, do_generate will only be called once, with facets=None.
        script = CacheRepresentationPerLane(db.session, manager=object(), cmd_args=[])
        assert [None] == list(script.facets(object()))

    def test_default_pagination(self, lane_script_fixture: LaneScriptFixture):
        db = lane_script_fixture.db
        # By default, do_generate will only be called once, with pagination=None.
        script = CacheRepresentationPerLane(db.session, manager=object(), cmd_args=[])
        assert [None] == list(script.pagination(object()))


class TestCacheFacetListsPerLane:
    def test_arguments(self, lane_script_fixture: LaneScriptFixture):
        db = lane_script_fixture.db
        # Verify that command-line arguments become attributes of
        # the CacheFacetListsPerLane object.
        script = CacheFacetListsPerLane(
            db.session, ["--order=title", "--order=added"], manager=object()
        )
        assert ["title", "added"] == script.orders
        script = CacheFacetListsPerLane(
            db.session,
            ["--availability=all", "--availability=always"],
            manager=object(),
        )
        assert ["all", "always"] == script.availabilities

        script = CacheFacetListsPerLane(
            db.session, ["--collection=main", "--collection=full"], manager=object()
        )
        assert ["main", "full"] == script.collections

        script = CacheFacetListsPerLane(
            db.session, ["--entrypoint=Audio", "--entrypoint=Book"], manager=object()
        )
        assert ["Audio", "Book"] == script.entrypoints

        script = CacheFacetListsPerLane(db.session, ["--pages=1"], manager=object())
        assert 1 == script.pages

    def test_facets(
        self, lane_script_fixture: LaneScriptFixture, library_fixture: LibraryFixture
    ):
        db = lane_script_fixture.db
        # Verify that CacheFacetListsPerLane.facets combines the items
        # found in the attributes created by command-line parsing.
        script = CacheFacetListsPerLane(db.session, manager=object(), cmd_args=[])
        script.orders = [Facets.ORDER_TITLE, Facets.ORDER_AUTHOR, "nonsense"]
        script.entrypoints = [
            AudiobooksEntryPoint.INTERNAL_NAME,
            "nonsense",
            EbooksEntryPoint.INTERNAL_NAME,
        ]
        script.availabilities = [Facets.AVAILABLE_NOW, "nonsense"]
        script.collections = [Facets.COLLECTION_FULL, "nonsense"]

        library = library_fixture.library()

        # EbooksEntryPoint is normally a valid entry point, but we're
        # going to disable it for this library.
        settings = library_fixture.mock_settings()
        settings.enabled_entry_points = [AudiobooksEntryPoint.INTERNAL_NAME]
        library.update_settings(settings)

        lane = db.lane(library=library)

        # We get one Facets object for every valid combination
        # of parameters. Here there are 2*1*1*1 combinations.
        f1, f2 = script.facets(lane)

        # The facets differ only in their .order.
        assert Facets.ORDER_TITLE == f1.order
        assert Facets.ORDER_AUTHOR == f2.order

        # All other fields are tied to the only acceptable values
        # given in the script attributes. The first (and only)
        # enabled entry point is treated as the default.
        for f in f1, f2:
            assert AudiobooksEntryPoint == f.entrypoint
            assert True == f.entrypoint_is_default
            assert Facets.AVAILABLE_NOW == f.availability
            assert Facets.COLLECTION_FULL == f.collection

        # The first entry point is treated as the default only for WorkLists
        # that have no parent. When the WorkList has a parent, the selected
        # entry point is treated as an explicit choice -- navigating downward
        # in the lane hierarchy ratifies the default value.
        sublane = db.lane(parent=lane, library=library)
        f1, f2 = script.facets(sublane)
        for f in f1, f2:
            assert False == f.entrypoint_is_default

    def test_pagination(self, lane_script_fixture: LaneScriptFixture):
        db = lane_script_fixture.db
        script = CacheFacetListsPerLane(db.session, manager=object(), cmd_args=[])
        script.pages = 3
        lane = db.lane()
        p1, p2, p3 = script.pagination(lane)
        pagination = Pagination.default()
        assert pagination.query_string == p1.query_string
        assert pagination.next_page.query_string == p2.query_string
        assert pagination.next_page.next_page.query_string == p3.query_string

    def test_do_generate(
        self,
        lane_script_fixture: LaneScriptFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        db = lane_script_fixture.db
        migration = end_to_end_search_fixture.external_search_index.start_migration()
        assert migration is not None
        migration.finish()

        # When it's time to generate a feed, AcquisitionFeed.page
        # is called with the right arguments.
        class MockAcquisitionFeed:
            called_with = None

            @classmethod
            def page(cls, **kwargs):
                cls.called_with = kwargs
                resp = MagicMock()
                resp.as_response.return_value = "here's your feed"
                return resp

        # Test our ability to generate a single feed.
        script = CacheFacetListsPerLane(db.session, testing=True, cmd_args=[])
        facets = Facets.default(db.default_library())
        pagination = Pagination.default()

        with script.app.test_request_context("/"):
            lane = db.lane()
            result = script.do_generate(
                lane, facets, pagination, feed_class=MockAcquisitionFeed
            )
            assert "here's your feed" == result

            args = MockAcquisitionFeed.called_with
            assert db.session == args["_db"]  # type: ignore
            assert lane == args["worklist"]  # type: ignore
            assert lane.display_name == args["title"]  # type: ignore

            # The Pagination object was passed into
            # MockAcquisitionFeed.page, and it was also used to make the
            # feed URL (see below).
            assert pagination == args["pagination"]  # type: ignore

            # The Facets object was passed into
            # MockAcquisitionFeed.page, and it was also used to make
            # the feed URL and to create the feed annotator.
            assert facets == args["facets"]  # type: ignore
            annotator = args["annotator"]  # type: ignore
            assert facets == annotator.facets
            assert args["url"] == annotator.feed_url(  # type: ignore
                lane, facets=facets, pagination=pagination
            )

            # Try again without mocking AcquisitionFeed, to verify that
            # we get a Flask Response containing an OPDS feed.
            response = script.do_generate(lane, facets, pagination)
            assert isinstance(response, OPDSFeedResponse)
            assert AcquisitionFeed.ACQUISITION_FEED_TYPE == response.content_type
            assert response.get_data(as_text=True).startswith("<feed")


class TestCacheOPDSGroupFeedPerLane:
    def test_should_process_lane(self, lane_script_fixture: LaneScriptFixture):
        db = lane_script_fixture.db
        parent = db.lane()
        child = db.lane(parent=parent)
        grandchild = db.lane(parent=child)

        # Only WorkLists which have children are processed.
        script = CacheOPDSGroupFeedPerLane(db.session, manager=object(), cmd_args=[])
        script.max_depth = 10
        assert True == script.should_process_lane(parent)
        assert True == script.should_process_lane(child)
        assert False == script.should_process_lane(grandchild)

        # If a WorkList is deeper in the hierarchy than max_depth,
        # it's not processed, even if it has children.
        script.max_depth = 0
        assert True == script.should_process_lane(parent)
        assert False == script.should_process_lane(child)

    def test_do_generate(
        self,
        lane_script_fixture: LaneScriptFixture,
        external_search_fixture: ExternalSearchFixture,
    ):
        db = lane_script_fixture.db
        external_search_fixture.init_indices()
        # When it's time to generate a feed, AcquisitionFeed.groups
        # is called with the right arguments.

        class MockAcquisitionFeed:
            called_with = None

            @classmethod
            def groups(cls, **kwargs):
                cls.called_with = kwargs
                resp = MagicMock()
                resp.as_response.return_value = "here's your feed"
                return resp

        # Test our ability to generate a single feed.
        script = CacheOPDSGroupFeedPerLane(db.session, testing=True, cmd_args=[])
        facets = FeaturedFacets(0.1, entrypoint=AudiobooksEntryPoint)
        pagination = None

        with script.app.test_request_context("/"):
            lane = db.lane()
            result = script.do_generate(
                lane, facets, pagination, feed_class=MockAcquisitionFeed
            )
            assert "here's your feed" == result

            args = MockAcquisitionFeed.called_with
            assert db.session == args["_db"]  # type: ignore
            assert lane == args["worklist"]  # type: ignore
            assert lane.display_name == args["title"]  # type: ignore
            assert pagination == None

            # The Facets object was passed into
            # MockAcquisitionFeed.page, and it was also used to make
            # the feed URL and to create the feed annotator.
            assert facets == args["facets"]  # type: ignore
            annotator = args["annotator"]  # type: ignore
            assert facets == annotator.facets
            assert args["url"] == annotator.groups_url(lane, facets)  # type: ignore

            # Try again without mocking AcquisitionFeed to verify that
            # we get a Flask response.
            response = script.do_generate(lane, facets, pagination)
            assert AcquisitionFeed.ACQUISITION_FEED_TYPE == response.content_type
            assert response.get_data(as_text=True).startswith("<feed")

    def test_facets(
        self, lane_script_fixture: LaneScriptFixture, library_fixture: LibraryFixture
    ):
        db = lane_script_fixture.db
        # Normally we yield one FeaturedFacets object for each of the
        # library's enabled entry points.
        library = db.default_library()
        script = CacheOPDSGroupFeedPerLane(db.session, manager=object(), cmd_args=[])
        settings = library_fixture.mock_settings()
        settings.enabled_entry_points = [
            AudiobooksEntryPoint.INTERNAL_NAME,
            EbooksEntryPoint.INTERNAL_NAME,
        ]
        library.update_settings(settings)

        lane = db.lane()
        audio_facets, ebook_facets = script.facets(lane)
        assert AudiobooksEntryPoint == audio_facets.entrypoint
        assert EbooksEntryPoint == ebook_facets.entrypoint

        # The first entry point in the library's list of enabled entry
        # points is treated as the default.
        assert True == audio_facets.entrypoint_is_default
        assert audio_facets.entrypoint == list(library.entrypoints)[0]
        assert False == ebook_facets.entrypoint_is_default

        for facets in (audio_facets, ebook_facets):
            # The FeaturedFacets objects knows to feature works at the
            # library's minimum quality level.
            assert (
                library.settings.minimum_featured_quality
                == facets.minimum_featured_quality
            )

        # The first entry point is treated as the default only for WorkLists
        # that have no parent. When the WorkList has a parent, the selected
        # entry point is treated as an explicit choice  -- navigating downward
        # in the lane hierarchy ratifies the default value.
        sublane = db.lane(parent=lane)
        f1, f2 = script.facets(sublane)
        for f in f1, f2:
            assert False == f.entrypoint_is_default

        # Make it look like the lane uses custom lists.
        lane.list_datasource = DataSource.lookup(db.session, DataSource.OVERDRIVE)

        # If the library has no enabled entry points, we yield one
        # FeaturedFacets object with no particular entry point.
        settings.enabled_entry_points = []
        library.update_settings(settings)
        (no_entry_point,) = script.facets(lane)
        assert None == no_entry_point.entrypoint

    # We no longer cache the feeds
    @pytest.mark.skip
    def test_do_run(
        self,
        lane_script_fixture: LaneScriptFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        db = lane_script_fixture.db

        work = db.work(fiction=True, with_license_pool=True, genre="Science Fiction")
        work.quality = 1
        lane = db.lane(display_name="Fantastic Fiction", fiction=True)
        sublane = db.lane(
            parent=lane,
            display_name="Science Fiction",
            fiction=True,
            genres=["Science Fiction"],
        )
        search_engine = external_search_fake_fixture.external_search
        search_engine.query_works_multi = MagicMock(  # type: ignore [method-assign]
            return_value=[fake_hits([work]), fake_hits([work])]
        )
        with mock_search_index(search_engine):
            script = CacheOPDSGroupFeedPerLane(db.session, cmd_args=[])
            script.do_run(cmd_args=[])

        # The Lane object was disconnected from its database session
        # when the app server was initialized. Reconnect it.
        lane = db.session.merge(lane)
        [feed] = lane.cachedfeeds

        assert "Fantastic Fiction" in feed.content
        assert "Science Fiction" in feed.content
        assert work.title in feed.content


class TestCacheMARCFilesFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.lane = db.lane(genres=["Science Fiction"])
        self.integration = db.external_integration(
            ExternalIntegration.MARC_EXPORT, ExternalIntegration.CATALOG_GOAL
        )

        self.exporter = MARCExporter(None, None, self.integration)
        self.mock_records = MagicMock()
        self.mock_services = MagicMock()
        self.exporter.records = self.mock_records

    def script(self, cmd_args: Optional[list[str]] = None) -> CacheMARCFiles:
        cmd_args = cmd_args or []
        return CacheMARCFiles(
            self.db.session, services=self.mock_services, cmd_args=cmd_args
        )

    def assert_call(self, call: Any) -> None:
        assert call.args[0] == self.lane
        assert isinstance(call.args[1], MARCLibraryAnnotator)
        assert call.args[2] == self.mock_services.storage.public.return_value

    def create_cached_file(self, end_time: datetime.datetime) -> CachedMARCFile:
        representation, _ = self.db.representation()
        cached, _ = create(
            self.db.session,
            CachedMARCFile,
            library=self.db.default_library(),
            lane=self.lane,
            representation=representation,
            end_time=end_time,
        )
        return cached


@pytest.fixture
def cache_marc_files(db: DatabaseTransactionFixture) -> TestCacheMARCFilesFixture:
    return TestCacheMARCFilesFixture(db)


class TestCacheMARCFiles:
    def test_should_process_library(self, lane_script_fixture: LaneScriptFixture):
        db = lane_script_fixture.db
        script = CacheMARCFiles(db.session, cmd_args=[])
        assert False == script.should_process_library(db.default_library())
        integration = db.external_integration(
            ExternalIntegration.MARC_EXPORT,
            ExternalIntegration.CATALOG_GOAL,
            libraries=[db.default_library()],
        )
        assert True == script.should_process_library(db.default_library())

    def test_should_process_lane(self, lane_script_fixture: LaneScriptFixture):
        db = lane_script_fixture.db
        parent = db.lane()
        parent.size = 100
        child = db.lane(parent=parent)
        child.size = 10
        grandchild = db.lane(parent=child)
        grandchild.size = 1
        wl = WorkList()
        empty = db.lane(fiction=False)
        empty.size = 0

        script = CacheMARCFiles(db.session, cmd_args=[])
        script.max_depth = 1
        assert True == script.should_process_lane(parent)
        assert True == script.should_process_lane(child)
        assert False == script.should_process_lane(grandchild)
        assert True == script.should_process_lane(wl)
        assert False == script.should_process_lane(empty)

        script.max_depth = 0
        assert True == script.should_process_lane(parent)
        assert False == script.should_process_lane(child)
        assert False == script.should_process_lane(grandchild)
        assert True == script.should_process_lane(wl)
        assert False == script.should_process_lane(empty)

    def test_process_lane_never_run(self, cache_marc_files: TestCacheMARCFilesFixture):
        script = cache_marc_files.script()
        script.process_lane(cache_marc_files.lane, cache_marc_files.exporter)

        # If the script has never been run before, it runs the exporter once
        # to create a file with all records.
        assert cache_marc_files.mock_records.call_count == 1
        cache_marc_files.assert_call(cache_marc_files.mock_records.call_args)

    def test_process_lane_cached_update(
        self, cache_marc_files: TestCacheMARCFilesFixture
    ):
        # If we have a cached file already, and it's old enough, the script will
        # run the exporter twice, first to update that file and second to create
        # a file with changes since that first file was originally created.
        db = cache_marc_files.db
        now = utc_now()
        last_week = now - datetime.timedelta(days=7)
        cache_marc_files.create_cached_file(last_week)
        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MARCExporter.UPDATE_FREQUENCY,
            db.default_library(),
            cache_marc_files.integration,
        ).value = 3

        script = cache_marc_files.script()
        script.process_lane(cache_marc_files.lane, cache_marc_files.exporter)
        assert cache_marc_files.mock_records.call_count == 2

        # First call
        cache_marc_files.assert_call(cache_marc_files.mock_records.call_args_list[0])

        # Second call
        cache_marc_files.assert_call(cache_marc_files.mock_records.call_args_list[1])
        assert (
            cache_marc_files.mock_records.call_args_list[1].kwargs["start_time"]
            < last_week
        )

    def test_process_lane_cached_recent(
        self, cache_marc_files: TestCacheMARCFilesFixture
    ):
        # If we already have a recent cached file, the script won't do anything.
        db = cache_marc_files.db
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        cache_marc_files.create_cached_file(yesterday)
        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MARCExporter.UPDATE_FREQUENCY,
            db.default_library(),
            cache_marc_files.integration,
        ).value = 3

        script = cache_marc_files.script()
        script.process_lane(cache_marc_files.lane, cache_marc_files.exporter)
        assert cache_marc_files.mock_records.call_count == 0

    def test_process_lane_cached_recent_force(
        self, cache_marc_files: TestCacheMARCFilesFixture
    ):
        # But we can force it to run anyway.
        db = cache_marc_files.db
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(days=7)
        cache_marc_files.create_cached_file(yesterday)
        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MARCExporter.UPDATE_FREQUENCY,
            db.default_library(),
            cache_marc_files.integration,
        ).value = 3

        script = cache_marc_files.script(cmd_args=["--force"])
        script.process_lane(cache_marc_files.lane, cache_marc_files.exporter)
        assert cache_marc_files.mock_records.call_count == 2

        # First call
        cache_marc_files.assert_call(cache_marc_files.mock_records.call_args_list[0])

        # Second call
        cache_marc_files.assert_call(cache_marc_files.mock_records.call_args_list[1])
        assert (
            cache_marc_files.mock_records.call_args_list[1].kwargs["start_time"]
            < yesterday
        )
        assert (
            cache_marc_files.mock_records.call_args_list[1].kwargs["start_time"]
            > last_week
        )

    def test_process_lane_cached_frequency_zero(
        self, cache_marc_files: TestCacheMARCFilesFixture
    ):
        # The update frequency can also be 0, in which case it will always run.
        # If we already have a recent cached file, the script won't do anything.
        db = cache_marc_files.db
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(days=7)
        cache_marc_files.create_cached_file(yesterday)
        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MARCExporter.UPDATE_FREQUENCY,
            db.default_library(),
            cache_marc_files.integration,
        ).value = 0
        script = cache_marc_files.script()
        script.process_lane(cache_marc_files.lane, cache_marc_files.exporter)

        assert cache_marc_files.mock_records.call_count == 2

        # First call
        cache_marc_files.assert_call(cache_marc_files.mock_records.call_args_list[0])

        # Second call
        cache_marc_files.assert_call(cache_marc_files.mock_records.call_args_list[1])
        assert (
            cache_marc_files.mock_records.call_args_list[1].kwargs["start_time"]
            < yesterday
        )
        assert (
            cache_marc_files.mock_records.call_args_list[1].kwargs["start_time"]
            > last_week
        )


class TestInstanceInitializationScript:
    # These are some basic tests for the instance initialization script. It is tested
    # more thoroughly as part of the migration tests, since migration tests are able
    # to test the script's interaction with the database.

    def test_run_locks_database(self, db: DatabaseTransactionFixture):
        # The script locks the database with a PostgreSQL advisory lock
        with patch("scripts.SessionManager") as session_manager:
            with patch("scripts.pg_advisory_lock") as advisory_lock:
                script = InstanceInitializationScript()
                script.initialize = MagicMock()
                script.run()

                advisory_lock.assert_called_once_with(
                    session_manager.engine().begin().__enter__(),
                    LOCK_ID_DB_INIT,
                )
                advisory_lock().__enter__.assert_called_once()
                advisory_lock().__exit__.assert_called_once()

    def test_initialize(self, db: DatabaseTransactionFixture):
        # Test that the script inspects the database and initializes or migrates the database
        # as necessary.
        with patch("scripts.inspect") as inspect:
            script = InstanceInitializationScript()
            script.migrate_database = MagicMock()  # type: ignore[method-assign]
            script.initialize_database = MagicMock()  # type: ignore[method-assign]
            script.initialize_search_indexes = MagicMock()  # type: ignore[method-assign]

            # If the database is uninitialized, initialize_database() is called.
            inspect().has_table.return_value = False
            script.initialize(MagicMock())
            script.initialize_database.assert_called_once()
            script.migrate_database.assert_not_called()

            # If the database is initialized, migrate_database() is called.
            script.initialize_database.reset_mock()
            script.migrate_database.reset_mock()
            inspect().has_table.return_value = True
            script.initialize(MagicMock())
            script.initialize_database.assert_not_called()
            script.migrate_database.assert_called_once()

    def test_initialize_alembic_exception(self, caplog: LogCaptureFixture):
        # Test that we handle a CommandError exception being returned by Alembic.
        with patch("scripts.inspect") as inspect:
            with patch("scripts.container_instance"):
                script = InstanceInitializationScript()

            caplog.set_level(logging.ERROR)
            script.migrate_database = MagicMock(side_effect=CommandError("test"))
            script.initialize_database = MagicMock()
            script.initialize_search_indexes = MagicMock()

            # If the database is initialized, migrate_database() is called.
            inspect().has_table.return_value = True
            script.initialize(MagicMock())
            script.initialize_database.assert_not_called()
            script.migrate_database.assert_called_once()

            assert "Error running database migrations" in caplog.text

    def test_initialize_database(self, db: DatabaseTransactionFixture):
        # Test that the script initializes the database.
        script = InstanceInitializationScript()
        mock_db = MagicMock()

        with patch(
            "scripts.SessionManager", autospec=SessionManager
        ) as session_manager:
            with patch(
                "scripts.ExternalSearchIndex", autospec=ExternalSearchIndex
            ) as search_index:
                with patch("scripts.command") as alemic_command:
                    script.initialize_database(mock_db)

        session_manager.initialize_data.assert_called_once()
        session_manager.initialize_schema.assert_called_once()
        search_index.assert_called_once()
        alemic_command.stamp.assert_called_once()

    def test_migrate_database(self, db: DatabaseTransactionFixture):
        script = InstanceInitializationScript()
        mock_db = MagicMock()

        with patch("scripts.command") as alemic_command:
            script.migrate_database(mock_db)

        alemic_command.upgrade.assert_called_once()

    def test_find_alembic_ini(self, db: DatabaseTransactionFixture):
        # Make sure we find alembic.ini for script command
        mock_connection = MagicMock()
        conf = InstanceInitializationScript._get_alembic_config(mock_connection)
        assert isinstance(conf.config_file_name, str)
        assert Path(conf.config_file_name).exists()
        assert conf.config_file_name.endswith("alembic.ini")
        assert conf.attributes["connection"] == mock_connection.engine
        assert conf.attributes["configure_logger"] is False

    def test_initialize_search_indexes(
        self, end_to_end_search_fixture: EndToEndSearchFixture
    ):
        db = end_to_end_search_fixture.db
        search = end_to_end_search_fixture.external_search_index
        base_name = search._revision_base_name
        script = InstanceInitializationScript()

        _mockable_search = ExternalSearchIndex(db.session)
        _mockable_search.start_migration = MagicMock()  # type: ignore [method-assign]
        _mockable_search.search_service = MagicMock()  # type: ignore [method-assign]
        _mockable_search.log = MagicMock()

        def mockable_search(*args):
            return _mockable_search

        # Initially this should not exist, if InstanceInit has not been run
        assert search.search_service().read_pointer() == None

        with patch("scripts.ExternalSearchIndex", new=mockable_search):
            # To fake "no migration is available", mock all the values

            _mockable_search.start_migration.return_value = None
            _mockable_search.search_service().is_pointer_empty.return_value = True
            # Migration should fail
            assert script.initialize_search_indexes(db.session) == False
            # Logs were emitted
            assert _mockable_search.log.warning.call_count == 1
            assert (
                "no migration was available"
                in _mockable_search.log.warning.call_args[0][0]
            )

            _mockable_search.search_service.reset_mock()
            _mockable_search.start_migration.reset_mock()
            _mockable_search.log.reset_mock()

            # In case there is no need for a migration, read pointer exists as a non-empty pointer
            _mockable_search.search_service().is_pointer_empty.return_value = False
            # Initialization should pass, as a no-op
            assert script.initialize_search_indexes(db.session) == True
            assert _mockable_search.start_migration.call_count == 0

        # Initialization should work now
        assert script.initialize_search_indexes(db.session) == True
        # Then we have the latest version index
        assert (
            search.search_service().read_pointer()
            == search._revision.name_for_index(base_name)
        )

    def test_initialize_search_indexes_no_integration(
        self, db: DatabaseTransactionFixture
    ):
        script = InstanceInitializationScript()
        script._log = MagicMock()
        # No integration mean no migration
        assert script.initialize_search_indexes(db.session) == False
        assert script._log.error.call_count == 2
        assert "No search integration" in script._log.error.call_args[0][0]


class TestLanguageListScript:
    def test_languages(self, db: DatabaseTransactionFixture):
        """Test the method that gives this script the bulk of its output."""
        english = db.work(language="eng", with_open_access_download=True)
        tagalog = db.work(language="tgl", with_license_pool=True)
        [pool] = tagalog.license_pools
        db.add_generic_delivery_mechanism(pool)
        script = LanguageListScript(db.session)
        output = list(script.languages(db.default_library()))

        # English is ignored because all its works are open-access.
        # Tagalog shows up with the correct estimate.
        assert ["tgl 1 (Tagalog)"] == output


class TestNovelistSnapshotScript:
    def mockNoveListAPI(self, *args, **kwargs):
        self.called_with = (args, kwargs)

    def test_do_run(self, db: DatabaseTransactionFixture):
        """Test that NovelistSnapshotScript.do_run() calls the NoveList api."""

        class MockNovelistSnapshotScript(NovelistSnapshotScript):
            pass

        oldNovelistConfig = NoveListAPI.from_config
        NoveListAPI.from_config = self.mockNoveListAPI

        l1 = db.library()
        cmd_args = [l1.name]
        script = MockNovelistSnapshotScript(db.session)
        script.do_run(cmd_args=cmd_args)

        (params, args) = self.called_with

        assert params[0] == l1

        NoveListAPI.from_config = oldNovelistConfig


class TestLocalAnalyticsExportScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        class MockLocalAnalyticsExporter:
            def export(self, _db, start, end):
                self.called_with = [start, end]
                return "test"

        output = StringIO()
        cmd_args = ["--start=20190820", "--end=20190827"]
        exporter = MockLocalAnalyticsExporter()
        script = LocalAnalyticsExportScript()
        script.do_run(output=output, cmd_args=cmd_args, exporter=exporter)
        assert "test" == output.getvalue()
        assert ["20190820", "20190827"] == exporter.called_with


class TestGenerateShortTokenScript:
    @pytest.fixture
    def script(self):
        return GenerateShortTokenScript()

    @pytest.fixture
    def output(self):
        return StringIO()

    @pytest.fixture
    def authdata(self, monkeypatch):
        authdata = AuthdataUtility(
            vendor_id="The Vendor ID",
            library_uri="http://your-library.org/",
            library_short_name="you",
            secret="Your library secret",
        )
        test_date = datetime_utc(2021, 5, 5)
        monkeypatch.setattr(authdata, "_now", lambda: test_date)
        return authdata

    @pytest.fixture
    def patron(self, authdata, db: DatabaseTransactionFixture):
        patron = db.patron(external_identifier="test")
        patron.authorization_identifier = "test"
        adobe_credential = db.credential(
            data_source_name=DataSource.INTERNAL_PROCESSING,
            patron=patron,
            type=authdata.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
        )
        adobe_credential.credential = "1234567"
        return patron

    @pytest.fixture
    def authentication_provider(
        self,
        db: DatabaseTransactionFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        barcode = "12345"
        pin = "abcd"
        create_simple_auth_integration(db.default_library(), barcode, pin)
        return barcode, pin

    def test_run_days(
        self, script, output, authdata, patron, db: DatabaseTransactionFixture
    ):
        # Test with --days
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--days=2",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert output.getvalue().split("\n") == [
            "Vendor ID: The Vendor ID",
            "Token: YOU|1620345600|1234567|ZP45vhpfs3fHREvFkDDVgDAmhoD699elFD3PGaZu7yo@",
            "Username: YOU|1620345600|1234567",
            "Password: ZP45vhpfs3fHREvFkDDVgDAmhoD699elFD3PGaZu7yo@",
            "",
        ]

    def test_run_minutes(
        self, script, output, authdata, patron, db: DatabaseTransactionFixture
    ):
        # Test with --minutes
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--minutes=20",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert output.getvalue().split("\n")[2] == "Username: YOU|1620174000|1234567"

    def test_run_hours(
        self, script, output, authdata, patron, db: DatabaseTransactionFixture
    ):
        # Test with --hours
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--hours=4",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert output.getvalue().split("\n")[2] == "Username: YOU|1620187200|1234567"

    def test_no_registry(self, script, output, patron, db: DatabaseTransactionFixture):
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--minutes=20",
            db.default_library().short_name,
        ]
        with pytest.raises(SystemExit) as pytest_exit:
            script.do_run(_db=db.session, output=output, cmd_args=cmd_args)
        assert pytest_exit.value.code == -1
        assert "Library not registered with library registry" in output.getvalue()

    def test_no_patron_auth_method(
        self, script, output, db: DatabaseTransactionFixture
    ):
        # Test running when the patron does not exist
        cmd_args = [
            "--barcode={}".format("1234567"),
            "--hours=4",
            db.default_library().short_name,
        ]
        with pytest.raises(SystemExit) as pytest_exit:
            script.do_run(_db=db.session, output=output, cmd_args=cmd_args)
        assert pytest_exit.value.code == -1
        assert "No methods to authenticate patron found" in output.getvalue()

    def test_patron_auth(
        self,
        script,
        output,
        authdata,
        authentication_provider,
        db: DatabaseTransactionFixture,
    ):
        barcode, pin = authentication_provider
        # Test running when the patron does not exist
        cmd_args = [
            f"--barcode={barcode}",
            f"--pin={pin}",
            "--hours=4",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert "Token: YOU|1620187200" in output.getvalue()

    def test_patron_auth_no_patron(
        self,
        script,
        output,
        authdata,
        authentication_provider,
        db: DatabaseTransactionFixture,
    ):
        barcode = "nonexistent"
        # Test running when the patron does not exist
        cmd_args = [
            f"--barcode={barcode}",
            "--hours=4",
            db.default_library().short_name,
        ]
        with pytest.raises(SystemExit) as pytest_exit:
            script.do_run(
                _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
            )
        assert pytest_exit.value.code == -1
        assert "Patron not found" in output.getvalue()
