import pytest

from core.entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EntryPoint,
    EverythingEntryPoint,
    MediumEntryPoint,
)
from core.external_search import Filter
from core.model import Edition, Work
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture


class TestEntryPoint:
    def test_defaults(self):
        everything, ebooks, audiobooks = EntryPoint.ENTRY_POINTS
        assert EverythingEntryPoint == everything
        assert EbooksEntryPoint == ebooks
        assert AudiobooksEntryPoint == audiobooks

        display = EntryPoint.DISPLAY_TITLES
        assert "eBooks" == display[ebooks]
        assert "Audiobooks" == display[audiobooks]

        assert Edition.BOOK_MEDIUM == EbooksEntryPoint.INTERNAL_NAME
        assert Edition.AUDIO_MEDIUM == AudiobooksEntryPoint.INTERNAL_NAME

        assert "http://schema.org/CreativeWork" == everything.URI
        for ep in (EbooksEntryPoint, AudiobooksEntryPoint):
            assert ep.URI == Edition.medium_to_additional_type[ep.INTERNAL_NAME]

    def test_no_changes(self, db: DatabaseTransactionFixture):
        # EntryPoint doesn't modify queries or search filters.
        qu = db.session.query(Edition)
        assert qu == EntryPoint.modify_database_query(db.session, qu)
        args = dict(arg="value")

        filter = object()
        assert filter == EverythingEntryPoint.modify_search_filter(filter)

    def test_register(self):
        class Mock:
            pass

        args = [Mock, "Mock!"]

        with pytest.raises(ValueError) as excinfo:
            EntryPoint.register(*args)
        assert "must define INTERNAL_NAME" in str(excinfo.value)

        # Test successful registration.
        Mock.INTERNAL_NAME = "a name"
        EntryPoint.register(*args)
        assert Mock in EntryPoint.ENTRY_POINTS
        assert "Mock!" == EntryPoint.DISPLAY_TITLES[Mock]
        assert Mock not in EntryPoint.DEFAULT_ENABLED

        # Can't register twice.
        with pytest.raises(ValueError) as excinfo:
            EntryPoint.register(*args)
        assert "Duplicate entry point internal name: a name" in str(excinfo.value)

        EntryPoint.unregister(Mock)

        # Test successful registration as a default-enabled entry point.
        EntryPoint.register(*args, default_enabled=True)
        assert Mock in EntryPoint.DEFAULT_ENABLED

        # Can't register two different entry points with the same
        # display name.
        class Mock2:
            INTERNAL_NAME = "mock2"

        with pytest.raises(ValueError) as excinfo:
            EntryPoint.register(Mock2, "Mock!")
        assert "Duplicate entry point display name: Mock!" in str(excinfo.value)

        EntryPoint.unregister(Mock)
        assert Mock not in EntryPoint.DEFAULT_ENABLED


class TestEverythingEntryPoint:
    def test_no_changes(self, db: DatabaseTransactionFixture):
        # EverythingEntryPoint doesn't modify queries or searches
        # beyond the default behavior for any entry point.
        assert "All" == EverythingEntryPoint.INTERNAL_NAME

        qu = db.session.query(Edition)
        assert qu == EntryPoint.modify_database_query(db.session, qu)
        args = dict(arg="value")

        filter = object()
        assert filter == EverythingEntryPoint.modify_search_filter(filter)


class TestMediumEntryPoint:
    def test_modify_database_query(self, db: DatabaseTransactionFixture):
        # Create a video, and a entry point that contains videos.
        work = db.work(with_license_pool=True)
        work.license_pools[0].presentation_edition.medium = Edition.VIDEO_MEDIUM

        class Videos(MediumEntryPoint):
            INTERNAL_NAME = Edition.VIDEO_MEDIUM

        qu = db.session.query(Work)

        # The default entry points filter out the video.
        for entrypoint in EbooksEntryPoint, AudiobooksEntryPoint:
            modified = entrypoint.modify_database_query(db.session, qu)
            assert [] == modified.all()

        # But the video entry point includes it.
        videos = Videos.modify_database_query(db.session, qu)
        assert [work.id] == [x.id for x in videos]

    def test_modify_search_filter(self):
        class Mock(MediumEntryPoint):
            INTERNAL_NAME = object()

        filter = Filter(media=object())
        Mock.modify_search_filter(filter)
        assert [Mock.INTERNAL_NAME] == filter.media


class TestLibrary:
    """Test a Library's interaction with EntryPoints."""

    def test_enabled_entrypoints(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        settings = library_fixture.mock_settings()
        l = library_fixture.library(settings=settings)

        # When the value is not set, the default is used.
        assert EntryPoint.DEFAULT_ENABLED == list(l.entrypoints)

        # Names that don't correspond to registered entry points are
        # ignored. Names that do are looked up.
        settings.enabled_entry_points = [
            "no such entry point",
            AudiobooksEntryPoint.INTERNAL_NAME,
        ]
        assert [AudiobooksEntryPoint] == list(l.entrypoints)

        # An empty list is a valid value.
        settings.enabled_entry_points = []
        assert [] == list(l.entrypoints)
