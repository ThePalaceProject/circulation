import uuid

import pytest
from sqlalchemy import select

from core.model.announcements import Announcement, AnnouncementData
from tests.fixtures.announcements import AnnouncementFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestAnnouncements:
    """Test the Announcements object."""

    def test_library_announcements(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        """Verify that we can create an Announcements object for a library."""
        l = db.default_library()

        # By default, a library has no announcements.
        library_announcements = db.session.execute(
            Announcement.library_announcements(l)
        ).all()
        assert [] == library_announcements

        # Give the library two announcements.
        active_expected = announcement_fixture.active_announcement(db.session, l)
        expired_expected = announcement_fixture.expired_announcement(db.session, l)
        library_announcements = (
            db.session.execute(Announcement.library_announcements(l)).scalars().all()
        )
        assert all(isinstance(a, Announcement) for a in library_announcements)

        expired, active = library_announcements
        assert active.id == active_expected.id
        assert expired.id == expired_expected.id

    def test_library_announcements_deleted(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        library = db.library()
        assert db.session.execute(select(Announcement)).all() == []

        # One active global announcement
        global_active_expected = announcement_fixture.active_announcement(db.session)

        # Four announcements for the library
        announcement_fixture.active_announcement(db.session, library)
        announcement_fixture.active_announcement(db.session, library)
        announcement_fixture.expired_announcement(db.session, library)
        announcement_fixture.forthcoming_announcement(db.session, library)

        # Make sure we have five announcements
        assert len(db.session.execute(select(Announcement)).all()) == 5

        # Delete the library
        db.session.delete(library)

        # The library announcements should be deleted, but the global
        # announcement should still be there.
        announcements = db.session.execute(select(Announcement)).scalars().all()
        assert len(announcements) == 1
        assert announcements == [global_active_expected]

    def test_global_announcements(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        assert [] == db.session.execute(Announcement.global_announcements()).all()

        # This should not show up
        library = db.default_library()
        announcement_fixture.active_announcement(db.session, library)

        # Only these two should show up
        active_expected = announcement_fixture.active_announcement(db.session)
        expired_expected = announcement_fixture.expired_announcement(db.session)

        expired, active = (
            db.session.execute(Announcement.global_announcements()).scalars().all()
        )

        assert active.id == active_expected.id
        assert isinstance(active, Announcement)
        assert expired.id == expired_expected.id
        assert isinstance(expired, Announcement)

    def test_authentication_document_announcements(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        """Verify that we can create an Announcements object for a library."""
        library = db.default_library()

        # Give the library four announcements.
        library_expected_today = announcement_fixture.active_announcement(
            db.session, library
        )
        library_expected_week_ago = announcement_fixture.create_announcement(
            db.session, library=library, start=announcement_fixture.a_week_ago
        )
        announcement_fixture.expired_announcement(db.session, library)
        announcement_fixture.forthcoming_announcement(db.session, library)

        # Create three global announcements.
        global_active_expected = announcement_fixture.active_announcement(db.session)
        announcement_fixture.expired_announcement(db.session)
        announcement_fixture.forthcoming_announcement(db.session)

        # The authentication document should only show the active announcements. The global announcement should
        # be first, followed by the library announcements. The announcements should be sorted by start date.
        auth_doc_announcements = Announcement.authentication_document_announcements(
            library
        )
        assert len(auth_doc_announcements) == 3
        assert auth_doc_announcements[0]["id"] == str(global_active_expected.id)
        assert auth_doc_announcements[1]["id"] == str(library_expected_week_ago.id)
        assert auth_doc_announcements[2]["id"] == str(library_expected_today.id)

    def test_from_data_global(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        """Verify that we can create an Announcement object from AnnouncementData."""
        # Create global announcement
        data = AnnouncementData(
            id=uuid.uuid4(),
            content="test",
            start=announcement_fixture.a_week_ago,
            finish=announcement_fixture.tomorrow,
        )
        announcement = Announcement.from_data(db.session, data)
        assert announcement.id == data.id
        assert announcement.content == data.content
        assert announcement.start == data.start
        assert announcement.finish == data.finish
        assert announcement.library is None

    def test_from_data_library(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        # Create library announcement
        library = db.default_library()
        data = AnnouncementData(
            id=uuid.uuid4(),
            content="test",
            start=announcement_fixture.a_week_ago,
            finish=announcement_fixture.tomorrow,
        )
        announcement = Announcement.from_data(db.session, data, library)
        assert announcement.id == data.id
        assert announcement.content == data.content
        assert announcement.start == data.start
        assert announcement.finish == data.finish
        assert announcement.library is library

    def test_from_data_no_uuid(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        # If no id is provided, one is generated
        data = AnnouncementData(
            content="test",
            start=announcement_fixture.a_week_ago,
            finish=announcement_fixture.tomorrow,
        )
        announcement = Announcement.from_data(db.session, data)
        assert data.id is None
        assert announcement.id is not None
        assert isinstance(announcement.id, uuid.UUID)
        assert announcement.content == data.content
        assert announcement.start == data.start
        assert announcement.finish == data.finish
        assert announcement.library is None

    def test_update(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        """Verify that we can update an Announcement object."""
        announcement = Announcement(
            id=uuid.uuid4(),
            content="test",
            start=announcement_fixture.today,
            finish=announcement_fixture.today,
        )
        data = AnnouncementData(
            content="new content",
            start=announcement_fixture.tomorrow,
            finish=announcement_fixture.in_a_week,
        )
        announcement.update(data)
        assert announcement.content == data.content
        assert announcement.start == data.start
        assert announcement.finish == data.finish

    def test_update_change_id(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        """Verify that we can't change the id of an Announcement object."""
        announcement = Announcement(
            id=uuid.uuid4(),
            content="test",
            start=announcement_fixture.today,
            finish=announcement_fixture.today,
        )
        data = announcement.to_data()
        data.id = uuid.uuid4()
        with pytest.raises(ValueError) as excinfo:
            announcement.update(data)
        assert "Cannot change announcement id from " in str(excinfo.value)

    def test_to_data(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        """Verify that we can convert an Announcement object to AnnouncementData."""
        announcement = Announcement(
            id=uuid.uuid4(),
            content="test",
            start=announcement_fixture.today,
            finish=announcement_fixture.today,
        )
        data = announcement.to_data()
        assert data.id == announcement.id
        assert data.content == announcement.content
        assert data.start == announcement.start
        assert data.finish == announcement.finish
