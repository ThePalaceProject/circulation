import json

from api.admin.announcement_list_validator import AnnouncementListValidator
from api.announcements import Announcement, Announcements
from core.model.configuration import ConfigurationSetting
from tests.fixtures.announcements import AnnouncementFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestAnnouncements:
    """Test the Announcements object."""

    def test_for_library(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        """Verify that we can create an Announcements object for a library."""
        l = db.default_library()

        # By default, a library has no announcements.
        library_announcements = Announcements.for_library(l)
        assert [] == library_announcements.announcements

        # Give the library an announcement by setting its
        # "announcements" ConfigurationSetting.
        setting = l.setting(Announcements.SETTING_NAME)
        setting.value = json.dumps(
            [announcement_fixture.active, announcement_fixture.expired]
        )

        announcements = Announcements.for_library(l).announcements
        assert all(isinstance(a, Announcement) for a in announcements)

        active, expired = announcements
        assert "active" == active.id
        assert "expired" == expired.id

        # Put a bad value in the ConfigurationSetting, and it's
        # treated as an empty list. In real life this would only
        # happen due to a bug or a bad bit of manually entered SQL.
        invalid = dict(announcement_fixture.active)
        invalid["id"] = "Another ID"
        invalid["finish"] = "Not a date"
        setting.value = json.dumps(
            [announcement_fixture.active, invalid, announcement_fixture.expired]
        )
        assert [] == Announcements.for_library(l).announcements

    def test_for_all(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        assert [] == Announcements.for_all(db.session).announcements

        # This should not show up
        library_based = db.default_library().setting(Announcements.GLOBAL_SETTING_NAME)
        library_based.value = json.dumps([announcement_fixture.active])

        # Only there should show up
        conf = ConfigurationSetting.sitewide(
            db.session, Announcements.GLOBAL_SETTING_NAME
        )
        conf.value = json.dumps(
            [announcement_fixture.active, announcement_fixture.expired]
        )

        announcements = Announcements.for_all(db.session)
        assert len(announcements.announcements) == 2
        assert len(list(announcements.active)) == 1

    def test_active(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        # The Announcements object keeps track of all announcements, but
        # Announcements.active only yields the active ones.
        announcements = Announcements(
            [
                announcement_fixture.active,
                announcement_fixture.expired,
                announcement_fixture.forthcoming,
            ]
        )
        assert 3 == len(announcements.announcements)
        assert ["active"] == [x.id for x in announcements.active]

    # Throw in a few minor tests of Announcement while we're here.

    def test_is_active(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        # Test the rules about when an Announcement is 'active'
        assert True == Announcement(**announcement_fixture.active).is_active
        assert False == Announcement(**announcement_fixture.expired).is_active
        assert False == Announcement(**announcement_fixture.forthcoming).is_active

        # An announcement that ends today is still active.
        expires_today = dict(announcement_fixture.active)
        expires_today["finish"] = announcement_fixture.today
        assert True == Announcement(**announcement_fixture.active).is_active

    def test_for_authentication_document(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        # Demonstrate the publishable form of an Announcement.
        #
        # 'start' and 'finish' will be ignored, as will the extra value
        # that has no meaning within Announcement.
        announcement = Announcement(extra="extra value", **announcement_fixture.active)
        assert (
            dict(id="active", content="A sample announcement.")
            == announcement.for_authentication_document
        )

    def test_json_ready(
        self, db: DatabaseTransactionFixture, announcement_fixture: AnnouncementFixture
    ):
        # Demonstrate the form of an Announcement used to store in the database.
        #
        # 'start' and 'finish' will be converted into strings the extra value
        # that has no meaning within Announcement will be ignored.
        announcement = Announcement(extra="extra value", **announcement_fixture.active)
        assert (
            dict(
                id="active",
                content="A sample announcement.",
                start=announcement.start.strftime(
                    AnnouncementListValidator.DATE_FORMAT
                ),
                finish=announcement.finish.strftime(
                    AnnouncementListValidator.DATE_FORMAT
                ),
            )
            == announcement.json_ready
        )
