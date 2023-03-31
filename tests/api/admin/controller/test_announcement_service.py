import json

from werkzeug.datastructures import MultiDict

from api.admin.controller.announcement_service import AnnouncementSettings
from api.announcements import Announcements
from core.model.configuration import ConfigurationSetting
from core.problem_details import INVALID_INPUT
from tests.fixtures.announcements import AnnouncementFixture
from tests.fixtures.api_admin import AdminControllerFixture


class TestAnnouncementService:
    def test_get(
        self,
        admin_ctrl_fixture: AdminControllerFixture,
        announcement_fixture: AnnouncementFixture,
    ):
        setting = ConfigurationSetting.sitewide(
            admin_ctrl_fixture.ctrl.db.session, Announcements.GLOBAL_SETTING_NAME
        )
        setting.value = json.dumps(
            [
                announcement_fixture.expired,
                announcement_fixture.active,
                announcement_fixture.forthcoming,
            ]
        )
        global_announcements = Announcements.for_all(admin_ctrl_fixture.ctrl.db.session)

        with admin_ctrl_fixture.request_context_with_admin("/", method="GET") as ctx:
            response = AnnouncementSettings(admin_ctrl_fixture.manager).process_many()

        assert set(response.keys()) == {"settings", "announcements"}
        announcements_in_response = response["announcements"]
        # Only one of the announcements should be active.
        assert 1 == len(list(global_announcements.active))
        # All announcements should be available for management.
        assert 3 == len(global_announcements.announcements)
        assert len(global_announcements.announcements) == len(announcements_in_response)

    def test_post(
        self,
        admin_ctrl_fixture: AdminControllerFixture,
        announcement_fixture: AnnouncementFixture,
    ):
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST") as ctx:
            ctx.request.form = MultiDict(
                [("announcements", json.dumps([announcement_fixture.active]))]
            )
            response = AnnouncementSettings(admin_ctrl_fixture.manager).process_many()

        announcements = Announcements.for_all(admin_ctrl_fixture.ctrl.db.session)
        assert len(announcements.announcements) == 1
        assert announcements.announcements[0].id == "active"

    def test_post_errors(
        self,
        admin_ctrl_fixture: AdminControllerFixture,
        announcement_fixture: AnnouncementFixture,
    ):
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST") as ctx:
            ctx.request.form = None
            response = AnnouncementSettings(admin_ctrl_fixture.manager).process_many()
            assert response == INVALID_INPUT

            ctx.request.form = MultiDict(
                [("somethingelse", json.dumps([announcement_fixture.active]))]
            )
            response = AnnouncementSettings(admin_ctrl_fixture.manager).process_many()
            assert response == INVALID_INPUT

            ctx.request.form = MultiDict(
                [("announcements", json.dumps([{"id": "xxx"}]))]
            )
            response = AnnouncementSettings(admin_ctrl_fixture.manager).process_many()
            assert "Missing required field: content" == response.detail
