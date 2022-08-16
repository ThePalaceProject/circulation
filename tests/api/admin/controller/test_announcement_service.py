import json

from werkzeug.datastructures import MultiDict

from api.admin.controller.announcement_service import AnnouncementSettings
from api.announcements import Announcements
from api.testing import AnnouncementTest
from core.model.configuration import ConfigurationSetting
from core.problem_details import INVALID_INPUT
from tests.api.admin.controller.test_controller import AdminControllerTest


class TestAnnouncementService(AnnouncementTest, AdminControllerTest):
    def test_get(self):
        setting = ConfigurationSetting.sitewide(
            self._db, Announcements.GLOBAL_SETTING_NAME
        )
        setting.value = json.dumps([self.expired, self.active])

        with self.request_context_with_admin("/", method="GET") as ctx:
            response = AnnouncementSettings(self.manager).process_many()

        assert set(response.keys()) == {"settings", "announcements"}
        assert len(response["announcements"]) == 1
        assert response["announcements"][0]["id"] == "active"

    def test_post(self):
        with self.request_context_with_admin("/", method="POST") as ctx:
            ctx.request.form = MultiDict([("announcements", json.dumps([self.active]))])
            response = AnnouncementSettings(self.manager).process_many()

        announcements = Announcements.for_all(self._db)
        assert len(announcements.announcements) == 1
        assert announcements.announcements[0].id == "active"

    def test_post_errors(self):
        with self.request_context_with_admin("/", method="POST") as ctx:
            ctx.request.form = None
            response = AnnouncementSettings(self.manager).process_many()
            assert response == INVALID_INPUT

            ctx.request.form = MultiDict([("somethingelse", json.dumps([self.active]))])
            response = AnnouncementSettings(self.manager).process_many()
            assert response == INVALID_INPUT

            ctx.request.form = MultiDict(
                [("announcements", json.dumps([{"id": "xxx"}]))]
            )
            response = AnnouncementSettings(self.manager).process_many()
            assert "Missing required field: content" == response.detail
