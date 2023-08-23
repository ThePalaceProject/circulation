import flask
import pytest
from werkzeug.datastructures import MultiDict

from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    MISSING_SITEWIDE_SETTING_KEY,
    MISSING_SITEWIDE_SETTING_VALUE,
)
from api.config import Configuration
from core.model import AdminRole, ConfigurationSetting


class TestSitewideSettings:
    def test_sitewide_settings_get(self, settings_ctrl_fixture):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_get()
            )
            settings = response.get("settings")
            all_settings = response.get("all_settings")

            assert [] == settings
            keys = [s.get("key") for s in all_settings]
            assert Configuration.LOG_LEVEL in keys
            assert Configuration.DATABASE_LOG_LEVEL in keys
            assert Configuration.SECRET_KEY in keys

        ConfigurationSetting.sitewide(
            settings_ctrl_fixture.ctrl.db.session, Configuration.DATABASE_LOG_LEVEL
        ).value = "INFO"
        ConfigurationSetting.sitewide(
            settings_ctrl_fixture.ctrl.db.session, Configuration.SECRET_KEY
        ).value = "secret"
        settings_ctrl_fixture.ctrl.db.session.flush()

        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_get()
            )
            settings = response.get("settings")
            all_settings = response.get("all_settings")

            assert 2 == len(settings)
            settings_by_key = {s.get("key"): s.get("value") for s in settings}
            assert "INFO" == settings_by_key.get(Configuration.DATABASE_LOG_LEVEL)
            assert "secret" == settings_by_key.get(Configuration.SECRET_KEY)
            keys = [s.get("key") for s in all_settings]
            assert Configuration.LOG_LEVEL in keys
            assert Configuration.DATABASE_LOG_LEVEL in keys
            assert Configuration.SECRET_KEY in keys

            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            settings_ctrl_fixture.ctrl.db.session.flush()
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_get,
            )

    def test_sitewide_settings_post_errors(self, settings_ctrl_fixture):
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([("key", None)])
            response = (
                settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_post()
            )
            assert response == MISSING_SITEWIDE_SETTING_KEY

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [("key", Configuration.SECRET_KEY), ("value", None)]
            )
            response = (
                settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_post()
            )
            assert response == MISSING_SITEWIDE_SETTING_VALUE

        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("key", Configuration.SECRET_KEY),
                    ("value", "secret"),
                ]
            )
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_post,
            )

    def test_sitewide_settings_post_create(self, settings_ctrl_fixture):
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("key", Configuration.DATABASE_LOG_LEVEL),
                    ("value", "10"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_post()
            )
            assert response.status_code == 200

        # The setting was created.
        setting = ConfigurationSetting.sitewide(
            settings_ctrl_fixture.ctrl.db.session, Configuration.DATABASE_LOG_LEVEL
        )
        assert setting.key == response.get_data(as_text=True)
        assert "10" == setting.value

    def test_sitewide_settings_post_edit(self, settings_ctrl_fixture):
        setting = ConfigurationSetting.sitewide(
            settings_ctrl_fixture.ctrl.db.session, Configuration.DATABASE_LOG_LEVEL
        )
        setting.value = "WARN"

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("key", Configuration.DATABASE_LOG_LEVEL),
                    ("value", "ERROR"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_post()
            )
            assert response.status_code == 200

        # The setting was changed.
        assert setting.key == response.get_data(as_text=True)
        assert "ERROR" == setting.value

    def test_sitewide_setting_delete(self, settings_ctrl_fixture):
        setting = ConfigurationSetting.sitewide(
            settings_ctrl_fixture.ctrl.db.session, Configuration.DATABASE_LOG_LEVEL
        )
        setting.value = "WARN"

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_delete,
                setting.key,
            )

            settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = settings_ctrl_fixture.manager.admin_sitewide_configuration_settings_controller.process_delete(
                setting.key
            )
            assert response.status_code == 200

        assert None == setting.value
