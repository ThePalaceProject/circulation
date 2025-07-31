import logging
from unittest.mock import MagicMock, patch

import pytest
from pytest import MonkeyPatch
from requests import RequestException

from palace.manager.api.admin.config import (
    Configuration as AdminConfig,
    OperationalMode,
)
from tests.fixtures.test_utils import MonkeyPatchEnvFixture


class TestAdminUI:
    def test_package_version_cached(self):
        with patch.object(AdminConfig, "env_package_version") as env_package_version:
            AdminConfig._version = None
            env_package_version.return_value = None

            # The first call to package_version() should call env_package_version()
            assert AdminConfig.package_version() == AdminConfig.PACKAGE_VERSION
            assert env_package_version.call_count == 1
            env_package_version.reset_mock()

            # The second call to package_version() should not call env_package_version()
            # because the result is cached.
            assert AdminConfig.package_version() == AdminConfig.PACKAGE_VERSION
            assert env_package_version.call_count == 0

    @pytest.mark.parametrize(
        "package_version, resolves, expected_result",
        [
            ["1.0.0", False, "1.0.0"],
            ["latest", True, "x.x.x"],
            ["next", True, "x.x.x"],
            ["dev", True, "x.x.x"],
            [None, False, None],
        ],
    )
    def test_env_package_version(
        self,
        monkeypatch_env: MonkeyPatchEnvFixture,
        package_version: str | None,
        resolves: bool,
        expected_result: str | None,
    ):
        with patch.object(
            AdminConfig, "resolve_package_version"
        ) as resolve_package_version:
            resolve_package_version.return_value = "x.x.x"
            monkeypatch_env("TPP_CIRCULATION_ADMIN_PACKAGE_VERSION", package_version)
            assert AdminConfig.env_package_version() == expected_result
            assert resolve_package_version.call_count == (1 if resolves else 0)

    def test_resolve_package_version(self, caplog):
        with patch("palace.manager.api.admin.config.HTTP") as http_patch:
            http_patch.get_with_timeout.return_value = MagicMock(
                status_code=200, json=MagicMock(return_value={"version": "1.0.0"})
            )
            assert (
                AdminConfig.resolve_package_version("some-package", "latest") == "1.0.0"
            )
            http_patch.get_with_timeout.assert_called_once_with(
                "https://data.jsdelivr.com/v1/packages/npm/some-package/resolved?specifier=latest"
            )

            # If there is an exception while trying to resolve the package version, return the default.
            caplog.set_level(logging.ERROR)
            http_patch.get_with_timeout.side_effect = RequestException()
            assert (
                AdminConfig.resolve_package_version("some-package", "latest")
                == "latest"
            )
            assert len(caplog.records) == 1
            assert "Failed to resolve package version" in caplog.text

    @pytest.mark.parametrize(
        "package_name, package_version, mode, expected_result_startswith",
        [
            [
                None,
                None,
                OperationalMode.production,
                "https://cdn.jsdelivr.net/npm/@thepalaceproject/circulation-admin@",
            ],
            [
                "@some-scope/some-package",
                "1.0.0",
                OperationalMode.production,
                "https://cdn.jsdelivr.net/npm/@some-scope/some-package@1.0.0",
            ],
            [
                "some-package",
                "1.0.0",
                OperationalMode.production,
                "https://cdn.jsdelivr.net/npm/some-package@1.0.0",
            ],
            [None, None, OperationalMode.development, "/"],
            [None, "1.0.0", OperationalMode.development, "/"],
            ["some-package", "1.0.0", OperationalMode.development, "/"],
        ],
    )
    def test_package_url(
        self,
        monkeypatch_env: MonkeyPatchEnvFixture,
        package_name: str | None,
        package_version: str | None,
        mode: OperationalMode,
        expected_result_startswith: str,
    ):
        monkeypatch_env("TPP_CIRCULATION_ADMIN_PACKAGE_NAME", package_name)
        monkeypatch_env("TPP_CIRCULATION_ADMIN_PACKAGE_VERSION", package_version)
        result = AdminConfig.package_url(_operational_mode=mode)
        assert result.startswith(expected_result_startswith)
        # Reset the cached version
        AdminConfig._version = None

    @pytest.mark.parametrize(
        "package_name, package_version, expected_result",
        [
            [
                None,
                None,
                "/my-base-dir/node_modules/@thepalaceproject/circulation-admin",
            ],
            [
                None,
                "1.0.0",
                "/my-base-dir/node_modules/@thepalaceproject/circulation-admin",
            ],
            ["some-package", "1.0.0", "/my-base-dir/node_modules/some-package"],
        ],
    )
    def test_package_development_directory(
        self,
        monkeypatch_env: MonkeyPatchEnvFixture,
        package_name: str | None,
        package_version: str | None,
        expected_result: str,
    ):
        monkeypatch_env("TPP_CIRCULATION_ADMIN_PACKAGE_NAME", package_name)
        monkeypatch_env("TPP_CIRCULATION_ADMIN_PACKAGE_VERSION", package_version)
        result = AdminConfig.package_development_directory(_base_dir="/my-base-dir")
        assert result == expected_result

    @pytest.mark.parametrize(
        "asset_key, operational_mode, expected_result",
        [
            [
                "admin_css",
                OperationalMode.development,
                "/admin/static/circulation-admin.css",
            ],
            [
                "admin_css",
                OperationalMode.production,
                "https://cdn.jsdelivr.net/npm/known-package-name@1.0.0/dist/circulation-admin.css",
            ],
            [
                "admin_js",
                OperationalMode.development,
                "/admin/static/circulation-admin.js",
            ],
            [
                "admin_js",
                OperationalMode.production,
                "https://cdn.jsdelivr.net/npm/known-package-name@1.0.0/dist/circulation-admin.js",
            ],
            [
                "another-asset.jpg",
                OperationalMode.development,
                "/admin/static/another-asset.jpg",
            ],
            [
                "another-asset.jpg",
                OperationalMode.production,
                "https://cdn.jsdelivr.net/npm/known-package-name@1.0.0/dist/another-asset.jpg",
            ],
        ],
    )
    def test_lookup_asset_url(
        self,
        monkeypatch_env: MonkeyPatchEnvFixture,
        asset_key: str,
        operational_mode: OperationalMode,
        expected_result: str,
    ):
        monkeypatch_env("TPP_CIRCULATION_ADMIN_PACKAGE_NAME", "known-package-name")
        monkeypatch_env("TPP_CIRCULATION_ADMIN_PACKAGE_VERSION", "1.0.0")
        result = AdminConfig.lookup_asset_url(
            key=asset_key, _operational_mode=operational_mode
        )
        assert result == expected_result


class TestAdminClientSettings:

    @patch("palace.manager.api.admin.config.AdminClientSettings")
    def test_admin_client_settings_cached(
        self,
        admin_client_settings_class: MagicMock,
        monkeypatch: MonkeyPatch,
    ):
        # Ensure that we don't have any cached settings.
        monkeypatch.setattr(AdminConfig, "_admin_client_settings", None)

        # with patch("palace.manager.api.admin.config.AdminClientSettings") as admin_client_settings_class:
        admin_client_settings_class.assert_not_called()

        # The first time through, we have to call to populate the cache.
        admin_client_settings_class.reset_mock()
        settings1 = AdminConfig.admin_client_settings()
        assert settings1 is not None
        assert admin_client_settings_class.call_count == 1

        # The second time through, we just get it from the cache.
        admin_client_settings_class.reset_mock()
        settings2 = AdminConfig.admin_client_settings()
        assert settings2 is not None
        assert settings2 == settings1
        assert admin_client_settings_class.call_count == 0

    @pytest.mark.parametrize(
        "should_hide, expected_setting",
        (
            pytest.param(None, True, id="unspecified"),
            pytest.param("true", True, id="true"),
            pytest.param("false", False, id="false"),
        ),
    )
    def test_hide_subscription_config(
        self,
        monkeypatch_env: MonkeyPatchEnvFixture,
        should_hide: str | None,
        expected_setting,
        monkeypatch: MonkeyPatch,
    ):
        monkeypatch_env("PALACE_ADMINUI_HIDE_SUBSCRIPTION_CONFIG", should_hide)
        monkeypatch.setattr(AdminConfig, "_admin_client_settings", None)
        assert (
            AdminConfig.admin_client_settings().hide_subscription_config
            == expected_setting
        )
