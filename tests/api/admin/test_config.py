import logging
import os
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from requests import RequestException

from api.admin.config import Configuration as AdminConfig
from api.admin.config import OperationalMode


class TestAdminUI:
    @staticmethod
    def _set_env(monkeypatch, key: str, value: Optional[str]):
        if value:
            monkeypatch.setenv(key, value)
        elif key in os.environ:
            monkeypatch.delenv(key)

    def test_package_version_cached(self, monkeypatch):
        with patch(
            "api.admin.config.Configuration.env_package_version"
        ) as env_package_version:
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
        monkeypatch,
        package_version: Optional[str],
        resolves: bool,
        expected_result: Optional[str],
    ):
        with patch(
            "api.admin.config.Configuration.resolve_package_version"
        ) as resolve_package_version:
            resolve_package_version.return_value = "x.x.x"
            self._set_env(
                monkeypatch, "TPP_CIRCULATION_ADMIN_PACKAGE_VERSION", package_version
            )
            assert AdminConfig.env_package_version() == expected_result
            assert resolve_package_version.call_count == (1 if resolves else 0)

    def test_resolve_package_version(self, caplog):
        with patch("api.admin.config.HTTP") as http_patch:
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
        monkeypatch,
        package_name: Optional[str],
        package_version: Optional[str],
        mode: OperationalMode,
        expected_result_startswith: str,
    ):
        self._set_env(monkeypatch, "TPP_CIRCULATION_ADMIN_PACKAGE_NAME", package_name)
        self._set_env(
            monkeypatch, "TPP_CIRCULATION_ADMIN_PACKAGE_VERSION", package_version
        )
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
        monkeypatch,
        package_name: Optional[str],
        package_version: Optional[str],
        expected_result: str,
    ):
        self._set_env(monkeypatch, "TPP_CIRCULATION_ADMIN_PACKAGE_NAME", package_name)
        self._set_env(
            monkeypatch, "TPP_CIRCULATION_ADMIN_PACKAGE_VERSION", package_version
        )
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
        monkeypatch,
        asset_key: str,
        operational_mode: OperationalMode,
        expected_result: str,
    ):
        self._set_env(
            monkeypatch, "TPP_CIRCULATION_ADMIN_PACKAGE_NAME", "known-package-name"
        )
        self._set_env(monkeypatch, "TPP_CIRCULATION_ADMIN_PACKAGE_VERSION", "1.0.0")
        result = AdminConfig.lookup_asset_url(
            key=asset_key, _operational_mode=operational_mode
        )
        assert result == expected_result
