import os
from typing import Optional

import pytest

from api.admin.config import Configuration as AdminConfig
from api.admin.config import OperationalMode


class TestAdminUI:
    @staticmethod
    def _set_env(monkeypatch, key: str, value: Optional[str]):
        if value:
            monkeypatch.setenv(key, value)
        elif key in os.environ:
            monkeypatch.delenv(key)

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
