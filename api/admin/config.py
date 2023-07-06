import os
from enum import Enum
from typing import Optional
from urllib.parse import urljoin


class OperationalMode(str, Enum):
    production = "production"
    development = "development"


class Configuration:

    APP_NAME = "Palace Collection Manager"
    PACKAGE_NAME = "@thepalaceproject/circulation-admin"
    PACKAGE_VERSION = "1.7.1"

    STATIC_ASSETS = {
        "admin_js": "circulation-admin.js",
        "admin_css": "circulation-admin.css",
        "admin_logo": "PalaceCollectionManagerLogo.svg",
    }

    # For proper operation, `package_url` MUST end with a slash ('/') and
    # `asset_rel_url` MUST NOT begin with one.
    PACKAGE_TEMPLATES = {
        OperationalMode.production: {
            "package_url": "https://cdn.jsdelivr.net/npm/{name}@{version}/",
            "asset_rel_url": "dist/{filename}",
        },
        OperationalMode.development: {
            "package_url": "/admin/",
            "asset_rel_url": "static/{filename}",
        },
    }

    DEVELOPMENT_MODE_PACKAGE_TEMPLATE = "node_modules/{name}"
    STATIC_ASSETS_REL_PATH = "dist"

    ADMIN_DIRECTORY = os.path.abspath(os.path.dirname(__file__))

    # Environment variables that contain admin client package information.
    ENV_ADMIN_UI_PACKAGE_NAME = "TPP_CIRCULATION_ADMIN_PACKAGE_NAME"
    ENV_ADMIN_UI_PACKAGE_VERSION = "TPP_CIRCULATION_ADMIN_PACKAGE_VERSION"

    @classmethod
    def operational_mode(cls) -> OperationalMode:
        return (
            OperationalMode.development
            if os.path.isdir(cls.package_development_directory())
            else OperationalMode.production
        )

    @classmethod
    def package_name(cls) -> str:
        """Get the effective package name.

        :return: A package name.
        :rtype: str
        """
        return os.environ.get(cls.ENV_ADMIN_UI_PACKAGE_NAME) or cls.PACKAGE_NAME

    @classmethod
    def package_version(cls) -> str:
        """Get the effective package version.

        :return Package verison.
        """
        return os.environ.get(cls.ENV_ADMIN_UI_PACKAGE_VERSION) or cls.PACKAGE_VERSION

    @classmethod
    def lookup_asset_url(
        cls, key: str, *, _operational_mode: Optional[OperationalMode] = None
    ) -> str:
        """Get the URL for the asset_type.

        :param key: The key used to lookup an asset's filename. If the key is
            not found in the asset list, then the key itself is used as the asset.
        :type key: str
        :param _operational_mode: Provided for testing purposes. The operational
            mode is normally determined by local state
        :type _operational_mode: OperationalMode
        :return: A URL string.
        :rtype: str
        """
        operational_mode = _operational_mode or cls.operational_mode()
        filename = cls.STATIC_ASSETS.get(key, key)
        return urljoin(
            cls.package_url(_operational_mode=operational_mode),
            cls.PACKAGE_TEMPLATES[operational_mode]["asset_rel_url"].format(
                filename=filename
            ),
        )

    @classmethod
    def package_url(cls, *, _operational_mode: Optional[OperationalMode] = None) -> str:
        """Compute the URL for the admin UI package.

        :param _operational_mode: For testing. The operational mode is
            normally determined by local state.
        :type _operational_mode: OperationalMode
        :return: String representation of the URL/path for either the asset
            of the given type or, if no type is specified, the base path
            of the package.
        :rtype: str
        """
        operational_mode = _operational_mode or cls.operational_mode()
        template = cls.PACKAGE_TEMPLATES[operational_mode]["package_url"]
        url = template.format(name=cls.package_name(), version=cls.package_version())
        if not url.endswith("/"):
            url += "/"
        return url

    @classmethod
    def package_development_directory(cls, *, _base_dir: Optional[str] = None) -> str:
        """Absolute path for the admin UI package when in development mode.

        :param _base_dir: For testing purposes. Not used in normal operation.
        :type _base_dir: str
        :returns: String containing absolute path to the admin UI package.
        :rtype: str
        """
        base_dir = _base_dir or cls.ADMIN_DIRECTORY
        return os.path.join(
            base_dir,
            cls.DEVELOPMENT_MODE_PACKAGE_TEMPLATE.format(name=cls.package_name()),
        )

    @classmethod
    def static_files_directory(cls, *, _base_dir: Optional[str] = None) -> str:
        """Absolute path for the admin UI static files.

        :param _base_dir: For testing purposes. Not used in normal operation.
        :type _base_dir: str
        :returns: String containing absolute path to the admin UI package.
        :rtype: str
        """
        package_dir = cls.package_development_directory(_base_dir=_base_dir)
        return os.path.join(package_dir, cls.STATIC_ASSETS_REL_PATH)
