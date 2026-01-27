import os
from enum import Enum
from typing import Any, ClassVar
from urllib.parse import urljoin, urlparse

from pydantic import AliasGenerator, Field, model_validator
from pydantic.alias_generators import to_camel
from pydantic_settings import SettingsConfigDict
from requests import RequestException

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.http.exception import RequestNetworkException
from palace.manager.util.http.http import HTTP
from palace.manager.util.log import LoggerMixin


class AdminClientFeatureFlags(ServiceConfiguration):
    # The following CAN be overridden by environment variables.
    reports_only_for_sysadmins: bool = Field(
        True,
        description="Show inventory reports only for sysadmins.",
    )
    quicksight_only_for_sysadmins: bool = Field(
        True,
        description="Show QuickSight dashboards only for sysadmins.",
    )

    # The following fields are currently not overridden anywhere in our config,
    # since they have been fully rolled out and are not expected to change.
    # TODO: Remove these fields here and in the admin client if we no longer
    #   need to support them as feature flags.
    enable_auto_list: bool = Field(
        True,
        description="Enable auto-list of items.",
    )
    show_circ_events_download: bool = Field(
        True,
        description="Show download button for Circulation Events.",
    )
    model_config = SettingsConfigDict(
        env_prefix="PALACE_ADMINUI_FEATURE_",
        alias_generator=AliasGenerator(serialization_alias=to_camel),
    )


class AdminClientSettings(ServiceConfiguration):
    """Settings for the admin client."""

    DEFAULT_SUPPORT_CONTACT_TEXT: ClassVar[str] = "Contact support."

    model_config = SettingsConfigDict(env_prefix="PALACE_ADMINUI_")

    # This flag suppresses visibility of the collection subscription config in the admin UI.
    hide_subscription_config: bool = True

    # This is an optional support contact URL. This will be embedded in
    # web pages. http[s] and mailto URLs work best here.
    support_contact_url: str | None = None
    # Optional support contact label text. This will be used as part of
    # the label for the support contact link in views and in the admin UI.
    support_contact_text: str | None = None

    @model_validator(mode="before")
    def set_support_contact_text_default(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Set sensible support contact link text, if URL is set but text is not."""
        support_contact_url = values.get("support_contact_url")
        support_contact_text = values.get("support_contact_text")
        if support_contact_text is None and support_contact_url is not None:
            parsed = urlparse(support_contact_url)
            values["support_contact_text"] = (
                f"Email {parsed.path}."
                if parsed.scheme == "mailto"
                else cls.DEFAULT_SUPPORT_CONTACT_TEXT
            )
        return values


class OperationalMode(str, Enum):
    production = "production"
    development = "development"


class Configuration(LoggerMixin):
    APP_NAME = "Palace Collection Manager"
    PACKAGE_NAME = "@thepalaceproject/circulation-admin"
    PACKAGE_VERSION = "1.35.0"

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

    METADATA_URL_TEMPLATE = (
        "https://data.jsdelivr.com/v1/packages/npm/{name}/resolved?specifier={version}"
    )

    DEVELOPMENT_MODE_PACKAGE_TEMPLATE = "node_modules/{name}"
    STATIC_ASSETS_REL_PATH = "dist"

    ADMIN_DIRECTORY = os.path.abspath(os.path.dirname(__file__))

    # Environment variables that contain admin client package information.
    ENV_ADMIN_UI_PACKAGE_NAME = "TPP_CIRCULATION_ADMIN_PACKAGE_NAME"
    ENV_ADMIN_UI_PACKAGE_VERSION = "TPP_CIRCULATION_ADMIN_PACKAGE_VERSION"

    # Cache the package version after first lookup.
    _version: str | None = None

    # Cache the feature flags after the first lookup.
    _admin_ui_feature_flags: AdminClientFeatureFlags | None = None
    _admin_client_settings: AdminClientSettings | None = None

    # Admin client feature flags
    @classmethod
    def admin_feature_flags(cls) -> AdminClientFeatureFlags:
        if not cls._admin_ui_feature_flags:
            cls._admin_ui_feature_flags = AdminClientFeatureFlags()
        return cls._admin_ui_feature_flags

    @classmethod
    def admin_client_settings(cls) -> AdminClientSettings:
        if not cls._admin_client_settings:
            cls._admin_client_settings = AdminClientSettings()
        return cls._admin_client_settings

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
    def resolve_package_version(cls, package_name: str, package_version: str) -> str:
        """Resolve a package version to a specific version, if necessary. For
        example, if the version is a tag or partial semver. This is done by
        querying the jsdelivr API."""
        url = cls.METADATA_URL_TEMPLATE.format(
            name=package_name, version=package_version
        )
        try:
            response = HTTP.get_with_timeout(url)
            if response.status_code == 200 and "version" in response.json():
                return str(response.json()["version"])
        except (RequestNetworkException, RequestException):
            cls.logger().exception("Failed to resolve package version.")
            # If the request fails, just return the version as-is.
            ...

        return package_version

    @classmethod
    def env_package_version(cls) -> str | None:
        """Get the package version specified in configuration or environment.

        :return Package verison.
        """
        if cls.ENV_ADMIN_UI_PACKAGE_VERSION not in os.environ:
            return None

        version = os.environ[cls.ENV_ADMIN_UI_PACKAGE_VERSION]
        if version in ["latest", "next", "dev"]:
            version = cls.resolve_package_version(cls.package_name(), version)

        return version

    @classmethod
    def package_version(cls) -> str:
        """Get the effective package version, resolved to a specific version,
        if necessary. For example, if the version is a tag or partial semver.
        This is done by querying the jsdelivr API.

        :return Package verison.
        """
        if cls._version is None:
            cls._version = cls.env_package_version() or cls.PACKAGE_VERSION

        return cls._version

    @classmethod
    def lookup_asset_url(
        cls, key: str, *, _operational_mode: OperationalMode | None = None
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
    def package_url(cls, *, _operational_mode: OperationalMode | None = None) -> str:
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
    def package_development_directory(cls, *, _base_dir: str | None = None) -> str:
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
    def static_files_directory(cls, *, _base_dir: str | None = None) -> str:
        """Absolute path for the admin UI static files.

        :param _base_dir: For testing purposes. Not used in normal operation.
        :type _base_dir: str
        :returns: String containing absolute path to the admin UI package.
        :rtype: str
        """
        package_dir = cls.package_development_directory(_base_dir=_base_dir)
        return os.path.join(package_dir, cls.STATIC_ASSETS_REL_PATH)
