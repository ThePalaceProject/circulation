from __future__ import annotations

# ExternalIntegration, ExternalIntegrationLink, ConfigurationSetting
import inspect
import json
import logging
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable, Iterator, List, Optional, TypeVar

from flask_babel import lazy_gettext as _
from sqlalchemy import Column, DateTime
from sqlalchemy import Enum as saEnum
from sqlalchemy import ForeignKey, Index, Integer, Unicode
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import and_

from core.model.hybrid import hybrid_property

from ..config import CannotLoadConfiguration, Configuration
from ..mirror import MirrorUploader
from ..util.datetime_helpers import utc_now
from ..util.string_helpers import random_string
from . import Base, get_one, get_one_or_create
from .constants import DataSourceConstants
from .hassessioncache import HasSessionCache
from .library import Library, externalintegrations_libraries

if TYPE_CHECKING:
    # This is needed during type checking so we have the
    # types of related models.
    from core.configuration.ignored_identifier import (  # noqa: autoflake
        IgnoredIdentifierConfiguration,
    )
    from core.model import Collection  # noqa: autoflake


class ExternalIntegrationLink(Base, HasSessionCache):

    __tablename__ = "externalintegrationslinks"

    NO_MIRROR_INTEGRATION = "NO_MIRROR"
    # Possible purposes that a storage external integration can be used for.
    # These string literals may be stored in the database, so changes to them
    # may need to be accompanied by a DB migration.
    COVERS = "covers_mirror"
    COVERS_KEY = f"{COVERS}_integration_id"

    OPEN_ACCESS_BOOKS = "books_mirror"
    OPEN_ACCESS_BOOKS_KEY = f"{OPEN_ACCESS_BOOKS}_integration_id"

    PROTECTED_ACCESS_BOOKS = "protected_access_books_mirror"
    PROTECTED_ACCESS_BOOKS_KEY = f"{PROTECTED_ACCESS_BOOKS}_integration_id"

    ANALYTICS = "analytics_mirror"
    ANALYTICS_KEY = f"{ANALYTICS}_integration_id"

    MARC = "MARC_mirror"

    id = Column(Integer, primary_key=True)
    external_integration_id = Column(
        Integer, ForeignKey("externalintegrations.id"), index=True
    )
    library_id = Column(Integer, ForeignKey("libraries.id"), index=True)
    other_integration_id = Column(
        Integer, ForeignKey("externalintegrations.id"), index=True
    )
    purpose = Column(Unicode, index=True)

    mirror_settings = [
        {
            "key": COVERS_KEY,
            "type": COVERS,
            "description_type": "cover images",
            "label": "Covers Mirror",
        },
        {
            "key": OPEN_ACCESS_BOOKS_KEY,
            "type": OPEN_ACCESS_BOOKS,
            "description_type": "free books",
            "label": "Open Access Books Mirror",
        },
        {
            "key": PROTECTED_ACCESS_BOOKS_KEY,
            "type": PROTECTED_ACCESS_BOOKS,
            "description_type": "self-hosted, commercially licensed books",
            "label": "Protected Access Books Mirror",
        },
        {
            "key": ANALYTICS_KEY,
            "type": ANALYTICS,
            "description_type": "Analytics",
            "label": "Analytics Mirror",
        },
    ]
    settings = []

    for mirror_setting in mirror_settings:
        mirror_type = mirror_setting["type"]
        mirror_description_type = mirror_setting["description_type"]
        mirror_label = mirror_setting["label"]

        settings.append(
            {
                "key": f"{mirror_type.lower()}_integration_id",
                "label": _(mirror_label),
                "description": _(
                    "Any {} encountered while importing content from this collection "
                    "can be mirrored to a server you control.".format(
                        mirror_description_type
                    )
                ),
                "type": "select",
                "options": [
                    {
                        "key": NO_MIRROR_INTEGRATION,
                        "label": _(f"None - Do not mirror {mirror_description_type}"),
                    }
                ],
            }
        )

    COLLECTION_MIRROR_SETTINGS = settings


class ExternalIntegrationError(Base):
    __tablename__ = "externalintegrationerrors"

    id = Column(Integer, primary_key=True)
    time = Column(DateTime, default=utc_now)
    error = Column(Unicode)
    external_integration_id = Column(
        Integer,
        ForeignKey(
            "externalintegrations.id",
            name="fk_error_externalintegrations_id",
            ondelete="CASCADE",
        ),
    )


class ExternalIntegration(Base):

    """An external integration contains configuration for connecting
    to a third-party API.
    """

    GREEN = "green"
    RED = "red"

    STATUS = saEnum(GREEN, RED, name="external_integration_status")

    # Possible goals of ExternalIntegrations.
    #
    # These integrations are associated with external services which authenticate library administrators
    ADMIN_AUTH_GOAL = "admin_auth"

    # These integrations are associated with external services such as
    # SIP2 which authenticate library patrons. Other constants related
    # to this are defined in the circulation manager.
    PATRON_AUTH_GOAL = "patron_auth"

    # These integrations are associated with external services such
    # as Overdrive which provide access to books.
    LICENSE_GOAL = "licenses"

    # These integrations are associated with external services such as
    # the metadata wrangler, which provide information about books,
    # but not the books themselves.
    METADATA_GOAL = "metadata"

    # These integrations are associated with external services such as
    # S3 that provide access to book covers.
    STORAGE_GOAL = MirrorUploader.STORAGE_GOAL

    # These integrations are associated with external services such as
    # Opensearch that provide indexed search.
    SEARCH_GOAL = "search"

    # These integrations are associated with external services such as
    # Google Analytics, which receive analytics events.
    ANALYTICS_GOAL = "analytics"

    # These integrations are associated with external services such as
    # Adobe Vendor ID, which manage access to DRM-dependent content.
    DRM_GOAL = "drm"

    # These integrations are associated with external services that
    # help patrons find libraries.
    DISCOVERY_GOAL = "discovery"

    # These integrations are associated with external services that
    # collect logs of server-side events.
    LOGGING_GOAL = "logging"

    # These integrations are associated with external services that
    # a library uses to manage its catalog.
    CATALOG_GOAL = "ils_catalog"

    # Supported protocols for ExternalIntegrations with LICENSE_GOAL.
    OPDS_IMPORT = "OPDS Import"
    OPDS2_IMPORT = "OPDS 2.0 Import"
    OVERDRIVE = DataSourceConstants.OVERDRIVE
    ODILO = DataSourceConstants.ODILO
    BIBLIOTHECA = DataSourceConstants.BIBLIOTHECA
    AXIS_360 = DataSourceConstants.AXIS_360
    OPDS_FOR_DISTRIBUTORS = "OPDS for Distributors"
    ENKI = DataSourceConstants.ENKI
    FEEDBOOKS = DataSourceConstants.FEEDBOOKS
    ODL = "ODL"
    ODL2 = "ODL 2.0"
    LCP = DataSourceConstants.LCP
    MANUAL = DataSourceConstants.MANUAL
    PROQUEST = DataSourceConstants.PROQUEST

    # These protocols were used on the Content Server when mirroring
    # content from a given directory or directly from Project
    # Gutenberg, respectively. DIRECTORY_IMPORT was replaced by
    # MANUAL.  GUTENBERG has yet to be replaced, but will eventually
    # be moved into LICENSE_PROTOCOLS.
    DIRECTORY_IMPORT = "Directory Import"
    GUTENBERG = DataSourceConstants.GUTENBERG

    LICENSE_PROTOCOLS = [
        OPDS_IMPORT,
        OVERDRIVE,
        ODILO,
        BIBLIOTHECA,
        AXIS_360,
        GUTENBERG,
        ENKI,
        MANUAL,
    ]

    # Some integrations with LICENSE_GOAL imply that the data and
    # licenses come from a specific data source.
    DATA_SOURCE_FOR_LICENSE_PROTOCOL = {
        OVERDRIVE: DataSourceConstants.OVERDRIVE,
        ODILO: DataSourceConstants.ODILO,
        BIBLIOTHECA: DataSourceConstants.BIBLIOTHECA,
        AXIS_360: DataSourceConstants.AXIS_360,
        ENKI: DataSourceConstants.ENKI,
        FEEDBOOKS: DataSourceConstants.FEEDBOOKS,
    }

    # Integrations with METADATA_GOAL
    BIBBLIO = "Bibblio"
    CONTENT_CAFE = "Content Cafe"
    NOVELIST = "NoveList Select"
    NYPL_SHADOWCAT = "Shadowcat"
    NYT = "New York Times"
    CONTENT_SERVER = "Content Server"

    # Integrations with STORAGE_GOAL
    S3 = "Amazon S3"
    MINIO = "MinIO"
    LCP = "LCP"

    # Integrations with SEARCH_GOAL
    OPENSEARCH = "Opensearch"

    # Integrations with DRM_GOAL
    ADOBE_VENDOR_ID = "Adobe Vendor ID"

    # Integrations with DISCOVERY_GOAL
    OPDS_REGISTRATION = "OPDS Registration"

    # Integrations with ANALYTICS_GOAL
    GOOGLE_ANALYTICS = "Google Analytics"

    # Integrations with LOGGING_GOAL
    INTERNAL_LOGGING = "Internal logging"
    CLOUDWATCH = "AWS Cloudwatch Logs"

    # Integrations with CATALOG_GOAL
    MARC_EXPORT = "MARC Export"

    # Keys for common configuration settings

    # If there is a special URL to use for access to this API,
    # put it here.
    URL = "url"

    # If access requires authentication, these settings represent the
    # username/password or key/secret combination necessary to
    # authenticate. If there's a secret but no key, it's stored in
    # 'password'.
    USERNAME = "username"
    PASSWORD = "password"

    # If the request should use a custom headers, put it here.
    CUSTOM_ACCEPT_HEADER = "custom_accept_header"

    # If want to use an identifier different from <id>, use this config.
    PRIMARY_IDENTIFIER_SOURCE = "primary_identifier_source"
    DCTERMS_IDENTIFIER = "first_dcterms_identifier"

    # If the library-collection pair should display books with holds when no loans are available
    DISPLAY_RESERVES = "dont_display_reserves"

    # The token auth for an opds/opds2 feed
    TOKEN_AUTH = "token_auth_endpoint"

    __tablename__ = "externalintegrations"
    id = Column(Integer, primary_key=True)

    # Each integration should have a protocol (explaining what type of
    # code or network traffic we need to run to get things done) and a
    # goal (explaining the real-world goal of the integration).
    #
    # Basically, the protocol is the 'how' and the goal is the 'why'.
    protocol = Column(Unicode, nullable=False)
    goal = Column(Unicode, nullable=True)

    # A unique name for this ExternalIntegration. This is primarily
    # used to identify ExternalIntegrations from command-line scripts.
    name = Column(Unicode, nullable=True, unique=True)

    status: Mapped[str] = Column(STATUS, server_default=str(GREEN))
    last_status_update = Column(DateTime, nullable=True)

    # Any additional configuration information goes into
    # ConfigurationSettings.
    settings: Mapped[List[ConfigurationSetting]] = relationship(
        "ConfigurationSetting",
        backref="external_integration",
        cascade="all, delete",
        uselist=True,
    )

    # Any number of Collections may designate an ExternalIntegration
    # as the source of their configuration
    collections: Mapped[List[Collection]] = relationship(
        "Collection",
        backref="_external_integration",
        foreign_keys="Collection.external_integration_id",
    )

    links: Mapped[List[ExternalIntegrationLink]] = relationship(
        "ExternalIntegrationLink",
        backref="integration",
        foreign_keys="ExternalIntegrationLink.external_integration_id",
        cascade="all, delete-orphan",
    )

    other_links: Mapped[List[ExternalIntegrationLink]] = relationship(
        "ExternalIntegrationLink",
        backref="other_integration",
        foreign_keys="ExternalIntegrationLink.other_integration_id",
        cascade="all, delete-orphan",
    )

    libraries: Mapped[List[Library]] = relationship(
        "Library",
        back_populates="integrations",
        secondary=lambda: externalintegrations_libraries,
        uselist=True,
    )

    def __repr__(self):
        return "<ExternalIntegration: protocol=%s goal='%s' settings=%d ID=%d>" % (
            self.protocol,
            self.goal,
            len(self.settings),
            self.id,
        )

    @classmethod
    def for_goal(cls, _db, goal):
        """Return all external integrations by goal type."""
        integrations = _db.query(cls).filter(cls.goal == goal).order_by(cls.name)

        return integrations

    @classmethod
    def for_collection_and_purpose(cls, _db, collection, purpose):
        """Find the ExternalIntegration for the collection.

        :param collection: Use the mirror configuration for this Collection.
        :param purpose: Use the purpose of the mirror configuration.
        """
        qu = (
            _db.query(cls)
            .join(
                ExternalIntegrationLink,
                ExternalIntegrationLink.other_integration_id == cls.id,
            )
            .filter(
                ExternalIntegrationLink.external_integration_id
                == collection.external_integration_id,
                ExternalIntegrationLink.purpose == purpose,
            )
        )
        integrations = qu.all()
        if not integrations:
            raise CannotLoadConfiguration(
                "No storage integration for collection '%s' and purpose '%s' is configured."
                % (collection.name, purpose)
            )
        if len(integrations) > 1:
            raise CannotLoadConfiguration(
                "Multiple integrations found for collection '%s' and purpose '%s'"
                % (collection.name, purpose)
            )

        [integration] = integrations
        return integration

    @classmethod
    def lookup(cls, _db, protocol, goal, library=None):

        integrations = _db.query(cls).filter(cls.protocol == protocol, cls.goal == goal)

        if library:
            integrations = integrations.join(cls.libraries).filter(
                Library.id == library.id
            )

        integrations = integrations.all()
        if len(integrations) > 1:
            logging.warning(f"Multiple integrations found for '{protocol}'/'{goal}'")

        if [i for i in integrations if i.libraries] and not library:
            raise ValueError(
                "This ExternalIntegration requires a library and none was provided."
            )

        if not integrations:
            return None
        return integrations[0]

    @classmethod
    def with_setting_value(cls, _db, protocol, goal, key, value):
        """Find ExternalIntegrations with the given protocol, goal, and with a
        particular ConfigurationSetting key/value pair.
        This is useful in a scenario where an ExternalIntegration is
        made unique by a ConfigurationSetting, such as
        ExternalIntegration.URL, rather than by anything in the
        ExternalIntecation itself.

        :param protocol: ExternalIntegrations must have this protocol.
        :param goal: ExternalIntegrations must have this goal.
        :param key: Look only at ExternalIntegrations with
            a ConfigurationSetting for this key.
        :param value: Find ExternalIntegrations whose ConfigurationSetting
            has this value.
        :return: A Query object.
        """
        return (
            _db.query(ExternalIntegration)
            .join(ExternalIntegration.settings)
            .filter(ExternalIntegration.goal == goal)
            .filter(ExternalIntegration.protocol == protocol)
            .filter(ConfigurationSetting.key == key)
            .filter(ConfigurationSetting.value == value)
        )

    @classmethod
    def admin_authentication(cls, _db):
        admin_auth = get_one(_db, cls, goal=cls.ADMIN_AUTH_GOAL)
        return admin_auth

    @classmethod
    def for_library_and_goal(cls, _db, library, goal):
        """Find all ExternalIntegrations associated with the given
        Library and the given goal.
        :return: A Query.
        """
        return (
            _db.query(ExternalIntegration)
            .join(ExternalIntegration.libraries)
            .filter(ExternalIntegration.goal == goal)
            .filter(Library.id == library.id)
        )

    @classmethod
    def one_for_library_and_goal(cls, _db, library, goal):
        """Find the ExternalIntegration associated with the given
        Library and the given goal.
        :return: An ExternalIntegration, or None.
        :raise: CannotLoadConfiguration
        """
        integrations = cls.for_library_and_goal(_db, library, goal).all()
        if len(integrations) == 0:
            return None
        if len(integrations) > 1:
            raise CannotLoadConfiguration(
                "Library %s defines multiple integrations with goal %s!"
                % (library.name, goal)
            )
        return integrations[0]

    def set_setting(self, key, value):
        """Create or update a key-value setting for this ExternalIntegration."""
        setting = self.setting(key)
        setting.value = value
        return setting

    def setting(self, key):
        """Find or create a ConfigurationSetting on this ExternalIntegration.
        :param key: Name of the setting.
        :return: A ConfigurationSetting
        """
        return ConfigurationSetting.for_externalintegration(key, self)

    @hybrid_property
    def url(self):
        return self.setting(self.URL).value

    @url.setter
    def url(self, new_url):
        self.set_setting(self.URL, new_url)

    @hybrid_property
    def username(self):
        return self.setting(self.USERNAME).value

    @username.setter
    def username(self, new_username):
        self.set_setting(self.USERNAME, new_username)

    @hybrid_property
    def password(self):
        return self.setting(self.PASSWORD).value

    @password.setter
    def password(self, new_password):
        return self.set_setting(self.PASSWORD, new_password)

    @hybrid_property
    def custom_accept_header(self):
        return self.setting(self.CUSTOM_ACCEPT_HEADER).value

    @custom_accept_header.setter
    def custom_accept_header(self, new_custom_accept_header):
        return self.set_setting(self.CUSTOM_ACCEPT_HEADER, new_custom_accept_header)

    @hybrid_property
    def primary_identifier_source(self):
        return self.setting(self.PRIMARY_IDENTIFIER_SOURCE).value

    @primary_identifier_source.setter
    def primary_identifier_source(self, new_primary_identifier_source):
        return self.set_setting(
            self.PRIMARY_IDENTIFIER_SOURCE, new_primary_identifier_source
        )

    def explain(self, library=None, include_secrets=False):
        """Create a series of human-readable strings to explain an
        ExternalIntegration's settings.

        :param library: Include additional settings imposed upon this
            ExternalIntegration by the given Library.
        :param include_secrets: For security reasons,
            sensitive settings such as passwords are not displayed by default.
        :return: A list of explanatory strings.
        """
        lines = []
        lines.append("ID: %s" % self.id)
        if self.name:
            lines.append("Name: %s" % self.name)
        lines.append(f"Protocol/Goal: {self.protocol}/{self.goal}")

        def key(setting):
            if setting.library:
                return setting.key, setting.library.name
            return (setting.key, None)

        for setting in sorted(self.settings, key=key):
            if library and setting.library and setting.library != library:
                # This is a different library's specialization of
                # this integration. Ignore it.
                continue
            if setting.value is None:
                # The setting has no value. Ignore it.
                continue
            explanation = f"{setting.key}='{setting.value}'"
            if setting.library:
                explanation = "{} (applies only to {})".format(
                    explanation,
                    setting.library.name,
                )
            if include_secrets or not setting.is_secret:
                lines.append(explanation)
        return lines


class ConfigurationSetting(Base, HasSessionCache):
    """An extra piece of site configuration.
    A ConfigurationSetting may be associated with an
    ExternalIntegration, a Library, both, or neither.
    * The secret used by the circulation manager to sign OAuth bearer
    tokens is not associated with an ExternalIntegration or with a
    Library.
    * The link to a library's privacy policy is associated with the
    Library, but not with any particular ExternalIntegration.
    * The "website ID" for an Overdrive collection is associated with
    an ExternalIntegration (the Overdrive integration), but not with
    any particular Library (since multiple libraries might share an
    Overdrive collection).
    * The "identifier prefix" used to determine which library a patron
    is a patron of, is associated with both a Library and an
    ExternalIntegration.
    """

    __tablename__ = "configurationsettings"
    id = Column(Integer, primary_key=True)
    external_integration_id = Column(
        Integer, ForeignKey("externalintegrations.id"), index=True
    )
    library_id = Column(Integer, ForeignKey("libraries.id"), index=True)
    key = Column(Unicode)
    _value = Column("value", Unicode)

    __table_args__ = (
        # Unique indexes to prevent the creation of redundant
        # configuration settings.
        # If both external_integration_id and library_id are null,
        # then the key--the name of a sitewide setting--must be unique.
        Index(
            "ix_configurationsettings_key",
            key,
            unique=True,
            postgresql_where=and_(external_integration_id == None, library_id == None),
        ),
        # If external_integration_id is null but library_id is not,
        # then (library_id, key) must be unique.
        Index(
            "ix_configurationsettings_library_id_key",
            library_id,
            key,
            unique=True,
            postgresql_where=(external_integration_id == None),
        ),
        # If library_id is null but external_integration_id is not,
        # then (external_integration_id, key) must be unique.
        Index(
            "ix_configurationsettings_external_integration_id_key",
            external_integration_id,
            key,
            unique=True,
            postgresql_where=library_id == None,
        ),
        # If both external_integration_id and library_id have values,
        # then (external_integration_id, library_id, key) must be
        # unique.
        Index(
            "ix_configurationsettings_external_integration_id_library_id_key",
            external_integration_id,
            library_id,
            key,
            unique=True,
        ),
    )

    def __repr__(self):
        return "<ConfigurationSetting: key=%s, ID=%d>" % (self.key, self.id)

    @classmethod
    def sitewide_secret(cls, _db, key):
        """Find or create a sitewide shared secret.
        The value of this setting doesn't matter, only that it's
        unique across the site and that it's always available.
        """
        secret = ConfigurationSetting.sitewide(_db, key)
        if not secret.value:
            secret.value = random_string(24)
            # Commit to get this in the database ASAP.
            _db.commit()
        return secret.value

    @classmethod
    def explain(cls, _db, include_secrets=False):
        """Explain all site-wide ConfigurationSettings."""
        lines = []
        site_wide_settings = []

        for setting in (
            _db.query(ConfigurationSetting)
            .filter(ConfigurationSetting.library == None)
            .filter(ConfigurationSetting.external_integration == None)
        ):
            if not include_secrets and setting.key.endswith("_secret"):
                continue
            site_wide_settings.append(setting)
        if site_wide_settings:
            lines.append("Site-wide configuration settings:")
            lines.append("---------------------------------")
        for setting in sorted(site_wide_settings, key=lambda s: s.key):
            if setting.value is None:
                continue
            lines.append(f"{setting.key}='{setting.value}'")
        return lines

    @classmethod
    def sitewide(cls, _db, key):
        """Find or create a sitewide ConfigurationSetting."""
        return cls.for_library_and_externalintegration(_db, key, None, None)

    @classmethod
    def for_library(cls, key, library):
        """Find or create a ConfigurationSetting for the given Library."""
        _db = Session.object_session(library)
        return cls.for_library_and_externalintegration(_db, key, library, None)

    @classmethod
    def for_externalintegration(cls, key, externalintegration):
        """Find or create a ConfigurationSetting for the given
        ExternalIntegration.
        """
        _db = Session.object_session(externalintegration)
        return cls.for_library_and_externalintegration(
            _db, key, None, externalintegration
        )

    @classmethod
    def _cache_key(cls, library, external_integration, key):
        if library:
            library_id = library.id
        else:
            library_id = None
        if external_integration:
            external_integration_id = external_integration.id
        else:
            external_integration_id = None
        return (library_id, external_integration_id, key)

    def cache_key(self):
        return self._cache_key(self.library, self.external_integration, self.key)

    @classmethod
    def for_library_and_externalintegration(
        cls, _db, key, library, external_integration
    ):
        """Find or create a ConfigurationSetting associated with a Library
        and an ExternalIntegration.
        """

        def create():
            """Function called when a ConfigurationSetting is not found in cache
            and must be created.
            """
            return get_one_or_create(
                _db,
                ConfigurationSetting,
                library=library,
                external_integration=external_integration,
                key=key,
            )

        # ConfigurationSettings are stored in cache based on their library,
        # external integration, and the name of the setting.
        cache_key = cls._cache_key(library, external_integration, key)
        setting, ignore = cls.by_cache_key(_db, cache_key, create)
        return setting

    @hybrid_property
    def value(self):

        """What's the current value of this configuration setting?
        If not present, the value may be inherited from some other
        ConfigurationSetting.
        """
        if self._value:
            # An explicitly set value always takes precedence.
            return self._value
        elif self.library and self.external_integration:
            # This is a library-specific specialization of an
            # ExternalIntegration. Treat the value set on the
            # ExternalIntegration as a default.
            return self.for_externalintegration(
                self.key, self.external_integration
            ).value
        elif self.library:
            # This is a library-specific setting. Treat the site-wide
            # value as a default.
            _db = Session.object_session(self)
            return self.sitewide(_db, self.key).value
        return self._value

    @value.setter
    def value(self, new_value):
        if isinstance(new_value, bytes):
            new_value = new_value.decode("utf8")
        elif new_value is not None:
            new_value = str(new_value)
        self._value = new_value

    @classmethod
    def _is_secret(self, key):
        """Should the value of the given key be treated as secret?
        This will have to do, in the absence of programmatic ways of
        saying that a specific setting should be treated as secret.
        """
        return any(
            key == x
            or key.startswith("%s_" % x)
            or key.endswith("_%s" % x)
            or ("_%s_" % x) in key
            for x in ("secret", "password")
        )

    @property
    def is_secret(self):
        """Should the value of this key be treated as secret?"""
        return self._is_secret(self.key)

    def value_or_default(self, default):
        """Return the value of this setting. If the value is None,
        set it to `default` and return that instead.
        """
        if self.value is None:
            self.value = default
        return self.value

    MEANS_YES = {"true", "t", "yes", "y"}

    @property
    def bool_value(self) -> bool | None:
        """Turn the value into a boolean if possible.
        :return: A boolean, or None if there is no value.
        """
        if self.value:
            if self.value.lower() in self.MEANS_YES:
                return True
            return False
        return None

    @property
    def int_value(self) -> int | None:
        """Turn the value into an int if possible.
        :return: An integer, or None if there is no value.
        :raise ValueError: If the value cannot be converted to an int.
        """
        if self.value:
            return int(self.value)
        return None

    @property
    def float_value(self) -> float | None:
        """Turn the value into an float if possible.
        :return: A float, or None if there is no value.
        :raise ValueError: If the value cannot be converted to a float.
        """
        if self.value:
            return float(self.value)
        return None

    @property
    def json_value(self):
        """Interpret the value as JSON if possible.
        :return: An object, or None if there is no value.
        :raise ValueError: If the value cannot be parsed as JSON.
        """
        if self.value:
            return json.loads(self.value)
        return None

    # As of this release of the software, this is our best guess as to
    # which data sources should have their audiobooks excluded from
    # lanes.
    EXCLUDED_AUDIO_DATA_SOURCES_DEFAULT: list[str] = []

    @classmethod
    def excluded_audio_data_sources(cls, _db):
        """List the data sources whose audiobooks should not be published in
        feeds, either because this server can't fulfill them or the
        expected client can't play them.
        Most methods like this go into Configuration, but this one needs
        to reference data model objects for its default value.
        """
        value = cls.sitewide(_db, Configuration.EXCLUDED_AUDIO_DATA_SOURCES).json_value
        if value is None:
            value = cls.EXCLUDED_AUDIO_DATA_SOURCES_DEFAULT
        return value


class HasExternalIntegration(metaclass=ABCMeta):
    """Interface allowing to get access to an external integration"""

    @abstractmethod
    def external_integration(self, db: Session) -> Optional[ExternalIntegration]:
        """Returns an external integration associated with this object

        :param db: Database session

        :return: External integration associated with this object
        """
        raise NotImplementedError()


class BaseConfigurationStorage(metaclass=ABCMeta):
    """Serializes and deserializes values as configuration settings"""

    @abstractmethod
    def save(self, db: Session, setting_name: str, value: Any):
        """Save the value as as a new configuration setting

        :param db: Database session
        :param setting_name: Name of the configuration setting
        :param value: Value to be saved
        """
        raise NotImplementedError()

    @abstractmethod
    def load(self, db: Session, setting_name: str) -> Any:
        """Loads and returns the library's configuration setting

        :param db: Database session
        :param setting_name: Name of the configuration setting
        """
        raise NotImplementedError()


class ConfigurationStorage(BaseConfigurationStorage):
    """Serializes and deserializes values as configuration settings"""

    def __init__(self, integration_association: HasExternalIntegration):
        """Initializes a new instance of ConfigurationStorage class

        :param integration_association: Association with an external integration
        """
        self._integration_association = integration_association

    def save(self, db: Session, setting_name: str, value: Any):
        """Save the value as as a new configuration setting

        :param db: Database session
        :param setting_name: Name of the configuration setting
        :param value: Value to be saved
        """
        integration = self._integration_association.external_integration(db)
        ConfigurationSetting.for_externalintegration(
            setting_name, integration
        ).value = value

    def load(self, db: Session, setting_name: str) -> Any:
        """Loads and returns the library's configuration setting

        :param db: Database session
        :param setting_name: Name of the library's configuration setting
        """
        integration = self._integration_association.external_integration(db)
        value = ConfigurationSetting.for_externalintegration(
            setting_name, integration
        ).value

        return value


class ConfigurationAttributeType(Enum):
    """Enumeration of configuration setting types"""

    TEXT = "text"
    TEXTAREA = "textarea"
    SELECT = "select"
    NUMBER = "number"
    LIST = "list"
    MENU = "menu"

    def to_control_type(self) -> str | None:
        """Converts the value to a attribute type understandable by circulation-admin

        :return: String representation of attribute's type
        """
        # NOTE: For some reason, circulation-admin converts "text" into <text> so we have to turn it into None
        # In this case circulation-admin will use <input>
        # TODO: To be fixed in https://jira.nypl.org/browse/SIMPLY-3008
        if self.value == ConfigurationAttributeType.TEXT.value:
            return None
        else:
            return self.value


class ConfigurationAttribute(Enum):
    """Enumeration of configuration setting attributes"""

    KEY = "key"
    LABEL = "label"
    DESCRIPTION = "description"
    TYPE = "type"
    REQUIRED = "required"
    DEFAULT = "default"
    OPTIONS = "options"
    CATEGORY = "category"
    FORMAT = "format"


class ConfigurationAttributeValue(Enum):
    """Enumeration of common configuration attribute values"""

    YESVALUE = "yes"
    NOVALUE = "no"


class ConfigurationOption:
    """Key-value pair containing information about configuration attribute option"""

    def __init__(self, key: str, label: str) -> None:
        """Initializes a new instance of ConfigurationOption class

        :param key: Key
        :param label: Label
        """
        self._key = key
        self._label = label

    def __eq__(self, other: object) -> bool:
        """Compares two ConfigurationOption objects

        :param other: ConfigurationOption object

        :return: Boolean value indicating whether two items are equal
        """
        if not isinstance(other, ConfigurationOption):
            return False

        return self.key == other.key and self.label == other.label

    @property
    def key(self) -> str:
        """Returns option's key

        :return: Option's key
        """
        return self._key

    @property
    def label(self) -> str:
        """Returns option's label

        :return: Option's label
        """
        return self._label

    def to_settings(self) -> dict[str, str]:
        """Returns a dictionary containing option metadata in the SETTINGS format

        :return: Dictionary containing option metadata in the SETTINGS format
        """
        return {"key": self.key, "label": self.label}

    @staticmethod
    def from_enum(cls: type[Enum]) -> list[ConfigurationOption]:
        """Convers Enum to a list of options in the SETTINGS format

        :param cls: Enum type

        :return: List of options in the SETTINGS format
        """
        if not issubclass(cls, Enum):
            raise ValueError("Class should be descendant of Enum")

        return [ConfigurationOption(element.value, element.name) for element in cls]


class HasConfigurationSettings(metaclass=ABCMeta):
    """Interface representing class containing ConfigurationMetadata properties"""

    @abstractmethod
    def get_setting_value(self, setting_name: str) -> Any:
        """Returns a settings'value

        :param setting_name: Name of the setting

        :return: Setting's value
        """
        raise NotImplementedError()

    @abstractmethod
    def set_setting_value(self, setting_name: str, setting_value: Any):
        """Sets setting's value

        :param setting_name: Name of the setting

        :param setting_value: New value of the setting
        """
        raise NotImplementedError()


class ConfigurationMetadata:
    """Contains configuration metadata"""

    _counter = 0

    def __init__(
        self,
        key: str,
        label: str,
        description: str,
        type: ConfigurationAttributeType,
        required: bool = False,
        default: Any | None = None,
        options: list[ConfigurationOption] | None = None,
        category: str | None = None,
        format=None,
        index=None,
    ):
        """Initializes a new instance of ConfigurationMetadata class

        :param key: Setting's key
        :param label: Setting's label
        :param description: Setting's description
        :param type: Setting's type
        :param required: Boolean value indicating whether the setting is required or not
        :param default: Setting's default value
        :param options: Setting's options (used in the case of select)
        :param category: Setting's category
        """
        self._key = key
        self._label = label
        self._description = description
        self._type = type
        self._required = required
        self._default = default
        self._options = options
        self._category = category
        self._format = format

        if index is not None:
            self._index = index
        else:
            ConfigurationMetadata._counter += 1
            self._index = ConfigurationMetadata._counter

    def __get__(
        self,
        owner_instance: HasConfigurationSettings
        | IgnoredIdentifierConfiguration
        | None,
        owner_type: type | None,
    ) -> Any:
        """Returns a value of the setting

        :param owner_instance: Instance of the owner, class having instance of ConfigurationMetadata as an attribute
        :param owner_type: Owner's class

        :return: ConfigurationMetadata instance (when called via a static method) or
            the setting's value (when called via an instance method)
        """
        # If owner_instance is empty, it means that this method was called
        # via a static method of ConfigurationMetadataOwner (for example, ConfigurationBucket.to_settings).
        # In this case we need to return the metadata instance itself
        if owner_instance is None:
            return self

        if not isinstance(owner_instance, HasConfigurationSettings):
            raise Exception(
                "owner must be an instance of HasConfigurationSettings type"
            )

        setting_value = owner_instance.get_setting_value(self._key)

        if setting_value is None:
            setting_value = self.default
        elif self.type == ConfigurationAttributeType.NUMBER:
            try:
                setting_value = float(setting_value)
            except ValueError:
                if setting_value != "":
                    # A non-empty value is a "bad" value, and should raise an exception
                    raise CannotLoadConfiguration(
                        f"Could not covert {self.label}'s value '{setting_value}'."
                    )
                setting_value = self.default
        else:
            # LIST and MENU configuration settings are stored as JSON-serialized lists in the database.
            # We need to deserialize them to get actual values.
            if self.type in (
                ConfigurationAttributeType.LIST,
                ConfigurationAttributeType.MENU,
            ):
                if isinstance(setting_value, str):
                    setting_value = json.loads(setting_value)
                else:
                    # We assume that LIST and MENU values can be either JSON or empty.
                    if setting_value is not None:
                        raise ValueError(
                            f"{self._type} configuration setting '{self._key}' has an incorrect format. "
                            f"Expected JSON-serialized list but got {setting_value}."
                        )

                    setting_value = []

        return setting_value

    def __set__(
        self,
        owner_instance: HasConfigurationSettings
        | IgnoredIdentifierConfiguration
        | None,
        value: Any,
    ) -> Any:
        """Updates the setting's value

        :param owner_instance: Instance of the owner, class having instance of ConfigurationMetadata as an attribute

        :param value: New setting's value
        """
        if not isinstance(owner_instance, HasConfigurationSettings):
            raise Exception(
                "owner must be an instance of HasConfigurationSettings type"
            )

        return owner_instance.set_setting_value(self._key, value)

    @property
    def key(self) -> str:
        """Returns the setting's key

        :return: Setting's key
        """
        return self._key

    @property
    def label(self) -> str:
        """Returns the setting's label

        :return: Setting's label
        """
        return self._label

    @property
    def description(self) -> str:
        """Returns the setting's description

        :return: Setting's description
        """
        return self._description

    @property
    def type(self) -> ConfigurationAttributeType:
        """Returns the setting's type

        :return: Setting's type
        """
        return self._type

    @property
    def required(self) -> bool:
        """Returns the boolean value indicating whether the setting is required or not

        :return: Boolean value indicating whether the setting is required or not
        """
        return self._required

    @property
    def default(self) -> Any | None:
        """Returns the setting's default value

        :return: Setting's default value
        """
        return self._default

    @property
    def options(self) -> list[ConfigurationOption] | None:
        """Returns the setting's options (used in the case of select)

        :return: Setting's options (used in the case of select)
        """
        return self._options

    @property
    def category(self) -> str | None:
        """Returns the setting's category

        :return: Setting's category
        """
        return self._category

    @property
    def format(self) -> str:
        """Returns the setting's format

        :return: Setting's format
        """
        return self._format

    @property
    def index(self):
        return self._index

    @staticmethod
    def get_configuration_metadata(cls) -> list[tuple[str, ConfigurationMetadata]]:
        """Returns a list of 2-tuples containing information ConfigurationMetadata properties in the specified class

        :param cls: Class
        :return: List of 2-tuples containing information ConfigurationMetadata properties in the specified class
        """
        members = inspect.getmembers(cls)
        configuration_metadata = []

        for name, member in members:
            if isinstance(member, ConfigurationMetadata):
                configuration_metadata.append((name, member))

        configuration_metadata.sort(key=lambda pair: pair[1].index)

        return configuration_metadata

    def to_settings(self):
        return {
            ConfigurationAttribute.KEY.value: self.key,
            ConfigurationAttribute.LABEL.value: self.label,
            ConfigurationAttribute.DESCRIPTION.value: self.description,
            ConfigurationAttribute.TYPE.value: self.type.to_control_type(),
            ConfigurationAttribute.REQUIRED.value: self.required,
            ConfigurationAttribute.DEFAULT.value: self.default,
            ConfigurationAttribute.OPTIONS.value: [
                option.to_settings() for option in self.options
            ]
            if self.options
            else None,
            ConfigurationAttribute.CATEGORY.value: self.category,
            ConfigurationAttribute.FORMAT.value: self.format,
        }

    @staticmethod
    def to_bool(metadata: ConfigurationMetadata) -> bool:
        """Return a boolean scalar indicating whether the configuration setting
            contains a value that can be treated as True (see ConfigurationSetting.MEANS_YES).

        :param metadata: ConfigurationMetadata object
        :return: Boolean scalar indicating
            whether this configuration setting contains a value that can be treated as True
        """
        return str(metadata).lower() in ConfigurationSetting.MEANS_YES


class ConfigurationGrouping(HasConfigurationSettings):
    """Base class for all classes containing configuration settings

    NOTE: Be aware that it's valid only while a database session is valid and must not be stored between requests
    """

    def __init__(
        self, configuration_storage: BaseConfigurationStorage, db: Session
    ) -> None:
        """Initializes a new instance of ConfigurationGrouping

        :param configuration_storage: ConfigurationStorage object
        :param db: Database session
        """
        self._logger = logging.getLogger()
        self._configuration_storage = configuration_storage
        self._db = db

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db = None

    def get_setting_value(self, setting_name: str) -> Any:
        """Returns a settings'value

        :param setting_name: Name of the setting
        :return: Setting's value
        """
        return self._configuration_storage.load(self._db, setting_name)

    def set_setting_value(self, setting_name: str, setting_value: Any) -> Any:
        """Sets setting's value

        :param setting_name: Name of the setting
        :param setting_value: New value of the setting
        """
        self._configuration_storage.save(self._db, setting_name, setting_value)

    @classmethod
    def to_settings_generator(cls) -> Iterable[dict]:
        """Return a generator object returning settings in a format understandable by circulation-admin.

        :return: list of settings in a format understandable by circulation-admin.
        """
        for name, member in ConfigurationMetadata.get_configuration_metadata(cls):
            key_attribute = getattr(member, ConfigurationAttribute.KEY.value, None)
            label_attribute = getattr(member, ConfigurationAttribute.LABEL.value, None)
            description_attribute = getattr(
                member, ConfigurationAttribute.DESCRIPTION.value, None
            )
            type_attribute = getattr(member, ConfigurationAttribute.TYPE.value, None)
            control_type = (
                type_attribute.to_control_type() if type_attribute is not None else None
            )
            required_attribute = getattr(
                member, ConfigurationAttribute.REQUIRED.value, None
            )
            default_attribute = getattr(
                member, ConfigurationAttribute.DEFAULT.value, None
            )
            options_attribute = getattr(
                member, ConfigurationAttribute.OPTIONS.value, None
            )
            category_attribute = getattr(
                member, ConfigurationAttribute.CATEGORY.value, None
            )

            yield {
                ConfigurationAttribute.KEY.value: key_attribute,
                ConfigurationAttribute.LABEL.value: label_attribute,
                ConfigurationAttribute.DESCRIPTION.value: description_attribute,
                ConfigurationAttribute.TYPE.value: control_type,
                ConfigurationAttribute.REQUIRED.value: required_attribute,
                ConfigurationAttribute.DEFAULT.value: default_attribute,
                ConfigurationAttribute.OPTIONS.value: [
                    option.to_settings() for option in options_attribute
                ]
                if options_attribute
                else None,
                ConfigurationAttribute.CATEGORY.value: category_attribute,
            }

    @classmethod
    def to_settings(cls) -> list[dict[str, Any]]:
        """Return a list of settings in a format understandable by circulation-admin.

        :return: list of settings in a format understandable by circulation-admin.
        """
        return list(cls.to_settings_generator())


C = TypeVar("C", bound="ConfigurationGrouping")


class ConfigurationFactory:
    """Factory creating new instances of ConfigurationGrouping class descendants."""

    @contextmanager
    def create(
        self,
        configuration_storage: ConfigurationStorage,
        db: Session,
        configuration_grouping_class: type[C],
    ) -> Iterator[C]:
        """Create a new instance of ConfigurationGrouping.

        :param configuration_storage: ConfigurationStorage object
        :param db: Database session
        :param configuration_grouping_class: Configuration bucket's class
        :return: ConfigurationGrouping instance
        """
        with configuration_grouping_class(
            configuration_storage, db
        ) as configuration_bucket:
            yield configuration_bucket
