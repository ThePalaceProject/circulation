from __future__ import annotations

# ExternalIntegration, ExternalIntegrationLink, ConfigurationSetting
import json
import logging
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Index, Integer, Unicode
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import and_

from core.config import CannotLoadConfiguration, Configuration
from core.model import Base, get_one, get_one_or_create
from core.model.constants import DataSourceConstants
from core.model.hassessioncache import HasSessionCache
from core.model.hybrid import hybrid_property
from core.model.library import Library, externalintegrations_libraries
from core.util.string_helpers import random_string

if TYPE_CHECKING:
    # This is needed during type checking so we have the
    # types of related models.
    from core.model import Collection  # noqa: autoflake


class ExternalIntegration(Base):

    """An external integration contains configuration for connecting
    to a third-party API.
    """

    # Possible goals of ExternalIntegrations.
    #
    # These integrations are associated with external services which authenticate library administrators
    ADMIN_AUTH_GOAL = "admin_auth"

    # These integrations are associated with external services such as
    # SIP2 which authenticate library patrons. Other constants related
    # to this are defined in the circulation manager.
    PATRON_AUTH_GOAL = "patron_auth"

    # These integrations are associated with external services such as
    # the metadata wrangler, which provide information about books,
    # but not the books themselves.
    METADATA_GOAL = "metadata"

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
    # collect logs of server-side events.
    LOGGING_GOAL = "logging"

    # Supported protocols for ExternalIntegrations with LICENSE_GOAL.
    OPDS_IMPORT = "OPDS Import"
    OPDS2_IMPORT = "OPDS 2.0 Import"
    OVERDRIVE = DataSourceConstants.OVERDRIVE
    BIBLIOTHECA = DataSourceConstants.BIBLIOTHECA
    AXIS_360 = DataSourceConstants.AXIS_360
    OPDS_FOR_DISTRIBUTORS = "OPDS for Distributors"
    ENKI = DataSourceConstants.ENKI
    FEEDBOOKS = DataSourceConstants.FEEDBOOKS
    ODL = "ODL"
    ODL2 = "ODL 2.0"
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
        BIBLIOTHECA,
        AXIS_360,
        GUTENBERG,
        ENKI,
    ]

    # Some integrations with LICENSE_GOAL imply that the data and
    # licenses come from a specific data source.
    DATA_SOURCE_FOR_LICENSE_PROTOCOL = {
        OVERDRIVE: DataSourceConstants.OVERDRIVE,
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

    # Integrations with SEARCH_GOAL
    OPENSEARCH = "Opensearch"

    # Integrations with ANALYTICS_GOAL
    GOOGLE_ANALYTICS = "Google Analytics"

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

    # The JWE Patron Auth protocol
    PATRON_AUTH_JWE = "patron_auth_jwe"

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

    # Any additional configuration information goes into
    # ConfigurationSettings.
    settings: Mapped[list[ConfigurationSetting]] = relationship(
        "ConfigurationSetting",
        back_populates="external_integration",
        cascade="all, delete",
        uselist=True,
    )

    libraries: Mapped[list[Library]] = relationship(
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
    external_integration: ExternalIntegration = relationship(
        "ExternalIntegration", back_populates="settings"
    )

    library_id = Column(Integer, ForeignKey("libraries.id"), index=True)
    library: Mapped[Library] = relationship(
        "Library", back_populates="external_integration_settings"
    )
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


class ConfigurationAttributeValue(Enum):
    """Enumeration of common configuration attribute values"""

    YESVALUE = "yes"
    NOVALUE = "no"
