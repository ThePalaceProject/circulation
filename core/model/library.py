# Library
from __future__ import annotations

import logging
from collections import Counter
from typing import TYPE_CHECKING, List, Tuple

from Crypto.PublicKey import RSA
from expiringdict import ExpiringDict
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    LargeBinary,
    Table,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import func

from core.model.hybrid import hybrid_property

from ..config import Configuration
from ..entrypoint import EntryPoint
from ..facets import FacetConstants
from . import Base, get_one
from .announcements import Announcement
from .customlist import customlist_sharedlibrary
from .edition import Edition
from .hassessioncache import HasSessionCache
from .licensing import LicensePool
from .work import Work

if TYPE_CHECKING:
    from core.lane import Lane
    from core.model import (  # noqa: autoflake
        AdminRole,
        CachedFeed,
        CachedMARCFile,
        CirculationEvent,
        Collection,
        ConfigurationSetting,
        CustomList,
        ExternalIntegration,
        Patron,
    )


class Library(Base, HasSessionCache):
    """A library that uses this circulation manager to authenticate
    its patrons and manage access to its content.
    A circulation manager may serve many libraries.
    """

    __tablename__ = "libraries"

    id = Column(Integer, primary_key=True)

    # The human-readable name of this library. Used in the library's
    # Authentication for OPDS document.
    name = Column(Unicode, unique=True)

    # A short name of this library, to use when identifying it in
    # scripts. e.g. "NYPL" for NYPL.
    short_name = Column(Unicode, unique=True, nullable=False)

    # A UUID that uniquely identifies the library among all libraries
    # in the world. This is used to serve the library's Authentication
    # for OPDS document, and it also goes to the library registry.
    uuid = Column(Unicode, unique=True)

    # One, and only one, library may be the default. The default
    # library is the one chosen when an incoming request does not
    # designate a library.
    _is_default = Column("is_default", Boolean, index=True, default=False)

    # The name of this library to use when signing short client tokens
    # for consumption by the library registry. e.g. "NYNYPL" for NYPL.
    # This name must be unique across the library registry.
    _library_registry_short_name = Column(
        "library_registry_short_name", Unicode, unique=True
    )

    # The shared secret to use when signing short client tokens for
    # consumption by the library registry.
    library_registry_shared_secret = Column(Unicode, unique=True)

    # A library may have many Patrons.
    patrons: Mapped[List[Patron]] = relationship(
        "Patron", back_populates="library", cascade="all, delete-orphan"
    )

    # An Library may have many admin roles.
    adminroles: Mapped[List[AdminRole]] = relationship(
        "AdminRole", back_populates="library", cascade="all, delete-orphan"
    )

    # A Library may have many CachedFeeds.
    cachedfeeds: Mapped[List[CachedFeed]] = relationship(
        "CachedFeed",
        backref="library",
        cascade="all, delete-orphan",
    )

    # A Library may have many CachedMARCFiles.
    cachedmarcfiles: Mapped[List[CachedMARCFile]] = relationship(
        "CachedMARCFile",
        backref="library",
        cascade="all, delete-orphan",
    )

    # A Library may have many CustomLists.
    custom_lists: Mapped[List[CustomList]] = relationship(
        "CustomList", backref="library", lazy="joined", uselist=True
    )

    # Lists shared with this library
    # shared_custom_lists: "CustomList"
    shared_custom_lists: Mapped[List[CustomList]] = relationship(
        "CustomList",
        secondary=lambda: customlist_sharedlibrary,
        back_populates="shared_locally_with_libraries",
        uselist=True,
    )

    # A Library may have many ExternalIntegrations.
    integrations: Mapped[List[ExternalIntegration]] = relationship(
        "ExternalIntegration",
        secondary=lambda: externalintegrations_libraries,
        back_populates="libraries",
    )

    # Any additional configuration information is stored as
    # ConfigurationSettings.
    settings: Mapped[List[ConfigurationSetting]] = relationship(
        "ConfigurationSetting",
        back_populates="library",
        lazy="joined",
        cascade="all, delete",
    )

    # A Library may have many CirculationEvents
    circulation_events: Mapped[List[CirculationEvent]] = relationship(
        "CirculationEvent", backref="library", cascade="all, delete-orphan"
    )

    library_announcements: Mapped[List[Announcement]] = relationship(
        "Announcement",
        back_populates="library",
        cascade="all, delete-orphan",
    )

    # A class-wide cache mapping library ID to the calculated value
    # used for Library.has_root_lane.  This is invalidated whenever
    # Lane configuration changes, and it will also expire on its own.
    _has_root_lane_cache = ExpiringDict(max_len=1000, max_age_seconds=3600)

    lanes: Mapped[List[Lane]] = relationship(
        "Lane",
        back_populates="library",
        cascade="all, delete-orphan",
    )

    # The library's public / private RSA key-pair.
    # The public key is stored in PEM format.
    public_key = Column(Unicode, nullable=False)
    # The private key is stored in DER binary format.
    private_key = Column(LargeBinary, nullable=False)

    # The libraries logo image, stored as a base64 encoded string.
    logo: Mapped[LibraryLogo] = relationship(
        "LibraryLogo",
        back_populates="library",
        cascade="all, delete-orphan",
        lazy="select",
        uselist=False,
    )

    # Typing specific
    collections: List[Collection]

    def __repr__(self):
        return (
            '<Library: name="%s", short name="%s", uuid="%s", library registry short name="%s">'
            % (self.name, self.short_name, self.uuid, self.library_registry_short_name)
        )

    def cache_key(self):
        return self.short_name

    @classmethod
    def lookup(cls, _db, short_name):
        """Look up a library by short name."""

        def _lookup():
            library = get_one(_db, Library, short_name=short_name)
            return library, False

        library, is_new = cls.by_cache_key(_db, short_name, _lookup)
        return library

    @classmethod
    def default(cls, _db):
        """Find the default Library."""
        # If for some reason there are multiple default libraries in
        # the database, they're not actually interchangeable, but
        # raising an error here might make it impossible to fix the
        # problem.
        defaults = (
            _db.query(Library)
            .filter(Library._is_default == True)
            .order_by(Library.id.asc())
            .all()
        )
        if len(defaults) == 1:
            # This is the normal case.
            return defaults[0]

        default_library = None
        if not defaults:
            # There is no current default. Find the library with the
            # lowest ID and make it the default.
            libraries = _db.query(Library).order_by(Library.id.asc()).limit(1)
            if not libraries.count():
                # There are no libraries in the system, so no default.
                return None
            [default_library] = libraries
            logging.warning(
                "No default library, setting %s as default."
                % (default_library.short_name)
            )
        else:
            # There is more than one default, probably caused by a
            # race condition. Fix it by arbitrarily designating one
            # of the libraries as the default.
            default_library = defaults[0]
            logging.warning(
                "Multiple default libraries, setting %s as default."
                % (default_library.short_name)
            )
        default_library.is_default = True
        return default_library

    @classmethod
    def generate_keypair(cls) -> Tuple[str, bytes]:
        """Generate a public / private keypair for a library."""
        private_key = RSA.generate(2048)
        public_key = private_key.public_key()
        public_key_str = public_key.export_key("PEM").decode("utf-8")
        private_key_bytes = private_key.export_key("DER")
        return public_key_str, private_key_bytes

    @hybrid_property
    def library_registry_short_name(self):
        """Gets library_registry_short_name from database"""
        return self._library_registry_short_name

    @library_registry_short_name.setter
    def library_registry_short_name(self, value):
        """Uppercase the library registry short name on the way in."""
        if value:
            value = value.upper()
            if "|" in value:
                raise ValueError(
                    "Library registry short name cannot contain the pipe character."
                )
            value = str(value)
        self._library_registry_short_name = value

    def setting(self, key):
        """Find or create a ConfigurationSetting on this Library.
        :param key: Name of the setting.
        :return: A ConfigurationSetting
        """
        from .configuration import ConfigurationSetting

        return ConfigurationSetting.for_library(key, self)

    @property
    def all_collections(self):
        for collection in self.collections:
            yield collection
            yield from collection.parents

    # Some specific per-library configuration settings.

    # The name of the per-library regular expression used to derive a patron's
    # external_type from their authorization_identifier.
    EXTERNAL_TYPE_REGULAR_EXPRESSION = "external_type_regular_expression"

    # The name of the per-library configuration policy that controls whether
    # books may be put on hold.
    ALLOW_HOLDS = Configuration.ALLOW_HOLDS

    # Each facet group has two associated per-library keys: one
    # configuring which facets are enabled for that facet group, and
    # one configuring which facet is the default.
    ENABLED_FACETS_KEY_PREFIX = Configuration.ENABLED_FACETS_KEY_PREFIX
    DEFAULT_FACET_KEY_PREFIX = Configuration.DEFAULT_FACET_KEY_PREFIX

    # Each library may set a minimum quality for the books that show
    # up in the 'featured' lanes that show up on the front page.
    MINIMUM_FEATURED_QUALITY = Configuration.MINIMUM_FEATURED_QUALITY

    # Each library may configure the maximum number of books in the
    # 'featured' lanes.
    FEATURED_LANE_SIZE = Configuration.FEATURED_LANE_SIZE

    @property
    def allow_holds(self):
        """Does this library allow patrons to put items on hold?"""
        value = self.setting(self.ALLOW_HOLDS).bool_value
        if value is None:
            # If the library has not set a value for this setting,
            # holds are allowed.
            value = True
        return value

    @property
    def minimum_featured_quality(self):
        """The minimum quality a book must have to be 'featured'."""
        value = self.setting(self.MINIMUM_FEATURED_QUALITY).float_value
        if value is None:
            value = 0.65
        return value

    @property
    def featured_lane_size(self):
        """The minimum quality a book must have to be 'featured'."""
        value = self.setting(self.FEATURED_LANE_SIZE).int_value
        if value is None:
            value = 15
        return value

    @property
    def entrypoints(self):
        """The EntryPoints enabled for this library."""
        values = self.setting(EntryPoint.ENABLED_SETTING).json_value
        if values is None:
            # No decision has been made about enabled EntryPoints.
            for cls in EntryPoint.DEFAULT_ENABLED:
                yield cls
        else:
            # It's okay for `values` to be an empty list--that means
            # the library wants to only use lanes, no entry points.
            for v in values:
                cls = EntryPoint.BY_INTERNAL_NAME.get(v)
                if cls:
                    yield cls

    def enabled_facets(self, group_name):
        """Look up the enabled facets for a given facet group."""
        if group_name == FacetConstants.DISTRIBUTOR_FACETS_GROUP_NAME:
            enabled = []
            for collection in self.collections:
                if collection.data_source and collection.data_source.name:
                    enabled.append(collection.data_source.name)
            return list(set(enabled))

        if group_name == FacetConstants.COLLECTION_NAME_FACETS_GROUP_NAME:
            enabled = []
            for collection in self.collections:
                enabled.append(collection.name)
            return enabled

        setting = self.enabled_facets_setting(group_name)
        value = None

        try:
            value = setting.json_value
        except ValueError as e:
            logging.error(
                "Invalid list of enabled facets for %s: %s", group_name, setting.value
            )
        if value is None:
            value = list(FacetConstants.DEFAULT_ENABLED_FACETS.get(group_name, []))
        return value

    def enabled_facets_setting(self, group_name):
        key = self.ENABLED_FACETS_KEY_PREFIX + group_name
        return self.setting(key)

    @property
    def has_root_lanes(self):
        """Does this library have any lanes that act as the root
        lane for a certain patron type?

        :return: A boolean
        """

        # NOTE: Although this fact is derived from the Lanes, not the
        # Library, the result is stored in the Library object for
        # performance reasons.
        #
        # This makes it important to clear the cache of Library
        # objects whenever the Lane configuration changes. Otherwise a
        # library that went from not having root lanes, to having them
        # (or vice versa) might not see the change take effect without
        # a server restart.
        value = Library._has_root_lane_cache.get(self.id, None)
        if value is None:
            from ..lane import Lane

            _db = Session.object_session(self)
            root_lanes = (
                _db.query(Lane)
                .filter(Lane.library == self)
                .filter(Lane.root_for_patron_type != None)
            )
            value = root_lanes.count() > 0
            Library._has_root_lane_cache[self.id] = value
        return value

    def restrict_to_ready_deliverable_works(
        self,
        query,
        collection_ids=None,
        show_suppressed=False,
    ):
        """Restrict a query to show only presentation-ready works present in
        an appropriate collection which the default client can
        fulfill.
        Note that this assumes the query has an active join against
        LicensePool.
        :param query: The query to restrict.
        :param collection_ids: Only include titles in the given
        collections.
        :param show_suppressed: Include titles that have nothing but
        suppressed LicensePools.
        """
        from .collection import Collection

        collection_ids = collection_ids or [x.id for x in self.all_collections]
        return Collection.restrict_to_ready_deliverable_works(
            query,
            collection_ids=collection_ids,
            show_suppressed=show_suppressed,
            allow_holds=self.allow_holds,
        )

    def estimated_holdings_by_language(self, include_open_access=True):
        """Estimate how many titles this library has in various languages.
        The estimate is pretty good but should not be relied upon as
        exact.
        :return: A Counter mapping languages to the estimated number
        of titles in that language.
        """
        _db = Session.object_session(self)
        qu = (
            _db.query(Edition.language, func.count(Work.id).label("work_count"))
            .select_from(Work)
            .join(Work.license_pools)
            .join(Work.presentation_edition)
            .filter(Edition.language != None)
            .group_by(Edition.language)
        )
        qu = self.restrict_to_ready_deliverable_works(qu)
        if not include_open_access:
            qu = qu.filter(LicensePool.open_access == False)
        counter = Counter()
        for language, count in qu:
            counter[language] = count
        return counter

    def default_facet(self, group_name):
        """Look up the default facet for a given facet group."""
        value = self.default_facet_setting(group_name).value
        if not value:
            value = FacetConstants.DEFAULT_FACET.get(group_name)
        return value

    def default_facet_setting(self, group_name):
        key = self.DEFAULT_FACET_KEY_PREFIX + group_name
        return self.setting(key)

    def explain(self, include_secrets=False):
        """Create a series of human-readable strings to explain a library's
        settings.

        :param include_secrets: For security reasons, secrets are not
            displayed by default.
        :return: A list of explanatory strings.
        """
        lines = []
        if self.uuid:
            lines.append('Library UUID: "%s"' % self.uuid)
        if self.name:
            lines.append('Name: "%s"' % self.name)
        if self.short_name:
            lines.append('Short name: "%s"' % self.short_name)

        if self.library_registry_short_name:
            lines.append(
                'Short name (for library registry): "%s"'
                % self.library_registry_short_name
            )
        if self.library_registry_shared_secret and include_secrets:
            lines.append(
                'Shared secret (for library registry): "%s"'
                % self.library_registry_shared_secret
            )

        # Find all ConfigurationSettings that are set on the library
        # itself and are not on the library + an external integration.
        settings = [x for x in self.settings if not x.external_integration]
        if settings:
            lines.append("")
            lines.append("Configuration settings:")
            lines.append("-----------------------")
        for setting in settings:
            if (include_secrets or not setting.is_secret) and setting.value is not None:
                lines.append(f"{setting.key}='{setting.value}'")

        integrations = list(self.integrations)
        if integrations:
            lines.append("")
            lines.append("External integrations:")
            lines.append("----------------------")
        for integration in integrations:
            lines.extend(integration.explain(self, include_secrets=include_secrets))
            lines.append("")
        return lines

    @property
    def is_default(self):
        return self._is_default

    @is_default.setter
    def is_default(self, new_is_default):
        """Set this library, and only this library, as the default."""
        if self._is_default and not new_is_default:
            raise ValueError(
                "You cannot stop a library from being the default library; you must designate a different library as the default."
            )

        _db = Session.object_session(self)
        for library in _db.query(Library):
            if library == self:
                library._is_default = True
            else:
                library._is_default = False


class LibraryLogo(Base):
    """
    A logo for a library. Stored in a separate table so that it can be
    loaded lazily.

    TODO: It would be nice to just store these in S3, so they don't have
          to hit the database at all.
    """

    __tablename__ = "libraries_logos"
    library_id = Column(Integer, ForeignKey("libraries.id"), primary_key=True)
    library: Mapped[Library] = relationship(
        "Library", back_populates="logo", uselist=False
    )

    # The logo stored as a base-64 encoded png.
    content = Column(LargeBinary, nullable=False)

    @property
    def data_url(self) -> str:
        """The logo stored as a data URL."""
        if self.content is None:
            raise RuntimeError("Logo content is None")
        return f"data:image/png;base64,{self.content.decode('utf8')}"


externalintegrations_libraries: Table = Table(
    "externalintegrations_libraries",
    Base.metadata,
    Column(
        "externalintegration_id",
        Integer,
        ForeignKey("externalintegrations.id"),
        index=True,
        nullable=False,
    ),
    Column(
        "library_id", Integer, ForeignKey("libraries.id"), index=True, nullable=False
    ),
    UniqueConstraint("externalintegration_id", "library_id"),
)
