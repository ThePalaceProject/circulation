# Library
from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any, NamedTuple

from Crypto.PublicKey import RSA
from expiringdict import ExpiringDict
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    LargeBinary,
    Unicode,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, Query, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import Select
from sqlalchemy.sql.functions import func

from palace.manager.core.entrypoint import EntryPoint
from palace.manager.feed.facets.constants import FacetConstants
from palace.manager.integration.base import (
    integration_settings_load,
    integration_settings_update,
)
from palace.manager.integration.configuration.library import LibrarySettings
from palace.manager.integration.goals import Goals
from palace.manager.sqlalchemy.hassessioncache import HasSessionCache
from palace.manager.sqlalchemy.hybrid import hybrid_property
from palace.manager.sqlalchemy.model.announcements import Announcement
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.admin import AdminRole
    from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
    from palace.manager.sqlalchemy.model.collection import Collection
    from palace.manager.sqlalchemy.model.customlist import CustomList
    from palace.manager.sqlalchemy.model.lane import Lane
    from palace.manager.sqlalchemy.model.patron import Patron


class CollectionInfoTuple(NamedTuple):
    """A tuple containing information about a collection"""

    id: int
    protocol: str


class Library(Base, HasSessionCache):
    """A library that uses this circulation manager to authenticate
    its patrons and manage access to its content.
    A circulation manager may serve many libraries.
    """

    __tablename__ = "libraries"

    id: Mapped[int] = Column(Integer, primary_key=True)

    # The human-readable name of this library. Used in the library's
    # Authentication for OPDS document.
    name = Column(Unicode, unique=True)

    # A short name of this library, to use when identifying it in
    # scripts. e.g. "NYPL" for NYPL.
    short_name: Mapped[str] = Column(Unicode, unique=True, nullable=False)

    # A UUID that uniquely identifies the library among all libraries
    # in the world. This is used to serve the library's Authentication
    # for OPDS document, and it also goes to the library registry.
    uuid = Column(Unicode, unique=True)

    # One, and only one, library may be the default. The default
    # library is the one chosen when an incoming request does not
    # designate a library.
    _is_default: Mapped[bool] = Column(
        "is_default", Boolean, index=True, default=False, nullable=False
    )

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
    patrons: Mapped[list[Patron]] = relationship(
        "Patron", back_populates="library", cascade="all, delete-orphan"
    )

    # An Library may have many admin roles.
    adminroles: Mapped[list[AdminRole]] = relationship(
        "AdminRole", back_populates="library", cascade="all, delete-orphan"
    )

    # A Library may have many CustomLists.
    custom_lists: Mapped[list[CustomList]] = relationship(
        "CustomList", back_populates="library", uselist=True
    )

    # Lists shared with this library
    # shared_custom_lists: "CustomList"
    shared_custom_lists: Mapped[list[CustomList]] = relationship(
        "CustomList",
        secondary="customlist_sharedlibraries",
        back_populates="shared_locally_with_libraries",
        uselist=True,
    )

    # Any additional configuration information is stored as JSON on this column.
    settings_dict: Mapped[dict[str, Any]] = Column(JSONB, nullable=False, default=dict)

    # A Library may have many CirculationEvents
    circulation_events: Mapped[list[CirculationEvent]] = relationship(
        "CirculationEvent", back_populates="library", cascade="all, delete-orphan"
    )

    library_announcements: Mapped[list[Announcement]] = relationship(
        "Announcement",
        back_populates="library",
        cascade="all, delete-orphan",
    )

    # A class-wide cache mapping library ID to the calculated value
    # used for Library.has_root_lane.  This is invalidated whenever
    # Lane configuration changes, and it will also expire on its own.
    _has_root_lane_cache: dict[int | None, bool] = ExpiringDict(
        max_len=1000, max_age_seconds=3600
    )

    # A Library can have many lanes
    lanes: Mapped[list[Lane]] = relationship(
        "Lane",
        back_populates="library",
        foreign_keys="Lane.library_id",
        cascade="all, delete-orphan",
    )

    # The library's public / private RSA key-pair.
    # The public key is stored in PEM format.
    public_key: Mapped[str] = Column(Unicode, nullable=False)
    # The private key is stored in DER binary format.
    private_key: Mapped[bytes] = Column(LargeBinary, nullable=False)

    # The libraries logo image, stored as a base64 encoded string.
    logo: Mapped[LibraryLogo | None] = relationship(
        "LibraryLogo",
        back_populates="library",
        cascade="all, delete-orphan",
        lazy="select",
        uselist=False,
    )

    def _associated_collections_query(self, base_query: Select | None = None) -> Select:
        from palace.manager.sqlalchemy.model.collection import Collection
        from palace.manager.sqlalchemy.model.integration import (
            IntegrationConfiguration,
            IntegrationLibraryConfiguration,
        )

        # We can pass in a base query to build on, but if we don't, we'll start from the Collection table.
        query = base_query if base_query is not None else select(Collection)
        return (
            query.join(IntegrationConfiguration)
            .join(IntegrationLibraryConfiguration)
            .where(
                IntegrationConfiguration.goal == Goals.LICENSE_GOAL,
                IntegrationLibraryConfiguration.library_id == self.id,
            )
        )

    def _active_collections_query(self) -> Select:
        """Return a SQLAlchemy query that selects active collections for this library."""
        from palace.manager.sqlalchemy.model.collection import Collection
        from palace.manager.sqlalchemy.model.integration import (
            IntegrationLibraryConfiguration,
        )

        return (
            Collection.active_collections_filter(sa_select=select(Collection))
            .join(IntegrationLibraryConfiguration)
            .where(
                IntegrationLibraryConfiguration.library_id == self.id,
            )
        )

    @property
    def associated_collections(self) -> Sequence[Collection]:
        """Get all associated collections for this library."""
        _db = Session.object_session(self)
        return _db.scalars(self._associated_collections_query()).all()

    @property
    def active_collections(self) -> Sequence[Collection]:
        """Active collections for this library, cached for the life of the session."""
        _db = Session.object_session(self)
        cache: dict[int, list[Collection]] = _db.info.setdefault(
            "_active_collections_cache", {}
        )
        if self.id not in cache:
            cache[self.id] = list(_db.scalars(self._active_collections_query()).all())
        return cache[self.id]

    @staticmethod
    def _clear_active_collections_cache(target: Base) -> None:
        """Helper to clear the cache for `active_collections`.

        It is called by SQLAlchemy event listeners.
        """
        session = Session.object_session(target)
        if session is not None:
            cache = session.info.get("_active_collections_cache")
            if cache is not None:
                cache.clear()

    # Cache of the libraries loaded settings object
    _settings: LibrarySettings | None

    def __repr__(self) -> str:
        return (
            '<Library: name="%s", short name="%s", uuid="%s", library registry short name="%s">'
            % (self.name, self.short_name, self.uuid, self.library_registry_short_name)
        )

    def cache_key(self) -> str | None:
        return self.short_name

    @classmethod
    def lookup(cls, _db: Session, short_name: str | None) -> Library | None:
        """Look up a library by short name."""

        def _lookup() -> tuple[Library | None, bool]:
            library = get_one(_db, Library, short_name=short_name)
            return library, False

        library, is_new = cls.by_cache_key(_db, short_name, _lookup)
        return library

    @classmethod
    def default(cls, _db: Session) -> Library | None:
        """Find the default Library."""
        # If for some reason there are multiple default libraries in
        # the database, they're not actually interchangeable, but
        # raising an error here might make it impossible to fix the
        # problem.
        defaults: list[Library] = (
            _db.query(Library)
            .filter(Library._is_default == True)
            .order_by(Library.id.asc())
            .all()
        )
        if len(defaults) == 1:
            # This is the normal case.
            return defaults[0]

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
        return default_library  # type: ignore[no-any-return]

    @classmethod
    def generate_keypair(cls) -> tuple[str, bytes]:
        """Generate a public / private keypair for a library."""
        private_key = RSA.generate(2048)
        public_key = private_key.public_key()
        public_key_str = public_key.export_key("PEM").decode("utf-8")
        private_key_bytes = private_key.export_key("DER")
        return public_key_str, private_key_bytes

    @hybrid_property
    def library_registry_short_name(self) -> str | None:
        """Gets library_registry_short_name from database"""
        return self._library_registry_short_name

    @library_registry_short_name.setter
    def library_registry_short_name(self, value: str | None) -> None:
        """Uppercase the library registry short name on the way in."""
        if value:
            value = value.upper()
            if "|" in value:
                raise ValueError(
                    "Library registry short name cannot contain the pipe character."
                )
            value = str(value)
        self._library_registry_short_name = value

    @property
    def settings(self) -> LibrarySettings:
        """Get the settings for this integration"""
        settings = getattr(self, "_settings", None)
        if settings is None:
            if not isinstance(self.settings_dict, dict):
                raise ValueError(
                    "settings_dict for library %s is not a dict: %r"
                    % (self.short_name, self.settings_dict)
                )
            settings = integration_settings_load(LibrarySettings, self)
            self._settings = settings
        return settings

    def update_settings(self, new_settings: LibrarySettings) -> None:
        """Update the settings for this integration"""
        self._settings = None
        integration_settings_update(LibrarySettings, self, new_settings, merge=True)

    @property
    def entrypoints(self) -> Generator[type[EntryPoint]]:
        """The EntryPoints enabled for this library."""
        values = self.settings.enabled_entry_points
        for v in values:
            cls = EntryPoint.BY_INTERNAL_NAME.get(v)
            if cls:
                yield cls

    def enabled_facets(self, group_name: str) -> list[str]:
        """Look up the enabled facets for a given facet group."""
        if group_name in [
            FacetConstants.DISTRIBUTOR_FACETS_GROUP_NAME,
            FacetConstants.COLLECTION_NAME_FACETS_GROUP_NAME,
        ]:
            enabled = []
            collections = self.active_collections

        if group_name == FacetConstants.DISTRIBUTOR_FACETS_GROUP_NAME:
            for collection in collections:
                if collection.data_source:
                    enabled.append(collection.data_source.active_name)
            return list(set(enabled))

        if group_name == FacetConstants.COLLECTION_NAME_FACETS_GROUP_NAME:
            for collection in collections:
                if collection.name is not None:
                    enabled.append(collection.name)
            return enabled

        return getattr(self.settings, f"facets_enabled_{group_name}")  # type: ignore[no-any-return]

    @property
    def has_root_lanes(self) -> bool:
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
            from palace.manager.sqlalchemy.model.lane import Lane

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
        query: Query[Work],
        collection_ids: list[int] | None = None,
        show_suppressed: bool = False,
    ) -> Query[Work]:
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
        from palace.manager.sqlalchemy.model.collection import Collection

        collection_ids = collection_ids or [
            x.id for x in self.associated_collections if x.id is not None
        ]
        return Collection.restrict_to_ready_deliverable_works(
            query,
            collection_ids=collection_ids,
            show_suppressed=show_suppressed,
            allow_holds=self.settings.allow_holds,
        )

    def estimated_holdings_by_language(
        self, include_open_access: bool = True
    ) -> Counter[str]:
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
        counter: Counter[str] = Counter()
        for language, count in qu:  # type: ignore[misc]
            counter[language] = count  # type: ignore[has-type]
        return counter

    def default_facet(self, group_name: str) -> str:
        if (
            group_name == FacetConstants.DISTRIBUTOR_FACETS_GROUP_NAME
            or group_name == FacetConstants.COLLECTION_NAME_FACETS_GROUP_NAME
        ):
            return FacetConstants.DEFAULT_FACET[group_name]

        """Look up the default facet for a given facet group."""
        return getattr(self.settings, "facets_default_" + group_name)  # type: ignore[no-any-return]

    def explain(self, include_secrets: bool = False) -> list[str]:
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

        # Find all settings that are set on the library
        lines.append("")
        lines.append("Configuration settings:")
        lines.append("-----------------------")
        for key, value in self.settings.model_dump(exclude_defaults=False).items():
            if value is not None:
                lines.append(f"{key}='{value}'")
        lines.append("")
        return lines

    @property
    def is_default(self) -> bool | None:
        return self._is_default

    @is_default.setter
    def is_default(self, new_is_default: bool) -> None:
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
    library_id: Mapped[int] = Column(
        Integer, ForeignKey("libraries.id"), primary_key=True
    )
    library: Mapped[Library] = relationship(
        "Library", back_populates="logo", uselist=False
    )

    # The logo stored as a base-64 encoded png.
    content: Mapped[bytes] = Column(LargeBinary, nullable=False)

    @property
    def data_url(self) -> str:
        """The logo stored as a data URL."""
        if self.content is None:
            raise RuntimeError("Logo content is None")
        return f"data:image/png;base64,{self.content.decode('utf8')}"
