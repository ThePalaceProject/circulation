from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Generator, List, Optional, Tuple, TypeVar

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    Table,
    Unicode,
    UniqueConstraint,
    exists,
)
from sqlalchemy.orm import Mapped, Query, backref, mapper, relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import and_, or_

from core.integration.goals import Goals
from core.model import Base, create, get_one_or_create
from core.model.configuration import ConfigurationSetting, ExternalIntegration
from core.model.constants import EditionConstants
from core.model.coverage import CoverageRecord
from core.model.datasource import DataSource
from core.model.edition import Edition
from core.model.hassessioncache import HasSessionCache
from core.model.hybrid import hybrid_property
from core.model.identifier import Identifier
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.model.library import Library
from core.model.licensing import LicensePool, LicensePoolDeliveryMechanism
from core.model.work import Work

if TYPE_CHECKING:
    from core.external_search import ExternalSearchIndex
    from core.model import Credential, CustomList, Timestamp


T = TypeVar("T")


class Collection(Base, HasSessionCache):

    """A Collection is a set of LicensePools obtained through some mechanism."""

    __tablename__ = "collections"
    id = Column(Integer, primary_key=True, nullable=False)

    name = Column(Unicode, unique=True, nullable=False, index=True)

    DATA_SOURCE_NAME_SETTING = "data_source"

    # For use in forms that edit Collections.
    EXTERNAL_ACCOUNT_ID_KEY = "external_account_id"

    # How does the provider of this collection distinguish it from
    # other collections it provides? On the other side this is usually
    # called a "library ID".
    external_account_id = Column(Unicode, nullable=True)

    # How do we connect to the provider of this collection? Any url,
    # authentication information, or additional configuration goes
    # into the external integration, as does the 'protocol', which
    # designates the integration technique we will use to actually get
    # the metadata and licenses. Each Collection has a distinct
    # ExternalIntegration.
    external_integration_id = Column(
        Integer, ForeignKey("externalintegrations.id"), unique=True, index=True
    )
    _external_integration: ExternalIntegration

    integration_configuration_id = Column(
        Integer,
        ForeignKey("integration_configurations.id", ondelete="SET NULL"),
        unique=True,
        index=True,
    )
    integration_configuration: Mapped[IntegrationConfiguration] = relationship(
        "IntegrationConfiguration",
        uselist=False,
        back_populates="collection",
        cascade="all,delete-orphan",
        single_parent=True,
    )

    # A Collection may specialize some other Collection. For instance,
    # an Overdrive Advantage collection is a specialization of an
    # ordinary Overdrive collection. It uses the same access key and
    # secret as the Overdrive collection, but it has a distinct
    # external_account_id.
    parent_id = Column(Integer, ForeignKey("collections.id"), index=True)
    # SQLAlchemy will create a Collection-typed field called "parent".
    parent: Collection

    # When deleting a collection, this flag is set to True so that the deletion
    # script can take care of deleting it in the background. This is
    # useful for deleting large collections which can timeout when deleting.
    marked_for_deletion = Column(Boolean, default=False)

    # A collection may have many child collections. For example,
    # An Overdrive collection may have many children corresponding
    # to Overdrive Advantage collections.
    children: Mapped[List[Collection]] = relationship(
        "Collection", backref=backref("parent", remote_side=[id]), uselist=True
    )

    # A Collection can provide books to many Libraries.
    libraries: Mapped[List[Library]] = relationship(
        "Library",
        secondary=lambda: collections_libraries,
        backref="collections",
        uselist=True,
    )

    # A Collection can include many LicensePools.
    licensepools: Mapped[List[LicensePool]] = relationship(
        "LicensePool",
        back_populates="collection",
        cascade="all, delete-orphan",
        uselist=True,
    )

    # A Collection can have many associated Credentials.
    credentials: Mapped[List[Credential]] = relationship(
        "Credential", back_populates="collection", cascade="delete"
    )

    # A Collection can be monitored by many Monitors, each of which
    # will have its own Timestamp.
    timestamps: Mapped[List[Timestamp]] = relationship(
        "Timestamp", back_populates="collection"
    )

    catalog: Mapped[List[Identifier]] = relationship(
        "Identifier", secondary=lambda: collections_identifiers, backref="collections"
    )

    # A Collection can be associated with multiple CoverageRecords
    # for Identifiers in its catalog.
    coverage_records: Mapped[List[CoverageRecord]] = relationship(
        "CoverageRecord", backref="collection", cascade="all"
    )

    # A collection may be associated with one or more custom lists.
    # When a new license pool is added to the collection, it will
    # also be added to the list. Admins can remove items from the
    # the list and they won't be added back, so the list doesn't
    # necessarily match the collection.
    customlists: Mapped[List[CustomList]] = relationship(
        "CustomList", secondary=lambda: collections_customlists, backref="collections"
    )

    # Most data sources offer different catalogs to different
    # libraries.  Data sources in this list offer the same catalog to
    # every library.
    GLOBAL_COLLECTION_DATA_SOURCES = [DataSource.ENKI]

    def __repr__(self) -> str:
        return f'<Collection "{self.name}"/"{self.protocol}" ID={self.id}>'

    def cache_key(self) -> Tuple[str | None, str | None]:
        return self.name, self.external_integration.protocol

    @classmethod
    def by_name_and_protocol(
        cls, _db: Session, name: str, protocol: str
    ) -> Tuple[Collection, bool]:
        """Find or create a Collection with the given name and the given
        protocol.

        This method uses the full-table cache if possible.

        :return: A 2-tuple (collection, is_new)
        """
        key = (name, protocol)

        def lookup_hook() -> Tuple[Collection, bool]:
            return cls._by_name_and_protocol(_db, key)

        return cls.by_cache_key(_db, key, lookup_hook)

    @classmethod
    def _by_name_and_protocol(
        cls, _db: Session, cache_key: Tuple[str, str]
    ) -> Tuple[Collection, bool]:
        """Find or create a Collection with the given name and the given
        protocol.

        We can't use get_one_or_create because the protocol is kept in
        a separate database object, (an ExternalIntegration).

        :return: A 2-tuple (collection, is_new)
        """
        name, protocol = cache_key

        qu = cls.by_protocol(_db, protocol)
        qu = qu.filter(Collection.name == name)
        try:
            collection = qu.one()
            is_new = False
        except NoResultFound as e:
            # Make a new Collection.
            collection, is_new = get_one_or_create(_db, Collection, name=name)
            if not is_new and collection.protocol != protocol:
                # The collection already exists, it just uses a different
                # protocol than the one we asked about.
                raise ValueError(
                    f'Collection "{name}" does not use protocol "{protocol}".'
                )
            integration = collection.create_external_integration(protocol=protocol)
            collection.external_integration.protocol = protocol
            collection.create_integration_configuration(protocol)
        return collection, is_new

    @classmethod
    def by_protocol(cls, _db: Session, protocol: str | None) -> Query[Collection]:
        """Query collections that get their licenses through the given protocol.

        Collections marked for deletion are not included.

        :param protocol: Protocol to use. If this is None, all
            Collections will be returned except those marked for deletion.
        """
        qu = _db.query(Collection)
        if protocol:
            qu = (
                qu.join(
                    IntegrationConfiguration,
                    IntegrationConfiguration.id
                    == Collection.integration_configuration_id,
                )
                .filter(IntegrationConfiguration.goal == Goals.LICENSE_GOAL)
                .filter(IntegrationConfiguration.protocol == protocol)
                .filter(Collection.marked_for_deletion == False)
            )

        return qu

    @classmethod
    def by_datasource(
        cls, _db: Session, data_source: DataSource | str
    ) -> Query[Collection]:
        """Query collections that are associated with the given DataSource.

        Collections marked for deletion are not included.
        """
        data_source_name = (
            data_source.name if isinstance(data_source, DataSource) else data_source
        )

        qu = (
            _db.query(cls)
            .join(
                IntegrationConfiguration,
                cls.integration_configuration_id == IntegrationConfiguration.id,
            )
            .filter(
                IntegrationConfiguration.settings_dict[
                    Collection.DATA_SOURCE_NAME_SETTING
                ].astext
                == data_source_name
            )
            .filter(Collection.marked_for_deletion == False)
        )
        return qu

    @hybrid_property
    def protocol(self) -> str:
        """What protocol do we need to use to get licenses for this
        collection?
        """
        if self.integration_configuration is None:
            raise ValueError("Collection has no integration configuration.")

        if self.integration_configuration.protocol is None:
            raise ValueError(
                "Collection has integration configuration but no protocol."
            )

        return self.integration_configuration.protocol

    @protocol.setter
    def protocol(self, new_protocol: str) -> None:
        """Modify the protocol in use by this Collection."""
        if self.parent and self.parent.protocol != new_protocol:
            raise ValueError(
                "Proposed new protocol (%s) contradicts parent collection's protocol (%s)."
                % (new_protocol, self.parent.protocol)
            )
        self.integration_configuration.protocol = new_protocol
        for child in self.children:
            child.protocol = new_protocol

    @hybrid_property
    def primary_identifier_source(self) -> str | None:
        """Identify if should try to use another identifier than <id>"""
        return self.integration_configuration.settings_dict.get(
            ExternalIntegration.PRIMARY_IDENTIFIER_SOURCE
        )

    @primary_identifier_source.setter
    def primary_identifier_source(self, new_primary_identifier_source: str) -> None:
        """Modify the primary identifier source in use by this Collection."""
        self.integration_configuration.settings_dict = (
            self.integration_configuration.settings_dict.copy()
        )
        self.integration_configuration.settings_dict[
            ExternalIntegration.PRIMARY_IDENTIFIER_SOURCE
        ] = new_primary_identifier_source

    # For collections that can control the duration of the loans they
    # create, the durations are stored in these settings and new loans are
    # expected to be created using these settings. For collections
    # where loan duration is negotiated out-of-bounds, all loans are
    # _assumed_ to have these durations unless we hear otherwise from
    # the server.
    AUDIOBOOK_LOAN_DURATION_KEY = "audio_loan_duration"
    EBOOK_LOAN_DURATION_KEY = "ebook_loan_duration"
    STANDARD_DEFAULT_LOAN_PERIOD = 21

    def default_loan_period(
        self, library: Library, medium: str = EditionConstants.BOOK_MEDIUM
    ) -> int:
        """Until we hear otherwise from the license provider, we assume
        that someone who borrows a non-open-access item from this
        collection has it for this number of days.
        """
        value = (
            self.default_loan_period_setting(library, medium)
            or self.STANDARD_DEFAULT_LOAN_PERIOD
        )
        return value

    @classmethod
    def loan_period_key(cls, medium: str = EditionConstants.BOOK_MEDIUM) -> str:
        if medium == EditionConstants.AUDIO_MEDIUM:
            return cls.AUDIOBOOK_LOAN_DURATION_KEY
        else:
            return cls.EBOOK_LOAN_DURATION_KEY

    def default_loan_period_setting(
        self,
        library: Library,
        medium: str = EditionConstants.BOOK_MEDIUM,
    ) -> Optional[int]:
        """Until we hear otherwise from the license provider, we assume
        that someone who borrows a non-open-access item from this
        collection has it for this number of days.
        """
        key = self.loan_period_key(medium)
        if library.id is None:
            return None

        config = self.integration_configuration.for_library(library.id)
        if config is None:
            return None

        return config.settings_dict.get(key)

    DEFAULT_RESERVATION_PERIOD_KEY = "default_reservation_period"
    STANDARD_DEFAULT_RESERVATION_PERIOD = 3

    def _set_settings(self, **kwargs: Any) -> None:
        settings_dict = self.integration_configuration.settings_dict.copy()
        settings_dict.update(kwargs)
        self.integration_configuration.settings_dict = settings_dict

    @hybrid_property
    def default_reservation_period(self) -> int:
        """Until we hear otherwise from the license provider, we assume
        that someone who puts an item on hold has this many days to
        check it out before it goes to the next person in line.
        """
        return (
            self.integration_configuration.settings_dict.get(
                self.DEFAULT_RESERVATION_PERIOD_KEY
            )
            or self.STANDARD_DEFAULT_RESERVATION_PERIOD
        )

    @default_reservation_period.setter
    def default_reservation_period(self, new_value: int) -> None:
        new_value = int(new_value)
        self._set_settings(**{self.DEFAULT_RESERVATION_PERIOD_KEY: new_value})

    # When you import an OPDS feed, you may know the intended audience of the works (e.g. children or researchers),
    # even though the OPDS feed may not contain that information.
    # It should be possible to configure a collection with a default audience,
    # so that books imported from the OPDS feed end up with the right audience.
    DEFAULT_AUDIENCE_KEY = "default_audience"

    @hybrid_property
    def default_audience(self) -> str:
        """Return the default audience set up for this collection.

        :return: Default audience
        """
        return (
            self.integration_configuration.settings_dict.get(self.DEFAULT_AUDIENCE_KEY)
            or ""
        )

    @default_audience.setter
    def default_audience(self, new_value: str) -> None:
        """Set the default audience for this collection.

        :param new_value: New default audience
        """
        self._set_settings(**{self.DEFAULT_AUDIENCE_KEY: str(new_value)})

    def create_external_integration(self, protocol: str) -> ExternalIntegration:
        """Create an ExternalIntegration for this Collection.

        To be used immediately after creating a new Collection,
        e.g. in by_name_and_protocol, from_metadata_identifier, and
        various test methods that create mock Collections.

        If an external integration already exists, return it instead
        of creating another one.

        :param protocol: The protocol known to be in use when getting
            licenses for this collection.
        """
        _db = Session.object_session(self)
        goal = ExternalIntegration.LICENSE_GOAL
        external_integration, is_new = get_one_or_create(
            _db,
            ExternalIntegration,
            id=self.external_integration_id,
            create_method_kwargs=dict(protocol=protocol, goal=goal),
        )
        if external_integration.protocol != protocol:
            raise ValueError(
                "Located ExternalIntegration, but its protocol (%s) does not match desired protocol (%s)."
                % (external_integration.protocol, protocol)
            )
        self.external_integration_id = external_integration.id
        return external_integration

    def create_integration_configuration(
        self, protocol: str
    ) -> IntegrationConfiguration:
        _db = Session.object_session(self)
        goal = Goals.LICENSE_GOAL
        if self.integration_configuration_id:
            integration = self.integration_configuration
        else:
            integration, is_new = create(
                _db,
                IntegrationConfiguration,
                protocol=protocol,
                goal=goal,
                name=self.name,
            )
        if integration.protocol != protocol:
            raise ValueError(
                "Located ExternalIntegration, but its protocol (%s) does not match desired protocol (%s)."
                % (integration.protocol, protocol)
            )
        self.integration_configuration_id = integration.id
        # Immediately accessing the relationship fills out the data
        return self.integration_configuration

    @property
    def external_integration(self) -> ExternalIntegration:
        """Find the external integration for this Collection, assuming
        it already exists.

        This is generally a safe assumption since by_name_and_protocol and
        from_metadata_identifier both create ExternalIntegrations for the
        Collections they create.
        """
        # We don't enforce this on the database level because it is
        # legitimate for a newly created Collection to have no
        # ExternalIntegration. But by the time it's being used for real,
        # it needs to have one.
        if not self.external_integration_id:
            raise ValueError(
                "No known external integration for collection %s" % self.name
            )
        return self._external_integration

    @hybrid_property
    def data_source(self) -> DataSource | None:
        """Find the data source associated with this Collection.

        Bibliographic metadata obtained through the collection
        protocol is recorded as coming from this data source. A
        LicensePool inserted into this collection will be associated
        with this data source, unless its bibliographic metadata
        indicates some other data source.

        For most Collections, the integration protocol sets the data
        source.  For collections that use the OPDS import protocol,
        the data source is a Collection-specific setting.
        """
        data_source = None
        name = None
        if self.protocol is not None:
            name = ExternalIntegration.DATA_SOURCE_FOR_LICENSE_PROTOCOL.get(
                self.protocol
            )
        if not name:
            name = self.integration_configuration.settings_dict.get(
                Collection.DATA_SOURCE_NAME_SETTING
            )
        _db = Session.object_session(self)
        if name:
            data_source = DataSource.lookup(_db, name, autocreate=True)
        return data_source

    @data_source.setter
    def data_source(self, new_value: DataSource | str) -> None:
        new_datasource_name = (
            new_value.name if isinstance(new_value, DataSource) else new_value
        )

        if self.protocol == new_datasource_name:
            return

        # Only set a DataSource for Collections that don't have an
        # implied source.
        if self.protocol not in ExternalIntegration.DATA_SOURCE_FOR_LICENSE_PROTOCOL:
            if new_datasource_name is not None:
                new_datasource_name = str(new_datasource_name)
            self._set_settings(
                **{Collection.DATA_SOURCE_NAME_SETTING: new_datasource_name}
            )

    @property
    def parents(self) -> Generator[Collection, None, None]:
        if not self.parent_id:
            return None

        _db = Session.object_session(self)
        parent = Collection.by_id(_db, self.parent_id)
        if parent is None:
            return None

        yield parent
        yield from parent.parents

    def disassociate_library(self, library: Library) -> None:
        """Disassociate a Library from this Collection and delete any relevant
        ConfigurationSettings.
        """
        if library is None or library not in self.libraries:
            # No-op.
            return

        _db = Session.object_session(self)
        if self.external_integration_id:
            qu = (
                _db.query(ConfigurationSetting)
                .filter(ConfigurationSetting.library == library)
                .filter(
                    ConfigurationSetting.external_integration
                    == self.external_integration
                )
            )
            qu.delete()
        else:
            raise ValueError(
                "No known external integration for collection %s" % self.name
            )
        if self.integration_configuration_id:
            qu = (
                _db.query(IntegrationLibraryConfiguration)
                .filter(IntegrationLibraryConfiguration.library_id == library.id)
                .filter(
                    IntegrationLibraryConfiguration.parent_id
                    == self.integration_configuration_id
                )
            )
            qu.delete()
        else:
            raise ValueError(
                "No known integration library configuration for collection %s"
                % self.name
            )

        self.libraries.remove(library)

    @property
    def pools_with_no_delivery_mechanisms(self) -> Query[LicensePool]:
        """Find all LicensePools in this Collection that have no delivery
        mechanisms whatsoever.

        :return: A query object.
        """
        _db = Session.object_session(self)
        qu = LicensePool.with_no_delivery_mechanisms(_db)
        return qu.filter(LicensePool.collection == self)  # type: ignore[no-any-return]

    def explain(self, include_secrets: bool = False) -> List[str]:
        """Create a series of human-readable strings to explain a collection's
        settings.

        :param include_secrets: For security reasons,
           sensitive settings such as passwords are not displayed by default.

        :return: A list of explanatory strings.
        """
        lines = []
        if self.name:
            lines.append('Name: "%s"' % self.name)
        if self.parent:
            lines.append("Parent: %s" % self.parent.name)
        integration = self.integration_configuration
        if integration.protocol:
            lines.append('Protocol: "%s"' % integration.protocol)
        for library in self.libraries:
            lines.append('Used by library: "%s"' % library.short_name)
        if self.external_account_id:
            lines.append('External account ID: "%s"' % self.external_account_id)
        for name in sorted(integration.settings_dict):
            value = integration.settings_dict[name]
            if (
                include_secrets or not ConfigurationSetting._is_secret(name)
            ) and value is not None:
                lines.append(f'Setting "{name}": "{value}"')
        return lines

    @classmethod
    def restrict_to_ready_deliverable_works(
        cls,
        query: Query[T],
        collection_ids: List[int] | None = None,
        show_suppressed: bool = False,
        allow_holds: bool = True,
    ) -> Query[T]:
        """Restrict a query to show only presentation-ready works present in
        an appropriate collection which the default client can
        fulfill.

        Note that this assumes the query has an active join against
        LicensePool and Edition.

        :param query: The query to restrict.

        :param show_suppressed: Include titles that have nothing but
            suppressed LicensePools.

        :param collection_ids: Only include titles in the given
            collections.

        :param allow_holds: If false, pools with no available copies
            will be hidden.
        """

        # Only find presentation-ready works.
        query = query.filter(Work.presentation_ready == True)

        # Only find books that have some kind of DeliveryMechanism.
        LPDM = LicensePoolDeliveryMechanism
        exists_clause = exists().where(
            and_(
                LicensePool.data_source_id == LPDM.data_source_id,
                LicensePool.identifier_id == LPDM.identifier_id,
            )
        )
        query = query.filter(exists_clause)

        # Some sources of audiobooks may be excluded because the
        # server can't fulfill them or the expected client can't play
        # them.
        _db = query.session
        excluded = ConfigurationSetting.excluded_audio_data_sources(_db)
        if excluded:
            audio_excluded_ids = [DataSource.lookup(_db, x).id for x in excluded]
            query = query.filter(
                or_(
                    Edition.medium != EditionConstants.AUDIO_MEDIUM,
                    ~LicensePool.data_source_id.in_(audio_excluded_ids),
                )
            )

        # Only find books with unsuppressed LicensePools.
        if not show_suppressed:
            query = query.filter(LicensePool.suppressed == False)

        # Only find books with available licenses or books from self-hosted collections using MirrorUploader
        query = query.filter(
            or_(
                LicensePool.licenses_owned > 0,
                LicensePool.open_access,
                LicensePool.unlimited_access,
            )
        )

        # Only find books in an appropriate collection.
        if collection_ids is not None:
            query = query.filter(LicensePool.collection_id.in_(collection_ids))

        # If we don't allow holds, hide any books with no available copies.
        if not allow_holds:
            query = query.filter(
                or_(
                    LicensePool.licenses_available > 0,
                    LicensePool.open_access,
                    LicensePool.unlimited_access,
                )
            )
        return query

    def delete(self, search_index: ExternalSearchIndex | None = None) -> None:
        """Delete a collection.

        Collections can have hundreds of thousands of
        LicensePools. This deletes a collection gradually in a way
        that can be confined to the background and survive interruption.
        """
        if not self.marked_for_deletion:
            raise Exception(
                "Cannot delete %s: it is not marked for deletion." % self.name
            )

        _db = Session.object_session(self)

        # Disassociate all libraries from this collection.
        for library in self.libraries:
            self.disassociate_library(library)

        # Delete all the license pools. This should be the only part
        # of the application where LicensePools are permanently
        # deleted.
        for i, pool in enumerate(self.licensepools):
            work = pool.work
            if work:
                # We need to remove the item from the collection manually, otherwise the deleted
                # pool will continue to be on the work until we call commit, so we'll never get to
                # the point where we delete the work.
                # https://docs.sqlalchemy.org/en/14/orm/cascades.html#notes-on-delete-deleting-objects-referenced-from-collections-and-scalar-relationships
                work.license_pools.remove(pool)
                if not work.license_pools:
                    work.delete(search_index)

            _db.delete(pool)

        # Delete the ExternalIntegration associated with this
        # Collection, assuming it wasn't deleted already.
        if self.external_integration:
            _db.delete(self.external_integration)

        # Now delete the Collection itself.
        _db.delete(self)
        _db.commit()


collections_libraries: Table = Table(
    "collections_libraries",
    Base.metadata,
    Column(
        "collection_id",
        Integer,
        ForeignKey("collections.id"),
        index=True,
        nullable=False,
    ),
    Column(
        "library_id", Integer, ForeignKey("libraries.id"), index=True, nullable=False
    ),
    UniqueConstraint("collection_id", "library_id"),
)


collections_identifiers: Table = Table(
    "collections_identifiers",
    Base.metadata,
    Column(
        "collection_id",
        Integer,
        ForeignKey("collections.id"),
        index=True,
        nullable=False,
    ),
    Column(
        "identifier_id",
        Integer,
        ForeignKey("identifiers.id"),
        index=True,
        nullable=False,
    ),
    UniqueConstraint("collection_id", "identifier_id"),
)


# Create an ORM model for the collections_identifiers join table
# so it can be used in a bulk_insert_mappings call.
class CollectionIdentifier:
    pass


class CollectionMissing(Exception):
    """An operation was attempted that can only happen within the context
    of a Collection, but there was no Collection available.
    """


mapper(
    CollectionIdentifier,
    collections_identifiers,
    primary_key=(
        collections_identifiers.columns.collection_id,
        collections_identifiers.columns.identifier_id,
    ),
)

collections_customlists: Table = Table(
    "collections_customlists",
    Base.metadata,
    Column(
        "collection_id",
        Integer,
        ForeignKey("collections.id"),
        index=True,
        nullable=False,
    ),
    Column(
        "customlist_id",
        Integer,
        ForeignKey("customlists.id"),
        index=True,
        nullable=False,
    ),
    UniqueConstraint("collection_id", "customlist_id"),
)


class HasExternalIntegrationPerCollection(metaclass=ABCMeta):
    """Interface allowing to get access to an external integration"""

    @abstractmethod
    def collection_external_integration(
        self, collection: Optional[Collection]
    ) -> ExternalIntegration:
        """Returns an external integration associated with the collection

        :param collection: Collection

        :return: External integration associated with the collection
        """
        raise NotImplementedError()
