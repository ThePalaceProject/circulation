from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

from dependency_injector.wiring import Provide, inject
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    ForeignKey,
    Integer,
    Table,
    UniqueConstraint,
    exists,
    not_,
    select,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, Query, aliased, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import Select
from sqlalchemy.sql.expression import and_, or_, true
from sqlalchemy.sql.functions import count

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.integration.goals import Goals
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.redis.key import RedisKeyMixin
from palace.manager.sqlalchemy.constants import EditionConstants
from palace.manager.sqlalchemy.hassessioncache import HasSessionCache
from palace.manager.sqlalchemy.hybrid import hybrid_property
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.coverage import CoverageRecord, Timestamp
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import create

if TYPE_CHECKING:
    from palace.manager.api.circulation.base import CirculationApiType
    from palace.manager.search.external_search import ExternalSearchIndex
    from palace.manager.sqlalchemy.model.credential import Credential
    from palace.manager.sqlalchemy.model.customlist import CustomList


class Collection(Base, HasSessionCache, RedisKeyMixin):
    """A Collection is a set of LicensePools obtained through some mechanism."""

    __tablename__ = "collections"
    id: Mapped[int] = Column(Integer, primary_key=True, nullable=False)

    # How do we connect to the provider of this collection? Any url,
    # authentication information, or additional configuration goes
    # into the external integration, as does the 'protocol', which
    # designates the integration technique we will use to actually get
    # the metadata and licenses. Each Collection has a distinct
    # integration configuration.
    integration_configuration_id: Mapped[int] = Column(
        Integer,
        ForeignKey("integration_configurations.id"),
        unique=True,
        index=True,
        nullable=False,
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
    parent: Mapped[Collection | None] = relationship(
        "Collection", remote_side=[id], back_populates="children"
    )

    # A collection may have many child collections. For example,
    # An Overdrive collection may have many children corresponding
    # to Overdrive Advantage collections.
    children: Mapped[list[Collection]] = relationship(
        "Collection", back_populates="parent", uselist=True
    )

    # When deleting a collection, this flag is set to True so that the deletion
    # script can take care of deleting it in the background. This is
    # useful for deleting large collections which can timeout when deleting.
    marked_for_deletion: Mapped[bool] = Column(Boolean, default=False, nullable=False)

    # A Collection can provide books to many Libraries.
    # https://docs.sqlalchemy.org/en/14/orm/extensions/associationproxy.html#composite-association-proxies
    associated_libraries: Mapped[list[Library]] = association_proxy(
        "integration_configuration", "libraries"
    )

    # A Collection can include many LicensePools.
    licensepools: Mapped[list[LicensePool]] = relationship(
        "LicensePool",
        back_populates="collection",
        cascade="all, delete-orphan",
        uselist=True,
    )

    # A Collection can have many associated Credentials.
    credentials: Mapped[list[Credential]] = relationship(
        "Credential", back_populates="collection", cascade="delete"
    )

    # A Collection can be monitored by many Monitors, each of which
    # will have its own Timestamp.
    timestamps: Mapped[list[Timestamp]] = relationship(
        "Timestamp", back_populates="collection"
    )

    catalog: Mapped[list[Identifier]] = relationship(
        "Identifier", secondary="collections_identifiers", back_populates="collections"
    )

    # A Collection can be associated with multiple CoverageRecords
    # for Identifiers in its catalog.
    coverage_records: Mapped[list[CoverageRecord]] = relationship(
        "CoverageRecord", back_populates="collection", cascade="all"
    )

    # A collection may be associated with one or more custom lists.
    # When a new license pool is added to the collection, it will
    # also be added to the list. Admins can remove items from the
    # the list and they won't be added back, so the list doesn't
    # necessarily match the collection.
    # Order by ID to ensure consistent lock acquisition order and prevent deadlocks.
    customlists: Mapped[list[CustomList]] = relationship(
        "CustomList",
        secondary="collections_customlists",
        back_populates="collections",
        order_by="CustomList.id",
    )

    export_marc_records: Mapped[bool] = Column(Boolean, default=False, nullable=False)

    def __repr__(self) -> str:
        return f'<Collection "{self.name}"/"{self.protocol}" ID={self.id}>'

    def cache_key(self) -> tuple[str | None, str | None]:
        return self.name, self.integration_configuration.protocol

    @classmethod
    def by_name_and_protocol(
        cls, _db: Session, name: str, protocol: str
    ) -> tuple[Collection, bool]:
        """Find or create a Collection with the given name and the given
        protocol.

        This method uses the full-table cache if possible.

        :return: A 2-tuple (collection, is_new)
        """
        key = (name, protocol)

        def lookup_hook() -> tuple[Collection, bool]:
            return cls._by_name_and_protocol(_db, key)

        return cls.by_cache_key(_db, key, lookup_hook)

    @classmethod
    def _by_name_and_protocol(
        cls, _db: Session, cache_key: tuple[str, str]
    ) -> tuple[Collection, bool]:
        """Find or create a Collection with the given name and the given
        protocol.

        We can't use get_one_or_create because the protocol is kept in
        a separate database object, (an IntegrationConfiguration).

        :return: A 2-tuple (collection, is_new)
        """
        name, protocol = cache_key

        query = select(IntegrationConfiguration).where(
            IntegrationConfiguration.name == name
        )
        integration_or_none = _db.execute(query).scalar_one_or_none()
        if integration_or_none is None:
            integration, _ = create(
                _db,
                IntegrationConfiguration,
                protocol=protocol,
                goal=Goals.LICENSE_GOAL,
                name=name,
            )
        else:
            integration = integration_or_none

        if integration.goal != Goals.LICENSE_GOAL:
            raise ValueError(
                f'Integration "{name}" does not have goal "{Goals.LICENSE_GOAL.name}".'
            )
        if integration.protocol != protocol:
            raise ValueError(
                f'Integration "{name}" does not use protocol "{protocol}".'
            )

        if integration.collection is not None:
            collection = integration.collection
            is_new = False
        else:
            collection, _ = create(  # type: ignore[unreachable]
                _db,
                Collection,
                integration_configuration=integration,
            )
            is_new = True

        return collection, is_new

    @classmethod
    def by_name(cls, _db: Session, name: str) -> Collection | None:
        """Find a Collection by name."""
        return _db.execute(
            select(Collection)
            .join(IntegrationConfiguration)
            .where(
                IntegrationConfiguration.name == name,
                IntegrationConfiguration.goal == Goals.LICENSE_GOAL,
            )
        ).scalar_one_or_none()

    @classmethod
    def by_protocol(
        cls, _db: Session, protocol: list[str] | str | None
    ) -> Query[Collection]:
        """Query collections that get their licenses through the given protocol.

        Collections marked for deletion are not included.

        NOTE: THIS FUNCTION IS DEPRECATED. ANY NEW CODE SHOULD USE
        `select_by_protocol` INSTEAD.

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
                .filter(Collection.marked_for_deletion == False)
            )
            if isinstance(protocol, str):
                qu = qu.filter(IntegrationConfiguration.protocol == protocol)
            else:
                qu = qu.filter(IntegrationConfiguration.protocol.in_(protocol))

        return qu

    @classmethod
    @inject
    def select_by_protocol(
        cls,
        protocol: str | type[CirculationApiType],
        *,
        registry: LicenseProvidersRegistry = Provide[
            "integration_registry.license_providers"
        ],
    ) -> Select:
        """Return a sqlalchemy select that queries for collections that get their licenses
        through the given protocol.

        Care is taken to make sure that the query looks for equivalent protocol names, so
        that collections using the same protocol but with different (or deprecated)
        protocol names are included.

        Collections marked for deletion are not included.

        :param protocol: Protocol to use. Either the protocol name as a string, or
          the protocol class itself (e.g. OverdriveAPI).
        """
        integration_query = registry.configurations_query(protocol)
        return (
            select(Collection)
            .join(integration_query.subquery())
            .where(Collection.marked_for_deletion == False)
            .order_by(Collection.id)
        )

    _CIRCULATION_API_CACHE_KEY = "_palace_collection_circulation_api_cache"

    def circulation_api(
        self,
        *,
        registry: LicenseProvidersRegistry = Provide[
            "integration_registry.license_providers"
        ],
    ) -> CirculationApiType:
        """
        Return the API object for this collection.

        The returned api object is cached for the session, as this function may be
        called repeatedly.

        We tie this cache to the session because the API objects that are created
        save references to the session, so they are only valid for the lifetime of
        the session.
        """
        # Import here to avoid circular import
        from palace.manager.service.integration_registry.base import LookupException

        session = Session.object_session(self)
        if self._CIRCULATION_API_CACHE_KEY not in session.info:
            session.info[self._CIRCULATION_API_CACHE_KEY] = {}
        cache = session.info[self._CIRCULATION_API_CACHE_KEY]
        if self.id not in cache:
            try:
                cache[self.id] = registry.from_collection(session, self)
            except LookupException:
                self.log.warning(
                    f"Collection '{self.name}' (id: {self.id}) has unknown protocol '{self.protocol}'. "
                    f"Cannot create circulation API."
                )
                raise
        return cache[self.id]  # type: ignore[no-any-return]

    @property
    def is_active(self) -> bool:
        """Return True if the collection is active, False otherwise."""
        active_query = self.active_collections_filter(sa_select=select(count())).where(
            Collection.id == self.id
        )
        _db = Session.object_session(self)
        count_ = _db.execute(active_query).scalar()
        return False if count_ is None else count_ > 0

    @classmethod
    def active_collections_filter(
        cls, *, sa_select: Select | None = None, today: datetime.date | None = None
    ) -> Select:
        """Filter to select from only collections that are considered active.

        A collection is considered active if it either:
            - has no activation/expiration settings; or
            - meets the criteria specified by the activation/expiration settings.

        :param sa_select: A SQLAlchemy Select object. Defaults to an empty Select.
        :param today: The date to use as the current date. Defaults to today.
        :return: A filtered SQLAlchemy Select object.
        """
        sa_select = sa_select if sa_select is not None else select()
        if today is None:
            today = datetime.date.today()
        return cls._filter_active_collections(
            sa_select=(sa_select.select_from(Collection).join(IntegrationConfiguration))
        )

    @staticmethod
    def _filter_active_collections(
        *, sa_select: Select, today: datetime.date | None = None
    ) -> Select:
        """Constrain to only active collections.

        A collection is considered active if it either:
            - has no activation/expiration settings; or
            - meets the criteria specified by the activation/expiration settings.

        :param sa_select: A SQLAlchemy Select object.
        :param today: The date to use as the current date. Defaults to today.
        :return: A filtered SQLAlchemy Select object.
        """
        if today is None:
            today = datetime.date.today()
        return sa_select.where(
            or_(
                not_(
                    IntegrationConfiguration.settings_dict.has_key(
                        "subscription_activation_date"
                    )
                ),
                IntegrationConfiguration.settings_dict[
                    "subscription_activation_date"
                ].astext.cast(Date)
                <= today,
            ),
            or_(
                not_(
                    IntegrationConfiguration.settings_dict.has_key(
                        "subscription_expiration_date"
                    )
                ),
                IntegrationConfiguration.settings_dict[
                    "subscription_expiration_date"
                ].astext.cast(Date)
                >= today,
            ),
        )

    @property
    def active_libraries(self) -> list[Library]:
        """Return a list of libraries that are active for this collection.

        Active means either that there is no subscription activation/expiration
        criteria set, or that the criteria specified are satisfied.
        """
        library = aliased(Library, name="library")
        query = (
            self.active_collections_filter(sa_select=select(library))
            .join(IntegrationLibraryConfiguration)
            .join(library)
            .where(Collection.id == self.id)
        )
        _db = Session.object_session(self)
        return [row.library for row in _db.execute(query)]

    @property
    def name(self) -> str:
        """What is the name of this collection?"""
        if self.integration_configuration is None:
            raise ValueError("Collection has no integration configuration.")
        name = self.integration_configuration.name
        if not name:
            raise ValueError("Collection has no name.")
        return name

    @property
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
        self, library: Library | None, medium: str = EditionConstants.BOOK_MEDIUM
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
        library: Library | None,
        medium: str = EditionConstants.BOOK_MEDIUM,
    ) -> int | None:
        """Until we hear otherwise from the license provider, we assume
        that someone who borrows a non-open-access item from this
        collection has it for this number of days.
        """
        key = self.loan_period_key(medium)

        config = self.integration_configuration.for_library(library)
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

    @property
    def data_source(self) -> DataSource:
        """
        Find the data source associated with this Collection.
        """
        return self.circulation_api().data_source

    @property
    def pools_with_no_delivery_mechanisms(self) -> Query[LicensePool]:
        """Find all LicensePools in this Collection that have no delivery
        mechanisms whatsoever.

        :return: A query object.
        """
        _db = Session.object_session(self)
        qu = LicensePool.with_no_delivery_mechanisms(_db)
        return qu.filter(LicensePool.collection == self)

    def explain(self, include_secrets: bool = False) -> list[str]:
        """Create a series of human-readable strings to explain a collection's
        settings.

        :param include_secrets: For security reasons,
           sensitive settings such as passwords are not displayed by default.

        :return: A list of explanatory strings.
        """
        integration = self.integration_configuration
        lines = integration.explain(include_secrets=include_secrets)
        if self.parent:
            # Insert the parents name after the integration info but before the rest of the settings.
            lines.insert(3, f"Parent: {self.parent.name}")
        return lines

    @classmethod
    def restrict_to_ready_deliverable_works[T](
        cls,
        query: Query[T],
        collection_ids: list[int] | None = None,
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

        # Only find books with unsuppressed LicensePools.
        if not show_suppressed:
            query = query.filter(LicensePool.suppressed == False)

        # Only find books that are either:
        #  - Metered or equivalent type and active
        #     - At least one available license (if holds are not allowed)
        #  - Unlimited type and active
        #     - Open access are a subset of unlimited
        metered_filter = and_(  # type: ignore[type-var]
            LicensePool.active_status == true(),
            LicensePool.metered_or_equivalent_type == true(),
        )
        if not allow_holds:
            metered_filter = and_(  # type: ignore[assignment]
                metered_filter,
                LicensePool.licenses_available > 0,
            )

        unlimited_filter = and_(  # type: ignore[type-var]
            LicensePool.unlimited_type == true(),
            LicensePool.active_status == true(),
        )

        query = query.filter(
            or_(
                metered_filter,
                unlimited_filter,
            )
        )

        # Only find books in an appropriate collection.
        if collection_ids is not None:
            query = query.filter(LicensePool.collection_id.in_(collection_ids))

        return query

    @inject
    def delete(
        self, *, search_index: ExternalSearchIndex = Provide["search.index"]
    ) -> None:
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
        self.associated_libraries.clear()

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
                    work.delete(search_index=search_index)

            _db.delete(pool)

        # Now delete the Collection itself.
        _db.delete(self)
        _db.commit()


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


class CollectionMissing(BasePalaceException):
    """An operation was attempted that can only happen within the context
    of a Collection, but there was no Collection available.
    """


Base.registry.map_imperatively(
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
