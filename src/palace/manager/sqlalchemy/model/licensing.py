# PolicyException LicensePool, LicensePoolDeliveryMechanism, DeliveryMechanism,
# RightsStatus
from __future__ import annotations

import datetime
import logging
from collections.abc import Sequence
from enum import IntEnum, auto
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, overload

from frozendict import frozendict
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum as AlchemyEnum,
    ForeignKey,
    Index,
    Integer,
    String,
    Unicode,
    UniqueConstraint,
    and_,
    or_,
    select,
    true,
)
from sqlalchemy.orm import Mapped, lazyload, relationship
from sqlalchemy.orm.session import Session

from palace.manager.api.circulation.exceptions import CannotHold, CannotLoan
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.opds.odl.info import LicenseStatus
from palace.manager.sqlalchemy.constants import (
    DataSourceConstants,
    EditionConstants,
    MediaTypes,
)
from palace.manager.sqlalchemy.hassessioncache import HasSessionCache
from palace.manager.sqlalchemy.hybrid import hybrid_property
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.util import flush, get_one, get_one_or_create
from palace.manager.util.datetime_helpers import utc_now

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.collection import Collection
    from palace.manager.sqlalchemy.model.datasource import DataSource
    from palace.manager.sqlalchemy.model.edition import Edition
    from palace.manager.sqlalchemy.model.identifier import Identifier
    from palace.manager.sqlalchemy.model.resource import Resource
    from palace.manager.sqlalchemy.model.work import Work


class PolicyException(BasePalaceException):
    pass


class LicenseFunctions:
    identifier: str | None
    checkout_url: str | None
    status_url: str | None
    status: LicenseStatus | None
    expires: datetime.datetime | None
    checkouts_left: int | None
    checkouts_available: int | None
    terms_concurrency: int | None

    @property
    def is_perpetual(self) -> bool:
        return (self.expires is None) and (self.checkouts_left is None)

    @property
    def is_time_limited(self) -> bool:
        return self.expires is not None

    @property
    def is_loan_limited(self) -> bool:
        return self.checkouts_left is not None

    @property
    def is_inactive(self) -> bool:
        now = utc_now()
        return (
            (self.expires and self.expires <= now)
            or (self.checkouts_left is not None and self.checkouts_left <= 0)
            or (self.status != LicenseStatus.available)
        )

    @property
    def total_remaining_loans(self) -> int | None:
        if self.is_inactive:
            return 0
        elif self.is_loan_limited:
            if self.terms_concurrency is not None:
                # We need a type ignore here because mypy doesn't understand that `is_loan_limited`
                # implies `checkouts_left` is not None.
                return min(self.checkouts_left, self.terms_concurrency)  # type: ignore[type-var]
            return self.checkouts_left
        else:
            return self.terms_concurrency

    @property
    def currently_available_loans(self) -> int | None:
        if self.is_inactive:
            return 0
        else:
            return self.checkouts_available


class License(Base, LicenseFunctions):
    """A single license for a work from a given source.

    The fields on this license are based on the license information available in the
    License Info Document used in the ODL specification here:
    https://drafts.opds.io/odl-1.0.html#4-license-info-document

    This model may have to be changed, if other vendors are tracking individual
    licenses in the future.

    TODO: This currently assumes all licenses for a pool have the same delivery
          mechanisms, which may not always be true.
    """

    __tablename__ = "licenses"
    __mapper_args__ = {"confirm_deleted_rows": False}
    id: Mapped[int] = Column(Integer, primary_key=True)

    identifier = Column(Unicode)
    checkout_url = Column(Unicode)
    status_url = Column(Unicode)
    status = Column(AlchemyEnum(LicenseStatus))

    expires = Column(DateTime(timezone=True))

    # License info document checkouts.left field
    checkouts_left = Column(Integer)

    # License info document checkouts.available field
    checkouts_available = Column(Integer)

    # License info document terms.concurrency field
    terms_concurrency = Column(Integer)

    # A License belongs to one LicensePool.
    license_pool_id: Mapped[int] = Column(
        Integer, ForeignKey("licensepools.id"), index=True, nullable=False
    )
    license_pool: Mapped[LicensePool] = relationship(
        "LicensePool", back_populates="licenses"
    )

    # One License can have many Loans.
    loans: Mapped[list[Loan]] = relationship(
        "Loan", back_populates="license", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("identifier", "license_pool_id"),)

    @property
    def is_available_for_borrowing(self) -> bool:
        """Can this license currently be used to borrow a book?"""
        return (
            not self.is_inactive
            and self.checkouts_available is not None
            and self.checkouts_available > 0
        )

    def loan_to(
        self,
        patron: Patron,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
        fulfillment: LicensePoolDeliveryMechanism | None = None,
        external_identifier: str | None = None,
    ) -> tuple[Loan, bool]:
        loan, is_new = self.license_pool.loan_to(
            patron, start, end, fulfillment, external_identifier
        )
        loan.license = self
        return loan, is_new

    def checkout(self):
        """
        Update licenses internal accounting when a license is checked out.
        """
        if not self.is_inactive:
            if self.checkouts_left:
                self.checkouts_left -= 1
            if self.checkouts_available:
                self.checkouts_available -= 1
        else:
            logging.warning(f"Checking out expired license # {self.identifier}.")

    def checkin(self):
        """
        Update a licenses internal accounting when a license is checked in.
        """
        if not self.is_inactive:
            available = [self.checkouts_available + 1, self.terms_concurrency]
            if self.is_loan_limited:
                available.append(self.checkouts_left)
            self.checkouts_available = min(available)
        else:
            logging.warning(f"Checking in expired license # {self.identifier}.")


class LicensePool(Base):
    """A pool of undifferentiated licenses for a work from a given source."""

    UNLIMITED_ACCESS = -1

    __tablename__ = "licensepools"
    __mapper_args__ = {"confirm_deleted_rows": False}
    id: Mapped[int] = Column(Integer, primary_key=True)

    # A LicensePool may be associated with a Work. (If it's not, no one
    # can check it out.)
    work_id = Column(Integer, ForeignKey("works.id"), index=True)
    work: Mapped[Work | None] = relationship("Work", back_populates="license_pools")

    # Each LicensePool is associated with one DataSource and one
    # Identifier.
    data_source_id: Mapped[int] = Column(
        Integer, ForeignKey("datasources.id"), index=True, nullable=False
    )
    data_source: Mapped[DataSource] = relationship(
        "DataSource", back_populates="license_pools", lazy="joined"
    )

    identifier_id: Mapped[int] = Column(
        Integer, ForeignKey("identifiers.id"), index=True, nullable=False
    )
    identifier: Mapped[Identifier] = relationship(
        "Identifier", back_populates="licensed_through", lazy="joined"
    )

    # Each LicensePool belongs to one Collection.
    collection_id: Mapped[int] = Column(
        Integer, ForeignKey("collections.id"), index=True, nullable=False
    )

    collection: Mapped[Collection] = relationship(
        "Collection", back_populates="licensepools"
    )

    # Each LicensePool has an Edition which contains the metadata used
    # to describe this book.
    presentation_edition_id = Column(Integer, ForeignKey("editions.id"), index=True)
    presentation_edition: Mapped[Edition | None] = relationship(
        "Edition", back_populates="is_presentation_for"
    )

    # If the source provides information about individual licenses, the
    # LicensePool may have many Licenses.
    licenses: Mapped[list[License]] = relationship(
        "License",
        back_populates="license_pool",
        cascade="all, delete-orphan",
        uselist=True,
    )

    # One LicensePool can have many Loans.
    loans: Mapped[list[Loan]] = relationship(
        "Loan", back_populates="license_pool", cascade="all, delete-orphan"
    )

    # One LicensePool can have many Holds.
    holds: Mapped[list[Hold]] = relationship(
        "Hold", back_populates="license_pool", cascade="all, delete-orphan"
    )

    # One LicensePool can have many CirculationEvents
    circulation_events: Mapped[list[CirculationEvent]] = relationship(
        "CirculationEvent", back_populates="license_pool", cascade="all, delete-orphan"
    )

    # The date this LicensePool was first created in our db
    # (the date we first discovered that ​we had that book in ​our collection).
    availability_time = Column(DateTime(timezone=True), index=True)

    # A LicensePool that seemingly looks fine may be manually suppressed
    # to be temporarily or permanently removed from the collection.
    suppressed: Mapped[bool] = Column(
        Boolean, default=False, index=True, nullable=False
    )

    # A textual description of a problem with this license pool
    # that caused us to suppress it.
    license_exception = Column(Unicode, index=True)

    open_access = Column(Boolean, index=True)
    last_checked = Column(DateTime(timezone=True), index=True)
    licenses_owned: Mapped[int] = Column(Integer, default=0, index=True, nullable=False)
    licenses_available: Mapped[int] = Column(
        Integer, default=0, index=True, nullable=False
    )
    licenses_reserved: Mapped[int] = Column(Integer, default=0, nullable=False)
    patrons_in_hold_queue: Mapped[int] = Column(Integer, default=0, nullable=False)
    should_track_playtime: Mapped[bool] = Column(Boolean, default=False, nullable=False)

    # A Collection can not have more than one LicensePool for a given
    # Identifier from a given DataSource.
    __table_args__ = (
        UniqueConstraint("identifier_id", "data_source_id", "collection_id"),
        # This index was added to speed up queries for generating MARC XML.
        Index("ix_licensepools_collection_id_work_id", collection_id, work_id),
    )

    # The available delivery mechanisms for this LicensePool. This is a read-only
    # relationship -- to add or remove delivery mechanisms you should call the
    # LicensePoolDeliveryMechanism.set() method.
    available_delivery_mechanisms: Mapped[list[LicensePoolDeliveryMechanism]] = (
        relationship(
            "LicensePoolDeliveryMechanism",
            primaryjoin=(
                "and_(LicensePool.data_source_id == LicensePoolDeliveryMechanism.data_source_id,"
                "LicensePool.identifier_id == LicensePoolDeliveryMechanism.identifier_id,"
                "LicensePoolDeliveryMechanism.available == true())"
            ),
            foreign_keys=(data_source_id, identifier_id),
            uselist=True,
            viewonly=True,
        )
    )

    # The delivery mechanisms for this LicensePool. This is also a read-only relationship.
    delivery_mechanisms: Mapped[list[LicensePoolDeliveryMechanism]] = relationship(
        "LicensePoolDeliveryMechanism",
        primaryjoin=(
            "and_(LicensePool.data_source_id == LicensePoolDeliveryMechanism.data_source_id,"
            "LicensePool.identifier_id == LicensePoolDeliveryMechanism.identifier_id)"
        ),
        foreign_keys=(data_source_id, identifier_id),
        uselist=True,
        viewonly=True,
    )

    def __repr__(self):
        if self.identifier:
            identifier = "{}/{}".format(
                self.identifier.type, self.identifier.identifier
            )
        else:
            identifier = "unknown identifier"
        return (
            "<LicensePool #%s for %s: owned=%d available=%d reserved=%d holds=%d>"
            % (
                self.id,
                identifier,
                self.licenses_owned,
                self.licenses_available,
                self.licenses_reserved,
                self.patrons_in_hold_queue,
            )
        )

    @hybrid_property
    def unlimited_access(self) -> bool:
        """Returns a Boolean value indicating whether this LicensePool allows unlimited access.
        For example, in the case of LCP books without explicit licensing information

        :return: Boolean value indicating whether this LicensePool allows unlimited access
        """
        return self.licenses_owned == self.UNLIMITED_ACCESS

    @unlimited_access.setter
    def unlimited_access(self, value: bool):
        """Sets value of unlimited_access property.
        If you set it to False, license_owned and license_available will be reset to 0

        :param value: Boolean value indicating whether this LicensePool allows unlimited access
        """
        if value:
            self.licenses_owned = self.UNLIMITED_ACCESS
            self.licenses_available = self.UNLIMITED_ACCESS
        else:
            self.licenses_owned = 0
            self.licenses_available = 0

    @classmethod
    @overload
    def for_foreign_id(
        self,
        _db: Session,
        data_source: DataSource | str,
        foreign_id_type: str,
        foreign_id: str,
        rights_status: str | None = ...,
        collection: Collection | None = ...,
        autocreate: Literal[True] = ...,
    ) -> tuple[LicensePool, bool]: ...

    @classmethod
    @overload
    def for_foreign_id(
        self,
        _db: Session,
        data_source: DataSource | str,
        foreign_id_type: str,
        foreign_id: str,
        rights_status: str | None = ...,
        collection: Collection | None = ...,
        autocreate: bool = ...,
    ) -> tuple[LicensePool | None, bool]: ...

    @classmethod
    def for_foreign_id(
        self,
        _db: Session,
        data_source: DataSource | str,
        foreign_id_type: str,
        foreign_id: str,
        rights_status: str | None = None,
        collection: Collection | None = None,
        autocreate: bool = True,
    ) -> tuple[LicensePool | None, bool]:
        """Find or create a LicensePool for the given foreign ID."""
        from palace.manager.sqlalchemy.model.collection import CollectionMissing
        from palace.manager.sqlalchemy.model.datasource import DataSource
        from palace.manager.sqlalchemy.model.identifier import Identifier

        if not collection:
            raise CollectionMissing()

        # Get the DataSource.
        if isinstance(data_source, str):
            data_source_or_none = DataSource.lookup(
                _db, data_source, autocreate=autocreate
            )
            if data_source_or_none is None:
                return None, False
            data_source = data_source_or_none

        # The type of the foreign ID must be the primary identifier
        # type for the data source.
        if (
            data_source.primary_identifier_type is not None
            and Identifier.get_active_type(foreign_id_type)
            != Identifier.get_active_type(data_source.primary_identifier_type)
        ):
            raise ValueError(
                "License pools for data source '%s' are keyed to "
                "identifier type '%s' (not '%s', which was provided)"
                % (
                    data_source.name,
                    data_source.primary_identifier_type,
                    foreign_id_type,
                )
            )

        # Get the Identifier.
        identifier, ignore = Identifier.for_foreign_id(
            _db, foreign_id_type, foreign_id, autocreate=autocreate
        )
        if identifier is None:
            return None, False

        kw: dict[str, Any] = dict(
            data_source=data_source, identifier=identifier, collection=collection
        )
        if rights_status:
            kw["rights_status"] = rights_status

        # Get the LicensePool that corresponds to the
        # DataSource/Identifier/Collection.
        if autocreate:
            license_pool, was_new = get_one_or_create(_db, LicensePool, **kw)

            if was_new:
                if not license_pool.availability_time:
                    now = utc_now()
                    license_pool.availability_time = now

                # Set the LicensePool's initial values to indicate
                # that we don't actually know how many copies we own.
                license_pool.licenses_owned = 0
                license_pool.licenses_available = 0
                license_pool.licenses_reserved = 0
                license_pool.patrons_in_hold_queue = 0

            return license_pool, was_new
        else:
            return get_one(_db, LicensePool, **kw), False

    @classmethod
    def with_no_work(cls, _db):
        """Find LicensePools that have no corresponding Work."""
        from palace.manager.sqlalchemy.model.work import Work

        return _db.query(LicensePool).outerjoin(Work).filter(Work.id == None).all()

    @classmethod
    def with_no_delivery_mechanisms(cls, _db):
        """Find LicensePools that have no delivery mechanisms.

        :return: A query object.
        """
        return (
            _db.query(LicensePool)
            .outerjoin(LicensePool.available_delivery_mechanisms)
            .filter(LicensePoolDeliveryMechanism.id == None)
        )

    @property
    def deliverable(self):
        """This LicensePool can actually be delivered to patrons."""
        return (self.open_access or self.licenses_owned > 0) and any(
            [
                dm.delivery_mechanism.default_client_can_fulfill
                for dm in self.available_delivery_mechanisms
            ]
        )

    def set_open_access_status(self):
        """Set .open_access based on whether there is currently
        an open-access LicensePoolDeliveryMechanism for this LicensePool.
        """
        old_status = self.open_access
        for dm in self.available_delivery_mechanisms:
            if dm.is_open_access:
                self.open_access = True
                break
        else:
            self.open_access = False

    def set_presentation_edition(self, equivalent_editions=None):
        """Create or update the presentation Edition for this LicensePool.
        The presentation Edition is made of metadata from all Editions
        associated with the LicensePool's identifier.
        :param equivalent_editions: An optional list of Edition objects
        that don't share this LicensePool's identifier but are associated
        with its equivalent identifiers in some way. This option is used
        to create Works on the Metadata Wrangler.
        :return: A boolean explaining whether any of the presentation
        information associated with this LicensePool actually changed.
        """
        from palace.manager.sqlalchemy.model.edition import Edition

        _db = Session.object_session(self)
        old_presentation_edition = self.presentation_edition
        changed = False

        editions = equivalent_editions
        if not editions:
            editions = self.identifier.primarily_identifies
        all_editions = list(Edition.sort_by_priority(editions, self.data_source))

        # Note: We can do a cleaner solution, if we refactor to not use metadata's
        # methods to update editions.  For now, we're choosing to go with the below approach.
        from palace.manager.data_layer.bibliographic import BibliographicData
        from palace.manager.data_layer.identifier import IdentifierData
        from palace.manager.data_layer.policy.replacement import ReplacementPolicy

        if len(all_editions) == 1:
            # There's only one edition associated with this
            # LicensePool. Use it as the presentation edition rather
            # than creating an identical composite.
            self.presentation_edition = all_editions[0]
        else:
            edition_identifier = IdentifierData.from_identifier(self.identifier)
            bibliographic = BibliographicData(
                data_source_name=DataSourceConstants.PRESENTATION_EDITION,
                primary_identifier_data=edition_identifier,
            )

            for edition in all_editions:
                if edition.data_source.name != DataSourceConstants.PRESENTATION_EDITION:
                    bibliographic.update(BibliographicData.from_edition(edition))

            edition, is_new = bibliographic.edition(_db)

            policy = ReplacementPolicy.from_metadata_source()
            self.presentation_edition, edition_core_changed = (
                bibliographic.apply_edition_only(
                    _db,
                    edition,
                    replace=policy,
                )
            )
            changed = changed or edition_core_changed

        presentation_changed = self.presentation_edition.calculate_presentation()
        changed = changed or presentation_changed

        # if the license pool is associated with a work, and the work currently has no presentation edition,
        # then do a courtesy call to the work, and tell it about the presentation edition.
        if self.work and not self.work.presentation_edition:
            self.work.set_presentation_edition(self.presentation_edition)

        return self.presentation_edition != old_presentation_edition or changed

    def add_link(
        self,
        rel,
        href,
        data_source,
        media_type=None,
        content=None,
        content_path=None,
        rights_status_uri=None,
        rights_explanation=None,
        original_resource=None,
        transformation_settings=None,
        db=None,
    ):
        """Add a link between this LicensePool and a Resource.

        :param rel: The relationship between this LicensePool and the resource
            on the other end of the link.
        :param href: The URI of the resource on the other end of the link.
        :param media_type: Media type of the representation associated
            with the resource.
        :param content: Content of the representation associated with the
            resource.
        :param content_path: Path (relative to DATA_DIRECTORY) of the
            representation associated with the resource.
        :param rights_status_uri: The URI of the RightsStatus for this resource.
        :param rights_explanation: A free text explanation of why the RightsStatus
            applies.
        :param original_resource: Another resource that this resource was derived from.
        :param transformation_settings: The settings used to transform the original
            resource into this resource.
        """
        return self.identifier.add_link(
            rel,
            href,
            data_source,
            media_type,
            content,
            content_path,
            rights_status_uri,
            rights_explanation,
            original_resource,
            transformation_settings,
            db,
        )

    def update_availability_from_licenses(
        self,
        as_of: datetime.datetime | None = None,
        ignored_holds: set[Hold] | None = None,
    ):
        """
        Update the LicensePool with new availability information, based on the
        licenses and holds that are associated with it.
        """
        _db = Session.object_session(self)

        licenses_owned = sum(
            l.total_remaining_loans
            for l in self.licenses
            if l.total_remaining_loans is not None
        )
        licenses_available = sum(
            l.currently_available_loans
            for l in self.licenses
            if l.currently_available_loans is not None
        )

        ignored_holds_ids = {h.id for h in (ignored_holds or set())}
        active_holds_ids = {h.id for h in self.get_active_holds()}
        patrons_in_hold_queue = len(active_holds_ids - ignored_holds_ids)
        if patrons_in_hold_queue > licenses_available:
            licenses_reserved = licenses_available
            licenses_available = 0
        else:
            licenses_reserved = patrons_in_hold_queue
            licenses_available -= licenses_reserved

        return self.update_availability(
            licenses_owned,
            licenses_available,
            licenses_reserved,
            patrons_in_hold_queue,
            as_of=as_of,
        )

    def get_active_holds(self, for_update: bool = False) -> list[Hold]:
        _db = Session.object_session(self)
        query = (
            select(Hold)
            .where(Hold.license_pool_id == self.id)
            .where(
                or_(
                    Hold.position != 0,
                    Hold.position == None,
                    and_(Hold.end > utc_now(), Hold.position == 0),
                )
            )
            .order_by(Hold.start)
        )

        if for_update:
            query = query.options(lazyload(Hold.patron)).with_for_update()

        return _db.execute(query).scalars().all()

    def update_availability(
        self,
        new_licenses_owned,
        new_licenses_available,
        new_licenses_reserved,
        new_patrons_in_hold_queue,
        as_of=None,
    ):
        """Update the LicensePool with new availability information.
        Log the implied changes with the analytics provider.
        """
        changes_made = False
        if not as_of:
            as_of = utc_now()
        elif as_of == CirculationEvent.NO_DATE:
            # The caller explicitly does not want
            # LicensePool.last_checked to be updated.
            as_of = None

        old_licenses_owned = self.licenses_owned
        old_licenses_available = self.licenses_available
        old_licenses_reserved = self.licenses_reserved
        old_patrons_in_hold_queue = self.patrons_in_hold_queue

        for old_value, new_value, more_event, fewer_event in (
            [
                self.patrons_in_hold_queue,
                new_patrons_in_hold_queue,
                CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
                CirculationEvent.DISTRIBUTOR_HOLD_RELEASE,
            ],
            [
                self.licenses_available,
                new_licenses_available,
                CirculationEvent.DISTRIBUTOR_CHECKIN,
                CirculationEvent.DISTRIBUTOR_CHECKOUT,
            ],
            [
                self.licenses_reserved,
                new_licenses_reserved,
                CirculationEvent.DISTRIBUTOR_AVAILABILITY_NOTIFY,
                None,
            ],
            [
                self.licenses_owned,
                new_licenses_owned,
                CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
                CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE,
            ],
        ):
            if new_value is None:
                continue
            if old_value == new_value:
                continue
            changes_made = True

        # Update the license pool with the latest information.
        any_data = False
        if new_licenses_owned is not None:
            self.licenses_owned = new_licenses_owned
            any_data = True
        if new_licenses_available is not None:
            self.licenses_available = new_licenses_available
            any_data = True
        if new_licenses_reserved is not None:
            self.licenses_reserved = new_licenses_reserved
            any_data = True
        if new_patrons_in_hold_queue is not None:
            self.patrons_in_hold_queue = new_patrons_in_hold_queue
            any_data = True

        if as_of and (any_data or changes_made):
            # Sometimes update_availability is called with no actual
            # numbers, but that's not the case this time. We got
            # numbers and they may have even changed our view of the
            # LicensePool.
            self.last_checked = as_of
            if self.work:
                self.work.last_update_time = as_of

        if changes_made:
            message, args = self.circulation_changelog(
                old_licenses_owned,
                old_licenses_available,
                old_licenses_reserved,
                old_patrons_in_hold_queue,
            )
            logging.info(message, *args)

        return changes_made

    def update_availability_from_delta(self, event_type, event_date, delta):
        """Call update_availability based on a single change seen in the
        distributor data, rather than a complete snapshot of
        distributor information as of a certain time.
        This information is unlikely to be completely accurate, but it
        should suffice until more accurate information can be
        obtained.
        No CirculationEvent is created until `update_availability` is
        called.
        Events must be processed in chronological order. Any event
        that happened than `LicensePool.last_checked` is ignored, and
        calling this method will update `LicensePool.last_checked` to
        the time of the event.
        :param event_type: A CirculationEvent constant representing the
        type of change that was seen.
        :param event_date: A datetime corresponding to when the
        change was seen.
        :param delta: The magnitude of the change that was seen.
        """
        ignore = False
        if (
            event_date != CirculationEvent.NO_DATE
            and self.last_checked
            and event_date < self.last_checked
        ):
            # This is an old event and its effect on availability has
            # already been taken into account.
            ignore = True

        elif self.last_checked and event_date == CirculationEvent.NO_DATE:
            # We have a history for this LicensePool and we don't know
            # where this event fits into that history. Ignore the
            # event.
            ignore = True

        if not ignore:
            (
                new_licenses_owned,
                new_licenses_available,
                new_licenses_reserved,
                new_patrons_in_hold_queue,
            ) = self._calculate_change_from_one_event(event_type, delta)

            changes_made = self.update_availability(
                new_licenses_owned,
                new_licenses_available,
                new_licenses_reserved,
                new_patrons_in_hold_queue,
                as_of=event_date,
            )

    def _calculate_change_from_one_event(self, type, delta):
        new_licenses_owned = self.licenses_owned
        new_licenses_available = self.licenses_available
        new_licenses_reserved = self.licenses_reserved
        new_patrons_in_hold_queue = self.patrons_in_hold_queue

        def deduct(value):
            # It's impossible for any of these numbers to be
            # negative.
            return max(value - delta, 0)

        CE = CirculationEvent
        added = False
        if type == CE.DISTRIBUTOR_HOLD_PLACE:
            new_patrons_in_hold_queue += delta
            if new_licenses_available:
                # If someone has put a book on hold, it must not be
                # immediately available.
                new_licenses_available = 0
        elif type == CE.DISTRIBUTOR_HOLD_RELEASE:
            new_patrons_in_hold_queue = deduct(new_patrons_in_hold_queue)
        elif type == CE.DISTRIBUTOR_CHECKIN:
            if self.patrons_in_hold_queue == 0:
                new_licenses_available += delta
            else:
                # When there are patrons in the hold queue, checking
                # in a single book does not make new licenses
                # available.  Checking in more books than there are
                # patrons in the hold queue _does_ make books
                # available.  However, in neither case do patrons
                # leave the hold queue. That will happen in the near
                # future as DISTRIBUTOR_AVAILABILITY_NOTIFICATION events
                # are sent out.
                if delta > new_patrons_in_hold_queue:
                    new_licenses_available += delta - new_patrons_in_hold_queue
        elif type == CE.DISTRIBUTOR_CHECKOUT:
            if new_licenses_available == 0:
                # The only way to borrow books while there are no
                # licenses available is to borrow reserved copies.
                new_licenses_reserved = deduct(new_licenses_reserved)
            else:
                # We don't know whether this checkout came from
                # licenses available or from a lingering reserved
                # copy, but in most cases it came from licenses
                # available.
                new_licenses_available = deduct(new_licenses_available)
        elif type == CE.DISTRIBUTOR_LICENSE_ADD:
            new_licenses_owned += delta
            # Newly added licenses start out as available, unless there
            # are patrons in the holds queue.
            if new_patrons_in_hold_queue == 0:
                new_licenses_available += delta
        elif type == CE.DISTRIBUTOR_LICENSE_REMOVE:
            new_licenses_owned = deduct(new_licenses_owned)
            # We can't say whether or not the removed licenses should
            # be deducted from the list of available licenses, because they
            # might already be checked out.
        elif type == CE.DISTRIBUTOR_AVAILABILITY_NOTIFY:
            new_patrons_in_hold_queue = deduct(new_patrons_in_hold_queue)
            new_licenses_reserved += delta
        if new_licenses_owned < new_licenses_available:
            # It's impossible to have more licenses available than
            # owned. We don't know whether this means there are some
            # extra licenses we never heard about, or whether some
            # licenses expired without us being notified, but the
            # latter is more likely.
            new_licenses_available = new_licenses_owned

        return (
            new_licenses_owned,
            new_licenses_available,
            new_licenses_reserved,
            new_patrons_in_hold_queue,
        )

    def circulation_changelog(
        self,
        old_licenses_owned,
        old_licenses_available,
        old_licenses_reserved,
        old_patrons_in_hold_queue,
    ):
        """Generate a log message describing a change to the circulation.
        :return: a 2-tuple (message, args) suitable for passing into
        logging.info or a similar method
        """
        edition = self.presentation_edition
        message = "CHANGED "
        args = []
        if self.identifier:
            identifier_template = "%s/%s"
            identifier_args = [self.identifier.type, self.identifier.identifier]
        else:
            identifier_template = "%s"
            identifier_args = [self.identifier]
        if edition:
            message += '%s "%s" %s (' + identifier_template + ")"
            args.extend(
                [
                    edition.medium,
                    edition.title or "[NO TITLE]",
                    edition.author or "[NO AUTHOR]",
                ]
            )
            args.extend(identifier_args)
        else:
            message += identifier_template
            args.extend(identifier_args)

        def _part(message, args, string, old_value, new_value):
            if old_value != new_value:
                args.extend([string, old_value, new_value])
                message += " %s: %s=>%s"
            return message, args

        message, args = _part(
            message, args, "OWN", old_licenses_owned, self.licenses_owned
        )

        message, args = _part(
            message, args, "AVAIL", old_licenses_available, self.licenses_available
        )

        message, args = _part(
            message, args, "RSRV", old_licenses_reserved, self.licenses_reserved
        )

        message, args = _part(
            message, args, "HOLD", old_patrons_in_hold_queue, self.patrons_in_hold_queue
        )
        return message, tuple(args)

    def loan_to(
        self,
        patron: Patron,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
        fulfillment: LicensePoolDeliveryMechanism | None = None,
        external_identifier: str | None = None,
    ) -> tuple[Loan, bool]:
        _db = Session.object_session(patron)
        kwargs = dict(start=start or utc_now(), end=end)

        # We can make new loans on active collections, but not on inactive ones.
        # But we can look up already-existing loans on inactive collections.
        if self.collection.is_active:
            loan, is_new = get_one_or_create(
                _db,
                Loan,
                patron=patron,
                license_pool=self,
                create_method_kwargs=kwargs,
            )
        else:
            _loan: Loan | None = get_one(
                _db,
                Loan,
                patron=patron,
                license_pool=self,
            )
            if not _loan:
                raise CannotLoan("Cannot create a new loan on an inactive collection.")
            loan = _loan
            is_new = False

        if fulfillment:
            loan.fulfillment = fulfillment
        if external_identifier:
            loan.external_identifier = external_identifier
        if start:
            loan.start = start
        if end:
            loan.end = end
        return loan, is_new

    def on_hold_to(
        self,
        patron: Patron,
        start=None,
        end=None,
        position=None,
    ):
        _db = Session.object_session(patron)
        if not patron.library.settings.allow_holds:
            raise PolicyException("Holds are disabled for this library.")
        start = start or utc_now()

        # We can create new holds on active collections, but not on inactive ones.
        # But we can look up already-existing holds in inactive collections.
        if self.collection.is_active:
            hold, new = get_one_or_create(_db, Hold, patron=patron, license_pool=self)
            hold.update(start, end, position)
        else:
            _hold = get_one(_db, Hold, patron=patron, license_pool=self)
            if not _hold:
                raise CannotHold("Cannot create a new hold on an inactive collection.")
            hold = _hold
            new = False
        return hold, new

    class _LicensePriority(IntEnum):
        TIME_LIMITED = auto()
        PERPETUAL = auto()
        TIME_AND_LOAN_LIMITED = auto()
        LOAN_LIMITED = auto()

    @staticmethod
    def _time_limited_sort_key(license_: License) -> int:
        if license_.expires is None:
            return 0
        return int(license_.expires.timestamp())

    @staticmethod
    def _loan_limited_sort_key(license_: License) -> int:
        return (license_.checkouts_left or 0) * -1

    @classmethod
    def _license_sort_func(cls, license_: License) -> tuple[_LicensePriority, int, int]:
        time_limited_key = cls._time_limited_sort_key(license_)
        loan_limited_key = cls._loan_limited_sort_key(license_)

        if license_.is_time_limited and license_.is_loan_limited:
            return (
                cls._LicensePriority.TIME_AND_LOAN_LIMITED,
                time_limited_key,
                loan_limited_key,
            )

        if license_.is_time_limited:
            return cls._LicensePriority.TIME_LIMITED, time_limited_key, loan_limited_key

        if license_.is_loan_limited:
            return cls._LicensePriority.LOAN_LIMITED, time_limited_key, loan_limited_key

        return cls._LicensePriority.PERPETUAL, time_limited_key, loan_limited_key

    def best_available_licenses(self) -> list[License]:
        """
        Determine the next license that should be lent out from this pool.

        This function returns a list of licenses that are available for lending, sorted
        by priority. The highest priority license (the one that the next loan should be made from)
        is the first one in the list.

        Time-limited licenses and perpetual licenses are the best. It doesn't matter which
        is used first, unless a time-limited license would expire within the loan period, in
        which case it's better to loan the time-limited license so the perpetual one remains
        available.

        We handle this by always loaning the time-limited one first, followed by the perpetual
        one. If there is more than one time-limited license, it's better to use the one
        expiring soonest.

        If no time-limited or perpetual licenses are available, the next best is a loan-limited
        license. If a license is both time-limited and loan-limited, it's better to use it before
        a license that is only loan-limited. We should choose the license with the most remaining
        loans, so that we'll maximize the number of concurrent checkouts available in the future.
        """
        return sorted(
            (l for l in self.licenses if l.is_available_for_borrowing),
            key=self._license_sort_func,
        )

    @classmethod
    def consolidate_works(cls, _db, batch_size=10):
        """Assign a (possibly new) Work to every unassigned LicensePool."""
        a = 0
        lps = cls.with_no_work(_db)
        logging.info("Assigning Works to %d LicensePools with no Work.", len(lps))
        for unassigned in lps:
            etext, new = unassigned.calculate_work()
            if not etext:
                # We could not create a work for this LicensePool,
                # most likely because it does not yet have any
                # associated Edition.
                continue
            a += 1
            logging.info("When consolidating works, created %r", etext)
            if a and not a % batch_size:
                _db.commit()
        _db.commit()

    def calculate_work(
        self, known_edition=None, exclude_search=False, even_if_no_title=False
    ):
        """Find or create a Work for this LicensePool.
        A pool that is not open-access will always have its own
        Work. Open-access LicensePools will be grouped together with
        other open-access LicensePools based on the permanent work ID
        of the LicensePool's presentation edition.
        :param even_if_no_title: Ordinarily this method will refuse to
        create a Work for a LicensePool whose Edition has no title.
        However, in components that don't present information directly
        to readers, it's sometimes useful to create a Work even if the
        title is unknown. In that case, pass in even_if_no_title=True
        and the Work will be created.
        TODO: I think known_edition is mostly useless. We should
        either remove it or replace it with a boolean that stops us
        from calling set_presentation_edition() and assumes we've
        already done that work.
        """
        from palace.manager.sqlalchemy.model.work import Work

        if known_edition:
            presentation_edition = known_edition
        else:
            self.set_presentation_edition()
            presentation_edition = self.presentation_edition
        if presentation_edition:
            if self not in presentation_edition.is_presentation_for:
                raise ValueError(
                    "Alleged presentation edition is not the presentation edition for the license pool for which work is being calculated!"
                )

        logging.info("Calculating work for %r", presentation_edition)
        if not presentation_edition:
            # We don't have any information about the identifier
            # associated with this LicensePool, so we can't create a work.
            logging.warning(
                "NO EDITION for %s, cowardly refusing to create work.", self.identifier
            )

            # If there was a work associated with this LicensePool,
            # it was by mistake. Remove it.
            self.work = None
            return None, False

        if not presentation_edition.title or not presentation_edition.author:
            presentation_edition.calculate_presentation()

        if not presentation_edition.title and not even_if_no_title:
            if presentation_edition.work:
                logging.warning(
                    "Edition %r has no title but has a Work assigned. This will not stand.",
                    presentation_edition,
                )
            else:
                logging.info(
                    "Edition %r has no title and it will not get a Work.",
                    presentation_edition,
                )
            self.work = None
            self.work_id = None
            return None, False

        presentation_edition.calculate_permanent_work_id()

        _db = Session.object_session(self)
        work = None
        is_new = False
        licensepools_changed = False
        if self.open_access and presentation_edition.permanent_work_id:
            # This is an open-access book. Use the Work for all
            # open-access books associated with this book's permanent
            # work ID.
            #
            # If the dataset is in an inconsistent state, calling
            # Work.open_access_for_permanent_work_id may result in works being
            # merged.
            work, is_new = Work.open_access_for_permanent_work_id(
                _db,
                presentation_edition.permanent_work_id,
                presentation_edition.medium,
                presentation_edition.language,
            )

            # Run a sanity check to make sure every LicensePool
            # associated with this Work actually belongs there. This
            # may result in new Works being created.
            #
            # This could go into Work.for_permanent_work_id, but that
            # could conceivably lead to an infinite loop, or at least
            # a very long recursive call, so I've put it here.
            work.make_exclusive_open_access_for_permanent_work_id(
                presentation_edition.permanent_work_id,
                presentation_edition.medium,
                presentation_edition.language,
            )
            self.work = work
            licensepools_changed = True

        # All LicensePools with a given Identifier must share a work.
        existing_works = {x.work for x in self.identifier.licensed_through}
        if len(existing_works) > 1:
            logging.warning(
                "LicensePools for %r have more than one Work between them. Removing them all and starting over.",
                self.identifier,
            )
            for lp in self.identifier.licensed_through:
                lp.work = None
                if lp.presentation_edition:
                    lp.presentation_edition.work = None
        else:
            # There is a consensus Work for this Identifier.
            [self.work] = existing_works

        if self.work:
            # This pool is already associated with a Work. Use that
            # Work.
            work = self.work
        elif presentation_edition.work:
            # This pool's presentation edition is already associated with
            # a Work. Use that Work.
            work = presentation_edition.work
            self.work = work

        if work:
            # There is already a Work associated with this LicensePool,
            # but we need to run a sanity check because occasionally
            # LicensePools get mis-grouped due to bugs.
            #
            # A commercially-licensed book should have a Work to
            # itself. All other LicensePools need to be kicked out and
            # associated with some other work.
            #
            # This won't cause an infinite recursion because we're
            # setting pool.work to None before calling
            # pool.calculate_work(), and the recursive call only
            # happens if self.work is set.
            for pool in list(work.license_pools):
                if pool is self:
                    continue
                if not (self.open_access and pool.open_access):
                    pool.work = None
                    pool.calculate_work(
                        exclude_search=exclude_search, even_if_no_title=even_if_no_title
                    )
                    licensepools_changed = True

        else:
            # There is no better choice than creating a brand new Work.
            is_new = True
            logging.info("Creating a new work for %r" % presentation_edition.title)
            work = Work()
            _db = Session.object_session(self)
            _db.add(work)
            flush(_db)
            licensepools_changed = True

        # Associate this LicensePool and its Edition with the work we
        # chose or created.
        if not self in work.license_pools:
            work.license_pools.append(self)
            licensepools_changed = True

        # Recalculate the display information for the Work. Either the
        # associated LicensePools have changed, which may have caused
        # the Work's presentation Edition to change, or
        # the caller has reason to believe that the presentation Edition
        # is changing for some other reason.
        work.calculate_presentation(exclude_search=exclude_search)

        # Ensure that all LicensePools with this Identifier share
        # the same Work. (We may have wiped out their .work earlier
        # in this method.)
        for lp in self.identifier.licensed_through:
            lp.work = work

        if is_new:
            logging.info("Created a new work: %r", work)

        # All done!
        return work, is_new

    def set_delivery_mechanism(
        self,
        content_type: str | None,
        drm_scheme: str | None,
        rights_uri: str | None,
        resource: Resource | None = None,
        available: bool | None = None,
        update_available: bool = True,
        db: Session | None = None,
    ) -> LicensePoolDeliveryMechanism:
        """Ensure that this LicensePool (and any other LicensePools for the same
        book) have a LicensePoolDeliveryMechanism for this media type,
        DRM scheme, rights status, and resource.
        """
        return LicensePoolDeliveryMechanism.set(
            self.data_source,
            self.identifier,
            content_type,
            drm_scheme,
            rights_uri,
            resource,
            available,
            update_available,
            db,
        )

    @property
    def sorted_available_delivery_mechanisms(
        self,
    ) -> list[LicensePoolDeliveryMechanism]:
        """
        Sort available_delivery_mechanisms based on the sorting algorithm defined in
        DeliveryMechanism.sort().
        """
        return DeliveryMechanism.sort(self.available_delivery_mechanisms)


Index(
    "ix_licensepools_data_source_id_identifier_id_collection_id",
    LicensePool.collection_id,
    LicensePool.data_source_id,
    LicensePool.identifier_id,
    unique=True,
)


class LicensePoolDeliveryMechanism(Base):
    """A mechanism for delivering a specific book from a specific
    distributor.
    It's presumed that all LicensePools for a given DataSource and
    Identifier have the same set of LicensePoolDeliveryMechanisms.
    This is mostly an association class between DataSource, Identifier and
    DeliveryMechanism, but it also may incorporate a specific Resource
    (i.e. a static link to a downloadable file) which explains exactly
    where to go for delivery.
    """

    __tablename__ = "licensepooldeliveries"

    id: Mapped[int] = Column(Integer, primary_key=True)

    data_source_id: Mapped[int] = Column(
        Integer, ForeignKey("datasources.id"), index=True, nullable=False
    )
    data_source: Mapped[DataSource] = relationship(
        "DataSource", back_populates="delivery_mechanisms"
    )

    identifier_id: Mapped[int] = Column(
        Integer, ForeignKey("identifiers.id"), index=True, nullable=False
    )
    identifier: Mapped[Identifier] = relationship(
        "Identifier", back_populates="delivery_mechanisms"
    )

    delivery_mechanism_id: Mapped[int] = Column(
        Integer, ForeignKey("deliverymechanisms.id"), index=True, nullable=False
    )
    delivery_mechanism: Mapped[DeliveryMechanism] = relationship(
        "DeliveryMechanism",
        back_populates="license_pool_delivery_mechanisms",
    )

    # With some distributors we don't know for sure if a particular delivery
    # mechanism is available until we try to use it. This tracks that, so that
    # we can set the availability of a delivery mechanism to False if it fails,
    # so we don't keep trying it.
    available: Mapped[bool] = Column(Boolean, server_default=true(), nullable=False)

    resource_id = Column(Integer, ForeignKey("resources.id"), nullable=True)
    resource: Mapped[Resource | None] = relationship(
        "Resource", back_populates="licensepooldeliverymechanisms"
    )

    # One LicensePoolDeliveryMechanism may fulfill many Loans.
    fulfills: Mapped[list[Loan]] = relationship("Loan", back_populates="fulfillment")

    # One LicensePoolDeliveryMechanism may be associated with one RightsStatus.
    rightsstatus_id = Column(Integer, ForeignKey("rightsstatus.id"), index=True)
    rights_status: Mapped[RightsStatus | None] = relationship(
        "RightsStatus", back_populates="licensepooldeliverymechanisms"
    )

    @classmethod
    def set(
        cls,
        data_source: DataSource,
        identifier: Identifier,
        content_type: str | None,
        drm_scheme: str | None,
        rights_uri: str | None,
        resource: Resource | None = None,
        available: bool | None = None,
        update_available: bool = True,
        db: Session | None = None,
    ) -> LicensePoolDeliveryMechanism:
        """Register the fact that a distributor makes a title available in a
        certain format.

        :param data_source: A DataSource identifying the distributor.
        :param identifier: An Identifier identifying the title.
        :param content_type: The title is available in this media type.
        :param drm_scheme: Access to the title is confounded by this
            DRM scheme.
        :param rights_uri: A URI representing the public's rights to the
            title.
        :param resource: A Resource representing the book itself in
            a freely redistributable form.
        :param available: Is this LicensePoolDeliveryMechanism currently
            available? If None the availability won't be changed, and
            will use the default value if its created.
        :param update_available: Should the value for available be updated
            if the LicensePoolDeliveryMechanism already exists. This
            parameter is ignored if available is None.
        :param db: Use this database connection. If this is not supplied
            the database connection will be taken from the data_source.
        """

        if db:
            _db = db
        else:
            _db = Session.object_session(data_source)

        delivery_mechanism, ignore = DeliveryMechanism.lookup(
            _db, content_type, drm_scheme
        )
        rights_status = RightsStatus.lookup(_db, rights_uri)

        create_method_kwargs = None if available is None else {"available": available}
        lpdm, dirty = get_one_or_create(
            _db,
            LicensePoolDeliveryMechanism,
            identifier=identifier,
            data_source=data_source,
            delivery_mechanism=delivery_mechanism,
            resource=resource,
            create_method_kwargs=create_method_kwargs,
        )
        if not dirty and update_available and available is not None:
            lpdm.available = available

        if not lpdm.rights_status or (
            lpdm.rights_status != rights_status
            and rights_status.uri != RightsStatus.UNKNOWN
        ):
            # We have better information available about the
            # rights status of this delivery mechanism.
            lpdm.rights_status = rights_status
            dirty = True

        if dirty:
            for pool in lpdm.license_pools:
                # expire pool required in order ensure delivery mechanisms are updated.
                _db.expire(pool)

                # Creating or modifying a LPDM might change the open-access status
                # of all LicensePools for that DataSource/Identifier.
                pool.set_open_access_status()

        return lpdm

    @property
    def is_open_access(self):
        """Is this an open-access delivery mechanism?"""
        return self.rights_status and self.rights_status.uri in RightsStatus.OPEN_ACCESS

    def compatible_with(self, other):
        """Can a single loan be fulfilled with both this
        LicensePoolDeliveryMechanism and the given one?

        :param other: A LicensePoolDeliveryMechanism.
        """
        if not isinstance(other, LicensePoolDeliveryMechanism):
            return False

        if other.id == self.id:
            # They two LicensePoolDeliveryMechanisms are the same object.
            return True

        # The two LicensePoolDeliveryMechanisms must be different ways
        # of getting the same book from the same source.
        if other.identifier_id != self.identifier_id:
            return False
        if other.data_source_id != self.data_source_id:
            return False

        if other.delivery_mechanism_id == self.delivery_mechanism_id:
            # We have two LicensePoolDeliveryMechanisms for the same
            # underlying delivery mechanism. This can happen when an
            # open-access book gets its content mirrored to two
            # different places.
            return True

        # If the DeliveryMechanisms themselves are compatible, then the
        # LicensePoolDeliveryMechanisms are compatible.
        #
        # In practice, this means that either the two
        # DeliveryMechanisms are the same or that one of them is a
        # streaming mechanism.
        open_access_rules = self.is_open_access and other.is_open_access
        return other.delivery_mechanism and self.delivery_mechanism.compatible_with(
            other.delivery_mechanism, open_access_rules
        )

    def delete(self):
        """Delete a LicensePoolDeliveryMechanism."""
        _db = Session.object_session(self)
        pools = list(self.license_pools)
        _db.delete(self)

        for pool in pools:
            # We need to expire pool here otherwise the delivery_mechanisms
            # will contain a stale reference to the deleted licensepooldeliverymechanism
            _db.expire(pool)
            # The deletion of a LicensePoolDeliveryMechanism might affect
            # the open-access status of its associated LicensePools.
            pool.set_open_access_status()

    def set_rights_status(self, uri):
        _db = Session.object_session(self)
        status = RightsStatus.lookup(_db, uri)

        changed = self.rights_status != status
        # A change to a LicensePoolDeliveryMechanism's rights status
        # might affect the open-access status of its associated
        # LicensePools.
        if changed:
            self.rights_status = status
            for pool in self.license_pools:
                pool.set_open_access_status()
        return status

    @property
    def license_pools(self):
        """Find all LicensePools for this LicensePoolDeliveryMechanism."""
        _db = Session.object_session(self)
        return (
            _db.query(LicensePool)
            .filter(LicensePool.data_source == self.data_source)
            .filter(LicensePool.identifier == self.identifier)
        )

    def __repr__(self):
        return "<LicensePoolDeliveryMechanism: data_source={}, identifier={}, mechanism={}>".format(
            str(self.data_source), repr(self.identifier), repr(self.delivery_mechanism)
        )

    __table_args__ = (
        UniqueConstraint(
            "data_source_id", "identifier_id", "delivery_mechanism_id", "resource_id"
        ),
    )


# The uniqueness constraint doesn't enforce uniqueness when one of the
# fields is null, and one of these fields -- resource_id -- is
# _usually_ null. So we also need a unique partial index to properly
# enforce the constraint.
Index(
    "ix_licensepooldeliveries_unique_when_no_resource",
    LicensePoolDeliveryMechanism.data_source_id,
    LicensePoolDeliveryMechanism.identifier_id,
    LicensePoolDeliveryMechanism.delivery_mechanism_id,
    unique=True,
    postgresql_where=(LicensePoolDeliveryMechanism.resource_id == None),
)


class DeliveryMechanismTuple(NamedTuple):
    content_type: str | None
    drm_scheme: str | None

    def __str__(self):
        return f"{self.content_type} (drm_scheme={self.drm_scheme})"


class DeliveryMechanism(Base, HasSessionCache):
    """A technique for delivering a book to a patron.
    There are two parts to this: a DRM scheme and a content
    type. Either may be identified with a MIME media type
    (e.g. "application/vnd.adobe.adept+xml" or "application/epub+zip") or an
    informal name ("Kindle via Amazon").
    """

    KINDLE_CONTENT_TYPE = "Kindle via Amazon"
    NOOK_CONTENT_TYPE = "Nook via B&N"
    STREAMING_TEXT_CONTENT_TYPE = "Streaming Text"
    STREAMING_AUDIO_CONTENT_TYPE = "Streaming Audio"
    STREAMING_VIDEO_CONTENT_TYPE = "Streaming Video"

    NO_DRM = None
    ADOBE_DRM = "application/vnd.adobe.adept+xml"
    FINDAWAY_DRM = "application/vnd.librarysimplified.findaway.license+json"
    BAKER_TAYLOR_KDRM_DRM = "application/vnd.thepalaceproject.baker-taylor.kdrm+json"
    KINDLE_DRM = "Kindle DRM"
    NOOK_DRM = "Nook DRM"
    STREAMING_DRM = "Streaming"
    LCP_DRM = "application/vnd.readium.lcp.license.v1.0+json"

    # This represents the DRM system used by the app called 'Overdrive'
    # and associated with the application/x-od-media media type.
    OVERDRIVE_DRM = "Overdrive DRM"

    # This represents the DRM system used by the app called 'Libby' and
    # associated with the
    # application/vnd.overdrive.circulation.api+json media type and
    # its profiles.
    LIBBY_DRM = "Libby DRM"

    KNOWN_DRM_TYPES = {
        ADOBE_DRM,
        FINDAWAY_DRM,
        BAKER_TAYLOR_KDRM_DRM,
        KINDLE_DRM,
        NOOK_DRM,
        STREAMING_DRM,
        LCP_DRM,
        OVERDRIVE_DRM,
        LIBBY_DRM,
    }

    BEARER_TOKEN = "application/vnd.librarysimplified.bearer-token+json"
    FEEDBOOKS_AUDIOBOOK_DRM = "http://www.feedbooks.com/audiobooks/access-restriction"

    FEEDBOOKS_AUDIOBOOK_PROFILE = ';profile="%s"' % FEEDBOOKS_AUDIOBOOK_DRM
    STREAMING_PROFILE = (
        ';profile="http://librarysimplified.org/terms/profiles/streaming-media"'
    )
    MEDIA_TYPES_FOR_STREAMING = {
        STREAMING_TEXT_CONTENT_TYPE: MediaTypes.TEXT_HTML_MEDIA_TYPE,
        STREAMING_AUDIO_CONTENT_TYPE: MediaTypes.TEXT_HTML_MEDIA_TYPE,
    }

    _DEFAULT_DELIVERY_MECHANISM_SORT_LOOKUP = frozendict(
        {
            key: index
            for index, key in enumerate(
                (
                    # Common content types with no DRM are the first priority.
                    DeliveryMechanismTuple(MediaTypes.EPUB_MEDIA_TYPE, NO_DRM),
                    DeliveryMechanismTuple(MediaTypes.PDF_MEDIA_TYPE, NO_DRM),
                    DeliveryMechanismTuple(
                        MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, NO_DRM
                    ),
                    # After that we prioritize common content types with bearer token DRM.
                    DeliveryMechanismTuple(MediaTypes.EPUB_MEDIA_TYPE, BEARER_TOKEN),
                    DeliveryMechanismTuple(MediaTypes.PDF_MEDIA_TYPE, BEARER_TOKEN),
                    DeliveryMechanismTuple(
                        MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, BEARER_TOKEN
                    ),
                    # EPubs with DRM
                    DeliveryMechanismTuple(MediaTypes.EPUB_MEDIA_TYPE, LCP_DRM),
                    DeliveryMechanismTuple(MediaTypes.EPUB_MEDIA_TYPE, ADOBE_DRM),
                    # Audiobook formats
                    # Until the apps support streaming LCP audiobooks, we prioritize the feedbooks
                    # DRM type ahead of the LCP audiobook manifest, since the user experience is
                    # worse with LCP, as the client has to download the entire book before it can be played.
                    DeliveryMechanismTuple(
                        MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                        FEEDBOOKS_AUDIOBOOK_DRM,
                    ),
                    DeliveryMechanismTuple(
                        MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, LCP_DRM
                    ),
                    DeliveryMechanismTuple(
                        MediaTypes.AUDIOBOOK_PACKAGE_LCP_MEDIA_TYPE, LCP_DRM
                    ),
                    DeliveryMechanismTuple(
                        MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE, LIBBY_DRM
                    ),
                    DeliveryMechanismTuple(None, FINDAWAY_DRM),
                )
            )
        }
    )

    __tablename__ = "deliverymechanisms"
    id: Mapped[int] = Column(Integer, primary_key=True)
    content_type = Column(String)
    drm_scheme = Column(String)

    # Can the Library Simplified client fulfill a book with this
    # content type and this DRM scheme?
    default_client_can_fulfill: Mapped[bool] = Column(
        Boolean, default=False, index=True, nullable=False
    )

    # These are the media type/DRM scheme combos known to be supported
    # by the default Library Simplified client.
    #
    # This is primarily used when deciding which books can be imported
    # from an OPDS For Distributors collection.
    default_client_can_fulfill_lookup = {
        # EPUB books
        DeliveryMechanismTuple(MediaTypes.EPUB_MEDIA_TYPE, NO_DRM),
        DeliveryMechanismTuple(MediaTypes.EPUB_MEDIA_TYPE, ADOBE_DRM),
        DeliveryMechanismTuple(MediaTypes.EPUB_MEDIA_TYPE, LCP_DRM),
        # PDF books
        DeliveryMechanismTuple(MediaTypes.PDF_MEDIA_TYPE, NO_DRM),
        # Various audiobook formats
        DeliveryMechanismTuple(None, FINDAWAY_DRM),
        DeliveryMechanismTuple(MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, NO_DRM),
        DeliveryMechanismTuple(MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, LCP_DRM),
        DeliveryMechanismTuple(
            MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE, LIBBY_DRM
        ),
    }

    # If the default client supports a given media type with no DRM,
    # we can infer that the client _also_ supports that media type via
    # bearer token exchange.
    for _media_type, _drm in list(default_client_can_fulfill_lookup):
        if _media_type is not None and _drm == NO_DRM:
            default_client_can_fulfill_lookup.add(
                DeliveryMechanismTuple(_media_type, BEARER_TOKEN)
            )

    license_pool_delivery_mechanisms: Mapped[list[LicensePoolDeliveryMechanism]] = (
        relationship(
            "LicensePoolDeliveryMechanism",
            back_populates="delivery_mechanism",
            uselist=True,
        )
    )

    __table_args__ = (UniqueConstraint("content_type", "drm_scheme"),)

    @property
    def name(self):
        if self.drm_scheme is self.NO_DRM:
            drm_scheme = "DRM-free"
        else:
            drm_scheme = self.drm_scheme
        return f"{self.content_type} ({drm_scheme})"

    @property
    def as_tuple(self) -> DeliveryMechanismTuple:
        """Return a tuple representation of this DeliveryMechanism."""
        return DeliveryMechanismTuple(self.content_type, self.drm_scheme)

    def cache_key(self) -> DeliveryMechanismTuple:
        return self.as_tuple

    def __repr__(self):
        if self.default_client_can_fulfill:
            fulfillable = "fulfillable"
        else:
            fulfillable = "not fulfillable"

        return f"<Delivery mechanism: {self.name}, {fulfillable})>"

    @classmethod
    def lookup(
        cls, _db: Session, content_type: str | None, drm_scheme: str | None
    ) -> tuple[DeliveryMechanism, bool]:
        def lookup_hook() -> tuple[DeliveryMechanism, bool]:
            return get_one_or_create(
                _db, DeliveryMechanism, content_type=content_type, drm_scheme=drm_scheme
            )

        return cls.by_cache_key(
            _db, DeliveryMechanismTuple(content_type, drm_scheme), lookup_hook
        )

    @property
    def implicit_medium(self):
        """What would be a good setting for EditionConstants.MEDIUM for an edition
        available through this DeliveryMechanism?
        """
        if self.content_type in (
            MediaTypes.EPUB_MEDIA_TYPE,
            MediaTypes.PDF_MEDIA_TYPE,
            "Kindle via Amazon",
            "Streaming Text",
        ):
            return EditionConstants.BOOK_MEDIUM
        elif self.content_type in (
            "Streaming Video" or self.content_type.startswith("video/")
        ):
            return EditionConstants.VIDEO_MEDIUM
        else:
            return None

    @classmethod
    def is_media_type(cls, x):
        "Does this string look like a media type?"
        if x is None:
            return False

        return any(
            x.startswith(prefix)
            for prefix in ["vnd.", "application", "text", "video", "audio", "image"]
        )

    @property
    def is_streaming(self):
        return self.content_type in list(self.MEDIA_TYPES_FOR_STREAMING.keys())

    @property
    def drm_scheme_media_type(self):
        """Return the media type for this delivery mechanism's
        DRM scheme, assuming it's represented that way.
        """
        if self.is_media_type(self.drm_scheme):
            return self.drm_scheme
        return None

    @property
    def content_type_media_type(self):
        """Return the media type for this delivery mechanism's
        content type, assuming it's represented as a media type.
        """
        if self.is_media_type(self.content_type):
            if self.drm_scheme == self.FEEDBOOKS_AUDIOBOOK_DRM:
                return self.content_type + self.FEEDBOOKS_AUDIOBOOK_PROFILE
            return self.content_type

        media_type_for_streaming = self.MEDIA_TYPES_FOR_STREAMING.get(self.content_type)
        if media_type_for_streaming:
            return media_type_for_streaming + self.STREAMING_PROFILE

        return None

    def compatible_with(self, other, open_access_rules=False):
        """Can a single loan be fulfilled with both this delivery mechanism
        and the given one?

        :param other: A DeliveryMechanism
        :param open_access: If this is True, the rules for open-access
            fulfillment will be applied. If not, the stricted rules
            for commercial fulfillment will be applied.
        """
        if not isinstance(other, DeliveryMechanism):
            return False

        if self.id == other.id:
            # The two DeliveryMechanisms are the same.
            return True

        # Streaming delivery mechanisms can be used even when a
        # license pool is locked into a non-streaming delivery
        # mechanism.
        if self.is_streaming or other.is_streaming:
            return True

        # For an open-access book, loans are not locked to delivery
        # mechanisms, so as long as neither delivery mechanism has
        # DRM, they're compatible.
        if (
            open_access_rules
            and self.drm_scheme == self.NO_DRM
            and other.drm_scheme == self.NO_DRM
        ):
            return True

        # For non-open-access books, locking a license pool to a
        # non-streaming delivery mechanism prohibits the use of any
        # other non-streaming delivery mechanism.
        return False

    @classmethod
    def sort(
        cls,
        mechanisms: Sequence[LicensePoolDeliveryMechanism],
    ) -> list[LicensePoolDeliveryMechanism]:
        """
        Sort a list of LicensePoolDeliveryMechanism objects by the priorities defined in
        cls._DEFAULT_DELIVERY_MECHANISM_SORT.

        The sort is stable, so that any mechanisms whose sort order is the same will remain in the
        same order they were provided.
        """
        priorities = cls._DEFAULT_DELIVERY_MECHANISM_SORT_LOOKUP
        default = len(cls._DEFAULT_DELIVERY_MECHANISM_SORT_LOOKUP)

        def key_func(
            lpdm: LicensePoolDeliveryMechanism,
        ) -> int:
            """Sort by the DeliveryMechanism's default_delivery_mechanism_sort."""
            return priorities.get(lpdm.delivery_mechanism.as_tuple, default)

        result = sorted(
            mechanisms,
            key=key_func,
        )
        return result


# The uniqueness constraint doesn't enforce uniqueness when one of the
# fields is null, and one of these fields -- drm_scheme -- is
# frequently null. So we also need a unique partial index to properly
# enforce the constraint.
Index(
    "ix_deliverymechanisms_unique_when_no_drm",
    DeliveryMechanism.content_type,
    unique=True,
    postgresql_where=(DeliveryMechanism.drm_scheme == None),
)


class RightsStatus(Base):
    """The terms under which a book has been made available to the general
    public.
    This will normally be 'in copyright', or 'public domain', or a
    Creative Commons license.
    """

    # Currently in copyright.
    IN_COPYRIGHT = "http://librarysimplified.org/terms/rights-status/in-copyright"

    # Public domain in the USA.
    PUBLIC_DOMAIN_USA = (
        "http://librarysimplified.org/terms/rights-status/public-domain-usa"
    )

    # Public domain in some unknown territory
    PUBLIC_DOMAIN_UNKNOWN = (
        "http://librarysimplified.org/terms/rights-status/public-domain-unknown"
    )

    # Creative Commons Public Domain Dedication (No rights reserved)
    CC0 = "https://creativecommons.org/publicdomain/zero/1.0/"

    # Creative Commons Attribution (CC BY)
    CC_BY = "http://creativecommons.org/licenses/by/4.0/"

    # Creative Commons Attribution-ShareAlike (CC BY-SA)
    CC_BY_SA = "https://creativecommons.org/licenses/by-sa/4.0"

    # Creative Commons Attribution-NoDerivs (CC BY-ND)
    CC_BY_ND = "https://creativecommons.org/licenses/by-nd/4.0"

    # Creative Commons Attribution-NonCommercial (CC BY-NC)
    CC_BY_NC = "https://creativecommons.org/licenses/by-nc/4.0"

    # Creative Commons Attribution-NonCommercial-ShareAlike (CC BY-NC-SA)
    CC_BY_NC_SA = "https://creativecommons.org/licenses/by-nc-sa/4.0"

    # Creative Commons Attribution-NonCommercial-NoDerivs (CC BY-NC-ND)
    CC_BY_NC_ND = "https://creativecommons.org/licenses/by-nc-nd/4.0"

    # Open access download but no explicit license
    GENERIC_OPEN_ACCESS = (
        "http://librarysimplified.org/terms/rights-status/generic-open-access"
    )

    # Unknown copyright status.
    UNKNOWN = "http://librarysimplified.org/terms/rights-status/unknown"

    OPEN_ACCESS = [
        PUBLIC_DOMAIN_USA,
        CC0,
        CC_BY,
        CC_BY_SA,
        CC_BY_ND,
        CC_BY_NC,
        CC_BY_NC_SA,
        CC_BY_NC_ND,
        GENERIC_OPEN_ACCESS,
    ]

    # These open access rights allow derivative works to be created, but may
    # require attribution or prohibit commercial use.
    ALLOWS_DERIVATIVES = [
        PUBLIC_DOMAIN_USA,
        CC0,
        CC_BY,
        CC_BY_SA,
        CC_BY_NC,
        CC_BY_NC_SA,
    ]

    NAMES = {
        IN_COPYRIGHT: "In Copyright",
        PUBLIC_DOMAIN_USA: "Public domain in the USA",
        CC0: "Creative Commons Public Domain Dedication (CC0)",
        CC_BY: "Creative Commons Attribution (CC BY)",
        CC_BY_SA: "Creative Commons Attribution-ShareAlike (CC BY-SA)",
        CC_BY_ND: "Creative Commons Attribution-NoDerivs (CC BY-ND)",
        CC_BY_NC: "Creative Commons Attribution-NonCommercial (CC BY-NC)",
        CC_BY_NC_SA: "Creative Commons Attribution-NonCommercial-ShareAlike (CC BY-NC-SA)",
        CC_BY_NC_ND: "Creative Commons Attribution-NonCommercial-NoDerivs (CC BY-NC-ND)",
        GENERIC_OPEN_ACCESS: "Open access with no specific license",
        UNKNOWN: "Unknown",
    }

    DATA_SOURCE_DEFAULT_RIGHTS_STATUS = {
        DataSourceConstants.GUTENBERG: PUBLIC_DOMAIN_USA,
        DataSourceConstants.PLYMPTON: CC_BY_NC,
        # workaround for opds-imported license pools with 'content server' as data source
        DataSourceConstants.OA_CONTENT_SERVER: GENERIC_OPEN_ACCESS,
        DataSourceConstants.OVERDRIVE: IN_COPYRIGHT,
        DataSourceConstants.BIBLIOTHECA: IN_COPYRIGHT,
        DataSourceConstants.BOUNDLESS: IN_COPYRIGHT,
    }

    __tablename__ = "rightsstatus"
    id: Mapped[int] = Column(Integer, primary_key=True)

    # A URI unique to the license. This may be a URL (e.g. Creative
    # Commons)
    uri = Column(String, index=True, unique=True)

    # Human-readable name of the license.
    name = Column(String, index=True)

    # One RightsStatus may apply to many LicensePoolDeliveryMechanisms.
    licensepooldeliverymechanisms: Mapped[list[LicensePoolDeliveryMechanism]] = (
        relationship("LicensePoolDeliveryMechanism", back_populates="rights_status")
    )

    # One RightsStatus may apply to many Resources.
    resources: Mapped[list[Resource]] = relationship(
        "Resource", back_populates="rights_status"
    )

    @classmethod
    def lookup(cls, _db: Session, uri: str | None) -> RightsStatus:
        if not uri in list(cls.NAMES.keys()):
            uri = cls.UNKNOWN
        name = cls.NAMES.get(uri)
        create_method_kwargs = dict(name=name)
        status, ignore = get_one_or_create(
            _db, RightsStatus, uri=uri, create_method_kwargs=create_method_kwargs
        )
        return status

    @classmethod
    def rights_uri_from_string(cls, rights: str) -> str:
        rights = rights.lower()
        if rights == "public domain in the usa.":
            return RightsStatus.PUBLIC_DOMAIN_USA
        elif rights == "public domain in the united states.":
            return RightsStatus.PUBLIC_DOMAIN_USA
        elif rights == "pd-us":
            return RightsStatus.PUBLIC_DOMAIN_USA
        elif rights.startswith("public domain"):
            return RightsStatus.PUBLIC_DOMAIN_UNKNOWN
        elif rights.startswith("copyrighted."):
            return RightsStatus.IN_COPYRIGHT
        elif rights == "cc0":
            return RightsStatus.CC0
        elif rights == "cc by":
            return RightsStatus.CC_BY
        elif rights == "cc by-sa":
            return RightsStatus.CC_BY_SA
        elif rights == "cc by-nd":
            return RightsStatus.CC_BY_ND
        elif rights == "cc by-nc":
            return RightsStatus.CC_BY_NC
        elif rights == "cc by-nc-sa":
            return RightsStatus.CC_BY_NC_SA
        elif rights == "cc by-nc-nd":
            return RightsStatus.CC_BY_NC_ND
        elif rights in RightsStatus.OPEN_ACCESS or rights == RightsStatus.IN_COPYRIGHT:
            return rights
        else:
            return RightsStatus.UNKNOWN
