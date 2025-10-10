from __future__ import annotations

import dataclasses
import datetime
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from functools import cached_property
from typing import TypedDict, TypeVar, Unpack

from celery.canvas import Signature
from flask_babel import lazy_gettext as _
from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import DeliveryMechanismError
from palace.manager.api.circulation.fulfillment import Fulfillment
from palace.manager.api.circulation.settings import BaseCirculationApiSettings
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.base import HasLibraryIntegrationConfiguration
from palace.manager.integration.settings import BaseSettings
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin


class CirculationInternalFormatsMixin:
    """A mixin for CirculationAPIs that have internal formats."""

    # Different APIs have different internal names for delivery
    # mechanisms. This is a mapping of (content_type, drm_type)
    # 2-tuples to those internal names.
    #
    # For instance, the combination ("application/epub+zip",
    # "vnd.adobe/adept+xml") is called "ePub" in Boundless and Bibliotheca, but
    # is called "ebook-epub-adobe" in Overdrive.
    delivery_mechanism_to_internal_format: dict[tuple[str | None, str | None], str] = {}

    def internal_format(self, delivery_mechanism: LicensePoolDeliveryMechanism) -> str:
        """Look up the internal format for this delivery mechanism or
        raise an exception.

        :param delivery_mechanism: A LicensePoolDeliveryMechanism
        """
        d = delivery_mechanism.delivery_mechanism
        key = (d.content_type, d.drm_scheme)
        internal_format = self.delivery_mechanism_to_internal_format.get(key)
        if internal_format is None:
            raise DeliveryMechanismError(
                _(
                    "Could not map delivery mechanism %(mechanism_name)s to internal delivery mechanism!",
                    mechanism_name=d.name,
                )
            )
        return internal_format


SettingsType = TypeVar("SettingsType", bound=BaseCirculationApiSettings, covariant=True)
LibrarySettingsType = TypeVar("LibrarySettingsType", bound=BaseSettings, covariant=True)
LoanOrHoldT = TypeVar("LoanOrHoldT", Loan, Hold)


class BaseCirculationAPI(
    HasLibraryIntegrationConfiguration[SettingsType, LibrarySettingsType],
    LoggerMixin,
    ABC,
):
    """Encapsulates logic common to all circulation APIs."""

    BORROW_STEP = "borrow"
    FULFILL_STEP = "fulfill"

    # In 3M only, when a book is in the 'reserved' state the patron
    # cannot revoke their hold on the book.
    CAN_REVOKE_HOLD_WHEN_RESERVED = True

    # If the client must set a delivery mechanism at the point of
    # checkout (Boundless), set this to BORROW_STEP. If the client may
    # wait til the point of fulfillment to set a delivery mechanism
    # (Overdrive), set this to FULFILL_STEP. If there is no choice of
    # delivery mechanisms (Bibliotheca), set this to None.
    SET_DELIVERY_MECHANISM_AT: str | None = FULFILL_STEP

    def __init__(self, _db: Session, collection: Collection):
        self._db = _db
        self._integration_configuration_id = collection.integration_configuration.id
        self.collection_id = collection.id

        if collection.protocol != self.label():
            raise ValueError(
                f"Collection protocol {collection.protocol} pass into wrong API class {self.__class__.__name__}."
            )

    @property
    def collection(self) -> Collection:
        collection = Collection.by_id(self._db, id=self.collection_id)
        if collection is None:
            raise PalaceValueError(
                f"Collection with id {self.collection_id} not found for {self.__class__.__name__}"
            )
        return collection

    def default_notification_email_address(
        self, patron: Patron, pin: str | None
    ) -> str | None:
        """What email address should be used to notify this library's
        patrons of changes?

        :param patron: a Patron.
        """
        library = patron.library
        return library.settings.default_notification_email_address

    def integration_configuration(self) -> IntegrationConfiguration:
        config = get_one(
            self._db, IntegrationConfiguration, id=self._integration_configuration_id
        )
        if config is None:
            raise ValueError(
                f"No Configuration available for {self.__class__.__name__} (id={self._integration_configuration_id})"
            )
        return config

    @cached_property
    def settings(self) -> SettingsType:
        return self.settings_load(self.integration_configuration())

    def library_settings(self, library: Library | int) -> LibrarySettingsType | None:
        libconfig = self.integration_configuration().for_library(library)
        if libconfig is None:
            return None
        config = self.library_settings_load(libconfig)
        return config

    def sort_delivery_mechanisms(
        self, lpdms: list[LicensePoolDeliveryMechanism]
    ) -> list[LicensePoolDeliveryMechanism]:
        """
        Sort delivery mechanisms.

        Overriding this method allows subclasses to implement custom sorting logic
        for delivery mechanisms based on their specific requirements.

        The default implementation simply returns the list as is.
        """
        return lpdms

    @abstractmethod
    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        """Return a book early.

        :param patron: a Patron object for the patron who wants to check out the book.
        :param pin: The patron's alleged password.
        :param licensepool: Contains lending info as well as link to parent Identifier.
        """
        ...

    @abstractmethod
    def checkout(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
    ) -> LoanInfo | HoldInfo:
        """Check out a book on behalf of a patron.

        :param patron: a Patron object for the patron who wants to check out the book.
        :param pin: The patron's alleged password.
        :param licensepool: Contains lending info as well as link to parent Identifier.
        :param delivery_mechanism: Represents the patron's desired book format.

        :return: a LoanInfo object.
        """
        ...

    def can_fulfill_without_loan(
        self,
        patron: Patron | None,
        pool: LicensePool,
        lpdm: LicensePoolDeliveryMechanism,
    ) -> bool:
        """In general, you can't fulfill a book without a loan."""
        return False

    class FulfillKwargs(TypedDict, total=False):
        """
        Keyword arguments for the fulfill method.

        These are used to pass parameters that are necessary for some specific
        provider API implementations to fulfill a loan.
        """

        # These parameters are used for Baker & Taylor KDRM fulfillment.
        client_ip: str | None
        """The IP address of the client requesting fulfillment."""
        device_id: str | None
        """A unique identifier for the device requesting fulfillment."""
        modulus: str | None
        """The modulus part of the RSA public key used for DRM."""
        exponent: str | None
        """The exponent part of the RSA public key used for DRM."""

    @classmethod
    def import_task(cls, collection_id: int, force: bool = False) -> Signature:
        """
        Return the signature for a Celery task that will import the collection.

        :param collection_id: The ID of the collection to import.
        :param force: If True, the import will be forced even if it has already been done.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def data_source(self) -> DataSource:
        """
        Return the DataSource for this CirculationAPI.
        """
        ...

    @abstractmethod
    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[FulfillKwargs],
    ) -> Fulfillment:
        """Get the actual resource file to the patron."""
        ...

    @abstractmethod
    def place_hold(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        notification_email_address: str | None,
    ) -> HoldInfo:
        """Place a book on hold.

        :return: A HoldInfo object
        """
        ...

    @abstractmethod
    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        """Release a patron's hold on a book.

        :raises CannotReleaseHold: If there is an error communicating
            with the provider, or the provider refuses to release the hold for
            any reason.
        """
        ...

    @abstractmethod
    def update_availability(self, licensepool: LicensePool) -> None:
        """Update availability information for a book."""
        ...


class PatronActivityCirculationAPI(
    BaseCirculationAPI[SettingsType, LibrarySettingsType], ABC
):
    """
    A CirculationAPI that can return a patron's current checkouts and holds, that
    were made outside the Palace platform.
    """

    @dataclasses.dataclass(frozen=True)
    class IdentifierKey:
        type: str | None
        identifier: str | None

    @abstractmethod
    def patron_activity(
        self, patron: Patron, pin: str | None
    ) -> Iterable[LoanInfo | HoldInfo]:
        """Return a patron's current checkouts and holds."""
        ...

    def remote_holds_and_loans(
        self, patron: Patron, pin: str | None
    ) -> tuple[dict[IdentifierKey, LoanInfo], dict[IdentifierKey, HoldInfo]]:
        remote_loans = {}
        remote_holds = {}
        for activity in self.patron_activity(patron, pin):
            key = self.IdentifierKey(activity.identifier_type, activity.identifier)
            if isinstance(activity, LoanInfo):
                remote_loans[key] = activity
            elif isinstance(activity, HoldInfo):
                remote_holds[key] = activity

        return remote_loans, remote_holds

    def local_loans_or_holds(
        self, patron: Patron, item_cls: type[LoanOrHoldT]
    ) -> dict[IdentifierKey, LoanOrHoldT]:
        items = self._db.scalars(
            select(item_cls)
            .join(LicensePool)
            .where(
                LicensePool.collection_id == self.collection_id,
                item_cls.patron == patron,
            )
        ).all()

        items_by_identifier = {}
        for item in items:
            license_pool = item.license_pool
            if license_pool.identifier is None:
                self.log.error(
                    "Active loan or hold (%r) on license pool (%r), which has no identifier.",
                    item,
                    license_pool,
                )
                continue

            key = self.IdentifierKey(
                license_pool.identifier.type,
                license_pool.identifier.identifier,
            )
            items_by_identifier[key] = item
        return items_by_identifier

    def local_loans_and_holds(
        self, patron: Patron
    ) -> tuple[dict[IdentifierKey, Loan], dict[IdentifierKey, Hold]]:
        return self.local_loans_or_holds(patron, Loan), self.local_loans_or_holds(
            patron, Hold
        )

    def delete_loans_or_holds(self, loans_or_holds: Iterable[LoanOrHoldT]) -> None:
        one_minute_ago = utc_now() - datetime.timedelta(minutes=1)
        for item in loans_or_holds:
            if item.start is not None and item.start > one_minute_ago:
                # This was just created, we shouldn't delete it.
                continue
            self.log.info(
                f"Deleting {item.__class__.__name__} ({item.id}) on license pool (id: {item.license_pool_id}) "
                f"for patron ({item.patron.authorization_identifier})"
            )
            self._db.delete(item)

    def sync_loans(
        self,
        patron: Patron,
        remote_loans: Mapping[IdentifierKey, LoanInfo],
        local_loans: Mapping[IdentifierKey, Loan],
    ) -> None:
        # Update the local loans and to match the remote loans
        for identifier, loan in remote_loans.items():
            loan.create_or_update(patron)

        loans_to_delete = [
            local_loans[i] for i in local_loans.keys() - remote_loans.keys()
        ]
        self.delete_loans_or_holds(loans_to_delete)

    def sync_holds(
        self,
        patron: Patron,
        remote_holds: Mapping[IdentifierKey, HoldInfo],
        local_holds: Mapping[IdentifierKey, Hold],
    ) -> None:
        # Update the local holds to match the remote holds
        for identifier, hold in remote_holds.items():
            hold.create_or_update(patron)

        holds_to_delete = [
            local_holds[i] for i in local_holds.keys() - remote_holds.keys()
        ]
        self.delete_loans_or_holds(holds_to_delete)

    def sync_patron_activity(self, patron: Patron, pin: str | None) -> None:
        remote_loans, remote_holds = self.remote_holds_and_loans(patron, pin)
        local_loans, local_holds = self.local_loans_and_holds(patron)

        self.sync_loans(patron, remote_loans, local_loans)
        self.sync_holds(patron, remote_holds, local_holds)


CirculationApiType = BaseCirculationAPI[BaseCirculationApiSettings, BaseSettings]
