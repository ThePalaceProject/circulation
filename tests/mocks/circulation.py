from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any, Unpack

from sqlalchemy.orm import Session

from palace.manager.api.circulation.base import (
    BaseCirculationAPI,
    CirculationApiType,
    PatronActivityCirculationAPI,
)
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.circulation.fulfillment import Fulfillment
from palace.manager.api.circulation_manager import CirculationManager
from palace.manager.integration.settings import BaseSettings
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.service.container import Services
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Patron


class MockBaseCirculationAPI(BaseCirculationAPI):
    def __init__(
        self,
        _db: Session,
        collection: Collection,
        set_delivery_mechanism_at: str | None = BaseCirculationAPI.FULFILL_STEP,
        can_revoke_hold_when_reserved: bool = True,
        data_source_name: str = "Test Data Source",
    ):
        old_protocol = collection.integration_configuration.protocol
        collection.integration_configuration.protocol = self.label()
        super().__init__(_db, collection)
        collection.integration_configuration.protocol = old_protocol
        self.SET_DELIVERY_MECHANISM_AT = set_delivery_mechanism_at
        self.CAN_REVOKE_HOLD_WHEN_RESERVED = can_revoke_hold_when_reserved
        self.responses: dict[str, list[Any]] = defaultdict(list)
        self.availability_updated_for: list[LicensePool] = []
        self.data_source_name = data_source_name

    @classmethod
    def label(cls) -> str:
        return ""

    @classmethod
    def description(cls) -> str:
        return ""

    @classmethod
    def settings_class(cls) -> type[BaseSettings]:
        return BaseSettings

    @classmethod
    def library_settings_class(cls) -> type[BaseSettings]:
        return BaseSettings

    @property
    def data_source(self) -> DataSource:
        return DataSource.lookup(self._db, self.data_source_name, autocreate=True)

    def checkout(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
    ) -> LoanInfo | HoldInfo:
        # Should be a LoanInfo.
        return self._return_or_raise("checkout")

    def update_availability(self, licensepool: LicensePool) -> None:
        """Simply record the fact that update_availability was called."""
        self.availability_updated_for.append(licensepool)

    def place_hold(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        notification_email_address: str | None,
    ) -> HoldInfo:
        # Should be a HoldInfo.
        return self._return_or_raise("hold")

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> Fulfillment:
        # Should be a Fulfillment.
        return self._return_or_raise("fulfill")

    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        # Return value is not checked.
        return self._return_or_raise("checkin")

    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        # Return value is not checked.
        return self._return_or_raise("release_hold")

    def queue_checkout(self, response: LoanInfo | HoldInfo | Exception) -> None:
        self._queue("checkout", response)

    def queue_hold(self, response: HoldInfo | Exception) -> None:
        self._queue("hold", response)

    def queue_fulfill(self, response: Fulfillment | Exception) -> None:
        self._queue("fulfill", response)

    def queue_checkin(self, response: None | Exception = None) -> None:
        self._queue("checkin", response)

    def queue_release_hold(self, response: None | Exception = None) -> None:
        self._queue("release_hold", response)

    def _queue(self, k: str, v: Any) -> None:
        self.responses[k].append(v)

    def _return_or_raise(self, key: str) -> Any:
        self.log.debug(key)
        response = self.responses[key].pop()
        if isinstance(response, Exception):
            raise response
        return response


class MockCirculationApiDispatcher(CirculationApiDispatcher):
    def __init__(
        self,
        db: Session,
        library: Library,
        library_collection_apis: Mapping[int | None, CirculationApiType],
        analytics: Analytics | None = None,
    ):
        super().__init__(db, library, library_collection_apis, analytics=analytics)
        self.remotes: dict[str, MockBaseCirculationAPI] = {}

    def queue_checkout(
        self, licensepool: LicensePool, response: LoanInfo | HoldInfo | Exception
    ) -> None:
        api = self.api_for_license_pool(licensepool)
        api.queue_checkout(response)

    def queue_hold(
        self, licensepool: LicensePool, response: HoldInfo | Exception
    ) -> None:
        api = self.api_for_license_pool(licensepool)
        api.queue_hold(response)

    def queue_fulfill(
        self, licensepool: LicensePool, response: Fulfillment | Exception
    ) -> None:
        api = self.api_for_license_pool(licensepool)
        api.queue_fulfill(response)

    def queue_checkin(
        self, licensepool: LicensePool, response: None | Exception = None
    ) -> None:
        api = self.api_for_license_pool(licensepool)
        api.queue_checkin(response)

    def queue_release_hold(
        self, licensepool: LicensePool, response: None | Exception = None
    ) -> None:
        api = self.api_for_license_pool(licensepool)
        api.queue_release_hold(response)

    def api_for_license_pool(self, licensepool: LicensePool) -> MockBaseCirculationAPI:
        source = licensepool.data_source.name
        assert source is not None
        if source not in self.remotes:
            set_delivery_mechanism_at = BaseCirculationAPI.FULFILL_STEP
            can_revoke_hold_when_reserved = True
            if source == DataSource.BOUNDLESS:
                set_delivery_mechanism_at = BaseCirculationAPI.BORROW_STEP
            if source == DataSource.BIBLIOTHECA:
                can_revoke_hold_when_reserved = False
            remote = MockBaseCirculationAPI(
                self._db,
                licensepool.collection,
                set_delivery_mechanism_at,
                can_revoke_hold_when_reserved,
            )
            self.remotes[source] = remote
        return self.remotes[source]

    def add_remote_api(
        self, licensepool: LicensePool, api: MockBaseCirculationAPI
    ) -> None:
        source = licensepool.data_source.name
        assert source is not None
        self.remotes[source] = api


class MockPatronActivityCirculationAPI(
    MockBaseCirculationAPI, PatronActivityCirculationAPI
):
    def __init__(self, _db: Session, collection: Collection):
        super().__init__(_db, collection)
        self.remote_loans: list[LoanInfo] = []
        self.remote_holds: list[HoldInfo] = []
        self.patron_activity_calls: list[tuple[Patron, str | None]] = []

    def add_remote_loan(
        self,
        loan: LoanInfo,
    ) -> None:
        self.remote_loans.append(loan)

    def add_remote_hold(self, hold: HoldInfo) -> None:
        self.remote_holds.append(hold)

    def patron_activity(
        self, patron: Patron, pin: str | None
    ) -> Iterable[LoanInfo | HoldInfo]:
        self.patron_activity_calls.append((patron, pin))
        yield from self.remote_loans
        yield from self.remote_holds


class MockCirculationManager(CirculationManager):
    d_circulation: MockCirculationApiDispatcher

    def __init__(self, db: Session, services: Services):
        super().__init__(db, services=services)

    def setup_circulation_api_dispatcher(
        self,
        db: Session,
        library: Library,
        library_collection_apis: Mapping[int | None, CirculationApiType],
        analytics: Analytics | None = None,
    ) -> MockCirculationApiDispatcher:
        return MockCirculationApiDispatcher(
            db, library, library_collection_apis, analytics=analytics
        )
