import logging
from collections import defaultdict

from api.circulation import BaseCirculationAPI, CirculationAPI, HoldInfo, LoanInfo
from api.shared_collection import SharedCollectionAPI
from core.model import DataSource, Hold, Loan


class MockRemoteAPI(BaseCirculationAPI):
    def __init__(self, set_delivery_mechanism_at, can_revoke_hold_when_reserved):
        self.SET_DELIVERY_MECHANISM_AT = set_delivery_mechanism_at
        self.CAN_REVOKE_HOLD_WHEN_RESERVED = can_revoke_hold_when_reserved
        self.responses = defaultdict(list)
        self.log = logging.getLogger("Mock remote API")
        self.availability_updated_for = []

    def checkout(self, patron_obj, patron_password, licensepool, delivery_mechanism):
        # Should be a LoanInfo.
        return self._return_or_raise("checkout")

    def update_availability(self, licensepool):
        """Simply record the fact that update_availability was called."""
        self.availability_updated_for.append(licensepool)

    def place_hold(self, patron, pin, licensepool, hold_notification_email=None):
        # Should be a HoldInfo.
        return self._return_or_raise("hold")

    def fulfill(
        self,
        patron,
        pin,
        licensepool,
        internal_format=None,
        part=None,
        fulfill_part_url=None,
    ):
        # Should be a FulfillmentInfo.
        return self._return_or_raise("fulfill")

    def checkin(self, patron, pin, licensepool):
        # Return value is not checked.
        return self._return_or_raise("checkin")

    def release_hold(self, patron, pin, licensepool):
        # Return value is not checked.
        return self._return_or_raise("release_hold")

    def internal_format(self, delivery_mechanism):
        return delivery_mechanism

    def update_loan(self, loan, status_doc):
        self.availability_updated_for.append(loan.license_pool)

    def queue_checkout(self, response):
        self._queue("checkout", response)

    def queue_hold(self, response):
        self._queue("hold", response)

    def queue_fulfill(self, response):
        self._queue("fulfill", response)

    def queue_checkin(self, response):
        self._queue("checkin", response)

    def queue_release_hold(self, response):
        self._queue("release_hold", response)

    def _queue(self, k, v):
        self.responses[k].append(v)

    def _return_or_raise(self, k):
        self.log.debug(k)
        l = self.responses[k]
        v = l.pop()
        if isinstance(v, Exception):
            raise v
        return v


class MockCirculationAPI(CirculationAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.responses = defaultdict(list)
        self.remote_loans = []
        self.remote_holds = []
        self.remotes = {}

    def local_loans(self, patron):
        return self._db.query(Loan).filter(Loan.patron == patron)

    def local_holds(self, patron):
        return self._db.query(Hold).filter(Hold.patron == patron)

    def add_remote_loan(self, *args, **kwargs):
        self.remote_loans.append(LoanInfo(*args, **kwargs))

    def add_remote_hold(self, *args, **kwargs):
        self.remote_holds.append(HoldInfo(*args, **kwargs))

    def patron_activity(self, patron, pin):
        """Return a 3-tuple (loans, holds, completeness)."""
        return self.remote_loans, self.remote_holds, True

    def queue_checkout(self, licensepool, response):
        self._queue("checkout", licensepool, response)

    def queue_hold(self, licensepool, response):
        self._queue("hold", licensepool, response)

    def queue_fulfill(self, licensepool, response):
        self._queue("fulfill", licensepool, response)

    def queue_checkin(self, licensepool, response):
        self._queue("checkin", licensepool, response)

    def queue_release_hold(self, licensepool, response):
        self._queue("release_hold", licensepool, response)

    def _queue(self, method, licensepool, response):
        mock = self.api_for_license_pool(licensepool)
        return mock._queue(method, response)

    def api_for_license_pool(self, licensepool):
        source = licensepool.data_source.name
        if source not in self.remotes:
            set_delivery_mechanism_at = BaseCirculationAPI.FULFILL_STEP
            can_revoke_hold_when_reserved = True
            if source == DataSource.AXIS_360:
                set_delivery_mechanism_at = BaseCirculationAPI.BORROW_STEP
            if source == DataSource.THREEM:
                can_revoke_hold_when_reserved = False
            remote = MockRemoteAPI(
                set_delivery_mechanism_at, can_revoke_hold_when_reserved
            )
            self.remotes[source] = remote
        return self.remotes[source]


class MockSharedCollectionAPI(SharedCollectionAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.responses = defaultdict(list)

    def _queue(self, k, v):
        self.responses[k].append(v)

    def _return_or_raise(self, k):
        self.log.debug(k)
        l = self.responses[k]
        v = l.pop(0)
        if isinstance(v, Exception):
            raise v
        return v

    def queue_register(self, response):
        self._queue("register", response)

    def register(self, collection, url):
        return self._return_or_raise("register")

    def queue_borrow(self, response):
        self._queue("borrow", response)

    def borrow(self, collection, client, pool, hold=None):
        return self._return_or_raise("borrow")

    def queue_revoke_loan(self, response):
        self._queue("revoke-loan", response)

    def revoke_loan(self, collection, client, loan):
        return self._return_or_raise("revoke-loan")

    def queue_fulfill(self, response):
        self._queue("fulfill", response)

    def fulfill(
        self,
        patron,
        pin,
        licensepool,
        internal_format=None,
        part=None,
        fulfill_part_url=None,
    ):
        return self._return_or_raise("fulfill")

    def queue_revoke_hold(self, response):
        self._queue("revoke-hold", response)

    def revoke_hold(self, collection, client, hold):
        return self._return_or_raise("revoke-hold")
