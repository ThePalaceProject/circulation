"""Test the CirculationAPI."""
from datetime import timedelta
from typing import Union
from unittest.mock import MagicMock

import flask
import pytest
from flask import Flask

from api.authentication.base import PatronData
from api.authenticator import LibraryAuthenticator
from api.circulation import (
    APIAwareFulfillmentInfo,
    BaseCirculationAPI,
    CirculationAPI,
    CirculationInfo,
    DeliveryMechanismInfo,
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
)
from api.circulation_exceptions import *
from core.config import CannotLoadConfiguration
from core.mock_analytics_provider import MockAnalyticsProvider
from core.model import (
    CirculationEvent,
    ConfigurationSetting,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hold,
    Hyperlink,
    Identifier,
    Loan,
    MediaTypes,
    Representation,
    RightsStatus,
)
from core.util.datetime_helpers import utc_now
from tests.api.mockapi.bibliotheca import MockBibliothecaAPI
from tests.api.mockapi.circulation import MockCirculationAPI

from ..fixtures.api_bibliotheca_files import BibliothecaFilesFixture
from ..fixtures.database import DatabaseTransactionFixture


class CirculationAPIFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.collection = MockBibliothecaAPI.mock_collection(db.session)
        edition, self.pool = db.edition(
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
            with_license_pool=True,
            collection=self.collection,
        )
        self.pool.open_access = False
        self.identifier = self.pool.identifier
        [self.delivery_mechanism] = self.pool.delivery_mechanisms
        self.patron = db.patron()
        self.analytics = MockAnalyticsProvider()
        self.circulation = MockCirculationAPI(
            db.session,
            db.default_library(),
            analytics=self.analytics,
            api_map={ExternalIntegration.BIBLIOTHECA: MockBibliothecaAPI},
        )
        self.remote = self.circulation.api_for_license_pool(self.pool)


@pytest.fixture(scope="function")
def circulation_api(db: DatabaseTransactionFixture) -> CirculationAPIFixture:
    return CirculationAPIFixture(db)


class TestCirculationAPI:
    YESTERDAY = utc_now() - timedelta(days=1)
    TODAY = utc_now()
    TOMORROW = utc_now() + timedelta(days=1)
    IN_TWO_WEEKS = utc_now() + timedelta(days=14)

    @staticmethod
    def borrow(circulation_api: CirculationAPIFixture):
        return circulation_api.circulation.borrow(
            circulation_api.patron,
            "1234",
            circulation_api.pool,
            circulation_api.delivery_mechanism,
        )

    @staticmethod
    def sync_bookshelf(circulation_api: CirculationAPIFixture):
        return circulation_api.circulation.sync_bookshelf(
            circulation_api.patron, "1234"
        )

    def test_circulationinfo_collection_id(
        self, circulation_api: CirculationAPIFixture
    ):
        # It's possible to instantiate CirculationInfo (the superclass of all
        # other circulation-related *Info classes) with either a
        # Collection-like object or a numeric collection ID.
        cls = CirculationInfo
        other_args = [None] * 3

        info = cls(100, *other_args)
        assert 100 == info.collection_id

        info = cls(circulation_api.pool.collection, *other_args)
        assert circulation_api.pool.collection.id == info.collection_id

    def test_borrow_sends_analytics_event(self, circulation_api: CirculationAPIFixture):
        now = utc_now()
        loaninfo = LoanInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.pool.identifier.type,
            circulation_api.pool.identifier.identifier,
            now,
            now + timedelta(seconds=3600),
            external_identifier=circulation_api.db.fresh_str(),
        )
        circulation_api.remote.queue_checkout(loaninfo)
        now = utc_now()

        loan, hold, is_new = self.borrow(circulation_api)

        # The Loan looks good.
        assert loaninfo.identifier == loan.license_pool.identifier.identifier
        assert circulation_api.patron == loan.patron
        assert None == hold
        assert True == is_new
        assert loaninfo.external_identifier == loan.external_identifier

        # An analytics event was created.
        assert 1 == circulation_api.analytics.count
        assert CirculationEvent.CM_CHECKOUT == circulation_api.analytics.event_type

        # Try to 'borrow' the same book again.
        circulation_api.remote.queue_checkout(AlreadyCheckedOut())  # type: ignore
        loan, hold, is_new = self.borrow(circulation_api)
        assert False == is_new
        assert loaninfo.external_identifier == loan.external_identifier

        # Since the loan already existed, no new analytics event was
        # sent.
        assert 1 == circulation_api.analytics.count

        # Now try to renew the book.
        circulation_api.remote.queue_checkout(loaninfo)
        loan, hold, is_new = self.borrow(circulation_api)
        assert False == is_new

        # Renewals are counted as loans, since from an accounting
        # perspective they _are_ loans.
        assert 2 == circulation_api.analytics.count

        # Loans of open-access books go through a different code
        # path, but they count as loans nonetheless.
        circulation_api.pool.open_access = True
        circulation_api.remote.queue_checkout(loaninfo)
        loan, hold, is_new = self.borrow(circulation_api)
        assert 3 == circulation_api.analytics.count

    def test_borrowing_of_self_hosted_book_succeeds(
        self, circulation_api: CirculationAPIFixture
    ):
        # Arrange
        circulation_api.pool.self_hosted = True

        # Act
        loan, hold, is_new = self.borrow(circulation_api)

        # Assert
        assert True == is_new
        assert circulation_api.pool == loan.license_pool
        assert circulation_api.patron == loan.patron
        assert hold is None

    def test_borrowing_of_unlimited_access_book_succeeds(
        self, circulation_api: CirculationAPIFixture
    ):
        """Ensure that unlimited access books that don't belong to collections
        having a custom CirculationAPI implementation (e.g., OPDS 1.x, OPDS 2.x collections)
        are checked out in the same way as OA and self-hosted books."""
        # Arrange

        # Reset the API map, this book belongs to the "basic" collection,
        # i.e. collection without a custom CirculationAPI implementation.
        circulation_api.circulation.api_for_license_pool = MagicMock(return_value=None)

        # Mark the book as unlimited access.
        circulation_api.pool.unlimited_access = True

        # Act
        loan, hold, is_new = self.borrow(circulation_api)

        # Assert
        assert True == is_new
        assert circulation_api.pool == loan.license_pool
        assert circulation_api.patron == loan.patron
        assert hold is None

    def test_attempt_borrow_with_existing_remote_loan(
        self, circulation_api: CirculationAPIFixture
    ):
        """The patron has a remote loan that the circ manager doesn't know
        about, and they just tried to borrow a book they already have
        a loan for.
        """
        # Remote loan.
        circulation_api.circulation.add_remote_loan(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            self.YESTERDAY,
            self.IN_TWO_WEEKS,
        )

        circulation_api.remote.queue_checkout(AlreadyCheckedOut())  # type: ignore
        now = utc_now()
        loan, hold, is_new = self.borrow(circulation_api)

        # There is now a new local loan representing the remote loan.
        assert True == is_new
        assert circulation_api.pool == loan.license_pool
        assert circulation_api.patron == loan.patron
        assert None == hold

        # The server told us 'there's already a loan for this book'
        # but didn't give us any useful information on when that loan
        # was created. We've faked it with values that should be okay
        # until the next sync.
        assert abs((loan.start - now).seconds) < 2
        assert 3600 == (loan.end - loan.start).seconds

    def test_attempt_borrow_with_existing_remote_hold(
        self, circulation_api: CirculationAPIFixture
    ):
        """The patron has a remote hold that the circ manager doesn't know
        about, and they just tried to borrow a book they already have
        on hold.
        """
        # Remote hold.
        circulation_api.circulation.add_remote_hold(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            self.YESTERDAY,
            self.IN_TWO_WEEKS,
            10,
        )

        circulation_api.remote.queue_checkout(AlreadyOnHold())  # type: ignore
        now = utc_now()
        loan, hold, is_new = self.borrow(circulation_api)

        # There is now a new local hold representing the remote hold.
        assert True == is_new
        assert None == loan
        assert circulation_api.pool == hold.license_pool
        assert circulation_api.patron == hold.patron

        # The server told us 'you already have this book on hold' but
        # didn't give us any useful information on when that hold was
        # created. We've set the hold start time to the time we found
        # out about it. We'll get the real information the next time
        # we do a sync.
        assert abs((hold.start - now).seconds) < 2
        assert None == hold.end
        assert None == hold.position

    def test_attempt_premature_renew_with_local_loan(
        self, circulation_api: CirculationAPIFixture
    ):
        """We have a local loan and a remote loan but the patron tried to
        borrow again -- probably to renew their loan.
        """
        # Local loan.
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)

        # Remote loan.
        circulation_api.circulation.add_remote_loan(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            self.YESTERDAY,
            self.IN_TWO_WEEKS,
        )

        # This is the expected behavior in most cases--you tried to
        # renew the loan and failed because it's not time yet.
        circulation_api.remote.queue_checkout(CannotRenew())  # type: ignore
        with pytest.raises(CannotRenew) as excinfo:  # type: ignore
            self.borrow(circulation_api)
        assert "CannotRenew" in str(excinfo.value)

    def test_attempt_renew_with_local_loan_and_no_available_copies(
        self, circulation_api: CirculationAPIFixture
    ):
        """We have a local loan and a remote loan but the patron tried to
        borrow again -- probably to renew their loan.
        """
        # Local loan.
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)

        # Remote loan.
        circulation_api.circulation.add_remote_loan(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            self.YESTERDAY,
            self.IN_TWO_WEEKS,
        )

        # NoAvailableCopies can happen if there are already people
        # waiting in line for the book. This case gives a more
        # specific error message.
        #
        # Contrast with the way NoAvailableCopies is handled in
        # test_loan_becomes_hold_if_no_available_copies.
        circulation_api.remote.queue_checkout(NoAvailableCopies())  # type: ignore
        with pytest.raises(CannotRenew) as excinfo:  # type: ignore
            self.borrow(circulation_api)
        assert "You cannot renew a loan if other patrons have the work on hold." in str(
            excinfo.value
        )

    def test_loan_becomes_hold_if_no_available_copies(
        self, circulation_api: CirculationAPIFixture
    ):
        # We want to borrow this book but there are no copies.
        circulation_api.remote.queue_checkout(NoAvailableCopies())  # type: ignore
        holdinfo = HoldInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            None,
            None,
            10,
        )
        circulation_api.remote.queue_hold(holdinfo)

        # As such, an attempt to renew our loan results in us actually
        # placing a hold on the book.
        loan, hold, is_new = self.borrow(circulation_api)
        assert None == loan
        assert True == is_new
        assert circulation_api.pool == hold.license_pool
        assert circulation_api.patron == hold.patron

    def test_borrow_creates_hold_if_api_returns_hold_info(
        self, circulation_api: CirculationAPIFixture
    ):
        # There are no available copies, but the remote API
        # places a hold for us right away instead of raising
        # an error.
        holdinfo = HoldInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            None,
            None,
            10,
        )
        circulation_api.remote.queue_checkout(holdinfo)

        # As such, an attempt to borrow results in us actually
        # placing a hold on the book.
        loan, hold, is_new = self.borrow(circulation_api)
        assert None == loan
        assert True == is_new
        assert circulation_api.pool == hold.license_pool
        assert circulation_api.patron == hold.patron

    def test_vendor_side_loan_limit_allows_for_hold_placement(
        self, circulation_api: CirculationAPIFixture
    ):
        # Attempting to borrow a book will trigger a vendor-side loan
        # limit.
        circulation_api.remote.queue_checkout(PatronLoanLimitReached())  # type: ignore

        # But the point is moot because the book isn't even available.
        # Attempting to place a hold will succeed.
        holdinfo = HoldInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            None,
            None,
            10,
        )
        circulation_api.remote.queue_hold(holdinfo)

        loan, hold, is_new = self.borrow(circulation_api)

        # No exception was raised, and the Hold looks good.
        assert holdinfo.identifier == hold.license_pool.identifier.identifier
        assert circulation_api.patron == hold.patron
        assert None == loan
        assert True == is_new

    def test_loan_exception_reraised_if_hold_placement_fails(
        self, circulation_api: CirculationAPIFixture
    ):
        # Attempting to borrow a book will trigger a vendor-side loan
        # limit.
        circulation_api.remote.queue_checkout(PatronLoanLimitReached())  # type: ignore

        # Attempting to place a hold will fail because the book is
        # available. (As opposed to the previous test, where the book
        # was _not_ available and hold placement succeeded.) This
        # indicates that we should have raised PatronLoanLimitReached
        # in the first place.
        circulation_api.remote.queue_hold(CurrentlyAvailable())  # type: ignore

        assert len(circulation_api.remote.responses["checkout"]) == 1
        assert len(circulation_api.remote.responses["hold"]) == 1

        # The exception raised is PatronLoanLimitReached, the first
        # one we encountered...
        pytest.raises(PatronLoanLimitReached, lambda: self.borrow(circulation_api))  # type: ignore

        # ...but we made both requests and have no more responses
        # queued.
        assert not circulation_api.remote.responses["checkout"]
        assert not circulation_api.remote.responses["hold"]

    def test_hold_sends_analytics_event(self, circulation_api: CirculationAPIFixture):
        circulation_api.remote.queue_checkout(NoAvailableCopies())  # type: ignore
        holdinfo = HoldInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            None,
            None,
            10,
        )
        circulation_api.remote.queue_hold(holdinfo)

        loan, hold, is_new = self.borrow(circulation_api)

        # The Hold looks good.
        assert holdinfo.identifier == hold.license_pool.identifier.identifier
        assert circulation_api.patron == hold.patron
        assert None == loan
        assert True == is_new

        # An analytics event was created.
        assert 1 == circulation_api.analytics.count
        assert CirculationEvent.CM_HOLD_PLACE == circulation_api.analytics.event_type

        # Try to 'borrow' the same book again.
        circulation_api.remote.queue_checkout(AlreadyOnHold())  # type: ignore
        loan, hold, is_new = self.borrow(circulation_api)
        assert False == is_new

        # Since the hold already existed, no new analytics event was
        # sent.
        assert 1 == circulation_api.analytics.count

    def test_loan_becomes_hold_if_no_available_copies_and_preexisting_loan(
        self, circulation_api: CirculationAPIFixture
    ):
        # Once upon a time, we had a loan for this book.
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)
        loan.start = self.YESTERDAY

        # But no longer! What's more, other patrons have taken all the
        # copies!
        circulation_api.remote.queue_checkout(NoAvailableCopies())  # type: ignore
        holdinfo = HoldInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            None,
            None,
            10,
        )
        circulation_api.remote.queue_hold(holdinfo)

        assert [] == circulation_api.remote.availability_updated_for

        # As such, an attempt to renew our loan results in us actually
        # placing a hold on the book.
        loan, hold, is_new = self.borrow(circulation_api)
        assert None == loan
        assert True == is_new
        assert circulation_api.pool == hold.license_pool
        assert circulation_api.patron == hold.patron

        # When NoAvailableCopies was raised, the circulation
        # information for the book was immediately updated, to reduce
        # the risk that other patrons would encounter the same
        # problem.
        assert [circulation_api.pool] == circulation_api.remote.availability_updated_for

    def test_borrow_with_expired_card_fails(
        self, circulation_api: CirculationAPIFixture
    ):
        # This checkout would succeed...
        now = utc_now()
        loaninfo = LoanInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.pool.identifier.type,
            circulation_api.pool.identifier.identifier,
            now,
            now + timedelta(seconds=3600),
        )
        circulation_api.remote.queue_checkout(loaninfo)

        # ...except the patron's library card has expired.
        old_expires = circulation_api.patron.authorization_expires
        yesterday = now - timedelta(days=1)
        circulation_api.patron.authorization_expires = yesterday

        pytest.raises(AuthorizationExpired, lambda: self.borrow(circulation_api))  # type: ignore
        circulation_api.patron.authorization_expires = old_expires

    def test_borrow_with_outstanding_fines(
        self, circulation_api: CirculationAPIFixture
    ):
        # This checkout would succeed...
        now = utc_now()
        loaninfo = LoanInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.pool.identifier.type,
            circulation_api.pool.identifier.identifier,
            now,
            now + timedelta(seconds=3600),
        )
        circulation_api.remote.queue_checkout(loaninfo)

        # ...except the patron has too many fines.
        old_fines = circulation_api.patron.fines
        circulation_api.patron.fines = 1000
        setting = ConfigurationSetting.for_library(
            Configuration.MAX_OUTSTANDING_FINES, circulation_api.db.default_library()  # type: ignore
        )
        setting.value = "$0.50"

        pytest.raises(OutstandingFines, lambda: self.borrow(circulation_api))  # type: ignore

        # Test the case where any amount of fines are too much.
        setting.value = "$0"
        pytest.raises(OutstandingFines, lambda: self.borrow(circulation_api))  # type: ignore

        # Remove the fine policy, and borrow succeeds.
        setting.value = None
        loan, i1, i2 = self.borrow(circulation_api)
        assert isinstance(loan, Loan)

        circulation_api.patron.fines = old_fines

    def test_borrow_with_block_fails(self, circulation_api: CirculationAPIFixture):
        # This checkout would succeed...
        now = utc_now()
        loaninfo = LoanInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.pool.identifier.type,
            circulation_api.pool.identifier.identifier,
            now,
            now + timedelta(seconds=3600),
        )
        circulation_api.remote.queue_checkout(loaninfo)

        # ...except the patron is blocked
        circulation_api.patron.block_reason = "some reason"
        pytest.raises(AuthorizationBlocked, lambda: self.borrow(circulation_api))  # type: ignore
        circulation_api.patron.block_reason = None

    def test_no_licenses_prompts_availability_update(
        self, circulation_api: CirculationAPIFixture
    ):
        # Once the library offered licenses for this book, but
        # the licenses just expired.
        circulation_api.remote.queue_checkout(NoLicenses())  # type: ignore
        assert [] == circulation_api.remote.availability_updated_for

        # We're not able to borrow the book...
        pytest.raises(NoLicenses, lambda: self.borrow(circulation_api))  # type: ignore

        # But the availability of the book gets immediately updated,
        # so that we don't keep offering the book.
        assert [circulation_api.pool] == circulation_api.remote.availability_updated_for

    def test_borrow_calls_enforce_limits(self, circulation_api: CirculationAPIFixture):
        # Verify that the normal behavior of CirculationAPI.borrow()
        # is to call enforce_limits() before trying to check out the
        # book.
        class MockVendorAPI(BaseCirculationAPI):
            # Short-circuit the borrowing process -- we just need to make sure
            # enforce_limits is called before checkout()
            def __init__(self):
                self.availability_updated = []

            def internal_format(self, *args, **kwargs):
                return "some format"

            def checkout(self, *args, **kwargs):
                raise NotImplementedError()

        api = MockVendorAPI()

        class MockCirculationAPI(CirculationAPI):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.enforce_limits_calls = []

            def enforce_limits(self, patron, licensepool):
                self.enforce_limits_calls.append((patron, licensepool))

            def api_for_license_pool(self, pool):
                # Always return the same mock MockVendorAPI.
                return api

        circulation_api.circulation = MockCirculationAPI(
            circulation_api.db.session, circulation_api.db.default_library()
        )

        # checkout() raised the expected NotImplementedError
        pytest.raises(NotImplementedError, lambda: self.borrow(circulation_api))

        # But before that happened, enforce_limits was called once.
        assert [
            (circulation_api.patron, circulation_api.pool)
        ] == circulation_api.circulation.enforce_limits_calls

    def test_patron_at_loan_limit(self, circulation_api: CirculationAPIFixture):
        # The loan limit is a per-library setting.
        setting = circulation_api.patron.library.setting(Configuration.LOAN_LIMIT)  # type: ignore

        future = utc_now() + timedelta(hours=1)

        # This patron has two loans that count towards the loan limit
        patron = circulation_api.patron
        circulation_api.pool.loan_to(circulation_api.patron, end=future)
        pool2 = circulation_api.db.licensepool(None)
        pool2.loan_to(circulation_api.patron, end=future)

        # An open-access loan doesn't count towards the limit.
        open_access_pool = circulation_api.db.licensepool(
            None, with_open_access_download=True
        )
        open_access_pool.loan_to(circulation_api.patron)

        # A loan of indefinite duration (no end date) doesn't count
        # towards the limit.
        indefinite_pool = circulation_api.db.licensepool(None)
        indefinite_pool.loan_to(circulation_api.patron, end=None)

        # Another patron's loans don't affect your limit.
        patron2 = circulation_api.db.patron()
        circulation_api.pool.loan_to(patron2)

        # patron_at_loan_limit returns True if your number of relevant
        # loans equals or exceeds the limit.
        m = circulation_api.circulation.patron_at_loan_limit
        assert None == setting.value
        assert False == m(patron)

        setting.value = 1
        assert True == m(patron)
        setting.value = 2
        assert True == m(patron)
        setting.value = 3
        assert False == m(patron)

        # Setting the loan limit to 0 is treated the same as disabling it.
        setting.value = 0
        assert False == m(patron)

        # Another library's setting doesn't affect your limit.
        other_library = circulation_api.db.library()
        other_library.setting(Configuration.LOAN_LIMIT).value = 1  # type: ignore
        assert False == m(patron)

    def test_patron_at_hold_limit(self, circulation_api: CirculationAPIFixture):
        # The hold limit is a per-library setting.
        setting = circulation_api.patron.library.setting(Configuration.HOLD_LIMIT)  # type: ignore

        # Unlike the loan limit, it's pretty simple -- every hold counts towards your limit.
        patron = circulation_api.patron
        circulation_api.pool.on_hold_to(circulation_api.patron)
        pool2 = circulation_api.db.licensepool(None)
        pool2.on_hold_to(circulation_api.patron)

        # Another patron's holds don't affect your limit.
        patron2 = circulation_api.db.patron()
        circulation_api.pool.on_hold_to(patron2)

        # patron_at_hold_limit returns True if your number of holds
        # equals or exceeds the limit.
        m = circulation_api.circulation.patron_at_hold_limit
        assert None == setting.value
        assert False == m(patron)

        setting.value = 1
        assert True == m(patron)
        setting.value = 2
        assert True == m(patron)
        setting.value = 3
        assert False == m(patron)

        # Setting the hold limit to 0 is treated the same as disabling it.
        setting.value = 0
        assert False == m(patron)

        # Another library's setting doesn't affect your limit.
        other_library = circulation_api.db.library()
        other_library.setting(Configuration.HOLD_LIMIT).value = 1  # type: ignore
        assert False == m(patron)

    def test_enforce_limits(self, circulation_api: CirculationAPIFixture):
        # Verify that enforce_limits works whether the patron is at one, both,
        # or neither of their loan limits.

        class MockVendorAPI:
            # Simulate a vendor API so we can watch license pool
            # availability being updated.
            def __init__(self):
                self.availability_updated = []

            def update_availability(self, pool):
                self.availability_updated.append(pool)

        # Set values for loan and hold limit, so we can verify those
        # values are propagated to the circulation exceptions raised
        # when a patron would exceed one of the limits.
        #
        # Both limits are set to the same value for the sake of
        # convenience in testing.
        circulation_api.db.default_library().setting(
            Configuration.LOAN_LIMIT  # type: ignore
        ).value = 12
        circulation_api.db.default_library().setting(
            Configuration.HOLD_LIMIT  # type: ignore
        ).value = 12

        api = MockVendorAPI()

        class Mock(MockCirculationAPI):
            # Mock the loan and hold limit settings, and return a mock
            # CirculationAPI as needed.
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.api = api
                self.api_for_license_pool_calls = []
                self.patron_at_loan_limit_calls = []
                self.patron_at_hold_limit_calls = []
                self.at_loan_limit = False
                self.at_hold_limit = False

            def api_for_license_pool(self, pool):
                # Always return the same mock vendor API
                self.api_for_license_pool_calls.append(pool)
                return self.api

            def patron_at_loan_limit(self, patron):
                # Return the value set for self.at_loan_limit
                self.patron_at_loan_limit_calls.append(patron)
                return self.at_loan_limit

            def patron_at_hold_limit(self, patron):
                # Return the value set for self.at_hold_limit
                self.patron_at_hold_limit_calls.append(patron)
                return self.at_hold_limit

        circulation = Mock(
            circulation_api.db.session, circulation_api.db.default_library()
        )

        # Sub-test 1: patron has reached neither limit.
        #
        patron = circulation_api.patron
        pool = object()
        circulation.at_loan_limit = False
        circulation.at_hold_limit = False

        assert None == circulation.enforce_limits(patron, pool)

        # To determine that the patron is under their limit, it was
        # necessary to call patron_at_loan_limit and
        # patron_at_hold_limit.
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()

        # But it was not necessary to update the availability for the
        # LicensePool, since the patron was not at either limit.
        assert [] == api.availability_updated

        # Sub-test 2: patron has reached both limits.
        #
        circulation.at_loan_limit = True
        circulation.at_hold_limit = True

        # We can't use assert_raises here because we need to examine the
        # exception object to make sure it was properly instantiated.
        def assert_enforce_limits_raises(expected_exception):
            try:
                circulation.enforce_limits(patron, pool)
                raise Exception("Expected a %r" % expected_exception)
            except Exception as e:
                assert isinstance(e, expected_exception)
                # If .limit is set it means we were able to find a
                # specific limit in the database, which means the
                # exception was instantiated correctly.
                #
                # The presence of .limit will let us give a more specific
                # error message when the exception is converted to a
                # problem detail document.
                assert 12 == e.limit

        assert_enforce_limits_raises(PatronLoanLimitReached)  # type: ignore

        # We were able to deduce that the patron can't do anything
        # with this book, without having to ask the API about
        # availability.
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()
        assert [] == api.availability_updated

        # At this point we need to start using a real LicensePool.
        pool = circulation_api.pool

        # Sub-test 3: patron is at loan limit but not hold limit.
        #
        circulation.at_loan_limit = True
        circulation.at_hold_limit = False

        # If the book is available, we get PatronLoanLimitReached
        pool.licenses_available = 1  # type: ignore
        assert_enforce_limits_raises(PatronLoanLimitReached)  # type: ignore

        # Reaching this conclusion required checking both patron
        # limits and asking the remote API for updated availability
        # information for this LicensePool.
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()
        assert pool == api.availability_updated.pop()

        # If the LicensePool is not available, we pass the
        # test. Placing a hold is fine here.
        pool.licenses_available = 0  # type: ignore
        assert None == circulation.enforce_limits(patron, pool)
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()
        assert pool == api.availability_updated.pop()

        # Sub-test 3: patron is at hold limit but not loan limit
        #
        circulation.at_loan_limit = False
        circulation.at_hold_limit = True

        # If the book is not available, we get PatronHoldLimitReached
        pool.licenses_available = 0  # type: ignore
        assert_enforce_limits_raises(PatronHoldLimitReached)  # type: ignore

        # Reaching this conclusion required checking both patron
        # limits and asking the remote API for updated availability
        # information for this LicensePool.
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()
        assert pool == api.availability_updated.pop()

        # If the book is available, we're fine -- we're not at our loan limit.
        pool.licenses_available = 1  # type: ignore
        assert None == circulation.enforce_limits(patron, pool)
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()
        assert pool == api.availability_updated.pop()

    def test_borrow_hold_limit_reached(self, circulation_api: CirculationAPIFixture):
        # Verify that you can't place a hold on an unavailable book
        # if you're at your hold limit.
        #
        # NOTE: This is redundant except as an end-to-end test.

        # The hold limit is 1, and the patron has a previous hold.
        circulation_api.patron.library.setting(Configuration.HOLD_LIMIT).value = 1  # type: ignore
        other_pool = circulation_api.db.licensepool(None)
        other_pool.open_access = False
        other_pool.on_hold_to(circulation_api.patron)

        # The patron wants to take out a loan on an unavailable title.
        circulation_api.pool.licenses_available = 0
        try:
            self.borrow(circulation_api)
        except Exception as e:
            # The result is a PatronHoldLimitReached configured with the
            # library's hold limit.
            assert isinstance(e, PatronHoldLimitReached)  # type: ignore
            assert 1 == e.limit

        # If we increase the limit, borrow succeeds.
        circulation_api.patron.library.setting(Configuration.HOLD_LIMIT).value = 2  # type: ignore
        circulation_api.remote.queue_checkout(NoAvailableCopies())  # type: ignore
        now = utc_now()
        holdinfo = HoldInfo(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.pool.identifier.type,
            circulation_api.pool.identifier.identifier,
            now,
            now + timedelta(seconds=3600),
            10,
        )
        circulation_api.remote.queue_hold(holdinfo)
        loan, hold, is_new = self.borrow(circulation_api)
        assert hold != None

    def test_fulfill_open_access(self, circulation_api: CirculationAPIFixture):
        # Here's an open-access title.
        circulation_api.pool.open_access = True

        # The patron has the title on loan.
        circulation_api.pool.loan_to(circulation_api.patron)

        # It has a LicensePoolDeliveryMechanism that is broken (has no
        # associated Resource).
        broken_lpdm = circulation_api.delivery_mechanism
        assert None == broken_lpdm.resource
        i_want_an_epub = broken_lpdm.delivery_mechanism

        # fulfill_open_access() and fulfill() will both raise
        # FormatNotAvailable.
        pytest.raises(
            FormatNotAvailable,  # type: ignore
            circulation_api.circulation.fulfill_open_access,
            circulation_api.pool,
            i_want_an_epub,
        )

        pytest.raises(
            FormatNotAvailable,  # type: ignore
            circulation_api.circulation.fulfill,
            circulation_api.patron,
            "1234",
            circulation_api.pool,
            broken_lpdm,
            sync_on_failure=False,
        )

        # Let's add a second LicensePoolDeliveryMechanism of the same
        # type which has an associated Resource.
        link, new = circulation_api.pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
            circulation_api.db.fresh_url(),
            circulation_api.pool.data_source,
        )

        working_lpdm = circulation_api.pool.set_delivery_mechanism(
            i_want_an_epub.content_type,
            i_want_an_epub.drm_scheme,
            RightsStatus.GENERIC_OPEN_ACCESS,
            link.resource,
        )

        # It's still not going to work because the Resource has no
        # Representation.
        assert None == link.resource.representation
        pytest.raises(
            FormatNotAvailable,  # type: ignore
            circulation_api.circulation.fulfill_open_access,
            circulation_api.pool,
            i_want_an_epub,
        )

        # Let's add a Representation to the Resource.
        representation, is_new = circulation_api.db.representation(
            link.resource.url,
            i_want_an_epub.content_type,
            "Dummy content",
            mirrored=True,
        )
        link.resource.representation = representation

        # We can finally fulfill a loan.
        result = circulation_api.circulation.fulfill_open_access(
            circulation_api.pool, broken_lpdm
        )
        assert isinstance(result, FulfillmentInfo)
        assert result.content_link == link.resource.representation.public_url
        assert result.content_type == i_want_an_epub.content_type

        # Now, if we try to call fulfill() with the broken
        # LicensePoolDeliveryMechanism we get a result from the
        # working DeliveryMechanism with the same format.
        result = circulation_api.circulation.fulfill(
            circulation_api.patron, "1234", circulation_api.pool, broken_lpdm
        )
        assert isinstance(result, FulfillmentInfo)
        assert result.content_link == link.resource.representation.public_url
        assert result.content_type == i_want_an_epub.content_type

        # We get the right result even if the code calling
        # fulfill_open_access() is incorrectly written and passes in
        # the broken LicensePoolDeliveryMechanism (as opposed to its
        # generic DeliveryMechanism).
        result = circulation_api.circulation.fulfill_open_access(
            circulation_api.pool, broken_lpdm
        )
        assert isinstance(result, FulfillmentInfo)
        assert result.content_link == link.resource.representation.public_url
        assert result.content_type == i_want_an_epub.content_type

        # If we change the working LPDM so that it serves a different
        # media type than the one we're asking for, we're back to
        # FormatNotAvailable errors.
        irrelevant_delivery_mechanism, ignore = DeliveryMechanism.lookup(
            circulation_api.db.session,
            "application/some-other-type",
            DeliveryMechanism.NO_DRM,
        )
        working_lpdm.delivery_mechanism = irrelevant_delivery_mechanism
        pytest.raises(
            FormatNotAvailable,  # type: ignore
            circulation_api.circulation.fulfill_open_access,
            circulation_api.pool,
            i_want_an_epub,
        )

    def test_fulfilment_of_unlimited_access_book_succeeds(
        self, circulation_api: CirculationAPIFixture
    ):
        """Ensure that unlimited access books that don't belong to collections
        having a custom CirculationAPI implementation (e.g., OPDS 1.x, OPDS 2.x collections)
        are fulfilled in the same way as OA and self-hosted books."""
        # Reset the API map, this book belongs to the "basic" collection,
        # i.e. collection without a custom CirculationAPI implementation.
        circulation_api.circulation.api_for_license_pool = MagicMock(return_value=None)

        # Mark the book as unlimited access.
        circulation_api.pool.unlimited_access = True

        media_type = MediaTypes.EPUB_MEDIA_TYPE

        # Create a borrow link.
        link, _ = circulation_api.pool.identifier.add_link(
            Hyperlink.BORROW,
            circulation_api.db.fresh_url(),
            circulation_api.pool.data_source,
        )

        # Create a license pool delivery mechanism.
        circulation_api.pool.set_delivery_mechanism(
            media_type,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
            link.resource,
        )

        # Create a representation.
        representation, _ = circulation_api.db.representation(
            link.resource.url, media_type, "Dummy content", mirrored=True
        )
        link.resource.representation = representation

        # Act
        circulation_api.pool.loan_to(circulation_api.patron)

        result = circulation_api.circulation.fulfill(
            circulation_api.patron,
            "1234",
            circulation_api.pool,
            circulation_api.pool.delivery_mechanisms[0],
        )

        # The fulfillment looks good.
        assert isinstance(result, FulfillmentInfo)
        assert result.content_link == link.resource.representation.public_url
        assert result.content_type == media_type

    def test_fulfill(self, circulation_api: CirculationAPIFixture):
        circulation_api.pool.loan_to(circulation_api.patron)

        fulfillment = circulation_api.pool.delivery_mechanisms[0]
        fulfillment.content = "Fulfilled."
        fulfillment.content_link = None
        circulation_api.remote.queue_fulfill(fulfillment)

        result = circulation_api.circulation.fulfill(
            circulation_api.patron,
            "1234",
            circulation_api.pool,
            circulation_api.pool.delivery_mechanisms[0],
        )

        # The fulfillment looks good.
        assert fulfillment == result

        # An analytics event was created.
        assert 1 == circulation_api.analytics.count
        assert CirculationEvent.CM_FULFILL == circulation_api.analytics.event_type

    def test_fulfill_without_loan(self, circulation_api: CirculationAPIFixture):
        # By default, a title cannot be fulfilled unless there is an active
        # loan for the title (tested above, in test_fulfill).
        fulfillment = circulation_api.pool.delivery_mechanisms[0]
        fulfillment.content = "Fulfilled."
        fulfillment.content_link = None
        circulation_api.remote.queue_fulfill(fulfillment)

        def try_to_fulfill():
            # Note that we're passing None for `patron`.
            return circulation_api.circulation.fulfill(
                None,
                "1234",
                circulation_api.pool,
                circulation_api.pool.delivery_mechanisms[0],
            )

        pytest.raises(NoActiveLoan, try_to_fulfill)  # type: ignore

        # However, if CirculationAPI.can_fulfill_without_loan() says it's
        # okay, the title will be fulfilled anyway.
        def yes_we_can(*args, **kwargs):
            return True

        circulation_api.circulation.can_fulfill_without_loan = yes_we_can
        result = try_to_fulfill()
        assert fulfillment == result

    @pytest.mark.parametrize(
        "open_access, self_hosted", [(True, False), (False, True), (False, False)]
    )
    def test_revoke_loan(
        self, circulation_api: CirculationAPIFixture, open_access, self_hosted
    ):
        circulation_api.pool.open_access = open_access
        circulation_api.pool.self_hosted = self_hosted

        circulation_api.patron.last_loan_activity_sync = utc_now()
        circulation_api.pool.loan_to(circulation_api.patron)
        circulation_api.remote.queue_checkin(True)

        result = circulation_api.circulation.revoke_loan(
            circulation_api.patron, "1234", circulation_api.pool
        )
        assert True == result

        # The patron's loan activity is now out of sync.
        assert None == circulation_api.patron.last_loan_activity_sync

        # An analytics event was created.
        assert 1 == circulation_api.analytics.count
        assert CirculationEvent.CM_CHECKIN == circulation_api.analytics.event_type

    @pytest.mark.parametrize(
        "open_access, self_hosted", [(True, False), (False, True), (False, False)]
    )
    def test_release_hold(
        self, circulation_api: CirculationAPIFixture, open_access, self_hosted
    ):
        circulation_api.pool.open_access = open_access
        circulation_api.pool.self_hosted = self_hosted

        circulation_api.patron.last_loan_activity_sync = utc_now()
        circulation_api.pool.on_hold_to(circulation_api.patron)
        circulation_api.remote.queue_release_hold(True)

        result = circulation_api.circulation.release_hold(
            circulation_api.patron, "1234", circulation_api.pool
        )
        assert True == result

        # The patron's loan activity is now out of sync.
        assert None == circulation_api.patron.last_loan_activity_sync

        # An analytics event was created.
        assert 1 == circulation_api.analytics.count
        assert CirculationEvent.CM_HOLD_RELEASE == circulation_api.analytics.event_type

    def test__collect_event(self, circulation_api: CirculationAPIFixture):
        # Test the _collect_event method, which gathers information
        # from the current request and sends out the appropriate
        # circulation events.
        class MockAnalytics:
            def __init__(self):
                self.events = []

            def collect_event(self, library, licensepool, name, neighborhood):
                self.events.append((library, licensepool, name, neighborhood))
                return True

        analytics = MockAnalytics()

        l1 = circulation_api.db.default_library()
        l2 = circulation_api.db.library()

        p1 = circulation_api.db.patron(library=l1)
        p2 = circulation_api.db.patron(library=l2)

        lp1 = circulation_api.db.licensepool(edition=None)
        lp2 = circulation_api.db.licensepool(edition=None)

        api = CirculationAPI(circulation_api.db.session, l1, analytics)

        def assert_event(inp, outp):
            # Assert that passing `inp` into the mock _collect_event
            # method calls collect_event() on the MockAnalytics object
            # with `outp` as the arguments

            # Call the method
            api._collect_event(*inp)

            # Check the 'event' that was created inside the method.
            assert outp == analytics.events.pop()

            # Validate that only one 'event' was created.
            assert [] == analytics.events

        # Worst case scenario -- the only information we can find is
        # the Library associated with the CirculationAPI object itself.
        assert_event((None, None, "event"), (l1, None, "event", None))

        # If a LicensePool is provided, it's passed right through
        # to Analytics.collect_event.
        assert_event((None, lp2, "event"), (l1, lp2, "event", None))

        # If a Patron is provided, their Library takes precedence over
        # the Library associated with the CirculationAPI (though this
        # shouldn't happen).
        assert_event((p2, None, "event"), (l2, None, "event", None))

        # We must run the rest of the tests in a simulated Flask request
        # context.
        app = Flask(__name__)
        with app.test_request_context():
            # The request library takes precedence over the Library
            # associated with the CirculationAPI (though this
            # shouldn't happen).
            flask.request.library = l2  # type: ignore
            assert_event((None, None, "event"), (l2, None, "event", None))

        with app.test_request_context():
            # The library of the request patron also takes precedence
            # over both (though again, this shouldn't happen).
            flask.request.library = l1  # type: ignore
            flask.request.patron = p2  # type: ignore
            assert_event((None, None, "event"), (l2, None, "event", None))

        # Now let's check neighborhood gathering.
        p2.neighborhood = "Compton"
        with app.test_request_context():
            # Neighborhood is only gathered if we explicitly ask for
            # it.
            flask.request.patron = p2  # type: ignore
            assert_event((p2, None, "event"), (l2, None, "event", None))
            assert_event((p2, None, "event", False), (l2, None, "event", None))
            assert_event((p2, None, "event", True), (l2, None, "event", "Compton"))

            # Neighborhood is not gathered if the request's active
            # patron is not the patron who triggered the event.
            assert_event((p1, None, "event", True), (l1, None, "event", None))

        with app.test_request_context():
            # Even if we ask for it, neighborhood is not gathered if
            # the data isn't available.
            flask.request.patron = p1  # type: ignore
            assert_event((p1, None, "event", True), (l1, None, "event", None))

        # Finally, remove the mock Analytics object entirely and
        # verify that calling _collect_event doesn't cause a crash.
        api.analytics = None
        api._collect_event(p1, None, "event")

    def test_sync_bookshelf_ignores_local_loan_with_no_identifier(
        self, circulation_api: CirculationAPIFixture
    ):
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)
        loan.start = self.YESTERDAY
        circulation_api.pool.identifier = None

        # Verify that we can sync without crashing.
        self.sync_bookshelf(circulation_api)

        # The invalid loan was ignored and is still there.
        loans = circulation_api.db.session.query(Loan).all()
        assert [loan] == loans

        # Even worse - the loan has no license pool!
        loan.license_pool = None

        # But we can still sync without crashing.
        self.sync_bookshelf(circulation_api)

    def test_sync_bookshelf_ignores_local_hold_with_no_identifier(
        self, circulation_api: CirculationAPIFixture
    ):
        hold, ignore = circulation_api.pool.on_hold_to(circulation_api.patron)
        circulation_api.pool.identifier = None

        # Verify that we can sync without crashing.
        self.sync_bookshelf(circulation_api)

        # The invalid hold was ignored and is still there.
        holds = circulation_api.db.session.query(Hold).all()
        assert [hold] == holds

        # Even worse - the hold has no license pool!
        hold.license_pool = None

        # But we can still sync without crashing.
        self.sync_bookshelf(circulation_api)

    def test_sync_bookshelf_with_old_local_loan_and_no_remote_loan_deletes_local_loan(
        self, circulation_api: CirculationAPIFixture
    ):
        # Local loan that was created yesterday.
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)
        loan.start = self.YESTERDAY

        # The loan is in the db.
        loans = circulation_api.db.session.query(Loan).all()
        assert [loan] == loans

        self.sync_bookshelf(circulation_api)

        # Now the local loan is gone.
        loans = circulation_api.db.session.query(Loan).all()
        assert [] == loans

    def test_sync_bookshelf_with_new_local_loan_and_no_remote_loan_keeps_local_loan(
        self, circulation_api: CirculationAPIFixture
    ):
        # Local loan that was just created.
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)
        loan.start = utc_now()

        # The loan is in the db.
        loans = circulation_api.db.session.query(Loan).all()
        assert [loan] == loans

        self.sync_bookshelf(circulation_api)

        # The loan is still in the db, since it was just created.
        loans = circulation_api.db.session.query(Loan).all()
        assert [loan] == loans

    def test_sync_bookshelf_with_incomplete_remotes_keeps_local_loan(
        self, circulation_api: CirculationAPIFixture
    ):
        circulation_api.patron.last_loan_activity_sync = utc_now()
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)
        loan.start = self.YESTERDAY

        class IncompleteCirculationAPI(MockCirculationAPI):
            def patron_activity(self, patron, pin):
                # A remote API failed, and we don't know if
                # the patron has any loans or holds.
                return [], [], False

        circulation = IncompleteCirculationAPI(
            circulation_api.db.session,
            circulation_api.db.default_library(),
            api_map={ExternalIntegration.BIBLIOTHECA: MockBibliothecaAPI},
        )
        circulation.sync_bookshelf(circulation_api.patron, "1234")

        # The loan is still in the db, since there was an
        # error from one of the remote APIs and we don't have
        # complete loan data.
        loans = circulation_api.db.session.query(Loan).all()
        assert [loan] == loans

        # Since we don't have complete loan data,
        # patron.last_loan_activity_sync has been cleared out -- we know
        # the data we have is unreliable.
        assert None == circulation_api.patron.last_loan_activity_sync

        class CompleteCirculationAPI(MockCirculationAPI):
            def patron_activity(self, patron, pin):
                # All the remote API calls succeeded, so
                # now we know the patron has no loans.
                return [], [], True

        circulation = CompleteCirculationAPI(
            circulation_api.db.session,
            circulation_api.db.default_library(),
            api_map={ExternalIntegration.BIBLIOTHECA: MockBibliothecaAPI},
        )
        circulation.sync_bookshelf(circulation_api.patron, "1234")

        # Now the loan is gone.
        loans = circulation_api.db.session.query(Loan).all()
        assert [] == loans

        # Since we know our picture of the patron's bookshelf is up-to-date,
        # patron.last_loan_activity_sync has been set to the current time.
        now = utc_now()
        assert (
            now - circulation_api.patron.last_loan_activity_sync
        ).total_seconds() < 2

    def test_sync_bookshelf_updates_local_loan_and_hold_with_modified_timestamps(
        self, circulation_api: CirculationAPIFixture
    ):
        # We have a local loan that supposedly runs from yesterday
        # until tomorrow.
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)
        loan.start = self.YESTERDAY
        loan.end = self.TOMORROW

        # But the remote thinks the loan runs from today until two
        # weeks from today.
        circulation_api.circulation.add_remote_loan(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            self.TODAY,
            self.IN_TWO_WEEKS,
        )

        # Similar situation for this hold on a different LicensePool.
        edition, pool2 = circulation_api.db.edition(
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
            with_license_pool=True,
            collection=circulation_api.collection,
        )

        hold, ignore = pool2.on_hold_to(circulation_api.patron)
        hold.start = self.YESTERDAY
        hold.end = self.TOMORROW
        hold.position = 10

        circulation_api.circulation.add_remote_hold(
            pool2.collection,
            pool2.data_source,
            pool2.identifier.type,
            pool2.identifier.identifier,
            self.TODAY,
            self.IN_TWO_WEEKS,
            0,
        )
        circulation_api.circulation.sync_bookshelf(circulation_api.patron, "1234")

        # Our local loans and holds have been updated to reflect the new
        # data from the source of truth.
        assert self.TODAY == loan.start
        assert self.IN_TWO_WEEKS == loan.end

        assert self.TODAY == hold.start
        assert self.IN_TWO_WEEKS == hold.end
        assert 0 == hold.position

    def test_sync_bookshelf_applies_locked_delivery_mechanism_to_loan(
        self, circulation_api: CirculationAPIFixture
    ):

        # By the time we hear about the patron's loan, they've already
        # locked in an oddball delivery mechanism.
        mechanism = DeliveryMechanismInfo(
            Representation.TEXT_HTML_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        pool = circulation_api.db.licensepool(None)
        circulation_api.circulation.add_remote_loan(
            pool.collection,
            pool.data_source.name,
            pool.identifier.type,
            pool.identifier.identifier,
            utc_now(),
            None,
            locked_to=mechanism,
        )
        circulation_api.circulation.sync_bookshelf(circulation_api.patron, "1234")

        # The oddball delivery mechanism is now associated with the loan...
        [loan] = circulation_api.patron.loans
        delivery = loan.fulfillment.delivery_mechanism
        assert Representation.TEXT_HTML_MEDIA_TYPE == delivery.content_type
        assert DeliveryMechanism.NO_DRM == delivery.drm_scheme

        # ... and (once we commit) with the LicensePool.
        circulation_api.db.session.commit()
        assert loan.fulfillment in pool.delivery_mechanisms

    def test_sync_bookshelf_respects_last_loan_activity_sync(
        self, circulation_api: CirculationAPIFixture
    ):

        # We believe we have up-to-date loan activity for this patron.
        now = utc_now()
        circulation_api.patron.last_loan_activity_sync = now

        # Little do we know that they just used a vendor website to
        # create a loan.
        circulation_api.circulation.add_remote_loan(
            circulation_api.pool.collection,
            circulation_api.pool.data_source,
            circulation_api.identifier.type,
            circulation_api.identifier.identifier,
            self.YESTERDAY,
            self.IN_TWO_WEEKS,
        )

        # Syncing our loans with the remote won't actually do anything.
        circulation_api.circulation.sync_bookshelf(circulation_api.patron, "1234")
        assert [] == circulation_api.patron.loans

        # But eventually, our local knowledge will grow stale.
        long_ago = now - timedelta(
            seconds=circulation_api.patron.loan_activity_max_age * 2
        )
        circulation_api.patron.last_loan_activity_sync = long_ago

        # At that point, sync_bookshelf _will_ go out to the remote.
        now = utc_now()
        circulation_api.circulation.sync_bookshelf(circulation_api.patron, "1234")
        assert 1 == len(circulation_api.patron.loans)

        # Once that happens, patron.last_loan_activity_sync is updated to
        # the current time.
        updated = circulation_api.patron.last_loan_activity_sync
        assert (updated - now).total_seconds() < 2

        # It's also possible to force a sync even when one wouldn't
        # normally happen, by passing force=True into sync_bookshelf.
        circulation_api.circulation.remote_loans = []

        # A hack to work around the rule that loans not found on
        # remote don't get deleted if they were created in the last 60
        # seconds.
        circulation_api.patron.loans[0].start = long_ago
        circulation_api.db.session.commit()

        circulation_api.circulation.sync_bookshelf(
            circulation_api.patron, "1234", force=True
        )
        assert [] == circulation_api.patron.loans
        assert circulation_api.patron.last_loan_activity_sync > updated

    def test_patron_activity(
        self,
        circulation_api: CirculationAPIFixture,
        api_bibliotheca_files_fixture: BibliothecaFilesFixture,
    ):
        # Get a CirculationAPI that doesn't mock out its API's patron activity.
        circulation = CirculationAPI(
            circulation_api.db.session,
            circulation_api.db.default_library(),
            api_map={ExternalIntegration.BIBLIOTHECA: MockBibliothecaAPI},
        )
        mock_bibliotheca = circulation.api_for_collection[circulation_api.collection.id]
        data = api_bibliotheca_files_fixture.sample_data("checkouts.xml")
        mock_bibliotheca.queue_response(200, content=data)

        loans, holds, complete = circulation.patron_activity(
            circulation_api.patron, "1234"
        )
        assert 2 == len(loans)
        assert 2 == len(holds)
        assert True == complete

        mock_bibliotheca.queue_response(500, content="Error")

        loans, holds, complete = circulation.patron_activity(
            circulation_api.patron, "1234"
        )
        assert 0 == len(loans)
        assert 0 == len(holds)
        assert False == complete

    def test_can_fulfill_without_loan(self, circulation_api: CirculationAPIFixture):
        """Can a title can be fulfilled without an active loan?  It depends on
        the BaseCirculationAPI implementation for that title's colelction.
        """

        class Mock(BaseCirculationAPI):
            def can_fulfill_without_loan(self, patron, pool, lpdm):
                return "yep"

        pool = circulation_api.db.licensepool(None)
        circulation = CirculationAPI(
            circulation_api.db.session, circulation_api.db.default_library()
        )
        circulation.api_for_collection[pool.collection.id] = Mock(
            circulation_api.db.session, circulation_api.db.default_library()
        )
        assert "yep" == circulation.can_fulfill_without_loan(None, pool, object())

        # If format data is missing or the BaseCirculationAPI cannot
        # be found, we assume the title cannot be fulfilled.
        assert False == circulation.can_fulfill_without_loan(None, pool, None)
        assert False == circulation.can_fulfill_without_loan(None, None, object())

        circulation.api_for_collection = {}
        assert False == circulation.can_fulfill_without_loan(None, pool, None)

        # An open access pool can be fulfilled even without the BaseCirculationAPI.
        pool.open_access = True
        assert True == circulation.can_fulfill_without_loan(None, pool, object())


class TestBaseCirculationAPI:
    def test_default_notification_email_address(self, db: DatabaseTransactionFixture):
        # Test the ability to get the default notification email address
        # for a patron or a library.
        db.default_library().setting(
            Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS  # type: ignore
        ).value = "help@library"
        m = BaseCirculationAPI.default_notification_email_address
        assert "help@library" == m(db.default_library(), None)
        assert "help@library" == m(db.patron(), None)
        other_library = db.library()
        assert None == m(other_library, None)

    def test_can_fulfill_without_loan(self, db: DatabaseTransactionFixture):
        """By default, there is a blanket prohibition on fulfilling a title
        when there is no active loan.
        """
        api = BaseCirculationAPI(db.session, db.default_library)
        assert False == api.can_fulfill_without_loan(object(), object(), object())

    def test_patron_email_address(self, db: DatabaseTransactionFixture):
        # Test the method that looks up a patron's actual email address
        # (the one they shared with the library) on demand.
        class Mock(BaseCirculationAPI):
            @classmethod
            def _library_authenticator(self, library):
                self._library_authenticator_called_with = library
                value = BaseCirculationAPI._library_authenticator(library)
                self._library_authenticator_returned = value
                return value

        api = Mock(db.session, db.default_library())
        patron = db.patron()
        library = patron.library

        # In a non-test scenario, a real LibraryAuthenticator is
        # created and used as a source of knowledge about a patron's
        # email address.
        #
        # However, the default library has no authentication providers
        # set up, so the patron has no email address -- there's no one
        # capable of providing an address.
        assert None == api.patron_email_address(patron)
        assert patron.library == api._library_authenticator_called_with
        assert isinstance(api._library_authenticator_returned, LibraryAuthenticator)

        # Now we're going to pass in our own LibraryAuthenticator,
        # which we've populated with mock authentication providers,
        # into a real BaseCirculationAPI.
        api = BaseCirculationAPI(db.session, db.default_library())
        authenticator = LibraryAuthenticator(_db=db.session, library=library)

        # This basic authentication provider _does_ implement
        # remote_patron_lookup, but doesn't provide the crucial
        # information, so still no help.
        class MockBasic:
            def remote_patron_lookup(self, patron):
                self.called_with = patron
                return PatronData(authorization_identifier="patron")

        basic: Union[MockBasic, "MockBasic2"] = MockBasic()

        authenticator.register_basic_auth_provider(basic)
        assert None == api.patron_email_address(
            patron, library_authenticator=authenticator
        )
        assert patron == basic.called_with  # type: ignore

        # This basic authentication provider gives us the information
        # we're after.
        class MockBasic2:
            def remote_patron_lookup(self, patron):
                self.called_with = patron
                return PatronData(email_address="me@email")

        basic = MockBasic2()
        authenticator.basic_auth_provider = basic
        assert "me@email" == api.patron_email_address(
            patron, library_authenticator=authenticator
        )
        assert patron == basic.called_with


class TestDeliveryMechanismInfo:
    def test_apply(self, db: DatabaseTransactionFixture):
        # Here's a LicensePool with one non-open-access delivery mechanism.
        pool = db.licensepool(None)
        assert False == pool.open_access
        [mechanism] = [lpdm.delivery_mechanism for lpdm in pool.delivery_mechanisms]
        assert Representation.EPUB_MEDIA_TYPE == mechanism.content_type
        assert DeliveryMechanism.ADOBE_DRM == mechanism.drm_scheme

        # This patron has the book out on loan, but as far as we no,
        # no delivery mechanism has been set.
        patron = db.patron()
        loan, ignore = pool.loan_to(patron)

        # When consulting with the source of the loan, we learn that
        # the patron has locked the delivery mechanism to a previously
        # unknown mechanism.
        info = DeliveryMechanismInfo(
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        info.apply(loan)

        # This results in the addition of a new delivery mechanism to
        # the LicensePool.
        [new_mechanism] = [
            lpdm.delivery_mechanism
            for lpdm in pool.delivery_mechanisms
            if lpdm.delivery_mechanism != mechanism
        ]
        assert Representation.PDF_MEDIA_TYPE == new_mechanism.content_type
        assert DeliveryMechanism.NO_DRM == new_mechanism.drm_scheme
        assert new_mechanism == loan.fulfillment.delivery_mechanism
        assert RightsStatus.IN_COPYRIGHT == loan.fulfillment.rights_status.uri

        # Calling apply() again with the same arguments does nothing.
        info.apply(loan)
        assert 2 == len(pool.delivery_mechanisms)

        # Although it's extremely unlikely that this will happen in
        # real life, it's possible for this operation to reveal a new
        # *open-access* delivery mechanism for a LicensePool.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
            db.fresh_url(),
            pool.data_source,
            Representation.EPUB_MEDIA_TYPE,
        )

        info = DeliveryMechanismInfo(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            RightsStatus.CC0,
            link.resource,
        )

        # Calling apply() on the loan we were using before will update
        # its associated LicensePoolDeliveryMechanism.
        info.apply(loan)
        [oa_lpdm] = [
            lpdm
            for lpdm in pool.delivery_mechanisms
            if lpdm.delivery_mechanism not in (mechanism, new_mechanism)
        ]
        assert oa_lpdm == loan.fulfillment

        # The correct resource and rights status have been associated
        # with the new LicensePoolDeliveryMechanism.
        assert RightsStatus.CC0 == oa_lpdm.rights_status.uri
        assert link.resource == oa_lpdm.resource

        # The LicensePool is now considered an open-access LicensePool,
        # since it has an open-access delivery mechanism.
        assert True == pool.open_access


class TestConfigurationFailures:
    class MisconfiguredAPI:
        def __init__(self, _db, collection):
            raise CannotLoadConfiguration("doomed!")

    def test_configuration_exception_is_stored(self, db: DatabaseTransactionFixture):
        # If the initialization of an API object raises
        # CannotLoadConfiguration, the exception is stored with the
        # CirculationAPI rather than being propagated.

        api_map = {db.default_collection().protocol: self.MisconfiguredAPI}
        circulation = CirculationAPI(
            db.session,
            db.default_library(),
            api_map=api_map,
        )

        # Although the CirculationAPI was created, it has no functioning
        # APIs.
        assert {} == circulation.api_for_collection

        # Instead, the CannotLoadConfiguration exception raised by the
        # constructor has been stored in initialization_exceptions.
        e = circulation.initialization_exceptions[db.default_collection().id]
        assert isinstance(e, CannotLoadConfiguration)
        assert "doomed!" == str(e)


class TestFulfillmentInfo:
    def test_as_response(self, db: DatabaseTransactionFixture):
        # The default behavior of as_response is to do nothing
        # and let controller code turn the FulfillmentInfo
        # into a Flask Response.
        info = FulfillmentInfo(
            db.default_collection(), None, None, None, None, None, None, None
        )
        assert None == info.as_response


class APIAwareFulfillmentFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.collection = db.default_collection()

        # Create a bunch of mock objects which will be used to initialize
        # the instance variables of MockAPIAwareFulfillmentInfo objects.
        self.mock_data_source_name = object()
        self.mock_identifier_type = object()
        self.mock_identifier = object()
        self.mock_key = object()


@pytest.fixture(scope="function")
def api_aware_fulfillment_fixture(
    db: DatabaseTransactionFixture,
) -> APIAwareFulfillmentFixture:
    return APIAwareFulfillmentFixture(db)


class TestAPIAwareFulfillmentInfo:
    # The APIAwareFulfillmentInfo class has the same properties as a
    # regular FulfillmentInfo -- content_link and so on -- but their
    # values are filled dynamically the first time one of them is
    # accessed, by calling the do_fetch() method.

    class MockAPIAwareFulfillmentInfo(APIAwareFulfillmentInfo):
        """An APIAwareFulfillmentInfo that implements do_fetch() by delegating
        to its API object.
        """

        def do_fetch(self):
            return self.api.do_fetch()

    class MockAPI:
        """An API class that sets a flag when do_fetch()
        is called.
        """

        def __init__(self, collection):
            self.collection = collection
            self.fetch_happened = False

        def do_fetch(self):
            self.fetch_happened = True

    def make_info(
        self, api_aware_fulfillment_fixture: APIAwareFulfillmentFixture, api=None
    ):
        # Create a MockAPIAwareFulfillmentInfo with
        # well-known mock values for its properties.
        return self.MockAPIAwareFulfillmentInfo(
            api,
            api_aware_fulfillment_fixture.mock_data_source_name,
            api_aware_fulfillment_fixture.mock_identifier_type,
            api_aware_fulfillment_fixture.mock_identifier,
            api_aware_fulfillment_fixture.mock_key,
        )

    def test_constructor(
        self, api_aware_fulfillment_fixture: APIAwareFulfillmentFixture
    ):
        data = api_aware_fulfillment_fixture

        # The constructor sets the instance variables appropriately,
        # but does not call do_fetch() or set any of the variables
        # that imply do_fetch() has happened.

        # Create a MockAPI
        api = self.MockAPI(data.collection)

        # Create an APIAwareFulfillmentInfo based on that API.
        info = self.make_info(api_aware_fulfillment_fixture, api)
        assert api == info.api
        assert data.mock_key == info.key
        assert data.collection == api.collection
        assert api.collection == info.collection(data.db.session)
        assert data.mock_data_source_name == info.data_source_name
        assert data.mock_identifier_type == info.identifier_type
        assert data.mock_identifier == info.identifier

        # The fetch has not happened.
        assert False == api.fetch_happened
        assert None == info._content_link
        assert None == info._content_type
        assert None == info._content
        assert None == info._content_expires

    def test_fetch(self, api_aware_fulfillment_fixture: APIAwareFulfillmentFixture):
        data = api_aware_fulfillment_fixture

        # Verify that fetch() calls api.do_fetch()
        api = self.MockAPI(data.collection)
        info = self.make_info(api_aware_fulfillment_fixture, api)
        assert False == info._fetched
        assert False == api.fetch_happened
        info.fetch()
        assert True == info._fetched
        assert True == api.fetch_happened

        # We don't check that values like _content_link were set,
        # because our implementation of do_fetch() doesn't set any of
        # them. Different implementations may set different subsets
        # of these values.

    def test_properties_fetch_on_demand(
        self, api_aware_fulfillment_fixture: APIAwareFulfillmentFixture
    ):
        data = api_aware_fulfillment_fixture

        # Verify that accessing each of the properties calls fetch()
        # if it hasn't been called already.
        api = self.MockAPI(data.collection)
        info = self.make_info(api_aware_fulfillment_fixture, api)
        assert False == info._fetched
        info.content_link
        assert True == info._fetched

        info = self.make_info(api_aware_fulfillment_fixture, api)
        assert False == info._fetched
        info.content_type
        assert True == info._fetched

        info = self.make_info(api_aware_fulfillment_fixture, api)
        assert False == info._fetched
        info.content
        assert True == info._fetched

        info = self.make_info(api_aware_fulfillment_fixture, api)
        assert False == info._fetched
        info.content_expires
        assert True == info._fetched

        # Once the data has been fetched, accessing one of the properties
        # doesn't call fetch() again.
        info.fetch_happened = False
        info.content_expires
        assert False == info.fetch_happened
