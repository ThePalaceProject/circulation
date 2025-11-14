"""Test the CirculationAPI."""

import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import MagicMock, create_autospec

import pytest
from flask import Flask
from freezegun import freeze_time

from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    AuthorizationBlocked,
    AuthorizationExpired,
    CannotRenew,
    CurrentlyAvailable,
    FormatNotAvailable,
    NoActiveLoan,
    NoAvailableCopies,
    NoLicenses,
    OutstandingFines,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePoolType
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.mocks.analytics_provider import MockAnalyticsProvider
from tests.mocks.bibliotheca import MockBibliothecaAPI
from tests.mocks.circulation import (
    MockCirculationApiDispatcher,
)


class CirculationApiDispatcherFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )
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
        self.circulation = MockCirculationApiDispatcher(
            db.session,
            db.default_library(),
            {self.collection.id: MockBibliothecaAPI(db.session, self.collection)},
            analytics=cast(Analytics, self.analytics),
        )
        self.remote = self.circulation.api_for_license_pool(self.pool)


@pytest.fixture(scope="function")
def circulation_api(db: DatabaseTransactionFixture) -> CirculationApiDispatcherFixture:
    return CirculationApiDispatcherFixture(db)


class TestCirculationApiDispatcher:
    YESTERDAY = utc_now() - timedelta(days=1)
    TODAY = utc_now()
    TOMORROW = utc_now() + timedelta(days=1)
    IN_TWO_WEEKS = utc_now() + timedelta(days=14)

    @staticmethod
    def borrow(circulation_api: CirculationApiDispatcherFixture):
        return circulation_api.circulation.borrow(
            circulation_api.patron,
            "1234",
            circulation_api.pool,
            circulation_api.delivery_mechanism,
        )

    def test_borrow_sends_analytics_event(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        now = utc_now()
        loaninfo = LoanInfo.from_license_pool(
            circulation_api.pool,
            start_date=now,
            end_date=now + timedelta(seconds=3600),
            external_identifier=circulation_api.db.fresh_str(),
        )
        circulation_api.remote.queue_checkout(loaninfo)
        now = utc_now()

        loan, hold, is_new = self.borrow(circulation_api)

        # The Loan looks good.
        assert loaninfo.identifier == loan.license_pool.identifier.identifier
        assert circulation_api.patron == loan.patron
        assert hold is None
        assert is_new == True
        assert loaninfo.external_identifier == loan.external_identifier

        # An analytics event was created.
        assert circulation_api.analytics.count == 1
        assert circulation_api.analytics.last_event_type == CirculationEvent.CM_CHECKOUT

        # Try to 'borrow' the same book again.
        circulation_api.remote.queue_checkout(AlreadyCheckedOut())
        loan, hold, is_new = self.borrow(circulation_api)
        assert is_new == False
        assert loaninfo.external_identifier == loan.external_identifier

        # Since the loan already existed, no new analytics event was
        # sent.
        assert 1 == circulation_api.analytics.count

        # Now try to renew the book.
        circulation_api.remote.queue_checkout(loaninfo)
        loan, hold, is_new = self.borrow(circulation_api)
        assert is_new == False

        # Renewals are counted as loans, since from an accounting
        # perspective they _are_ loans.
        assert circulation_api.analytics.count == 2

        # Loans of open-access books go through a different code
        # path, but they count as loans nonetheless.
        circulation_api.pool.open_access = True
        circulation_api.remote.queue_checkout(loaninfo)
        loan, hold, is_new = self.borrow(circulation_api)
        assert circulation_api.analytics.count == 3

    @freeze_time()
    def test_attempt_borrow_with_existing_remote_loan(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        """The patron has a remote loan that the circ manager doesn't know
        about, and they just tried to borrow a book they already have
        a loan for.
        """
        # Remote loan.
        circulation_api.remote.queue_checkout(AlreadyCheckedOut())
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
        assert (loan.start - now).seconds == 0
        assert (loan.end - loan.start).seconds == 3600

    def test_attempt_borrow_with_existing_remote_hold(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        """The patron has a remote hold that the circ manager doesn't know
        about, and they just tried to borrow a book they already have
        on hold.
        """
        # Remote hold.
        circulation_api.remote.queue_checkout(AlreadyOnHold())
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
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        """We have a local loan and a remote loan but the patron tried to
        borrow again -- probably to renew their loan.
        """
        # Local loan.
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)

        # This is the expected behavior in most cases--you tried to
        # renew the loan and failed because it's not time yet.
        circulation_api.remote.queue_checkout(CannotRenew())
        with pytest.raises(CannotRenew) as excinfo:
            self.borrow(circulation_api)
        assert "CannotRenew" in str(excinfo.value)

    def test_attempt_renew_with_local_loan_and_no_available_copies(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        """We have a local loan and a remote loan but the patron tried to
        borrow again -- probably to renew their loan.
        """
        # Local loan.
        loan, ignore = circulation_api.pool.loan_to(circulation_api.patron)

        # Remote loan.

        # NoAvailableCopies can happen if there are already people
        # waiting in line for the book. This case gives a more
        # specific error message.
        #
        # Contrast with the way NoAvailableCopies is handled in
        # test_loan_becomes_hold_if_no_available_copies.
        circulation_api.remote.queue_checkout(NoAvailableCopies())
        with pytest.raises(CannotRenew) as excinfo:
            self.borrow(circulation_api)
        assert "You cannot renew a loan if other patrons have the work on hold." in str(
            excinfo.value
        )

    def test_loan_becomes_hold_if_no_available_copies(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        # We want to borrow this book but there are no copies.
        circulation_api.remote.queue_checkout(NoAvailableCopies())
        holdinfo = HoldInfo.from_license_pool(
            circulation_api.pool,
            hold_position=10,
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
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        # There are no available copies, but the remote API
        # places a hold for us right away instead of raising
        # an error.
        holdinfo = HoldInfo.from_license_pool(
            circulation_api.pool,
            hold_position=10,
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
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        # Attempting to borrow a book will trigger a vendor-side loan
        # limit.
        circulation_api.remote.queue_checkout(PatronLoanLimitReached())

        # But the point is moot because the book isn't even available.
        # Attempting to place a hold will succeed.
        holdinfo = HoldInfo.from_license_pool(
            circulation_api.pool,
            hold_position=10,
        )
        circulation_api.remote.queue_hold(holdinfo)

        loan, hold, is_new = self.borrow(circulation_api)

        # No exception was raised, and the Hold looks good.
        assert holdinfo.identifier == hold.license_pool.identifier.identifier
        assert circulation_api.patron == hold.patron
        assert None == loan
        assert True == is_new

    def test_loan_exception_reraised_if_hold_placement_fails(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        # Attempting to borrow a book will trigger a vendor-side loan
        # limit.
        circulation_api.remote.queue_checkout(PatronLoanLimitReached())

        # Attempting to place a hold will fail because the book is
        # available. (As opposed to the previous test, where the book
        # was _not_ available and hold placement succeeded.) This
        # indicates that we should have raised PatronLoanLimitReached
        # in the first place.
        circulation_api.remote.queue_hold(CurrentlyAvailable())

        assert len(circulation_api.remote.responses["checkout"]) == 1
        assert len(circulation_api.remote.responses["hold"]) == 1

        # The exception raised is PatronLoanLimitReached, the first
        # one we encountered...
        pytest.raises(PatronLoanLimitReached, lambda: self.borrow(circulation_api))

        # ...but we made both requests and have no more responses
        # queued.
        assert not circulation_api.remote.responses["checkout"]
        assert not circulation_api.remote.responses["hold"]

    def test_hold_sends_analytics_event(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        circulation_api.remote.queue_checkout(NoAvailableCopies())
        holdinfo = HoldInfo.from_license_pool(
            circulation_api.pool,
            hold_position=10,
        )
        circulation_api.remote.queue_hold(holdinfo)

        loan, hold, is_new = self.borrow(circulation_api)

        # The Hold looks good.
        assert holdinfo.identifier == hold.license_pool.identifier.identifier
        assert circulation_api.patron == hold.patron
        assert loan is None
        assert is_new == True

        # An analytics event was created.
        assert 1 == circulation_api.analytics.count
        assert (
            circulation_api.analytics.last_event_type == CirculationEvent.CM_HOLD_PLACE
        )

        # Try to 'borrow' the same book again.
        circulation_api.remote.queue_checkout(AlreadyOnHold())
        loan, hold, is_new = self.borrow(circulation_api)
        assert is_new == False

        # Since the hold already existed, no new analytics event was
        # sent.
        assert circulation_api.analytics.count == 1

    def test_hold_is_ready_converts_to_loan_on_borrow(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        now = utc_now()
        loaninfo = LoanInfo.from_license_pool(
            circulation_api.pool,
            start_date=now,
            end_date=now + timedelta(seconds=3600),
            external_identifier=circulation_api.db.fresh_str(),
        )
        circulation_api.remote.queue_checkout(loaninfo)
        circulation_api.pool.on_hold_to(patron=circulation_api.patron, position=0)
        loan, hold, is_new = self.borrow(circulation_api)

        # The Hold is gone and there is a new loan.
        assert loan is not None
        assert hold is None
        assert is_new is True

        assert circulation_api.analytics.count == 2
        # A hold converted analytics event was recorded
        assert (
            circulation_api.analytics.event_types[0]
            == CirculationEvent.CM_HOLD_CONVERTED_TO_LOAN
        )
        # A check event was recorded
        assert circulation_api.analytics.event_types[1] == CirculationEvent.CM_CHECKOUT

    def test_borrow_with_expired_card_fails(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        # This checkout would succeed...
        # We use local time here, rather than UTC time, because we use
        # local time when checking for expired cards in authorization_is_active.
        now = datetime.datetime.now()
        loaninfo = LoanInfo.from_license_pool(
            circulation_api.pool,
            start_date=now,
            end_date=now + timedelta(seconds=3600),
        )
        circulation_api.remote.queue_checkout(loaninfo)

        # ...except the patron's library card has expired.
        old_expires = circulation_api.patron.authorization_expires
        yesterday = now - timedelta(days=1)
        circulation_api.patron.authorization_expires = yesterday

        pytest.raises(AuthorizationExpired, lambda: self.borrow(circulation_api))
        circulation_api.patron.authorization_expires = old_expires

    def test_borrow_with_outstanding_fines(
        self,
        circulation_api: CirculationApiDispatcherFixture,
        library_fixture: LibraryFixture,
    ):
        # This checkout would succeed...
        now = utc_now()
        loaninfo = LoanInfo.from_license_pool(
            circulation_api.pool,
            start_date=now,
            end_date=now + timedelta(seconds=3600),
        )
        circulation_api.remote.queue_checkout(loaninfo)

        # ...except the patron has too many fines.
        old_fines = circulation_api.patron.fines
        circulation_api.patron.fines = 1000
        library = circulation_api.db.default_library()
        library_settings = library_fixture.settings(library)
        library_settings.max_outstanding_fines = 0.50

        pytest.raises(OutstandingFines, lambda: self.borrow(circulation_api))

        # Test the case where any amount of fines is too much.
        library_settings.max_outstanding_fines = 0
        pytest.raises(OutstandingFines, lambda: self.borrow(circulation_api))

        # Remove the fine policy, and borrow succeeds.
        library_settings.max_outstanding_fines = None
        loan, i1, i2 = self.borrow(circulation_api)
        assert isinstance(loan, Loan)

        circulation_api.patron.fines = old_fines

    def test_borrow_with_block_fails(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        # This checkout would succeed...
        now = utc_now()
        loaninfo = LoanInfo.from_license_pool(
            circulation_api.pool,
            start_date=now,
            end_date=now + timedelta(seconds=3600),
        )
        circulation_api.remote.queue_checkout(loaninfo)

        # ...except the patron is blocked
        circulation_api.patron.block_reason = "some reason"
        pytest.raises(AuthorizationBlocked, lambda: self.borrow(circulation_api))
        circulation_api.patron.block_reason = None

    def test_no_licenses_prompts_availability_update(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        # Once the library offered licenses for this book, but
        # the licenses just expired.
        circulation_api.remote.queue_checkout(NoLicenses())
        assert [] == circulation_api.remote.availability_updated_for

        # We're not able to borrow the book...
        pytest.raises(NoLicenses, lambda: self.borrow(circulation_api))

        # But the availability of the book gets immediately updated,
        # so that we don't keep offering the book.
        assert [circulation_api.pool] == circulation_api.remote.availability_updated_for

    def test_borrow_calls_enforce_limits(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        # Verify that the normal behavior of CirculationAPI.borrow()
        # is to call enforce_limits() before trying to check out the
        # book.

        mock_api = create_autospec(BaseCirculationAPI)
        mock_api.checkout.side_effect = NotImplementedError()

        mock_circulation = circulation_api.circulation
        mock_circulation.enforce_limits = MagicMock()
        mock_circulation.api_for_license_pool = MagicMock(return_value=mock_api)

        # checkout() raised the expected NotImplementedError
        with pytest.raises(NotImplementedError):
            mock_circulation.borrow(
                circulation_api.patron,
                "",
                circulation_api.pool,
                circulation_api.pool,
                circulation_api.delivery_mechanism,
            )

        # But before that happened, enforce_limits was called once.
        mock_circulation.enforce_limits.assert_called_once_with(
            circulation_api.patron, circulation_api.pool
        )

    def test_patron_at_loan_limit(
        self,
        circulation_api: CirculationApiDispatcherFixture,
        library_fixture: LibraryFixture,
    ):
        # The loan limit is a per-library setting.
        settings = library_fixture.settings(circulation_api.patron.library)

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
        assert settings.loan_limit is None
        assert m(patron) is False

        settings.loan_limit = 1
        assert m(patron) is True
        settings.loan_limit = 2
        assert m(patron) is True
        settings.loan_limit = 3
        assert m(patron) is False

        # Setting the loan limit to 0 is treated the same as disabling it.
        settings.loan_limit = 0
        assert m(patron) is False

        # Another library's setting doesn't affect your limit.
        other_library = circulation_api.db.library()
        library_fixture.settings(other_library).loan_limit = 1
        assert False is m(patron)

    def test_patron_at_hold_limit(
        self,
        circulation_api: CirculationApiDispatcherFixture,
        library_fixture: LibraryFixture,
    ):
        # The hold limit is a per-library setting.
        library = circulation_api.patron.library
        library_settings = library_fixture.settings(library)

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
        assert library.settings.hold_limit == None
        assert m(patron) is False

        library_settings.hold_limit = 1
        assert m(patron) is True
        library_settings.hold_limit = 2
        assert m(patron) is True
        library_settings.hold_limit = 3
        assert m(patron) is False

        # Setting the hold limit to 0 is treated the same as disabling it.
        library_settings.hold_limit = 0
        assert m(patron) is False

        # Another library's setting doesn't affect your limit.
        other_library = library_fixture.library()
        library_fixture.settings(other_library).hold_limit = 1
        assert m(patron) is False

    def test_enforce_limits(
        self,
        circulation_api: CirculationApiDispatcherFixture,
        library_fixture: LibraryFixture,
    ):
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

        mock_settings = library_fixture.mock_settings()
        mock_settings.loan_limit = 12
        mock_settings.hold_limit = 12
        library = library_fixture.library(settings=mock_settings)
        api = MockVendorAPI()

        class Mock(MockCirculationApiDispatcher):
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

        circulation = Mock(circulation_api.db.session, library, {})

        # Sub-test 1: patron has reached neither limit.
        #
        patron = circulation_api.db.patron(library=library)
        pool = MagicMock()
        pool.open_access = False
        pool.type = LicensePoolType.METERED
        pool.licenses_owned = 0
        pool.licenses_available = 0
        pool.unlimited_access = False
        circulation.at_loan_limit = False
        circulation.at_hold_limit = False

        circulation.enforce_limits(patron, pool)

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

        with pytest.raises(PatronLoanLimitReached) as excinfo:
            circulation.enforce_limits(patron, pool)
        # If .limit is set it means we were able to find a
        # specific limit, which means the exception was instantiated
        # correctly.
        #
        # The presence of .limit will let us give a more specific
        # error message when the exception is converted to a
        # problem detail document.
        assert 12 == excinfo.value.limit

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
        pool.licenses_available = 1
        with pytest.raises(PatronLoanLimitReached) as loan_limit_info:
            circulation.enforce_limits(patron, pool)
        assert 12 == loan_limit_info.value.limit

        # Reaching this conclusion required checking both patron
        # limits and asking the remote API for updated availability
        # information for this LicensePool.
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()
        assert pool == api.availability_updated.pop()

        # If the LicensePool is not available, we pass the
        # test. Placing a hold is fine here.
        pool.licenses_available = 0
        circulation.enforce_limits(patron, pool)
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()
        assert pool == api.availability_updated.pop()

        # Sub-test 3: patron is at hold limit but not loan limit
        #
        circulation.at_loan_limit = False
        circulation.at_hold_limit = True

        # If the book is not available, we get PatronHoldLimitReached
        pool.licenses_available = 0
        with pytest.raises(PatronHoldLimitReached) as hold_limit_info:
            circulation.enforce_limits(patron, pool)
        assert 12 == hold_limit_info.value.limit

        # Reaching this conclusion required checking both patron
        # limits and asking the remote API for updated availability
        # information for this LicensePool.
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()
        assert pool == api.availability_updated.pop()

        # If the book is available, we're fine -- we're not at our loan limit.
        pool.licenses_available = 1
        circulation.enforce_limits(patron, pool)
        assert patron == circulation.patron_at_loan_limit_calls.pop()
        assert patron == circulation.patron_at_hold_limit_calls.pop()
        assert pool == api.availability_updated.pop()

    def test_borrow_hold_limit_reached(
        self,
        circulation_api: CirculationApiDispatcherFixture,
        library_fixture: LibraryFixture,
    ):
        # Verify that you can't place a hold on an unavailable book
        # if you're at your hold limit.
        #
        # NOTE: This is redundant except as an end-to-end test.

        # The hold limit is 1, and the patron has a previous hold.
        library_fixture.settings(circulation_api.patron.library).hold_limit = 1
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
            assert isinstance(e, PatronHoldLimitReached)
            assert 1 == e.limit

        # If we increase the limit, borrow succeeds.
        library_fixture.settings(circulation_api.patron.library).hold_limit = 2
        circulation_api.remote.queue_checkout(NoAvailableCopies())
        now = utc_now()
        holdinfo = HoldInfo.from_license_pool(
            circulation_api.pool,
            start_date=now,
            end_date=now + timedelta(seconds=3600),
            hold_position=10,
        )
        circulation_api.remote.queue_hold(holdinfo)
        loan, hold, is_new = self.borrow(circulation_api)
        assert hold != None

    def test_fulfill_errors(self, circulation_api: CirculationApiDispatcherFixture):
        # Here's an open-access title.
        collection = circulation_api.db.collection()
        circulation_api.pool.open_access = True
        circulation_api.pool.collection = collection

        # The patron has the title on loan.
        circulation_api.pool.loan_to(circulation_api.patron)

        # It has a LicensePoolDeliveryMechanism that is broken (has no
        # associated Resource).
        circulation_api.circulation.queue_fulfill(
            circulation_api.pool, FormatNotAvailable()
        )

        # fulfill() will raise FormatNotAvailable.
        pytest.raises(
            FormatNotAvailable,
            circulation_api.circulation.fulfill,
            circulation_api.patron,
            "1234",
            circulation_api.pool,
            circulation_api.delivery_mechanism,
        )

    def test_fulfill(self, circulation_api: CirculationApiDispatcherFixture):
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
        assert result == fulfillment

        # An analytics event was created.
        assert circulation_api.analytics.count == 1
        assert circulation_api.analytics.last_event_type == CirculationEvent.CM_FULFILL

    def test_fulfill_without_loan(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
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

        pytest.raises(NoActiveLoan, try_to_fulfill)

        # However, if CirculationAPI.can_fulfill_without_loan() says it's
        # okay, the title will be fulfilled anyway.
        def yes_we_can(*args, **kwargs):
            return True

        circulation_api.circulation.can_fulfill_without_loan = yes_we_can
        result = try_to_fulfill()
        assert fulfillment == result

    @pytest.mark.parametrize("open_access", [True, False])
    def test_revoke_loan(
        self, circulation_api: CirculationApiDispatcherFixture, open_access
    ):
        circulation_api.pool.open_access = open_access

        circulation_api.pool.loan_to(circulation_api.patron)
        circulation_api.remote.queue_checkin()

        result = circulation_api.circulation.revoke_loan(
            circulation_api.patron, "1234", circulation_api.pool
        )
        assert result == True

        # An analytics event was created.
        assert circulation_api.analytics.count == 1
        assert circulation_api.analytics.last_event_type == CirculationEvent.CM_CHECKIN

    @pytest.mark.parametrize("open_access", [True, False])
    def test_release_hold(
        self, circulation_api: CirculationApiDispatcherFixture, open_access
    ):
        circulation_api.pool.open_access = open_access

        circulation_api.pool.on_hold_to(circulation_api.patron)
        circulation_api.remote.queue_release_hold()

        result = circulation_api.circulation.release_hold(
            circulation_api.patron, "1234", circulation_api.pool
        )
        assert result == True

        # An analytics event was created.
        assert circulation_api.analytics.count == 1
        assert (
            circulation_api.analytics.last_event_type
            == CirculationEvent.CM_HOLD_RELEASE
        )

    def test__collect_event(self, circulation_api: CirculationApiDispatcherFixture):
        # Test the _collect_event method, which gathers information
        # from the current request and sends out the appropriate
        # circulation events.
        class MockAnalytics:
            def __init__(self):
                self.events = []

            def collect_event(self, library, licensepool, name, patron=None):
                self.events.append((library, licensepool, name, patron))
                return True

        analytics = MockAnalytics()

        l1 = circulation_api.db.default_library()
        l2 = circulation_api.db.library()

        p1 = circulation_api.db.patron(library=l1)
        p2 = circulation_api.db.patron(library=l2)

        lp1 = circulation_api.db.licensepool(edition=None)
        lp2 = circulation_api.db.licensepool(edition=None)

        api = circulation_api.circulation
        api.analytics = cast(Analytics, analytics)

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
        assert_event(
            (None, None, "event"),
            (
                l1,
                None,
                "event",
                None,
            ),
        )

        # If a LicensePool is provided, it's passed right through
        # to Analytics.collect_event.
        assert_event(
            (None, lp2, "event"),
            (
                l1,
                lp2,
                "event",
                None,
            ),
        )

        # If a Patron is provided, their Library takes precedence over
        # the Library associated with the CirculationAPI (though this
        # shouldn't happen).
        assert_event(
            (p2, None, "event"),
            (
                l2,
                None,
                "event",
                p2,
            ),
        )

        # We must run the rest of the tests in a simulated Flask request
        # context.
        app = Flask(__name__)
        with app.test_request_context() as ctx:
            # The request library takes precedence over the Library
            # associated with the CirculationAPI (though this
            # shouldn't happen).
            setattr(ctx.request, "library", l2)
            assert_event(
                (None, None, "event"),
                (
                    l2,
                    None,
                    "event",
                    None,
                ),
            )

        with app.test_request_context() as ctx:
            # The library of the request patron also takes precedence
            # over both (though again, this shouldn't happen).
            setattr(ctx.request, "library", l1)
            setattr(ctx.request, "patron", p2)
            assert_event(
                (None, None, "event"),
                (
                    l2,
                    None,
                    "event",
                    p2,
                ),
            )

        # Finally, remove the mock Analytics object entirely and
        # verify that calling _collect_event doesn't cause a crash.
        api.analytics = None
        api._collect_event(p1, None, "event")

    def test_can_fulfill_without_loan(
        self, circulation_api: CirculationApiDispatcherFixture
    ):
        """Can a title can be fulfilled without an active loan?  It depends on
        the BaseCirculationAPI implementation for that title's collection.
        """

        pool = circulation_api.db.licensepool(None)
        mock = create_autospec(BaseCirculationAPI)
        mock.can_fulfill_without_loan = MagicMock(return_value="yep")
        circulation = CirculationApiDispatcher(
            circulation_api.db.session,
            circulation_api.db.default_library(),
            {pool.collection.id: mock},
        )
        assert "yep" == circulation.can_fulfill_without_loan(None, pool, MagicMock())

        # If format data is missing or the BaseCirculationAPI cannot
        # be found, we assume the title cannot be fulfilled.
        assert False == circulation.can_fulfill_without_loan(None, pool, None)
        assert False == circulation.can_fulfill_without_loan(None, None, MagicMock())

        circulation.api_for_collection = {}
        assert False == circulation.can_fulfill_without_loan(None, pool, None)

        # An open access pool can be fulfilled even without the BaseCirculationAPI.
        pool.open_access = True
        assert True == circulation.can_fulfill_without_loan(None, pool, MagicMock())
