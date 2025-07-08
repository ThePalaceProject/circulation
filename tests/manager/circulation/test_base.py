from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from palace.manager.api.bibliotheca import BibliothecaAPI
from palace.manager.circulation.data import HoldInfo, LoanInfo
from palace.manager.data_layer.format import FormatData
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.mocks.circulation import (
    MockBaseCirculationAPI,
    MockPatronActivityCirculationAPI,
)


class TestBaseCirculationAPI:
    def test_default_notification_email_address(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        # Test the ability to get the default notification email address
        # for a patron
        settings = library_fixture.mock_settings()
        settings.default_notification_email_address = "help@library"
        library = library_fixture.library(settings=settings)
        patron = db.patron(library=library)
        api = MockBaseCirculationAPI(db.session, db.default_collection())
        assert "help@library" == api.default_notification_email_address(patron, "")

    def test_can_fulfill_without_loan(self, db: DatabaseTransactionFixture):
        """By default, there is a blanket prohibition on fulfilling a title
        when there is no active loan.
        """
        api = MockBaseCirculationAPI(db.session, db.default_collection())
        assert False == api.can_fulfill_without_loan(
            MagicMock(), MagicMock(), MagicMock()
        )


class PatronActivityCirculationAPIFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.patron = db.patron()
        self.collection = db.collection(protocol=BibliothecaAPI)
        edition, self.pool = db.edition(
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
            with_license_pool=True,
            collection=self.collection,
        )
        self.identifier = self.pool.identifier
        self.api = MockPatronActivityCirculationAPI(db.session, self.collection)
        self.now = utc_now()
        self.yesterday = self.now - timedelta(days=1)
        self.tomorrow = self.now + timedelta(days=1)
        self.in_two_weeks = self.now + timedelta(weeks=2)

    def sync_patron_activity(self) -> None:
        self.api.sync_patron_activity(self.patron, "1234")


@pytest.fixture
def patron_activity_circulation_api(
    db: DatabaseTransactionFixture,
) -> PatronActivityCirculationAPIFixture:
    return PatronActivityCirculationAPIFixture(db)


class TestPatronActivityCirculationAPI:
    def test_sync_patron_activity_with_old_local_loan_and_no_remote_loan_deletes_local_loan(
        self,
        db: DatabaseTransactionFixture,
        patron_activity_circulation_api: PatronActivityCirculationAPIFixture,
    ):
        # Local loan that was created yesterday.
        loan, _ = patron_activity_circulation_api.pool.loan_to(
            patron_activity_circulation_api.patron
        )
        loan.start = patron_activity_circulation_api.yesterday

        # The loan is in the db.
        loans = db.session.query(Loan).all()
        assert [loan] == loans

        patron_activity_circulation_api.sync_patron_activity()

        # Now the local loan is gone.
        loans = db.session.query(Loan).all()
        assert [] == loans

    def test_sync_patron_activity_with_new_local_loan_and_no_remote_loan_keeps_local_loan(
        self,
        db: DatabaseTransactionFixture,
        patron_activity_circulation_api: PatronActivityCirculationAPIFixture,
    ):
        # Local loan that was just created.
        loan, _ = patron_activity_circulation_api.pool.loan_to(
            patron_activity_circulation_api.patron
        )
        loan.start = utc_now()

        # The loan is in the db.
        loans = db.session.query(Loan).all()
        assert [loan] == loans

        patron_activity_circulation_api.sync_patron_activity()

        # The loan is still in the db, since it was just created.
        loans = db.session.query(Loan).all()
        assert [loan] == loans

    def test_sync_patron_activity_updates_local_loan_and_hold_with_modified_timestamps(
        self,
        db: DatabaseTransactionFixture,
        patron_activity_circulation_api: PatronActivityCirculationAPIFixture,
    ):
        # We have a local loan that supposedly runs from yesterday
        # until tomorrow.
        loan, _ = patron_activity_circulation_api.pool.loan_to(
            patron_activity_circulation_api.patron
        )
        loan.start = patron_activity_circulation_api.yesterday
        loan.end = patron_activity_circulation_api.tomorrow

        # But the remote thinks the loan runs from today until two
        # weeks from today.
        patron_activity_circulation_api.api.add_remote_loan(
            LoanInfo.from_license_pool(
                patron_activity_circulation_api.pool,
                start_date=patron_activity_circulation_api.now,
                end_date=patron_activity_circulation_api.in_two_weeks,
            )
        )

        # Similar situation for this hold on a different LicensePool.
        _, pool2 = db.edition(
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
            with_license_pool=True,
            collection=patron_activity_circulation_api.collection,
        )

        hold, ignore = pool2.on_hold_to(patron_activity_circulation_api.patron)
        hold.start = patron_activity_circulation_api.yesterday
        hold.end = patron_activity_circulation_api.tomorrow
        hold.position = 10

        patron_activity_circulation_api.api.add_remote_hold(
            HoldInfo.from_license_pool(
                pool2,
                start_date=patron_activity_circulation_api.now,
                end_date=patron_activity_circulation_api.in_two_weeks,
                hold_position=0,
            )
        )
        patron_activity_circulation_api.sync_patron_activity()

        # Our local loans and holds have been updated to reflect the new
        # data from the source of truth.
        assert loan.start == patron_activity_circulation_api.now
        assert loan.end == patron_activity_circulation_api.in_two_weeks

        assert hold.start == patron_activity_circulation_api.now
        assert hold.end == patron_activity_circulation_api.in_two_weeks
        assert hold.position == 0

    def test_sync_patron_activity_applies_locked_delivery_mechanism_to_loan(
        self,
        db: DatabaseTransactionFixture,
        patron_activity_circulation_api: PatronActivityCirculationAPIFixture,
    ):
        # By the time we hear about the patron's loan, they've already
        # locked in an oddball delivery mechanism.
        mechanism = FormatData(
            content_type=Representation.TEXT_HTML_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.NO_DRM,
        )
        data_source = db.default_collection().data_source
        assert data_source is not None
        pool = db.licensepool(None, data_source_name=data_source.name)
        patron_activity_circulation_api.api.add_remote_loan(
            LoanInfo.from_license_pool(
                pool,
                start_date=utc_now(),
                end_date=None,
                locked_to=mechanism,
            )
        )
        patron_activity_circulation_api.sync_patron_activity()

        # The oddball delivery mechanism is now associated with the loan...
        [loan] = patron_activity_circulation_api.patron.loans
        assert loan.fulfillment is not None
        delivery = loan.fulfillment.delivery_mechanism
        assert Representation.TEXT_HTML_MEDIA_TYPE == delivery.content_type
        assert DeliveryMechanism.NO_DRM == delivery.drm_scheme

        # ... and (once we commit) with the LicensePool.
        db.session.commit()
        assert loan.fulfillment in pool.delivery_mechanisms
