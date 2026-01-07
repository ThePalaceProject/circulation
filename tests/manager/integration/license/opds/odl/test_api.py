from __future__ import annotations

import datetime
import json
import urllib
import uuid
from unittest.mock import MagicMock, create_autospec
from urllib.parse import parse_qs, urlparse

import dateutil
import pytest
from freezegun import freeze_time
from sqlalchemy import delete

from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotFulfill,
    CannotLoan,
    CannotReturn,
    CurrentlyAvailable,
    HoldOnUnlimitedAccess,
    HoldsNotPermitted,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotOnHold,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.api.circulation.fulfillment import (
    DirectFulfillment,
    FetchFulfillment,
    RedirectFulfillment,
)
from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.integration.license.opds.requests import OAuthOpdsRequest
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolDeliveryMechanism,
    LicensePoolType,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.patron import Hold, Loan
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDS2FilesFixture
from tests.fixtures.odl import OPDS2WithODLApiFixture
from tests.mocks.odl import MockOPDS2WithODLApi


class TestOPDS2WithODLApi:
    def test_loan_limit(self, opds2_with_odl_api_fixture: OPDS2WithODLApiFixture):
        """Test the loan limit collection setting"""
        # Set the loan limit
        opds2_with_odl_api_fixture.api.loan_limit = 1

        response = opds2_with_odl_api_fixture.checkout(
            patron=opds2_with_odl_api_fixture.patron,
            pool=opds2_with_odl_api_fixture.work.active_license_pool(),
            create_loan=True,
        )
        # Did the loan take place correctly?
        assert (
            response.identifier
            == opds2_with_odl_api_fixture.work.presentation_edition.primary_identifier.identifier
        )

        # Second loan for the patron should fail due to the loan limit
        work2: Work = opds2_with_odl_api_fixture.create_work(
            opds2_with_odl_api_fixture.collection
        )
        with pytest.raises(PatronLoanLimitReached) as exc:
            opds2_with_odl_api_fixture.checkout(
                patron=opds2_with_odl_api_fixture.patron,
                pool=work2.active_license_pool(),
            )
        assert exc.value.limit == 1

    @pytest.mark.parametrize(
        "open_access,unlimited_access",
        [
            pytest.param(False, True, id="unlimited_access"),
            pytest.param(True, True, id="open_access"),
        ],
    )
    def test_hold_unlimited_access(
        self,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
        open_access: bool,
        unlimited_access: bool,
    ):
        """Tests that placing a hold on an open-access work will always fail,
        since these items are always available to borrow"""
        # Create an open-access work
        pool = opds2_with_odl_api_fixture.work.license_pools[0]
        pool.open_access = open_access
        pool.licenses_owned = 0
        pool.licenses_available = 0
        if unlimited_access:
            pool.type = LicensePoolType.UNLIMITED
        else:
            pool.type = LicensePoolType.METERED

        with pytest.raises(HoldOnUnlimitedAccess):
            opds2_with_odl_api_fixture.place_hold(pool=pool)

    def test_hold_limit(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ):
        """Test the hold limit collection setting"""
        patron1 = db.patron()

        # First checkout with patron1, then place a hold with the test patron
        pool = opds2_with_odl_api_fixture.work.active_license_pool()
        assert pool is not None
        loan_info = opds2_with_odl_api_fixture.checkout(
            patron=patron1, pool=pool, create_loan=True
        )
        assert (
            loan_info.identifier
            == opds2_with_odl_api_fixture.work.presentation_edition.primary_identifier.identifier
        )

        # Set the hold limit to zero (holds disallowed) and ensure hold fails.
        opds2_with_odl_api_fixture.api.hold_limit = 0
        with pytest.raises(HoldsNotPermitted) as exc:
            opds2_with_odl_api_fixture.place_hold(
                opds2_with_odl_api_fixture.patron,
                pool,
            )
        assert exc.value.problem_detail.title is not None
        assert exc.value.problem_detail.detail is not None
        assert "Holds not permitted" in exc.value.problem_detail.title
        assert "Holds are not permitted" in exc.value.problem_detail.detail

        # Set the hold limit to 1.
        opds2_with_odl_api_fixture.api.hold_limit = 1

        hold_response = opds2_with_odl_api_fixture.place_hold(
            opds2_with_odl_api_fixture.patron, pool, create_hold=True
        )
        # Hold was successful
        assert hold_response.hold_position == 1

        # Second work should fail for the test patron due to the hold limit
        work2: Work = opds2_with_odl_api_fixture.create_work(
            opds2_with_odl_api_fixture.collection
        )
        # Generate a license
        opds2_with_odl_api_fixture.setup_license(work2)

        # Do the same, patron1 checkout and test patron hold
        pool = work2.active_license_pool()
        assert pool is not None
        response = opds2_with_odl_api_fixture.checkout(
            patron=patron1, pool=pool, create_loan=True
        )
        assert (
            response.identifier
            == work2.presentation_edition.primary_identifier.identifier
        )

        # Hold should fail
        with pytest.raises(PatronHoldLimitReached) as exc2:
            opds2_with_odl_api_fixture.place_hold(
                opds2_with_odl_api_fixture.patron, pool
            )
        assert exc2.value.limit == 1

        # Set the hold limit to None (unlimited) and ensure hold succeeds.
        opds2_with_odl_api_fixture.api.hold_limit = None
        hold_response = opds2_with_odl_api_fixture.place_hold(
            opds2_with_odl_api_fixture.patron, pool, create_hold=True
        )
        assert hold_response.hold_position == 1

        # Verify that there are now two holds that  our test patron has both of them.
        assert 2 == db.session.query(Hold).count()
        assert (
            2
            == db.session.query(Hold)
            .filter(Hold.patron_id == opds2_with_odl_api_fixture.patron.id)
            .count()
        )

    @pytest.mark.parametrize(
        "status_code",
        [pytest.param(200, id="existing loan"), pytest.param(200, id="new loan")],
    )
    def test__request_loan_status_success(
        self, opds2_with_odl_api_fixture: OPDS2WithODLApiFixture, status_code: int
    ) -> None:
        expected_document = opds2_with_odl_api_fixture.loan_status_document("active")

        opds2_with_odl_api_fixture.mock_http.queue_response(
            status_code, content=expected_document.model_dump_json()
        )
        requested_document = opds2_with_odl_api_fixture.api._request_loan_status(
            "GET", "http://loan"
        )
        assert "GET" == opds2_with_odl_api_fixture.mock_http.requests_methods.pop()
        assert "http://loan" == opds2_with_odl_api_fixture.mock_http.requests.pop()
        assert requested_document == expected_document

    @pytest.mark.parametrize(
        "status, headers, content, exception, expected_log_message",
        [
            pytest.param(
                200,
                {},
                "not json",
                RemoteIntegrationException,
                "Error validating Loan Status Document. 'http://loan' returned and invalid document.",
                id="invalid json",
            ),
            pytest.param(
                200,
                {},
                json.dumps(dict(status="unknown")),
                RemoteIntegrationException,
                "Error validating Loan Status Document. 'http://loan' returned and invalid document.",
                id="invalid document",
            ),
            pytest.param(
                403,
                {"header": "value"},
                "server error",
                RemoteIntegrationException,
                "Error requesting Loan Status Document. 'http://loan' returned status code 403. "
                "Response headers: header: value. Response content: server error.",
                id="bad status code",
            ),
            pytest.param(
                403,
                {"Content-Type": "application/api-problem+json"},
                json.dumps(
                    dict(
                        type="http://problem-detail-uri",
                        title="server error",
                        detail="broken",
                    )
                ),
                RemoteIntegrationException,
                "Error requesting Loan Status Document. 'http://loan' returned status code 403. "
                "Problem Detail: 'http://problem-detail-uri' - server error - broken",
                id="problem detail response",
            ),
        ],
    )
    def test__request_loan_status_errors(
        self,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
        caplog: pytest.LogCaptureFixture,
        status: int,
        headers: dict[str, str],
        content: str,
        exception: type[Exception],
        expected_log_message: str,
    ) -> None:
        # The response can't be parsed as JSON.
        opds2_with_odl_api_fixture.mock_http.queue_response(
            status, headers=headers, content=content
        )
        with pytest.raises(exception):
            opds2_with_odl_api_fixture.api._request_loan_status("GET", "http://loan")
        assert expected_log_message in caplog.text

    def test_checkin_success(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # A patron has a copy of this book checked out.
        opds2_with_odl_api_fixture.setup_license(concurrency=7, available=6)

        loan, _ = opds2_with_odl_api_fixture.license.loan_to(
            opds2_with_odl_api_fixture.patron
        )
        loan.external_identifier = "http://loan/" + db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        # The patron returns the book successfully.
        opds2_with_odl_api_fixture.checkin()
        assert 2 == len(opds2_with_odl_api_fixture.mock_http.requests)
        assert "http://loan" in opds2_with_odl_api_fixture.mock_http.requests[0]
        assert "http://return" == opds2_with_odl_api_fixture.mock_http.requests[1]

        # The pool's availability has increased
        assert 7 == opds2_with_odl_api_fixture.pool.licenses_available

        # The license on the pool has also been updated
        assert 7 == opds2_with_odl_api_fixture.license.checkouts_available

    def test_checkin_success_with_holds_queue(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # A patron has the only copy of this book checked out.
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=0)
        loan, _ = opds2_with_odl_api_fixture.license.loan_to(
            opds2_with_odl_api_fixture.patron
        )
        loan.external_identifier = "http://loan/" + db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        # Another patron has the book on hold.
        patron_with_hold = db.patron()
        opds2_with_odl_api_fixture.pool.patrons_in_hold_queue = 1
        hold, ignore = opds2_with_odl_api_fixture.pool.on_hold_to(
            patron_with_hold, start=utc_now(), end=None, position=1
        )

        # The first patron returns the book successfully.
        opds2_with_odl_api_fixture.checkin()
        assert 2 == len(opds2_with_odl_api_fixture.mock_http.requests)
        assert "http://loan" in opds2_with_odl_api_fixture.mock_http.requests[0]
        assert "http://return" == opds2_with_odl_api_fixture.mock_http.requests[1]

        # Now the license is reserved for the next patron.
        assert 0 == opds2_with_odl_api_fixture.pool.licenses_available
        assert 1 == opds2_with_odl_api_fixture.pool.licenses_reserved
        assert 1 == opds2_with_odl_api_fixture.pool.patrons_in_hold_queue

    def test_checkin_not_checked_out(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # Not checked out locally.
        pytest.raises(
            NotCheckedOut,
            opds2_with_odl_api_fixture.api.checkin,
            opds2_with_odl_api_fixture.patron,
            "pin",
            opds2_with_odl_api_fixture.pool,
        )

        # Not checked out according to the distributor.
        loan, _ = opds2_with_odl_api_fixture.license.loan_to(
            opds2_with_odl_api_fixture.patron
        )
        loan.external_identifier = db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        opds2_with_odl_api_fixture.mock_http.queue_response(
            200,
            content=opds2_with_odl_api_fixture.loan_status_document(
                "revoked"
            ).model_dump_json(),
        )
        # Checking in silently does nothing.
        opds2_with_odl_api_fixture.api.checkin(
            opds2_with_odl_api_fixture.patron,
            "pin",
            opds2_with_odl_api_fixture.pool,
        )

    def test_checkin_cannot_return(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # Not fulfilled yet, but no return link from the distributor.
        loan, ignore = opds2_with_odl_api_fixture.license.loan_to(
            opds2_with_odl_api_fixture.patron
        )
        loan.external_identifier = db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        opds2_with_odl_api_fixture.mock_http.queue_response(
            200,
            content=opds2_with_odl_api_fixture.loan_status_document(
                "ready", return_link=False
            ).model_dump_json(),
        )
        # Checking in raises the CannotReturn exception, since the distributor
        # does not support returning the book.
        with pytest.raises(CannotReturn):
            opds2_with_odl_api_fixture.api.checkin(
                opds2_with_odl_api_fixture.patron,
                "pin",
                opds2_with_odl_api_fixture.pool,
            )

        # If the return link doesn't change the status, we raise the same exception.
        lsd = opds2_with_odl_api_fixture.loan_status_document(
            "ready", return_link="http://return"
        ).model_dump_json()

        opds2_with_odl_api_fixture.mock_http.queue_response(200, content=lsd)
        opds2_with_odl_api_fixture.mock_http.queue_response(200, content=lsd)
        with pytest.raises(CannotReturn):
            opds2_with_odl_api_fixture.api.checkin(
                opds2_with_odl_api_fixture.patron,
                "pin",
                opds2_with_odl_api_fixture.pool,
            )

    def test_checkin_open_access(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # Checking in an open-access book doesn't need to call out to the distributor API.
        oa_work = db.work(
            with_open_access_download=True,
            collection=opds2_with_odl_api_fixture.collection,
        )
        pool = oa_work.license_pools[0]
        loan, ignore = pool.loan_to(opds2_with_odl_api_fixture.patron)

        # make sure that _checkin isn't called since it is not needed for an open access work
        opds2_with_odl_api_fixture.api._checkin = MagicMock(
            side_effect=Exception("Should not be called")
        )

        opds2_with_odl_api_fixture.api.checkin(
            opds2_with_odl_api_fixture.patron, "pin", pool
        )

    def test__notification_url(self):
        short_name = "short_name"
        patron_id = str(uuid.uuid4())
        license_id = str(uuid.uuid4())

        def get_path(path: str) -> str:
            return urlparse(path).path

        # Import the app so we can setup a request context to verify that we can correctly generate
        # notification url via url_for.
        from palace.manager.api.app import app

        # Test that we generated the expected URL
        with app.test_request_context():
            notification_url = OPDS2WithODLApi._notification_url(
                short_name, patron_id, license_id
            )

        assert (
            get_path(notification_url)
            == f"/{short_name}/odl/notify/{patron_id}/{license_id}"
        )

        # Test that our mock generates the same URL
        with app.test_request_context():
            assert get_path(
                OPDS2WithODLApi._notification_url(short_name, patron_id, license_id)
            ) == get_path(
                MockOPDS2WithODLApi._notification_url(short_name, patron_id, license_id)
            )

    def test_checkout_success(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # This book is available to check out.
        opds2_with_odl_api_fixture.setup_license(concurrency=6, available=6, left=30)

        # A patron checks out the book successfully.
        loan_url = db.fresh_str()
        loan = opds2_with_odl_api_fixture.checkout(loan_url=loan_url)

        assert opds2_with_odl_api_fixture.collection == loan.collection(db.session)
        assert opds2_with_odl_api_fixture.pool.identifier.type == loan.identifier_type
        assert opds2_with_odl_api_fixture.pool.identifier.identifier == loan.identifier
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end_date
        assert loan_url == loan.external_identifier

        # The pool's availability and the license's remaining checkouts have decreased.
        assert 5 == opds2_with_odl_api_fixture.pool.licenses_available
        assert 29 == opds2_with_odl_api_fixture.license.checkouts_left

        # The parameters that we templated into the checkout URL are correct.
        requested_url = opds2_with_odl_api_fixture.mock_http.requests.pop()

        parsed = urlparse(requested_url)
        assert "https" == parsed.scheme
        assert "loan.feedbooks.net" == parsed.netloc
        params = parse_qs(parsed.query)

        assert (
            opds2_with_odl_api_fixture.api.settings.passphrase_hint == params["hint"][0]
        )
        assert (
            opds2_with_odl_api_fixture.api.settings.passphrase_hint_url
            == params["hint_url"][0]
        )

        assert opds2_with_odl_api_fixture.license.identifier == params["id"][0]

        # The checkout id is a random UUID.
        checkout_id = params["checkout_id"][0]
        assert uuid.UUID(checkout_id)

        # The patron id is the UUID of the patron, for this distributor.
        expected_patron_id = (
            opds2_with_odl_api_fixture.patron.identifier_to_remote_service(
                opds2_with_odl_api_fixture.pool.data_source
            )
        )
        patron_id = params["patron_id"][0]
        assert uuid.UUID(patron_id)
        assert patron_id == expected_patron_id

        # Loans expire in 21 days by default.
        now = utc_now()
        after_expiration = now + datetime.timedelta(days=23)
        expires = urllib.parse.unquote(params["expires"][0])

        # The expiration time passed to the server is associated with
        # the UTC time zone.
        assert expires.endswith("+00:00")
        expires_t = dateutil.parser.parse(expires)
        assert expires_t.tzinfo == dateutil.tz.tz.tzutc()

        # It's a time in the future, but not _too far_ in the future.
        assert expires_t > now
        assert expires_t < after_expiration

        notification_url = urllib.parse.unquote_plus(params["notification_url"][0])
        expected_notification_url = opds2_with_odl_api_fixture.api._notification_url(
            opds2_with_odl_api_fixture.library.short_name,
            expected_patron_id,
            opds2_with_odl_api_fixture.license.identifier,
        )
        assert notification_url == expected_notification_url

    def test_checkout_open_access(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # This book is available to check out.
        oa_work = db.work(
            with_open_access_download=True,
            collection=opds2_with_odl_api_fixture.collection,
        )
        loan = opds2_with_odl_api_fixture.api_checkout(
            licensepool=oa_work.license_pools[0],
        )

        assert loan.collection(db.session) == opds2_with_odl_api_fixture.collection
        assert loan.identifier == oa_work.license_pools[0].identifier.identifier
        assert loan.identifier_type == oa_work.license_pools[0].identifier.type
        assert loan.start_date is None
        assert loan.end_date is None
        assert loan.external_identifier is None

    def test_checkout_success_with_hold(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # A patron has this book on hold, and the book just became available to check out.
        opds2_with_odl_api_fixture.pool.on_hold_to(
            opds2_with_odl_api_fixture.patron,
            start=utc_now() - datetime.timedelta(days=1),
            end=utc_now() + datetime.timedelta(days=1),
            position=0,
        )
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=1, left=5)

        assert opds2_with_odl_api_fixture.pool.licenses_available == 0
        assert opds2_with_odl_api_fixture.pool.licenses_reserved == 1
        assert opds2_with_odl_api_fixture.pool.patrons_in_hold_queue == 1

        # The patron checks out the book.
        loan_url = db.fresh_str()
        loan = opds2_with_odl_api_fixture.checkout(loan_url=loan_url)

        # The patron gets a loan successfully.
        assert opds2_with_odl_api_fixture.collection == loan.collection(db.session)
        assert opds2_with_odl_api_fixture.pool.identifier.type == loan.identifier_type
        assert opds2_with_odl_api_fixture.pool.identifier.identifier == loan.identifier
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end_date
        assert loan_url == loan.external_identifier

        # The book is no longer reserved for the patron.
        assert 0 == opds2_with_odl_api_fixture.pool.licenses_reserved
        assert 0 == opds2_with_odl_api_fixture.pool.licenses_available
        assert 0 == opds2_with_odl_api_fixture.pool.patrons_in_hold_queue

    def test_checkout_success_external_identifier_fallback(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        # This book is available to check out.
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=1)

        # The server returns a loan status document with no self link, but the license document
        # has a link to the loan status document, so we make the extra request to get the external identifier
        # from the license document.
        opds2_with_odl_api_fixture.mock_http.queue_response(
            201,
            content=opds2_with_odl_api_fixture.loan_status_document(
                self_link=False,
            ).model_dump_json(),
        )
        opds2_with_odl_api_fixture.mock_http.queue_response(
            201, content=opds2_files_fixture.sample_text("lcp/license/ul.json")
        )
        loan = opds2_with_odl_api_fixture.api_checkout()
        assert (
            loan.external_identifier
            == "https://license.example.com/licenses/123-456/status"
        )
        assert len(opds2_with_odl_api_fixture.mock_http.requests) == 2

    def test_checkout_already_checked_out(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        opds2_with_odl_api_fixture.setup_license(concurrency=2, available=1)

        # Checkout succeeds the first time
        opds2_with_odl_api_fixture.checkout(create_loan=True)

        # But raises an exception the second time
        with pytest.raises(AlreadyCheckedOut):
            opds2_with_odl_api_fixture.checkout()

        assert 1 == db.session.query(Loan).count()

    def test_checkout_expired_hold(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # The patron was at the beginning of the hold queue, but the hold already expired.
        yesterday = utc_now() - datetime.timedelta(days=1)
        hold, _ = opds2_with_odl_api_fixture.pool.on_hold_to(
            opds2_with_odl_api_fixture.patron,
            start=yesterday,
            end=yesterday,
            position=0,
        )
        other_hold, _ = opds2_with_odl_api_fixture.pool.on_hold_to(
            db.patron(), start=utc_now()
        )
        opds2_with_odl_api_fixture.setup_license(concurrency=2, available=1)

        with pytest.raises(NoAvailableCopies):
            opds2_with_odl_api_fixture.api_checkout()

    def test_checkout_no_available_copies(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # A different patron has the only copy checked out.
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=0)
        existing_loan, _ = opds2_with_odl_api_fixture.license.loan_to(db.patron())

        with pytest.raises(NoAvailableCopies):
            opds2_with_odl_api_fixture.api_checkout()

        assert 1 == db.session.query(Loan).count()

        db.session.delete(existing_loan)

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(weeks=1)

        # A different patron has the only copy reserved.
        other_patron_hold, _ = opds2_with_odl_api_fixture.pool.on_hold_to(
            db.patron(), position=0, start=last_week
        )
        opds2_with_odl_api_fixture.pool.update_availability_from_licenses()

        with pytest.raises(NoAvailableCopies):
            opds2_with_odl_api_fixture.api_checkout()

        assert 0 == db.session.query(Loan).count()

        # The patron has a hold, but another patron is ahead in the holds queue.
        hold, _ = opds2_with_odl_api_fixture.pool.on_hold_to(
            db.patron(), position=1, start=yesterday
        )
        opds2_with_odl_api_fixture.pool.update_availability_from_licenses()

        with pytest.raises(NoAvailableCopies):
            opds2_with_odl_api_fixture.api_checkout()

        assert 0 == db.session.query(Loan).count()

        # The patron has the first hold, but it's expired.
        hold.start = last_week - datetime.timedelta(days=1)
        hold.end = yesterday
        opds2_with_odl_api_fixture.pool.update_availability_from_licenses()

        with pytest.raises(NoAvailableCopies):
            opds2_with_odl_api_fixture.api_checkout()

        assert 0 == db.session.query(Loan).count()

    @pytest.mark.parametrize(
        "response_type",
        ["application/api-problem+json", "application/problem+json"],
    )
    def test_checkout_no_available_copies_unknown_to_us(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
        response_type: str,
    ) -> None:
        """
        The title has no available copies, but we are out of sync with the distributor, so we think there
        are copies available.
        """
        # We think there are copies available.
        pool = opds2_with_odl_api_fixture.pool
        pool.licenses = []
        license_1 = db.license(pool, terms_concurrency=1, checkouts_available=1)
        license_2 = db.license(pool, checkouts_available=1)
        pool.update_availability_from_licenses()

        # But the distributor says there are no available copies.
        opds2_with_odl_api_fixture.mock_http.queue_response(
            400,
            media_type=response_type,
            content=opds2_with_odl_api_fixture.files.sample_text("unavailable.json"),
        )
        opds2_with_odl_api_fixture.mock_http.queue_response(
            400,
            media_type=response_type,
            content=opds2_with_odl_api_fixture.files.sample_text("unavailable.json"),
        )

        with pytest.raises(NoAvailableCopies):
            opds2_with_odl_api_fixture.api_checkout()

        assert db.session.query(Loan).count() == 0
        assert opds2_with_odl_api_fixture.pool.licenses_available == 0
        assert license_1.checkouts_available == 0
        assert license_2.checkouts_available == 0

    def test_checkout_many_licenses(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        """
        The title has 5 different licenses. Several of them seem to have copies available. But
        we are out of sync, so it turns out that not all of them do.
        """
        # We think there are copies available.
        pool = opds2_with_odl_api_fixture.pool
        pool.licenses = []
        license_unavailable_1 = db.license(
            pool, checkouts_available=2, expires=utc_now() + datetime.timedelta(weeks=4)
        )
        license_unavailable_2 = db.license(
            pool, terms_concurrency=1, checkouts_available=1
        )
        license_untouched = db.license(pool, checkouts_left=1, checkouts_available=1)
        license_lent = db.license(
            pool,
            checkouts_left=4,
            checkouts_available=4,
            expires=utc_now() + datetime.timedelta(weeks=1),
        )
        license_expired = db.license(
            pool,
            terms_concurrency=10,
            checkouts_available=10,
            expires=utc_now() - datetime.timedelta(weeks=1),
        )
        pool.update_availability_from_licenses()
        assert pool.licenses_available == 8

        assert opds2_with_odl_api_fixture.pool.best_available_licenses() == [
            license_unavailable_1,
            license_unavailable_2,
            license_lent,
            license_untouched,
        ]

        # But the distributor says there are no available copies for license_unavailable_1
        opds2_with_odl_api_fixture.mock_http.queue_response(
            400,
            media_type="application/api-problem+json",
            content=opds2_with_odl_api_fixture.files.sample_text("unavailable.json"),
        )
        # And for license_unavailable_2
        opds2_with_odl_api_fixture.mock_http.queue_response(
            400,
            media_type="application/api-problem+json",
            content=opds2_with_odl_api_fixture.files.sample_text("unavailable.json"),
        )
        # But license_lent is still available, and we successfully check it out
        opds2_with_odl_api_fixture.mock_http.queue_response(
            201,
            content=opds2_with_odl_api_fixture.loan_status_document().model_dump_json(),
        )

        loan_info = opds2_with_odl_api_fixture.api_checkout()

        assert opds2_with_odl_api_fixture.pool.licenses_available == 4
        assert license_unavailable_2.checkouts_available == 0
        assert license_unavailable_1.checkouts_available == 0
        assert license_lent.checkouts_available == 3
        assert license_untouched.checkouts_available == 1

        assert loan_info.license_identifier == license_lent.identifier

    def test_checkout_ready_hold_no_available_copies(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        """
        We think there is a hold ready for us, but we are out of sync with the distributor,
        so there actually isn't a copy ready for our hold.
        """
        # We think there is a copy available for this hold.
        hold, _ = opds2_with_odl_api_fixture.pool.on_hold_to(
            opds2_with_odl_api_fixture.patron,
            start=utc_now() - datetime.timedelta(days=1),
            end=utc_now() + datetime.timedelta(days=1),
            position=0,
        )
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=1)

        assert opds2_with_odl_api_fixture.pool.licenses_available == 0
        assert opds2_with_odl_api_fixture.pool.licenses_reserved == 1
        assert opds2_with_odl_api_fixture.pool.patrons_in_hold_queue == 1

        # But the distributor says there are no available copies.
        opds2_with_odl_api_fixture.mock_http.queue_response(
            400,
            media_type="application/api-problem+json",
            content=opds2_with_odl_api_fixture.files.sample_text("unavailable.json"),
        )

        with pytest.raises(NoAvailableCopies):
            opds2_with_odl_api_fixture.api_checkout()

        assert db.session.query(Loan).count() == 0
        assert db.session.query(Hold).count() == 1

        # The availability has been updated.
        assert opds2_with_odl_api_fixture.pool.licenses_available == 0
        assert opds2_with_odl_api_fixture.pool.licenses_reserved == 0
        assert opds2_with_odl_api_fixture.pool.patrons_in_hold_queue == 1

        # The hold has been updated to reflect the new availability.
        assert hold.position == 1
        assert hold.end is None

    def test_checkout_failures(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        # We think there are copies available.
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=1)

        # Test the case where we get bad JSON back from the distributor.
        opds2_with_odl_api_fixture.mock_http.queue_response(
            400,
            media_type="application/api-problem+json",
            content="hot garbage",
        )

        with pytest.raises(BadResponseException):
            opds2_with_odl_api_fixture.api_checkout()

        # Test the case where we just get an unknown bad response.
        opds2_with_odl_api_fixture.mock_http.queue_response(
            500, media_type="text/plain", content="halt and catch fire 🔥"
        )
        with pytest.raises(BadResponseException):
            opds2_with_odl_api_fixture.api_checkout()

    def test_checkout_no_licenses(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=1, left=0)

        with pytest.raises(NoLicenses):
            opds2_with_odl_api_fixture.api_checkout()

        assert 0 == db.session.query(Loan).count()

    def test_checkout_when_all_licenses_expired(
        self, opds2_with_odl_api_fixture: OPDS2WithODLApiFixture
    ) -> None:
        # license expired by expiration date
        opds2_with_odl_api_fixture.setup_license(
            concurrency=1,
            available=2,
            left=1,
            expires=utc_now() - datetime.timedelta(weeks=1),
        )

        with pytest.raises(NoLicenses):
            opds2_with_odl_api_fixture.api_checkout()

        # license expired by no remaining checkouts
        opds2_with_odl_api_fixture.setup_license(
            concurrency=1,
            available=2,
            left=0,
            expires=utc_now() + datetime.timedelta(weeks=1),
        )

        with pytest.raises(NoLicenses):
            opds2_with_odl_api_fixture.api_checkout()

    def test_checkout_cannot_loan(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        opds2_with_odl_api_fixture.mock_http.queue_response(
            200,
            content=opds2_with_odl_api_fixture.loan_status_document(
                "revoked"
            ).model_dump_json(),
        )
        with pytest.raises(CannotLoan):
            opds2_with_odl_api_fixture.api_checkout()
        assert 0 == db.session.query(Loan).count()

        # No external identifier.
        opds2_with_odl_api_fixture.mock_http.queue_response(
            200,
            content=opds2_with_odl_api_fixture.loan_status_document(
                self_link=False, license_link=False
            ).model_dump_json(),
        )
        with pytest.raises(CannotLoan):
            opds2_with_odl_api_fixture.api_checkout()
        assert 0 == db.session.query(Loan).count()

    @pytest.mark.parametrize(
        "content_type, drm_scheme, correct_type, correct_link, links",
        [
            pytest.param(
                "ignored/format",
                DeliveryMechanism.ADOBE_DRM,
                DeliveryMechanism.ADOBE_DRM,
                "http://acsm",
                [
                    {
                        "rel": "license",
                        "href": "http://acsm",
                        "type": DeliveryMechanism.ADOBE_DRM,
                    }
                ],
                id="adobe drm",
            ),
            pytest.param(
                "ignored/format",
                DeliveryMechanism.LCP_DRM,
                DeliveryMechanism.LCP_DRM,
                "http://lcp",
                [
                    {
                        "rel": "license",
                        "href": "http://lcp",
                        "type": DeliveryMechanism.LCP_DRM,
                    }
                ],
                id="lcp drm",
            ),
            pytest.param(
                "application/epub+zip",
                DeliveryMechanism.NO_DRM,
                "application/epub+zip",
                "http://publication",
                [
                    {
                        "rel": "publication",
                        "href": "http://publication",
                        "type": "application/epub+zip",
                    }
                ],
                id="no drm",
            ),
            pytest.param(
                "ignored/format",
                DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
                FEEDBOOKS_AUDIO,
                "http://correct",
                [
                    {
                        "rel": "license",
                        "href": "http://acsm",
                        "type": DeliveryMechanism.ADOBE_DRM,
                    },
                    {
                        "rel": "manifest",
                        "href": "http://correct",
                        "type": FEEDBOOKS_AUDIO,
                    },
                ],
                id="feedbooks audio",
            ),
            pytest.param(
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
                MediaTypes.TEXT_HTML_MEDIA_TYPE,
                "http://streaming",
                [
                    {
                        "rel": "publication",
                        "href": "http://streaming",
                        "type": MediaTypes.TEXT_HTML_MEDIA_TYPE,
                    }
                ],
                id="streaming drm",
            ),
        ],
    )
    def test_fulfill_success(
        self,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
        db: DatabaseTransactionFixture,
        content_type: str,
        drm_scheme: str,
        correct_type: str,
        correct_link: str,
        links: list[dict[str, str]],
    ) -> None:
        # Fulfill a loan in a way that gives access to a license file.
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=1)
        opds2_with_odl_api_fixture.checkout(create_loan=True)

        lpdm = MagicMock(spec=LicensePoolDeliveryMechanism)
        lpdm.delivery_mechanism = MagicMock(spec=DeliveryMechanism)
        lpdm.delivery_mechanism.content_type = content_type
        lpdm.delivery_mechanism.drm_scheme = drm_scheme

        lsd = opds2_with_odl_api_fixture.loan_status_document("active", links=links)
        opds2_with_odl_api_fixture.mock_http.queue_response(
            200, content=lsd.model_dump_json()
        )
        fulfillment = opds2_with_odl_api_fixture.api.fulfill(
            opds2_with_odl_api_fixture.patron,
            "pin",
            opds2_with_odl_api_fixture.pool,
            lpdm,
        )
        if drm_scheme in (DeliveryMechanism.NO_DRM, DeliveryMechanism.STREAMING_DRM):
            assert isinstance(fulfillment, RedirectFulfillment)
        else:
            assert isinstance(fulfillment, FetchFulfillment)
            assert fulfillment.allowed_response_codes == ["2xx"]

        assert correct_link == fulfillment.content_link
        assert correct_type == fulfillment.content_type

    def test_fulfill_streaming_unsupported_content_type(
        self,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        """Test that an unsupported streaming content type raises CannotFulfill."""
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=1)
        opds2_with_odl_api_fixture.checkout(create_loan=True)

        lpdm = MagicMock(spec=LicensePoolDeliveryMechanism)
        lpdm.delivery_mechanism = MagicMock(spec=DeliveryMechanism)
        lpdm.delivery_mechanism.content_type = "unsupported/content-type"
        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.STREAMING_DRM

        lsd = opds2_with_odl_api_fixture.loan_status_document("active", links=[])
        opds2_with_odl_api_fixture.mock_http.queue_response(
            200, content=lsd.model_dump_json()
        )

        with pytest.raises(CannotFulfill) as exc_info:
            opds2_with_odl_api_fixture.api.fulfill(
                opds2_with_odl_api_fixture.patron,
                "pin",
                opds2_with_odl_api_fixture.pool,
                lpdm,
            )
        assert (
            exc_info.value.message == "The requested streaming format is not available."
        )

    def test_fulfill_open_access(
        self,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
        db: DatabaseTransactionFixture,
    ) -> None:
        oa_work = db.work(
            with_open_access_download=True,
            collection=opds2_with_odl_api_fixture.collection,
        )
        pool = oa_work.license_pools[0]
        loan, ignore = pool.loan_to(opds2_with_odl_api_fixture.patron)

        # If we can't find a delivery mechanism, we can't fulfill the loan.
        mock_lpdm = MagicMock(
            spec=LicensePoolDeliveryMechanism,
            delivery_mechanism=MagicMock(drm_scheme=None),
        )
        with pytest.raises(CannotFulfill):
            opds2_with_odl_api_fixture.api.fulfill(
                opds2_with_odl_api_fixture.patron, "pin", pool, mock_lpdm
            )

        lpdm = pool.delivery_mechanisms[0]
        fulfillment = opds2_with_odl_api_fixture.api.fulfill(
            opds2_with_odl_api_fixture.patron, "pin", pool, lpdm
        )

        assert isinstance(fulfillment, RedirectFulfillment)
        assert fulfillment.content_link is not None
        assert fulfillment.content_type == lpdm.delivery_mechanism.content_type

    @freeze_time()
    def test_fulfill_bearer_token(
        self,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
        db: DatabaseTransactionFixture,
    ) -> None:
        work = db.work()
        pool = db.licensepool(
            work.presentation_edition,
            work=work,
            unlimited_access=True,
            with_open_access_download=True,
            collection=opds2_with_odl_api_fixture.collection,
        )
        url = "http://test.com/" + db.fresh_str()
        media_type = MediaTypes.EPUB_MEDIA_TYPE
        link, new = pool.identifier.add_link(
            Hyperlink.GENERIC_OPDS_ACQUISITION, url, pool.data_source, media_type
        )

        # Add a DeliveryMechanism for this download
        lpdm = pool.set_delivery_mechanism(
            media_type,
            DeliveryMechanism.BEARER_TOKEN,
            RightsStatus.IN_COPYRIGHT,
            link.resource,
        )

        pool.loan_to(opds2_with_odl_api_fixture.patron)

        # If the collection isn't configured to use OAuth, we can't fulfill the loan.
        with pytest.raises(CannotFulfill):
            opds2_with_odl_api_fixture.api.fulfill(
                opds2_with_odl_api_fixture.patron, "pin", pool, lpdm
            )

        # Configure API to use OAuth
        token = OAuthTokenResponse(
            access_token="token",
            expires_in=3600,
            token_type="Bearer",
        )
        request = OAuthOpdsRequest("http://feed.com/url", "username", "password")
        request._token_url = "mock token url"
        mock_refresh = create_autospec(
            request._oauth_session_token_refresh, return_value=token
        )
        request._oauth_session_token_refresh = mock_refresh
        opds2_with_odl_api_fixture.api._request = request

        fulfillment = opds2_with_odl_api_fixture.api.fulfill(
            opds2_with_odl_api_fixture.patron, "pin", pool, lpdm
        )

        assert isinstance(fulfillment, DirectFulfillment)
        assert fulfillment.content_type == DeliveryMechanism.BEARER_TOKEN
        assert fulfillment.content is not None
        token_doc = json.loads(fulfillment.content)
        assert token_doc.get("access_token") == token.access_token
        assert token_doc.get("expires_in") == int(
            (token.expires - utc_now()).total_seconds()
        )
        assert token_doc.get("token_type") == "Bearer"
        assert token_doc.get("location") == url
        assert mock_refresh.call_count == 1

        # A second call to fulfill should not refresh the token
        fulfillment_2 = opds2_with_odl_api_fixture.api.fulfill(
            opds2_with_odl_api_fixture.patron, "pin", pool, lpdm
        )
        assert isinstance(fulfillment_2, DirectFulfillment)
        assert fulfillment_2.content == fulfillment.content
        assert mock_refresh.call_count == 1

    @pytest.mark.parametrize(
        "status_document",
        [
            pytest.param(
                OPDS2WithODLApiFixture.loan_status_document("revoked"),
                id="revoked",
            ),
            pytest.param(
                OPDS2WithODLApiFixture.loan_status_document("cancelled"),
                id="cancelled",
            ),
            pytest.param(
                OPDS2WithODLApiFixture.loan_status_document("active"),
                id="missing link",
            ),
        ],
    )
    def test_fulfill_cannot_fulfill(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
        status_document: LoanStatus,
    ) -> None:
        opds2_with_odl_api_fixture.setup_license(concurrency=7, available=7)
        opds2_with_odl_api_fixture.checkout(create_loan=True)

        assert 1 == db.session.query(Loan).count()
        assert 6 == opds2_with_odl_api_fixture.pool.licenses_available

        opds2_with_odl_api_fixture.mock_http.queue_response(
            200, content=status_document.model_dump_json()
        )
        with pytest.raises(CannotFulfill):
            opds2_with_odl_api_fixture.api.fulfill(
                opds2_with_odl_api_fixture.patron,
                "pin",
                opds2_with_odl_api_fixture.pool,
                MagicMock(),
            )

    @freeze_time()
    def test_place_hold_success(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        loan = opds2_with_odl_api_fixture.checkout(patron=db.patron(), create_loan=True)
        hold = opds2_with_odl_api_fixture.place_hold()

        assert 1 == opds2_with_odl_api_fixture.pool.patrons_in_hold_queue
        assert opds2_with_odl_api_fixture.collection == hold.collection(db.session)
        assert opds2_with_odl_api_fixture.pool.identifier.type == hold.identifier_type
        assert opds2_with_odl_api_fixture.pool.identifier.identifier == hold.identifier
        assert hold.start_date is not None
        assert hold.start_date == utc_now()
        assert 1 == hold.hold_position

    def test_place_hold_already_on_hold(
        self, opds2_with_odl_api_fixture: OPDS2WithODLApiFixture
    ) -> None:
        opds2_with_odl_api_fixture.setup_license(concurrency=1, available=0)
        opds2_with_odl_api_fixture.place_hold(create_hold=True)
        with pytest.raises(AlreadyOnHold):
            opds2_with_odl_api_fixture.place_hold()

    def test_place_hold_currently_available(
        self, opds2_with_odl_api_fixture: OPDS2WithODLApiFixture
    ) -> None:
        with pytest.raises(CurrentlyAvailable):
            opds2_with_odl_api_fixture.place_hold()

    def test_release_hold_success(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
    ) -> None:
        loan_patron = db.patron()
        hold1_patron = db.patron()
        hold2_patron = db.patron()

        opds2_with_odl_api_fixture.checkout(patron=loan_patron, create_loan=True)
        opds2_with_odl_api_fixture.place_hold(patron=hold1_patron, create_hold=True)
        opds2_with_odl_api_fixture.place_hold(patron=hold2_patron, create_hold=True)

        assert 0 == opds2_with_odl_api_fixture.pool.licenses_available
        assert 0 == opds2_with_odl_api_fixture.pool.licenses_reserved
        assert 2 == opds2_with_odl_api_fixture.pool.patrons_in_hold_queue

        opds2_with_odl_api_fixture.api.release_hold(
            hold1_patron, "pin", opds2_with_odl_api_fixture.pool
        )
        db.session.execute(delete(Hold).where(Hold.patron == hold1_patron))
        assert 0 == opds2_with_odl_api_fixture.pool.licenses_available
        assert 0 == opds2_with_odl_api_fixture.pool.licenses_reserved
        assert 1 == opds2_with_odl_api_fixture.pool.patrons_in_hold_queue

        opds2_with_odl_api_fixture.checkin(patron=loan_patron)
        assert 0 == opds2_with_odl_api_fixture.pool.licenses_available
        assert 1 == opds2_with_odl_api_fixture.pool.licenses_reserved
        assert 1 == opds2_with_odl_api_fixture.pool.patrons_in_hold_queue

        opds2_with_odl_api_fixture.api.release_hold(
            hold2_patron, "pin", opds2_with_odl_api_fixture.pool
        )
        assert 1 == opds2_with_odl_api_fixture.pool.licenses_available
        assert 0 == opds2_with_odl_api_fixture.pool.licenses_reserved
        assert 0 == opds2_with_odl_api_fixture.pool.patrons_in_hold_queue

    def test_release_hold_not_on_hold(
        self, opds2_with_odl_api_fixture: OPDS2WithODLApiFixture
    ) -> None:
        pytest.raises(
            NotOnHold,
            opds2_with_odl_api_fixture.api.release_hold,
            opds2_with_odl_api_fixture.patron,
            "pin",
            opds2_with_odl_api_fixture.pool,
        )

    def test_can_fulfill_without_loan(
        self, opds2_with_odl_api_fixture: OPDS2WithODLApiFixture
    ):
        assert not opds2_with_odl_api_fixture.api.can_fulfill_without_loan(
            MagicMock(), MagicMock(), MagicMock()
        )
