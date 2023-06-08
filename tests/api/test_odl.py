import datetime
import json
import urllib.parse
import uuid
from typing import TYPE_CHECKING, Callable, List, Optional, Tuple, Union

import dateutil
import pytest
from freezegun import freeze_time
from jinja2 import Template

from api.circulation import HoldInfo
from api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotFulfill,
    CannotLoan,
    CannotReleaseHold,
    CannotReturn,
    CurrentlyAvailable,
    FormatNotAvailable,
    LibraryAuthorizationFailedException,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotOnHold,
)
from api.odl import (
    ODLAPIConfiguration,
    ODLHoldReaper,
    ODLImporter,
    SharedODLAPI,
    SharedODLImporter,
)
from api.problem_details import NO_LICENSES
from core.model import (
    Collection,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hold,
    Hyperlink,
    Loan,
    MediaTypes,
    Representation,
    RightsStatus,
)
from core.util import datetime_helpers
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.http import BadResponseException, RemoteIntegrationException
from core.util.string_helpers import base64
from tests.api.mockapi.odl import MockSharedODLAPI
from tests.fixtures.api_odl_files import ODLAPIFilesFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import APIFilesFixture
from tests.fixtures.odl import ODLAPITestFixture, ODLTestFixture

if TYPE_CHECKING:
    from core.model import LicensePool


class LicenseHelper:
    """Represents an ODL license."""

    def __init__(
        self,
        identifier: Optional[str] = None,
        checkouts: Optional[int] = None,
        concurrency: Optional[int] = None,
        expires: Optional[Union[datetime.datetime, str]] = None,
    ) -> None:
        """Initialize a new instance of LicenseHelper class.

        :param identifier: License's identifier
        :param checkouts: Total number of checkouts before a license expires
        :param concurrency: Number of concurrent checkouts allowed
        :param expires: Date & time when a license expires
        """
        self.identifier: str = identifier if identifier else f"urn:uuid:{uuid.uuid1()}"
        self.checkouts: Optional[int] = checkouts
        self.concurrency: Optional[int] = concurrency
        if isinstance(expires, datetime.datetime):
            self.expires = expires.isoformat()
        else:
            self.expires: Optional[str] = expires  # type: ignore


class LicenseInfoHelper:
    """Represents information about the current state of a license stored in the License Info Document."""

    def __init__(
        self,
        license: LicenseHelper,
        available: int,
        status: str = "available",
        left: Optional[int] = None,
    ) -> None:
        """Initialize a new instance of LicenseInfoHelper class."""
        self.license: LicenseHelper = license
        self.status: str = status
        self.left: Optional[int] = left
        self.available: int = available

    def __str__(self) -> str:
        """Return a JSON representation of a part of the License Info Document."""
        output = {
            "identifier": self.license.identifier,
            "status": self.status,
            "terms": {
                "concurrency": self.license.concurrency,
            },
            "checkouts": {
                "available": self.available,
            },
        }
        if self.license.expires is not None:
            output["terms"]["expires"] = self.license.expires  # type: ignore
        if self.left is not None:
            output["checkouts"]["left"] = self.left  # type: ignore
        return json.dumps(output)


class TestODLAPI:
    def test_get_license_status_document_success(
        self, odl_api_test_fixture: ODLAPITestFixture
    ):
        # With a new loan.
        loan, _ = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.patron)
        odl_api_test_fixture.api.queue_response(
            200, content=json.dumps(dict(status="ready"))
        )
        odl_api_test_fixture.api.get_license_status_document(loan)
        requested_url = odl_api_test_fixture.api.requests[0][0]

        parsed = urllib.parse.urlparse(requested_url)
        assert "https" == parsed.scheme
        assert "loan.feedbooks.net" == parsed.netloc
        params = urllib.parse.parse_qs(parsed.query)

        assert ODLAPIConfiguration.passphrase_hint.default == params.get("hint")[0]  # type: ignore
        assert (
            ODLAPIConfiguration.passphrase_hint_url.default == params.get("hint_url")[0]  # type: ignore
        )

        assert odl_api_test_fixture.license.identifier == params.get("id")[0]  # type: ignore

        # The checkout id and patron id are random UUIDs.
        checkout_id = params.get("checkout_id")[0]  # type: ignore
        assert len(checkout_id) > 0
        patron_id = params.get("patron_id")[0]  # type: ignore
        assert len(patron_id) > 0

        # Loans expire in 21 days by default.
        now = utc_now()
        after_expiration = now + datetime.timedelta(days=23)
        expires = urllib.parse.unquote(params.get("expires")[0])  # type: ignore

        # The expiration time passed to the server is associated with
        # the UTC time zone.
        assert expires.endswith("+00:00")
        expires_t = dateutil.parser.parse(expires)
        assert expires_t.tzinfo == dateutil.tz.tz.tzutc()

        # It's a time in the future, but not _too far_ in the future.
        assert expires_t > now
        assert expires_t < after_expiration

        notification_url = urllib.parse.unquote_plus(params.get("notification_url")[0])  # type: ignore
        assert (
            "http://odl_notify?library_short_name=%s&loan_id=%s"
            % (odl_api_test_fixture.library.short_name, loan.id)
            == notification_url
        )

        # With an existing loan.
        loan, _ = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.patron)
        loan.external_identifier = odl_api_test_fixture.db.fresh_str()

        odl_api_test_fixture.api.queue_response(
            200, content=json.dumps(dict(status="active"))
        )
        odl_api_test_fixture.api.get_license_status_document(loan)
        requested_url = odl_api_test_fixture.api.requests[1][0]
        assert loan.external_identifier == requested_url

    def test_get_license_status_document_errors(
        self, odl_api_test_fixture: ODLAPITestFixture
    ):
        loan, _ = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.patron)

        odl_api_test_fixture.api.queue_response(200, content="not json")
        pytest.raises(
            BadResponseException,
            odl_api_test_fixture.api.get_license_status_document,
            loan,
        )

        odl_api_test_fixture.api.queue_response(
            200, content=json.dumps(dict(status="unknown"))
        )
        pytest.raises(
            BadResponseException,
            odl_api_test_fixture.api.get_license_status_document,
            loan,
        )

    def test_checkin_success(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # A patron has a copy of this book checked out.
        odl_api_test_fixture.license.setup(concurrency=7, available=6)

        loan, _ = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.patron)
        loan.external_identifier = "http://loan/" + db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        # The patron returns the book successfully.
        odl_api_test_fixture.checkin()
        assert 3 == len(odl_api_test_fixture.api.requests)
        assert "http://loan" in odl_api_test_fixture.api.requests[0][0]
        assert "http://return" == odl_api_test_fixture.api.requests[1][0]
        assert "http://loan" in odl_api_test_fixture.api.requests[2][0]

        # The pool's availability has increased, and the local loan has
        # been deleted.
        assert 7 == odl_api_test_fixture.pool.licenses_available
        assert 0 == db.session.query(Loan).count()

        # The license on the pool has also been updated
        assert 7 == odl_api_test_fixture.license.checkouts_available

    def test_checkin_success_with_holds_queue(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # A patron has the only copy of this book checked out.
        odl_api_test_fixture.license.setup(concurrency=1, available=0)
        loan, _ = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.patron)
        loan.external_identifier = "http://loan/" + db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        # Another patron has the book on hold.
        patron_with_hold = db.patron()
        odl_api_test_fixture.pool.patrons_in_hold_queue = 1
        hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            patron_with_hold, start=utc_now(), end=None, position=1
        )

        # The first patron returns the book successfully.
        odl_api_test_fixture.checkin()
        assert 3 == len(odl_api_test_fixture.api.requests)
        assert "http://loan" in odl_api_test_fixture.api.requests[0][0]
        assert "http://return" == odl_api_test_fixture.api.requests[1][0]
        assert "http://loan" in odl_api_test_fixture.api.requests[2][0]

        # Now the license is reserved for the next patron.
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 1 == odl_api_test_fixture.pool.licenses_reserved
        assert 1 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 0 == db.session.query(Loan).count()
        assert 0 == hold.position

    def test_checkin_already_fulfilled(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # The loan is already fulfilled.
        odl_api_test_fixture.license.setup(concurrency=7, available=6)
        loan, _ = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.patron)
        loan.external_identifier = db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        lsd = json.dumps(
            {
                "status": "active",
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        # Checking in the book silently does nothing.
        odl_api_test_fixture.api.checkin(
            odl_api_test_fixture.patron, "pinn", odl_api_test_fixture.pool
        )
        assert 1 == len(odl_api_test_fixture.api.requests)
        assert 6 == odl_api_test_fixture.pool.licenses_available
        assert 1 == db.session.query(Loan).count()

    def test_checkin_not_checked_out(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # Not checked out locally.
        pytest.raises(
            NotCheckedOut,
            odl_api_test_fixture.api.checkin,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
        )

        # Not checked out according to the distributor.
        loan, _ = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.patron)
        loan.external_identifier = db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        lsd = json.dumps(
            {
                "status": "revoked",
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        pytest.raises(
            NotCheckedOut,
            odl_api_test_fixture.api.checkin,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
        )

    def test_checkin_cannot_return(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # Not fulfilled yet, but no return link from the distributor.
        loan, ignore = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.patron)
        loan.external_identifier = db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        lsd = json.dumps(
            {
                "status": "ready",
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        # Checking in silently does nothing.
        odl_api_test_fixture.api.checkin(
            odl_api_test_fixture.patron, "pin", odl_api_test_fixture.pool
        )

        # If the return link doesn't change the status, it still
        # silently ignores the problem.
        lsd = json.dumps(
            {
                "status": "ready",
                "links": [
                    {
                        "rel": "return",
                        "href": "http://return",
                    }
                ],
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        odl_api_test_fixture.api.queue_response(200, content="Deleted")
        odl_api_test_fixture.api.queue_response(200, content=lsd)
        odl_api_test_fixture.api.checkin(
            odl_api_test_fixture.patron, "pin", odl_api_test_fixture.pool
        )

    def test_checkout_success(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # This book is available to check out.
        odl_api_test_fixture.license.setup(concurrency=6, available=6, left=30)

        # A patron checks out the book successfully.
        loan_url = db.fresh_str()
        loan, _ = odl_api_test_fixture.checkout(loan_url=loan_url)

        assert odl_api_test_fixture.collection == loan.collection(db.session)
        assert odl_api_test_fixture.pool.data_source.name == loan.data_source_name
        assert odl_api_test_fixture.pool.identifier.type == loan.identifier_type
        assert odl_api_test_fixture.pool.identifier.identifier == loan.identifier
        assert loan.start_date > utc_now() - datetime.timedelta(minutes=1)
        assert loan.start_date < utc_now() + datetime.timedelta(minutes=1)
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end_date
        assert loan_url == loan.external_identifier
        assert 1 == db.session.query(Loan).count()

        # Now the patron has a loan in the database that matches the LoanInfo
        # returned by the API.
        db_loan = db.session.query(Loan).one()
        assert odl_api_test_fixture.pool == db_loan.license_pool
        assert odl_api_test_fixture.license == db_loan.license
        assert loan.start_date == db_loan.start
        assert loan.end_date == db_loan.end

        # The pool's availability and the license's remaining checkouts have decreased.
        assert 5 == odl_api_test_fixture.pool.licenses_available
        assert 29 == odl_api_test_fixture.license.checkouts_left

    def test_checkout_success_with_hold(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # A patron has this book on hold, and the book just became available to check out.
        odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.patron,
            start=utc_now() - datetime.timedelta(days=1),
            position=0,
        )
        odl_api_test_fixture.license.setup(concurrency=1, available=1, left=5)

        assert odl_api_test_fixture.pool.licenses_available == 0
        assert odl_api_test_fixture.pool.licenses_reserved == 1
        assert odl_api_test_fixture.pool.patrons_in_hold_queue == 1

        # The patron checks out the book.
        loan_url = db.fresh_str()
        loan, _ = odl_api_test_fixture.checkout(loan_url=loan_url)

        # The patron gets a loan successfully.
        assert odl_api_test_fixture.collection == loan.collection(db.session)
        assert odl_api_test_fixture.pool.data_source.name == loan.data_source_name
        assert odl_api_test_fixture.pool.identifier.type == loan.identifier_type
        assert odl_api_test_fixture.pool.identifier.identifier == loan.identifier
        assert loan.start_date > utc_now() - datetime.timedelta(minutes=1)
        assert loan.start_date < utc_now() + datetime.timedelta(minutes=1)
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end_date
        assert loan_url == loan.external_identifier
        assert 1 == db.session.query(Loan).count()

        db_loan = db.session.query(Loan).one()
        assert odl_api_test_fixture.pool == db_loan.license_pool
        assert odl_api_test_fixture.license == db_loan.license
        assert 4 == odl_api_test_fixture.license.checkouts_left

        # The book is no longer reserved for the patron, and the hold has been deleted.
        assert 0 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 0 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 0 == db.session.query(Hold).count()

    def test_checkout_already_checked_out(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        odl_api_test_fixture.license.setup(concurrency=2, available=1)

        # Checkout succeeds the first time
        odl_api_test_fixture.checkout()

        # But raises an exception the second time
        pytest.raises(AlreadyCheckedOut, odl_api_test_fixture.checkout)

        assert 1 == db.session.query(Loan).count()

    def test_checkout_expired_hold(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # The patron was at the beginning of the hold queue, but the hold already expired.
        yesterday = utc_now() - datetime.timedelta(days=1)
        hold, _ = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.patron, start=yesterday, end=yesterday, position=0
        )
        other_hold, _ = odl_api_test_fixture.pool.on_hold_to(
            db.patron(), start=utc_now()
        )
        odl_api_test_fixture.license.setup(concurrency=2, available=1)

        pytest.raises(
            NoAvailableCopies,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

    def test_checkout_no_available_copies(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # A different patron has the only copy checked out.
        odl_api_test_fixture.license.setup(concurrency=1, available=0)
        existing_loan, _ = odl_api_test_fixture.license.loan_to(db.patron())

        pytest.raises(
            NoAvailableCopies,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 1 == db.session.query(Loan).count()

        db.session.delete(existing_loan)

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(weeks=1)

        # A different patron has the only copy reserved.
        other_patron_hold, _ = odl_api_test_fixture.pool.on_hold_to(
            db.patron(), position=0, start=last_week
        )
        odl_api_test_fixture.pool.update_availability_from_licenses()

        pytest.raises(
            NoAvailableCopies,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.session.query(Loan).count()

        # The patron has a hold, but another patron is ahead in the holds queue.
        hold, _ = odl_api_test_fixture.pool.on_hold_to(
            db.patron(), position=1, start=yesterday
        )
        odl_api_test_fixture.pool.update_availability_from_licenses()

        pytest.raises(
            NoAvailableCopies,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.session.query(Loan).count()

        # The patron has the first hold, but it's expired.
        hold.start = last_week - datetime.timedelta(days=1)
        hold.end = yesterday
        odl_api_test_fixture.pool.update_availability_from_licenses()

        pytest.raises(
            NoAvailableCopies,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.session.query(Loan).count()

    def test_checkout_no_licenses(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        odl_api_test_fixture.license.setup(concurrency=1, available=1, left=0)

        pytest.raises(
            NoLicenses,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.session.query(Loan).count()

    def test_checkout_when_all_licenses_expired(
        self, odl_api_test_fixture: ODLAPITestFixture
    ):
        # license expired by expiration date
        odl_api_test_fixture.license.setup(
            concurrency=1,
            available=2,
            left=1,
            expires=utc_now() - datetime.timedelta(weeks=1),
        )

        pytest.raises(
            NoLicenses,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        # license expired by no remaining checkouts
        odl_api_test_fixture.license.setup(
            concurrency=1,
            available=2,
            left=0,
            expires=utc_now() + datetime.timedelta(weeks=1),
        )

        pytest.raises(
            NoLicenses,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

    def test_checkout_cannot_loan(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        lsd = json.dumps(
            {
                "status": "revoked",
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        pytest.raises(
            CannotLoan,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.session.query(Loan).count()

        # No external identifier.
        lsd = json.dumps(
            {
                "status": "ready",
                "potential_rights": {"end": "2017-10-21T11:12:13Z"},
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        pytest.raises(
            CannotLoan,
            odl_api_test_fixture.api.checkout,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.session.query(Loan).count()

    @pytest.mark.parametrize(
        "delivery_mechanism, correct_type, correct_link, links",
        [
            (
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
            ),
            (
                MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                "http://manifest",
                [
                    {
                        "rel": "manifest",
                        "href": "http://manifest",
                        "type": MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                    }
                ],
            ),
            (
                DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
                ODLImporter.FEEDBOOKS_AUDIO,
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
                        "type": ODLImporter.FEEDBOOKS_AUDIO,
                    },
                ],
            ),
        ],
    )
    def test_fulfill_success(
        self,
        odl_api_test_fixture: ODLAPITestFixture,
        db: DatabaseTransactionFixture,
        delivery_mechanism,
        correct_type,
        correct_link,
        links,
    ):
        # Fulfill a loan in a way that gives access to a license file.
        odl_api_test_fixture.license.setup(concurrency=1, available=1)
        odl_api_test_fixture.checkout()

        lsd = json.dumps(
            {
                "status": "ready",
                "potential_rights": {"end": "2017-10-21T11:12:13Z"},
                "links": links,
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        fulfillment = odl_api_test_fixture.api.fulfill(
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            delivery_mechanism,
        )

        assert odl_api_test_fixture.collection == fulfillment.collection(db.session)
        assert (
            odl_api_test_fixture.pool.data_source.name == fulfillment.data_source_name
        )
        assert odl_api_test_fixture.pool.identifier.type == fulfillment.identifier_type
        assert odl_api_test_fixture.pool.identifier.identifier == fulfillment.identifier
        assert datetime_utc(2017, 10, 21, 11, 12, 13) == fulfillment.content_expires
        assert correct_link == fulfillment.content_link
        assert correct_type == fulfillment.content_type

    def test_fulfill_cannot_fulfill(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        odl_api_test_fixture.license.setup(concurrency=7, available=7)
        odl_api_test_fixture.checkout()

        assert 1 == db.session.query(Loan).count()
        assert 6 == odl_api_test_fixture.pool.licenses_available

        lsd = json.dumps(
            {
                "status": "revoked",
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        pytest.raises(
            CannotFulfill,
            odl_api_test_fixture.api.fulfill,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        # The pool's availability has been updated and the local
        # loan has been deleted, since we found out the loan is
        # no longer active.
        assert 7 == odl_api_test_fixture.pool.licenses_available
        assert 0 == db.session.query(Loan).count()

    def _holdinfo_from_hold(self, hold: Hold) -> HoldInfo:
        pool: LicensePool = hold.license_pool
        return HoldInfo(
            pool.collection,
            pool.data_source.name,
            pool.identifier.type,
            pool.identifier.identifier,
            hold.start,
            hold.end,
            hold.position,
        )

    def test_count_holds_before(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(weeks=1)

        hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.patron, start=now
        )

        info = self._holdinfo_from_hold(hold)
        assert 0 == odl_api_test_fixture.api._count_holds_before(
            info, hold.license_pool
        )

        # A previous hold.
        odl_api_test_fixture.pool.on_hold_to(db.patron(), start=yesterday)
        assert 1 == odl_api_test_fixture.api._count_holds_before(
            info, hold.license_pool
        )

        # Expired holds don't count.
        odl_api_test_fixture.pool.on_hold_to(
            db.patron(), start=last_week, end=yesterday, position=0
        )
        assert 1 == odl_api_test_fixture.api._count_holds_before(
            info, hold.license_pool
        )

        # Later holds don't count.
        odl_api_test_fixture.pool.on_hold_to(db.patron(), start=tomorrow)
        assert 1 == odl_api_test_fixture.api._count_holds_before(
            info, hold.license_pool
        )

        # Holds on another pool don't count.
        other_pool = db.licensepool(None)
        other_pool.on_hold_to(odl_api_test_fixture.patron, start=yesterday)
        assert 1 == odl_api_test_fixture.api._count_holds_before(
            info, hold.license_pool
        )

        for i in range(3):
            odl_api_test_fixture.pool.on_hold_to(
                db.patron(), start=yesterday, end=tomorrow, position=1
            )
        assert 4 == odl_api_test_fixture.api._count_holds_before(
            info, hold.license_pool
        )

    def test_update_hold_end_date(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        now = utc_now()
        tomorrow = now + datetime.timedelta(days=1)
        yesterday = now - datetime.timedelta(days=1)
        next_week = now + datetime.timedelta(days=7)
        last_week = now - datetime.timedelta(days=7)

        odl_api_test_fixture.pool.licenses_owned = 1
        odl_api_test_fixture.pool.licenses_reserved = 1

        hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.patron, start=now, position=0
        )
        info = self._holdinfo_from_hold(hold)
        library = hold.patron.library

        # Set the reservation period and loan period.
        odl_api_test_fixture.collection.integration_configuration.set(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY, 3
        )
        odl_api_test_fixture.collection.integration_configuration.set(
            Collection.EBOOK_LOAN_DURATION_KEY, 6
        )
        odl_api_test_fixture.db.session.commit()

        # A hold that's already reserved and has an end date doesn't change.
        info.end_date = tomorrow
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert tomorrow == info.end_date
        info.end_date = yesterday
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert yesterday == info.end_date

        # Updating a hold that's reserved but doesn't have an end date starts the
        # reservation period.
        info.end_date = None
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert info.end_date < next_week
        assert info.end_date > now

        # Updating a hold that has an end date but just became reserved starts
        # the reservation period.
        info.end_date = yesterday
        info.hold_position = 1
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert info.end_date < next_week
        assert info.end_date > now

        # When there's a holds queue, the end date is the maximum time it could take for
        # a license to become available.

        # One copy, one loan, hold position 1.
        # The hold will be available as soon as the loan expires.
        odl_api_test_fixture.pool.licenses_available = 0
        odl_api_test_fixture.pool.licenses_reserved = 0
        odl_api_test_fixture.pool.licenses_owned = 1
        loan, ignore = odl_api_test_fixture.license.loan_to(db.patron(), end=tomorrow)
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert tomorrow == info.end_date

        # One copy, one loan, hold position 2.
        # The hold will be available after the loan expires + 1 cycle.
        first_hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            db.patron(), start=last_week
        )
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert tomorrow + datetime.timedelta(days=9) == info.end_date

        # Two copies, one loan, one reserved hold, hold position 2.
        # The hold will be available after the loan expires.
        odl_api_test_fixture.pool.licenses_reserved = 1
        odl_api_test_fixture.pool.licenses_owned = 2
        odl_api_test_fixture.license.checkouts_available = 2
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert tomorrow == info.end_date

        # Two copies, one loan, one reserved hold, hold position 3.
        # The hold will be available after the reserved hold is checked out
        # at the latest possible time and that loan expires.
        second_hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            db.patron(), start=yesterday
        )
        first_hold.end = next_week
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert next_week + datetime.timedelta(days=6) == info.end_date

        # One copy, no loans, one reserved hold, hold position 3.
        # The hold will be available after the reserved hold is checked out
        # at the latest possible time and that loan expires + 1 cycle.
        db.session.delete(loan)
        odl_api_test_fixture.pool.licenses_owned = 1
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert next_week + datetime.timedelta(days=15) == info.end_date

        # One copy, no loans, one reserved hold, hold position 2.
        # The hold will be available after the reserved hold is checked out
        # at the latest possible time and that loan expires.
        db.session.delete(second_hold)
        odl_api_test_fixture.pool.licenses_owned = 1
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert next_week + datetime.timedelta(days=6) == info.end_date

        db.session.delete(first_hold)

        # Ten copies, seven loans, three reserved holds, hold position 9.
        # The hold will be available after the sixth loan expires.
        odl_api_test_fixture.pool.licenses_owned = 10
        for i in range(5):
            odl_api_test_fixture.pool.loan_to(db.patron(), end=next_week)
        odl_api_test_fixture.pool.loan_to(
            db.patron(), end=next_week + datetime.timedelta(days=1)
        )
        odl_api_test_fixture.pool.loan_to(
            db.patron(), end=next_week + datetime.timedelta(days=2)
        )
        odl_api_test_fixture.pool.licenses_reserved = 3
        for i in range(3):
            odl_api_test_fixture.pool.on_hold_to(
                db.patron(),
                start=last_week + datetime.timedelta(days=i),
                end=next_week + datetime.timedelta(days=i),
                position=0,
            )
        for i in range(5):
            odl_api_test_fixture.pool.on_hold_to(db.patron(), start=yesterday)
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert next_week + datetime.timedelta(days=1) == info.end_date

        # Ten copies, seven loans, three reserved holds, hold position 12.
        # The hold will be available after the second reserved hold is checked
        # out and that loan expires.
        for i in range(3):
            odl_api_test_fixture.pool.on_hold_to(db.patron(), start=yesterday)
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert next_week + datetime.timedelta(days=7) == info.end_date

        # Ten copies, seven loans, three reserved holds, hold position 29.
        # The hold will be available after the sixth loan expires + 2 cycles.
        for i in range(17):
            odl_api_test_fixture.pool.on_hold_to(db.patron(), start=yesterday)
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert next_week + datetime.timedelta(days=19) == info.end_date

        # Ten copies, seven loans, three reserved holds, hold position 32.
        # The hold will be available after the second reserved hold is checked
        # out and that loan expires + 2 cycles.
        for i in range(3):
            odl_api_test_fixture.pool.on_hold_to(db.patron(), start=yesterday)
        odl_api_test_fixture.api._update_hold_end_date(
            info, hold.license_pool, library=library
        )
        assert next_week + datetime.timedelta(days=25) == info.end_date

    def test_update_hold_position(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.patron, start=now
        )
        info = self._holdinfo_from_hold(hold)

        odl_api_test_fixture.pool.licenses_owned = 1

        # When there are no other holds and no licenses reserved, hold position is 1.
        loan, _ = odl_api_test_fixture.license.loan_to(db.patron())
        odl_api_test_fixture.api._update_hold_position(info, hold.license_pool)
        assert 1 == info.hold_position

        # When a license is reserved, position is 0.
        db.session.delete(loan)
        odl_api_test_fixture.api._update_hold_position(info, hold.license_pool)
        assert 0 == info.hold_position

        # If another hold has the reserved licenses, position is 2.
        odl_api_test_fixture.pool.on_hold_to(db.patron(), start=yesterday)
        odl_api_test_fixture.api._update_hold_position(info, hold.license_pool)
        assert 2 == info.hold_position

        # If another license is reserved, position goes back to 0.
        odl_api_test_fixture.pool.licenses_owned = 2
        odl_api_test_fixture.license.checkouts_available = 2
        odl_api_test_fixture.api._update_hold_position(info, hold.license_pool)
        assert 0 == info.hold_position

        # If there's an earlier hold but it expired, it doesn't
        # affect the position.
        odl_api_test_fixture.pool.on_hold_to(
            db.patron(), start=yesterday, end=yesterday, position=0
        )
        odl_api_test_fixture.api._update_hold_position(info, hold.license_pool)
        assert 0 == info.hold_position

        # Hold position is after all earlier non-expired holds...
        for i in range(3):
            odl_api_test_fixture.pool.on_hold_to(db.patron(), start=yesterday)
        odl_api_test_fixture.api._update_hold_position(info, hold.license_pool)
        assert 5 == info.hold_position

        # and before any later holds.
        for i in range(2):
            odl_api_test_fixture.pool.on_hold_to(db.patron(), start=tomorrow)
        odl_api_test_fixture.api._update_hold_position(info, hold.license_pool)
        assert 5 == info.hold_position

    def test_update_hold_data(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        hold, is_new = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.patron,
            utc_now(),
            utc_now() + datetime.timedelta(days=100),
            9,
        )
        odl_api_test_fixture.api._update_hold_data(hold)
        assert hold.position == 0
        assert hold.end.date() == (hold.start + datetime.timedelta(days=3)).date()

    def test_update_hold_queue(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        licenses = [odl_api_test_fixture.license]

        odl_api_test_fixture.collection.integration_configuration.set(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY, 3
        )

        # If there's no holds queue when we try to update the queue, it
        # will remove a reserved license and make it available instead.
        odl_api_test_fixture.pool.licenses_owned = 1
        odl_api_test_fixture.pool.licenses_available = 0
        odl_api_test_fixture.pool.licenses_reserved = 1
        odl_api_test_fixture.pool.patrons_in_hold_queue = 0
        last_update = utc_now() - datetime.timedelta(minutes=5)
        odl_api_test_fixture.work.last_update_time = last_update
        odl_api_test_fixture.api.update_licensepool(odl_api_test_fixture.pool)
        assert 1 == odl_api_test_fixture.pool.licenses_available
        assert 0 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.patrons_in_hold_queue
        # The work's last update time is changed so it will be moved up in the crawlable OPDS feed.
        assert odl_api_test_fixture.work.last_update_time > last_update

        # If there are holds, a license will get reserved for the next hold
        # and its end date will be set.
        hold, _ = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.patron, start=utc_now(), position=1
        )
        later_hold, _ = odl_api_test_fixture.pool.on_hold_to(
            db.patron(), start=utc_now() + datetime.timedelta(days=1), position=2
        )
        odl_api_test_fixture.api.update_licensepool(odl_api_test_fixture.pool)

        # The pool's licenses were updated.
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 1 == odl_api_test_fixture.pool.licenses_reserved
        assert 2 == odl_api_test_fixture.pool.patrons_in_hold_queue

        # And the first hold changed.
        assert 0 == hold.position
        assert hold.end - utc_now() - datetime.timedelta(days=3) < datetime.timedelta(
            hours=1
        )

        # The later hold is the same.
        assert 2 == later_hold.position

        # Now there's a reserved hold. If we add another license, it's reserved and,
        # the later hold is also updated.
        l = db.license(
            odl_api_test_fixture.pool, terms_concurrency=1, checkouts_available=1
        )
        licenses.append(l)
        odl_api_test_fixture.api.update_licensepool(odl_api_test_fixture.pool)

        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 2 == odl_api_test_fixture.pool.licenses_reserved
        assert 2 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 0 == later_hold.position
        assert later_hold.end - utc_now() - datetime.timedelta(
            days=3
        ) < datetime.timedelta(hours=1)

        # Now there are no more holds. If we add another license,
        # it ends up being available.
        l = db.license(
            odl_api_test_fixture.pool, terms_concurrency=1, checkouts_available=1
        )
        licenses.append(l)
        odl_api_test_fixture.api.update_licensepool(odl_api_test_fixture.pool)
        assert 1 == odl_api_test_fixture.pool.licenses_available
        assert 2 == odl_api_test_fixture.pool.licenses_reserved
        assert 2 == odl_api_test_fixture.pool.patrons_in_hold_queue

        # License pool is updated when the holds are removed.
        db.session.delete(hold)
        db.session.delete(later_hold)
        odl_api_test_fixture.api.update_licensepool(odl_api_test_fixture.pool)
        assert 3 == odl_api_test_fixture.pool.licenses_available
        assert 0 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.patrons_in_hold_queue

        # We can also make multiple licenses reserved at once.
        loans = []
        holds = []
        for i in range(3):
            p = db.patron()
            loan, _ = odl_api_test_fixture.checkout(patron=p)
            loans.append((loan, p))
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 0 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.patrons_in_hold_queue

        l = db.license(
            odl_api_test_fixture.pool, terms_concurrency=2, checkouts_available=2
        )
        licenses.append(l)
        for i in range(3):
            hold, ignore = odl_api_test_fixture.pool.on_hold_to(
                db.patron(),
                start=utc_now() - datetime.timedelta(days=3 - i),
                position=i + 1,
            )
            holds.append(hold)

        odl_api_test_fixture.api.update_licensepool(odl_api_test_fixture.pool)
        assert 2 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 3 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 0 == holds[0].position
        assert 0 == holds[1].position
        assert 3 == holds[2].position
        assert holds[0].end - utc_now() - datetime.timedelta(
            days=3
        ) < datetime.timedelta(hours=1)
        assert holds[1].end - utc_now() - datetime.timedelta(
            days=3
        ) < datetime.timedelta(hours=1)

        # If there are more licenses that change than holds, some of them become available.
        for i in range(2):
            _, p = loans[i]
            odl_api_test_fixture.checkin(patron=p)
        assert 3 == odl_api_test_fixture.pool.licenses_reserved
        assert 1 == odl_api_test_fixture.pool.licenses_available
        assert 3 == odl_api_test_fixture.pool.patrons_in_hold_queue
        for hold in holds:
            assert 0 == hold.position
            assert hold.end - utc_now() - datetime.timedelta(
                days=3
            ) < datetime.timedelta(hours=1)

    def test_place_hold_success(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        loan, _ = odl_api_test_fixture.checkout(patron=db.patron())

        hold = odl_api_test_fixture.api.place_hold(
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            "notifications@librarysimplified.org",
        )

        assert 1 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert odl_api_test_fixture.collection == hold.collection(db.session)
        assert odl_api_test_fixture.pool.data_source.name == hold.data_source_name
        assert odl_api_test_fixture.pool.identifier.type == hold.identifier_type
        assert odl_api_test_fixture.pool.identifier.identifier == hold.identifier
        assert hold.start_date > utc_now() - datetime.timedelta(minutes=1)
        assert hold.start_date < utc_now() + datetime.timedelta(minutes=1)
        assert loan.end_date == hold.end_date
        assert 1 == hold.hold_position

    def test_place_hold_already_on_hold(self, odl_api_test_fixture: ODLAPITestFixture):
        odl_api_test_fixture.license.setup(concurrency=1, available=0)
        odl_api_test_fixture.pool.on_hold_to(odl_api_test_fixture.patron)
        pytest.raises(
            AlreadyOnHold,
            odl_api_test_fixture.api.place_hold,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            "notifications@librarysimplified.org",
        )

    def test_place_hold_currently_available(
        self, odl_api_test_fixture: ODLAPITestFixture
    ):
        pytest.raises(
            CurrentlyAvailable,
            odl_api_test_fixture.api.place_hold,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
            "notifications@librarysimplified.org",
        )

    def test_release_hold_success(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        loan_patron = db.patron()
        odl_api_test_fixture.checkout(patron=loan_patron)
        odl_api_test_fixture.pool.on_hold_to(odl_api_test_fixture.patron, position=1)

        assert True == odl_api_test_fixture.api.release_hold(
            odl_api_test_fixture.patron, "pin", odl_api_test_fixture.pool
        )
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 0 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 0 == db.session.query(Hold).count()

        odl_api_test_fixture.pool.on_hold_to(odl_api_test_fixture.patron, position=0)
        odl_api_test_fixture.checkin(patron=loan_patron)

        assert True == odl_api_test_fixture.api.release_hold(
            odl_api_test_fixture.patron, "pin", odl_api_test_fixture.pool
        )
        assert 1 == odl_api_test_fixture.pool.licenses_available
        assert 0 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 0 == db.session.query(Hold).count()

        odl_api_test_fixture.pool.on_hold_to(odl_api_test_fixture.patron, position=0)
        other_hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            db.patron(), position=2
        )

        assert True == odl_api_test_fixture.api.release_hold(
            odl_api_test_fixture.patron, "pin", odl_api_test_fixture.pool
        )
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 1 == odl_api_test_fixture.pool.licenses_reserved
        assert 1 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 1 == db.session.query(Hold).count()
        assert 0 == other_hold.position

    def test_release_hold_not_on_hold(self, odl_api_test_fixture: ODLAPITestFixture):
        pytest.raises(
            NotOnHold,
            odl_api_test_fixture.api.release_hold,
            odl_api_test_fixture.patron,
            "pin",
            odl_api_test_fixture.pool,
        )

    def test_patron_activity_loan(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # No loans yet.
        assert [] == odl_api_test_fixture.api.patron_activity(
            odl_api_test_fixture.patron, "pin"
        )

        # One loan.
        _, loan = odl_api_test_fixture.checkout()

        activity = odl_api_test_fixture.api.patron_activity(
            odl_api_test_fixture.patron, "pin"
        )
        assert 1 == len(activity)
        assert odl_api_test_fixture.collection == activity[0].collection(db.session)
        assert (
            odl_api_test_fixture.pool.data_source.name == activity[0].data_source_name
        )
        assert odl_api_test_fixture.pool.identifier.type == activity[0].identifier_type
        assert odl_api_test_fixture.pool.identifier.identifier == activity[0].identifier
        assert loan.start == activity[0].start_date
        assert loan.end == activity[0].end_date
        assert loan.external_identifier == activity[0].external_identifier

        # Two loans.
        pool2 = db.licensepool(None, collection=odl_api_test_fixture.collection)
        license2 = db.license(pool2, terms_concurrency=1, checkouts_available=1)
        _, loan2 = odl_api_test_fixture.checkout(pool=pool2)

        activity = odl_api_test_fixture.api.patron_activity(
            odl_api_test_fixture.patron, "pin"
        )
        assert 2 == len(activity)
        [l1, l2] = sorted(activity, key=lambda x: x.start_date)

        assert odl_api_test_fixture.collection == l1.collection(db.session)
        assert odl_api_test_fixture.pool.data_source.name == l1.data_source_name
        assert odl_api_test_fixture.pool.identifier.type == l1.identifier_type
        assert odl_api_test_fixture.pool.identifier.identifier == l1.identifier
        assert loan.start == l1.start_date
        assert loan.end == l1.end_date
        assert loan.external_identifier == l1.external_identifier

        assert odl_api_test_fixture.collection == l2.collection(db.session)
        assert pool2.data_source.name == l2.data_source_name
        assert pool2.identifier.type == l2.identifier_type
        assert pool2.identifier.identifier == l2.identifier
        assert loan2.start == l2.start_date
        assert loan2.end == l2.end_date
        assert loan2.external_identifier == l2.external_identifier

        # If a loan is expired already, it's left out.
        loan2.end = utc_now() - datetime.timedelta(days=2)
        activity = odl_api_test_fixture.api.patron_activity(
            odl_api_test_fixture.patron, "pin"
        )
        assert 1 == len(activity)
        assert odl_api_test_fixture.pool.identifier.identifier == activity[0].identifier
        odl_api_test_fixture.checkin(pool=pool2)

        # One hold.
        other_patron = db.patron()
        odl_api_test_fixture.checkout(patron=other_patron, pool=pool2)
        hold, _ = pool2.on_hold_to(odl_api_test_fixture.patron)
        hold.start = utc_now() - datetime.timedelta(days=2)
        hold.end = hold.start + datetime.timedelta(days=3)
        hold.position = 3
        activity = odl_api_test_fixture.api.patron_activity(
            odl_api_test_fixture.patron, "pin"
        )
        assert 2 == len(activity)
        [h1, l1] = sorted(activity, key=lambda x: x.start_date)

        assert odl_api_test_fixture.collection == h1.collection(db.session)
        assert pool2.data_source.name == h1.data_source_name
        assert pool2.identifier.type == h1.identifier_type
        assert pool2.identifier.identifier == h1.identifier
        assert hold.start == h1.start_date
        assert hold.end == h1.end_date
        # Hold position was updated.
        assert 1 == h1.hold_position
        assert 1 == hold.position

        # If the hold is expired, it's deleted right away and the license
        # is made available again.
        odl_api_test_fixture.checkin(patron=other_patron, pool=pool2)
        hold.end = utc_now() - datetime.timedelta(days=1)
        hold.position = 0
        activity = odl_api_test_fixture.api.patron_activity(
            odl_api_test_fixture.patron, "pin"
        )
        assert 1 == len(activity)
        assert 0 == db.session.query(Hold).count()
        assert 1 == pool2.licenses_available
        assert 0 == pool2.licenses_reserved

    def test_update_loan_still_active(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        odl_api_test_fixture.license.setup(concurrency=6, available=6)
        loan, _ = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.patron)
        loan.external_identifier = db.fresh_str()
        status_doc = {
            "status": "active",
        }

        odl_api_test_fixture.api.update_loan(loan, status_doc)
        # Availability hasn't changed, and the loan still exists.
        assert 6 == odl_api_test_fixture.pool.licenses_available
        assert 1 == db.session.query(Loan).count()

    def test_update_loan_removes_loan(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        odl_api_test_fixture.license.setup(concurrency=7, available=7)
        _, loan = odl_api_test_fixture.checkout()

        assert 6 == odl_api_test_fixture.pool.licenses_available
        assert 1 == db.session.query(Loan).count()

        status_doc = {
            "status": "cancelled",
        }

        odl_api_test_fixture.api.update_loan(loan, status_doc)

        # Availability has increased, and the loan is gone.
        assert 7 == odl_api_test_fixture.pool.licenses_available
        assert 0 == db.session.query(Loan).count()

    def test_update_loan_removes_loan_with_hold_queue(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        _, loan = odl_api_test_fixture.checkout()
        hold, _ = odl_api_test_fixture.pool.on_hold_to(db.patron(), position=1)
        odl_api_test_fixture.pool.update_availability_from_licenses()

        assert odl_api_test_fixture.pool.licenses_owned == 1
        assert odl_api_test_fixture.pool.licenses_available == 0
        assert odl_api_test_fixture.pool.licenses_reserved == 0
        assert odl_api_test_fixture.pool.patrons_in_hold_queue == 1

        status_doc = {
            "status": "cancelled",
        }

        odl_api_test_fixture.api.update_loan(loan, status_doc)

        # The license is reserved for the next patron, and the loan is gone.
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 1 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == hold.position
        assert 0 == db.session.query(Loan).count()

    def test_checkout_from_external_library(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # This book is available to check out.
        odl_api_test_fixture.pool.licenses_owned = 6
        odl_api_test_fixture.pool.licenses_available = 6
        odl_api_test_fixture.license.checkouts_available = 6
        odl_api_test_fixture.license.checkouts_left = 10

        # An integration client checks out the book successfully.
        loan_url = db.fresh_str()
        lsd = json.dumps(
            {
                "status": "ready",
                "potential_rights": {"end": "3017-10-21T11:12:13Z"},
                "links": [
                    {
                        "rel": "self",
                        "href": loan_url,
                    }
                ],
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        loan = odl_api_test_fixture.api.checkout_to_external_library(
            odl_api_test_fixture.client, odl_api_test_fixture.pool
        )
        assert odl_api_test_fixture.client == loan.integration_client
        assert odl_api_test_fixture.pool == loan.license_pool
        assert loan.start > utc_now() - datetime.timedelta(minutes=1)
        assert loan.start < utc_now() + datetime.timedelta(minutes=1)
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end
        assert loan_url == loan.external_identifier
        assert 1 == db.session.query(Loan).count()

        # The pool's availability and the license's remaining checkouts have decreased.
        assert 5 == odl_api_test_fixture.pool.licenses_available
        assert 9 == odl_api_test_fixture.license.checkouts_left

        # The book can also be placed on hold to an external library,
        # if there are no copies available.
        odl_api_test_fixture.license.setup(concurrency=1, available=0)

        holdinfo = odl_api_test_fixture.api.checkout_to_external_library(
            odl_api_test_fixture.client, odl_api_test_fixture.pool
        )

        assert 1 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert odl_api_test_fixture.client == holdinfo.integration_client
        assert holdinfo.start_date > utc_now() - datetime.timedelta(minutes=1)
        assert holdinfo.start_date < utc_now() + datetime.timedelta(minutes=1)
        assert holdinfo.end_date > utc_now() + datetime.timedelta(days=7)
        assert 1 == holdinfo.hold_position

    def test_checkout_from_external_library_with_hold(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # An integration client has this book on hold, and the book just became available to check out.
        odl_api_test_fixture.pool.licenses_owned = 1
        odl_api_test_fixture.pool.licenses_available = 0
        odl_api_test_fixture.pool.licenses_reserved = 1
        odl_api_test_fixture.pool.patrons_in_hold_queue = 1
        hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.client,
            start=utc_now() - datetime.timedelta(days=1),
            position=0,
        )

        # The patron checks out the book.
        loan_url = db.fresh_str()
        lsd = json.dumps(
            {
                "status": "ready",
                "potential_rights": {"end": "3017-10-21T11:12:13Z"},
                "links": [
                    {
                        "rel": "self",
                        "href": loan_url,
                    }
                ],
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)

        # The patron gets a loan successfully.
        loan = odl_api_test_fixture.api.checkout_to_external_library(
            odl_api_test_fixture.client, odl_api_test_fixture.pool, hold
        )
        assert odl_api_test_fixture.client == loan.integration_client
        assert odl_api_test_fixture.pool == loan.license_pool
        assert loan.start > utc_now() - datetime.timedelta(minutes=1)
        assert loan.start < utc_now() + datetime.timedelta(minutes=1)
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end
        assert loan_url == loan.external_identifier
        assert 1 == db.session.query(Loan).count()

        # The book is no longer reserved for the patron, and the hold has been deleted.
        assert 0 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 0 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 0 == db.session.query(Hold).count()

    def test_checkin_from_external_library(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        # An integration client has a copy of this book checked out.
        odl_api_test_fixture.license.setup(concurrency=7, available=6)
        loan, ignore = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.client)
        loan.external_identifier = "http://loan/" + db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        # The patron returns the book successfully.
        lsd = json.dumps(
            {
                "status": "ready",
                "links": [
                    {
                        "rel": "return",
                        "href": "http://return",
                    }
                ],
            }
        )
        returned_lsd = json.dumps(
            {
                "status": "returned",
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        odl_api_test_fixture.api.queue_response(200)
        odl_api_test_fixture.api.queue_response(200, content=returned_lsd)
        odl_api_test_fixture.api.checkin_from_external_library(
            odl_api_test_fixture.client, loan
        )
        assert 3 == len(odl_api_test_fixture.api.requests)
        assert "http://loan" in odl_api_test_fixture.api.requests[0][0]
        assert "http://return" == odl_api_test_fixture.api.requests[1][0]
        assert "http://loan" in odl_api_test_fixture.api.requests[2][0]

        # The pool's availability has increased, and the local loan has
        # been deleted.
        assert 7 == odl_api_test_fixture.pool.licenses_available
        assert 0 == db.session.query(Loan).count()

    def test_fulfill_for_external_library(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        loan, ignore = odl_api_test_fixture.license.loan_to(odl_api_test_fixture.client)
        loan.external_identifier = db.fresh_str()
        loan.end = utc_now() + datetime.timedelta(days=3)

        lsd = json.dumps(
            {
                "status": "ready",
                "potential_rights": {"end": "2017-10-21T11:12:13Z"},
                "links": [
                    {
                        "rel": "license",
                        "href": "http://acsm",
                        "type": DeliveryMechanism.ADOBE_DRM,
                    }
                ],
            }
        )

        odl_api_test_fixture.api.queue_response(200, content=lsd)
        fulfillment = odl_api_test_fixture.api.fulfill_for_external_library(
            odl_api_test_fixture.client, loan, None
        )
        assert odl_api_test_fixture.collection == fulfillment.collection(db.session)
        assert (
            odl_api_test_fixture.pool.data_source.name == fulfillment.data_source_name
        )
        assert odl_api_test_fixture.pool.identifier.type == fulfillment.identifier_type
        assert odl_api_test_fixture.pool.identifier.identifier == fulfillment.identifier
        assert datetime_utc(2017, 10, 21, 11, 12, 13) == fulfillment.content_expires
        assert "http://acsm" == fulfillment.content_link
        assert DeliveryMechanism.ADOBE_DRM == fulfillment.content_type

    def test_release_hold_from_external_library(
        self, db: DatabaseTransactionFixture, odl_api_test_fixture: ODLAPITestFixture
    ):
        odl_api_test_fixture.license.setup(concurrency=1, available=1)
        other_patron = db.patron()
        odl_api_test_fixture.checkout(patron=other_patron)
        hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.client, position=1
        )

        assert (
            odl_api_test_fixture.api.release_hold_from_external_library(
                odl_api_test_fixture.client, hold
            )
            is True
        )
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 0 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 0 == db.session.query(Hold).count()

        odl_api_test_fixture.checkin(patron=other_patron)
        hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.client, position=0
        )

        assert (
            odl_api_test_fixture.api.release_hold_from_external_library(
                odl_api_test_fixture.client, hold
            )
            is True
        )
        assert 1 == odl_api_test_fixture.pool.licenses_available
        assert 0 == odl_api_test_fixture.pool.licenses_reserved
        assert 0 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 0 == db.session.query(Hold).count()

        hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            odl_api_test_fixture.client, position=0
        )
        other_hold, ignore = odl_api_test_fixture.pool.on_hold_to(
            db.patron(), position=2
        )

        assert (
            odl_api_test_fixture.api.release_hold_from_external_library(
                odl_api_test_fixture.client, hold
            )
            is True
        )
        assert 0 == odl_api_test_fixture.pool.licenses_available
        assert 1 == odl_api_test_fixture.pool.licenses_reserved
        assert 1 == odl_api_test_fixture.pool.patrons_in_hold_queue
        assert 1 == db.session.query(Hold).count()
        assert 0 == other_hold.position


class TestODLImporter:
    class MockGet:
        def __init__(self):
            self.responses = []

        def get(self, *args, **kwargs):
            return 200, {}, str(self.responses.pop(0))

        def add(self, item):
            return self.responses.append(item)

    class MockMetadataClient:
        def canonicalize_author_name(self, identifier, working_display_name):
            return working_display_name

    @pytest.fixture()
    def mock_get(self) -> MockGet:
        return self.MockGet()

    @pytest.fixture()
    def importer(
        self,
        db: DatabaseTransactionFixture,
        odl_test_fixture: ODLTestFixture,
        mock_get,
    ) -> ODLImporter:
        library = odl_test_fixture.library()
        return ODLImporter(
            db.session,
            collection=odl_test_fixture.collection(library),
            http_get=mock_get.get,
        )

    @pytest.fixture()
    def datasource(
        self, db: DatabaseTransactionFixture, odl_test_fixture: ODLTestFixture
    ) -> DataSource:
        collection = odl_test_fixture.collection(odl_test_fixture.library())
        data_source = DataSource.lookup(db.session, "Feedbooks", autocreate=True)
        collection.integration_configuration.set(
            Collection.DATA_SOURCE_NAME_SETTING, data_source.name
        )
        return data_source

    @pytest.fixture()
    def feed_template(self):
        return "feed_template.xml.jinja"

    @pytest.fixture()
    def import_templated(
        self,
        mock_get,
        importer,
        feed_template: str,
        api_odl_files_fixture: ODLAPIFilesFixture,
    ) -> Callable:
        def i(licenses: List[LicenseInfoHelper]) -> Tuple[List, List, List, List]:
            feed_licenses = [l.license for l in licenses]
            [mock_get.add(l) for l in licenses]
            feed = self.get_templated_feed(
                files=api_odl_files_fixture,
                filename=feed_template,
                licenses=feed_licenses,
            )
            return importer.import_from_feed(feed)

        return i

    def get_templated_feed(
        self, files: APIFilesFixture, filename: str, licenses: List[LicenseHelper]
    ) -> str:
        """Get the test ODL feed with specific licensing information.

        :param files: Access to test files
        :param filename: Name of template to load
        :param licenses: List of ODL licenses

        :return: Test ODL feed
        """
        text = files.sample_text(filename)
        template = Template(text)
        feed = template.render(licenses=licenses)
        return feed

    @freeze_time("2019-01-01T00:00:00+00:00")
    def test_import(self, importer, mock_get, odl_test_fixture: ODLTestFixture):
        """Ensure that ODLImporter correctly processes and imports the ODL feed encoded using OPDS 1.x.

        NOTE: `freeze_time` decorator is required to treat the licenses in the ODL feed as non-expired.
        """
        feed = odl_test_fixture.files.sample_data("feedbooks_bibliographic.atom")

        warrior_time_limited = LicenseInfoHelper(
            license=LicenseHelper(
                identifier="1", concurrency=1, expires="2019-03-31T03:13:35+02:00"
            ),
            left=52,
            available=1,
        )
        canadianity_loan_limited = LicenseInfoHelper(
            license=LicenseHelper(identifier="2", concurrency=10), left=40, available=10
        )
        canadianity_perpetual = LicenseInfoHelper(
            license=LicenseHelper(identifier="3", concurrency=1), available=1
        )
        midnight_loan_limited_1 = LicenseInfoHelper(
            license=LicenseHelper(
                identifier="4",
                concurrency=1,
            ),
            left=20,
            available=1,
        )
        midnight_loan_limited_2 = LicenseInfoHelper(
            license=LicenseHelper(identifier="5", concurrency=1), left=52, available=1
        )
        dragons_loan = LicenseInfoHelper(
            license=LicenseHelper(
                identifier="urn:uuid:01234567-890a-bcde-f012-3456789abcde",
                concurrency=5,
            ),
            left=10,
            available=5,
        )

        [
            mock_get.add(r)
            for r in [
                warrior_time_limited,
                canadianity_loan_limited,
                canadianity_perpetual,
                midnight_loan_limited_1,
                midnight_loan_limited_2,
                dragons_loan,
            ]
        ]

        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = importer.import_from_feed(feed)

        # This importer works the same as the base OPDSImporter, except that
        # it extracts format information from 'odl:license' tags and creates
        # LicensePoolDeliveryMechanisms.

        # The importer created 6 editions, pools, and works.
        assert {} == failures
        assert 6 == len(imported_editions)
        assert 6 == len(imported_pools)
        assert 6 == len(imported_works)

        [
            canadianity,
            everglades,
            dragons,
            warrior,
            blazing,
            midnight,
        ] = sorted(imported_editions, key=lambda x: x.title)
        assert "The Blazing World" == blazing.title
        assert "Sun Warrior" == warrior.title
        assert "Canadianity" == canadianity.title
        assert "The Midnight Dance" == midnight.title
        assert "Everglades Wildguide" == everglades.title
        assert "Rise of the Dragons, Book 1" == dragons.title

        # This book is open access and has no applicable DRM
        [blazing_pool] = [
            p for p in imported_pools if p.identifier == blazing.primary_identifier
        ]
        assert True == blazing_pool.open_access
        [lpdm] = blazing_pool.delivery_mechanisms
        assert Representation.EPUB_MEDIA_TYPE == lpdm.delivery_mechanism.content_type
        assert DeliveryMechanism.NO_DRM == lpdm.delivery_mechanism.drm_scheme

        # # This book has a single 'odl:license' tag.
        [warrior_pool] = [
            p for p in imported_pools if p.identifier == warrior.primary_identifier
        ]
        assert False == warrior_pool.open_access
        [lpdm] = warrior_pool.delivery_mechanisms
        assert Edition.BOOK_MEDIUM == warrior_pool.presentation_edition.medium
        assert Representation.EPUB_MEDIA_TYPE == lpdm.delivery_mechanism.content_type
        assert DeliveryMechanism.ADOBE_DRM == lpdm.delivery_mechanism.drm_scheme
        assert RightsStatus.IN_COPYRIGHT == lpdm.rights_status.uri
        assert (
            52 == warrior_pool.licenses_owned
        )  # 52 remaining checkouts in the License Info Document
        assert 1 == warrior_pool.licenses_available
        [license] = warrior_pool.licenses
        assert "1" == license.identifier
        assert (
            "https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}"
            == license.checkout_url
        )
        assert (
            "https://license.feedbooks.net/license/status/?uuid=1" == license.status_url
        )

        # The original value for 'expires' in the ODL is:
        # 2019-03-31T03:13:35+02:00
        #
        # As stored in the database, license.expires may not have the
        # same tzinfo, but it does represent the same point in time.
        assert (
            datetime.datetime(
                2019, 3, 31, 3, 13, 35, tzinfo=dateutil.tz.tzoffset("", 3600 * 2)
            )
            == license.expires
        )
        assert (
            52 == license.checkouts_left
        )  # 52 remaining checkouts in the License Info Document
        assert 1 == license.checkouts_available

        # This item is an open access audiobook.
        [everglades_pool] = [
            p for p in imported_pools if p.identifier == everglades.primary_identifier
        ]
        assert True == everglades_pool.open_access
        [lpdm] = everglades_pool.delivery_mechanisms
        assert Edition.AUDIO_MEDIUM == everglades_pool.presentation_edition.medium

        assert (
            Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
            == lpdm.delivery_mechanism.content_type
        )
        assert DeliveryMechanism.NO_DRM == lpdm.delivery_mechanism.drm_scheme

        # This is a non-open access audiobook. There is no
        # <odl:protection> tag; the drm_scheme is implied by the value
        # of <dcterms:format>.
        [dragons_pool] = [
            p for p in imported_pools if p.identifier == dragons.primary_identifier
        ]
        assert Edition.AUDIO_MEDIUM == dragons_pool.presentation_edition.medium
        assert False == dragons_pool.open_access
        [lpdm] = dragons_pool.delivery_mechanisms

        assert (
            Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
            == lpdm.delivery_mechanism.content_type
        )
        assert (
            DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM
            == lpdm.delivery_mechanism.drm_scheme
        )

        # This book has two 'odl:license' tags for the same format and drm scheme
        # (this happens if the library purchases two copies).
        [canadianity_pool] = [
            p for p in imported_pools if p.identifier == canadianity.primary_identifier
        ]
        assert False == canadianity_pool.open_access
        [lpdm] = canadianity_pool.delivery_mechanisms
        assert Representation.EPUB_MEDIA_TYPE == lpdm.delivery_mechanism.content_type
        assert DeliveryMechanism.ADOBE_DRM == lpdm.delivery_mechanism.drm_scheme
        assert RightsStatus.IN_COPYRIGHT == lpdm.rights_status.uri
        assert (
            41 == canadianity_pool.licenses_owned
        )  # 40 remaining checkouts + 1 perpetual license in the License Info Documents
        assert 11 == canadianity_pool.licenses_available
        [license1, license2] = sorted(
            canadianity_pool.licenses, key=lambda x: x.identifier
        )
        assert "2" == license1.identifier
        assert (
            "https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}"
            == license1.checkout_url
        )
        assert (
            "https://license.feedbooks.net/license/status/?uuid=2"
            == license1.status_url
        )
        assert None == license1.expires
        assert 40 == license1.checkouts_left
        assert 10 == license1.checkouts_available
        assert "3" == license2.identifier
        assert (
            "https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}"
            == license2.checkout_url
        )
        assert (
            "https://license.feedbooks.net/license/status/?uuid=3"
            == license2.status_url
        )
        assert None == license2.expires
        assert None == license2.checkouts_left
        assert 1 == license2.checkouts_available

        # This book has two 'odl:license' tags, and they have different formats.
        # TODO: the format+license association is not handled yet.
        [midnight_pool] = [
            p for p in imported_pools if p.identifier == midnight.primary_identifier
        ]
        assert False == midnight_pool.open_access
        lpdms = midnight_pool.delivery_mechanisms
        assert 2 == len(lpdms)
        assert {Representation.EPUB_MEDIA_TYPE, Representation.PDF_MEDIA_TYPE} == {
            lpdm.delivery_mechanism.content_type for lpdm in lpdms
        }
        assert [DeliveryMechanism.ADOBE_DRM, DeliveryMechanism.ADOBE_DRM] == [
            lpdm.delivery_mechanism.drm_scheme for lpdm in lpdms
        ]
        assert [RightsStatus.IN_COPYRIGHT, RightsStatus.IN_COPYRIGHT] == [
            lpdm.rights_status.uri for lpdm in lpdms
        ]
        assert (
            72 == midnight_pool.licenses_owned
        )  # 20 + 52 remaining checkouts in corresponding License Info Documents
        assert 2 == midnight_pool.licenses_available
        [license1, license2] = sorted(
            midnight_pool.licenses, key=lambda x: x.identifier
        )
        assert "4" == license1.identifier
        assert (
            "https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}"
            == license1.checkout_url
        )
        assert (
            "https://license.feedbooks.net/license/status/?uuid=4"
            == license1.status_url
        )
        assert None == license1.expires
        assert 20 == license1.checkouts_left
        assert 1 == license1.checkouts_available
        assert "5" == license2.identifier
        assert (
            "https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}"
            == license2.checkout_url
        )
        assert (
            "https://license.feedbooks.net/license/status/?uuid=5"
            == license2.status_url
        )
        assert None == license2.expires
        assert 52 == license2.checkouts_left
        assert 1 == license2.checkouts_available

    @pytest.mark.parametrize(
        "license",
        [
            pytest.param(
                LicenseInfoHelper(
                    license=LicenseHelper(
                        concurrency=1, expires="2021-01-01T00:01:00+01:00"
                    ),
                    left=52,
                    available=1,
                ),
                id="expiration_date_in_the_past",
            ),
            pytest.param(
                LicenseInfoHelper(
                    license=LicenseHelper(
                        concurrency=1,
                    ),
                    left=0,
                    available=1,
                ),
                id="left_is_zero",
            ),
            pytest.param(
                LicenseInfoHelper(
                    license=LicenseHelper(
                        concurrency=1,
                    ),
                    available=1,
                    status="unavailable",
                ),
                id="status_unavailable",
            ),
        ],
    )
    @freeze_time("2021-01-01T00:00:00+00:00")
    def test_odl_importer_expired_licenses(self, import_templated, license):
        """Ensure ODLImporter imports expired licenses, but does not count them."""
        # Import the test feed with an expired ODL license.
        imported_editions, imported_pools, imported_works, failures = import_templated(
            [license]
        )

        # The importer created 1 edition and 1 work with no failures.
        assert failures == {}
        assert len(imported_editions) == 1
        assert len(imported_works) == 1

        # Ensure that the license pool was successfully created, with no available copies.
        assert len(imported_pools) == 1

        [imported_pool] = imported_pools
        assert imported_pool.licenses_owned == 0
        assert imported_pool.licenses_available == 0
        assert len(imported_pool.licenses) == 1

        # Ensure the license was imported and is expired.
        [imported_license] = imported_pool.licenses
        assert imported_license.is_inactive is True

    def test_odl_importer_reimport_expired_licenses(self, import_templated):
        license_expiry = dateutil.parser.parse("2021-01-01T00:01:00+00:00")
        licenses = [
            LicenseInfoHelper(
                license=LicenseHelper(concurrency=1, expires=license_expiry),
                available=1,
            )
        ]

        # First import the license when it is not expired
        with freeze_time(license_expiry - datetime.timedelta(days=1)):

            # Import the test feed.
            (
                imported_editions,
                imported_pools,
                imported_works,
                failures,
            ) = import_templated(licenses)

            # The importer created 1 edition and 1 work with no failures.
            assert failures == {}
            assert len(imported_editions) == 1
            assert len(imported_works) == 1
            assert len(imported_pools) == 1

            # Ensure that the license pool was successfully created, with available copies.
            [imported_pool] = imported_pools
            assert imported_pool.licenses_owned == 1
            assert imported_pool.licenses_available == 1
            assert len(imported_pool.licenses) == 1

            # Ensure the license was imported and is not expired.
            [imported_license] = imported_pool.licenses
            assert imported_license.is_inactive is False

        # Reimport the license when it is expired
        with freeze_time(license_expiry + datetime.timedelta(days=1)):

            # Import the test feed.
            (
                imported_editions,
                imported_pools,
                imported_works,
                failures,
            ) = import_templated(licenses)

            # The importer created 1 edition and 1 work with no failures.
            assert failures == {}
            assert len(imported_editions) == 1
            assert len(imported_works) == 1
            assert len(imported_pools) == 1

            # Ensure that the license pool was successfully created, with no available copies.
            [imported_pool] = imported_pools
            assert imported_pool.licenses_owned == 0
            assert imported_pool.licenses_available == 0
            assert len(imported_pool.licenses) == 1

            # Ensure the license was imported and is expired.
            [imported_license] = imported_pool.licenses
            assert imported_license.is_inactive is True

    @freeze_time("2021-01-01T00:00:00+00:00")
    def test_odl_importer_multiple_expired_licenses(self, import_templated):
        """Ensure ODLImporter imports expired licenses
        and does not count them in the total number of available licenses."""

        # 1.1. Import the test feed with three inactive ODL licenses and two active licenses.
        inactive = [
            LicenseInfoHelper(
                # Expired
                # (expiry date in the past)
                license=LicenseHelper(
                    concurrency=1,
                    expires=datetime_helpers.utc_now() - datetime.timedelta(days=1),
                ),
                available=1,
            ),
            LicenseInfoHelper(
                # Expired
                # (left is 0)
                license=LicenseHelper(concurrency=1),
                available=1,
                left=0,
            ),
            LicenseInfoHelper(
                # Expired
                # (status is unavailable)
                license=LicenseHelper(concurrency=1),
                available=1,
                status="unavailable",
            ),
        ]
        active = [
            LicenseInfoHelper(
                # Valid
                license=LicenseHelper(concurrency=1),
                available=1,
            ),
            LicenseInfoHelper(
                # Valid
                license=LicenseHelper(concurrency=5),
                available=5,
                left=40,
            ),
        ]
        imported_editions, imported_pools, imported_works, failures = import_templated(
            active + inactive
        )

        assert failures == {}

        # License pool was successfully created
        assert len(imported_pools) == 1
        [imported_pool] = imported_pools

        # All licenses were imported
        assert len(imported_pool.licenses) == 5

        # Make sure that the license statistics are correct and include only active licenses.
        assert imported_pool.licenses_owned == 41
        assert imported_pool.licenses_available == 6

        # Correct number of active and inactive licenses
        assert sum(not l.is_inactive for l in imported_pool.licenses) == len(active)
        assert sum(l.is_inactive for l in imported_pool.licenses) == len(inactive)

    def test_odl_importer_reimport_multiple_licenses(self, import_templated):
        """Ensure ODLImporter correctly imports licenses that have already been imported."""

        # 1.1. Import the test feed with ODL licenses that are not expired.
        license_expiry = dateutil.parser.parse("2021-01-01T00:01:00+00:00")

        date = LicenseInfoHelper(
            license=LicenseHelper(
                concurrency=1,
                expires=license_expiry,
            ),
            available=1,
        )
        left = LicenseInfoHelper(
            license=LicenseHelper(concurrency=2), available=1, left=5
        )
        perpetual = LicenseInfoHelper(license=LicenseHelper(concurrency=1), available=0)
        licenses = [date, left, perpetual]

        # Import with all licenses valid
        with freeze_time(license_expiry - datetime.timedelta(days=1)):
            (
                imported_editions,
                imported_pools,
                imported_works,
                failures,
            ) = import_templated(licenses)

            # No failures in the import
            assert failures == {}

            assert len(imported_pools) == 1

            [imported_pool] = imported_pools
            assert len(imported_pool.licenses) == 3
            assert imported_pool.licenses_available == 2
            assert imported_pool.licenses_owned == 7

            # No licenses are expired
            assert sum(not l.is_inactive for l in imported_pool.licenses) == len(
                licenses
            )

        # Expire the first two licenses

        # The first one is expired by changing the time
        with freeze_time(license_expiry + datetime.timedelta(days=1)):
            # The second one is expired by setting left to 0
            left.left = 0

            # The perpetual license has a copy available
            perpetual.available = 1

            # Reimport
            (
                imported_editions,
                imported_pools,
                imported_works,
                failures,
            ) = import_templated(licenses)

            # No failures in the import
            assert failures == {}

            assert len(imported_pools) == 1

            [imported_pool] = imported_pools
            assert len(imported_pool.licenses) == 3
            assert imported_pool.licenses_available == 1
            assert imported_pool.licenses_owned == 1

            # One license not expired
            assert sum(not l.is_inactive for l in imported_pool.licenses) == 1

            # Two licenses expired
            assert sum(l.is_inactive for l in imported_pool.licenses) == 2


class TestODLHoldReaper:
    def test_run_once(
        self, odl_test_fixture: ODLTestFixture, db: DatabaseTransactionFixture
    ):
        library = odl_test_fixture.library()
        collection = odl_test_fixture.collection(library)
        work = odl_test_fixture.work(collection)
        license = odl_test_fixture.license(work)
        api = odl_test_fixture.api(collection)
        pool = odl_test_fixture.pool(license)

        data_source = DataSource.lookup(db.session, "Feedbooks", autocreate=True)
        collection.integration_configuration.set(
            Collection.DATA_SOURCE_NAME_SETTING, data_source.name
        )
        reaper = ODLHoldReaper(db.session, collection, api=api)

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)

        license.setup(concurrency=3, available=3)
        expired_hold1, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=0)
        expired_hold2, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=0)
        expired_hold3, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=0)
        current_hold, ignore = pool.on_hold_to(db.patron(), position=3)
        # This hold has an end date in the past, but its position is greater than 0
        # so the end date is not reliable.
        bad_end_date, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=4)

        progress = reaper.run_once(reaper.timestamp().to_data())

        # The expired holds have been deleted and the other holds have been updated.
        assert 2 == db.session.query(Hold).count()
        assert [current_hold, bad_end_date] == db.session.query(Hold).order_by(
            Hold.start
        ).all()
        assert 0 == current_hold.position
        assert 0 == bad_end_date.position
        assert current_hold.end > now
        assert bad_end_date.end > now
        assert 1 == pool.licenses_available
        assert 2 == pool.licenses_reserved

        # The TimestampData returned reflects what work was done.
        assert "Holds deleted: 3. License pools updated: 1" == progress.achievements

        # The TimestampData does not include any timing information --
        # that will be applied by run().
        assert None == progress.start
        assert None == progress.finish


class SharedODLAPIFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, api_odl_files_fixture: ODLAPIFilesFixture
    ):
        self.db = db
        self.files = api_odl_files_fixture
        self.collection = MockSharedODLAPI.mock_collection(db.session)
        self.collection.integration_configuration.set(
            Collection.DATA_SOURCE_NAME_SETTING, "Feedbooks"
        )
        self.api = MockSharedODLAPI(db.session, self.collection)
        self.pool = db.licensepool(None, collection=self.collection)
        self.pool.identifier.add_link(
            Hyperlink.BORROW, db.fresh_str(), self.collection.data_source
        )
        self.patron = db.patron()


@pytest.fixture(scope="function")
def shared_odl(
    db: DatabaseTransactionFixture, api_odl_files_fixture: ODLAPIFilesFixture
) -> SharedODLAPIFixture:
    return SharedODLAPIFixture(db, api_odl_files_fixture)


class TestSharedODLAPI:
    def test_get(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        # Create a SharedODLAPI to test the _get method. The other tests use a
        # mock API class that overrides _get.
        api = SharedODLAPI(db.session, shared_odl.collection)

        # The library has not registered with the remote collection yet.
        def do_get(url, headers=None, allowed_response_codes=None):
            raise Exception("do_get should not be called")

        pytest.raises(
            LibraryAuthorizationFailedException,
            api._get,
            "test url",
            patron=shared_odl.patron,
            do_get=do_get,
        )

        # Once the library registers, it gets a shared secret that is included
        # in request headers.
        config = shared_odl.collection.integration_configuration
        config.for_library(shared_odl.patron.library.id, create=True).set(
            ExternalIntegration.PASSWORD, "secret"
        )

        def do_get2(url, headers=None, allowed_response_codes=None):
            assert "test url" == url
            assert "test header value" == headers.get("test_key")
            assert "Bearer " + base64.b64encode("secret") == headers.get(
                "Authorization"
            )
            assert ["200"] == allowed_response_codes

        api._get(
            "test url",
            headers=dict(test_key="test header value"),
            patron=shared_odl.patron,
            allowed_response_codes=["200"],
            do_get=do_get2,
        )

    def test_checkout_success(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        response = files.sample_data("shared_collection_borrow_success.opds")
        shared_odl.api.queue_response(200, content=response)

        loan = shared_odl.api.checkout(
            shared_odl.patron, "pin", shared_odl.pool, Representation.EPUB_MEDIA_TYPE
        )
        assert shared_odl.collection == loan.collection(db.session)
        assert shared_odl.pool.data_source.name == loan.data_source_name
        assert shared_odl.pool.identifier.type == loan.identifier_type
        assert shared_odl.pool.identifier.identifier == loan.identifier
        assert datetime_utc(2018, 3, 8, 17, 41, 31) == loan.start_date
        assert datetime_utc(2018, 3, 29, 17, 41, 30) == loan.end_date
        assert (
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/31"
            == loan.external_identifier
        )

        assert [
            shared_odl.pool.identifier.links[0].resource.url
        ] == shared_odl.api.requests

    def test_checkout_from_hold(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        hold, ignore = shared_odl.pool.on_hold_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        hold_info_response = files.sample_data("shared_collection_hold_info_ready.opds")
        shared_odl.api.queue_response(200, content=hold_info_response)
        borrow_response = files.sample_data("shared_collection_borrow_success.opds")
        shared_odl.api.queue_response(200, content=borrow_response)

        loan = shared_odl.api.checkout(
            shared_odl.patron, "pin", shared_odl.pool, Representation.EPUB_MEDIA_TYPE
        )
        assert shared_odl.collection == loan.collection(db.session)
        assert shared_odl.pool.data_source.name == loan.data_source_name
        assert shared_odl.pool.identifier.type == loan.identifier_type
        assert shared_odl.pool.identifier.identifier == loan.identifier
        assert datetime_utc(2018, 3, 8, 17, 41, 31) == loan.start_date
        assert datetime_utc(2018, 3, 29, 17, 41, 30) == loan.end_date
        assert (
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/31"
            == loan.external_identifier
        )

        assert [
            hold.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/17/borrow",
        ] == shared_odl.api.requests

    def test_checkout_already_checked_out(self, shared_odl: SharedODLAPIFixture):
        loan, ignore = shared_odl.pool.loan_to(shared_odl.patron)
        pytest.raises(
            AlreadyCheckedOut,
            shared_odl.api.checkout,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [] == shared_odl.api.requests

    def test_checkout_no_available_copies(self, shared_odl: SharedODLAPIFixture):
        shared_odl.api.queue_response(403)
        pytest.raises(
            NoAvailableCopies,
            shared_odl.api.checkout,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [
            shared_odl.pool.identifier.links[0].resource.url
        ] == shared_odl.api.requests

    def test_checkout_no_licenses(self, shared_odl: SharedODLAPIFixture):
        shared_odl.api.queue_response(
            NO_LICENSES.response[1],
            headers=NO_LICENSES.response[2],
            content=NO_LICENSES.response[0],
        )
        pytest.raises(
            NoLicenses,
            shared_odl.api.checkout,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [
            shared_odl.pool.identifier.links[0].resource.url
        ] == shared_odl.api.requests

    def test_checkout_from_hold_not_available(self, shared_odl: SharedODLAPIFixture):
        files = shared_odl.files
        hold, ignore = shared_odl.pool.on_hold_to(shared_odl.patron)
        hold_info_response = files.sample_data(
            "shared_collection_hold_info_reserved.opds"
        )
        shared_odl.api.queue_response(200, content=hold_info_response)
        pytest.raises(
            NoAvailableCopies,
            shared_odl.api.checkout,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [hold.external_identifier] == shared_odl.api.requests

    def test_checkout_cannot_loan(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        shared_odl.api.queue_response(500)
        pytest.raises(
            CannotLoan,
            shared_odl.api.checkout,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [
            shared_odl.pool.identifier.links[0].resource.url
        ] == shared_odl.api.requests

        # This pool has no borrow link.
        pool = db.licensepool(None, collection=shared_odl.collection)
        pytest.raises(
            CannotLoan,
            shared_odl.api.checkout,
            shared_odl.patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

    def test_checkin_success(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        loan, ignore = shared_odl.pool.loan_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        loan_info_response = files.sample_data("shared_collection_loan_info.opds")
        shared_odl.api.queue_response(200, content=loan_info_response)
        shared_odl.api.queue_response(200, content="Deleted")
        response = shared_odl.api.checkin(shared_odl.patron, "pin", shared_odl.pool)
        assert True == response
        assert [
            loan.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/revoke",
        ] == shared_odl.api.requests

    def test_checkin_not_checked_out(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        pytest.raises(
            NotCheckedOut,
            shared_odl.api.checkin,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
        )
        assert [] == shared_odl.api.requests

        loan, ignore = shared_odl.pool.loan_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        shared_odl.api.queue_response(404)
        pytest.raises(
            NotCheckedOut,
            shared_odl.api.checkin,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
        )
        assert [loan.external_identifier] == shared_odl.api.requests

    def test_checkin_cannot_return(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        loan, ignore = shared_odl.pool.loan_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        shared_odl.api.queue_response(500)
        pytest.raises(
            CannotReturn,
            shared_odl.api.checkin,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
        )
        assert [loan.external_identifier] == shared_odl.api.requests

        loan_info_response = files.sample_data("shared_collection_loan_info.opds")
        shared_odl.api.queue_response(200, content=loan_info_response)
        shared_odl.api.queue_response(500)
        pytest.raises(
            CannotReturn,
            shared_odl.api.checkin,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
        )
        assert [
            loan.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/revoke",
        ] == shared_odl.api.requests[1:]

    def test_fulfill_success(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        loan, ignore = shared_odl.pool.loan_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        loan_info_response = files.sample_data("shared_collection_loan_info.opds")
        shared_odl.api.queue_response(200, content=loan_info_response)
        shared_odl.api.queue_response(200, content="An ACSM file")
        fulfillment = shared_odl.api.fulfill(
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            shared_odl.pool.delivery_mechanisms[0],
        )
        assert shared_odl.collection == fulfillment.collection(db.session)
        assert shared_odl.pool.data_source.name == fulfillment.data_source_name
        assert shared_odl.pool.identifier.type == fulfillment.identifier_type
        assert shared_odl.pool.identifier.identifier == fulfillment.identifier
        assert None == fulfillment.content_link
        assert b"An ACSM file" == fulfillment.content
        assert datetime_utc(2018, 3, 29, 17, 44, 11) == fulfillment.content_expires

        assert [
            loan.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/fulfill/2",
        ] == shared_odl.api.requests

    def test_fulfill_not_checked_out(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        pytest.raises(
            NotCheckedOut,
            shared_odl.api.fulfill,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            shared_odl.pool.delivery_mechanisms[0],
        )
        assert [] == shared_odl.api.requests

        loan, ignore = shared_odl.pool.loan_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        shared_odl.api.queue_response(404)
        pytest.raises(
            NotCheckedOut,
            shared_odl.api.fulfill,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            shared_odl.pool.delivery_mechanisms[0],
        )
        assert [loan.external_identifier] == shared_odl.api.requests

    def test_fulfill_cannot_fulfill(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        loan, ignore = shared_odl.pool.loan_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        shared_odl.api.queue_response(500)
        pytest.raises(
            CannotFulfill,
            shared_odl.api.fulfill,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            shared_odl.pool.delivery_mechanisms[0],
        )
        assert [loan.external_identifier] == shared_odl.api.requests

        shared_odl.api.queue_response(200, content="not opds")
        pytest.raises(
            CannotFulfill,
            shared_odl.api.fulfill,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            shared_odl.pool.delivery_mechanisms[0],
        )
        assert [loan.external_identifier] == shared_odl.api.requests[1:]

        loan_info_response = files.sample_data("shared_collection_loan_info.opds")
        shared_odl.api.queue_response(200, content=loan_info_response)
        shared_odl.api.queue_response(500)
        pytest.raises(
            CannotFulfill,
            shared_odl.api.fulfill,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            shared_odl.pool.delivery_mechanisms[0],
        )
        assert [
            loan.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/fulfill/2",
        ] == shared_odl.api.requests[2:]

    def test_fulfill_format_not_available(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        loan, ignore = shared_odl.pool.loan_to(shared_odl.patron)
        loan_info_response = files.sample_data(
            "shared_collection_loan_info_no_epub.opds"
        )
        shared_odl.api.queue_response(200, content=loan_info_response)
        pytest.raises(
            FormatNotAvailable,
            shared_odl.api.fulfill,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            shared_odl.pool.delivery_mechanisms[0],
        )
        assert [loan.external_identifier] == shared_odl.api.requests

    def test_place_hold_success(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        hold_response = files.sample_data("shared_collection_hold_info_reserved.opds")
        shared_odl.api.queue_response(200, content=hold_response)
        hold = shared_odl.api.place_hold(
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            "notifications@librarysimplified.org",
        )
        assert shared_odl.collection == hold.collection(db.session)
        assert shared_odl.pool.data_source.name == hold.data_source_name
        assert shared_odl.pool.identifier.type == hold.identifier_type
        assert shared_odl.pool.identifier.identifier == hold.identifier
        assert datetime_utc(2018, 3, 8, 18, 50, 18) == hold.start_date
        assert datetime_utc(2018, 3, 29, 17, 44, 1) == hold.end_date
        assert 1 == hold.hold_position
        assert (
            "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/18"
            == hold.external_identifier
        )

        assert [
            shared_odl.pool.identifier.links[0].resource.url
        ] == shared_odl.api.requests

    def test_place_hold_already_checked_out(self, shared_odl: SharedODLAPIFixture):
        loan, ignore = shared_odl.pool.loan_to(shared_odl.patron)
        pytest.raises(
            AlreadyCheckedOut,
            shared_odl.api.place_hold,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
            "notification@librarysimplified.org",
        )
        assert [] == shared_odl.api.requests

    def test_release_hold_success(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        hold, ignore = shared_odl.pool.on_hold_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        hold_response = files.sample_data("shared_collection_hold_info_reserved.opds")
        shared_odl.api.queue_response(200, content=hold_response)
        shared_odl.api.queue_response(200, content="Deleted")
        response = shared_odl.api.release_hold(
            shared_odl.patron, "pin", shared_odl.pool
        )
        assert True == response
        assert [
            hold.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/18/revoke",
        ] == shared_odl.api.requests

    def test_release_hold_not_on_hold(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        pytest.raises(
            NotOnHold,
            shared_odl.api.release_hold,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
        )
        assert [] == shared_odl.api.requests

        hold, ignore = shared_odl.pool.on_hold_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        shared_odl.api.queue_response(404)
        pytest.raises(
            NotOnHold,
            shared_odl.api.release_hold,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
        )
        assert [hold.external_identifier] == shared_odl.api.requests

    def test_release_hold_cannot_release_hold(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        hold, ignore = shared_odl.pool.on_hold_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        shared_odl.api.queue_response(500)
        pytest.raises(
            CannotReleaseHold,
            shared_odl.api.release_hold,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
        )
        assert [hold.external_identifier] == shared_odl.api.requests

        hold_response = files.sample_data("shared_collection_hold_info_reserved.opds")
        shared_odl.api.queue_response(200, content=hold_response)
        shared_odl.api.queue_response(500)
        pytest.raises(
            CannotReleaseHold,
            shared_odl.api.release_hold,
            shared_odl.patron,
            "pin",
            shared_odl.pool,
        )
        assert [
            hold.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/18/revoke",
        ] == shared_odl.api.requests[1:]

    def test_patron_activity_success(self, shared_odl: SharedODLAPIFixture):
        db, files = shared_odl.db, shared_odl.files

        # The patron has one loan, and the remote circ manager returns it.
        loan, ignore = shared_odl.pool.loan_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        loan_response = files.sample_data("shared_collection_loan_info.opds")
        shared_odl.api.queue_response(200, content=loan_response)
        activity = shared_odl.api.patron_activity(shared_odl.patron, "pin")
        assert 1 == len(activity)
        [loan_info] = activity
        assert shared_odl.collection == loan_info.collection(db.session)
        assert shared_odl.pool.data_source.name == loan_info.data_source_name
        assert shared_odl.pool.identifier.type == loan_info.identifier_type
        assert shared_odl.pool.identifier.identifier == loan_info.identifier
        assert datetime_utc(2018, 3, 8, 17, 44, 12) == loan_info.start_date
        assert datetime_utc(2018, 3, 29, 17, 44, 11) == loan_info.end_date
        assert [loan.external_identifier] == shared_odl.api.requests

        # The _get method was passed a patron - this is necessary because
        # the patron_activity method may be called from a thread without
        # access to the flask request.
        assert shared_odl.patron == shared_odl.api.request_args[0][0]

        # The patron's loan has been deleted on the remote.
        shared_odl.api.queue_response(404, content="No loan here")
        activity = shared_odl.api.patron_activity(shared_odl.patron, "pin")
        assert 0 == len(activity)
        assert [loan.external_identifier] == shared_odl.api.requests[1:]

        # Now the patron has a hold instead.
        db.session.delete(loan)
        hold, ignore = shared_odl.pool.on_hold_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        hold_response = files.sample_data("shared_collection_hold_info_reserved.opds")
        shared_odl.api.queue_response(200, content=hold_response)
        activity = shared_odl.api.patron_activity(shared_odl.patron, "pin")
        assert 1 == len(activity)
        [hold_info] = activity
        assert shared_odl.collection == hold_info.collection(db.session)
        assert shared_odl.pool.data_source.name == hold_info.data_source_name
        assert shared_odl.pool.identifier.type == hold_info.identifier_type
        assert shared_odl.pool.identifier.identifier == hold_info.identifier
        assert datetime_utc(2018, 3, 8, 18, 50, 18) == hold_info.start_date
        assert datetime_utc(2018, 3, 29, 17, 44, 1) == hold_info.end_date
        assert [hold.external_identifier] == shared_odl.api.requests[2:]

        # The patron's hold has been deleted on the remote.
        shared_odl.api.queue_response(404, content="No hold here")
        activity = shared_odl.api.patron_activity(shared_odl.patron, "pin")
        assert 0 == len(activity)
        assert [hold.external_identifier] == shared_odl.api.requests[3:]

    def test_patron_activity_remote_integration_exception(
        self, shared_odl: SharedODLAPIFixture
    ):
        db, files = shared_odl.db, shared_odl.files

        loan, ignore = shared_odl.pool.loan_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        shared_odl.api.queue_response(500)
        pytest.raises(
            RemoteIntegrationException,
            shared_odl.api.patron_activity,
            shared_odl.patron,
            "pin",
        )
        assert [loan.external_identifier] == shared_odl.api.requests
        db.session.delete(loan)

        hold, ignore = shared_odl.pool.on_hold_to(
            shared_odl.patron, external_identifier=db.fresh_str()
        )
        shared_odl.api.queue_response(500)
        pytest.raises(
            RemoteIntegrationException,
            shared_odl.api.patron_activity,
            shared_odl.patron,
            "pin",
        )
        assert [hold.external_identifier] == shared_odl.api.requests[1:]


class TestSharedODLImporter:
    def test_get_fulfill_url(self, api_odl_files_fixture: ODLAPIFilesFixture):
        entry = api_odl_files_fixture.sample_data("shared_collection_loan_info.opds")
        assert (
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/fulfill/2"
            == SharedODLImporter.get_fulfill_url(
                entry, "application/epub+zip", "application/vnd.adobe.adept+xml"
            )
        )
        assert None == SharedODLImporter.get_fulfill_url(
            entry, "application/pdf", "application/vnd.adobe.adept+xml"
        )
        assert None == SharedODLImporter.get_fulfill_url(
            entry, "application/epub+zip", None
        )

    def test_import(
        self, db: DatabaseTransactionFixture, api_odl_files_fixture: ODLAPIFilesFixture
    ):
        feed = api_odl_files_fixture.sample_data("shared_collection_feed.opds")
        data_source = DataSource.lookup(db.session, "DPLA Exchange", autocreate=True)
        collection = MockSharedODLAPI.mock_collection(db.session)
        collection.integration_configuration.set(
            Collection.DATA_SOURCE_NAME_SETTING, data_source.name
        )

        class MockMetadataClient:
            def canonicalize_author_name(self, identifier, working_display_name):
                return working_display_name

        importer = SharedODLImporter(db.session, collection=collection)

        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = importer.import_from_feed(feed)

        # This importer works the same as the base OPDSImporter, except that
        # it extracts license pool information from acquisition links.

        # The importer created 3 editions, pools, and works.
        assert 3 == len(imported_editions)
        assert 3 == len(imported_pools)
        assert 3 == len(imported_works)

        [six_months, essex, gatsby] = sorted(imported_editions, key=lambda x: x.title)
        assert "Six Months, Three Days, Five Others" == six_months.title
        assert "The Essex Serpent" == essex.title
        assert "The Great Gatsby" == gatsby.title

        # This book is open access.
        [gatsby_pool] = [
            p for p in imported_pools if p.identifier == gatsby.primary_identifier
        ]
        assert True == gatsby_pool.open_access
        # This pool has two delivery mechanisms, from a borrow link and an open-access link.
        # Both are DRM-free epubs.
        lpdms = gatsby_pool.delivery_mechanisms
        assert 2 == len(lpdms)
        for lpdm in lpdms:
            assert (
                Representation.EPUB_MEDIA_TYPE == lpdm.delivery_mechanism.content_type
            )
            assert DeliveryMechanism.NO_DRM == lpdm.delivery_mechanism.drm_scheme

        # This book is already checked out and has a hold.
        [six_months_pool] = [
            p for p in imported_pools if p.identifier == six_months.primary_identifier
        ]
        assert False == six_months_pool.open_access
        assert 1 == six_months_pool.licenses_owned
        assert 0 == six_months_pool.licenses_available
        assert 1 == six_months_pool.patrons_in_hold_queue
        [lpdm] = six_months_pool.delivery_mechanisms
        assert Representation.EPUB_MEDIA_TYPE == lpdm.delivery_mechanism.content_type
        assert DeliveryMechanism.ADOBE_DRM == lpdm.delivery_mechanism.drm_scheme
        assert RightsStatus.IN_COPYRIGHT == lpdm.rights_status.uri
        [borrow_link] = [
            l for l in six_months_pool.identifier.links if l.rel == Hyperlink.BORROW
        ]
        assert (
            "http://localhost:6500/AL/works/URI/http://www.feedbooks.com/item/2493650/borrow"
            == borrow_link.resource.url
        )

        # This book is currently available.
        [essex_pool] = [
            p for p in imported_pools if p.identifier == essex.primary_identifier
        ]
        assert False == essex_pool.open_access
        assert 4 == essex_pool.licenses_owned
        assert 4 == essex_pool.licenses_available
        assert 0 == essex_pool.patrons_in_hold_queue
        [lpdm] = essex_pool.delivery_mechanisms
        assert Representation.EPUB_MEDIA_TYPE == lpdm.delivery_mechanism.content_type
        assert DeliveryMechanism.ADOBE_DRM == lpdm.delivery_mechanism.drm_scheme
        assert RightsStatus.IN_COPYRIGHT == lpdm.rights_status.uri
        [borrow_link] = [
            l for l in essex_pool.identifier.links if l.rel == Hyperlink.BORROW
        ]
        assert (
            "http://localhost:6500/AL/works/URI/http://www.feedbooks.com/item/1946289/borrow"
            == borrow_link.resource.url
        )
