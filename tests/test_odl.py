import datetime
import json
import os
import types
import urllib.parse
import uuid
from typing import Callable, List, Optional, Tuple, Union

import dateutil
import pytest
from freezegun import freeze_time
from jinja2 import Template

from api.circulation_exceptions import *
from api.odl import (
    ODLAPI,
    MockSharedODLAPI,
    ODLAPIConfiguration,
    ODLHoldReaper,
    ODLImporter,
    SharedODLAPI,
    SharedODLImporter,
)
from core.model import (
    Collection,
    ConfigurationSetting,
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
    get_one_or_create,
)
from core.testing import DatabaseTest, MockRequestsResponse
from core.util import datetime_helpers
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.http import HTTP, BadResponseException, RemoteIntegrationException
from core.util.string_helpers import base64


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
        self.identifier: str = (
            identifier if identifier else "urn:uuid:{}".format(uuid.uuid1())
        )
        self.checkouts: Optional[int] = checkouts
        self.concurrency: Optional[int] = concurrency
        if isinstance(expires, datetime.datetime):
            self.expires = expires.isoformat()
        else:
            self.expires: Optional[str] = expires


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
        self.left: int = left
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
            output["terms"]["expires"] = self.license.expires
        if self.left is not None:
            output["checkouts"]["left"] = self.left
        return json.dumps(output)


class BaseODLTest:
    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "odl")

    @classmethod
    def get_data(cls, filename):
        path = os.path.join(cls.resource_path, filename)
        return open(path, "r").read()

    @pytest.fixture()
    def db(self):
        return self._db

    @pytest.fixture()
    def library(self, db):
        return DatabaseTest.make_default_library(db)

    @pytest.fixture()
    def integration_protocol(self):
        return ODLAPI.NAME

    @pytest.fixture()
    def collection(self, db, library, integration_protocol):
        """Create a mock ODL collection to use in tests."""
        collection, ignore = get_one_or_create(
            db,
            Collection,
            name="Test ODL Collection",
            create_method_kwargs=dict(
                external_account_id="http://odl",
            ),
        )
        integration = collection.create_external_integration(
            protocol=integration_protocol
        )
        integration.username = "a"
        integration.password = "b"
        integration.url = "http://metadata"
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, "Feedbooks"
        )
        library.collections.append(collection)
        return collection

    @pytest.fixture()
    def patron(self):
        return self._patron()

    @pytest.fixture()
    def work(self, collection):
        return self._work(with_license_pool=True, collection=collection)

    @pytest.fixture()
    def pool(self, license):
        return license.license_pool

    @pytest.fixture()
    def license(self, work):
        def setup(self, available, concurrency, left=None, expires=None):
            self.checkouts_available = available
            self.checkouts_left = left
            self.terms_concurrency = concurrency
            self.expires = expires
            self.license_pool.update_availability_from_licenses()

        pool = work.license_pools[0]
        l = self._license(
            pool,
            checkout_url="https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url,hint,hint_url}",
            checkouts_available=1,
            terms_concurrency=1,
        )
        l.setup = types.MethodType(setup, l)
        pool.update_availability_from_licenses()
        return l


class BaseODLAPITest(BaseODLTest):
    @pytest.fixture()
    def api_class(self, monkeypatch, db):
        def queue_response(self, status_code, headers={}, content=None):
            self.responses.insert(
                0, MockRequestsResponse(status_code, headers, content)
            )

        def _get(self, url, headers=None):
            self.requests.append([url, headers])
            response = self.responses.pop()
            return HTTP._process_response(url, response)

        def _url_for(self, *args, **kwargs):
            del kwargs["_external"]
            return "http://%s?%s" % (
                "/".join(args),
                "&".join(["%s=%s" % (key, val) for key, val in list(kwargs.items())]),
            )

        monkeypatch.setattr(ODLAPI, "_get", _get)
        monkeypatch.setattr(ODLAPI, "_url_for", _url_for)
        monkeypatch.setattr(ODLAPI, "queue_response", queue_response, raising=False)
        return ODLAPI

    @pytest.fixture()
    def api(self, db, api_class, collection):
        api = api_class(db, collection)
        api.requests = []
        api.responses = []
        return api

    @pytest.fixture()
    def client(self):
        return self._integration_client()

    @pytest.fixture()
    def checkin(self, api, patron, pool):
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

        def c(patron=patron, pool=pool):
            api.queue_response(200, content=lsd)
            api.queue_response(200)
            api.queue_response(200, content=returned_lsd)
            api.checkin(patron, "pin", pool)

        return c

    @pytest.fixture()
    def checkout(self, api, patron, pool, db):
        def c(patron=patron, pool=pool, loan_url=None):
            loan_url = loan_url or self._str
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
            api.queue_response(200, content=lsd)
            loan = api.checkout(patron, "pin", pool, Representation.EPUB_MEDIA_TYPE)
            loan_db = (
                db.query(Loan)
                .filter(Loan.license_pool == pool, Loan.patron == patron)
                .one()
            )
            return loan, loan_db

        return c


class TestODLAPI(DatabaseTest, BaseODLAPITest):
    def test_get_license_status_document_success(self, license, patron, api, library):
        # With a new loan.
        loan, _ = license.loan_to(patron)
        api.queue_response(200, content=json.dumps(dict(status="ready")))
        api.get_license_status_document(loan)
        requested_url = api.requests[0][0]

        parsed = urllib.parse.urlparse(requested_url)
        assert "https" == parsed.scheme
        assert "loan.feedbooks.net" == parsed.netloc
        params = urllib.parse.parse_qs(parsed.query)

        assert ODLAPIConfiguration.passphrase_hint.default == params.get("hint")[0]
        assert (
            ODLAPIConfiguration.passphrase_hint_url.default == params.get("hint_url")[0]
        )

        assert license.identifier == params.get("id")[0]

        # The checkout id and patron id are random UUIDs.
        checkout_id = params.get("checkout_id")[0]
        assert len(checkout_id) > 0
        patron_id = params.get("patron_id")[0]
        assert len(patron_id) > 0

        # Loans expire in 21 days by default.
        now = utc_now()
        after_expiration = now + datetime.timedelta(days=23)
        expires = urllib.parse.unquote(params.get("expires")[0])

        # The expiration time passed to the server is associated with
        # the UTC time zone.
        assert expires.endswith("+00:00")
        expires = dateutil.parser.parse(expires)
        assert expires.tzinfo == dateutil.tz.tz.tzutc()

        # It's a time in the future, but not _too far_ in the future.
        assert expires > now
        assert expires < after_expiration

        notification_url = urllib.parse.unquote_plus(params.get("notification_url")[0])
        assert (
            "http://odl_notify?library_short_name=%s&loan_id=%s"
            % (library.short_name, loan.id)
            == notification_url
        )

        # With an existing loan.
        loan, _ = license.loan_to(patron)
        loan.external_identifier = self._str

        api.queue_response(200, content=json.dumps(dict(status="active")))
        api.get_license_status_document(loan)
        requested_url = api.requests[1][0]
        assert loan.external_identifier == requested_url

    def test_get_license_status_document_errors(self, license, api, patron):
        loan, _ = license.loan_to(patron)

        api.queue_response(200, content="not json")
        pytest.raises(
            BadResponseException,
            api.get_license_status_document,
            loan,
        )

        api.queue_response(200, content=json.dumps(dict(status="unknown")))
        pytest.raises(
            BadResponseException,
            api.get_license_status_document,
            loan,
        )

    def test_checkin_success(self, license, patron, api, pool, db, checkin):
        # A patron has a copy of this book checked out.
        license.setup(concurrency=7, available=6)

        loan, _ = license.loan_to(patron)
        loan.external_identifier = "http://loan/" + self._str
        loan.end = utc_now() + datetime.timedelta(days=3)

        # The patron returns the book successfully.
        checkin()
        assert 3 == len(api.requests)
        assert "http://loan" in api.requests[0][0]
        assert "http://return" == api.requests[1][0]
        assert "http://loan" in api.requests[2][0]

        # The pool's availability has increased, and the local loan has
        # been deleted.
        assert 7 == pool.licenses_available
        assert 0 == db.query(Loan).count()

        # The license on the pool has also been updated
        assert 7 == license.checkouts_available

    def test_checkin_success_with_holds_queue(
        self, license, patron, checkin, pool, api, db
    ):
        # A patron has the only copy of this book checked out.
        license.setup(concurrency=1, available=0)
        loan, _ = license.loan_to(patron)
        loan.external_identifier = "http://loan/" + self._str
        loan.end = utc_now() + datetime.timedelta(days=3)

        # Another patron has the book on hold.
        patron_with_hold = self._patron()
        pool.patrons_in_hold_queue = 1
        hold, ignore = pool.on_hold_to(
            patron_with_hold, start=utc_now(), end=None, position=1
        )

        # The first patron returns the book successfully.
        checkin()
        assert 3 == len(api.requests)
        assert "http://loan" in api.requests[0][0]
        assert "http://return" == api.requests[1][0]
        assert "http://loan" in api.requests[2][0]

        # Now the license is reserved for the next patron.
        assert 0 == pool.licenses_available
        assert 1 == pool.licenses_reserved
        assert 1 == pool.patrons_in_hold_queue
        assert 0 == db.query(Loan).count()
        assert 0 == hold.position

    def test_checkin_already_fulfilled(self, license, patron, api, pool, db):
        # The loan is already fulfilled.
        license.setup(concurrency=7, available=6)
        loan, _ = license.loan_to(patron)
        loan.external_identifier = self._str
        loan.end = utc_now() + datetime.timedelta(days=3)

        lsd = json.dumps(
            {
                "status": "active",
            }
        )

        api.queue_response(200, content=lsd)
        # Checking in the book silently does nothing.
        api.checkin(patron, "pinn", pool)
        assert 1 == len(api.requests)
        assert 6 == pool.licenses_available
        assert 1 == db.query(Loan).count()

    def test_checkin_not_checked_out(self, api, patron, pool, license):
        # Not checked out locally.
        pytest.raises(
            NotCheckedOut,
            api.checkin,
            patron,
            "pin",
            pool,
        )

        # Not checked out according to the distributor.
        loan, _ = license.loan_to(patron)
        loan.external_identifier = self._str
        loan.end = utc_now() + datetime.timedelta(days=3)

        lsd = json.dumps(
            {
                "status": "revoked",
            }
        )

        api.queue_response(200, content=lsd)
        pytest.raises(
            NotCheckedOut,
            api.checkin,
            patron,
            "pin",
            pool,
        )

    def test_checkin_cannot_return(self, license, patron, pool, api):
        # Not fulfilled yet, but no return link from the distributor.
        loan, ignore = license.loan_to(patron)
        loan.external_identifier = self._str
        loan.end = utc_now() + datetime.timedelta(days=3)

        lsd = json.dumps(
            {
                "status": "ready",
            }
        )

        api.queue_response(200, content=lsd)
        # Checking in silently does nothing.
        api.checkin(patron, "pin", pool)

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

        api.queue_response(200, content=lsd)
        api.queue_response(200, content="Deleted")
        api.queue_response(200, content=lsd)
        api.checkin(patron, "pin", pool)

    def test_checkout_success(self, license, checkout, collection, db, pool):
        # This book is available to check out.
        license.setup(concurrency=6, available=6, left=30)

        # A patron checks out the book successfully.
        loan_url = self._str
        loan, _ = checkout(loan_url=loan_url)

        assert collection == loan.collection(db)
        assert pool.data_source.name == loan.data_source_name
        assert pool.identifier.type == loan.identifier_type
        assert pool.identifier.identifier == loan.identifier
        assert loan.start_date > utc_now() - datetime.timedelta(minutes=1)
        assert loan.start_date < utc_now() + datetime.timedelta(minutes=1)
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end_date
        assert loan_url == loan.external_identifier
        assert 1 == db.query(Loan).count()

        # Now the patron has a loan in the database that matches the LoanInfo
        # returned by the API.
        db_loan = db.query(Loan).one()
        assert pool == db_loan.license_pool
        assert license == db_loan.license
        assert loan.start_date == db_loan.start
        assert loan.end_date == db_loan.end

        # The pool's availability and the license's remaining checkouts have decreased.
        assert 5 == pool.licenses_available
        assert 29 == license.checkouts_left

    def test_checkout_success_with_hold(
        self, license, pool, checkout, patron, collection, db
    ):
        # A patron has this book on hold, and the book just became available to check out.
        pool.on_hold_to(
            patron, start=utc_now() - datetime.timedelta(days=1), position=0
        )
        license.setup(concurrency=1, available=1, left=5)

        assert pool.licenses_available == 0
        assert pool.licenses_reserved == 1
        assert pool.patrons_in_hold_queue == 1

        # The patron checks out the book.
        loan_url = self._str
        loan, _ = checkout(loan_url=loan_url)

        # The patron gets a loan successfully.
        assert collection == loan.collection(db)
        assert pool.data_source.name == loan.data_source_name
        assert pool.identifier.type == loan.identifier_type
        assert pool.identifier.identifier == loan.identifier
        assert loan.start_date > utc_now() - datetime.timedelta(minutes=1)
        assert loan.start_date < utc_now() + datetime.timedelta(minutes=1)
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end_date
        assert loan_url == loan.external_identifier
        assert 1 == db.query(Loan).count()

        db_loan = db.query(Loan).one()
        assert pool == db_loan.license_pool
        assert license == db_loan.license
        assert 4 == license.checkouts_left

        # The book is no longer reserved for the patron, and the hold has been deleted.
        assert 0 == pool.licenses_reserved
        assert 0 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue
        assert 0 == db.query(Hold).count()

    def test_checkout_already_checked_out(self, license, checkout, db):
        license.setup(concurrency=2, available=1)

        # Checkout succeeds the first time
        checkout()

        # But raises an exception the second time
        pytest.raises(AlreadyCheckedOut, checkout)

        assert 1 == db.query(Loan).count()

    def test_checkout_expired_hold(self, pool, patron, api, license):
        # The patron was at the beginning of the hold queue, but the hold already expired.
        yesterday = utc_now() - datetime.timedelta(days=1)
        hold, _ = pool.on_hold_to(patron, start=yesterday, end=yesterday, position=0)
        other_hold, _ = pool.on_hold_to(self._patron(), start=utc_now())
        license.setup(concurrency=2, available=1)

        pytest.raises(
            NoAvailableCopies,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

    def test_checkout_no_available_copies(self, pool, license, api, patron, db):
        # A different patron has the only copy checked out.
        license.setup(concurrency=1, available=0)
        existing_loan, _ = license.loan_to(self._patron())

        pytest.raises(
            NoAvailableCopies,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 1 == db.query(Loan).count()

        db.delete(existing_loan)

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(weeks=1)

        # A different patron has the only copy reserved.
        other_patron_hold, _ = pool.on_hold_to(
            self._patron(), position=0, start=last_week
        )
        pool.update_availability_from_licenses()

        pytest.raises(
            NoAvailableCopies,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.query(Loan).count()

        # The patron has a hold, but another patron is ahead in the holds queue.
        hold, _ = pool.on_hold_to(self._patron(), position=1, start=yesterday)
        pool.update_availability_from_licenses()

        pytest.raises(
            NoAvailableCopies,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.query(Loan).count()

        # The patron has the first hold, but it's expired.
        hold.start = last_week - datetime.timedelta(days=1)
        hold.end = yesterday
        pool.update_availability_from_licenses()

        pytest.raises(
            NoAvailableCopies,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.query(Loan).count()

    def test_checkout_no_licenses(self, license, api, pool, patron, db):
        license.setup(concurrency=1, available=1, left=0)

        pytest.raises(
            NoLicenses,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.query(Loan).count()

    def test_checkout_when_all_licenses_expired(self, license, api, patron, pool):
        # license expired by expiration date
        license.setup(
            concurrency=1,
            available=2,
            left=1,
            expires=utc_now() - datetime.timedelta(weeks=1),
        )

        pytest.raises(
            NoLicenses,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        # license expired by no remaining checkouts
        license.setup(
            concurrency=1,
            available=2,
            left=0,
            expires=utc_now() + datetime.timedelta(weeks=1),
        )

        pytest.raises(
            NoLicenses,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

    def test_checkout_cannot_loan(self, api, patron, pool, db):
        lsd = json.dumps(
            {
                "status": "revoked",
            }
        )

        api.queue_response(200, content=lsd)
        pytest.raises(
            CannotLoan,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.query(Loan).count()

        # No external identifier.
        lsd = json.dumps(
            {
                "status": "ready",
                "potential_rights": {"end": "2017-10-21T11:12:13Z"},
            }
        )

        api.queue_response(200, content=lsd)
        pytest.raises(
            CannotLoan,
            api.checkout,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        assert 0 == db.query(Loan).count()

    @pytest.mark.parametrize(
        "delivery_mechanism, correct_link, links",
        [
            (
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
        license,
        patron,
        api,
        checkout,
        pool,
        collection,
        db,
        delivery_mechanism,
        correct_link,
        links,
    ):
        # Fulfill a loan in a way that gives access to a license file.
        license.setup(concurrency=1, available=1)
        checkout()

        lsd = json.dumps(
            {
                "status": "ready",
                "potential_rights": {"end": "2017-10-21T11:12:13Z"},
                "links": links,
            }
        )

        api.queue_response(200, content=lsd)
        fulfillment = api.fulfill(patron, "pin", pool, delivery_mechanism)

        assert collection == fulfillment.collection(db)
        assert pool.data_source.name == fulfillment.data_source_name
        assert pool.identifier.type == fulfillment.identifier_type
        assert pool.identifier.identifier == fulfillment.identifier
        assert datetime_utc(2017, 10, 21, 11, 12, 13) == fulfillment.content_expires
        assert correct_link == fulfillment.content_link
        assert delivery_mechanism == fulfillment.content_type

    def test_fulfill_cannot_fulfill(self, license, checkout, db, api, patron, pool):
        license.setup(concurrency=7, available=7)
        checkout()

        assert 1 == db.query(Loan).count()
        assert 6 == pool.licenses_available

        lsd = json.dumps(
            {
                "status": "revoked",
            }
        )

        api.queue_response(200, content=lsd)
        pytest.raises(
            CannotFulfill,
            api.fulfill,
            patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

        # The pool's availability has been updated and the local
        # loan has been deleted, since we found out the loan is
        # no longer active.
        assert 7 == pool.licenses_available
        assert 0 == db.query(Loan).count()

    def test_count_holds_before(self, api, pool, patron):
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(weeks=1)

        hold, ignore = pool.on_hold_to(patron, start=now)

        assert 0 == api._count_holds_before(hold)

        # A previous hold.
        pool.on_hold_to(self._patron(), start=yesterday)
        assert 1 == api._count_holds_before(hold)

        # Expired holds don't count.
        pool.on_hold_to(self._patron(), start=last_week, end=yesterday, position=0)
        assert 1 == api._count_holds_before(hold)

        # Later holds don't count.
        pool.on_hold_to(self._patron(), start=tomorrow)
        assert 1 == api._count_holds_before(hold)

        # Holds on another pool don't count.
        other_pool = self._licensepool(None)
        other_pool.on_hold_to(patron, start=yesterday)
        assert 1 == api._count_holds_before(hold)

        for i in range(3):
            pool.on_hold_to(self._patron(), start=yesterday, end=tomorrow, position=1)
        assert 4 == api._count_holds_before(hold)

    def test_update_hold_end_date(self, pool, api, patron, license, db, collection):
        now = utc_now()
        tomorrow = now + datetime.timedelta(days=1)
        yesterday = now - datetime.timedelta(days=1)
        next_week = now + datetime.timedelta(days=7)
        last_week = now - datetime.timedelta(days=7)

        pool.licenses_owned = 1
        pool.licenses_reserved = 1

        hold, ignore = pool.on_hold_to(patron, start=now, position=0)

        # Set the reservation period and loan period.
        collection.external_integration.set_setting(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY, 3
        )
        collection.external_integration.set_setting(
            Collection.EBOOK_LOAN_DURATION_KEY, 6
        )

        # A hold that's already reserved and has an end date doesn't change.
        hold.end = tomorrow
        api._update_hold_end_date(hold)
        assert tomorrow == hold.end
        hold.end = yesterday
        api._update_hold_end_date(hold)
        assert yesterday == hold.end

        # Updating a hold that's reserved but doesn't have an end date starts the
        # reservation period.
        hold.end = None
        api._update_hold_end_date(hold)
        assert hold.end < next_week
        assert hold.end > now

        # Updating a hold that has an end date but just became reserved starts
        # the reservation period.
        hold.end = yesterday
        hold.position = 1
        api._update_hold_end_date(hold)
        assert hold.end < next_week
        assert hold.end > now

        # When there's a holds queue, the end date is the maximum time it could take for
        # a license to become available.

        # One copy, one loan, hold position 1.
        # The hold will be available as soon as the loan expires.
        pool.licenses_available = 0
        pool.licenses_reserved = 0
        pool.licenses_owned = 1
        loan, ignore = license.loan_to(self._patron(), end=tomorrow)
        api._update_hold_end_date(hold)
        assert tomorrow == hold.end

        # One copy, one loan, hold position 2.
        # The hold will be available after the loan expires + 1 cycle.
        first_hold, ignore = pool.on_hold_to(self._patron(), start=last_week)
        api._update_hold_end_date(hold)
        assert tomorrow + datetime.timedelta(days=9) == hold.end

        # Two copies, one loan, one reserved hold, hold position 2.
        # The hold will be available after the loan expires.
        pool.licenses_reserved = 1
        pool.licenses_owned = 2
        license.checkouts_available = 2
        api._update_hold_end_date(hold)
        assert tomorrow == hold.end

        # Two copies, one loan, one reserved hold, hold position 3.
        # The hold will be available after the reserved hold is checked out
        # at the latest possible time and that loan expires.
        second_hold, ignore = pool.on_hold_to(self._patron(), start=yesterday)
        first_hold.end = next_week
        api._update_hold_end_date(hold)
        assert next_week + datetime.timedelta(days=6) == hold.end

        # One copy, no loans, one reserved hold, hold position 3.
        # The hold will be available after the reserved hold is checked out
        # at the latest possible time and that loan expires + 1 cycle.
        db.delete(loan)
        pool.licenses_owned = 1
        api._update_hold_end_date(hold)
        assert next_week + datetime.timedelta(days=15) == hold.end

        # One copy, no loans, one reserved hold, hold position 2.
        # The hold will be available after the reserved hold is checked out
        # at the latest possible time and that loan expires.
        db.delete(second_hold)
        pool.licenses_owned = 1
        api._update_hold_end_date(hold)
        assert next_week + datetime.timedelta(days=6) == hold.end

        db.delete(first_hold)

        # Ten copies, seven loans, three reserved holds, hold position 9.
        # The hold will be available after the sixth loan expires.
        pool.licenses_owned = 10
        for i in range(5):
            pool.loan_to(self._patron(), end=next_week)
        pool.loan_to(self._patron(), end=next_week + datetime.timedelta(days=1))
        pool.loan_to(self._patron(), end=next_week + datetime.timedelta(days=2))
        pool.licenses_reserved = 3
        for i in range(3):
            pool.on_hold_to(
                self._patron(),
                start=last_week + datetime.timedelta(days=i),
                end=next_week + datetime.timedelta(days=i),
                position=0,
            )
        for i in range(5):
            pool.on_hold_to(self._patron(), start=yesterday)
        api._update_hold_end_date(hold)
        assert next_week + datetime.timedelta(days=1) == hold.end

        # Ten copies, seven loans, three reserved holds, hold position 12.
        # The hold will be available after the second reserved hold is checked
        # out and that loan expires.
        for i in range(3):
            pool.on_hold_to(self._patron(), start=yesterday)
        api._update_hold_end_date(hold)
        assert next_week + datetime.timedelta(days=7) == hold.end

        # Ten copies, seven loans, three reserved holds, hold position 29.
        # The hold will be available after the sixth loan expires + 2 cycles.
        for i in range(17):
            pool.on_hold_to(self._patron(), start=yesterday)
        api._update_hold_end_date(hold)
        assert next_week + datetime.timedelta(days=19) == hold.end

        # Ten copies, seven loans, three reserved holds, hold position 32.
        # The hold will be available after the second reserved hold is checked
        # out and that loan expires + 2 cycles.
        for i in range(3):
            pool.on_hold_to(self._patron(), start=yesterday)
        api._update_hold_end_date(hold)
        assert next_week + datetime.timedelta(days=25) == hold.end

    def test_update_hold_position(self, pool, patron, license, api, db):
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        hold, ignore = pool.on_hold_to(patron, start=now)

        pool.licenses_owned = 1

        # When there are no other holds and no licenses reserved, hold position is 1.
        loan, _ = license.loan_to(self._patron())
        api._update_hold_position(hold)
        assert 1 == hold.position

        # When a license is reserved, position is 0.
        db.delete(loan)
        api._update_hold_position(hold)
        assert 0 == hold.position

        # If another hold has the reserved licenses, position is 2.
        pool.on_hold_to(self._patron(), start=yesterday)
        api._update_hold_position(hold)
        assert 2 == hold.position

        # If another license is reserved, position goes back to 0.
        pool.licenses_owned = 2
        license.checkouts_available = 2
        api._update_hold_position(hold)
        assert 0 == hold.position

        # If there's an earlier hold but it expired, it doesn't
        # affect the position.
        pool.on_hold_to(self._patron(), start=yesterday, end=yesterday, position=0)
        api._update_hold_position(hold)
        assert 0 == hold.position

        # Hold position is after all earlier non-expired holds...
        for i in range(3):
            pool.on_hold_to(self._patron(), start=yesterday)
        api._update_hold_position(hold)
        assert 5 == hold.position

        # and before any later holds.
        for i in range(2):
            pool.on_hold_to(self._patron(), start=tomorrow)
        api._update_hold_position(hold)
        assert 5 == hold.position

    def test_update_hold_queue(
        self, license, collection, pool, work, api, db, checkout, checkin, patron
    ):
        licenses = [license]

        collection.external_integration.set_setting(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY, 3
        )

        # If there's no holds queue when we try to update the queue, it
        # will remove a reserved license and make it available instead.
        pool.licenses_owned = 1
        pool.licenses_available = 0
        pool.licenses_reserved = 1
        pool.patrons_in_hold_queue = 0
        last_update = utc_now() - datetime.timedelta(minutes=5)
        work.last_update_time = last_update
        api.update_licensepool(pool)
        assert 1 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue
        # The work's last update time is changed so it will be moved up in the crawlable OPDS feed.
        assert work.last_update_time > last_update

        # If there are holds, a license will get reserved for the next hold
        # and its end date will be set.
        hold, _ = pool.on_hold_to(patron, start=utc_now(), position=1)
        later_hold, _ = pool.on_hold_to(
            self._patron(), start=utc_now() + datetime.timedelta(days=1), position=2
        )
        api.update_licensepool(pool)

        # The pool's licenses were updated.
        assert 0 == pool.licenses_available
        assert 1 == pool.licenses_reserved
        assert 2 == pool.patrons_in_hold_queue

        # And the first hold changed.
        assert 0 == hold.position
        assert hold.end - utc_now() - datetime.timedelta(days=3) < datetime.timedelta(
            hours=1
        )

        # The later hold is the same.
        assert 2 == later_hold.position

        # Now there's a reserved hold. If we add another license, it's reserved and,
        # the later hold is also updated.
        l = self._license(pool, terms_concurrency=1, checkouts_available=1)
        licenses.append(l)
        api.update_licensepool(pool)

        assert 0 == pool.licenses_available
        assert 2 == pool.licenses_reserved
        assert 2 == pool.patrons_in_hold_queue
        assert 0 == later_hold.position
        assert later_hold.end - utc_now() - datetime.timedelta(
            days=3
        ) < datetime.timedelta(hours=1)

        # Now there are no more holds. If we add another license,
        # it ends up being available.
        l = self._license(pool, terms_concurrency=1, checkouts_available=1)
        licenses.append(l)
        api.update_licensepool(pool)
        assert 1 == pool.licenses_available
        assert 2 == pool.licenses_reserved
        assert 2 == pool.patrons_in_hold_queue

        # License pool is updated when the holds are removed.
        db.delete(hold)
        db.delete(later_hold)
        api.update_licensepool(pool)
        assert 3 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

        # We can also make multiple licenses reserved at once.
        loans = []
        holds = []
        for i in range(3):
            p = self._patron()
            loan, _ = checkout(patron=p)
            loans.append((loan, p))
        assert 0 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

        l = self._license(pool, terms_concurrency=2, checkouts_available=2)
        licenses.append(l)
        for i in range(3):
            hold, ignore = pool.on_hold_to(
                self._patron(),
                start=utc_now() - datetime.timedelta(days=3 - i),
                position=i + 1,
            )
            holds.append(hold)

        api.update_licensepool(pool)
        assert 2 == pool.licenses_reserved
        assert 0 == pool.licenses_available
        assert 3 == pool.patrons_in_hold_queue
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
            checkin(patron=p)
        assert 3 == pool.licenses_reserved
        assert 1 == pool.licenses_available
        assert 3 == pool.patrons_in_hold_queue
        for hold in holds:
            assert 0 == hold.position
            assert hold.end - utc_now() - datetime.timedelta(
                days=3
            ) < datetime.timedelta(hours=1)

    def test_place_hold_success(self, pool, api, db, collection, patron, checkout):
        loan, _ = checkout(patron=self._patron())

        hold = api.place_hold(
            patron, "pin", pool, "notifications@librarysimplified.org"
        )

        assert 1 == pool.patrons_in_hold_queue
        assert collection == hold.collection(db)
        assert pool.data_source.name == hold.data_source_name
        assert pool.identifier.type == hold.identifier_type
        assert pool.identifier.identifier == hold.identifier
        assert hold.start_date > utc_now() - datetime.timedelta(minutes=1)
        assert hold.start_date < utc_now() + datetime.timedelta(minutes=1)
        assert loan.end_date == hold.end_date
        assert 1 == hold.hold_position
        assert 1 == db.query(Hold).count()

    def test_place_hold_already_on_hold(self, pool, patron, license, api):
        license.setup(concurrency=1, available=0)
        pool.on_hold_to(patron)
        pytest.raises(
            AlreadyOnHold,
            api.place_hold,
            patron,
            "pin",
            pool,
            "notifications@librarysimplified.org",
        )

    def test_place_hold_currently_available(self, pool, api, patron):
        pytest.raises(
            CurrentlyAvailable,
            api.place_hold,
            patron,
            "pin",
            pool,
            "notifications@librarysimplified.org",
        )

    def test_release_hold_success(self, checkout, pool, patron, api, db, checkin):
        loan_patron = self._patron()
        checkout(patron=loan_patron)
        pool.on_hold_to(patron, position=1)

        assert True == api.release_hold(patron, "pin", pool)
        assert 0 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue
        assert 0 == db.query(Hold).count()

        pool.on_hold_to(patron, position=0)
        checkin(patron=loan_patron)

        assert True == api.release_hold(patron, "pin", pool)
        assert 1 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue
        assert 0 == db.query(Hold).count()

        pool.on_hold_to(patron, position=0)
        other_hold, ignore = pool.on_hold_to(self._patron(), position=2)

        assert True == api.release_hold(patron, "pin", pool)
        assert 0 == pool.licenses_available
        assert 1 == pool.licenses_reserved
        assert 1 == pool.patrons_in_hold_queue
        assert 1 == db.query(Hold).count()
        assert 0 == other_hold.position

    def test_release_hold_not_on_hold(self, api, patron, pool):
        pytest.raises(
            NotOnHold,
            api.release_hold,
            patron,
            "pin",
            pool,
        )

    def test_patron_activity_loan(
        self, api, patron, license, db, pool, collection, checkout, checkin
    ):
        # No loans yet.
        assert [] == api.patron_activity(patron, "pin")

        # One loan.
        _, loan = checkout()

        activity = api.patron_activity(patron, "pin")
        assert 1 == len(activity)
        assert collection == activity[0].collection(db)
        assert pool.data_source.name == activity[0].data_source_name
        assert pool.identifier.type == activity[0].identifier_type
        assert pool.identifier.identifier == activity[0].identifier
        assert loan.start == activity[0].start_date
        assert loan.end == activity[0].end_date
        assert loan.external_identifier == activity[0].external_identifier

        # Two loans.
        pool2 = self._licensepool(None, collection=collection)
        license2 = self._license(pool2, terms_concurrency=1, checkouts_available=1)
        _, loan2 = checkout(pool=pool2)

        activity = api.patron_activity(patron, "pin")
        assert 2 == len(activity)
        [l1, l2] = sorted(activity, key=lambda x: x.start_date)

        assert collection == l1.collection(db)
        assert pool.data_source.name == l1.data_source_name
        assert pool.identifier.type == l1.identifier_type
        assert pool.identifier.identifier == l1.identifier
        assert loan.start == l1.start_date
        assert loan.end == l1.end_date
        assert loan.external_identifier == l1.external_identifier

        assert collection == l2.collection(db)
        assert pool2.data_source.name == l2.data_source_name
        assert pool2.identifier.type == l2.identifier_type
        assert pool2.identifier.identifier == l2.identifier
        assert loan2.start == l2.start_date
        assert loan2.end == l2.end_date
        assert loan2.external_identifier == l2.external_identifier

        # If a loan is expired already, it's left out.
        loan2.end = utc_now() - datetime.timedelta(days=2)
        activity = api.patron_activity(patron, "pin")
        assert 1 == len(activity)
        assert pool.identifier.identifier == activity[0].identifier
        checkin(pool=pool2)

        # One hold.
        other_patron = self._patron()
        checkout(patron=other_patron, pool=pool2)
        hold, _ = pool2.on_hold_to(patron)
        hold.start = utc_now() - datetime.timedelta(days=2)
        hold.end = hold.start + datetime.timedelta(days=3)
        hold.position = 3
        activity = api.patron_activity(patron, "pin")
        assert 2 == len(activity)
        [h1, l1] = sorted(activity, key=lambda x: x.start_date)

        assert collection == h1.collection(db)
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
        checkin(patron=other_patron, pool=pool2)
        hold.end = utc_now() - datetime.timedelta(days=1)
        hold.position = 0
        activity = api.patron_activity(patron, "pin")
        assert 1 == len(activity)
        assert 0 == db.query(Hold).count()
        assert 1 == pool2.licenses_available
        assert 0 == pool2.licenses_reserved

    def test_update_loan_still_active(self, license, patron, api, pool, db):
        license.setup(concurrency=6, available=6)
        loan, _ = license.loan_to(patron)
        loan.external_identifier = self._str
        status_doc = {
            "status": "active",
        }

        api.update_loan(loan, status_doc)
        # Availability hasn't changed, and the loan still exists.
        assert 6 == pool.licenses_available
        assert 1 == db.query(Loan).count()

    def test_update_loan_removes_loan(self, checkout, license, patron, api, pool, db):
        license.setup(concurrency=7, available=7)
        _, loan = checkout()

        assert 6 == pool.licenses_available
        assert 1 == db.query(Loan).count()

        status_doc = {
            "status": "cancelled",
        }

        api.update_loan(loan, status_doc)

        # Availability has increased, and the loan is gone.
        assert 7 == pool.licenses_available
        assert 0 == db.query(Loan).count()

    def test_update_loan_removes_loan_with_hold_queue(
        self, checkout, pool, license, api, db
    ):
        _, loan = checkout()
        hold, _ = pool.on_hold_to(self._patron(), position=1)
        pool.update_availability_from_licenses()

        assert pool.licenses_owned == 1
        assert pool.licenses_available == 0
        assert pool.licenses_reserved == 0
        assert pool.patrons_in_hold_queue == 1

        status_doc = {
            "status": "cancelled",
        }

        api.update_loan(loan, status_doc)

        # The license is reserved for the next patron, and the loan is gone.
        assert 0 == pool.licenses_available
        assert 1 == pool.licenses_reserved
        assert 0 == hold.position
        assert 0 == db.query(Loan).count()

    def test_checkout_from_external_library(self, pool, license, api, client, db):
        # This book is available to check out.
        pool.licenses_owned = 6
        pool.licenses_available = 6
        license.checkouts_available = 6
        license.checkouts_left = 10

        # An integration client checks out the book successfully.
        loan_url = self._str
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

        api.queue_response(200, content=lsd)
        loan = api.checkout_to_external_library(client, pool)
        assert client == loan.integration_client
        assert pool == loan.license_pool
        assert loan.start > utc_now() - datetime.timedelta(minutes=1)
        assert loan.start < utc_now() + datetime.timedelta(minutes=1)
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end
        assert loan_url == loan.external_identifier
        assert 1 == db.query(Loan).count()

        # The pool's availability and the license's remaining checkouts have decreased.
        assert 5 == pool.licenses_available
        assert 9 == license.checkouts_left

        # The book can also be placed on hold to an external library,
        # if there are no copies available.
        license.setup(concurrency=1, available=0)

        hold = api.checkout_to_external_library(client, pool)

        assert 1 == pool.patrons_in_hold_queue
        assert client == hold.integration_client
        assert pool == hold.license_pool
        assert hold.start > utc_now() - datetime.timedelta(minutes=1)
        assert hold.start < utc_now() + datetime.timedelta(minutes=1)
        assert hold.end > utc_now() + datetime.timedelta(days=7)
        assert 1 == hold.position
        assert 1 == db.query(Hold).count()

    def test_checkout_from_external_library_with_hold(self, pool, client, api, db):
        # An integration client has this book on hold, and the book just became available to check out.
        pool.licenses_owned = 1
        pool.licenses_available = 0
        pool.licenses_reserved = 1
        pool.patrons_in_hold_queue = 1
        hold, ignore = pool.on_hold_to(
            client, start=utc_now() - datetime.timedelta(days=1), position=0
        )

        # The patron checks out the book.
        loan_url = self._str
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

        api.queue_response(200, content=lsd)

        # The patron gets a loan successfully.
        loan = api.checkout_to_external_library(client, pool, hold)
        assert client == loan.integration_client
        assert pool == loan.license_pool
        assert loan.start > utc_now() - datetime.timedelta(minutes=1)
        assert loan.start < utc_now() + datetime.timedelta(minutes=1)
        assert datetime_utc(3017, 10, 21, 11, 12, 13) == loan.end
        assert loan_url == loan.external_identifier
        assert 1 == db.query(Loan).count()

        # The book is no longer reserved for the patron, and the hold has been deleted.
        assert 0 == pool.licenses_reserved
        assert 0 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue
        assert 0 == db.query(Hold).count()

    def test_checkin_from_external_library(self, pool, license, api, client, db):
        # An integration client has a copy of this book checked out.
        license.setup(concurrency=7, available=6)
        loan, ignore = license.loan_to(client)
        loan.external_identifier = "http://loan/" + self._str
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

        api.queue_response(200, content=lsd)
        api.queue_response(200)
        api.queue_response(200, content=returned_lsd)
        api.checkin_from_external_library(client, loan)
        assert 3 == len(api.requests)
        assert "http://loan" in api.requests[0][0]
        assert "http://return" == api.requests[1][0]
        assert "http://loan" in api.requests[2][0]

        # The pool's availability has increased, and the local loan has
        # been deleted.
        assert 7 == pool.licenses_available
        assert 0 == db.query(Loan).count()

    def test_fulfill_for_external_library(
        self, license, client, api, collection, pool, db
    ):
        loan, ignore = license.loan_to(client)
        loan.external_identifier = self._str
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

        api.queue_response(200, content=lsd)
        fulfillment = api.fulfill_for_external_library(client, loan, None)
        assert collection == fulfillment.collection(db)
        assert pool.data_source.name == fulfillment.data_source_name
        assert pool.identifier.type == fulfillment.identifier_type
        assert pool.identifier.identifier == fulfillment.identifier
        assert datetime_utc(2017, 10, 21, 11, 12, 13) == fulfillment.content_expires
        assert "http://acsm" == fulfillment.content_link
        assert DeliveryMechanism.ADOBE_DRM == fulfillment.content_type

    def test_release_hold_from_external_library(
        self, pool, license, db, api, client, checkout, checkin
    ):
        license.setup(concurrency=1, available=1)
        other_patron = self._patron()
        checkout(patron=other_patron)
        hold, ignore = pool.on_hold_to(client, position=1)

        assert api.release_hold_from_external_library(client, hold) is True
        assert 0 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue
        assert 0 == db.query(Hold).count()

        checkin(patron=other_patron)
        hold, ignore = pool.on_hold_to(client, position=0)

        assert api.release_hold_from_external_library(client, hold) is True
        assert 1 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue
        assert 0 == db.query(Hold).count()

        hold, ignore = pool.on_hold_to(client, position=0)
        other_hold, ignore = pool.on_hold_to(self._patron(), position=2)

        assert api.release_hold_from_external_library(client, hold) is True
        assert 0 == pool.licenses_available
        assert 1 == pool.licenses_reserved
        assert 1 == pool.patrons_in_hold_queue
        assert 1 == db.query(Hold).count()
        assert 0 == other_hold.position


class TestODLImporter(DatabaseTest, BaseODLTest):
    class MockGet:
        def __init__(self):
            self.responses = []

        def get(self, *args, **kwargs):
            return 200, {}, str(self.responses.pop(0))

        def add(self, item):
            return self.responses.append(item)

    class MockMetadataClient(object):
        def canonicalize_author_name(self, identifier, working_display_name):
            return working_display_name

    @pytest.fixture()
    def mock_get(self) -> MockGet:
        return self.MockGet()

    @pytest.fixture()
    def importer(self, collection, db, mock_get, metadata_client) -> ODLImporter:
        return ODLImporter(
            db,
            collection=collection,
            http_get=mock_get.get,
            metadata_client=metadata_client,
        )

    @pytest.fixture()
    def datasource(self, db, collection) -> DataSource:
        data_source = DataSource.lookup(db, "Feedbooks", autocreate=True)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, data_source.name
        )
        return data_source

    @pytest.fixture()
    def metadata_client(self) -> MockMetadataClient:
        return self.MockMetadataClient()

    @pytest.fixture()
    def feed_template(self):
        return "feed_template.xml.jinja"

    @pytest.fixture()
    def import_templated(self, mock_get, importer, feed_template) -> Callable:
        def i(licenses: List[LicenseInfoHelper]) -> Tuple[List, List, List, List]:
            feed_licenses = [l.license for l in licenses]
            [mock_get.add(l) for l in licenses]
            feed = self.get_templated_feed(feed_template, feed_licenses)
            return importer.import_from_feed(feed)

        return i

    def get_templated_feed(self, filename: str, licenses: List[LicenseHelper]) -> str:
        """Get the test ODL feed with specific licensing information.

        :param filename: Name of template to load
        :param licenses: List of ODL licenses

        :return: Test ODL feed
        """
        template = Template(self.get_data(filename))
        feed = template.render(licenses=licenses)
        return feed

    @freeze_time("2019-01-01T00:00:00+00:00")
    def test_import(self, importer, mock_get):
        """Ensure that ODLImporter correctly processes and imports the ODL feed encoded using OPDS 1.x.

        NOTE: `freeze_time` decorator is required to treat the licenses in the ODL feed as non-expired.
        """
        feed = self.get_data("feedbooks_bibliographic.atom")

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
        assert set(
            [Representation.EPUB_MEDIA_TYPE, Representation.PDF_MEDIA_TYPE]
        ) == set([lpdm.delivery_mechanism.content_type for lpdm in lpdms])
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
        assert sum([not l.is_inactive for l in imported_pool.licenses]) == len(active)
        assert sum([l.is_inactive for l in imported_pool.licenses]) == len(inactive)

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
            assert sum([not l.is_inactive for l in imported_pool.licenses]) == len(
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
            assert sum([not l.is_inactive for l in imported_pool.licenses]) == 1

            # Two licenses expired
            assert sum([l.is_inactive for l in imported_pool.licenses]) == 2


class TestODLHoldReaper(DatabaseTest, BaseODLAPITest):
    def test_run_once(self, collection, api, db, pool, license):
        data_source = DataSource.lookup(self._db, "Feedbooks", autocreate=True)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, data_source.name
        )
        reaper = ODLHoldReaper(db, collection, api=api)

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)

        license.setup(concurrency=3, available=3)
        expired_hold1, ignore = pool.on_hold_to(
            self._patron(), end=yesterday, position=0
        )
        expired_hold2, ignore = pool.on_hold_to(
            self._patron(), end=yesterday, position=0
        )
        expired_hold3, ignore = pool.on_hold_to(
            self._patron(), end=yesterday, position=0
        )
        current_hold, ignore = pool.on_hold_to(self._patron(), position=3)
        # This hold has an end date in the past, but its position is greater than 0
        # so the end date is not reliable.
        bad_end_date, ignore = pool.on_hold_to(
            self._patron(), end=yesterday, position=4
        )

        progress = reaper.run_once(reaper.timestamp().to_data())

        # The expired holds have been deleted and the other holds have been updated.
        assert 2 == db.query(Hold).count()
        assert [current_hold, bad_end_date] == db.query(Hold).order_by(Hold.start).all()
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


class TestSharedODLAPI(DatabaseTest, BaseODLTest):
    def setup_method(self):
        super(TestSharedODLAPI, self).setup_method()
        self.collection = MockSharedODLAPI.mock_collection(self._db)
        self.collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, "Feedbooks"
        )
        self.api = MockSharedODLAPI(self._db, self.collection)
        self.pool = self._licensepool(None, collection=self.collection)
        self.pool.identifier.add_link(
            Hyperlink.BORROW, self._str, self.collection.data_source
        )
        self.patron = self._patron()

    def test_get(self):
        # Create a SharedODLAPI to test the _get method. The other tests use a
        # mock API class that overrides _get.
        api = SharedODLAPI(self._db, self.collection)

        # The library has not registered with the remote collection yet.
        def do_get(url, headers=None, allowed_response_codes=None):
            raise Exception("do_get should not be called")

        pytest.raises(
            LibraryAuthorizationFailedException,
            api._get,
            "test url",
            patron=self.patron,
            do_get=do_get,
        )

        # Once the library registers, it gets a shared secret that is included
        # in request headers.
        ConfigurationSetting.for_library_and_externalintegration(
            self._db,
            ExternalIntegration.PASSWORD,
            self.patron.library,
            self.collection.external_integration,
        ).value = "secret"

        def do_get(url, headers=None, allowed_response_codes=None):
            assert "test url" == url
            assert "test header value" == headers.get("test_key")
            assert "Bearer " + base64.b64encode("secret") == headers.get(
                "Authorization"
            )
            assert ["200"] == allowed_response_codes

        api._get(
            "test url",
            headers=dict(test_key="test header value"),
            patron=self.patron,
            allowed_response_codes=["200"],
            do_get=do_get,
        )

    def test_checkout_success(self):
        response = self.get_data("shared_collection_borrow_success.opds")
        self.api.queue_response(200, content=response)

        loan = self.api.checkout(
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE
        )
        assert self.collection == loan.collection(self._db)
        assert self.pool.data_source.name == loan.data_source_name
        assert self.pool.identifier.type == loan.identifier_type
        assert self.pool.identifier.identifier == loan.identifier
        assert datetime_utc(2018, 3, 8, 17, 41, 31) == loan.start_date
        assert datetime_utc(2018, 3, 29, 17, 41, 30) == loan.end_date
        assert (
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/31"
            == loan.external_identifier
        )

        assert [self.pool.identifier.links[0].resource.url] == self.api.requests

    def test_checkout_from_hold(self):
        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        hold_info_response = self.get_data("shared_collection_hold_info_ready.opds")
        self.api.queue_response(200, content=hold_info_response)
        borrow_response = self.get_data("shared_collection_borrow_success.opds")
        self.api.queue_response(200, content=borrow_response)

        loan = self.api.checkout(
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE
        )
        assert self.collection == loan.collection(self._db)
        assert self.pool.data_source.name == loan.data_source_name
        assert self.pool.identifier.type == loan.identifier_type
        assert self.pool.identifier.identifier == loan.identifier
        assert datetime_utc(2018, 3, 8, 17, 41, 31) == loan.start_date
        assert datetime_utc(2018, 3, 29, 17, 41, 30) == loan.end_date
        assert (
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/31"
            == loan.external_identifier
        )

        assert [
            hold.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/17/borrow",
        ] == self.api.requests

    def test_checkout_already_checked_out(self):
        loan, ignore = self.pool.loan_to(self.patron)
        pytest.raises(
            AlreadyCheckedOut,
            self.api.checkout,
            self.patron,
            "pin",
            self.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [] == self.api.requests

    def test_checkout_no_available_copies(self):
        self.api.queue_response(403)
        pytest.raises(
            NoAvailableCopies,
            self.api.checkout,
            self.patron,
            "pin",
            self.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [self.pool.identifier.links[0].resource.url] == self.api.requests

    def test_checkout_no_licenses(self):
        self.api.queue_response(
            NO_LICENSES.response[1],
            headers=NO_LICENSES.response[2],
            content=NO_LICENSES.response[0],
        )
        pytest.raises(
            NoLicenses,
            self.api.checkout,
            self.patron,
            "pin",
            self.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [self.pool.identifier.links[0].resource.url] == self.api.requests

    def test_checkout_from_hold_not_available(self):
        hold, ignore = self.pool.on_hold_to(self.patron)
        hold_info_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_info_response)
        pytest.raises(
            NoAvailableCopies,
            self.api.checkout,
            self.patron,
            "pin",
            self.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [hold.external_identifier] == self.api.requests

    def test_checkout_cannot_loan(self):
        self.api.queue_response(500)
        pytest.raises(
            CannotLoan,
            self.api.checkout,
            self.patron,
            "pin",
            self.pool,
            Representation.EPUB_MEDIA_TYPE,
        )
        assert [self.pool.identifier.links[0].resource.url] == self.api.requests

        # This pool has no borrow link.
        pool = self._licensepool(None, collection=self.collection)
        pytest.raises(
            CannotLoan,
            self.api.checkout,
            self.patron,
            "pin",
            pool,
            Representation.EPUB_MEDIA_TYPE,
        )

    def test_checkin_success(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        loan_info_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_info_response)
        self.api.queue_response(200, content="Deleted")
        response = self.api.checkin(self.patron, "pin", self.pool)
        assert True == response
        assert [
            loan.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/revoke",
        ] == self.api.requests

    def test_checkin_not_checked_out(self):
        pytest.raises(NotCheckedOut, self.api.checkin, self.patron, "pin", self.pool)
        assert [] == self.api.requests

        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(404)
        pytest.raises(NotCheckedOut, self.api.checkin, self.patron, "pin", self.pool)
        assert [loan.external_identifier] == self.api.requests

    def test_checkin_cannot_return(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        pytest.raises(CannotReturn, self.api.checkin, self.patron, "pin", self.pool)
        assert [loan.external_identifier] == self.api.requests

        loan_info_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_info_response)
        self.api.queue_response(500)
        pytest.raises(CannotReturn, self.api.checkin, self.patron, "pin", self.pool)
        assert [
            loan.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/revoke",
        ] == self.api.requests[1:]

    def test_fulfill_success(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        loan_info_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_info_response)
        self.api.queue_response(200, content="An ACSM file")
        fulfillment = self.api.fulfill(
            self.patron, "pin", self.pool, self.pool.delivery_mechanisms[0]
        )
        assert self.collection == fulfillment.collection(self._db)
        assert self.pool.data_source.name == fulfillment.data_source_name
        assert self.pool.identifier.type == fulfillment.identifier_type
        assert self.pool.identifier.identifier == fulfillment.identifier
        assert None == fulfillment.content_link
        assert b"An ACSM file" == fulfillment.content
        assert datetime_utc(2018, 3, 29, 17, 44, 11) == fulfillment.content_expires

        assert [
            loan.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/fulfill/2",
        ] == self.api.requests

    def test_fulfill_not_checked_out(self):
        pytest.raises(
            NotCheckedOut,
            self.api.fulfill,
            self.patron,
            "pin",
            self.pool,
            self.pool.delivery_mechanisms[0],
        )
        assert [] == self.api.requests

        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(404)
        pytest.raises(
            NotCheckedOut,
            self.api.fulfill,
            self.patron,
            "pin",
            self.pool,
            self.pool.delivery_mechanisms[0],
        )
        assert [loan.external_identifier] == self.api.requests

    def test_fulfill_cannot_fulfill(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        pytest.raises(
            CannotFulfill,
            self.api.fulfill,
            self.patron,
            "pin",
            self.pool,
            self.pool.delivery_mechanisms[0],
        )
        assert [loan.external_identifier] == self.api.requests

        self.api.queue_response(200, content="not opds")
        pytest.raises(
            CannotFulfill,
            self.api.fulfill,
            self.patron,
            "pin",
            self.pool,
            self.pool.delivery_mechanisms[0],
        )
        assert [loan.external_identifier] == self.api.requests[1:]

        loan_info_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_info_response)
        self.api.queue_response(500)
        pytest.raises(
            CannotFulfill,
            self.api.fulfill,
            self.patron,
            "pin",
            self.pool,
            self.pool.delivery_mechanisms[0],
        )
        assert [
            loan.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/fulfill/2",
        ] == self.api.requests[2:]

    def test_fulfill_format_not_available(self):
        loan, ignore = self.pool.loan_to(self.patron)
        loan_info_response = self.get_data("shared_collection_loan_info_no_epub.opds")
        self.api.queue_response(200, content=loan_info_response)
        pytest.raises(
            FormatNotAvailable,
            self.api.fulfill,
            self.patron,
            "pin",
            self.pool,
            self.pool.delivery_mechanisms[0],
        )
        assert [loan.external_identifier] == self.api.requests

    def test_place_hold_success(self):
        hold_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_response)
        hold = self.api.place_hold(
            self.patron, "pin", self.pool, "notifications@librarysimplified.org"
        )
        assert self.collection == hold.collection(self._db)
        assert self.pool.data_source.name == hold.data_source_name
        assert self.pool.identifier.type == hold.identifier_type
        assert self.pool.identifier.identifier == hold.identifier
        assert datetime_utc(2018, 3, 8, 18, 50, 18) == hold.start_date
        assert datetime_utc(2018, 3, 29, 17, 44, 1) == hold.end_date
        assert 1 == hold.hold_position
        assert (
            "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/18"
            == hold.external_identifier
        )

        assert [self.pool.identifier.links[0].resource.url] == self.api.requests

    def test_place_hold_already_checked_out(self):
        loan, ignore = self.pool.loan_to(self.patron)
        pytest.raises(
            AlreadyCheckedOut,
            self.api.place_hold,
            self.patron,
            "pin",
            self.pool,
            "notification@librarysimplified.org",
        )
        assert [] == self.api.requests

    def test_release_hold_success(self):
        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        hold_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_response)
        self.api.queue_response(200, content="Deleted")
        response = self.api.release_hold(self.patron, "pin", self.pool)
        assert True == response
        assert [
            hold.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/18/revoke",
        ] == self.api.requests

    def test_release_hold_not_on_hold(self):
        pytest.raises(NotOnHold, self.api.release_hold, self.patron, "pin", self.pool)
        assert [] == self.api.requests

        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        self.api.queue_response(404)
        pytest.raises(NotOnHold, self.api.release_hold, self.patron, "pin", self.pool)
        assert [hold.external_identifier] == self.api.requests

    def test_release_hold_cannot_release_hold(self):
        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        pytest.raises(
            CannotReleaseHold, self.api.release_hold, self.patron, "pin", self.pool
        )
        assert [hold.external_identifier] == self.api.requests

        hold_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_response)
        self.api.queue_response(500)
        pytest.raises(
            CannotReleaseHold, self.api.release_hold, self.patron, "pin", self.pool
        )
        assert [
            hold.external_identifier,
            "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/18/revoke",
        ] == self.api.requests[1:]

    def test_patron_activity_success(self):
        # The patron has one loan, and the remote circ manager returns it.
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        loan_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_response)
        activity = self.api.patron_activity(self.patron, "pin")
        assert 1 == len(activity)
        [loan_info] = activity
        assert self.collection == loan_info.collection(self._db)
        assert self.pool.data_source.name == loan_info.data_source_name
        assert self.pool.identifier.type == loan_info.identifier_type
        assert self.pool.identifier.identifier == loan_info.identifier
        assert datetime_utc(2018, 3, 8, 17, 44, 12) == loan_info.start_date
        assert datetime_utc(2018, 3, 29, 17, 44, 11) == loan_info.end_date
        assert [loan.external_identifier] == self.api.requests

        # The _get method was passed a patron - this is necessary because
        # the patron_activity method may be called from a thread without
        # access to the flask request.
        assert self.patron == self.api.request_args[0][0]

        # The patron's loan has been deleted on the remote.
        self.api.queue_response(404, content="No loan here")
        activity = self.api.patron_activity(self.patron, "pin")
        assert 0 == len(activity)
        assert [loan.external_identifier] == self.api.requests[1:]

        # Now the patron has a hold instead.
        self._db.delete(loan)
        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        hold_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_response)
        activity = self.api.patron_activity(self.patron, "pin")
        assert 1 == len(activity)
        [hold_info] = activity
        assert self.collection == hold_info.collection(self._db)
        assert self.pool.data_source.name == hold_info.data_source_name
        assert self.pool.identifier.type == hold_info.identifier_type
        assert self.pool.identifier.identifier == hold_info.identifier
        assert datetime_utc(2018, 3, 8, 18, 50, 18) == hold_info.start_date
        assert datetime_utc(2018, 3, 29, 17, 44, 1) == hold_info.end_date
        assert [hold.external_identifier] == self.api.requests[2:]

        # The patron's hold has been deleted on the remote.
        self.api.queue_response(404, content="No hold here")
        activity = self.api.patron_activity(self.patron, "pin")
        assert 0 == len(activity)
        assert [hold.external_identifier] == self.api.requests[3:]

    def test_patron_activity_remote_integration_exception(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        pytest.raises(
            RemoteIntegrationException, self.api.patron_activity, self.patron, "pin"
        )
        assert [loan.external_identifier] == self.api.requests
        self._db.delete(loan)

        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        pytest.raises(
            RemoteIntegrationException, self.api.patron_activity, self.patron, "pin"
        )
        assert [hold.external_identifier] == self.api.requests[1:]


class TestSharedODLImporter(DatabaseTest, BaseODLTest):
    def test_get_fulfill_url(self):
        entry = self.get_data("shared_collection_loan_info.opds")
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

    def test_import(self):
        feed = self.get_data("shared_collection_feed.opds")
        data_source = DataSource.lookup(self._db, "DPLA Exchange", autocreate=True)
        collection = MockSharedODLAPI.mock_collection(self._db)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, data_source.name
        )

        class MockMetadataClient(object):
            def canonicalize_author_name(self, identifier, working_display_name):
                return working_display_name

        metadata_client = MockMetadataClient()
        importer = SharedODLImporter(
            self._db,
            collection=collection,
            metadata_client=metadata_client,
        )

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
