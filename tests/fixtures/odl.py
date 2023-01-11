import json
import types
from typing import Callable, Tuple, Any, Optional

import pytest
from _pytest.monkeypatch import MonkeyPatch

from api.odl import ODLAPI
from core.model import get_one_or_create, Collection, Patron, LicensePool, Loan, Representation, Library, Work, License, \
    IntegrationClient
from core.testing import MockRequestsResponse, DatabaseTest
from core.util.http import HTTP
from tests.fixtures.api_odl_files import ODLAPIFilesFixture
from tests.fixtures.database import DatabaseTransactionFixture


class MonkeyPatchedODLFixture:
    """A fixture that patches the ODLAPI to make it possible to intercept HTTP requests for testing."""

    def __init__(self, monkeypatch: MonkeyPatch):
        self.monkeypatch = monkeypatch


@pytest.fixture(scope="function")
def monkey_patch_odl(monkeypatch) -> MonkeyPatchedODLFixture:
    """A fixture that patches the ODLAPI to make it possible to intercept HTTP requests for testing."""

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _get(self, url, headers=None):
        self.requests.append([url, headers])
        response = self.responses.pop()
        return HTTP._process_response(url, response)

    def _url_for(self, *args, **kwargs):
        del kwargs["_external"]
        return "http://{}?{}".format(
            "/".join(args),
            "&".join([f"{key}={val}" for key, val in list(kwargs.items())]),
        )

    monkeypatch.setattr(ODLAPI, "_get", _get)
    monkeypatch.setattr(ODLAPI, "_url_for", _url_for)
    monkeypatch.setattr(ODLAPI, "queue_response", queue_response, raising=False)
    return MonkeyPatchedODLFixture(monkeypatch)


class ODLTestFixture:
    """A basic ODL fixture that collects various bits of information shared by all tests."""

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        files: ODLAPIFilesFixture,
        patched: MonkeyPatchedODLFixture,
    ):
        self.db = db
        self.files = files
        self.patched = patched

    def library(self):
        return DatabaseTest.make_default_library(self.db.session)

    def collection(self, library):
        """Create a mock ODL collection to use in tests."""
        integration_protocol = ODLAPI.NAME
        collection, ignore = get_one_or_create(
            self.db.session,
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

    def work(self, collection):
        return self.db.work(with_license_pool=True, collection=collection)

    def pool(self, license):
        return license.license_pool

    def license(self, work):
        def setup(self, available, concurrency, left=None, expires=None):
            self.checkouts_available = available
            self.checkouts_left = left
            self.terms_concurrency = concurrency
            self.expires = expires
            self.license_pool.update_availability_from_licenses()

        pool = work.license_pools[0]
        l = self.db.license(
            pool,
            checkout_url="https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url,hint,hint_url}",
            checkouts_available=1,
            terms_concurrency=1,
        )
        l.setup = types.MethodType(setup, l)
        pool.update_availability_from_licenses()
        return l

    def api(self, collection):
        api = ODLAPI(self.db.session, collection)
        api.requests = []
        api.responses = []
        return api

    def client(self):
        return self.db.integration_client()

    def checkin(self, api, patron: Patron, pool: LicensePool) -> Callable[[], None]:
        """Create a function that, when evaluated, performs a checkin."""

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

        def c():
            api.queue_response(200, content=lsd)
            api.queue_response(200)
            api.queue_response(200, content=returned_lsd)
            api.checkin(patron, "pin", pool)

        return c

    def checkout(
        self,
        api,
        patron: Patron,
        pool: LicensePool,
        db: DatabaseTransactionFixture,
        loan_url: str,
    ) -> Callable[[], Tuple[Loan, Any]]:
        """Create a function that, when evaluated, performs a checkout."""

        def c():
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
                db.session.query(Loan)
                .filter(Loan.license_pool == pool, Loan.patron == patron)
                .one()
            )
            return loan, loan_db

        return c


@pytest.fixture(scope="function")
def odl_test_fixture(
    db: DatabaseTransactionFixture,
    api_odl_files_fixture: ODLAPIFilesFixture,
    monkey_patch_odl: MonkeyPatchedODLFixture,
) -> ODLTestFixture:
    return ODLTestFixture(db, api_odl_files_fixture, monkey_patch_odl)


class ODLAPITestFixture:
    """An ODL fixture that sets up extra information for API testing on top of the base ODL fixture."""

    def __init__(
        self,
        odl_fixture: ODLTestFixture,
        library: Library,
        collection: Collection,
        work: Work,
        license: License,
        api,
        patron: Patron,
        client: IntegrationClient,
    ):
        self.fixture = odl_fixture
        self.db = odl_fixture.db
        self.files = odl_fixture.files
        self.library = library
        self.collection = collection
        self.work = work
        self.license = license
        self.api = api
        self.patron = patron
        self.pool = license.license_pool  # type: ignore
        self.client = client

    def checkin(
        self, patron: Optional[Patron] = None, pool: Optional[LicensePool] = None
    ):
        patron = patron or self.patron
        pool = pool or self.pool
        return self.fixture.checkin(self.api, patron=patron, pool=pool)()

    def checkout(
        self,
        loan_url: Optional[str] = None,
        patron: Optional[Patron] = None,
        pool: Optional[LicensePool] = None,
    ) -> Tuple[Loan, Any]:
        patron = patron or self.patron
        pool = pool or self.pool
        loan_url = loan_url or self.db.fresh_url()
        return self.fixture.checkout(
            self.api, patron=patron, pool=pool, db=self.db, loan_url=loan_url
        )()


@pytest.fixture(scope="function")
def odl_api_test_fixture(odl_test_fixture: ODLTestFixture) -> ODLAPITestFixture:
    library = odl_test_fixture.library()
    collection = odl_test_fixture.collection(library)
    work = odl_test_fixture.work(collection)
    license = odl_test_fixture.license(work)
    api = odl_test_fixture.api(collection)
    patron = odl_test_fixture.db.patron()
    client = odl_test_fixture.client()
    return ODLAPITestFixture(
        odl_test_fixture, library, collection, work, license, api, patron, client
    )
