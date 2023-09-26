import json
import types
from typing import Any, Callable, Optional, Tuple

import pytest
from _pytest.monkeypatch import MonkeyPatch

from api.circulation import LoanInfo
from api.odl import ODLAPI
from api.odl2 import ODL2API
from core.model import (
    Collection,
    Library,
    License,
    LicensePool,
    Loan,
    Patron,
    Representation,
    Work,
    get_one_or_create,
)
from core.model.configuration import ExternalIntegration
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse
from tests.fixtures.api_odl import ODL2APIFilesFixture, ODLAPIFilesFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import APIFilesFixture


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
        files: APIFilesFixture,
        patched: MonkeyPatchedODLFixture,
    ):
        self.db = db
        self.files = files
        self.patched = patched

    def library(self):
        return self.db.default_library()

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
        config = collection.create_integration_configuration(integration_protocol)
        config.settings_dict = {
            "username": "a",
            "password": "b",
            "url": "http://metadata",
            Collection.DATA_SOURCE_NAME_SETTING: "Feedbooks",
        }
        config.for_library(library.id, create=True)
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
    ) -> Callable[[], Tuple[LoanInfo, Any]]:
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
        self.pool = license.license_pool

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
    ) -> Tuple[LoanInfo, Any]:
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
    return ODLAPITestFixture(
        odl_test_fixture, library, collection, work, license, api, patron
    )


class ODL2TestFixture(ODLTestFixture):
    """An ODL2 test fixture that mirrors the ODL test fixture except for the API class being used"""

    def collection(self, library) -> Collection:
        collection = super().collection(library)
        collection.name = "Test ODL2 Collection"
        collection.integration_configuration.protocol = ExternalIntegration.ODL2
        return collection

    def api(self, collection) -> ODL2API:
        api = ODL2API(self.db.session, collection)
        api.requests = []  # type: ignore
        api.responses = []  # type: ignore
        return api


class ODL2APITestFixture(ODLAPITestFixture):
    """The ODL2 API fixture has no changes in terms of data, from the ODL API fixture"""


@pytest.fixture(scope="function")
def odl2_test_fixture(
    db: DatabaseTransactionFixture,
    api_odl2_files_fixture: ODL2APIFilesFixture,
    monkey_patch_odl: MonkeyPatchedODLFixture,
) -> ODL2TestFixture:
    """The ODL2 API uses the ODL API in the background, so the mockeypatching is the same"""
    return ODL2TestFixture(db, api_odl2_files_fixture, monkey_patch_odl)


@pytest.fixture(scope="function")
def odl2_api_test_fixture(odl2_test_fixture: ODL2TestFixture) -> ODL2APITestFixture:
    library = odl2_test_fixture.library()
    collection = odl2_test_fixture.collection(library)
    work = odl2_test_fixture.work(collection)
    license = odl2_test_fixture.license(work)
    api = odl2_test_fixture.api(collection)
    patron = odl2_test_fixture.db.patron()
    return ODL2APITestFixture(
        odl2_test_fixture, library, collection, work, license, api, patron
    )
