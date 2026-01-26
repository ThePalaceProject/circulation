from __future__ import annotations

import datetime
import uuid
from functools import partial
from typing import Any, Literal
from unittest.mock import MagicMock

import pytest

from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.opds.lcp.license import LicenseDocument
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    License,
    LicensePool,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import FilesFixture, OPDS2WithODLFilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.mocks.odl import MockOPDS2WithODLApi


class OPDS2WithODLApiFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
        files: FilesFixture,
    ):
        self.db = db
        self.files = files

        self.library = db.default_library()
        self.collection = self.create_collection(self.library)
        self.work = self.create_work(self.collection)
        self.license = self.setup_license()
        self.mock_http = http_client
        self.api = MockOPDS2WithODLApi(self.db.session, self.collection)
        self.patron = db.patron()
        self.pool = self.license.license_pool
        self.license_document = partial(
            LicenseDocument,
            id=str(uuid.uuid4()),
            issued=utc_now(),
            provider="Tests",
        )
        self.api_checkout = partial(
            self.api.checkout,
            patron=self.patron,
            pin="pin",
            licensepool=self.pool,
            delivery_mechanism=MagicMock(),
        )

    def create_work(self, collection: Collection) -> Work:
        return self.db.work(with_license_pool=True, collection=collection)

    def create_collection(self, library: Library) -> Collection:
        return self.db.collection(
            f"Test {OPDS2WithODLApi.__name__} Collection",
            protocol=OPDS2WithODLApi,
            library=library,
            settings=self.db.opds2_odl_settings(
                username="a",
                password="b",
                external_account_id="http://odl",
                data_source="Feedbooks",
            ),
        )

    def setup_license(
        self,
        work: Work | None = None,
        available: int = 1,
        concurrency: int = 1,
        left: int | None = None,
        expires: datetime.datetime | None = None,
    ) -> License:
        work = work or self.work
        pool = work.license_pools[0]

        if len(pool.licenses) == 0:
            self.db.license(pool)

        license_ = pool.licenses[0]
        license_.checkout_url = "https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url,hint,hint_url}"
        license_.checkouts_available = available
        license_.terms_concurrency = concurrency
        license_.expires = expires
        license_.checkouts_left = left
        pool.update_availability_from_licenses()
        return license_

    @staticmethod
    def loan_status_document(
        status: str = "ready",
        self_link: str | Literal[False] = "http://status",
        return_link: str | Literal[False] = "http://return",
        license_link: str | Literal[False] = "http://license",
        links: list[dict[str, Any]] | None = None,
    ) -> LoanStatus:
        if links is None:
            links = []

        if license_link:
            links.append(
                {
                    "rel": "license",
                    "href": license_link,
                    "type": LicenseDocument.content_type(),
                },
            )

        if self_link:
            links.append(
                {
                    "rel": "self",
                    "href": self_link,
                    "type": LoanStatus.content_type(),
                }
            )

        if return_link:
            links.append(
                {
                    "rel": "return",
                    "href": return_link,
                    "type": LoanStatus.content_type(),
                }
            )

        return LoanStatus.model_validate(
            dict(
                id=str(uuid.uuid4()),
                status=status,
                message="This is a message",
                updated={
                    "license": utc_now(),
                    "status": utc_now(),
                },
                links=links,
                potential_rights={"end": "3017-10-21T11:12:13Z"},
            )
        )

    def checkin(
        self, patron: Patron | None = None, pool: LicensePool | None = None
    ) -> None:
        patron = patron or self.patron
        pool = pool or self.pool

        self.mock_http.queue_response(
            200, content=self.loan_status_document().model_dump_json()
        )
        self.mock_http.queue_response(
            200, content=self.loan_status_document("returned").model_dump_json()
        )
        self.api.checkin(patron, "pin", pool)

    def checkout(
        self,
        loan_url: str | None = None,
        patron: Patron | None = None,
        pool: LicensePool | None = None,
        create_loan: bool = False,
    ) -> LoanInfo:
        patron = patron or self.patron
        pool = pool or self.pool
        loan_url = loan_url or self.db.fresh_url()

        self.mock_http.queue_response(
            201, content=self.loan_status_document(self_link=loan_url).model_dump_json()
        )
        loan_info = self.api_checkout(patron=patron, licensepool=pool)
        if create_loan:
            loan_info.create_or_update(patron, pool)
        return loan_info

    def place_hold(
        self,
        patron: Patron | None = None,
        pool: LicensePool | None = None,
        create_hold: bool = False,
    ) -> HoldInfo:
        patron = patron or self.patron
        pool = pool or self.pool

        hold_info = self.api.place_hold(patron, "pin", pool, "dummy@email.com")
        if create_hold:
            hold_info.create_or_update(patron, pool)
        return hold_info


@pytest.fixture(scope="function")
def opds2_with_odl_api_fixture(
    db: DatabaseTransactionFixture,
    http_client: MockHttpClientFixture,
    opds2_with_odl_files_fixture: OPDS2WithODLFilesFixture,
) -> OPDS2WithODLApiFixture:
    return OPDS2WithODLApiFixture(db, http_client, opds2_with_odl_files_fixture)
