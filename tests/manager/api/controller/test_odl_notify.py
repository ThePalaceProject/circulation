from unittest.mock import MagicMock

import pytest

from palace.manager.api.controller.odl_notification import ODLNotificationController
from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.api.problem_details import (
    INVALID_LOAN_FOR_ODL_NOTIFICATION,
    NO_ACTIVE_LOAN,
)
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture
from tests.fixtures.odl import OPDS2WithODLApiFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.mock import MockHTTPClient
from tests.mocks.odl import MockOPDS2WithODLApi


class ODLFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ) -> None:
        self.db = db
        self.library = self.db.default_library()
        self.registry = (
            services_fixture.services.integration_registry.license_providers()
        )
        self.collection = db.collection(
            protocol=self.registry.get_protocol(OPDS2WithODLApi),
            settings={
                "username": "a",
                "password": "b",
                "external_account_id": "http://odl",
                "data_source": "Feedbooks",
            },
        )
        self.work = self.db.work(with_license_pool=True, collection=self.collection)
        self.pool = self.work.license_pools[0]
        self.license = self.db.license(
            self.pool,
            checkout_url="https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url,hint,hint_url}",
            checkouts_available=1,
            terms_concurrency=1,
        )
        self.pool.update_availability_from_licenses()
        self.patron = self.db.patron()
        self.http_client = MockHTTPClient()
        self.api = MockOPDS2WithODLApi(db.session, self.collection, self.http_client)
        self.mock_circulation_manager = MagicMock()
        self.mock_circulation_manager.circulation_apis[
            self.library.id
        ].api_for_license_pool.return_value = self.api
        self.controller = ODLNotificationController(
            db.session, self.mock_circulation_manager, self.registry
        )
        self.loan_status_document = OPDS2WithODLApiFixture.loan_status_document


@pytest.fixture(scope="function")
def odl_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> ODLFixture:
    return ODLFixture(db, services_fixture)


class TestODLNotificationController:
    """Test that an ODL distributor can notify the circulation manager
    when a loan's status changes."""

    def test_notify_success(
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        odl_fixture: ODLFixture,
    ) -> None:
        odl_fixture.license.checkout()
        loan, ignore = odl_fixture.license.loan_to(odl_fixture.patron)
        loan.external_identifier = db.fresh_str()

        assert odl_fixture.license.checkouts_available == 0

        status_doc = odl_fixture.loan_status_document("revoked")
        with flask_app_fixture.test_request_context(
            "/",
            method="POST",
            data=status_doc.model_dump_json(),
            library=odl_fixture.library,
        ):
            assert loan.id is not None
            response = odl_fixture.controller.notify(loan.id)
            assert response.status_code == 200

        assert odl_fixture.license.checkouts_available == 1

    def test_notify_errors(
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        odl_fixture: ODLFixture,
    ):
        # No loan.
        with flask_app_fixture.test_request_context(
            "/", method="POST", library=odl_fixture.library
        ):
            response = odl_fixture.controller.notify(-55)
        assert isinstance(response, ProblemDetail)
        assert response.uri == NO_ACTIVE_LOAN.uri

        # Bad JSON.
        patron = db.patron()
        pool = db.licensepool(None)
        loan, ignore = pool.loan_to(patron)
        loan.external_identifier = db.fresh_str()
        with flask_app_fixture.test_request_context(
            "/", method="POST", library=odl_fixture.library
        ):
            response = odl_fixture.controller.notify(loan.id)
        assert response == INVALID_INPUT

        # Loan from a non-ODL collection.
        with flask_app_fixture.test_request_context(
            "/",
            method="POST",
            library=odl_fixture.library,
            data=odl_fixture.loan_status_document("active").model_dump_json(),
        ):
            response = odl_fixture.controller.notify(loan.id)
        assert isinstance(response, ProblemDetail)
        assert response == INVALID_LOAN_FOR_ODL_NOTIFICATION
