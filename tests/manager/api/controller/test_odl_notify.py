import json
import types
from unittest.mock import create_autospec

import flask
import pytest

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.api.problem_details import (
    INVALID_LOAN_FOR_ODL_NOTIFICATION,
    NO_ACTIVE_LOAN,
)
from palace.manager.sqlalchemy.model.collection import Collection
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture


class ODLFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.library = self.db.default_library()

        """Create a mock ODL collection to use in tests."""
        self.collection, _ = Collection.by_name_and_protocol(
            self.db.session, "Test ODL Collection", OPDS2WithODLApi.label()
        )
        self.collection.integration_configuration.settings_dict = {
            "username": "a",
            "password": "b",
            "url": "http://metadata",
            "external_integration_id": "http://odl",
            Collection.DATA_SOURCE_NAME_SETTING: "Feedbooks",
        }
        self.collection.libraries.append(self.library)
        self.work = self.db.work(with_license_pool=True, collection=self.collection)

        def setup(self, available, concurrency, left=None, expires=None):
            self.checkouts_available = available
            self.checkouts_left = left
            self.terms_concurrency = concurrency
            self.expires = expires
            self.license_pool.update_availability_from_licenses()

        self.pool = self.work.license_pools[0]
        self.license = self.db.license(
            self.pool,
            checkout_url="https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url,hint,hint_url}",
            checkouts_available=1,
            terms_concurrency=1,
        )
        types.MethodType(setup, self.license)
        self.pool.update_availability_from_licenses()
        self.patron = self.db.patron()

    @staticmethod
    def integration_protocol():
        return OPDS2WithODLApi.label()


@pytest.fixture(scope="function")
def odl_fixture(db: DatabaseTransactionFixture) -> ODLFixture:
    return ODLFixture(db)


class TestODLNotificationController:
    """Test that an ODL distributor can notify the circulation manager
    when a loan's status changes."""

    @pytest.mark.parametrize(
        "api_cls",
        [
            pytest.param(OPDS2WithODLApi, id="ODL 2.x collection"),
        ],
    )
    def test_notify_success(
        self,
        api_cls: type[OPDS2WithODLApi],
        controller_fixture: ControllerFixture,
        odl_fixture: ODLFixture,
    ):
        db = controller_fixture.db

        odl_fixture.collection.integration_configuration.protocol = api_cls.label()
        odl_fixture.pool.licenses_owned = 10
        odl_fixture.pool.licenses_available = 5
        loan, ignore = odl_fixture.pool.loan_to(odl_fixture.patron)
        loan.external_identifier = db.fresh_str()

        api = controller_fixture.manager.circulation_apis[
            db.default_library().id
        ].api_for_license_pool(loan.license_pool)
        update_loan_mock = create_autospec(api_cls.update_loan)
        api.update_loan = update_loan_mock

        with controller_fixture.request_context_with_library("/", method="POST"):
            text = json.dumps(
                {
                    "id": loan.external_identifier,
                    "status": "revoked",
                }
            )
            data = bytes(text, "utf-8")
            flask.request.data = data
            response = controller_fixture.manager.odl_notification_controller.notify(
                loan.id
            )
            assert 200 == response.status_code

            # Update loan was called with the expected arguments.
            update_loan_mock.assert_called_once_with(loan, json.loads(text))

    def test_notify_errors(self, controller_fixture: ControllerFixture):
        db = controller_fixture.db

        # No loan.
        with controller_fixture.request_context_with_library("/", method="POST"):
            response = controller_fixture.manager.odl_notification_controller.notify(
                db.fresh_str()
            )
            assert NO_ACTIVE_LOAN.uri == response.uri

        # Loan from a non-ODL collection.
        patron = db.patron()
        pool = db.licensepool(None)
        loan, ignore = pool.loan_to(patron)
        loan.external_identifier = db.fresh_str()

        with controller_fixture.request_context_with_library("/", method="POST"):
            response = controller_fixture.manager.odl_notification_controller.notify(
                loan.id
            )
            assert INVALID_LOAN_FOR_ODL_NOTIFICATION == response
