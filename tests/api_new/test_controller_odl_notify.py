import json

import flask
import pytest

from api.odl import ODLAPI
from api.odl2 import ODL2API
from api.problem_details import INVALID_LOAN_FOR_ODL_NOTIFICATION, NO_ACTIVE_LOAN
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.odl import ODLFixture


class TestODLNotificationController:
    """Test that an ODL distributor can notify the circulation manager
    when a loan's status changes."""

    @pytest.mark.parametrize(
        "protocol",
        [
            pytest.param(ODLAPI.NAME, id="ODL 1.x collection"),
            pytest.param(ODL2API.NAME, id="ODL 2.x collection"),
        ],
    )
    def test_notify_success(
        self, protocol, controller_fixture: ControllerFixture, odl_fixture: ODLFixture
    ):
        db = controller_fixture.db

        odl_fixture.collection.external_integration.protocol = protocol
        odl_fixture.pool.licenses_owned = 10
        odl_fixture.pool.licenses_available = 5
        loan, ignore = odl_fixture.pool.loan_to(odl_fixture.patron)
        loan.external_identifier = db.fresh_str()

        with controller_fixture.request_context_with_library("/", method="POST"):
            text = json.dumps(
                {
                    "id": loan.external_identifier,
                    "status": "revoked",
                }
            )
            data = bytes(text, "utf-8")
            flask.request.data = data  # type: ignore
            response = controller_fixture.manager.odl_notification_controller.notify(
                loan.id
            )
            assert 200 == response.status_code

            # The pool's availability has been updated.
            api = controller_fixture.manager.circulation_apis[
                db.default_library().id
            ].api_for_license_pool(loan.license_pool)
            assert [loan.license_pool] == api.availability_updated_for

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
