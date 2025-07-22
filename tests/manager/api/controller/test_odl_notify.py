from unittest.mock import PropertyMock, create_autospec

import pytest
from flask import Response
from freezegun import freeze_time
from sqlalchemy.orm.exc import StaleDataError

from palace.manager.api.controller.odl_notification import ODLNotificationController
from palace.manager.api.problem_details import (
    INVALID_LOAN_FOR_ODL_NOTIFICATION,
    NO_ACTIVE_LOAN,
)
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.integration.goals import Goals
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import License
from palace.manager.sqlalchemy.model.patron import Loan, Patron
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture
from tests.fixtures.odl import OPDS2WithODLApiFixture
from tests.fixtures.problem_detail import raises_problem_detail
from tests.fixtures.services import ServicesFixture


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
            protocol=OPDS2WithODLApi,
        )
        self.license = self.create_license()
        self.patron, self.patron_identifier = self.create_patron()
        self.controller = ODLNotificationController(db.session, self.registry)
        self.loan_status_document = OPDS2WithODLApiFixture.loan_status_document

    def create_license(self, collection: Collection | None = None) -> License:
        collection = collection or self.collection
        assert collection.data_source is not None
        pool = self.db.licensepool(
            None, collection=collection, data_source_name=collection.data_source.name
        )
        license = self.db.license(
            pool,
            checkout_url="https://provider.net/loan",
            checkouts_available=1,
            terms_concurrency=1,
        )
        pool.update_availability_from_licenses()
        return license

    def create_patron(self) -> tuple[Patron, str]:
        patron = self.db.patron()
        data_source = self.collection.data_source
        assert data_source is not None
        patron_identifier = patron.identifier_to_remote_service(data_source)
        return patron, patron_identifier

    def create_loan(
        self, license: License | None = None, patron: Patron | None = None
    ) -> Loan:
        if license is None:
            license = self.license
        if patron is None:
            patron = self.patron
        license.checkout()
        loan, _ = license.loan_to(patron)
        loan.external_identifier = self.db.fresh_str()
        return loan


@pytest.fixture(scope="function")
def odl_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> ODLFixture:
    return ODLFixture(db, services_fixture)


class TestODLNotificationController:
    """Test that an ODL distributor can notify the circulation manager
    when a loan's status changes."""

    def test__get_loan(
        self, db: DatabaseTransactionFixture, odl_fixture: ODLFixture
    ) -> None:
        patron1, patron_id_1 = odl_fixture.create_patron()
        patron2, patron_id_2 = odl_fixture.create_patron()
        patron3, patron_id_3 = odl_fixture.create_patron()

        license1 = odl_fixture.create_license()
        license2 = odl_fixture.create_license()
        license3 = odl_fixture.create_license()

        loan1 = odl_fixture.create_loan(license=license1, patron=patron1)
        loan2 = odl_fixture.create_loan(license=license2, patron=patron1)
        loan3 = odl_fixture.create_loan(license=license3, patron=patron2)

        # We get the correct loan for each patron and license.
        assert (
            odl_fixture.controller._get_loan(patron_id_1, license1.identifier) == loan1
        )
        assert (
            odl_fixture.controller._get_loan(patron_id_1, license2.identifier) == loan2
        )
        assert (
            odl_fixture.controller._get_loan(patron_id_2, license3.identifier) == loan3
        )

        # We get None if the patron doesn't have a loan for the license.
        assert (
            odl_fixture.controller._get_loan(patron_id_1, license3.identifier) is None
        )
        assert (
            odl_fixture.controller._get_loan(patron_id_2, license1.identifier) is None
        )
        assert (
            odl_fixture.controller._get_loan(patron_id_3, license1.identifier) is None
        )

        # We get None if the patron or license identifiers are None.
        assert odl_fixture.controller._get_loan(None, license1.identifier) is None
        assert odl_fixture.controller._get_loan(patron_id_1, None) is None
        assert odl_fixture.controller._get_loan(None, None) is None

    @freeze_time()
    def test_notify_success(
        self,
        flask_app_fixture: FlaskAppFixture,
        odl_fixture: ODLFixture,
    ) -> None:
        loan = odl_fixture.create_loan()
        assert odl_fixture.license.checkouts_available == 0

        status_doc = odl_fixture.loan_status_document("active")
        with flask_app_fixture.test_request_context(
            "/",
            method="POST",
            data=status_doc.model_dump_json(),
            library=odl_fixture.library,
        ):
            assert odl_fixture.license.identifier is not None
            response = odl_fixture.controller.notify(
                odl_fixture.patron_identifier, odl_fixture.license.identifier
            )
            assert response.status_code == 204

        assert loan.end != utc_now()

        status_doc = odl_fixture.loan_status_document("revoked")
        with flask_app_fixture.test_request_context(
            "/",
            method="POST",
            data=status_doc.model_dump_json(),
            library=odl_fixture.library,
        ):
            response = odl_fixture.controller.notify(
                odl_fixture.patron_identifier, odl_fixture.license.identifier
            )
            assert response.status_code == 204

        assert loan.end == utc_now()

    def test_notify_errors(
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        odl_fixture: ODLFixture,
    ) -> None:
        # Bad JSON.
        non_odl_collection = db.collection()
        license = odl_fixture.create_license(collection=non_odl_collection)
        odl_fixture.create_loan(license=license)

        with (
            flask_app_fixture.test_request_context(
                "/", method="POST", library=odl_fixture.library
            ),
            raises_problem_detail(pd=INVALID_INPUT),
        ):
            assert license.identifier is not None
            odl_fixture.controller.notify(
                odl_fixture.patron_identifier, license.identifier
            )

        # Loan from a non-ODL collection.
        with (
            flask_app_fixture.test_request_context(
                "/",
                method="POST",
                library=odl_fixture.library,
                data=odl_fixture.loan_status_document("active").model_dump_json(),
            ),
            raises_problem_detail(pd=INVALID_LOAN_FOR_ODL_NOTIFICATION),
        ):
            odl_fixture.controller.notify(
                odl_fixture.patron_identifier, license.identifier
            )

        # No loan, but distributor thinks it isn't active
        NON_EXISTENT_LICENSE_IDENTIFIER = "Foo"
        with flask_app_fixture.test_request_context(
            "/",
            method="POST",
            library=odl_fixture.library,
            data=odl_fixture.loan_status_document("returned").model_dump_json(),
        ):
            response = odl_fixture.controller.notify(
                odl_fixture.patron_identifier, NON_EXISTENT_LICENSE_IDENTIFIER
            )
        assert isinstance(response, Response)
        assert response.status_code == 204

        # No loan, but distributor thinks it is active
        with (
            flask_app_fixture.test_request_context(
                "/",
                method="POST",
                library=odl_fixture.library,
                data=odl_fixture.loan_status_document("active").model_dump_json(),
            ),
            raises_problem_detail(
                pd=NO_ACTIVE_LOAN.detailed("No loan was found.", 404)
            ),
        ):
            odl_fixture.controller.notify(
                odl_fixture.patron_identifier, NON_EXISTENT_LICENSE_IDENTIFIER
            )

    def test__process_notification_already_deleted(
        self,
        odl_fixture: ODLFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ) -> None:
        mock_loan = create_autospec(Loan)
        type(mock_loan).end = PropertyMock(side_effect=StaleDataError())
        mock_loan.license_pool.collection.integration_configuration.protocol = (
            db.protocol_string(Goals.LICENSE_GOAL, OPDS2WithODLApi)
        )
        with flask_app_fixture.test_request_context(
            "/",
            method="POST",
            library=odl_fixture.library,
            data=odl_fixture.loan_status_document("revoked").model_dump_json(),
        ):
            response = odl_fixture.controller._process_notification(mock_loan)
        assert response.status_code == 204
