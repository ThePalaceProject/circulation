import pytest

from core.jobs.update_integration_status import UpdateIntegrationStatusScript
from core.model import create
from core.model.configuration import ExternalIntegration, ExternalIntegrationError
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class UpdateIntegrationStatusFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db: DatabaseTransactionFixture = db
        self.script: UpdateIntegrationStatusScript = UpdateIntegrationStatusScript(
            _db=db.session
        )
        self.patron_integration = db.external_integration(
            "AUTH", ExternalIntegration.PATRON_AUTH_GOAL
        )
        self.not_patron_integration = db.external_integration(
            "NOTAUTH", "NOT PATRON AUTH"
        )

    def record_error(self, time=None):
        create(
            self.db.session,
            ExternalIntegrationError,
            external_integration_id=self.patron_integration.id,
            time=time or utc_now(),
        )

    def record_enough_errors(self, integration: ExternalIntegration, less=0):
        """Record enough errors for a status change to RED"""
        # Record errors
        for _ in range(self.script.ERROR_WINDOW_COUNT - less):
            self.record_error()


@pytest.fixture(scope="function")
def integration_status_fixture(
    db: DatabaseTransactionFixture,
) -> UpdateIntegrationStatusFixture:
    return UpdateIntegrationStatusFixture(db)


class TestUpdateIntegrationStatus:
    def test_auth_errors(
        self, integration_status_fixture: UpdateIntegrationStatusFixture
    ):
        assert (
            integration_status_fixture.patron_integration.status
            == ExternalIntegration.GREEN
        )
        integration_status_fixture.record_enough_errors(
            integration_status_fixture.patron_integration
        )
        integration_status_fixture.script.do_run()
        assert (
            integration_status_fixture.patron_integration.status
            == ExternalIntegration.RED
        )

    def test_non_patron_errors(
        self, integration_status_fixture: UpdateIntegrationStatusFixture
    ):
        # Non patron auth goals should not be affected by the job
        assert (
            integration_status_fixture.not_patron_integration.status
            == ExternalIntegration.GREEN
        )
        integration_status_fixture.record_enough_errors(
            integration_status_fixture.not_patron_integration
        )
        integration_status_fixture.script.do_run()
        assert (
            integration_status_fixture.not_patron_integration.status
            == ExternalIntegration.GREEN
        )

    def test_red_to_green(
        self, integration_status_fixture: UpdateIntegrationStatusFixture
    ):
        # There's one less than required record, so the status should stay green
        integration_status_fixture.record_enough_errors(
            integration_status_fixture.patron_integration, less=1
        )
        integration_status_fixture.script.do_run()
        assert (
            integration_status_fixture.patron_integration.status
            == ExternalIntegration.GREEN
        )

        # This should flip back to green
        integration_status_fixture.patron_integration.status = ExternalIntegration.RED
        integration_status_fixture.script.do_run()
        assert (
            integration_status_fixture.patron_integration.status
            == ExternalIntegration.GREEN
        )
