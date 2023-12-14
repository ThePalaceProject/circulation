from contextlib import contextmanager

import flask
import pytest

from api.admin.controller import setup_admin_controllers
from api.app import initialize_admin
from api.circulation_manager import CirculationManager
from api.config import Configuration
from core.integration.goals import Goals
from core.model import create
from core.model.admin import Admin, AdminRole
from core.model.configuration import ConfigurationSetting
from core.model.integration import IntegrationConfiguration
from core.util.http import HTTP
from tests.fixtures.api_controller import ControllerFixture, WorkSpec


class AdminControllerFixture:
    ctrl: ControllerFixture
    admin: Admin
    manager: CirculationManager

    BOOKS: list[WorkSpec] = []

    def __init__(self, controller_fixture: ControllerFixture):
        self.ctrl = controller_fixture
        self.manager = self.ctrl.manager

        ConfigurationSetting.sitewide(
            controller_fixture.db.session, Configuration.SECRET_KEY
        ).value = "a secret"

        initialize_admin(controller_fixture.db.session)
        setup_admin_controllers(controller_fixture.manager)
        self.admin, ignore = create(
            controller_fixture.db.session,
            Admin,
            email="example@nypl.org",
        )
        # This is a hash for 'password', we use the hash directly to avoid the cost
        # of doing the password hashing during test setup.
        self.admin.password_hashed = (
            "$2a$12$Dw74btoAgh49.vtOB56xPuumtcOY9HCZKS3RYImR42lR5IiT7PIOW"
        )

    @contextmanager
    def request_context_with_admin(self, route, *args, **kwargs):
        admin = self.admin
        if "admin" in kwargs:
            admin = kwargs.pop("admin")
        with self.ctrl.app.test_request_context(route, *args, **kwargs) as c:
            flask.request.form = {}
            flask.request.files = {}
            self.ctrl.db.session.begin_nested()
            flask.request.admin = admin
            yield c
            self.ctrl.db.session.commit()

    @contextmanager
    def request_context_with_library_and_admin(self, route, *args, **kwargs):
        admin = self.admin
        if "admin" in kwargs:
            admin = kwargs.pop("admin")
        with self.ctrl.request_context_with_library(route, *args, **kwargs) as c:
            flask.request.form = {}
            flask.request.files = {}
            self.ctrl.db.session.begin_nested()
            flask.request.admin = admin
            yield c
            self.ctrl.db.session.commit()


@pytest.fixture(scope="function")
def admin_ctrl_fixture(controller_fixture: ControllerFixture) -> AdminControllerFixture:
    return AdminControllerFixture(controller_fixture)


class SettingsControllerFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)

        # Delete any existing patron auth services created by controller test setup.
        for auth_service in self.ctrl.db.session.query(IntegrationConfiguration).filter(
            IntegrationConfiguration.goal == Goals.PATRON_AUTH_GOAL
        ):
            self.ctrl.db.session.delete(auth_service)

        # Delete any existing sitewide ConfigurationSettings.
        for setting in (
            self.ctrl.db.session.query(ConfigurationSetting)
            .filter(ConfigurationSetting.library_id == None)
            .filter(ConfigurationSetting.external_integration_id == None)
        ):
            self.ctrl.db.session.delete(setting)

        self.responses: list = []
        self.requests: list = []

        # Make the admin a system admin so they can do everything by default.
        self.admin.add_role(AdminRole.SYSTEM_ADMIN)

    def do_request(self, url, *args, **kwargs):
        """Mock HTTP get/post method to replace HTTP.get_with_timeout or post_with_timeout."""
        self.requests.append((url, args, kwargs))
        response = self.responses.pop()
        return HTTP.process_debuggable_response(url, response)

    def mock_prior_test_results(self, *args, **kwargs):
        self.prior_test_results_called_with = (args, kwargs)
        self_test_results = dict(
            duration=0.9,
            start="2018-08-08T16:04:05Z",
            end="2018-08-08T16:05:05Z",
            results=[],
        )
        self.self_test_results = self_test_results

        return self_test_results

    def mock_run_self_tests(self, *args, **kwargs):
        # This mocks the entire HasSelfTests.run_self_tests
        # process. In general, controllers don't care what's returned
        # from this method, because they only display the test results
        # as they were stored alongside the ExternalIntegration
        # as a side effect of run_self_tests running.
        self.run_self_tests_called_with = (args, kwargs)
        return ("value", "results")

    def mock_failed_run_self_tests(self, *args, **kwargs):
        self.failed_run_self_tests_called_with = (args, kwargs)
        return (None, None)


@pytest.fixture(scope="function")
def settings_ctrl_fixture(
    controller_fixture: ControllerFixture,
) -> SettingsControllerFixture:
    return SettingsControllerFixture(controller_fixture)


class AdminLibrarianFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)
        self.admin.add_role(
            AdminRole.LIBRARIAN, controller_fixture.db.default_library()
        )


@pytest.fixture(scope="function")
def admin_librarian_fixture(
    controller_fixture: ControllerFixture,
) -> AdminLibrarianFixture:
    return AdminLibrarianFixture(controller_fixture)
