from api.admin.problem_details import *
from api.simple_authentication import SimpleAuthenticationProvider
from core.model import ExternalIntegration, create
from core.selftest import HasSelfTests


class TestPatronAuthSelfTests:
    def _auth_service(self, db_session, libraries=None):
        if libraries is None:
            libraries = []

        auth_service, ignore = create(
            db_session,
            ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            name="name",
            libraries=libraries,
        )
        return auth_service

    def test_patron_auth_self_tests_with_no_identifier(self, settings_ctrl_fixture):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = settings_ctrl_fixture.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(
                None
            )
            assert response.title == MISSING_IDENTIFIER.title
            assert response.detail == MISSING_IDENTIFIER.detail
            assert response.status_code == 400

    def test_patron_auth_self_tests_with_no_auth_service_found(
        self, settings_ctrl_fixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = settings_ctrl_fixture.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(
                -1
            )
            assert response == MISSING_SERVICE
            assert response.status_code == 404

    def test_patron_auth_self_tests_get_with_no_libraries(self, settings_ctrl_fixture):
        auth_service = self._auth_service(settings_ctrl_fixture.ctrl.db.session)
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = settings_ctrl_fixture.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(
                auth_service.id
            )
            results = response.get("self_test_results").get("self_test_results")
            assert results.get("disabled") == True
            assert (
                results.get("exception")
                == "You must associate this service with at least one library before you can run self tests for it."
            )

    def test_patron_auth_self_tests_test_get(self, settings_ctrl_fixture):
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = settings_ctrl_fixture.mock_prior_test_results
        auth_service = self._auth_service(
            settings_ctrl_fixture.ctrl.db.session,
            [settings_ctrl_fixture.ctrl.db.library()],
        )

        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's self tests object.
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = settings_ctrl_fixture.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(
                auth_service.id
            )
            response_auth_service = response.get("self_test_results")

            assert response_auth_service.get("name") == auth_service.name
            assert response_auth_service.get("protocol") == auth_service.protocol
            assert response_auth_service.get("id") == auth_service.id
            assert response_auth_service.get("goal") == auth_service.goal
            assert (
                response_auth_service.get("self_test_results")
                == settings_ctrl_fixture.self_test_results
            )

        HasSelfTests.prior_test_results = old_prior_test_results

    def test_patron_auth_self_tests_post_with_no_libraries(self, settings_ctrl_fixture):
        auth_service = self._auth_service(settings_ctrl_fixture.ctrl.db.session)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            response = settings_ctrl_fixture.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(
                auth_service.id
            )
            assert response.title == FAILED_TO_RUN_SELF_TESTS.title
            assert (
                response.detail
                == "Failed to run self tests for this patron authentication service."
            )
            assert response.status_code == 400

    def test_patron_auth_self_tests_test_post(self, settings_ctrl_fixture):
        old_run_self_tests = HasSelfTests.run_self_tests
        HasSelfTests.run_self_tests = settings_ctrl_fixture.mock_run_self_tests
        auth_service = self._auth_service(
            settings_ctrl_fixture.ctrl.db.session,
            [settings_ctrl_fixture.ctrl.db.library()],
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            response = settings_ctrl_fixture.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(
                auth_service.id
            )
            assert response._status == "200 OK"
            assert "Successfully ran new self tests" == response.get_data(as_text=True)

        # run_self_tests was called with the database twice (the
        # second time to be used in the ExternalSearchIntegration
        # constructor). There were no keyword arguments.
        assert (
            (
                settings_ctrl_fixture.ctrl.db.session,
                None,
                auth_service.libraries[0],
                auth_service,
            ),
            {},
        ) == settings_ctrl_fixture.run_self_tests_called_with

        HasSelfTests.run_self_tests = old_run_self_tests
