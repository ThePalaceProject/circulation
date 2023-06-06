from api.admin.problem_details import *
from core.model import ExternalIntegration, create
from core.selftest import HasSelfTests


class TestSearchServiceSelfTests:
    def test_search_service_self_tests_with_no_identifier(self, settings_ctrl_fixture):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = settings_ctrl_fixture.manager.admin_search_service_self_tests_controller.process_search_service_self_tests(
                None
            )
            assert response.title == MISSING_IDENTIFIER.title
            assert response.detail == MISSING_IDENTIFIER.detail
            assert response.status_code == 400

    def test_search_service_self_tests_with_no_search_service_found(
        self, settings_ctrl_fixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = settings_ctrl_fixture.manager.admin_search_service_self_tests_controller.process_search_service_self_tests(
                -1
            )
            assert response == MISSING_SERVICE
            assert response.status_code == 404

    def test_search_service_self_tests_test_get(self, settings_ctrl_fixture):
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = settings_ctrl_fixture.mock_prior_test_results
        search_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPENSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's self tests object.
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = settings_ctrl_fixture.manager.admin_search_service_self_tests_controller.process_search_service_self_tests(
                search_service.id
            )
            response_search_service = response.get("self_test_results")

            assert response_search_service.get("id") == search_service.id
            assert response_search_service.get("name") == search_service.name
            assert (
                response_search_service.get("protocol").get("label")
                == search_service.protocol
            )
            assert response_search_service.get("goal") == search_service.goal
            assert (
                response_search_service.get("self_test_results")
                == HasSelfTests.prior_test_results()
            )

        HasSelfTests.prior_test_results = old_prior_test_results

    def test_search_service_self_tests_post(self, settings_ctrl_fixture):
        old_run_self_tests = HasSelfTests.run_self_tests
        HasSelfTests.run_self_tests = settings_ctrl_fixture.mock_run_self_tests

        search_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPENSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        m = (
            settings_ctrl_fixture.manager.admin_search_service_self_tests_controller.self_tests_process_post
        )
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            response = m(search_service.id)
            assert response._status == "200 OK"
            assert "Successfully ran new self tests" == response.get_data(as_text=True)

        positional, keyword = settings_ctrl_fixture.run_self_tests_called_with
        # run_self_tests was called with positional arguments:
        # * The database connection
        # * The method to call to instantiate a HasSelfTests implementation
        #   (None -- this means to use the default ExternalSearchIndex
        #   constructor.)
        # * The database connection again (to be passed into
        #   the ExternalSearchIndex constructor).
        assert (
            settings_ctrl_fixture.ctrl.db.session,
            None,
            settings_ctrl_fixture.ctrl.db.session,
        ) == positional

        # run_self_tests was not called with any keyword arguments.
        assert {} == keyword

        # Undo the mock.
        HasSelfTests.run_self_tests = old_run_self_tests
