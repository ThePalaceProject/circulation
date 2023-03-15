from api.admin.controller.sitewide_services import *
from core.model import ExternalIntegration


class TestSitewideServices:
    def test_sitewide_service_management(self, settings_ctrl_fixture):
        # The configuration of search and logging collections is delegated to
        # the _manage_sitewide_service and _delete_integration methods.
        #
        # Search collections are more comprehensively tested in test_search_services.

        EI = ExternalIntegration

        class MockSearch(SearchServicesController):
            def _manage_sitewide_service(self, *args):
                self.manage_called_with = args

            def _delete_integration(self, *args):
                self.delete_called_with = args

        controller = MockSearch(settings_ctrl_fixture.manager)

        with settings_ctrl_fixture.request_context_with_admin("/"):
            controller.process_services()
            goal, apis, key_name, problem = controller.manage_called_with
            assert EI.SEARCH_GOAL == goal
            assert ExternalSearchIndex in apis
            assert "search_services" == key_name
            assert "new search service" in problem

        with settings_ctrl_fixture.request_context_with_admin("/"):
            id = object()
            controller.process_delete(id)
            assert (id, EI.SEARCH_GOAL) == controller.delete_called_with
