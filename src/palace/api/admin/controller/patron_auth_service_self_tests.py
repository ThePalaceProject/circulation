from palace.api.admin.controller.patron_auth_services import (
    PatronAuthServicesController,
)
from palace.api.admin.controller.self_tests import SelfTestsController
from palace.api.admin.problem_details import *
from palace.core.model import ExternalIntegration, get_one


class PatronAuthServiceSelfTestsController(
    SelfTestsController, PatronAuthServicesController
):
    def process_patron_auth_service_self_tests(self, identifier):
        return self._manage_self_tests(identifier)

    def look_up_by_id(self, identifier):
        service = get_one(
            self._db,
            ExternalIntegration,
            id=identifier,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
        )
        if not service:
            return MISSING_SERVICE
        return service

    def get_info(self, patron_auth_service):
        [protocol] = [
            p
            for p in self._get_integration_protocols(self.provider_apis)
            if p.get("name") == patron_auth_service.protocol
        ]
        info = dict(
            id=patron_auth_service.id,
            name=patron_auth_service.name,
            protocol=patron_auth_service.protocol,
            goal=patron_auth_service.goal,
            settings=protocol.get("settings"),
        )
        return info

    def run_tests(self, patron_auth_service):
        # If the auth service doesn't have at least one library associated with it,
        # then admins will not be able to access the button to run self tests for it, so
        # this code will never be reached; hence, no need to check here that :library exists.
        value = None
        if len(patron_auth_service.libraries):
            library = patron_auth_service.libraries[0]
            value = self._find_protocol_class(patron_auth_service).run_self_tests(
                self._db, None, library, patron_auth_service
            )
        return value
