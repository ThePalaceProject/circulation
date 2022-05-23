"""Self-tests for metadata integrations."""
from flask_babel import lazy_gettext as _

from api.admin.controller.metadata_services import MetadataServicesController
from api.admin.controller.self_tests import SelfTestsController
from core.model import ExternalIntegration


class MetadataServiceSelfTestsController(
    MetadataServicesController, SelfTestsController
):
    def __init__(self, manager):
        super().__init__(manager)
        self.type = _("metadata service")

    def process_metadata_service_self_tests(self, identifier):
        return self._manage_self_tests(identifier)

    def look_up_by_id(self, id):
        return self.look_up_service_by_id(
            id, protocol=None, goal=ExternalIntegration.METADATA_GOAL
        )
