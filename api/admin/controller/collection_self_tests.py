from flask_babel import lazy_gettext as _

from api.admin.controller.self_tests import SelfTestsController
from api.admin.problem_details import *
from api.integration.registry.license_providers import LicenseProvidersRegistry
from api.selftest import HasCollectionSelfTests
from core.model import Collection
from core.opds_import import OPDSImporter, OPDSImportMonitor


class CollectionSelfTestsController(SelfTestsController):
    def __init__(self, manager):
        super().__init__(manager)
        self.type = _("collection")
        self.registry = LicenseProvidersRegistry()
        self.protocols = self._get_collection_protocols(self.registry.integrations)

    def process_collection_self_tests(self, identifier):
        return self._manage_self_tests(identifier)

    def look_up_by_id(self, identifier):
        """Find the collection to display self test results or run self tests for;
        display an error message if a collection with this ID turns out not to exist"""

        collection = Collection.by_id(self._db, identifier)
        if not collection:
            return NO_SUCH_COLLECTION

        self.protocol_class = self._find_protocol_class(collection)
        return collection

    def get_info(self, collection):
        """Compile information about this collection, including the results from the last time, if ever,
        that the self tests were run."""

        return dict(
            id=collection.id,
            name=collection.name,
            protocol=collection.protocol,
            parent_id=collection.parent_id,
            settings=dict(external_account_id=collection.external_account_id),
        )

    def _find_protocol_class(self, collection):
        """Figure out which protocol is providing books to this collection"""
        return self.registry.get(collection.protocol)

    def run_tests(self, collection):
        collection_protocol = collection.protocol or None

        if self.protocol_class:
            value = None
            if collection_protocol == OPDSImportMonitor.PROTOCOL:
                self.protocol_class = OPDSImportMonitor
                value, results = self.protocol_class.run_self_tests(
                    self._db, self.protocol_class, self._db, collection, OPDSImporter
                )
            elif issubclass(self.protocol_class, HasCollectionSelfTests):
                value, results = self.protocol_class.run_self_tests(
                    self._db, self.protocol_class, self._db, collection
                )

            return value
