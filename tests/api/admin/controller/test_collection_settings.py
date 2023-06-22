from typing import Any
from unittest.mock import PropertyMock, create_autospec, patch

from api.admin.controller.collection_settings import CollectionSettingsController
from api.controller import CirculationManager
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from tests.fixtures.database import DatabaseTransactionFixture


class TestCollectionSettingsController:
    def test_duplicate_protocol_settings(self, db: DatabaseTransactionFixture):
        """Dedupe protocol settings using the last settings of the same value"""
        manager = create_autospec(spec=CirculationManager)
        manager._db = PropertyMock(return_value=db.session)

        class MockProviderAPI:
            NAME = "NAME"
            SETTINGS = [
                dict(key="k1", value="v1"),
                dict(key="k2", value="v2"),  # This should get overwritten
                dict(key="k2", value="v3"),  # Only this should remain
            ]

        controller = CollectionSettingsController(manager)
        with patch.object(
            controller, "registry", IntegrationRegistry[Any](Goals.LICENSE_GOAL)
        ) as registry:
            registry.register(MockProviderAPI, canonical=MockProviderAPI.NAME)
            protocols = controller._get_collection_protocols()

        k2_list = list(filter(lambda x: x["key"] == "k2", protocols[0]["settings"]))
        assert len(k2_list) == 1
        assert k2_list[0]["value"] == "v3"

        class MockProviderAPIMulti:
            NAME = "NAME"
            SETTINGS = [
                dict(key="k1", value="v0"),  # This should get overwritten
                dict(key="k1", value="v1"),  # Only this should remain
                dict(key="k2", value="v1"),  # This should get overwritten
                dict(key="k2", value="v2"),  # This should get overwritten
                dict(key="k2", value="v4"),  # Only this should remain
            ]

        with patch.object(
            controller, "registry", IntegrationRegistry[Any](Goals.LICENSE_GOAL)
        ) as registry:
            registry.register(MockProviderAPIMulti, canonical=MockProviderAPIMulti.NAME)
            protocols = controller._get_collection_protocols()

        k2_list = list(filter(lambda x: x["key"] == "k2", protocols[0]["settings"]))
        assert len(k2_list) == 1
        assert k2_list[0]["value"] == "v4"

        k1_list = list(filter(lambda x: x["key"] == "k1", protocols[0]["settings"]))
        assert len(k1_list) == 1
        assert k1_list[0]["value"] == "v1"
