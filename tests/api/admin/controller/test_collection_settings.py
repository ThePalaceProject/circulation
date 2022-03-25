import json
from unittest.mock import PropertyMock, create_autospec

from api.admin.controller.collection_settings import CollectionSettingsController
from api.controller import CirculationManager
from api.proquest.importer import ProQuestOPDS2ImporterConfiguration
from api.saml.metadata.model import SAMLAttributeType
from core.model import ConfigurationSetting
from core.testing import DatabaseTest


class TestCollectionSettingsController(DatabaseTest):
    def test_load_settings_correctly_loads_menu_values(self):
        # Arrange
        manager = create_autospec(spec=CirculationManager)
        manager._db = PropertyMock(return_value=self._db)
        controller = CollectionSettingsController(manager)

        # We'll be using affiliation_attributes configuration setting defined in the ProQuest integration.
        affiliation_attributes_key = (
            ProQuestOPDS2ImporterConfiguration.affiliation_attributes.key
        )
        expected_affiliation_attributes = [
            SAMLAttributeType.eduPersonPrincipalName.name,
            SAMLAttributeType.eduPersonScopedAffiliation.name,
        ]
        protocol_settings = [
            ProQuestOPDS2ImporterConfiguration.affiliation_attributes.to_settings()
        ]
        collection_settings = None
        collection = self._default_collection

        # We need to explicitly set the value of "affiliation_attributes" configuration setting.
        ConfigurationSetting.for_externalintegration(
            affiliation_attributes_key, collection.external_integration
        ).value = json.dumps(expected_affiliation_attributes)

        # Act
        settings = controller.load_settings(
            protocol_settings, collection, collection_settings
        )

        # Assert
        assert True == (affiliation_attributes_key in settings)

        # We want to make sure that the result setting array contains a correct value in a list format.
        saved_affiliation_attributes = settings[affiliation_attributes_key]
        assert expected_affiliation_attributes == saved_affiliation_attributes

    def test_duplicate_protocol_settings(self):
        """Dedupe protocol settings using the last settings of the same value"""
        manager = create_autospec(spec=CirculationManager)
        manager._db = PropertyMock(return_value=self._db)

        class MockProviderAPI:
            NAME = "NAME"
            SETTINGS = [
                dict(key="k1", value="v1"),
                dict(key="k2", value="v2"),  # This should get overwritten
                dict(key="k2", value="v3"),  # Only this should remain
            ]

        controller = CollectionSettingsController(manager)
        controller.PROVIDER_APIS = [MockProviderAPI]
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

        controller.PROVIDER_APIS = [MockProviderAPIMulti]
        protocols = controller._get_collection_protocols()

        k2_list = list(filter(lambda x: x["key"] == "k2", protocols[0]["settings"]))
        assert len(k2_list) == 1
        assert k2_list[0]["value"] == "v4"

        k1_list = list(filter(lambda x: x["key"] == "k1", protocols[0]["settings"]))
        assert len(k1_list) == 1
        assert k1_list[0]["value"] == "v1"
