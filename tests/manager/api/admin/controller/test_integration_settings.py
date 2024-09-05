from functools import partial

import pytest

from palace.manager.api.admin.controller.integration_settings import (
    IntegrationSettingsController,
)
from palace.manager.integration.base import HasIntegrationConfiguration
from palace.manager.integration.goals import Goals
from palace.manager.integration.settings import BaseSettings
from palace.manager.service.integration_registry.base import IntegrationRegistry
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.util.problem_detail import ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture


class MockIntegrationBase(HasIntegrationConfiguration):
    @classmethod
    def label(cls) -> str:
        return cls.__name__

    @classmethod
    def description(cls) -> str:
        return "A mock integration"

    @classmethod
    def settings_class(cls) -> type[BaseSettings]:
        return BaseSettings


class MockIntegration1(MockIntegrationBase):
    ...


class MockIntegration2(MockIntegrationBase):
    ...


class MockIntegration3(MockIntegrationBase):
    ...


class MockController(IntegrationSettingsController[MockIntegrationBase]):
    ...


class IntegrationSettingsControllerFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self._db = db
        self.goal = Goals.PATRON_AUTH_GOAL
        self.registry: IntegrationRegistry[MockIntegrationBase] = IntegrationRegistry(
            self.goal
        )

        self.registry.register(MockIntegration1, canonical="mock_integration_1")
        self.registry.register(MockIntegration2, canonical="mock_integration_2")
        self.registry.register(
            MockIntegration3, aliases=["mock_integration_3", "mockIntegration3"]
        )

        self.controller = MockController(db.session, self.registry)

        self.integration_configuration = partial(
            db.integration_configuration, goal=self.goal
        )


@pytest.fixture
def integration_settings_controller_fixture(
    db: DatabaseTransactionFixture,
) -> IntegrationSettingsControllerFixture:
    return IntegrationSettingsControllerFixture(db)


class TestIntegrationSettingsController:
    def test_configured_service_info(
        self,
        integration_settings_controller_fixture: IntegrationSettingsControllerFixture,
    ):
        controller = integration_settings_controller_fixture.controller
        integration_configuration = (
            integration_settings_controller_fixture.integration_configuration
        )
        integration = integration_configuration("mock_integration_3")
        assert controller.configured_service_info(integration) == {
            "id": integration.id,
            "name": integration.name,
            "goal": integration_settings_controller_fixture.goal.value,
            "protocol": "MockIntegration3",
            "settings": integration.settings_dict,
        }

        # Integration protocol is not registered
        integration = integration_configuration("mock_integration_4")
        assert controller.configured_service_info(integration) is None

        # Integration has no protocol set
        integration = IntegrationConfiguration()
        assert controller.configured_service_info(integration) is None

    def test_get_existing_service(
        self,
        integration_settings_controller_fixture: IntegrationSettingsControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = integration_settings_controller_fixture.controller
        integration_configuration = (
            integration_settings_controller_fixture.integration_configuration
        )
        integration = integration_configuration("MockIntegration1")
        assert integration.id is not None
        assert controller.get_existing_service(integration.id) is integration
        assert (
            controller.get_existing_service(
                integration.id, protocol="mock_integration_1"
            )
            is integration
        )
        with pytest.raises(ProblemDetailException, match="Cannot change protocol"):
            controller.get_existing_service(
                integration.id, protocol="mock_integration_2"
            )
