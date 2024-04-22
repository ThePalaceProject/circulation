from palace.manager.integration.goals import Goals
from tests.fixtures.database import DatabaseTransactionFixture


class TestIntegrationConfiguration:
    def test_explain(self, db: DatabaseTransactionFixture) -> None:
        integration = db.integration_configuration(
            name="test_integration", protocol="test_protocol", goal=Goals.DISCOVERY_GOAL
        )

        assert integration.explain() == [
            f"ID: {integration.id}",
            f"Name: {integration.name}",
            f"Protocol/Goal: {integration.protocol}/{integration.goal}",
        ]

        integration.settings_dict = {"key": "value"}
        assert integration.explain()[-2:] == ["Settings:", "  key: ********"]
        assert integration.explain(include_secrets=True)[-2:] == [
            "Settings:",
            "  key: value",
        ]

        integration.context = {
            "setting": "context value",
            "other_setting": "other context value",
        }
        assert integration.explain()[-3:] == [
            "Context:",
            "  other_setting: other context value",
            "  setting: context value",
        ]

        integration.self_test_results = {"result": "success"}
        assert integration.explain()[-2:] == [
            "Self Test Results:",
            "{'result': 'success'}",
        ]

        l1 = db.library(name="library 1", short_name="l1")
        l2 = db.library(name="library 2", short_name="l2")
        integration.libraries = [l1, l2]

        assert integration.explain()[-3:] == [
            "Configured libraries:",
            "  l1 - library 1",
            "  l2 - library 2",
        ]

        integration.for_library(l1).settings_dict = {"b": "one value", "a": "two value"}
        integration.for_library(l2).settings_dict = {"password": "super secret"}
        assert integration.explain()[-8:] == [
            "Configured libraries:",
            "  l1 - library 1",
            "    Settings:",
            "      a: two value",
            "      b: one value",
            "  l2 - library 2",
            "    Settings:",
            "      password: ********",
        ]

        assert integration.explain(include_secrets=True)[-3:] == [
            "  l2 - library 2",
            "    Settings:",
            "      password: super secret",
        ]
