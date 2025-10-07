import pytest
from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.goals import Goals
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from tests.fixtures.database import DatabaseFixture, DatabaseTransactionFixture


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

        library_integration_l1 = integration.for_library(l1)
        assert library_integration_l1 is not None
        library_integration_l1.settings_dict = {"b": "one value", "a": "two value"}

        library_integration_l2 = integration.for_library(l2)
        assert library_integration_l2 is not None
        library_integration_l2.settings_dict = {"password": "super secret"}
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

    def test_context_update_basic(self, db: DatabaseTransactionFixture) -> None:
        """Test basic context_update functionality"""
        integration = db.integration_configuration(
            name="test_integration", protocol="test_protocol", goal=Goals.DISCOVERY_GOAL
        )
        integration.context = {"key1": "value1"}

        # Update with new keys
        integration.context_update({"key2": "value2", "key3": "value3"})

        # Verify the context was updated
        assert integration.context == {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        }

        # Verify it persists in the database
        db.session.expire(integration)
        db.session.refresh(integration)
        assert integration.context == {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        }

    def test_context_update_overwrite(self, db: DatabaseTransactionFixture) -> None:
        """Test that context_update overwrites existing keys"""
        integration = db.integration_configuration(
            name="test_integration", protocol="test_protocol", goal=Goals.DISCOVERY_GOAL
        )
        integration.context = {"key1": "value1", "key2": "value2"}

        # Update with overlapping keys
        integration.context_update({"key2": "new_value2", "key3": "value3"})

        # Verify the context was updated correctly
        assert integration.context == {
            "key1": "value1",
            "key2": "new_value2",
            "key3": "value3",
        }

    def test_context_update_no_session(self, db: DatabaseTransactionFixture) -> None:
        """Test that context_update raises an error if object is not bound to a session"""
        integration = IntegrationConfiguration(
            name="test_integration",
            protocol="test_protocol",
            goal=Goals.DISCOVERY_GOAL,
        )

        # Should raise PalaceValueError because object is not in a session
        with pytest.raises(PalaceValueError, match="not bound to a session"):
            integration.context_update({"key": "value"})

    def test_context_update_atomic(self, function_database: DatabaseFixture) -> None:
        """
        Test that context_update uses atomic database operations and prevents lost updates.

        This test simulates concurrent updates from two separate sessions to verify
        that the atomic JSONB || operator prevents the race condition where one
        update could overwrite another.

        Note: This test uses the function_database fixture to make sure we can manipulate
        sessions directly.
        """
        # Create the integration in one session and commit
        with Session(bind=function_database.engine) as setup_session:
            integration = IntegrationConfiguration(
                name="test_integration",
                protocol="test_protocol",
                goal=Goals.DISCOVERY_GOAL,
            )
            integration.context = {"initial": "value"}
            setup_session.add(integration)
            setup_session.commit()
            integration_id = integration.id

        # Simulate concurrent updates from two different sessions
        with (
            Session(bind=function_database.engine) as session1,
            Session(bind=function_database.engine) as session2,
        ):
            # Both sessions load the same integration
            integration1 = session1.get(IntegrationConfiguration, integration_id)
            integration2 = session2.get(IntegrationConfiguration, integration_id)

            assert integration1 is not None
            assert integration2 is not None

            # Both see the initial state
            assert integration1.context == {"initial": "value"}
            assert integration2.context == {"initial": "value"}

            # Session 1 updates with key1
            integration1.context_update({"key1": "value1"})
            session1.commit()

            # Session 2 updates with key2 (without seeing session1's changes yet)
            # With the old implementation, this would overwrite key1
            # With the atomic implementation, both updates are preserved
            integration2.context_update({"key2": "value2"})
            session2.commit()

        # Verify both updates are present (no lost updates)
        with Session(bind=function_database.engine) as verify_session:
            final_integration = verify_session.get(
                IntegrationConfiguration, integration_id
            )
            assert final_integration is not None

            # Both updates should be preserved due to atomic JSONB || operation
            assert final_integration.context == {
                "initial": "value",
                "key1": "value1",
                "key2": "value2",
            }
