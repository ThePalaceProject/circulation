from __future__ import annotations

import pytest

from palace.manager.api.overdrive.advantage import OverdriveAdvantageAccount
from palace.manager.api.overdrive.api import OverdriveAPI
from palace.manager.integration.goals import Goals
from tests.fixtures.overdrive import OverdriveAPIFixture


class TestOverdriveAdvantageAccount:
    def test_no_advantage_accounts(self, overdrive_api_fixture: OverdriveAPIFixture):
        """When there are no Advantage accounts, get_advantage_accounts()
        returns an empty list.
        """
        fixture = overdrive_api_fixture
        fixture.api.queue_collection_token()
        assert [] == list(fixture.api.get_advantage_accounts())

    def test_from_representation(self, overdrive_api_fixture: OverdriveAPIFixture):
        """Test the creation of OverdriveAdvantageAccount objects
        from Overdrive's representation of a list of accounts.
        """
        fixture = overdrive_api_fixture
        raw, data = fixture.sample_json("advantage_accounts.json")
        [ac1, ac2] = OverdriveAdvantageAccount.from_representation(raw)

        # The two Advantage accounts have the same parent library ID.
        assert "1225" == ac1.parent_library_id
        assert "1225" == ac2.parent_library_id

        # But they have different names and library IDs.
        assert "3" == ac1.library_id
        assert "The Other Side of Town Library" == ac1.name

        assert "9" == ac2.library_id
        assert "The Common Community Library" == ac2.name

    def test_to_collection(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Test that we can turn an OverdriveAdvantageAccount object into
        # a Collection object.
        fixture = overdrive_api_fixture
        transaction, session = (
            fixture.db,
            fixture.db.session,
        )

        account = OverdriveAdvantageAccount(
            "parent_id",
            "child_id",
            "Library Name",
            "token value",
        )

        # We can't just create a Collection object for this object because
        # the parent doesn't exist.
        with pytest.raises(ValueError) as excinfo:
            account.to_collection(session)
        assert "Cannot create a Collection whose parent does not already exist." in str(
            excinfo.value
        )

        # So, create a Collection to be the parent.
        parent = transaction.collection(
            name="Parent",
            protocol=OverdriveAPI,
            settings=transaction.overdrive_settings(external_account_id="parent_id"),
        )

        # Now it works.
        p, collection = account.to_collection(session)
        assert p == parent
        assert parent == collection.parent
        assert (
            collection.integration_configuration.settings_dict["external_account_id"]
            == account.library_id
        )
        assert OverdriveAPI.label() == collection.protocol
        assert Goals.LICENSE_GOAL == collection.integration_configuration.goal

        # To ensure uniqueness, the collection was named after its
        # parent.
        assert f"{parent.name} / {account.name}" == collection.name
