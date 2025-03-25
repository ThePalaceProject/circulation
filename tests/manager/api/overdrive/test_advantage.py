from __future__ import annotations

import pytest

from palace.manager.api.overdrive.advantage import OverdriveAdvantageAccount
from palace.manager.api.overdrive.api import OverdriveAPI
from palace.manager.integration.goals import Goals
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.overdrive import OverdriveAPIFixture


class TestOverdriveAdvantageAccount:
    def test_no_advantage_accounts(self, overdrive_api_fixture: OverdriveAPIFixture):
        """When there are no Advantage accounts, get_advantage_accounts()
        returns an empty list.
        """
        api = overdrive_api_fixture.api
        overdrive_api_fixture.queue_collection_token()
        assert list(api.get_advantage_accounts()) == []

    def test_from_representation(self, overdrive_api_fixture: OverdriveAPIFixture):
        """Test the creation of OverdriveAdvantageAccount objects
        from Overdrive's representation of a list of accounts.
        """
        fixture = overdrive_api_fixture
        raw, data = fixture.sample_json("advantage_accounts.json")
        [ac1, ac2] = OverdriveAdvantageAccount.from_representation(raw.decode())

        # The two Advantage accounts have the same parent library ID.
        assert ac1.parent_library_id == "1225"
        assert ac2.parent_library_id == "1225"

        # But they have different names and library IDs.
        assert ac1.library_id == "3"
        assert ac1.name == "The Other Side of Town Library"

        assert ac2.library_id == "9"
        assert ac2.name == "The Common Community Library"

    def test_to_collection(self, db: DatabaseTransactionFixture):
        # Test that we can turn an OverdriveAdvantageAccount object into
        # a Collection object.
        account = OverdriveAdvantageAccount(
            "parent_id",
            "child_id",
            "Library Name",
            "token value",
        )

        # We can't just create a Collection object for this object because
        # the parent doesn't exist.
        with pytest.raises(
            ValueError,
            match="Cannot create a Collection whose parent does not already exist",
        ):
            account.to_collection(db.session)

        # So, create a Collection to be the parent.
        parent = db.collection(
            name="Parent",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="parent_id"),
        )

        # Now it works.
        p, collection = account.to_collection(db.session)
        assert p == parent
        assert parent == collection.parent
        assert (
            collection.integration_configuration.settings_dict["external_account_id"]
            == account.library_id
        )
        assert collection.protocol == OverdriveAPI.label()
        assert collection.integration_configuration.goal == Goals.LICENSE_GOAL

        # To ensure uniqueness, the collection was named after its
        # parent.
        assert collection.name == f"{parent.name} / {account.name}"
