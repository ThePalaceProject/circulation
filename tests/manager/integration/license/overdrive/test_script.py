from __future__ import annotations

import csv
import os
from unittest.mock import MagicMock, patch

from palace.manager.integration.license.overdrive.advantage import (
    OverdriveAdvantageAccount,
)
from palace.manager.integration.license.overdrive.script import (
    GenerateOverdriveAdvantageAccountList,
)
from palace.manager.sqlalchemy.model.collection import Collection
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.overdrive import OverdriveAPIFixture


class TestGenerateOverdriveAdvantageAccountList:
    def test_generate_od_advantage_account_list(
        self, db: DatabaseTransactionFixture, overdrive_api_fixture: OverdriveAPIFixture
    ):
        output_file_path = "test-output.csv"
        circ_manager_name = "circ_man_name"
        parent_library_name = "Parent"
        parent_od_library_id = "parent_id"
        child1_library_name = "child1"
        child1_advantage_library_id = "1"
        child1_token = "token1"
        child2_library_name = "child2"
        child2_advantage_library_id = "2"
        child2_token = "token2"
        client_key = "ck"
        client_secret = "cs"
        library_token = "lt"

        db.session.delete(overdrive_api_fixture.collection)
        library = db.library()
        parent: Collection = overdrive_api_fixture.create_collection(
            library,
            name=parent_library_name,
            library_id=parent_od_library_id,
            client_key=client_key,
            client_secret=client_secret,
        )
        child1: Collection = overdrive_api_fixture.create_collection(
            library,
            name=child1_library_name,
            library_id=child1_advantage_library_id,
        )
        child1.parent = parent
        overdrive_api = overdrive_api_fixture.create_mock_api(parent)
        mock_get_advantage_accounts = MagicMock()
        mock_get_advantage_accounts.return_value = [
            OverdriveAdvantageAccount(
                parent_od_library_id,
                child1_advantage_library_id,
                child1_library_name,
                child1_token,
            ),
            OverdriveAdvantageAccount(
                parent_od_library_id,
                child2_advantage_library_id,
                child2_library_name,
                child2_token,
            ),
        ]
        overdrive_api.get_advantage_accounts = mock_get_advantage_accounts
        overdrive_api._collection_token = library_token

        with patch.object(
            GenerateOverdriveAdvantageAccountList, "_create_overdrive_api"
        ) as create_od_api:
            create_od_api.return_value = overdrive_api
            GenerateOverdriveAdvantageAccountList(db.session).do_run(
                cmd_args=[
                    "--output-file-path",
                    output_file_path,
                    "--circulation-manager-name",
                    circ_manager_name,
                ]
            )

            with open(output_file_path, newline="") as csv_file:
                csvreader = csv.reader(csv_file)
                for index, row in enumerate(csvreader):
                    if index == 0:
                        assert "cm" == row[0]
                        assert "collection" == row[1]
                        assert "overdrive_library_id" == row[2]
                        assert "client_key" == row[3]
                        assert "client_secret" == row[4]
                        assert "library_token" == row[5]
                        assert "advantage_name" == row[6]
                        assert "advantage_id" == row[7]
                        assert "advantage_token" == row[8]
                        assert "already_configured" == row[9]
                    elif index == 1:
                        assert circ_manager_name == row[0]
                        assert parent_library_name == row[1]
                        assert parent_od_library_id == row[2]
                        assert client_key == row[3]
                        assert client_secret == row[4]
                        assert library_token == row[5]
                        assert child1_library_name == row[6]
                        assert child1_advantage_library_id == row[7]
                        assert child1_token == row[8]
                        assert "True" == row[9]
                    else:
                        assert circ_manager_name == row[0]
                        assert parent_library_name == row[1]
                        assert parent_od_library_id == row[2]
                        assert client_key == row[3]
                        assert client_secret == row[4]
                        assert library_token == row[5]
                        assert child2_library_name == row[6]
                        assert child2_advantage_library_id == row[7]
                        assert child2_token == row[8]
                        assert "False" == row[9]
                    last_index = index

            os.remove(output_file_path)
            assert last_index == 2
            overdrive_api.get_advantage_accounts.assert_called_once()
