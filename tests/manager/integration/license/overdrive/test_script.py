from __future__ import annotations

import csv
import os
from unittest.mock import MagicMock, Mock, patch

import pytest

from palace.manager.integration.license.overdrive.advantage import (
    OverdriveAdvantageAccount,
)
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.integration.license.overdrive.script import (
    GenerateOverdriveAdvantageAccountList,
    ImportCollection,
    ImportCollectionGroup,
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


class TestImportCollection:
    """Tests for the ImportCollection script."""

    @pytest.fixture
    def script(self, db: DatabaseTransactionFixture):
        """Create a script instance for testing."""
        return ImportCollection(db.session)

    def test_arg_parser_creates_correct_arguments(self):
        """Test arg_parser creates parser with collection-name and import-all arguments."""
        parser = ImportCollection.arg_parser()

        # Parse with both arguments
        args = parser.parse_args(
            [
                "--collection-name",
                "My Collection",
                "--import-all",
            ]
        )

        assert args.collection_name == "My Collection"
        assert args.import_all is True

    def test_arg_parser_import_all_defaults_to_false(self):
        """Test that import_all flag defaults to False when not provided."""
        parser = ImportCollection.arg_parser()

        args = parser.parse_args(["--collection-name", "My Collection"])

        assert args.collection_name == "My Collection"
        assert args.import_all is False

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.import_collection"
    )
    def test_do_run_kicks_off_import_task(
        self,
        mock_import_task: MagicMock,
        script: ImportCollection,
        db: DatabaseTransactionFixture,
    ):
        """Test do_run kicks off the import_collection Celery task."""
        # Create a collection
        collection = db.collection(
            name="Test Collection",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="test123"),
        )

        # Mock the delay method
        mock_import_task.delay.return_value = Mock()

        # Run the script
        script.do_run(cmd_args=["--collection-name", "Test Collection"])

        # Verify task was called with correct parameters
        mock_import_task.delay.assert_called_once_with(
            collection_id=collection.id,
            import_all=False,
        )

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.import_collection"
    )
    def test_do_run_with_import_all_flag(
        self,
        mock_import_task: MagicMock,
        script: ImportCollection,
        db: DatabaseTransactionFixture,
    ):
        """Test do_run with import_all flag set to True."""
        collection = db.collection(
            name="Test Collection",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="test123"),
        )

        mock_import_task.delay.return_value = Mock()

        # Run with import_all flag
        script.do_run(cmd_args=["--collection-name", "Test Collection", "--import-all"])

        # Verify import_all=True was passed
        mock_import_task.delay.assert_called_once_with(
            collection_id=collection.id,
            import_all=True,
        )

    def test_do_run_raises_error_for_nonexistent_collection(
        self, script: ImportCollection, db: DatabaseTransactionFixture
    ):
        """Test do_run raises ValueError if collection doesn't exist."""
        with pytest.raises(ValueError, match='No collection found named "Nonexistent"'):
            script.do_run(cmd_args=["--collection-name", "Nonexistent"])

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.import_collection"
    )
    def test_do_run_works_for_parent_collections(
        self,
        mock_import_task: MagicMock,
        script: ImportCollection,
        db: DatabaseTransactionFixture,
    ):
        """Test do_run works for parent (main) Overdrive collections."""
        # Create a parent collection
        collection = db.collection(
            name="Parent Collection",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="parent123"),
        )

        mock_import_task.delay.return_value = Mock()

        # Run the script
        script.do_run(cmd_args=["--collection-name", "Parent Collection"])

        # Verify task was called
        mock_import_task.delay.assert_called_once_with(
            collection_id=collection.id,
            import_all=False,
        )

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.import_collection"
    )
    def test_do_run_works_for_advantage_collections(
        self,
        mock_import_task: MagicMock,
        script: ImportCollection,
        db: DatabaseTransactionFixture,
    ):
        """Test do_run works for advantage (child) collections.

        Unlike ImportCollectionGroup, ImportCollection can be used on both
        parent and child collections. This is useful for importing a single
        advantage collection without its parent.
        """
        # Create parent and child
        parent = db.collection(
            name="Parent Collection",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="parent123"),
        )

        child = db.collection(
            name="Child Collection",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="child123"),
        )
        child.parent = parent
        db.session.flush()

        mock_import_task.delay.return_value = Mock()

        # Run with child collection - should work (unlike ImportCollectionGroup)
        script.do_run(cmd_args=["--collection-name", "Child Collection"])

        # Verify task was called with child collection
        mock_import_task.delay.assert_called_once_with(
            collection_id=child.id,
            import_all=False,
        )


class TestImportCollectionGroup:
    """Tests for the ImportCollectionGroup script."""

    @pytest.fixture
    def script(self, db: DatabaseTransactionFixture):
        """Create a script instance for testing."""
        return ImportCollectionGroup(db.session)

    def test_arg_parser_creates_correct_arguments(self):
        """Test arg_parser creates parser with collection-name and import-all arguments."""
        parser = ImportCollectionGroup.arg_parser()

        # Parse with both arguments
        args = parser.parse_args(
            [
                "--collection-name",
                "My Collection",
                "--import-all",
            ]
        )

        assert args.collection_name == "My Collection"
        assert args.import_all is True

    def test_arg_parser_import_all_defaults_to_false(self):
        """Test that import_all flag defaults to False when not provided."""
        parser = ImportCollectionGroup.arg_parser()

        args = parser.parse_args(["--collection-name", "My Collection"])

        assert args.collection_name == "My Collection"
        assert args.import_all is False

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.import_collection_group"
    )
    def test_do_run_kicks_off_import_collection_group_task(
        self,
        mock_import_group_task: MagicMock,
        script: ImportCollectionGroup,
        db: DatabaseTransactionFixture,
    ):
        """Test do_run kicks off the import_collection_group Celery task."""
        # Create a parent collection
        collection = db.collection(
            name="Test Collection",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="test123"),
        )

        # Mock the delay method
        mock_import_group_task.delay.return_value = Mock()

        # Run the script
        script.do_run(cmd_args=["--collection-name", "Test Collection"])

        # Verify task was called with correct parameters
        mock_import_group_task.delay.assert_called_once_with(
            collection_id=collection.id,
            import_all=False,
        )

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.import_collection_group"
    )
    def test_do_run_with_import_all_flag(
        self,
        mock_import_group_task: MagicMock,
        script: ImportCollectionGroup,
        db: DatabaseTransactionFixture,
    ):
        """Test do_run with import_all flag set to True."""
        collection = db.collection(
            name="Test Collection",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="test123"),
        )

        mock_import_group_task.delay.return_value = Mock()

        # Run with import_all flag
        script.do_run(cmd_args=["--collection-name", "Test Collection", "--import-all"])

        # Verify import_all=True was passed
        mock_import_group_task.delay.assert_called_once_with(
            collection_id=collection.id,
            import_all=True,
        )

    def test_do_run_raises_error_for_nonexistent_collection(
        self, script: ImportCollectionGroup, db: DatabaseTransactionFixture
    ):
        """Test do_run raises ValueError if collection doesn't exist."""
        with pytest.raises(ValueError, match='No collection found named "Nonexistent"'):
            script.do_run(cmd_args=["--collection-name", "Nonexistent"])

    def test_do_run_raises_error_for_advantage_collection(
        self, script: ImportCollectionGroup, db: DatabaseTransactionFixture
    ):
        """Test do_run raises ValueError if collection is an advantage (child) collection.

        ImportCollectionGroup should only be called on parent collections, not child
        advantage collections. If called on a child, it should suggest using the parent.
        """
        # Create parent collection
        parent = db.collection(
            name="Parent Collection",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="parent123"),
        )

        # Create child (advantage) collection
        child = db.collection(
            name="Child Collection",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="child123"),
        )
        child.parent = parent
        db.session.flush()

        # Try to run with the child collection - should raise error
        with pytest.raises(ValueError) as exc_info:
            script.do_run(cmd_args=["--collection-name", "Child Collection"])

        error_message = str(exc_info.value)
        assert "is an advantage collection" in error_message
        assert "Parent Collection" in error_message

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.import_collection_group"
    )
    def test_do_run_accepts_parent_collection_without_children(
        self,
        mock_import_group_task: MagicMock,
        script: ImportCollectionGroup,
        db: DatabaseTransactionFixture,
    ):
        """Test do_run works for parent collections even if they have no children."""
        # Create a parent collection with no children
        collection = db.collection(
            name="Parent Without Children",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="parent123"),
        )

        mock_import_group_task.delay.return_value = Mock()

        # Run the script - should succeed
        script.do_run(cmd_args=["--collection-name", "Parent Without Children"])

        # Verify task was called
        mock_import_group_task.delay.assert_called_once_with(
            collection_id=collection.id,
            import_all=False,
        )
