from __future__ import annotations

import csv
import os
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from palace.util.datetime_helpers import datetime_utc, utc_now
from palace.util.exceptions import PalaceValueError
from palace.util.log import LogLevel

from palace.manager.celery.tasks import overdrive
from palace.manager.integration.license.overdrive.advantage import (
    OverdriveAdvantageAccount,
)
from palace.manager.integration.license.overdrive.api import (
    OverdriveAPI,
    OverdriveToken,
)
from palace.manager.integration.license.overdrive.script import (
    BackfillCollection,
    GenerateOverdriveAdvantageAccountList,
    ImportCollection,
    ImportCollectionGroup,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
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
        overdrive_api = overdrive_api_fixture.create_api(parent)
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
        overdrive_api._cached_collection_token = OverdriveToken(
            library_token, utc_now() + timedelta(hours=1)
        )

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
        mock_db_session = MagicMock()
        parser = ImportCollection.arg_parser(mock_db_session)

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
        mock_db_session = MagicMock()
        parser = ImportCollection.arg_parser(mock_db_session)

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
        mock_db_session = MagicMock()
        parser = ImportCollectionGroup.arg_parser(mock_db_session)

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
        mock_db_session = MagicMock()
        parser = ImportCollectionGroup.arg_parser(mock_db_session)

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


class TestBackfillCollection:
    """Tests for the BackfillCollection script."""

    @pytest.fixture
    def script(self, db: DatabaseTransactionFixture):
        return BackfillCollection(db.session)

    def test_arg_parser_requires_a_target(self):
        """Neither naming a collection nor asking for all of them is an error."""
        parser = BackfillCollection.arg_parser(MagicMock())
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_arg_parser_rejects_both_targets(self):
        parser = BackfillCollection.arg_parser(MagicMock())
        with pytest.raises(SystemExit):
            parser.parse_args(
                ["--collection-name", "My Collection", "--all-collections"]
            )

    def test_arg_parser_batch_size_defaults(self):
        parser = BackfillCollection.arg_parser(MagicMock())
        args = parser.parse_args(["--collection-name", "My Collection"])
        assert args.collection_name == "My Collection"
        assert args.all_collections is False
        assert args.batch_size == overdrive.BACKFILL_BATCH_SIZE

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.backfill_collection"
    )
    def test_do_run_queues_backfill_for_named_collection(
        self,
        mock_task: MagicMock,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
    ):
        collection = db.collection(protocol=OverdriveAPI)

        script.do_run(["--collection-name", collection.name, "--batch-size", "7"])

        mock_task.delay.assert_called_once_with(
            collection_id=collection.id,
            batch_size=7,
            max_titles=None,
            imported_since=None,
            include_suspected_magazines=False,
        )

    def test_arg_parser_new_options_default_to_off(self):
        parser = BackfillCollection.arg_parser(MagicMock())
        args = parser.parse_args(["--collection-name", "My Collection"])
        assert args.max_titles is None
        assert args.import_since is None
        assert args.dry_run is False
        assert args.include_suspected_magazines is False

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.backfill_collection"
    )
    def test_do_run_passes_max_titles_and_import_since(
        self,
        mock_task: MagicMock,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
    ):
        collection = db.collection(protocol=OverdriveAPI)

        script.do_run(
            [
                "--collection-name",
                collection.name,
                "--max-titles",
                "25",
                "--import-since",
                "2026-08-01",
            ]
        )

        mock_task.delay.assert_called_once_with(
            collection_id=collection.id,
            batch_size=overdrive.BACKFILL_BATCH_SIZE,
            max_titles=25,
            imported_since=datetime_utc(2026, 8, 1),
            include_suspected_magazines=False,
        )

    def test_do_run_rejects_a_useless_cap(
        self, script: BackfillCollection, db: DatabaseTransactionFixture
    ):
        collection = db.collection(protocol=OverdriveAPI)
        with pytest.raises(PalaceValueError, match="at least 1"):
            script.do_run(["--collection-name", collection.name, "--max-titles", "0"])

    @pytest.mark.parametrize("batch_size", ["0", "-1"])
    def test_do_run_rejects_a_useless_batch_size(
        self,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
        batch_size: str,
    ) -> None:
        """Zero would select nothing and call the backfill finished; negative is invalid SQL."""
        collection = db.collection(protocol=OverdriveAPI)
        with pytest.raises(PalaceValueError, match="at least 1"):
            script.do_run(
                ["--collection-name", collection.name, "--batch-size", batch_size]
            )

    def _magazine_and_real_book(
        self, db: DatabaseTransactionFixture, collection: Any
    ) -> None:
        """One suspected magazine and one ordinary damaged title, both unopenable."""
        for series in ("Monthly Thing", None):
            edition = db.edition(title="Monthly Thing", authors=["Author, An"])
            edition.series = series
            work = db.work(presentation_edition=edition)
            work.presentation_ready = True
            pool = db.licensepool(
                edition,
                collection=collection,
                with_open_access_download=True,
                set_edition_as_presentation=True,
                work=work,
            )
            for mechanism in list(pool.delivery_mechanisms):
                mechanism.delete()
        db.session.flush()

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.backfill_collection"
    )
    def test_suspected_magazines_are_skipped_by_default(
        self,
        mock_task: MagicMock,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
    ) -> None:
        """A title whose series is its own title is left alone unless asked for.

        We cannot make a magazine openable, so repairing one spends a blocking
        Overdrive request and rewrites both of its hashes to no effect.
        """
        collection = db.collection(protocol=OverdriveAPI)
        self._magazine_and_real_book(db, collection)

        script.do_run(["--collection-name", collection.name])
        assert mock_task.delay.call_args.kwargs["include_suspected_magazines"] is False

        script.do_run(
            ["--collection-name", collection.name, "--include-suspected-magazines"]
        )
        assert mock_task.delay.call_args.kwargs["include_suspected_magazines"] is True

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.backfill_collection"
    )
    def test_dry_run_always_reports_the_magazine_count(
        self,
        mock_task: MagicMock,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The count is reported either way, because it decides whether to run at all.

        A collection that is mostly magazines would spend nearly every request, and
        nearly every rewritten hash, on titles it cannot make openable.
        """
        caplog.set_level(LogLevel.info)
        collection = db.collection(protocol=OverdriveAPI)
        self._magazine_and_real_book(db, collection)

        script.do_run(["--collection-name", collection.name, "--dry-run"])
        assert (
            "2 damaged title(s) in this collection, of which 1 suspected magazine(s)"
            in caplog.text
        )
        assert "skipped by default" in caplog.text
        assert "1 would be processed" in caplog.text

        caplog.clear()
        script.do_run(
            [
                "--collection-name",
                collection.name,
                "--dry-run",
                "--include-suspected-magazines",
            ]
        )
        assert (
            "2 damaged title(s) in this collection, of which 1 suspected magazine(s)"
            in caplog.text
        )
        assert "included" in caplog.text
        assert "2 would be processed" in caplog.text
        mock_task.delay.assert_not_called()

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.backfill_collection"
    )
    def test_dry_run_reports_each_symptom_separately(
        self,
        mock_task: MagicMock,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The report has to show whether the three symptoms overlap.

        One symptom is expected to cover nearly every damaged title. The other two
        are worth keeping only if they find titles it misses, and a single combined
        total cannot answer that.
        """
        caplog.set_level(LogLevel.info)
        collection = db.collection(protocol=OverdriveAPI)

        def pool(publisher: str | None) -> Any:
            edition = db.edition(authors=["Author, An"])
            edition.publisher = publisher
            work = db.work(presentation_edition=edition)
            work.presentation_ready = True
            return db.licensepool(
                edition,
                collection=collection,
                with_open_access_download=True,
                set_edition_as_presentation=True,
                work=work,
            )

        # Unopenable, but otherwise complete: found only by the first symptom.
        unopenable = pool("A Publisher")
        for mechanism in list(unopenable.delivery_mechanisms):
            mechanism.delete()

        # Openable and searchable, but with no publisher: found only by the third.
        pool(None)
        db.session.flush()

        script.do_run(["--collection-name", collection.name, "--dry-run"])

        mock_task.delay.assert_not_called()
        assert "2 damaged title(s)" in caplog.text
        assert "1 unopenable" in caplog.text
        assert "1 found only by the other two symptoms" in caplog.text

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.backfill_collection"
    )
    def test_dry_run_reports_without_queuing_anything(
        self,
        mock_task: MagicMock,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """A dry run is the cheap way to size a run: it counts and queues nothing.

        Each title in a real run costs one blocking Overdrive request, so knowing the
        number beforehand is the difference between a measured rollout and a guess.
        """
        caplog.set_level(LogLevel.info)
        collection = db.collection(protocol=OverdriveAPI)
        for _ in range(3):
            db.licensepool(db.edition(), collection=collection)
        db.session.flush()

        script.do_run(["--collection-name", collection.name, "--dry-run"])

        mock_task.delay.assert_not_called()
        assert "3 damaged title(s)" in caplog.text
        assert "Nothing has been queued" in caplog.text

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.backfill_collection"
    )
    def test_dry_run_honours_the_cap(
        self,
        mock_task: MagicMock,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """The reported number is what would actually be processed, cap included."""
        caplog.set_level(LogLevel.info)
        collection = db.collection(protocol=OverdriveAPI)
        for _ in range(5):
            db.licensepool(db.edition(), collection=collection)
        db.session.flush()

        script.do_run(
            ["--collection-name", collection.name, "--dry-run", "--max-titles", "2"]
        )

        mock_task.delay.assert_not_called()
        assert "5 damaged title(s)" in caplog.text
        assert "2 would be processed" in caplog.text

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.backfill_collection"
    )
    @pytest.mark.parametrize("cap", [None, 5])
    def test_dry_run_overlap_is_not_distorted_by_the_cap(
        self,
        mock_task: MagicMock,
        cap: int | None,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """The shared-title figure describes the data, not the cap.

        The per-collection numbers are capped by --max-titles but the distinct title
        count is not, so deriving the overlap from the capped ones reported a smaller
        overlap than the catalogue has. It must come out the same either way.
        """
        caplog.set_level(LogLevel.info)
        parent = db.collection(protocol=OverdriveAPI)
        child = db.collection(protocol=OverdriveAPI)
        child.parent = parent

        # Four titles in both collections, two more in the parent alone: ten damaged
        # pools, six distinct titles, an overlap of four.
        for _ in range(4):
            shared = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
            for collection in (parent, child):
                LicensePool.for_foreign_id(
                    db.session,
                    DataSource.OVERDRIVE,
                    Identifier.OVERDRIVE_ID,
                    shared.identifier,
                    collection=collection,
                )
        for _ in range(2):
            db.licensepool(db.edition(), collection=parent)
        db.session.flush()

        args = ["--all-collections", "--dry-run"]
        if cap is not None:
            args += ["--max-titles", str(cap)]
        script.do_run(args)

        mock_task.delay.assert_not_called()
        assert "6 distinct title(s)" in caplog.text
        assert "That is 4 fewer than the 10 counted across" in caplog.text
        assert ("A cap of 5 per collection" in caplog.text) is (cap is not None)

    @patch(
        "palace.manager.integration.license.overdrive.script.overdrive.backfill_all_collections"
    )
    def test_do_run_queues_backfill_for_all_collections(
        self,
        mock_task: MagicMock,
        script: BackfillCollection,
        db: DatabaseTransactionFixture,
    ):
        script.do_run(["--all-collections"])
        mock_task.delay.assert_called_once_with(
            max_titles=None,
            imported_since=None,
            include_suspected_magazines=False,
        )

    def test_do_run_rejects_unknown_collection(
        self, script: BackfillCollection, db: DatabaseTransactionFixture
    ):
        with pytest.raises(PalaceValueError, match="No collection found named"):
            script.do_run(["--collection-name", "Does Not Exist"])

    def test_do_run_rejects_non_overdrive_collection(
        self, script: BackfillCollection, db: DatabaseTransactionFixture
    ):
        collection = db.collection()
        with pytest.raises(PalaceValueError, match="is not an Overdrive collection"):
            script.do_run(["--collection-name", collection.name])
