"""Tests for Bibliotheca operator scripts."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from palace.util.exceptions import PalaceValueError

from palace.manager.celery.tasks import bibliotheca
from palace.manager.integration.license.bibliotheca_purchase_record_importer import (
    DEFAULT_PURCHASE_RECORD_START_TIME,
)
from palace.manager.integration.license.bibliotheca_scripts import (
    ImportEventCollection,
    ImportPurchaseRecordCollection,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.bibliotheca import MockBibliothecaAPI


class TestImportEventCollection:
    def test_import_all_queues_import_all_collections(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """--import-all dispatches import_all_collections."""
        with patch.object(bibliotheca, "import_all_collections") as mock_task:
            ImportEventCollection(db.session).do_run(["--import-all"])
        mock_task.delay.assert_called_once_with()

    def test_collection_queues_import_collection(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """--collection <name> dispatches import_collection for that collection."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="My Bibliotheca"
        )
        with patch.object(bibliotheca, "import_collection") as mock_task:
            ImportEventCollection(db.session).do_run(["--collection", collection.name])
        mock_task.delay.assert_called_once_with(collection_id=collection.id)

    def test_collection_not_found_raises(self, db: DatabaseTransactionFixture) -> None:
        """--collection with an unknown name raises PalaceValueError."""
        with pytest.raises(PalaceValueError, match='No collection found named "Ghost"'):
            ImportEventCollection(db.session).do_run(["--collection", "Ghost"])

    def test_no_args_raises(self, db: DatabaseTransactionFixture) -> None:
        """Omitting both --collection and --import-all is an error."""
        with pytest.raises(SystemExit):
            ImportEventCollection(db.session).do_run([])

    def test_both_args_raises(self, db: DatabaseTransactionFixture) -> None:
        """Specifying both --collection and --import-all is an error."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )
        with pytest.raises(SystemExit):
            ImportEventCollection(db.session).do_run(
                ["--collection", collection.name, "--import-all"]
            )


class TestImportPurchaseRecordCollection:
    def test_import_all_queues_import_purchase_records_for_all_collections(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """--import-all dispatches import_purchase_records_for_all_collections."""
        with patch.object(
            bibliotheca, "import_purchase_records_for_all_collections"
        ) as mock_task:
            ImportPurchaseRecordCollection(db.session).do_run(["--import-all"])
        mock_task.delay.assert_called_once_with(force_reimport=False)

    def test_import_all_force_reimport_passes_flag(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """--import-all --force-reimport passes force_reimport=True to the task."""
        with patch.object(
            bibliotheca, "import_purchase_records_for_all_collections"
        ) as mock_task:
            ImportPurchaseRecordCollection(db.session).do_run(
                ["--import-all", "--force-reimport"]
            )
        mock_task.delay.assert_called_once_with(force_reimport=True)

    def test_collection_queues_import_purchase_records_by_collection(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """--collection <name> dispatches import_purchase_records_by_collection for that collection."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="My Bibliotheca"
        )
        with patch.object(
            bibliotheca, "import_purchase_records_by_collection"
        ) as mock_task:
            ImportPurchaseRecordCollection(db.session).do_run(
                ["--collection", collection.name]
            )
        mock_task.delay.assert_called_once_with(
            collection_id=collection.id, current_day=None, reset_timestamp=False
        )

    def test_collection_force_reimport_passes_start_date(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """--collection --force-reimport passes current_day=DEFAULT_PURCHASE_RECORD_START_TIME."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="My Bibliotheca"
        )
        with patch.object(
            bibliotheca, "import_purchase_records_by_collection"
        ) as mock_task:
            ImportPurchaseRecordCollection(db.session).do_run(
                ["--collection", collection.name, "--force-reimport"]
            )
        mock_task.delay.assert_called_once_with(
            collection_id=collection.id,
            current_day=DEFAULT_PURCHASE_RECORD_START_TIME,
            reset_timestamp=True,
        )

    def test_collection_not_found_raises(self, db: DatabaseTransactionFixture) -> None:
        """--collection with an unknown name raises PalaceValueError."""
        with pytest.raises(PalaceValueError, match='No collection found named "Ghost"'):
            ImportPurchaseRecordCollection(db.session).do_run(["--collection", "Ghost"])

    def test_no_args_raises(self, db: DatabaseTransactionFixture) -> None:
        """Omitting both --collection and --import-all is an error."""
        with pytest.raises(SystemExit):
            ImportPurchaseRecordCollection(db.session).do_run([])

    def test_both_args_raises(self, db: DatabaseTransactionFixture) -> None:
        """Specifying both --collection and --import-all is an error."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )
        with pytest.raises(SystemExit):
            ImportPurchaseRecordCollection(db.session).do_run(
                ["--collection", collection.name, "--import-all"]
            )
