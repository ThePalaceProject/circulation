"""Tests for Bibliotheca operator scripts."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from palace.util.exceptions import PalaceValueError

from palace.manager.celery.tasks import bibliotheca
from palace.manager.integration.license.bibliotheca_scripts import (
    MonitorEventCollection,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.bibliotheca import MockBibliothecaAPI


class TestMonitorEventCollection:
    def test_import_all_queues_monitor_all_collections(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """--import-all dispatches monitor_all_collections."""
        with patch.object(bibliotheca, "monitor_all_collections") as mock_task:
            MonitorEventCollection(db.session).do_run(["--import-all"])
        mock_task.delay.assert_called_once_with()

    def test_collection_queues_monitor_collection(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """--collection <name> dispatches monitor_collection for that collection."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="My Bibliotheca"
        )
        with patch.object(bibliotheca, "monitor_collection") as mock_task:
            MonitorEventCollection(db.session).do_run(["--collection", collection.name])
        mock_task.delay.assert_called_once_with(collection_id=collection.id)

    def test_collection_not_found_raises(self, db: DatabaseTransactionFixture) -> None:
        """--collection with an unknown name raises PalaceValueError."""
        with pytest.raises(PalaceValueError, match='No collection found named "Ghost"'):
            MonitorEventCollection(db.session).do_run(["--collection", "Ghost"])

    def test_no_args_raises(self, db: DatabaseTransactionFixture) -> None:
        """Omitting both --collection and --import-all is an error."""
        with pytest.raises(SystemExit):
            MonitorEventCollection(db.session).do_run([])

    def test_both_args_raises(self, db: DatabaseTransactionFixture) -> None:
        """Specifying both --collection and --import-all is an error."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )
        with pytest.raises(SystemExit):
            MonitorEventCollection(db.session).do_run(
                ["--collection", collection.name, "--import-all"]
            )
