from unittest.mock import patch

import pytest

from palace.manager.celery.task import Task
from palace.manager.celery.tasks import opds1
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.opds.opds1.scripts import (
    Opds1ImportScript,
    Opds1ReaperScript,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestOpds1ImportScript:
    def test_do_run(self, db: DatabaseTransactionFixture) -> None:
        collection = db.collection(name="Test Collection", protocol=OPDSAPI)

        # No collection provided, should call import_all
        with patch.object(opds1, "import_all", autospec=Task) as mock_import_all:
            Opds1ImportScript(db=db.session).do_run(cmd_args=["--force"])
        mock_import_all.delay.assert_called_once_with(force=True)

        with patch.object(opds1, "import_all", autospec=Task) as mock_import_all:
            Opds1ImportScript(db=db.session).do_run(cmd_args=[])
        mock_import_all.delay.assert_called_once_with(force=False)

        # Collection provided, should call import_collection
        with patch.object(
            opds1, "import_collection", autospec=Task
        ) as mock_import_collection:
            Opds1ImportScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name, "--force"]
            )
        mock_import_collection.delay.assert_called_once_with(
            collection_id=collection.id,
            force=True,
        )

        with patch.object(
            opds1, "import_collection", autospec=Task
        ) as mock_import_collection:
            Opds1ImportScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name]
            )
        mock_import_collection.delay.assert_called_once_with(
            collection_id=collection.id,
            force=False,
        )


class TestOpds1ReaperScript:
    def test_do_run(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        collection = db.collection(name="Test Collection", protocol=OPDSAPI)

        # No collection provided, should raise an error
        with pytest.raises(SystemExit):
            Opds1ReaperScript(db=db.session).do_run(cmd_args=[])
        assert "You must specify at least one collection." in caplog.text

        # Collection provided, should call reaper_collection
        with patch.object(opds1, "import_and_reap_not_found_chord") as mock_reap:
            Opds1ReaperScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name]
            )
        mock_reap.assert_called_once_with(
            collection_id=collection.id,
            force=False,
        )
        mock_reap.return_value.delay.assert_called_once_with()
