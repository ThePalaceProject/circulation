from unittest.mock import patch

import pytest

from palace.manager.celery.task import Task
from palace.manager.celery.tasks import opds2
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.license.opds.opds2.scripts import (
    OPDS2ImportScript,
    OPDS2ReaperScript,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestOPDS2ImportScript:
    def test_do_run(self, db: DatabaseTransactionFixture) -> None:
        collection = db.collection(name="Test Collection", protocol=OPDS2API)

        # No collection provided, should call import_all
        with patch.object(opds2, "import_all", autospec=Task) as mock_import_all:
            OPDS2ImportScript(db=db.session).do_run(cmd_args=["--force"])
        mock_import_all.delay.assert_called_once_with(force=True)

        with patch.object(opds2, "import_all", autospec=Task) as mock_import_all:
            OPDS2ImportScript(db=db.session).do_run(cmd_args=[])
        mock_import_all.delay.assert_called_once_with(force=False)

        # Collection provided, should call import_collection
        with patch.object(
            opds2, "import_collection", autospec=Task
        ) as mock_import_collection:
            OPDS2ImportScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name, "--force"]
            )
        mock_import_collection.delay.assert_called_once_with(
            collection_id=collection.id,
            force=True,
        )

        with patch.object(
            opds2, "import_collection", autospec=Task
        ) as mock_import_collection:
            OPDS2ImportScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name]
            )
        mock_import_collection.delay.assert_called_once_with(
            collection_id=collection.id,
            force=False,
        )


class TestOPDS2ReaperScript:
    def test_do_run(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        collection = db.collection(name="Test Collection", protocol=OPDS2API)

        # No collection provided, should raise an error
        with pytest.raises(SystemExit):
            OPDS2ReaperScript(db=db.session).do_run(cmd_args=[])
        assert "You must specify at least one collection." in caplog.text

        # Collection provided, should call reaper_collection
        with patch.object(opds2, "import_and_reap_not_found_chord") as mock_reap:
            OPDS2ReaperScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name]
            )
        mock_reap.assert_called_once_with(
            collection_id=collection.id,
            force=False,
        )
        mock_reap.return_value.delay.assert_called_once_with()
