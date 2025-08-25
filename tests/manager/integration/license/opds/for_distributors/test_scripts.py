from unittest.mock import patch

import pytest

from palace.manager.celery.task import Task
from palace.manager.celery.tasks import opds_for_distributors
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.for_distributors.scripts import (
    OpdsForDistributorsImportScript,
    OpdsForDistributorsReaperScript,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestOpdsForDistributorsImportScript:
    def test_do_run(self, db: DatabaseTransactionFixture) -> None:
        collection = db.collection(
            name="Test Collection", protocol=OPDSForDistributorsAPI
        )

        # No collection provided, should call import_all
        with patch.object(
            opds_for_distributors, "import_all", autospec=Task
        ) as mock_import_all:
            OpdsForDistributorsImportScript(db=db.session).do_run(cmd_args=["--force"])
        mock_import_all.delay.assert_called_once_with(force=True)

        with patch.object(
            opds_for_distributors, "import_all", autospec=Task
        ) as mock_import_all:
            OpdsForDistributorsImportScript(db=db.session).do_run(cmd_args=[])
        mock_import_all.delay.assert_called_once_with(force=False)

        # Collection provided, should call import_collection
        with patch.object(
            opds_for_distributors, "import_collection", autospec=Task
        ) as mock_import_collection:
            OpdsForDistributorsImportScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name, "--force"]
            )
        mock_import_collection.delay.assert_called_once_with(
            collection_id=collection.id,
            force=True,
        )

        with patch.object(
            opds_for_distributors, "import_collection", autospec=Task
        ) as mock_import_collection:
            OpdsForDistributorsImportScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name]
            )
        mock_import_collection.delay.assert_called_once_with(
            collection_id=collection.id,
            force=False,
        )


class TestOpdsForDistributorsReaperScript:
    def test_do_run(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        collection = db.collection(
            name="Test Collection", protocol=OPDSForDistributorsAPI
        )

        # No collection provided, we call the reap all task
        with patch.object(
            opds_for_distributors, "reap_all", autospec=Task
        ) as mock_reap_all:
            OpdsForDistributorsReaperScript(db=db.session).do_run(cmd_args=[])
        mock_reap_all.delay.assert_called_once_with(
            force=False,
        )

        # Collection provided, should call reaper_collection
        with patch.object(
            opds_for_distributors, "import_and_reap_not_found_chord"
        ) as mock_reap:
            OpdsForDistributorsReaperScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name]
            )
        mock_reap.assert_called_once_with(
            collection_id=collection.id,
            force=False,
        )
        mock_reap.return_value.delay.assert_called_once_with()
