from unittest.mock import patch

from palace.manager.celery.task import Task
from palace.manager.celery.tasks import opds_odl
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.scripts import OPDS2WithODLImportScript
from tests.fixtures.database import DatabaseTransactionFixture


class TestOPDS2WithODLImportScript:
    def test_do_run(self, db: DatabaseTransactionFixture) -> None:
        collection = db.collection(name="Test Collection", protocol=OPDS2WithODLApi)

        # No collection provided, should call import_all
        with patch.object(opds_odl, "import_all", autospec=Task) as mock_import_all:
            OPDS2WithODLImportScript(db=db.session).do_run(cmd_args=["--force"])
        mock_import_all.delay.assert_called_once_with(force=True)

        with patch.object(opds_odl, "import_all", autospec=Task) as mock_import_all:
            OPDS2WithODLImportScript(db=db.session).do_run(cmd_args=[])
        mock_import_all.delay.assert_called_once_with(force=False)

        # Collection provided, should call import_collection
        with patch.object(
            opds_odl, "import_collection", autospec=Task
        ) as mock_import_collection:
            OPDS2WithODLImportScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name, "--force"]
            )
        mock_import_collection.delay.assert_called_once_with(
            collection_id=collection.id,
            force=True,
        )

        with patch.object(
            opds_odl, "import_collection", autospec=Task
        ) as mock_import_collection:
            OPDS2WithODLImportScript(db=db.session).do_run(
                cmd_args=["--collection", collection.name]
            )
        mock_import_collection.delay.assert_called_once_with(
            collection_id=collection.id,
            force=False,
        )
