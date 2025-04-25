from unittest.mock import patch

import pytest

from palace.manager.api.axis import Axis360API
from palace.manager.celery.tasks.axis import import_identifiers
from palace.manager.scripts import axis_import
from palace.manager.scripts.axis_import import ImportCollection
from tests.fixtures.database import DatabaseTransactionFixture


class TestAxisCollectionImportScript:

    def test_axis_import(self, db: DatabaseTransactionFixture):

        collection_name = "test_collection"
        collection = db.collection(collection_name, protocol=Axis360API)
        with patch.object(axis_import, "list_identifiers_for_import") as list_import:
            ImportCollection(db.session).do_run(
                ["--collection-name", collection.name, "--import-all"]
            )
            assert list_import.apply_async.assert_called_once

            assert list_import.apply_async.call_args[1] == {
                "kwargs": {"collection_id": collection.id, "import_all": True},
                "link": import_identifiers.s(collection_id=collection.id),
            }

    def test_axis_import_collection_not_found(self, db: DatabaseTransactionFixture):
        collection_name = "test_collection"
        with pytest.raises(ValueError) as e:
            ImportCollection(db.session).do_run(["--collection-name", collection_name])
            assert f'No collection found named "{collection_name}".' in str(e.value)
