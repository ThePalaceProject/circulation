from unittest.mock import patch

import pytest

from palace.manager.api.boundless.api import BoundlessApi
from palace.manager.celery.tasks import boundless
from palace.manager.celery.tasks.boundless import import_identifiers
from palace.manager.scripts.boundless_import import ImportCollection
from tests.fixtures.database import DatabaseTransactionFixture


class TestBoundlessCollectionImportScript:

    def test_boundless_import(self, db: DatabaseTransactionFixture):

        collection_name = "test_collection"
        collection = db.collection(collection_name, protocol=BoundlessApi)
        with patch.object(boundless, "list_identifiers_for_import") as list_import:
            ImportCollection(db.session).do_run(
                ["--collection-name", collection.name, "--import-all"]
            )
            assert list_import.apply_async.assert_called_once

            assert list_import.apply_async.call_args[1] == {
                "kwargs": {"collection_id": collection.id, "import_all": True},
                "link": import_identifiers.s(collection_id=collection.id),
            }

    def test_boundless_import_collection_not_found(
        self, db: DatabaseTransactionFixture
    ):
        collection_name = "test_collection"
        with pytest.raises(ValueError) as e:
            ImportCollection(db.session).do_run(["--collection-name", collection_name])
            assert f'No collection found named "{collection_name}".' in str(e.value)
