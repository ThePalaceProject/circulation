from unittest.mock import patch

import pytest

from palace.manager.celery.tasks import boundless
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.scripts import ImportCollection
from tests.fixtures.database import DatabaseTransactionFixture


class TestBoundlessCollectionImportScript:

    @pytest.mark.parametrize(
        "import_all",
        [
            pytest.param(True, id="import all flag"),
            pytest.param(False, id="no import all flag"),
        ],
    )
    def test_boundless_import(self, db: DatabaseTransactionFixture, import_all: bool):
        collection = db.collection("test_collection", protocol=BoundlessApi)
        with patch.object(boundless, "import_collection") as import_collection:
            ImportCollection(db.session).do_run(
                [
                    "--collection-name",
                    collection.name,
                    "--import-all" if import_all else "",
                ]
            )
        import_collection.delay.assert_called_once_with(
            collection_id=collection.id, import_all=import_all
        )

    def test_boundless_import_collection_not_found(
        self, db: DatabaseTransactionFixture
    ):
        collection_name = "test_collection"
        with pytest.raises(
            ValueError, match=f'No collection found named "{collection_name}".'
        ):
            ImportCollection(db.session).do_run(["--collection-name", collection_name])
