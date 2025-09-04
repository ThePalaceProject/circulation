import pytest

from palace.manager.celery.utils import (
    ModelNotFoundError,
    load_from_id,
    validate_not_none,
)
from palace.manager.core.exceptions import PalaceTypeError
from palace.manager.sqlalchemy.model.collection import Collection
from tests.fixtures.database import DatabaseTransactionFixture


class TestLoadFromId:
    def test_load(self, db: DatabaseTransactionFixture) -> None:
        collection = db.collection()
        loaded = load_from_id(db.session, Collection, collection.id)
        assert isinstance(loaded, Collection)
        assert loaded is collection

    def test_load_not_found(self, db: DatabaseTransactionFixture) -> None:
        collection = db.collection()
        collection_id = collection.id
        db.session.delete(collection)

        with pytest.raises(
            ModelNotFoundError, match=f"Collection with id '{collection_id}' not found."
        ):
            load_from_id(db.session, Collection, collection_id)


class TestValidateNotNone:
    def test_validate_not_none(self) -> None:
        assert validate_not_none(1, "Should not be None") == 1
        assert validate_not_none("test", "Should not be None") == "test"

        with pytest.raises(PalaceTypeError, match="Should not be None"):
            validate_not_none(None, "Should not be None")
