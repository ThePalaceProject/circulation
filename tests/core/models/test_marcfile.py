from datetime import datetime

import pytest

from core.model import MarcFile
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.mark.parametrize(
    "delete_library, delete_collection",
    [
        (False, True),
        (True, False),
        (True, True),
    ],
)
def test_delete_library_collection(
    db: DatabaseTransactionFixture, delete_library: bool, delete_collection: bool
) -> None:
    library = db.default_library()
    collection = db.default_collection()
    session = db.session

    file = MarcFile(
        library=library, collection=collection, key="key", created=datetime.now()
    )
    session.add(file)
    session.commit()

    if delete_library:
        session.delete(library)
    if delete_collection:
        session.delete(collection)
    session.commit()

    assert session.query(MarcFile).count() == 1

    assert (
        session.query(MarcFile).first().library is None if delete_library else library
    )
    assert (
        session.query(MarcFile).first().collection is None
        if delete_collection
        else collection
    )
