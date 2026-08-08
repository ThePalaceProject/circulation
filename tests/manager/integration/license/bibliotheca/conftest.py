from __future__ import annotations

import pytest

from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import BibliothecaFilesFixture
from tests.mocks.bibliotheca import MockBibliothecaAPI


class BibliothecaAPITestFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        files: BibliothecaFilesFixture,
    ):
        self.files = files
        self.db = db
        self.collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )
        self.api = MockBibliothecaAPI(db.session, self.collection)


@pytest.fixture(scope="function")
def bibliotheca_fixture(
    db: DatabaseTransactionFixture,
    bibliotheca_files_fixture: BibliothecaFilesFixture,
) -> BibliothecaAPITestFixture:
    return BibliothecaAPITestFixture(db, bibliotheca_files_fixture)
