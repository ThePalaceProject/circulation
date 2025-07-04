from functools import partial
from unittest.mock import patch

import pytest
from bidict import frozenbidict
from sqlalchemy.orm.exc import NoResultFound

from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from tests.fixtures.database import DatabaseTransactionFixture


class TestDataSource:
    def test_lookup(self, db: DatabaseTransactionFixture):
        key = DataSource.GUTENBERG

        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)
        assert key == gutenberg.name
        assert True == gutenberg.offers_licenses
        assert key == gutenberg.cache_key()

        # Object has been loaded into cache.
        assert (gutenberg, False) == DataSource.by_cache_key(db.session, key, None)  # type: ignore[arg-type]

        # Now try creating a new data source.
        key = "New data source"
        new_source = DataSource.lookup(
            db.session, key, autocreate=True, offers_licenses=True
        )

        # A new data source has been created.
        assert key == new_source.name
        assert True == new_source.offers_licenses

        assert (new_source, False) == DataSource.by_cache_key(db.session, key, None)  # type: ignore[arg-type]

    @patch.object(
        DataSource, "DEPRECATED_NAMES", frozenbidict({"Old Name": "New Name"})
    )
    def test_lookup_by_deprecated_name(self, db: DatabaseTransactionFixture):
        session = db.session
        lookup = partial(DataSource.lookup, session, autocreate=True)

        # Looking up a deprecated name should return the new name.
        assert lookup("Old Name").name == "New Name"
        assert lookup("Old Name", autocreate=False).name == "New Name"
        assert lookup("New Name").name == "New Name"
        assert lookup("New Name", autocreate=False).name == "New Name"

        # The old name exists, and the new name does not
        session.delete(lookup("Old Name"))
        session.add(DataSource(name="Old Name"))
        session.commit()

        # In this case, the lookup will return the old name
        assert lookup("Old Name").name == "Old Name"
        assert lookup("Old Name", autocreate=False).name == "Old Name"
        assert lookup("New Name").name == "Old Name"
        assert lookup("New Name", autocreate=False).name == "Old Name"

    def test_lookup_returns_none_for_nonexistent_source(
        self, db: DatabaseTransactionFixture
    ):
        assert None == DataSource.lookup(
            db.session, "No such data source " + db.fresh_str()
        )

    def test_lookup_with_autocreate(self, db: DatabaseTransactionFixture):
        name = "Brand new data source " + db.fresh_str()
        new_source = DataSource.lookup(db.session, name, autocreate=True)
        assert name == new_source.name
        assert False == new_source.offers_licenses

        name = "New data source with licenses" + db.fresh_str()
        new_source = DataSource.lookup(
            db.session, name, autocreate=True, offers_licenses=True
        )
        assert True == new_source.offers_licenses

    def test_metadata_sources_for(self, db: DatabaseTransactionFixture):
        content_cafe = DataSource.lookup(db.session, DataSource.CONTENT_CAFE)
        isbn_metadata_sources = DataSource.metadata_sources_for(
            db.session, Identifier.ISBN
        )

        assert 1 == len(isbn_metadata_sources)
        assert [content_cafe] == isbn_metadata_sources

    def test_license_source_for(self, db: DatabaseTransactionFixture):
        identifier = db.identifier(Identifier.OVERDRIVE_ID)
        source = DataSource.license_source_for(db.session, identifier)
        assert DataSource.OVERDRIVE == source.name

    def test_license_source_for_string(self, db: DatabaseTransactionFixture):
        session = db.session
        source = DataSource.license_source_for(session, Identifier.BIBLIOTHECA_ID)
        assert DataSource.BIBLIOTHECA == source.name

    def test_license_source_fails_if_identifier_type_does_not_provide_licenses(
        self, db
    ):
        identifier = db.identifier(DataSource.MANUAL)
        pytest.raises(
            NoResultFound, DataSource.license_source_for, db.session, identifier
        )
