import pytest
from sqlalchemy.orm.exc import NoResultFound

from core.model.datasource import DataSource
from core.model.identifier import Identifier
from tests.fixtures.database import DatabaseTransactionFixture


class TestDataSource:
    def test_lookup(self, database_transaction: DatabaseTransactionFixture):
        session = database_transaction.session()
        key = DataSource.GUTENBERG

        gutenberg = DataSource.lookup(session, DataSource.GUTENBERG)
        assert key == gutenberg.name
        assert True == gutenberg.offers_licenses
        assert key == gutenberg.cache_key()

        # Object has been loaded into cache.
        assert (gutenberg, False) == DataSource.by_cache_key(session, key, None)

        # Now try creating a new data source.
        key = "New data source"
        new_source = DataSource.lookup(
            session, key, autocreate=True, offers_licenses=True
        )

        # A new data source has been created.
        assert key == new_source.name
        assert True == new_source.offers_licenses

        assert (new_source, False) == DataSource.by_cache_key(session, key, None)

    def test_lookup_by_deprecated_name(
        self, database_transaction: DatabaseTransactionFixture
    ):
        session = database_transaction.session()
        threem = DataSource.lookup(session, "3M")
        assert DataSource.BIBLIOTHECA == threem.name
        assert DataSource.BIBLIOTHECA != "3M"

    def test_lookup_returns_none_for_nonexistent_source(
        self, database_transaction: DatabaseTransactionFixture
    ):
        session = database_transaction.session()
        assert None == DataSource.lookup(
            session, "No such data source " + database_transaction.fresh_str()
        )

    def test_lookup_with_autocreate(
        self, database_transaction: DatabaseTransactionFixture
    ):
        session = database_transaction.session()
        name = "Brand new data source " + database_transaction.fresh_str()
        new_source = DataSource.lookup(session, name, autocreate=True)
        assert name == new_source.name
        assert False == new_source.offers_licenses

        name = "New data source with licenses" + database_transaction.fresh_str()
        new_source = DataSource.lookup(
            session, name, autocreate=True, offers_licenses=True
        )
        assert True == new_source.offers_licenses

    def test_metadata_sources_for(
        self, database_transaction: DatabaseTransactionFixture
    ):
        session = database_transaction.session()
        content_cafe = DataSource.lookup(session, DataSource.CONTENT_CAFE)
        isbn_metadata_sources = DataSource.metadata_sources_for(
            session, Identifier.ISBN
        )

        assert 1 == len(isbn_metadata_sources)
        assert [content_cafe] == isbn_metadata_sources

    def test_license_source_for(self, database_transaction: DatabaseTransactionFixture):
        session = database_transaction.session()
        identifier = database_transaction.identifier(Identifier.OVERDRIVE_ID)
        source = DataSource.license_source_for(session, identifier)
        assert DataSource.OVERDRIVE == source.name

    def test_license_source_for_string(
        self, database_transaction: DatabaseTransactionFixture
    ):
        session = database_transaction.session()
        source = DataSource.license_source_for(session, Identifier.THREEM_ID)
        assert DataSource.THREEM == source.name

    def test_license_source_fails_if_identifier_type_does_not_provide_licenses(
        self, database_transaction: DatabaseTransactionFixture
    ):
        session = database_transaction.session()
        identifier = database_transaction.identifier(DataSource.MANUAL)
        pytest.raises(NoResultFound, DataSource.license_source_for, session, identifier)
