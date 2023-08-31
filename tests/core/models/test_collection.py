import datetime
import json

import pytest

from core.config import Configuration
from core.model import create, get_one_or_create
from core.model.circulationevent import CirculationEvent
from core.model.collection import Collection
from core.model.configuration import (
    ConfigurationSetting,
    ExternalIntegration,
    ExternalIntegrationLink,
)
from core.model.coverage import CoverageRecord, WorkCoverageRecord
from core.model.customlist import CustomList
from core.model.datasource import DataSource
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.model.licensing import Hold, License, LicensePool, Loan
from core.model.work import Work
from core.util.datetime_helpers import utc_now
from core.util.string_helpers import base64
from tests.fixtures.database import DatabaseTransactionFixture


class ExampleCollectionFixture:
    collection: Collection
    database_fixture: DatabaseTransactionFixture

    def __init__(
        self, collection: Collection, database_transaction: DatabaseTransactionFixture
    ):
        self.collection = collection
        self.database_fixture = database_transaction

    def set_default_loan_period(self, medium, value, library=None):
        config = self.collection.integration_configuration
        if library is not None:
            config = config.for_library(library.id)
        DatabaseTransactionFixture.set_settings(
            config, **{self.collection.loan_period_key(medium): value}
        )


@pytest.fixture()
def example_collection_fixture(
    db: DatabaseTransactionFixture,
) -> ExampleCollectionFixture:
    c = db.collection(name="test collection", protocol=ExternalIntegration.OVERDRIVE)
    return ExampleCollectionFixture(c, db)


class TestCollection:
    def test_by_name_and_protocol(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        name = "A name"
        protocol = ExternalIntegration.OVERDRIVE
        key = (name, protocol)

        # Cache is empty.
        cache = Collection._cache_from_session(db.session)
        assert len(cache.id) == 0
        assert len(cache.key) == 0

        collection1, is_new = Collection.by_name_and_protocol(
            db.session, name, ExternalIntegration.OVERDRIVE
        )
        assert True == is_new

        # Cache was populated
        assert len(cache.id) == 1
        assert len(cache.key) == 1
        assert cache.stats.hits == 0
        assert cache.stats.misses == 1

        collection2, is_new = Collection.by_name_and_protocol(
            db.session, name, ExternalIntegration.OVERDRIVE
        )
        assert collection1 == collection2
        assert False == is_new

        # Item is in cache
        assert len(cache.id) == 1
        assert len(cache.key) == 1
        assert cache.stats.hits == 1
        assert cache.stats.misses == 1
        assert collection1 == cache.key[key]

        # You'll get an exception if you look up an existing name
        # but the protocol doesn't match.
        with pytest.raises(ValueError) as excinfo:
            Collection.by_name_and_protocol(
                db.session, name, ExternalIntegration.BIBLIOTHECA
            )
        assert 'Collection "A name" does not use protocol "Bibliotheca".' in str(
            excinfo.value
        )

    def test_by_protocol(self, example_collection_fixture: ExampleCollectionFixture):
        """Verify the ability to find all collections that implement
        a certain protocol.
        """
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA
        c1 = db.collection(db.fresh_str(), protocol=overdrive)
        c1.parent = test_collection
        c2 = db.collection(db.fresh_str(), protocol=bibliotheca)
        assert {test_collection, c1} == set(
            Collection.by_protocol(db.session, overdrive).all()
        )
        assert ([c2]) == Collection.by_protocol(db.session, bibliotheca).all()
        assert {test_collection, c1, c2} == set(
            Collection.by_protocol(db.session, None).all()
        )

        # A collection marked for deletion is filtered out.
        c1.marked_for_deletion = True
        assert [test_collection] == Collection.by_protocol(db.session, overdrive).all()

    def test_by_datasource(self, example_collection_fixture: ExampleCollectionFixture):
        """Collections can be found by their associated DataSource"""
        db = example_collection_fixture.database_fixture
        c1 = db.collection(data_source_name=DataSource.GUTENBERG)
        c2 = db.collection(data_source_name=DataSource.OVERDRIVE)

        # Using the DataSource name
        assert {c1} == set(
            Collection.by_datasource(db.session, DataSource.GUTENBERG).all()
        )

        # Using the DataSource itself
        overdrive = DataSource.lookup(db.session, DataSource.OVERDRIVE)
        assert {c2} == set(Collection.by_datasource(db.session, overdrive).all())

        # A collection marked for deletion is filtered out.
        c2.marked_for_deletion = True
        assert 0 == Collection.by_datasource(db.session, overdrive).count()

    def test_parents(self, example_collection_fixture: ExampleCollectionFixture):
        db = example_collection_fixture.database_fixture

        # Collections can return all their parents recursively.
        c1 = db.collection()
        assert [] == list(c1.parents)

        c2 = db.collection()
        c2.parent_id = c1.id
        assert [c1] == list(c2.parents)

        c3 = db.collection()
        c3.parent_id = c2.id
        assert [c2, c1] == list(c3.parents)

    def test_create_external_integration(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        # A newly created Collection has no associated ExternalIntegration.
        db = example_collection_fixture.database_fixture
        collection, ignore = get_one_or_create(
            db.session, Collection, name=db.fresh_str()
        )
        assert None == collection.external_integration_id
        with pytest.raises(ValueError) as excinfo:
            getattr(collection, "external_integration")
        assert "No known external integration for collection" in str(excinfo.value)

        # We can create one with create_external_integration().
        overdrive = ExternalIntegration.OVERDRIVE
        integration = collection.create_external_integration(protocol=overdrive)
        assert integration.id == collection.external_integration_id
        assert overdrive == integration.protocol

        # If we call create_external_integration() again we get the same
        # ExternalIntegration as before.
        integration2 = collection.create_external_integration(protocol=overdrive)
        assert integration == integration2

        # If we try to initialize an ExternalIntegration with a different
        # protocol, we get an error.
        with pytest.raises(ValueError) as excinfo:
            collection.create_external_integration(protocol="blah")
        assert (
            "Located ExternalIntegration, but its protocol (Overdrive) does not match desired protocol (blah)."
            in str(excinfo.value)
        )

    def test_unique_account_id(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture

        # Most collections work like this:
        overdrive = db.collection(
            external_account_id="od1", data_source_name=DataSource.OVERDRIVE
        )
        od_child = db.collection(
            external_account_id="odchild", data_source_name=DataSource.OVERDRIVE
        )
        od_child.parent = overdrive

        # The unique account ID of a primary collection is the
        # external account ID.
        assert "od1" == overdrive.unique_account_id

        # For children of those collections, the unique account ID is scoped
        # to the parent collection.
        assert "od1+odchild" == od_child.unique_account_id

        # Enki works a little differently. Enki collections don't have
        # an external account ID, because all Enki collections are
        # identical.
        enki = db.collection(data_source_name=DataSource.ENKI)

        # So the unique account ID is the name of the data source.
        assert DataSource.ENKI == enki.unique_account_id

        # A (currently hypothetical) library-specific subcollection of
        # the global Enki collection must have an external_account_id,
        # and its name is scoped to the parent collection as usual.
        enki_child = db.collection(
            external_account_id="enkichild", data_source_name=DataSource.ENKI
        )
        enki_child.parent = enki
        assert DataSource.ENKI + "+enkichild" == enki_child.unique_account_id

    def test_change_protocol(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA

        # Create a parent and a child collection, both with
        # protocol=Overdrive.
        child = db.collection(db.fresh_str(), protocol=overdrive)
        child.parent = test_collection

        # We can't change the child's protocol to a value that contradicts
        # the parent's protocol.
        child.protocol = overdrive

        def set_child_protocol():
            child.protocol = bibliotheca

        with pytest.raises(ValueError) as excinfo:
            set_child_protocol()
        assert (
            "Proposed new protocol (Bibliotheca) contradicts parent collection's protocol (Overdrive)."
            in str(excinfo.value)
        )

        # If we change the parent's protocol, the children are
        # automatically updated.
        test_collection.protocol = bibliotheca
        assert bibliotheca == child.protocol

    def test_data_source(self, example_collection_fixture: ExampleCollectionFixture):
        db = example_collection_fixture.database_fixture

        opds = db.collection()
        bibliotheca = db.collection(protocol=ExternalIntegration.BIBLIOTHECA)

        # The rote data_source is returned for the obvious collection.
        assert DataSource.BIBLIOTHECA == bibliotheca.data_source.name

        # The less obvious OPDS collection doesn't have a DataSource.
        assert None == opds.data_source

        # Trying to change the Bibliotheca collection's data_source does nothing.
        bibliotheca.data_source = DataSource.AXIS_360
        assert isinstance(bibliotheca.data_source, DataSource)
        assert DataSource.BIBLIOTHECA == bibliotheca.data_source.name

        # Trying to change the opds collection's data_source is fine.
        opds.data_source = DataSource.PLYMPTON
        assert isinstance(opds.data_source, DataSource)
        assert DataSource.PLYMPTON == opds.data_source.name

        # Resetting it to something else is fine.
        opds.data_source = DataSource.OA_CONTENT_SERVER
        assert isinstance(opds.data_source, DataSource)
        assert DataSource.OA_CONTENT_SERVER == opds.data_source.name

        # Resetting it to None is fine.
        opds.data_source = None
        assert None == opds.data_source

    def test_default_loan_period(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        library = db.default_library()
        library.collections.append(test_collection)
        assert isinstance(library.id, int)
        test_collection.integration_configuration.for_library(library.id, create=True)

        ebook = Edition.BOOK_MEDIUM
        audio = Edition.AUDIO_MEDIUM

        # The default when no value is set.
        assert (
            Collection.STANDARD_DEFAULT_LOAN_PERIOD
            == test_collection.default_loan_period(library, ebook)
        )

        assert (
            Collection.STANDARD_DEFAULT_LOAN_PERIOD
            == test_collection.default_loan_period(library, audio)
        )

        # Set a value, and it's used.
        example_collection_fixture.set_default_loan_period(ebook, 604, library=library)
        assert 604 == test_collection.default_loan_period(library)
        assert (
            Collection.STANDARD_DEFAULT_LOAN_PERIOD
            == test_collection.default_loan_period(library, audio)
        )

        example_collection_fixture.set_default_loan_period(audio, 606, library=library)
        assert 606 == test_collection.default_loan_period(library, audio)

    def test_default_reservation_period(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        library = db.default_library()
        # The default when no value is set.
        assert (
            Collection.STANDARD_DEFAULT_RESERVATION_PERIOD
            == test_collection.default_reservation_period
        )

        # Set a value, and it's used.
        test_collection.default_reservation_period = 601
        assert 601 == test_collection.default_reservation_period

        # The underlying value is controlled by a ConfigurationSetting.
        DatabaseTransactionFixture.set_settings(
            test_collection.integration_configuration,
            Collection.DEFAULT_RESERVATION_PERIOD_KEY,
            954,
        )
        assert 954 == test_collection.default_reservation_period

    def test_pools_with_no_delivery_mechanisms(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        session = db.session

        # Collection.pools_with_no_delivery_mechanisms returns a query
        # that finds all LicensePools in the Collection which are
        # missing delivery mechanisms.
        collection1 = db.default_collection()
        collection2 = db.collection()
        pool1 = db.licensepool(None, collection=collection1)
        pool2 = db.licensepool(None, collection=collection2)

        # At first, the query matches nothing, because
        # all LicensePools have delivery mechanisms.
        qu = collection1.pools_with_no_delivery_mechanisms
        assert [] == qu.all()

        # Let's delete all the delivery mechanisms.
        for pool in (pool1, pool2):
            for x in pool.delivery_mechanisms:
                session.delete(x)

        # Now the query matches LicensePools if they are in the
        # appropriate collection.
        assert [pool1] == qu.all()
        assert [pool2] == collection2.pools_with_no_delivery_mechanisms.all()

    def test_explain(self, example_collection_fixture: ExampleCollectionFixture):
        """Test that Collection.explain gives all relevant information
        about a Collection.
        """
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        library = db.default_library()
        library.name = "The only library"
        library.short_name = "only one"
        library.collections.append(test_collection)

        test_collection.external_account_id = "id"
        test_collection.integration_configuration.settings_dict = {
            "url": "url",
            "username": "username",
            "password": "password",
            "setting": "value",
        }

        data = test_collection.explain()
        assert [
            'Name: "test collection"',
            'Protocol: "Overdrive"',
            'Used by library: "only one"',
            'External account ID: "id"',
            'Setting "setting": "value"',
            'Setting "url": "url"',
            'Setting "username": "username"',
        ] == data

        with_password = test_collection.explain(include_secrets=True)
        assert 'Setting "password": "password"' in with_password

        # If the collection is the child of another collection,
        # its parent is mentioned.
        child = Collection(name="Child", external_account_id="id2")
        child.parent = test_collection

        child.create_external_integration(protocol=ExternalIntegration.OVERDRIVE)
        child.create_integration_configuration(protocol=ExternalIntegration.OVERDRIVE)
        data = child.explain()
        assert [
            'Name: "Child"',
            "Parent: test collection",
            'Protocol: "Overdrive"',
            'External account ID: "id2"',
        ] == data

    def test_metadata_identifier(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        # If the collection doesn't have its unique identifier, an error
        # is raised.
        pytest.raises(ValueError, getattr, test_collection, "metadata_identifier")

        def build_expected(protocol, unique_id):
            encode = base64.urlsafe_b64encode
            encoded = [encode(value) for value in [protocol, unique_id]]
            joined = ":".join(encoded)
            return encode(joined)

        # With a unique identifier, we get back the expected identifier.
        test_collection.external_account_id = "id"
        expected = build_expected(ExternalIntegration.OVERDRIVE, "id")
        assert expected == test_collection.metadata_identifier

        # If there's a parent, its unique id is incorporated into the result.
        child = db.collection(
            name="Child",
            protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=db.fresh_url(),
        )
        child.parent = test_collection
        expected = build_expected(
            ExternalIntegration.OPDS_IMPORT, "id+%s" % child.external_account_id
        )
        assert expected == child.metadata_identifier

        # If it's an OPDS_IMPORT collection with a url external_account_id,
        # closing '/' marks are removed.
        opds = db.collection(
            name="OPDS",
            protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=(db.fresh_url() + "/"),
        )
        assert isinstance(opds.external_account_id, str)
        expected = build_expected(
            ExternalIntegration.OPDS_IMPORT, opds.external_account_id[:-1]
        )
        assert expected == opds.metadata_identifier

    def test_from_metadata_identifier(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        data_source = "New data source"

        # A ValueError results if we try to look up using an invalid
        # identifier.
        with pytest.raises(ValueError) as excinfo:
            Collection.from_metadata_identifier(
                db.session, "not a real identifier", data_source=data_source
            )
        assert (
            "Metadata identifier 'not a real identifier' is invalid: Incorrect padding"
            in str(excinfo.value)
        )

        # Of if we pass in the empty string.
        with pytest.raises(ValueError) as excinfo:
            Collection.from_metadata_identifier(db.session, "", data_source=data_source)
        assert "No metadata identifier provided" in str(excinfo.value)

        # No new data source was created.
        def new_data_source():
            return DataSource.lookup(db.session, data_source)

        assert None == new_data_source()

        # If a mirrored collection doesn't exist, it is created.
        test_collection.external_account_id = "id"
        mirror_collection, is_new = Collection.from_metadata_identifier(
            db.session, test_collection.metadata_identifier, data_source=data_source
        )
        assert True == is_new
        assert test_collection.metadata_identifier == mirror_collection.name
        assert test_collection.protocol == mirror_collection.protocol

        # Because this isn't an OPDS collection, the external account
        # ID is not stored, the data source is the default source for
        # the protocol, and no new data source was created.
        assert None == mirror_collection.external_account_id
        assert DataSource.OVERDRIVE == mirror_collection.data_source.name
        assert None == new_data_source()

        # If the mirrored collection already exists, it is returned.
        collection = db.collection(external_account_id=db.fresh_url())
        mirror_collection = create(
            db.session, Collection, name=collection.metadata_identifier
        )[0]
        mirror_collection.create_external_integration(collection.protocol)
        mirror_collection.create_integration_configuration(collection.protocol)

        # Confirm that there's no external_account_id and no DataSource.
        # TODO I don't understand why we don't store this information,
        # even if only to keep it in an easy-to-read form.
        assert None == mirror_collection.external_account_id
        assert None == mirror_collection.data_source
        assert None == new_data_source()

        # Now try a lookup of an OPDS Import-type collection.
        result, is_new = Collection.from_metadata_identifier(
            db.session, collection.metadata_identifier, data_source=data_source
        )
        assert False == is_new
        assert mirror_collection == result
        # The external_account_id and data_source have been set now.
        assert collection.external_account_id == mirror_collection.external_account_id

        # A new DataSource object has been created.
        source = new_data_source()
        assert "New data source" == source.name
        assert source == mirror_collection.data_source

    def test_catalog_identifier(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        """#catalog_identifier associates an identifier with the catalog"""
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        identifier = db.identifier()
        test_collection.catalog_identifier(identifier)

        assert 1 == len(test_collection.catalog)
        assert identifier == test_collection.catalog[0]

    def test_catalog_identifiers(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        """#catalog_identifier associates multiple identifiers with a catalog"""
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        i1 = db.identifier()
        i2 = db.identifier()
        i3 = db.identifier()

        # One of the identifiers is already in the catalog.
        test_collection.catalog_identifier(i3)

        test_collection.catalog_identifiers([i1, i2, i3])

        # Now all three identifiers are in the catalog.
        assert sorted([i1, i2, i3]) == sorted(test_collection.catalog)

    def test_unresolved_catalog(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        # A regular schmegular identifier: untouched, pure.
        pure_id = db.identifier()

        # A 'resolved' identifier that doesn't have a work yet.
        # (This isn't supposed to happen, but jic.)
        source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        operation = "test-thyself"
        resolved_id = db.identifier()
        db.coverage_record(
            resolved_id, source, operation=operation, status=CoverageRecord.SUCCESS
        )

        # An unresolved identifier--we tried to resolve it, but
        # it all fell apart.
        unresolved_id = db.identifier()
        db.coverage_record(
            unresolved_id,
            source,
            operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE,
        )

        # An identifier with a Work already.
        id_with_work = db.work().presentation_edition.primary_identifier

        test_collection.catalog_identifiers(
            [pure_id, resolved_id, unresolved_id, id_with_work]
        )

        result = test_collection.unresolved_catalog(db.session, source.name, operation)

        # Only the failing identifier is in the query.
        assert [unresolved_id] == result.all()

    def test_disassociate_library(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        # Here's a Collection.
        collection = db.default_collection()

        # It's associated with two different libraries.
        assert db.default_library() in collection.libraries
        other_library = db.library()
        collection.libraries.append(other_library)

        # It has an ExternalIntegration, which has some settings.
        integration = collection.integration_configuration
        DatabaseTransactionFixture.set_settings(
            integration, **{"integration setting": "value2"}
        )
        setting2 = integration.for_library(db.default_library().id)
        DatabaseTransactionFixture.set_settings(
            setting2, **{"default_library+integration setting": "value2"}
        )
        setting3 = integration.for_library(other_library.id, create=True)
        DatabaseTransactionFixture.set_settings(
            setting3, **{"other_library+integration setting": "value3"}
        )

        # Now, disassociate one of the libraries from the collection.
        collection.disassociate_library(db.default_library())

        # It's gone.
        assert db.default_library() not in collection.libraries
        assert collection not in db.default_library().collections

        # Furthermore, ConfigurationSettings that configure that
        # Library's relationship to this Collection's
        # ExternalIntegration have been deleted.
        all_settings = db.session.query(IntegrationConfiguration).all()
        all_library_settings = db.session.query(IntegrationLibraryConfiguration).all()
        assert setting2 not in all_library_settings

        # The other library is unaffected.
        assert other_library in collection.libraries
        assert collection in other_library.collections
        assert setting3 in all_library_settings

        # As is the library-independent configuration of this Collection's
        # ExternalIntegration.
        assert integration in all_settings

        # Calling disassociate_library again is a no-op.
        collection.disassociate_library(db.default_library())
        assert db.default_library() not in collection.libraries

        # If you somehow manage to call disassociate_library on a Collection
        # that has no associated ExternalIntegration, an exception is raised.
        collection.integration_configuration_id = None
        with pytest.raises(ValueError) as excinfo:
            collection.disassociate_library(other_library)
        assert "No known integration library configuration for collection" in str(
            excinfo.value
        )

        collection.external_integration_id = None
        with pytest.raises(ValueError) as excinfo:
            collection.disassociate_library(other_library)
        assert "No known external integration for collection" in str(excinfo.value)

    def test_licensepools_with_works_updated_since(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        m = test_collection.licensepools_with_works_updated_since

        # Verify our ability to find LicensePools with works whose
        # OPDS entries were updated since a given time.
        w1 = db.work(with_license_pool=True)
        w2 = db.work(with_license_pool=True)
        w3 = db.work(with_license_pool=True)

        # An empty catalog returns nothing.
        timestamp = utc_now()
        assert [] == m(db.session, timestamp).all()

        test_collection.catalog_identifier(w1.license_pools[0].identifier)
        test_collection.catalog_identifier(w2.license_pools[0].identifier)

        # This Work is catalogued in another catalog and will never show up.
        collection2 = db.collection()
        in_other_catalog = db.work(with_license_pool=True, collection=collection2)
        collection2.catalog_identifier(in_other_catalog.license_pools[0].identifier)

        # When no timestamp is passed, all LicensePeols in the catalog
        # are returned, in order of the WorkCoverageRecord
        # timestamp on the associated Work.
        lp1, lp2 = m(db.session, None).all()
        assert w1 == lp1.work
        assert w2 == lp2.work

        # When a timestamp is passed, only LicensePools whose works
        # have been updated since then will be returned.
        [w1_coverage_record] = [
            c
            for c in w1.coverage_records
            if c.operation == WorkCoverageRecord.GENERATE_OPDS_OPERATION
        ]
        w1_coverage_record.timestamp = utc_now()
        assert [w1] == [x.work for x in m(db.session, timestamp)]

    def test_isbns_updated_since(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        i1 = db.identifier(identifier_type=Identifier.ISBN, foreign_id=db.isbn_take())
        i2 = db.identifier(identifier_type=Identifier.ISBN, foreign_id=db.isbn_take())
        i3 = db.identifier(identifier_type=Identifier.ISBN, foreign_id=db.isbn_take())
        i4 = db.identifier(identifier_type=Identifier.ISBN, foreign_id=db.isbn_take())

        timestamp = utc_now()

        # An empty catalog returns nothing..
        assert [] == test_collection.isbns_updated_since(db.session, None).all()

        # Give the ISBNs some coverage.
        content_cafe = DataSource.lookup(db.session, DataSource.CONTENT_CAFE)
        for isbn in [i2, i3, i1]:
            db.coverage_record(isbn, content_cafe)

        # Give one ISBN more than one coverage record.
        oclc = DataSource.lookup(db.session, DataSource.OCLC)
        i1_oclc_record = db.coverage_record(i1, oclc)

        def assert_isbns(expected, result_query):
            results = [r[0] for r in result_query]
            assert expected == results

        # When no timestamp is given, all ISBNs in the catalog are returned,
        # in order of their CoverageRecord timestamp.
        test_collection.catalog_identifiers([i1, i2])
        updated_isbns = test_collection.isbns_updated_since(db.session, None).all()
        assert_isbns([i2, i1], updated_isbns)

        # That CoverageRecord timestamp is also returned.
        i1_timestamp = updated_isbns[1][1]
        assert isinstance(i1_timestamp, datetime.datetime)
        assert i1_oclc_record.timestamp == i1_timestamp

        # When a timestamp is passed, only works that have been updated since
        # then will be returned.
        timestamp = utc_now()
        i1.coverage_records[0].timestamp = utc_now()
        updated_isbns = test_collection.isbns_updated_since(db.session, timestamp)
        assert_isbns([i1], updated_isbns)

        # Prepare an ISBN associated with a Work.
        work = db.work(with_license_pool=True)
        work.license_pools[0].identifier = i2
        i2.coverage_records[0].timestamp = utc_now()

        # ISBNs that have a Work will be ignored.
        updated_isbns = test_collection.isbns_updated_since(db.session, timestamp)
        assert_isbns([i1], updated_isbns)

    def test_custom_lists(self, example_collection_fixture: ExampleCollectionFixture):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection

        # A Collection can be associated with one or more CustomLists.
        list1, ignore = get_one_or_create(db.session, CustomList, name=db.fresh_str())
        list2, ignore = get_one_or_create(db.session, CustomList, name=db.fresh_str())
        test_collection.customlists = [list1, list2]
        assert 0 == len(list1.entries)
        assert 0 == len(list2.entries)

        # When a new pool is added to the collection and its presentation edition is
        # calculated for the first time, it's automatically added to the lists.
        work = db.work(collection=test_collection, with_license_pool=True)
        assert 1 == len(list1.entries)
        assert 1 == len(list2.entries)
        assert work == list1.entries[0].work
        assert work == list2.entries[0].work

        # Now remove it from one of the lists. If its presentation edition changes
        # again or its pool changes works, it's not added back.
        db.session.delete(list1.entries[0])
        db.session.commit()
        assert 0 == len(list1.entries)
        assert 1 == len(list2.entries)

        pool = work.license_pools[0]
        identifier = pool.identifier
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)
        staff_edition, ignore = Edition.for_foreign_id(
            db.session, staff_data_source, identifier.type, identifier.identifier
        )

        staff_edition.title = db.fresh_str()
        work.calculate_presentation()
        assert 0 == len(list1.entries)
        assert 1 == len(list2.entries)

        new_work = db.work(collection=test_collection)
        pool.work = new_work
        assert 0 == len(list1.entries)
        assert 1 == len(list2.entries)

    def test_restrict_to_ready_deliverable_works(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        """A partial test of restrict_to_ready_deliverable_works.

        This test covers the bit that excludes audiobooks from certain data sources.

        The other cases are tested indirectly in lane.py, but could use a more explicit test here.
        """
        db = example_collection_fixture.database_fixture
        # Create two audiobooks and one ebook.
        overdrive_audiobook = db.work(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            title="Overdrive Audiobook",
        )
        overdrive_audiobook.presentation_edition.medium = Edition.AUDIO_MEDIUM
        overdrive_ebook = db.work(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            title="Overdrive Ebook",
        )
        feedbooks_audiobook = db.work(
            data_source_name=DataSource.FEEDBOOKS,
            with_license_pool=True,
            title="Feedbooks Audiobook",
        )
        feedbooks_audiobook.presentation_edition.medium = Edition.AUDIO_MEDIUM

        def expect(qu, works):
            """Modify the query `qu` by calling
            restrict_to_ready_deliverable_works(), then verify that
            the query returns the works expected by `works`.
            """
            restricted_query = Collection.restrict_to_ready_deliverable_works(qu)
            expect_ids = [x.id for x in works]
            actual_ids = [x.id for x in restricted_query]
            assert set(expect_ids) == set(actual_ids)

        # Here's the setting which controls which data sources should
        # have their audiobooks excluded.
        setting = ConfigurationSetting.sitewide(
            db.session, Configuration.EXCLUDED_AUDIO_DATA_SOURCES
        )
        qu = (
            db.session.query(Work)
            .join(Work.license_pools)
            .join(Work.presentation_edition)
        )
        # When its value is set to the empty list, every work shows
        # up.
        setting.value = json.dumps([])
        expect(
            qu,
            [
                overdrive_ebook,
                overdrive_audiobook,
                feedbooks_audiobook,
            ],
        )
        # Putting a data source in the list excludes its audiobooks, but
        # not its ebooks.
        setting.value = json.dumps([DataSource.OVERDRIVE])
        expect(
            qu,
            [
                overdrive_ebook,
                feedbooks_audiobook,
            ],
        )
        setting.value = json.dumps([DataSource.OVERDRIVE, DataSource.FEEDBOOKS])
        expect(qu, [overdrive_ebook])

    def test_delete(self, example_collection_fixture: ExampleCollectionFixture):
        """Verify that Collection.delete will only operate on collections
        flagged for deletion, and that deletion cascades to all
        relevant related database objects.
        """
        db = example_collection_fixture.database_fixture
        # This collection is doomed.
        collection = db.default_collection()

        # It's associated with a library.
        assert db.default_library() in collection.libraries

        # It has an ExternalIntegration, which has some settings.
        integration = collection.external_integration
        setting1 = integration.set_setting("integration setting", "value2")
        setting2 = ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            "library+integration setting",
            db.default_library(),
            integration,
        )
        setting2.value = "value2"

        # Also it has links to another independent ExternalIntegration (S3 storage in this case).
        s3_storage = db.external_integration(
            ExternalIntegration.S3,
            ExternalIntegration.STORAGE_GOAL,
            libraries=[db.default_library()],
        )
        link1 = db.external_integration_link(
            integration,
            db.default_library(),
            s3_storage,
            ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS,
        )
        link2 = db.external_integration_link(
            integration,
            db.default_library(),
            s3_storage,
            ExternalIntegrationLink.COVERS,
        )

        integration.links.append(link1)
        integration.links.append(link2)

        # It's got a Work that has a LicensePool, which has a License,
        # which has a loan.
        work = db.work(with_license_pool=True)
        [pool] = work.license_pools
        license = db.license(pool)
        patron = db.patron()
        loan, is_new = license.loan_to(patron)

        # The LicensePool also has a hold.
        patron2 = db.patron()
        hold, is_new = pool.on_hold_to(patron2)

        # And a CirculationEvent.
        CirculationEvent.log(
            db.session, pool, CirculationEvent.DISTRIBUTOR_TITLE_ADD, 0, 1
        )

        # There's a second Work which has _two_ LicensePools from two
        # different Collections -- the one we're about to delete and
        # another Collection.
        work2 = db.work(with_license_pool=True)
        collection2 = db.collection()
        pool2 = db.licensepool(None, collection=collection2)
        work2.license_pools.append(pool2)

        record, _ = CoverageRecord.add_for(
            work.presentation_edition, collection.data_source, collection=collection
        )
        assert (
            CoverageRecord.lookup(
                work.presentation_edition, collection.data_source, collection=collection
            )
            != None
        )

        # Finally, here's a mock ExternalSearchIndex so we can track when
        # Works are removed from the search index.
        class MockExternalSearchIndex:
            removed = []

            def remove_work(self, work):
                self.removed.append(work)

        index = MockExternalSearchIndex()

        # delete() will not work on a collection that's not marked for
        # deletion.
        with pytest.raises(Exception) as excinfo:
            collection.delete()
        assert (
            "Cannot delete %s: it is not marked for deletion." % collection.name
            in str(excinfo.value)
        )

        # Delete the collection.
        collection.marked_for_deletion = True
        collection.delete(search_index=index)

        # It's gone.
        assert collection not in db.session.query(Collection).all()

        # The default library now has no collections.
        assert [] == db.default_library().collections

        # The collection based coverage record got deleted
        assert db.session.query(CoverageRecord).get(record.id) == None

        # The deletion of the Collection's sole LicensePool has
        # cascaded to Loan, Hold, License, and
        # CirculationEvent.
        assert [] == patron.loans
        assert [] == patron2.holds
        for cls in (Loan, Hold, License, CirculationEvent):
            assert [] == db.session.query(cls).all()

        # n.b. Annotations are associated with Identifier, not
        # LicensePool, so they can and should survive the deletion of
        # the Collection in which they were originally created.

        # The first Work has been deleted, since it lost all of its
        # LicensePools.
        assert [work2] == db.session.query(Work).all()

        # The second Work is still around, and it still has the other
        # LicensePool.
        assert [pool2] == work2.license_pools

        # Our search index was told to remove the first work (which no longer
        # has any LicensePools), but not the second.
        assert [work] == index.removed

        # The collection ExternalIntegration, its settings, and links to other integrations have been deleted.
        # The storage ExternalIntegration remains.
        external_integrations = db.session.query(ExternalIntegration).all()
        assert integration not in external_integrations
        assert s3_storage in external_integrations

        settings = db.session.query(ConfigurationSetting).all()
        for setting in (setting1, setting2):
            assert setting not in settings

        links = db.session.query(ExternalIntegrationLink).all()
        for link in (link1, link2):
            assert link not in links

        # If no search_index is passed into delete() (the default behavior),
        # we try to instantiate the normal ExternalSearchIndex object. Since
        # no search index is configured, this will raise an exception -- but
        # delete() will catch the exception and carry out the delete,
        # without trying to delete any Works from the search index.
        collection2.marked_for_deletion = True
        collection2.delete()

        # We've now deleted every LicensePool created for this test.
        assert 0 == db.session.query(LicensePool).count()
        assert [] == work2.license_pools


class TestCollectionForMetadataWrangler:
    """Tests that requirements to the metadata wrangler's use of Collection
    are being met by continued development on the Collection class.

    If any of these tests are failing, development will be required on the
    metadata wrangler to meet the needs of the new Collection class.
    """

    def test_only_name_is_required(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        """Test that only name is a required field on
        the Collection class.
        """
        db = example_collection_fixture.database_fixture
        collection = create(db.session, Collection, name="banana")[0]
        assert True == isinstance(collection, Collection)
