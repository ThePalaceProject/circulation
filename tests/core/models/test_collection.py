import json
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from core.config import Configuration
from core.external_search import ExternalSearchIndex
from core.integration.goals import Goals
from core.model import get_one_or_create
from core.model.circulationevent import CirculationEvent
from core.model.collection import Collection
from core.model.configuration import ConfigurationSetting, ExternalIntegration
from core.model.coverage import CoverageRecord
from core.model.customlist import CustomList
from core.model.datasource import DataSource
from core.model.edition import Edition
from core.model.integration import IntegrationLibraryConfiguration
from core.model.licensing import Hold, License, LicensePool, Loan
from core.model.work import Work
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
    def test_by_name_and_protocol(self, db: DatabaseTransactionFixture):
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
        assert 'Integration "A name" does not use protocol "Bibliotheca".' in str(
            excinfo.value
        )

        # You'll get an exception if you look up an existing integration
        # but the goal doesn't match.
        db.integration_configuration(
            protocol=protocol, goal=Goals.DISCOVERY_GOAL, name="another name"
        )

        with pytest.raises(ValueError) as excinfo:
            Collection.by_name_and_protocol(
                db.session, "another name", ExternalIntegration.OVERDRIVE
            )

        assert 'Integration "another name" does not have goal "LICENSE_GOAL".' in str(
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

    def test_get_protocol(self, db: DatabaseTransactionFixture):
        test_collection = db.collection()
        integration = test_collection.integration_configuration
        test_collection.integration_configuration = None

        # A collection with no associated ExternalIntegration has no protocol.
        with pytest.raises(ValueError) as excinfo:
            getattr(test_collection, "protocol")

        assert "Collection has no integration configuration" in str(excinfo.value)

        integration.protocol = None
        test_collection.integration_configuration = integration

        # If a collection has an integration that doesn't have a protocol set,
        # it has no protocol, so we get an exception.
        with pytest.raises(ValueError) as excinfo:
            getattr(test_collection, "protocol")

        assert "Collection has integration configuration but no protocol" in str(
            excinfo.value
        )

        integration.protocol = "test protocol"
        assert test_collection.protocol == "test protocol"

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
        assert bibliotheca.data_source is not None
        assert DataSource.BIBLIOTHECA == bibliotheca.data_source.name

        # The less obvious OPDS collection doesn't have a DataSource.
        assert None == opds.data_source

        # Trying to change the Bibliotheca collection's data_source does nothing.
        bibliotheca.data_source = DataSource.AXIS_360  # type: ignore[assignment]
        assert isinstance(bibliotheca.data_source, DataSource)
        assert DataSource.BIBLIOTHECA == bibliotheca.data_source.name

        # Trying to change the opds collection's data_source is fine.
        opds.data_source = DataSource.PLYMPTON  # type: ignore[assignment]
        assert isinstance(opds.data_source, DataSource)
        assert DataSource.PLYMPTON == opds.data_source.name

        # Resetting it to something else is fine.
        opds.data_source = DataSource.OA_CONTENT_SERVER  # type: ignore[assignment]
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
        test_collection.libraries.append(library)

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

        library = db.default_library()
        library.name = "The only library"
        library.short_name = "only one"

        test_collection = example_collection_fixture.collection
        test_collection.libraries.append(library)

        test_collection.integration_configuration.settings_dict = {
            "url": "url",
            "username": "username",
            "password": "password",
            "setting": "value",
            "external_account_id": "id",
        }

        data = test_collection.explain()
        assert [
            'Name: "test collection"',
            'Protocol: "Overdrive"',
            'Used by library: "only one"',
            'Setting "external_account_id": "id"',
            'Setting "setting": "value"',
            'Setting "url": "url"',
            'Setting "username": "username"',
        ] == data

        with_password = test_collection.explain(include_secrets=True)
        assert 'Setting "password": "password"' in with_password

        # If the collection is the child of another collection,
        # its parent is mentioned.
        child = db.collection(
            name="Child",
            external_account_id="id2",
            protocol=ExternalIntegration.OVERDRIVE,
        )
        child.parent = test_collection
        data = child.explain()
        assert [
            'Name: "Child"',
            "Parent: test collection",
            'Protocol: "Overdrive"',
            'Setting "external_account_id": "id2"',
        ] == data

    def test_disassociate_libraries(
        self, example_collection_fixture: ExampleCollectionFixture
    ):
        db = example_collection_fixture.database_fixture
        # Here's a Collection.
        collection = db.default_collection()

        # It's associated with two different libraries.
        assert db.default_library() in collection.libraries
        other_library = db.library()
        collection.libraries.append(other_library)

        # It has an integration, which has some settings.
        integration = collection.integration_configuration
        integration.settings_dict = {"key": "value"}

        # And it has some library-specific settings.
        default_library_settings = integration.for_library(db.default_library())
        assert default_library_settings is not None
        default_library_settings.settings_dict = {"a": "b"}
        other_library_settings = integration.for_library(other_library)
        assert other_library_settings is not None
        other_library_settings.settings_dict = {"c": "d"}

        # Now, disassociate one of the libraries from the collection.
        collection.libraries.remove(db.default_library())

        # It's gone.
        assert db.default_library() not in collection.libraries
        assert collection not in db.default_library().collections

        # The library-specific settings for that library have been deleted.
        library_config_ids = [
            l.library_id
            for l in db.session.execute(
                select(IntegrationLibraryConfiguration.library_id)
            )
        ]
        assert db.default_library().id not in library_config_ids

        # But the library-specific settings for the other library are still there.
        assert other_library in collection.libraries
        assert other_library.id in library_config_ids
        assert collection.integration_configuration.library_configurations[
            0
        ].settings_dict == {"c": "d"}

        # We now disassociate all libraries from the collection.
        collection.libraries.clear()

        # All the library-specific settings have been deleted.
        assert collection.integration_configuration.library_configurations == []
        assert collection.integration_configuration.libraries == []
        assert collection.libraries == []

        # The integration settings are still there.
        assert collection.integration_configuration.settings_dict == {"key": "value"}

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
        index = MagicMock(spec=ExternalSearchIndex)

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
        index.remove_work.assert_called_once_with(work)

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
