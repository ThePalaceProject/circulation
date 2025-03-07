import datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from palace.manager.api.bibliotheca import BibliothecaAPI
from palace.manager.api.overdrive import OverdriveAPI, OverdriveLibrarySettings
from palace.manager.core.opds_import import OPDSImporterSettings
from palace.manager.integration.base import integration_settings_update
from palace.manager.integration.goals import Goals
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import CoverageRecord
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.integration import IntegrationLibraryConfiguration
from palace.manager.sqlalchemy.model.licensing import Hold, License, LicensePool, Loan
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class ExampleCollectionFixture:
    def __init__(self, database_transaction: DatabaseTransactionFixture):
        self.collection = database_transaction.collection(
            name="test collection", protocol=OverdriveAPI
        )
        self.database_fixture = database_transaction


@pytest.fixture()
def example_collection_fixture(
    db: DatabaseTransactionFixture,
) -> ExampleCollectionFixture:
    return ExampleCollectionFixture(db)


class TestCollection:
    def test_by_name_and_protocol(self, db: DatabaseTransactionFixture):
        name = "A name"
        protocol = OverdriveAPI.label()
        key = (name, protocol)

        # Cache is empty.
        cache = Collection._cache_from_session(db.session)
        assert len(cache.id) == 0
        assert len(cache.key) == 0

        collection1, is_new = Collection.by_name_and_protocol(
            db.session, name, OverdriveAPI.label()
        )
        assert True == is_new

        # Cache was populated
        assert len(cache.id) == 1
        assert len(cache.key) == 1
        assert cache.stats.hits == 0
        assert cache.stats.misses == 1

        collection2, is_new = Collection.by_name_and_protocol(
            db.session, name, OverdriveAPI.label()
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
            Collection.by_name_and_protocol(db.session, name, BibliothecaAPI.label())
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
                db.session, "another name", OverdriveAPI.label()
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

        overdrive = db.protocol_string(Goals.LICENSE_GOAL, OverdriveAPI)
        bibliotheca = db.protocol_string(Goals.LICENSE_GOAL, BibliothecaAPI)
        c1 = db.collection(db.fresh_str(), protocol=OverdriveAPI)
        c1.parent = test_collection
        c2 = db.collection(db.fresh_str(), protocol=BibliothecaAPI)
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

    def test_get_protocol(self, db: DatabaseTransactionFixture):
        test_collection = db.collection()
        integration = test_collection.integration_configuration
        test_collection.integration_configuration = None

        # A collection with no associated IntegrationConfiguration has no protocol.
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

        bibliotheca = db.protocol_string(Goals.LICENSE_GOAL, BibliothecaAPI)

        # Create a parent and a child collection, both with
        # protocol=Overdrive.
        child = db.collection(db.fresh_str(), protocol=OverdriveAPI)
        child.parent = test_collection

        # We can't change the child's protocol to a value that contradicts
        # the parent's protocol.
        child.protocol = db.protocol_string(Goals.LICENSE_GOAL, OverdriveAPI)

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
        assert child.protocol == bibliotheca

    def test_data_source(self, example_collection_fixture: ExampleCollectionFixture):
        db = example_collection_fixture.database_fixture

        opds = db.collection(settings=db.opds_settings(data_source="Foo"))
        bibliotheca = db.collection(protocol=BibliothecaAPI)

        # The rote data_source is returned for the obvious collection.
        assert bibliotheca.data_source is not None
        assert DataSource.BIBLIOTHECA == bibliotheca.data_source.name

        # The OPDS collection has a data source derived from its settings.
        assert opds.data_source is not None
        assert "Foo" == opds.data_source.name

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
        self,
        example_collection_fixture: ExampleCollectionFixture,
        db: DatabaseTransactionFixture,
    ):
        db = example_collection_fixture.database_fixture
        test_collection = example_collection_fixture.collection
        library = db.default_library()
        test_collection.associated_libraries.append(library)

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
        db.integration_library_configuration(
            test_collection.integration_configuration,
            library=library,
            settings=OverdriveLibrarySettings(
                audio_loan_duration=606, ebook_loan_duration=604
            ),
        )
        assert 604 == test_collection.default_loan_period(library)
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

        # The underlying value is controlled by an integration setting.
        test_collection.integration_configuration.settings_dict.update(
            {Collection.DEFAULT_RESERVATION_PERIOD_KEY: 954}
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
        test_collection.associated_libraries.append(library)

        data = test_collection.explain()
        assert [
            f"ID: {test_collection.integration_configuration.id}",
            "Name: test collection",
            "Protocol/Goal: Overdrive/Goals.LICENSE_GOAL",
            "Settings:",
            "  external_account_id: library_id",
            "  overdrive_client_key: ********",
            "  overdrive_client_secret: ********",
            "  overdrive_website_id: website_id",
            "Configured libraries:",
            "  only one - The only library",
        ] == data

        with_password = test_collection.explain(include_secrets=True)
        assert [
            f"ID: {test_collection.integration_configuration.id}",
            "Name: test collection",
            "Protocol/Goal: Overdrive/Goals.LICENSE_GOAL",
            "Settings:",
            "  external_account_id: library_id",
            "  overdrive_client_key: client_key",
            "  overdrive_client_secret: client_secret",
            "  overdrive_website_id: website_id",
            "Configured libraries:",
            "  only one - The only library",
        ] == with_password

        # If the collection is the child of another collection,
        # its parent is mentioned.
        child = db.collection(
            name="Child",
            settings=dict(external_account_id="id2"),
            protocol=OverdriveAPI,
        )
        child.parent = test_collection
        data = child.explain()
        assert [
            f"ID: {child.integration_configuration.id}",
            "Name: Child",
            "Protocol/Goal: Overdrive/Goals.LICENSE_GOAL",
            "Parent: test collection",
            "Settings:",
            "  external_account_id: id2",
        ] == data

    def test_disassociate_libraries(self, db: DatabaseTransactionFixture):

        # Here's a Collection.
        collection = db.collection()

        # It's associated with two different libraries.
        default_library = db.library()
        other_library = db.library()
        collection.associated_libraries.append(default_library)
        collection.associated_libraries.append(other_library)

        # It has an integration, which has some settings.
        integration = collection.integration_configuration
        integration.settings_dict = {"key": "value"}

        def update_library_settings(integration, library, settings):
            # And it has some library-specific settings.
            library_integration = integration.for_library(library)
            assert library_integration is not None
            library_integration.settings_dict = settings

        update_library_settings(integration, default_library, {"a": "b"})
        update_library_settings(integration, other_library, {"c": "d"})

        # Now, disassociate one of the libraries from the collection.
        collection.associated_libraries.remove(default_library)

        # It's gone.
        assert db.default_library() not in collection.associated_libraries
        assert collection not in default_library.associated_collections

        # The library-specific settings for that library have been deleted.
        library_config_ids = [
            l.library_id
            for l in db.session.execute(
                select(IntegrationLibraryConfiguration.library_id)
            )
        ]
        assert default_library.id not in library_config_ids

        # But the library-specific settings for the other library are still there.
        assert other_library in collection.associated_libraries
        assert other_library.id in library_config_ids
        assert collection.integration_configuration.library_configurations[
            0
        ].settings_dict == {"c": "d"}

        # We now disassociate all libraries from the collection.
        collection.associated_libraries.clear()

        # All the library-specific settings have been deleted.
        assert collection.integration_configuration.library_configurations == []
        assert collection.integration_configuration.libraries == []
        assert collection.associated_libraries == []

        # The integration settings are still there.
        assert collection.integration_configuration.settings_dict == {"key": "value"}

    # TODO: Pydantic and FreezeGun don't play well together, so we'll use
    #  dates well into the past and into the future to avoid any flakiness.
    @pytest.mark.parametrize(
        "activation_date, expiration_date, expect_active",
        (
            pytest.param(None, None, True, id="no start/end dates"),
            pytest.param(None, datetime.date(2222, 8, 31), True, id="no start date"),
            pytest.param(datetime.date(1960, 8, 1), None, True, id="no end date"),
            pytest.param(
                datetime.date(1960, 8, 1),
                datetime.date(2222, 8, 31),
                True,
                id="both dates",
            ),
            pytest.param(
                datetime.date(1960, 8, 1),
                datetime.date(1961, 8, 15),
                False,
                id="ends before today",
            ),
            pytest.param(
                datetime.date(2222, 9, 1),
                None,
                False,
                id="starts after today",
            ),
        ),
    )
    def test_collection_subscription(
        self,
        db: DatabaseTransactionFixture,
        activation_date: datetime.date | None,
        expiration_date: datetime.date | None,
        expect_active: bool,
    ):
        # Collection subscription settings.
        test_subscription_settings = (
            {"subscription_activation_date": activation_date} if activation_date else {}
        ) | (
            {"subscription_expiration_date": expiration_date} if expiration_date else {}
        )

        # Here's a Collection.
        collection = db.default_collection()
        assert collection.is_active is True

        # It's associated with two different libraries.
        assert db.default_library() in collection.associated_libraries
        other_library = db.library()
        collection.associated_libraries.append(other_library)

        # Initially there are no subscription settings.
        integration = collection.integration_configuration
        initial_settings = integration.settings_dict
        assert "subscription_activation_date" not in initial_settings
        assert "subscription_expiration_date" not in initial_settings

        # So our associated libraries are active by default.
        assert db.default_library() in collection.active_libraries
        assert other_library in collection.active_libraries

        # Now we apply the settings for the current case.
        integration_settings_update(
            OPDSImporterSettings, integration, test_subscription_settings, merge=True
        )

        # Whether the collection is active or not depends on the
        # subscription settings.
        assert collection.is_active == expect_active
        # And the libraries should have the active status that we expect.
        assert (db.default_library() in collection.active_libraries) == expect_active
        assert (other_library in collection.active_libraries) == expect_active

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

    @pytest.mark.parametrize(
        "is_inactive, active_collection_count",
        (
            pytest.param(True, 0, id="inactive collection"),
            pytest.param(False, 1, id="active collection"),
        ),
    )
    def test_delete(
        self,
        db: DatabaseTransactionFixture,
        services_fixture_wired: ServicesFixture,
        is_inactive: bool,
        active_collection_count: int,
    ):
        """Verify that Collection.delete will only operate on collections
        flagged for deletion, and that deletion cascades to all
        relevant related database objects.

        This test also demonstrates that the deletion works the same for active
        and inactive collections.
        """
        library = db.library()
        collection = db.collection(library=library)

        # It's associated with a library.
        assert collection.associated_libraries == [library]
        assert len(library.associated_collections) == 1
        # Even if we're going to test deletion of an inactive collection,
        # we should start with it being active, so that loans and holds
        # can be created. We'll make it inactive later.
        assert len(library.active_collections) == 1

        # It's got a Work that has a LicensePool, which has a License,
        # which has a loan.
        work = db.work(with_license_pool=True, collection=collection)
        [pool] = work.license_pools
        license = db.license(pool)
        patron = db.patron(library=library)
        loan, is_new = license.loan_to(patron)

        # The LicensePool also has a hold.
        patron2 = db.patron(library=library)
        hold, is_new = pool.on_hold_to(patron2)

        # And a CirculationEvent.
        get_one_or_create(
            db.session,
            CirculationEvent,
            license_pool=pool,
            type=CirculationEvent.DISTRIBUTOR_TITLE_ADD,
            start=utc_now(),
            end=utc_now(),
        )

        # There's a second Work which has _two_ LicensePools from two
        # different Collections -- the one we're about to delete and
        # another Collection.
        work2 = db.work(with_license_pool=True, collection=collection)
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

        # If we're meant to test an inactive collection, make it inactive.
        if is_inactive:
            db.make_collection_inactive(collection)

        assert len(library.associated_collections) == 1
        assert len(library.active_collections) == active_collection_count

        # delete() will not work on a collection that's not marked for
        # deletion.
        with pytest.raises(Exception) as excinfo:
            collection.delete()
        assert (
            f"Cannot delete {collection.name}: it is not marked for deletion."
            in str(excinfo.value)
        )

        # Delete the collection.
        collection.marked_for_deletion = True
        collection.delete(search_index=index)

        # It's gone.
        assert collection not in db.session.query(Collection).all()

        # The default library now has no collections.
        assert [] == library.associated_collections

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
        # then it will use the search index injected from the services
        # container.
        collection2.marked_for_deletion = True
        collection2.delete()

        # The search index was injected and told to remove the second work.
        services_fixture_wired.search_fixture.index_mock.remove_work.assert_called_once_with(
            work2
        )

        # We've now deleted every LicensePool created for this test.
        assert 0 == db.session.query(LicensePool).count()
        assert [] == work2.license_pools

    def test_redis_key(self, example_collection_fixture: ExampleCollectionFixture):
        collection = example_collection_fixture.collection

        # The key is based on the collection's ID.
        assert collection.redis_key() == f"Collection::{collection.id}"

        # If we know the collection's ID, we can get the key without a database query.
        assert Collection.redis_key_from_id(collection.id) == collection.redis_key()

        # A collection with no id raises an exception.
        collection_no_id = Collection()
        with pytest.raises(TypeError) as excinfo:
            collection_no_id.redis_key()
        assert "Collection must have an id to generate a redis key." in str(
            excinfo.value
        )
