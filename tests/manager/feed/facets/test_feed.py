from unittest.mock import patch

import pytest
from bidict import frozenbidict

from palace.manager.core.config import Configuration
from palace.manager.core.entrypoint import AudiobooksEntryPoint, EbooksEntryPoint
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.feed.facets.feed import (
    DefaultSortOrderFacets,
    Facets,
    FeaturedFacets,
)
from palace.manager.feed.worklist.base import WorkList
from palace.manager.search.filter import Filter
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolStatus,
    LicensePoolType,
)
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture


class TestFacets:
    @staticmethod
    def _configure_facets(library, enabled, default):
        """Set facet configuration for the given Library."""
        for key, values in list(enabled.items()):
            library.settings_dict[f"facets_enabled_{key}"] = values
        for key, value in list(default.items()):
            library.settings_dict[f"facets_default_{key}"] = value
        library._settings = None

    def test_facet_groups(self, db: DatabaseTransactionFixture):
        facets = Facets(
            db.default_library(),
            Facets.AVAILABLE_ALL,
            Facets.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
        )
        all_groups = list(facets.facet_groups)

        # By default, there are 10 facet transitions: two groups of three
        # and 2 datasource groups and 2 for collection names
        assert 10 == len(all_groups)

        # available=all and order=title are the selected
        # facets.
        selected = sorted(x[:2] for x in all_groups if x[-2] == True)
        assert [
            ("available", "all"),
            ("collectionName", "All"),
            ("distributor", "All"),
            ("order", "title"),
        ] == selected

        # Distributor and CollectionName facets are generated at runtime, they are not a setting value
        test_enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME: [Facets.ORDER_WORK_ID, Facets.ORDER_TITLE],
            Facets.AVAILABILITY_FACET_GROUP_NAME: [Facets.AVAILABLE_ALL],
        }
        test_default_facets = {
            Facets.ORDER_FACET_GROUP_NAME: Facets.ORDER_TITLE,
            Facets.AVAILABILITY_FACET_GROUP_NAME: Facets.AVAILABLE_ALL,
        }
        library = db.default_library()
        self._configure_facets(library, test_enabled_facets, test_default_facets)

        facets = Facets(db.default_library(), None, Facets.ORDER_TITLE, None, None)
        all_groups = list(facets.facet_groups)
        # We have disabled almost all the facets, so the list of
        # facet transitions includes only two items.
        #
        # 'Sort by title' was selected, and it shows up as the selected
        # item in this facet group.
        expect = [
            ["collectionName", "All", True],
            ["collectionName", db.default_collection().name, False],
            ["distributor", "All", True],
            ["distributor", "OPDS", False],
            ["order", "title", True],
            ["order", "work_id", False],
        ]
        assert expect == sorted(list(x[:2]) + [x[-2]] for x in all_groups)

    def test_default(self, db: DatabaseTransactionFixture):
        # Calling Facets.default() is like calling the constructor with
        # no arguments except the library.
        class Mock(Facets):
            def __init__(self, library, **kwargs):
                self.library = library
                self.kwargs = kwargs

        facets = Mock.default(db.default_library())
        assert db.default_library() == facets.library
        assert (
            dict(
                collection=None,
                availability=None,
                order=None,
                distributor=None,
                collection_name=None,
                entrypoint=None,
            )
            == facets.kwargs
        )

    def test_default_facet_is_always_available(self):
        # By definition, the default facet must be enabled. So if the
        # default facet for a given facet group is not enabled by the
        # current configuration, it's added to the beginning anyway.
        class MockConfiguration:
            def enabled_facets(self, facet_group_name):
                self.called_with = facet_group_name
                return ["facet1", "facet2"]

        class MockFacets(Facets):
            @classmethod
            def default_facet(cls, config, facet_group_name):
                cls.called_with = (config, facet_group_name)
                return "facet3"

        config = MockConfiguration()
        available = MockFacets.available_facets(config, "some facet group")

        # MockConfiguration.enabled_facets() was called to get the
        # enabled facets for the facet group.
        assert "some facet group" == config.called_with

        # Then Mock.default_facet() was called to get the default
        # facet for that group.
        assert (config, "some facet group") == MockFacets.called_with

        # Since the default facet was not found in the 'enabled'
        # group, it was added to the beginning of the list.
        assert ["facet3", "facet1", "facet2"] == available

        # If the default facet _is_ found in the 'enabled' group, it's
        # not added again.
        class MockFacets(Facets):
            @classmethod
            def default_facet(cls, config, facet_group_name):
                cls.called_with = (config, facet_group_name)
                return "facet2"

        available = MockFacets.available_facets(config, "some facet group")
        assert ["facet1", "facet2"] == available

    def test_default_availability(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        # Normally, the availability will be the library's default availability
        # facet.
        test_enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME: [Facets.ORDER_WORK_ID],
            Facets.AVAILABILITY_FACET_GROUP_NAME: [
                Facets.AVAILABLE_ALL,
                Facets.AVAILABLE_NOW,
            ],
        }
        test_default_facets = {
            Facets.ORDER_FACET_GROUP_NAME: Facets.ORDER_TITLE,
            Facets.AVAILABILITY_FACET_GROUP_NAME: Facets.AVAILABLE_ALL,
        }
        library = db.default_library()
        self._configure_facets(library, test_enabled_facets, test_default_facets)
        facets = Facets(library, None, None, None, None, None)
        assert Facets.AVAILABLE_ALL == facets.availability

        # However, if the library does not allow holds, we only show
        # books that are currently available.
        settings = library_fixture.settings(library)
        settings.allow_holds = False
        facets = Facets(library, None, None, None, None, None)
        assert Facets.AVAILABLE_NOW == facets.availability

        # Unless 'now' is not one of the enabled facets - then we keep
        # using the library's default.
        test_enabled_facets[Facets.AVAILABILITY_FACET_GROUP_NAME] = [
            Facets.AVAILABLE_ALL
        ]
        self._configure_facets(library, test_enabled_facets, test_default_facets)
        facets = Facets(library, None, None, None, None, None)
        assert Facets.AVAILABLE_ALL == facets.availability

    def test_facets_can_be_enabled_at_initialization(
        self, db: DatabaseTransactionFixture
    ):
        enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME: [
                Facets.ORDER_TITLE,
                Facets.ORDER_AUTHOR,
            ],
            Facets.AVAILABILITY_FACET_GROUP_NAME: [Facets.AVAILABLE_OPEN_ACCESS],
        }
        library = db.default_library()
        self._configure_facets(library, enabled_facets, {})

        # Create a new Facets object with these facets enabled,
        # no matter the Configuration.
        facets = Facets(
            db.default_library(),
            Facets.AVAILABLE_OPEN_ACCESS,
            Facets.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
            enabled_facets=enabled_facets,
        )
        all_groups = list(facets.facet_groups)
        expect = [["order", "author", False], ["order", "title", True]]
        assert expect == sorted(list(x[:2]) + [x[-2]] for x in all_groups)

    def test_facets_dont_need_a_library(self):
        enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME: [
                Facets.ORDER_TITLE,
                Facets.ORDER_AUTHOR,
            ],
            Facets.AVAILABILITY_FACET_GROUP_NAME: [Facets.AVAILABLE_OPEN_ACCESS],
        }

        facets = Facets(
            None,
            Facets.AVAILABLE_OPEN_ACCESS,
            Facets.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
            enabled_facets=enabled_facets,
        )
        all_groups = list(facets.facet_groups)
        expect = [["order", "author", False], ["order", "title", True]]
        assert expect == sorted(list(x[:2]) + [x[-2]] for x in all_groups)

    def test_items(self, db: DatabaseTransactionFixture):
        """Verify that Facets.items() returns all information necessary
        to recreate the Facets object.
        """
        facets = Facets(
            db.default_library(),
            Facets.AVAILABLE_ALL,
            Facets.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
            entrypoint=AudiobooksEntryPoint,
        )
        assert [
            ("available", Facets.AVAILABLE_ALL),
            ("collectionName", Facets.COLLECTION_NAME_ALL),
            ("distributor", Facets.DISTRIBUTOR_ALL),
            ("entrypoint", AudiobooksEntryPoint.INTERNAL_NAME),
            ("order", Facets.ORDER_TITLE),
        ] == sorted(facets.items())

    def test_default_order_ascending(self, db: DatabaseTransactionFixture):
        # Name-based facets are ordered ascending by default (A-Z).
        for order in (Facets.ORDER_TITLE, Facets.ORDER_AUTHOR):
            f = Facets(
                db.default_library(),
                availability=Facets.AVAILABLE_ALL,
                order=order,
                distributor=Facets.DISTRIBUTOR_ALL,
                collection_name=Facets.COLLECTION_NAME_ALL,
            )
            assert True == f.order_ascending

        # But the time-based facets are ordered descending by default
        # (newest->oldest)
        assert {Facets.ORDER_ADDED_TO_COLLECTION, Facets.ORDER_LAST_UPDATE} == set(
            Facets.ORDER_DESCENDING_BY_DEFAULT
        )
        for order in Facets.ORDER_DESCENDING_BY_DEFAULT:
            f = Facets(
                db.default_library(),
                availability=Facets.AVAILABLE_ALL,
                order=order,
                distributor=Facets.DISTRIBUTOR_ALL,
                collection_name=Facets.COLLECTION_NAME_ALL,
            )
            assert False == f.order_ascending

    def test_navigate(self, db: DatabaseTransactionFixture):
        """Test the ability of navigate() to move between slight
        variations of a FeaturedFacets object.
        """
        F = Facets

        ebooks = EbooksEntryPoint
        f = Facets(
            db.default_library(),
            F.AVAILABLE_ALL,
            F.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
            entrypoint=ebooks,
        )

        different_availability = f.navigate(availability=F.AVAILABLE_NOW)
        assert F.AVAILABLE_NOW == different_availability.availability
        assert F.ORDER_TITLE == different_availability.order
        assert F.DISTRIBUTOR_ALL == different_availability.distributor
        assert F.COLLECTION_NAME_ALL == different_availability.collection_name
        assert ebooks == different_availability.entrypoint

        different_order = f.navigate(order=F.ORDER_AUTHOR)
        assert F.AVAILABLE_ALL == different_order.availability
        assert F.ORDER_AUTHOR == different_order.order
        assert F.DISTRIBUTOR_ALL == different_order.distributor
        assert F.COLLECTION_NAME_ALL == different_order.collection_name
        assert ebooks == different_order.entrypoint

        audiobooks = AudiobooksEntryPoint
        different_entrypoint = f.navigate(entrypoint=audiobooks)
        assert F.AVAILABLE_ALL == different_entrypoint.availability
        assert F.ORDER_TITLE == different_entrypoint.order
        assert F.DISTRIBUTOR_ALL == different_entrypoint.distributor
        assert F.COLLECTION_NAME_ALL == different_entrypoint.collection_name
        assert audiobooks == different_entrypoint.entrypoint

        different_distributor = f.navigate(distributor=DataSource.AMAZON)
        assert F.AVAILABLE_ALL == different_distributor.availability
        assert F.ORDER_TITLE == different_distributor.order
        assert F.COLLECTION_NAME_ALL == different_distributor.collection_name
        assert DataSource.AMAZON == different_distributor.distributor

        different_collection_name = f.navigate(collection_name="Collection Name")
        assert F.AVAILABLE_ALL == different_collection_name.availability
        assert F.ORDER_TITLE == different_collection_name.order
        assert F.DISTRIBUTOR_ALL == different_collection_name.distributor
        assert "Collection Name" == different_collection_name.collection_name

    def test_from_request(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        settings = library_fixture.mock_settings()
        settings.enabled_entry_points = [
            AudiobooksEntryPoint.INTERNAL_NAME,
            EbooksEntryPoint.INTERNAL_NAME,
        ]
        library = library_fixture.library(settings=settings)

        config = library
        worklist = WorkList()
        worklist.initialize(library)

        m = Facets.from_request

        # Valid object using the default settings.
        default_order = config.default_facet(Facets.ORDER_FACET_GROUP_NAME)
        default_availability = config.default_facet(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        args: dict[str, str] = {}
        headers: dict = {}
        facets = m(library, library, args.get, headers.get, worklist)
        assert default_order == facets.order
        assert default_availability == facets.availability
        assert library == facets.library

        # The AudiobooksEntryPoint was selected as a default.
        assert AudiobooksEntryPoint == facets.entrypoint
        assert True == facets.entrypoint_is_default

        # Valid object using non-default settings.
        args = dict(
            order=Facets.ORDER_TITLE,
            available=Facets.AVAILABLE_OPEN_ACCESS,
            entrypoint=EbooksEntryPoint.INTERNAL_NAME,
        )
        facets = m(library, library, args.get, headers.get, worklist)
        assert Facets.ORDER_TITLE == facets.order
        assert Facets.AVAILABLE_OPEN_ACCESS == facets.availability
        assert library == facets.library
        assert EbooksEntryPoint == facets.entrypoint

        # Invalid order
        args = dict(order="no such order")
        invalid_order = m(library, library, args.get, headers.get, None)
        assert INVALID_INPUT.uri == invalid_order.uri
        assert (
            "I don't know how to order a feed by 'no such order'"
            == invalid_order.detail
        )

        # Invalid availability
        args = dict(available="no such availability")
        invalid_availability = m(library, library, args.get, headers.get, None)
        assert INVALID_INPUT.uri == invalid_availability.uri
        assert (
            "I don't understand the availability term 'no such availability'"
            == invalid_availability.detail
        )

    def test_from_request_gets_available_facets_through_hook_methods(
        self, db: DatabaseTransactionFixture
    ):
        # Available and default facets are determined by calling the
        # available_facets() and default_facets() methods. This gives
        # subclasses a chance to add extra facets or change defaults.
        class Mock(Facets):
            available_facets_calls: list[tuple] = []
            default_facet_calls: list[tuple] = []

            # For whatever reason, this faceting object allows only a
            # single setting for each facet group.
            mock_enabled = dict(
                order=[Facets.ORDER_TITLE],
                available=[Facets.AVAILABLE_OPEN_ACCESS],
                distributor=[Facets.DISTRIBUTOR_ALL],
                collectionName=[Facets.COLLECTION_NAME_ALL],
            )

            @classmethod
            def available_facets(cls, config, facet_group_name):
                cls.available_facets_calls.append((config, facet_group_name))
                return cls.mock_enabled[facet_group_name]

            @classmethod
            def default_facet(cls, config, facet_group_name):
                cls.default_facet_calls.append((config, facet_group_name))
                return cls.mock_enabled[facet_group_name][0]

        library = db.default_library()
        result = Mock.from_request(library, library, {}.get, {}.get, None)

        (
            order,
            available,
            distributor,
            collection_name,
        ) = Mock.available_facets_calls
        # available_facets was called three times, to ask the Mock class what it thinks
        # the options for order and availability should be.
        assert (library, "order") == order
        assert (library, "available") == available
        assert (library, "distributor") == distributor
        assert (library, "collectionName") == collection_name

        # default_facet was called three times, to ask the Mock class what it thinks
        # the default order, availability, and collection should be.
        (
            order_d,
            available_d,
            distributor_d,
            collection_name_d,
        ) = Mock.default_facet_calls
        assert (library, "order") == order_d
        assert (library, "available") == available_d
        assert (library, "distributor") == distributor_d
        assert (library, "collectionName") == collection_name_d

        # Finally, verify that the return values from the mocked methods were actually used.

        # The facets enabled during initialization are the limited
        # subset established by available_facets().
        assert Mock.mock_enabled == result.facets_enabled_at_init

        # The current values came from the defaults provided by default_facet().
        assert Facets.ORDER_TITLE == result.order
        assert Facets.AVAILABLE_OPEN_ACCESS == result.availability
        assert Facets.DISTRIBUTOR_ALL == result.distributor
        assert Facets.COLLECTION_NAME_ALL == result.collection_name

    def test_modify_search_filter(self, db: DatabaseTransactionFixture):
        # Test superclass behavior -- filter is modified by entrypoint.
        facets = Facets(
            db.default_library(),
            None,
            None,
            None,
            None,
            entrypoint=AudiobooksEntryPoint,
        )
        filter = Filter()
        facets.modify_search_filter(filter)
        assert [Edition.AUDIO_MEDIUM] == filter.media

        # Now test the subclass behavior.
        facets = Facets(
            db.default_library(),
            "some availability",
            order=Facets.ORDER_ADDED_TO_COLLECTION,
            distributor=DataSource.OVERDRIVE,
            collection_name=None,
            order_ascending="yep",
        )
        facets.modify_search_filter(filter)

        # The library's minimum featured quality is passed in.
        assert (
            db.default_library().settings.minimum_featured_quality
            == filter.minimum_featured_quality
        )

        # Availability and collection and distributor are propagated with no
        # validation.
        assert "some availability" == filter.availability
        assert [
            DataSource.lookup(db.session, DataSource.OVERDRIVE).id
        ] == filter.license_datasources

        # The sort order constant is converted to the name of an
        # Opensearch field.
        expect = Facets.SORT_ORDER_TO_OPENSEARCH_FIELD_NAME[
            Facets.ORDER_ADDED_TO_COLLECTION
        ]
        assert expect == filter.order
        assert "yep" == filter.order_ascending

        # Specifying an invalid sort order doesn't cause a crash, but you
        # don't get a sort order.
        facets = Facets(db.default_library(), None, "invalid order", None, None)
        filter = Filter()
        facets.modify_search_filter(filter)
        assert None == filter.order

        facets = Facets(
            db.default_library(), None, None, None, db.default_collection().name
        )
        filter = Filter()
        facets.modify_search_filter(filter)
        assert [db.default_collection().id] == filter.collection_ids

        # If you use a deprecated datasource, it is converted to the active one.
        filter = Filter()
        facets = Facets(
            db.default_library(),
            "some availability",
            order=Facets.ORDER_ADDED_TO_COLLECTION,
            distributor="deprecated datasource",
            collection_name=None,
        )
        with patch.object(
            DataSource,
            "DEPRECATED_NAMES",
            frozenbidict({"deprecated datasource": DataSource.GUTENBERG}),
        ):
            facets.modify_search_filter(filter)
        facets.modify_search_filter(filter)
        assert [
            DataSource.lookup(db.session, DataSource.GUTENBERG).id
        ] == filter.license_datasources

    def test_modify_database_query(self, db: DatabaseTransactionFixture):
        # Make sure that modify_database_query handles the various
        # reasons why a book might or might not be 'available'.
        open_access = db.work(with_open_access_download=True, title="open access")
        open_access.quality = 1
        unlimited_access = db.work(
            with_license_pool=True, unlimited_access=True, title="unlimited access"
        )

        available = db.work(with_license_pool=True, title="available")
        [pool] = available.license_pools
        pool.licenses_owned = 1
        pool.licenses_available = 1

        not_available = db.work(with_license_pool=True, title="not available")
        [pool] = not_available.license_pools
        pool.licenses_owned = 1
        pool.licenses_available = 0

        not_licensed = db.work(with_license_pool=True, title="exhausted license")
        [pool] = not_licensed.license_pools
        pool.licenses_owned = 0
        pool.licenses_available = 0
        pool.status = LicensePoolStatus.EXHAUSTED

        removed_oa = db.work(with_license_pool=True, title="removed open-access")
        [pool] = removed_oa.license_pools
        pool.status = LicensePoolStatus.REMOVED
        pool.type = LicensePoolType.UNLIMITED
        pool.open_access = True

        removed_ua = db.work(with_license_pool=True, title="removed unlimited-access")
        [pool] = removed_ua.license_pools
        pool.status = LicensePoolStatus.REMOVED
        pool.type = LicensePoolType.UNLIMITED
        pool.open_access = False

        qu = (
            db.session.query(Work)
            .join(Work.license_pools)
            .join(LicensePool.presentation_edition)
        )

        for availability, expect in [
            (
                Facets.AVAILABLE_NOW,
                {open_access, available, unlimited_access},
            ),
            (
                Facets.AVAILABLE_ALL,
                {open_access, available, not_available, unlimited_access},
            ),
            (Facets.AVAILABLE_NOT_NOW, {not_available}),
        ]:
            facets = Facets(db.default_library(), availability, None, None, None)
            modified = facets.modify_database_query(db.session, qu)
            assert (availability, set(modified)) == (availability, expect)

        # Test the case where there is an unknown availability facet.
        facets = Facets(
            db.default_library(), "invalid_availability_value", None, None, None
        )
        with pytest.raises(
            PalaceValueError,
            match="Unknown availability facet: invalid_availability_value",
        ):
            facets.modify_database_query(db.session, qu)


class TestDefaultSortOrderFacets:
    def _check_other_groups_not_changed(self, cls, config: Library):
        # Verify that nothing has changed for the
        # availability facet group.
        group_name = Facets.AVAILABILITY_FACET_GROUP_NAME
        assert Facets.available_facets(config, group_name) == cls.available_facets(
            config, group_name
        )
        assert Facets.default_facet(config, group_name) == cls.default_facet(
            config, group_name
        )

    def test_sort_order_rearrangement(self, db: DatabaseTransactionFixture):
        config = db.default_library()

        # Test the case where a DefaultSortOrderFacets does nothing but
        # rearrange the default sort orders.

        class TitleFirst(DefaultSortOrderFacets):
            DEFAULT_SORT_ORDER = Facets.ORDER_TITLE

        # In general, TitleFirst has the same options and
        # defaults as a normal Facets object.
        self._check_other_groups_not_changed(TitleFirst, config)

        # But the default sort order for TitleFirst is ORDER_TITLE.
        order = Facets.ORDER_FACET_GROUP_NAME
        assert TitleFirst.DEFAULT_SORT_ORDER == TitleFirst.default_facet(config, order)
        assert Facets.default_facet(config, order) != TitleFirst.DEFAULT_SORT_ORDER

        # TitleFirst has the same sort orders as Facets, but ORDER_TITLE
        # comes first in the list.
        default_orders = Facets.available_facets(config, order)
        title_first_orders = TitleFirst.available_facets(config, order)
        assert set(default_orders) == set(title_first_orders)
        assert Facets.ORDER_TITLE == title_first_orders[0]
        assert default_orders[0] != Facets.ORDER_TITLE

    def test_new_sort_order(self, db: DatabaseTransactionFixture):
        config = db.default_library()

        # Test the case where DefaultSortOrderFacets adds a sort order
        # not ordinarily supported.
        class SeriesFirst(DefaultSortOrderFacets):
            DEFAULT_SORT_ORDER = Facets.ORDER_SERIES_POSITION

        # In general, SeriesFirst has the same options and
        # defaults as a normal Facets object.
        self._check_other_groups_not_changed(SeriesFirst, config)

        # But its default sort order is ORDER_SERIES.
        order = Facets.ORDER_FACET_GROUP_NAME
        assert SeriesFirst.DEFAULT_SORT_ORDER == SeriesFirst.default_facet(
            config, order
        )
        assert Facets.default_facet(config, order) != SeriesFirst.DEFAULT_SORT_ORDER

        # Its list of sort orders is the same as Facets, except Series
        # has been added to the front of the list.
        default = Facets.available_facets(config, order)
        series = SeriesFirst.available_facets(config, order)
        assert [SeriesFirst.DEFAULT_SORT_ORDER] + default == series


class TestFeaturedFacets:
    def test_constructor(self):
        # Verify that constructor arguments are stored.
        entrypoint = object()
        facets = FeaturedFacets(1, entrypoint, entrypoint_is_default=True)
        assert 1 == facets.minimum_featured_quality
        assert entrypoint == facets.entrypoint
        assert True == facets.entrypoint_is_default

    def test_default(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        # Check how FeaturedFacets gets its minimum_featured_quality value.
        library1_settings = library_fixture.mock_settings()
        library1_settings.minimum_featured_quality = 0.22
        library1 = library_fixture.library(settings=library1_settings)
        library2_settings = library_fixture.mock_settings()
        library2_settings.minimum_featured_quality = 0.99
        library2 = library_fixture.library(settings=library2_settings)
        lane = db.lane(library=library2)

        # FeaturedFacets can be instantiated for a library...
        facets = FeaturedFacets.default(library1)
        assert (
            library1.settings.minimum_featured_quality
            == facets.minimum_featured_quality
        )

        # Or for a lane -- in which case it will take on the value for
        # the library associated with that lane.
        facets = FeaturedFacets.default(lane)
        assert (
            library2.settings.minimum_featured_quality
            == facets.minimum_featured_quality
        )

        # Or with nothing -- in which case the default value is used.
        facets = FeaturedFacets.default(None)
        assert (
            Configuration.DEFAULT_MINIMUM_FEATURED_QUALITY
            == facets.minimum_featured_quality
        )

    def test_navigate(self):
        # Test the ability of navigate() to move between slight
        # variations of a FeaturedFacets object.
        entrypoint = EbooksEntryPoint
        f = FeaturedFacets(1, entrypoint)

        different_entrypoint = f.navigate(entrypoint=AudiobooksEntryPoint)
        assert 1 == different_entrypoint.minimum_featured_quality
        assert AudiobooksEntryPoint == different_entrypoint.entrypoint

        different_quality = f.navigate(minimum_featured_quality=2)
        assert 2 == different_quality.minimum_featured_quality
        assert entrypoint == different_quality.entrypoint
