import datetime
import logging
import random
from typing import List, Tuple
from unittest.mock import MagicMock, call

import pytest
from opensearchpy.exceptions import OpenSearchException
from sqlalchemy import and_, text

from core.classifier import Classifier
from core.config import Configuration
from core.entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EntryPoint,
    EverythingEntryPoint,
)
from core.external_search import Filter, WorkSearchResult, mock_search_index
from core.lane import (
    DatabaseBackedFacets,
    DatabaseBackedWorkList,
    DefaultSortOrderFacets,
    FacetConstants,
    Facets,
    FacetsWithEntryPoint,
    FeaturedFacets,
    Lane,
    Pagination,
    SearchFacets,
    TopLevelWorkList,
    WorkList,
)
from core.model import (
    CustomList,
    DataSource,
    Edition,
    Genre,
    Library,
    LicensePool,
    Work,
    WorkGenre,
    get_one_or_create,
    tuple_to_numericrange,
)
from core.model.collection import Collection
from core.model.configuration import ConfigurationSetting, ExternalIntegration
from core.problem_details import INVALID_INPUT
from core.util.datetime_helpers import utc_now
from core.util.opds_writer import OPDSFeed
from tests.core.mock import LogCaptureHandler
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixtureFake


class TestFacetsWithEntryPoint:
    class MockFacetConfig:
        """Pass this in when you call FacetsWithEntryPoint.from_request
        but you don't care which EntryPoints are configured.
        """

        entrypoints: List = []

    def test_items(self):
        ep = AudiobooksEntryPoint
        f = FacetsWithEntryPoint(ep)
        expect_items = (f.ENTRY_POINT_FACET_GROUP_NAME, ep.INTERNAL_NAME)
        assert [expect_items] == list(f.items())
        assert "%s=%s" % expect_items == f.query_string

        expect_items = [
            (f.ENTRY_POINT_FACET_GROUP_NAME, ep.INTERNAL_NAME),
        ]
        assert expect_items == list(f.items())

    def test_modify_database_query(self):
        class MockEntryPoint:
            def modify_database_query(self, _db, qu):
                self.called_with = (_db, qu)

        ep = MockEntryPoint()
        f = FacetsWithEntryPoint(ep)
        _db = object()
        qu = object()
        f.modify_database_query(_db, qu)
        assert (_db, qu) == ep.called_with

    def test_navigate(self):
        # navigate creates a new FacetsWithEntryPoint.

        old_entrypoint = object()
        kwargs = dict(extra_key="extra_value")
        facets = FacetsWithEntryPoint(
            old_entrypoint, entrypoint_is_default=True, **kwargs
        )
        new_entrypoint = object()
        new_facets = facets.navigate(new_entrypoint)

        # A new FacetsWithEntryPoint was created.
        assert isinstance(new_facets, FacetsWithEntryPoint)

        # It has the new entry point.
        assert new_entrypoint == new_facets.entrypoint

        # Since navigating from one Facets object to another is a choice,
        # the new Facets object is not using a default EntryPoint.
        assert False == new_facets.entrypoint_is_default

        # The keyword arguments used to create the original faceting
        # object were propagated to its constructor.
        assert kwargs == new_facets.constructor_kwargs

    def test_from_request(self):
        # from_request just calls the _from_request class method
        expect = object()

        class Mock(FacetsWithEntryPoint):
            @classmethod
            def _from_request(cls, *args, **kwargs):
                cls.called_with = (args, kwargs)
                return expect

        result = Mock.from_request(
            "library",
            "facet config",
            "get_argument",
            "get_header",
            "worklist",
            "default entrypoint",
            extra="extra argument",
        )

        # The arguments given to from_request were propagated to _from_request.
        args, kwargs = Mock.called_with
        assert (
            "facet config",
            "get_argument",
            "get_header",
            "worklist",
            "default entrypoint",
        ) == args
        assert dict(extra="extra argument") == kwargs

        # The return value of _from_request was propagated through
        # from_request.
        assert expect == result

    def test__from_request(self):
        # _from_request calls load_entrypoint() and instantiates
        # the class with the result.

        class MockFacetsWithEntryPoint(FacetsWithEntryPoint):
            # Mock load_entrypoint() to
            # return whatever values we have set up ahead of time.

            @classmethod
            def selectable_entrypoints(cls, facet_config):
                cls.selectable_entrypoints_called_with = facet_config
                return ["Selectable entrypoints"]

            @classmethod
            def load_entrypoint(cls, entrypoint_name, entrypoints, default=None):
                cls.load_entrypoint_called_with = (
                    entrypoint_name,
                    entrypoints,
                    default,
                )
                return cls.expect_load_entrypoint

        # Mock the functions that pull information out of an HTTP
        # request.

        # EntryPoint.load_entrypoint pulls the facet group name and
        # the maximum cache age out of the 'request' and passes those
        # values into load_entrypoint()
        def get_argument(key, default):
            if key == Facets.ENTRY_POINT_FACET_GROUP_NAME:
                return "entrypoint name from request"

        # FacetsWithEntryPoint.load_entrypoint does not use
        # get_header().
        def get_header(name):
            raise Exception("I'll never be called")

        config = self.MockFacetConfig
        mock_worklist = object()
        default_entrypoint = object()

        def m():
            return MockFacetsWithEntryPoint._from_request(
                config,
                get_argument,
                get_header,
                mock_worklist,
                default_entrypoint=default_entrypoint,
                extra="extra kwarg",
            )

        # First, test failure. If load_entrypoint() returns a
        # ProblemDetail, that object is returned instead of the
        # faceting class.
        MockFacetsWithEntryPoint.expect_load_entrypoint = INVALID_INPUT
        assert INVALID_INPUT == m()

        expect_entrypoint = object()
        expect_is_default = object()
        MockFacetsWithEntryPoint.expect_load_entrypoint = (
            expect_entrypoint,
            expect_is_default,
        )

        # Next, test success. The return value of load_entrypoint() is
        # is passed as 'entrypoint' into the FacetsWithEntryPoint
        # constructor.
        #
        # The object returned by load_entrypoint() does not need to be a
        # currently enabled entrypoint for the library.
        facets = m()
        assert isinstance(facets, FacetsWithEntryPoint)
        assert expect_entrypoint == facets.entrypoint
        assert expect_is_default == facets.entrypoint_is_default
        assert (
            "entrypoint name from request",
            ["Selectable entrypoints"],
            default_entrypoint,
        ) == MockFacetsWithEntryPoint.load_entrypoint_called_with
        assert dict(extra="extra kwarg") == facets.constructor_kwargs
        assert MockFacetsWithEntryPoint.selectable_entrypoints_called_with == config

    def test_load_entrypoint(self):
        audio = AudiobooksEntryPoint
        ebooks = EbooksEntryPoint

        # These are the allowable entrypoints for this site -- we'll
        # be passing this in to load_entrypoint every time.
        entrypoints = [audio, ebooks]

        worklist = object()
        m = FacetsWithEntryPoint.load_entrypoint

        # This request does not ask for any particular entrypoint, and
        # it doesn't specify a default, so it gets the first available
        # entrypoint.
        audio_default, is_default = m(None, entrypoints)
        assert audio == audio_default
        assert True == is_default

        # This request does not ask for any particular entrypoint, so
        # it gets the specified default.
        default = object()
        assert (default, True) == m(None, entrypoints, default)

        # This request asks for an entrypoint and gets it.
        assert (ebooks, False) == m(ebooks.INTERNAL_NAME, entrypoints)

        # This request asks for an entrypoint that is not available,
        # and gets the default.
        assert (audio, True) == m("no such entrypoint", entrypoints)

        # If no EntryPoints are available, load_entrypoint returns
        # nothing.
        assert (None, True) == m(audio.INTERNAL_NAME, [])

    def test_selectable_entrypoints(self):
        """The default implementation of selectable_entrypoints just returns
        the worklist's entrypoints.
        """

        class MockWorkList:
            def __init__(self, entrypoints):
                self.entrypoints = entrypoints

        mock_entrypoints = object()
        worklist = MockWorkList(mock_entrypoints)

        m = FacetsWithEntryPoint.selectable_entrypoints
        assert mock_entrypoints == m(worklist)
        assert [] == m(None)

    def test_modify_search_filter(self):
        # When an entry point is selected, search filters are modified so
        # that they only find works that fit that entry point.
        filter = Filter()
        facets = FacetsWithEntryPoint(AudiobooksEntryPoint)
        facets.modify_search_filter(filter)
        assert [Edition.AUDIO_MEDIUM] == filter.media

        # If no entry point is selected, the filter is not modified.
        filter = Filter()
        facets = FacetsWithEntryPoint()
        facets.modify_search_filter(filter)
        assert None == filter.media


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
        db.default_collection().data_source = DataSource.AMAZON
        facets = Facets(
            db.default_library(),
            Facets.COLLECTION_FULL,
            Facets.AVAILABLE_ALL,
            Facets.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
        )
        all_groups = list(facets.facet_groups)

        # By default, there are 10 facet transitions: two groups of three
        # and one group of two and 2 datasource groups and 2 for collection names
        assert 12 == len(all_groups)

        # available=all, collection=full, and order=title are the selected
        # facets.
        selected = sorted(x[:2] for x in all_groups if x[-1] == True)
        assert [
            ("available", "all"),
            ("collection", "full"),
            ("collectionName", "All"),
            ("distributor", "All"),
            ("order", "title"),
        ] == selected

        # Distributor and CollectionName facets are generated at runtime, they are not a setting value
        test_enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME: [Facets.ORDER_WORK_ID, Facets.ORDER_TITLE],
            Facets.COLLECTION_FACET_GROUP_NAME: [Facets.COLLECTION_FEATURED],
            Facets.AVAILABILITY_FACET_GROUP_NAME: [Facets.AVAILABLE_ALL],
        }
        test_default_facets = {
            Facets.ORDER_FACET_GROUP_NAME: Facets.ORDER_TITLE,
            Facets.COLLECTION_FACET_GROUP_NAME: Facets.COLLECTION_FEATURED,
            Facets.AVAILABILITY_FACET_GROUP_NAME: Facets.AVAILABLE_ALL,
        }
        library = db.default_library()
        self._configure_facets(library, test_enabled_facets, test_default_facets)

        facets = Facets(
            db.default_library(), None, None, Facets.ORDER_TITLE, None, None
        )
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
            ["distributor", DataSource.AMAZON, False],
            ["order", "title", True],
            ["order", "work_id", False],
        ]
        assert expect == sorted(list(x[:2]) + [x[-1]] for x in all_groups)

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
            Facets.COLLECTION_FACET_GROUP_NAME: [Facets.COLLECTION_FULL],
            Facets.AVAILABILITY_FACET_GROUP_NAME: [
                Facets.AVAILABLE_ALL,
                Facets.AVAILABLE_NOW,
            ],
        }
        test_default_facets = {
            Facets.ORDER_FACET_GROUP_NAME: Facets.ORDER_TITLE,
            Facets.COLLECTION_FACET_GROUP_NAME: Facets.COLLECTION_FULL,
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
            Facets.COLLECTION_FACET_GROUP_NAME: [Facets.COLLECTION_FULL],
            Facets.AVAILABILITY_FACET_GROUP_NAME: [Facets.AVAILABLE_OPEN_ACCESS],
        }
        library = db.default_library()
        self._configure_facets(library, enabled_facets, {})

        # Create a new Facets object with these facets enabled,
        # no matter the Configuration.
        facets = Facets(
            db.default_library(),
            Facets.COLLECTION_FULL,
            Facets.AVAILABLE_OPEN_ACCESS,
            Facets.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
            enabled_facets=enabled_facets,
        )
        all_groups = list(facets.facet_groups)
        expect = [["order", "author", False], ["order", "title", True]]
        assert expect == sorted(list(x[:2]) + [x[-1]] for x in all_groups)

    def test_facets_dont_need_a_library(self):
        enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME: [
                Facets.ORDER_TITLE,
                Facets.ORDER_AUTHOR,
            ],
            Facets.COLLECTION_FACET_GROUP_NAME: [Facets.COLLECTION_FULL],
            Facets.AVAILABILITY_FACET_GROUP_NAME: [Facets.AVAILABLE_OPEN_ACCESS],
        }

        facets = Facets(
            None,
            Facets.COLLECTION_FULL,
            Facets.AVAILABLE_OPEN_ACCESS,
            Facets.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
            enabled_facets=enabled_facets,
        )
        all_groups = list(facets.facet_groups)
        expect = [["order", "author", False], ["order", "title", True]]
        assert expect == sorted(list(x[:2]) + [x[-1]] for x in all_groups)

    def test_items(self, db: DatabaseTransactionFixture):
        """Verify that Facets.items() returns all information necessary
        to recreate the Facets object.
        """
        facets = Facets(
            db.default_library(),
            Facets.COLLECTION_FULL,
            Facets.AVAILABLE_ALL,
            Facets.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
            entrypoint=AudiobooksEntryPoint,
        )
        assert [
            ("available", Facets.AVAILABLE_ALL),
            ("collection", Facets.COLLECTION_FULL),
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
                collection=Facets.COLLECTION_FULL,
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
                collection=Facets.COLLECTION_FULL,
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
            F.COLLECTION_FULL,
            F.AVAILABLE_ALL,
            F.ORDER_TITLE,
            Facets.DISTRIBUTOR_ALL,
            Facets.COLLECTION_NAME_ALL,
            entrypoint=ebooks,
        )

        different_collection = f.navigate(collection=F.COLLECTION_FEATURED)
        assert F.COLLECTION_FEATURED == different_collection.collection
        assert F.AVAILABLE_ALL == different_collection.availability
        assert F.ORDER_TITLE == different_collection.order
        assert F.DISTRIBUTOR_ALL == different_collection.distributor
        assert F.COLLECTION_NAME_ALL == different_collection.collection_name
        assert ebooks == different_collection.entrypoint

        different_availability = f.navigate(availability=F.AVAILABLE_NOW)
        assert F.COLLECTION_FULL == different_availability.collection
        assert F.AVAILABLE_NOW == different_availability.availability
        assert F.ORDER_TITLE == different_availability.order
        assert F.DISTRIBUTOR_ALL == different_availability.distributor
        assert F.COLLECTION_NAME_ALL == different_availability.collection_name
        assert ebooks == different_availability.entrypoint

        different_order = f.navigate(order=F.ORDER_AUTHOR)
        assert F.COLLECTION_FULL == different_order.collection
        assert F.AVAILABLE_ALL == different_order.availability
        assert F.ORDER_AUTHOR == different_order.order
        assert F.DISTRIBUTOR_ALL == different_order.distributor
        assert F.COLLECTION_NAME_ALL == different_order.collection_name
        assert ebooks == different_order.entrypoint

        audiobooks = AudiobooksEntryPoint
        different_entrypoint = f.navigate(entrypoint=audiobooks)
        assert F.COLLECTION_FULL == different_entrypoint.collection
        assert F.AVAILABLE_ALL == different_entrypoint.availability
        assert F.ORDER_TITLE == different_entrypoint.order
        assert F.DISTRIBUTOR_ALL == different_entrypoint.distributor
        assert F.COLLECTION_NAME_ALL == different_entrypoint.collection_name
        assert audiobooks == different_entrypoint.entrypoint

        different_distributor = f.navigate(distributor=DataSource.AMAZON)
        assert F.COLLECTION_FULL == different_distributor.collection
        assert F.AVAILABLE_ALL == different_distributor.availability
        assert F.ORDER_TITLE == different_distributor.order
        assert F.COLLECTION_NAME_ALL == different_distributor.collection_name
        assert DataSource.AMAZON == different_distributor.distributor

        different_collection_name = f.navigate(collection_name="Collection Name")
        assert F.COLLECTION_FULL == different_collection_name.collection
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
        default_collection = config.default_facet(Facets.COLLECTION_FACET_GROUP_NAME)
        default_availability = config.default_facet(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        args: dict[str, str] = {}
        headers: dict = {}
        facets = m(library, library, args.get, headers.get, worklist)
        assert default_order == facets.order
        assert default_collection == facets.collection
        assert default_availability == facets.availability
        assert library == facets.library

        # The AudiobooksEntryPoint was selected as a default.
        assert AudiobooksEntryPoint == facets.entrypoint
        assert True == facets.entrypoint_is_default

        # Valid object using non-default settings.
        args = dict(
            order=Facets.ORDER_TITLE,
            collection=Facets.COLLECTION_FULL,
            available=Facets.AVAILABLE_OPEN_ACCESS,
            entrypoint=EbooksEntryPoint.INTERNAL_NAME,
        )
        facets = m(library, library, args.get, headers.get, worklist)
        assert Facets.ORDER_TITLE == facets.order
        assert Facets.COLLECTION_FULL == facets.collection
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

        # Invalid collection
        args = dict(collection="no such collection")
        invalid_collection = m(library, library, args.get, headers.get, None)
        assert INVALID_INPUT.uri == invalid_collection.uri
        assert (
            "I don't understand what 'no such collection' refers to."
            == invalid_collection.detail
        )

    def test_from_request_gets_available_facets_through_hook_methods(
        self, db: DatabaseTransactionFixture
    ):
        # Available and default facets are determined by calling the
        # available_facets() and default_facets() methods. This gives
        # subclasses a chance to add extra facets or change defaults.
        class Mock(Facets):
            available_facets_calls: List[Tuple] = []
            default_facet_calls: List[Tuple] = []

            # For whatever reason, this faceting object allows only a
            # single setting for each facet group.
            mock_enabled = dict(
                order=[Facets.ORDER_TITLE],
                available=[Facets.AVAILABLE_OPEN_ACCESS],
                collection=[Facets.COLLECTION_FULL],
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
            collection,
            distributor,
            collection_name,
        ) = Mock.available_facets_calls
        # available_facets was called three times, to ask the Mock class what it thinks
        # the options for order, availability, and collection should be.
        assert (library, "order") == order
        assert (library, "available") == available
        assert (library, "collection") == collection
        assert (library, "distributor") == distributor
        assert (library, "collectionName") == collection_name

        # default_facet was called three times, to ask the Mock class what it thinks
        # the default order, availability, and collection should be.
        (
            order_d,
            available_d,
            collection_d,
            distributor_d,
            collection_name_d,
        ) = Mock.default_facet_calls
        assert (library, "order") == order_d
        assert (library, "available") == available_d
        assert (library, "collection") == collection_d
        assert (library, "distributor") == distributor_d
        assert (library, "collectionName") == collection_name_d

        # Finally, verify that the return values from the mocked methods were actually used.

        # The facets enabled during initialization are the limited
        # subset established by available_facets().
        assert Mock.mock_enabled == result.facets_enabled_at_init

        # The current values came from the defaults provided by default_facet().
        assert Facets.ORDER_TITLE == result.order
        assert Facets.AVAILABLE_OPEN_ACCESS == result.availability
        assert Facets.COLLECTION_FULL == result.collection
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
            None,
            entrypoint=AudiobooksEntryPoint,
        )
        filter = Filter()
        facets.modify_search_filter(filter)
        assert [Edition.AUDIO_MEDIUM] == filter.media

        # Now test the subclass behavior.
        facets = Facets(
            db.default_library(),
            "some collection",
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
        assert "some collection" == filter.subcollection
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
        facets = Facets(db.default_library(), None, None, "invalid order", None, None)
        filter = Filter()
        facets.modify_search_filter(filter)
        assert None == filter.order

        facets = Facets(
            db.default_library(), None, None, None, None, db.default_collection().name
        )
        filter = Filter()
        facets.modify_search_filter(filter)
        assert [db.default_collection().id] == filter.collection_ids

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

        not_licensed = db.work(with_license_pool=True, title="not licensed")
        [pool] = not_licensed.license_pools
        pool.licenses_owned = 0
        pool.licenses_available = 0
        qu = (
            db.session.query(Work)
            .join(Work.license_pools)
            .join(LicensePool.presentation_edition)
        )

        for availability, expect in [
            (
                Facets.AVAILABLE_NOW,
                [open_access, available, unlimited_access],
            ),
            (
                Facets.AVAILABLE_ALL,
                [open_access, available, not_available, unlimited_access],
            ),
            (Facets.AVAILABLE_NOT_NOW, [not_available]),
        ]:
            facets = Facets(db.default_library(), None, availability, None, None, None)
            modified = facets.modify_database_query(db.session, qu)
            assert (availability, sorted(x.title for x in modified)) == (
                availability,
                sorted(x.title for x in expect),
            )

        # Setting the 'featured' collection includes only known
        # high-quality works.
        for collection, expect in [
            (
                Facets.COLLECTION_FULL,
                [open_access, available, unlimited_access],
            ),
            (Facets.COLLECTION_FEATURED, [open_access]),
        ]:
            facets = Facets(
                db.default_library(), collection, Facets.AVAILABLE_NOW, None, None, None
            )
            modified = facets.modify_database_query(db.session, qu)
            assert (collection, sorted(x.title for x in modified)) == (
                collection,
                sorted(x.title for x in expect),
            )


class TestDefaultSortOrderFacets:
    def _check_other_groups_not_changed(self, cls, config: Library):
        # Verify that nothing has changed for the collection or
        # availability facet groups.
        for group_name in (
            Facets.COLLECTION_FACET_GROUP_NAME,
            Facets.AVAILABILITY_FACET_GROUP_NAME,
        ):
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


class TestDatabaseBackedFacets:
    def test_available_facets(self, db: DatabaseTransactionFixture):
        # The only available sort orders are the ones that map
        # directly onto a database field.

        f1 = Facets
        f2 = DatabaseBackedFacets

        # The sort orders available to a DatabaseBackedFacets are a
        # subset of the ones available to a Facets under the same
        # configuration.
        f1_orders = f1.available_facets(
            db.default_library(), FacetConstants.ORDER_FACET_GROUP_NAME
        )

        f2_orders = f2.available_facets(
            db.default_library(), FacetConstants.ORDER_FACET_GROUP_NAME
        )
        assert len(f2_orders) < len(f1_orders)
        for order in f2_orders:
            assert order in f1_orders and order in f2.ORDER_FACET_TO_DATABASE_FIELD

        # The rules for collection and availability are the same.
        for group in (
            FacetConstants.COLLECTION_FACET_GROUP_NAME,
            FacetConstants.AVAILABILITY_FACET_GROUP_NAME,
        ):
            assert f1.available_facets(
                db.default_library(), group
            ) == f2.available_facets(db.default_library(), group)

    def test_default_facets(self, db: DatabaseTransactionFixture):
        # If the configured default sort order is not available,
        # DatabaseBackedFacets chooses the first enabled sort order.
        f1 = Facets
        f2 = DatabaseBackedFacets

        # The rules for collection and availability are the same.
        for group in (
            FacetConstants.COLLECTION_FACET_GROUP_NAME,
            FacetConstants.AVAILABILITY_FACET_GROUP_NAME,
        ):
            assert f1.default_facet(db.default_library(), group) == f2.default_facet(
                db.default_library(), group
            )

        # In this bizarre library, the default sort order is 'time
        # added to collection' -- an order not supported by
        # DatabaseBackedFacets.
        class Mock:
            enabled = [
                FacetConstants.ORDER_ADDED_TO_COLLECTION,
                FacetConstants.ORDER_TITLE,
                FacetConstants.ORDER_AUTHOR,
            ]

            def enabled_facets(self, group_name):
                return self.enabled

            def default_facet(self, group_name):
                return FacetConstants.ORDER_ADDED_TO_COLLECTION

        # A Facets object uses the 'time added to collection' order by
        # default.
        config = Mock()
        assert f1.ORDER_ADDED_TO_COLLECTION == f1.default_facet(
            config, f1.ORDER_FACET_GROUP_NAME
        )

        # A DatabaseBacked Facets can't do that. It finds the first
        # enabled sort order that it can support, and uses it instead.
        assert f2.ORDER_TITLE == f2.default_facet(config, f2.ORDER_FACET_GROUP_NAME)

        # If no enabled sort orders are supported, it just sorts
        # by Work ID, so that there is always _some_ sort order.
        config.enabled = [FacetConstants.ORDER_ADDED_TO_COLLECTION]
        assert f2.ORDER_WORK_ID == f2.default_facet(config, f2.ORDER_FACET_GROUP_NAME)

    def test_order_by(self, db: DatabaseTransactionFixture):
        E = Edition
        W = Work

        def order(facet, ascending=None):
            f = DatabaseBackedFacets(
                db.default_library(),
                collection=Facets.COLLECTION_FULL,
                availability=Facets.AVAILABLE_ALL,
                order=facet,
                distributor=None,
                collection_name=None,
                order_ascending=ascending,
            )
            return f.order_by()[0]

        def compare(a, b):
            assert len(a) == len(b)
            for i in range(0, len(a)):
                assert a[i].compare(b[i])

        expect = [E.sort_author.asc(), E.sort_title.asc(), W.id.asc()]
        actual = order(Facets.ORDER_AUTHOR, True)
        compare(expect, actual)

        expect = [E.sort_author.desc(), E.sort_title.asc(), W.id.asc()]
        actual = order(Facets.ORDER_AUTHOR, False)
        compare(expect, actual)

        expect = [E.sort_title.asc(), E.sort_author.asc(), W.id.asc()]
        actual = order(Facets.ORDER_TITLE, True)
        compare(expect, actual)

        expect = [
            W.last_update_time.asc(),
            E.sort_author.asc(),
            E.sort_title.asc(),
            W.id.asc(),
        ]
        actual = order(Facets.ORDER_LAST_UPDATE, True)
        compare(expect, actual)

        # Unsupported sort order -> default (author, title, work ID)
        expect = [E.sort_author.asc(), E.sort_title.asc(), W.id.asc()]
        actual = order(Facets.ORDER_ADDED_TO_COLLECTION, True)
        compare(expect, actual)

    def test_modify_database_query(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        # Set up works that are matched by different types of collections.

        # A high-quality open-access work.
        open_access_high = db.work(with_open_access_download=True)
        open_access_high.quality = 0.8

        # A low-quality open-access work.
        open_access_low = db.work(with_open_access_download=True)
        open_access_low.quality = 0.2

        # A high-quality licensed work which is not currently available.
        (licensed_e1, licensed_p1) = db.edition(
            data_source_name=DataSource.OVERDRIVE, with_license_pool=True
        )
        licensed_high = db.work(presentation_edition=licensed_e1)
        licensed_high.license_pools.append(licensed_p1)
        licensed_high.quality = 0.8
        licensed_p1.open_access = False
        licensed_p1.licenses_owned = 1
        licensed_p1.licenses_available = 0

        # A low-quality licensed work which is currently available.
        (licensed_e2, licensed_p2) = db.edition(
            data_source_name=DataSource.OVERDRIVE, with_license_pool=True
        )
        licensed_p2.open_access = False
        licensed_low = db.work(presentation_edition=licensed_e2)
        licensed_low.license_pools.append(licensed_p2)
        licensed_low.quality = 0.2
        licensed_p2.licenses_owned = 1
        licensed_p2.licenses_available = 1

        # A high-quality work with unlimited access.
        unlimited_access_high = db.work(with_license_pool=True, unlimited_access=True)
        unlimited_access_high.quality = 0.8

        qu = DatabaseBackedWorkList.base_query(db.session)

        def facetify(
            collection=Facets.COLLECTION_FULL,
            available=Facets.AVAILABLE_ALL,
            order=Facets.ORDER_TITLE,
        ):
            f = DatabaseBackedFacets(
                db.default_library(), collection, available, order, None, None
            )
            return f.modify_database_query(db.session, qu)

        # When holds are allowed, we can find all works by asking
        # for everything.
        library = db.default_library()
        settings = library_fixture.settings(library)
        settings.allow_holds = True
        everything = facetify()
        assert 5 == everything.count()

        # If we disallow holds, we lose one book even when we ask for
        # everything.
        settings.allow_holds = False
        everything = facetify()
        assert 4 == everything.count()
        assert licensed_high not in everything

        settings.allow_holds = True
        # Even when holds are allowed, if we restrict to books
        # currently available we lose the unavailable book.
        available_now = facetify(available=Facets.AVAILABLE_NOW)
        assert 4 == available_now.count()
        assert licensed_high not in available_now

        # If we restrict to open-access books we lose the two licensed
        # books.
        open_access = facetify(available=Facets.AVAILABLE_OPEN_ACCESS)
        assert 2 == open_access.count()
        assert licensed_high not in open_access
        assert licensed_low not in open_access
        assert unlimited_access_high not in open_access

        # If we restrict to the featured collection we lose the two
        # low-quality books.
        featured_collection = facetify(collection=Facets.COLLECTION_FEATURED)
        assert 3 == featured_collection.count()
        assert open_access_low not in featured_collection
        assert licensed_low not in featured_collection

        # Try some different orderings to verify that order_by()
        # is called and used properly.
        title_order = facetify(order=Facets.ORDER_TITLE)
        assert [
            open_access_high.id,
            open_access_low.id,
            licensed_high.id,
            licensed_low.id,
            unlimited_access_high.id,
        ] == [x.id for x in title_order]
        assert ["sort_title", "sort_author", "id"] == [
            x.name for x in title_order._distinct_on
        ]

        # This sort order is not supported, so the default is used.
        unsupported_order = facetify(order=Facets.ORDER_ADDED_TO_COLLECTION)
        assert [
            unlimited_access_high.id,
            licensed_low.id,
            licensed_high.id,
            open_access_low.id,
            open_access_high.id,
        ] == [x.id for x in unsupported_order]
        assert ["sort_author", "sort_title", "id"] == [
            x.name for x in unsupported_order._distinct_on
        ]


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
        library1_settings.minimum_featured_quality = 0.22  # type: ignore[assignment]
        library1 = library_fixture.library(settings=library1_settings)
        library2_settings = library_fixture.mock_settings()
        library2_settings.minimum_featured_quality = 0.99  # type: ignore[assignment]
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


class TestSearchFacets:
    def test_constructor(self):
        # The SearchFacets constructor allows you to specify
        # a medium and language (or a list of them) as well
        # as an entrypoint.

        m = SearchFacets

        # If you don't pass any information in, you get a SearchFacets
        # that does nothing.
        defaults = m()
        assert None == defaults.entrypoint
        assert None == defaults.languages
        assert None == defaults.media
        assert m.ORDER_BY_RELEVANCE == defaults.order
        assert None == defaults.min_score
        assert False == defaults._language_from_query

        mock_entrypoint = object()

        # If you pass in a single value for medium or language
        # they are turned into a list.
        with_single_value = m(
            entrypoint=mock_entrypoint,
            media=Edition.BOOK_MEDIUM,
            languages="eng",
            language_from_query=True,
        )
        assert mock_entrypoint == with_single_value.entrypoint
        assert [Edition.BOOK_MEDIUM] == with_single_value.media
        assert ["eng"] == with_single_value.languages
        assert True == with_single_value._language_from_query

        # If you pass in a list of values, it's left alone.
        media = [Edition.BOOK_MEDIUM, Edition.AUDIO_MEDIUM]
        languages = ["eng", "spa"]
        with_multiple_values = m(media=media, languages=languages)
        assert media == with_multiple_values.media
        assert languages == with_multiple_values.languages

        # The only exception is if you pass in Edition.ALL_MEDIUM
        # as 'medium' -- that's passed through as is.
        every_medium = m(media=Edition.ALL_MEDIUM)
        assert Edition.ALL_MEDIUM == every_medium.media

        # Pass in a value for min_score, and it's stored for later.
        mock_min_score = object()
        with_min_score = m(min_score=mock_min_score)
        assert mock_min_score == with_min_score.min_score

        # Pass in a value for order, and you automatically get a
        # reasonably tight value for min_score.
        order = object()
        with_order = m(order=order)
        assert order == with_order.order
        assert SearchFacets.DEFAULT_MIN_SCORE == with_order.min_score

    def test_from_request(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        # An HTTP client can customize which SearchFacets object
        # is created by sending different HTTP requests.

        # These variables mock the query string arguments and
        # HTTP headers of an HTTP request.
        arguments = dict(
            entrypoint=EbooksEntryPoint.INTERNAL_NAME,
            media=Edition.AUDIO_MEDIUM,
            min_score="123",
        )
        headers = {"Accept-Language": "da, en-gb;q=0.8"}
        get_argument = arguments.get
        get_header = headers.get

        unused = object()

        library_settings = library_fixture.mock_settings()
        library_settings.enabled_entry_points = [
            AudiobooksEntryPoint.INTERNAL_NAME,
            EbooksEntryPoint.INTERNAL_NAME,
        ]
        library = db.library(settings=library_settings)

        def from_request(**extra):
            return SearchFacets.from_request(
                library,
                library,
                get_argument,
                get_header,
                unused,
                **extra,
            )

        facets = from_request(extra="value")
        assert (
            dict(extra="value", language_from_query=False) == facets.constructor_kwargs
        )

        # The superclass's from_request implementation pulled the
        # requested EntryPoint out of the request.
        assert EbooksEntryPoint == facets.entrypoint

        # The SearchFacets implementation pulled the 'media' query
        # string argument.
        #
        # The medium from the 'media' argument contradicts the medium
        # implied by the entry point, but that's not our problem.
        assert [Edition.AUDIO_MEDIUM] == facets.media

        # The SearchFacets implementation turned the 'min_score'
        # argument into a numeric minimum score.
        assert 123 == facets.min_score

        # The SearchFacets implementation turned the 'Accept-Language'
        # header into a set of language codes.
        assert ["dan", "eng"] == facets.languages
        assert False == facets._language_from_query

        # Try again with bogus media, languages, and minimum score.
        arguments["media"] = "Unknown Media"
        arguments["min_score"] = "not a number"
        headers["Accept-Language"] = "xx, ql"

        # None of the bogus information was used.
        facets = from_request()
        assert None == facets.media
        assert None == facets.languages
        assert None == facets.min_score

        # Reading the language from the query, with a search type
        arguments["language"] = "all"
        arguments["search_type"] = "json"
        headers["Accept-Language"] = "da, en-gb;q=0.8"

        facets = from_request()
        assert ["all"] == facets.languages
        assert True == facets._language_from_query
        assert "json" == facets.search_type

        # Try again with no information.
        del arguments["media"]
        del arguments["language"]
        del headers["Accept-Language"]

        facets = from_request()
        assert None == facets.media
        assert None == facets.languages

    def test_from_request_from_admin_search(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        # If the SearchFacets object is being created by a search run from the admin interface,
        # there might be order and language arguments which should be used to filter search results.

        arguments = dict(
            order="author",
            language="fre",
            entrypoint=EbooksEntryPoint.INTERNAL_NAME,
            media=Edition.AUDIO_MEDIUM,
            min_score="123",
        )
        headers = {"Accept-Language": "da, en-gb;q=0.8"}
        get_argument = arguments.get
        get_header = headers.get

        unused = object()

        library_settings = library_fixture.mock_settings()
        library_settings.enabled_entry_points = [
            AudiobooksEntryPoint.INTERNAL_NAME,
            EbooksEntryPoint.INTERNAL_NAME,
        ]
        library = library_fixture.library(settings=library_settings)

        def from_request(**extra):
            return SearchFacets.from_request(
                library,
                library,
                get_argument,
                get_header,
                unused,
                **extra,
            )

        facets = from_request(extra="value")
        # The SearchFacets implementation uses the order and language values submitted by the admin.
        assert "author" == facets.order
        assert ["fre"] == facets.languages

    def test_selectable_entrypoints(self):
        """If the WorkList has more than one facet, an 'everything' facet
        is added for search purposes.
        """

        class MockWorkList:
            def __init__(self):
                self.entrypoints = None

        ep1 = object()
        ep2 = object()
        worklist = MockWorkList()

        # No WorkList, no EntryPoints.
        m = SearchFacets.selectable_entrypoints
        assert [] == m(None)

        # If there is one EntryPoint, it is returned as-is.
        worklist.entrypoints = [ep1]
        assert [ep1] == m(worklist)

        # If there are multiple EntryPoints, EverythingEntryPoint
        # shows up at the beginning.
        worklist.entrypoints = [ep1, ep2]
        assert [EverythingEntryPoint, ep1, ep2] == m(worklist)

        # If EverythingEntryPoint is already in the list, it's not
        # added twice.
        worklist.entrypoints = [ep1, EverythingEntryPoint, ep2]
        assert worklist.entrypoints == m(worklist)

    def test_items(self):
        facets = SearchFacets(
            entrypoint=EverythingEntryPoint,
            media=Edition.BOOK_MEDIUM,
            languages=["eng"],
            min_score=123,
        )

        # When we call items(), e.g. to create a query string that
        # propagates the facet settings, both entrypoint and
        # media are propagated if present.
        #
        # language is not propagated, because it's set through
        # the Accept-Language header rather than through a query
        # string.
        assert [
            ("entrypoint", EverythingEntryPoint.INTERNAL_NAME),
            (Facets.ORDER_FACET_GROUP_NAME, SearchFacets.ORDER_BY_RELEVANCE),
            (Facets.AVAILABILITY_FACET_GROUP_NAME, Facets.AVAILABLE_ALL),
            (Facets.COLLECTION_FACET_GROUP_NAME, Facets.COLLECTION_FULL),
            ("media", Edition.BOOK_MEDIUM),
            ("min_score", "123"),
            ("search_type", "default"),
        ] == list(facets.items())

        # In case the language came from a query argument
        facets = SearchFacets(
            languages=["eng"],
            language_from_query=True,
        )

        assert dict(facets.items())["language"] == ["eng"]

    def test_navigation(self):
        """Navigating from one SearchFacets to another gives a new
        SearchFacets object. A number of fields can be changed,
        including min_score, which is SearchFacets-specific.
        """
        facets = SearchFacets(entrypoint=object(), order="field1", min_score=100)
        new_ep = object()
        new_facets = facets.navigate(entrypoint=new_ep, order="field2", min_score=120)
        assert isinstance(new_facets, SearchFacets)
        assert new_ep == new_facets.entrypoint
        assert "field2" == new_facets.order
        assert 120 == new_facets.min_score

    def test_modify_search_filter(self):
        # Test superclass behavior -- filter is modified by entrypoint.
        facets = SearchFacets(entrypoint=AudiobooksEntryPoint)
        filter = Filter()
        facets.modify_search_filter(filter)
        assert [Edition.AUDIO_MEDIUM] == filter.media

        # The medium specified in the constructor overrides anything
        # already present in the filter.
        facets = SearchFacets(entrypoint=None, media=Edition.BOOK_MEDIUM)
        filter = Filter(media=Edition.AUDIO_MEDIUM)
        facets.modify_search_filter(filter)
        assert [Edition.BOOK_MEDIUM] == filter.media

        # It also overrides anything specified by the EntryPoint.
        facets = SearchFacets(
            entrypoint=AudiobooksEntryPoint, media=Edition.BOOK_MEDIUM
        )
        filter = Filter()
        facets.modify_search_filter(filter)
        assert [Edition.BOOK_MEDIUM] == filter.media

        # The language specified in the constructor _adds_ to any
        # languages already present in the filter.
        facets = SearchFacets(languages=["eng", "spa"])
        filter = Filter(languages="spa")
        facets.modify_search_filter(filter)
        assert ["eng", "spa"] == filter.languages

        # It doesn't override those values.
        facets = SearchFacets(languages="eng")
        filter = Filter(languages="spa")
        facets.modify_search_filter(filter)
        assert ["eng", "spa"] == filter.languages

        # This may result in modify_search_filter being a no-op.
        facets = SearchFacets(languages="eng")
        filter = Filter(languages="eng")
        facets.modify_search_filter(filter)
        assert ["eng"] == filter.languages

        # If no languages are specified in the SearchFacets, the value
        # set by the filter is used by itself.
        facets = SearchFacets(languages=None)
        filter = Filter(languages="spa")
        facets.modify_search_filter(filter)
        assert ["spa"] == filter.languages

        # If neither facets nor filter includes any languages, there
        # is no language filter.
        facets = SearchFacets(languages=None)
        filter = Filter(languages=None)
        facets.modify_search_filter(filter)
        assert None == filter.languages

        # We don't interfere with the languages filter when languages is ["all"]
        facets = SearchFacets(languages="all")
        filter = Filter(languages=["spa"])
        facets.modify_search_filter(filter)
        assert ["spa"] == filter.languages

    def test_modify_search_filter_accepts_relevance_order(self):
        # By default, Opensearch orders by relevance, so if order
        # is specified as "relevance", filter should not have an
        # `order` property.
        with LogCaptureHandler(logging.root) as logs:
            facets = SearchFacets()
            filter = Filter()
            facets.modify_search_filter(filter)
            assert None == filter.order
            assert 0 == len(logs.error)

        with LogCaptureHandler(logging.root) as logs:
            facets = SearchFacets(order="relevance")
            filter = Filter()
            facets.modify_search_filter(filter)
            assert None == filter.order
            assert 0 == len(logs.error)

        with LogCaptureHandler(logging.root) as logs:
            supported_order = "author"
            facets = SearchFacets(order=supported_order)
            filter = Filter()
            facets.modify_search_filter(filter)
            assert filter.order is not None
            assert len(filter.order) > 0
            assert 0 == len(logs.error)

        with LogCaptureHandler(logging.root) as logs:
            unsupported_order = "some_order_we_do_not_support"
            facets = SearchFacets(order=unsupported_order)
            filter = Filter()
            facets.modify_search_filter(filter)
            assert None == filter.order
            assert "Unrecognized sort order: %s" % unsupported_order in logs.error


class TestPagination:
    def test_from_request(self):
        # No arguments -> Class defaults.
        pagination = Pagination.from_request({}.get, None)
        assert isinstance(pagination, Pagination)
        assert Pagination.DEFAULT_SIZE == pagination.size
        assert 0 == pagination.offset

        # Override the default page size.
        pagination = Pagination.from_request({}.get, 100)
        assert isinstance(pagination, Pagination)
        assert 100 == pagination.size
        assert 0 == pagination.offset

        # The most common usages.
        pagination = Pagination.from_request(dict(size="4").get)
        assert isinstance(pagination, Pagination)
        assert 4 == pagination.size
        assert 0 == pagination.offset

        pagination = Pagination.from_request(dict(after="6").get)
        assert isinstance(pagination, Pagination)
        assert Pagination.DEFAULT_SIZE == pagination.size
        assert 6 == pagination.offset

        pagination = Pagination.from_request(dict(size=4, after=6).get)
        assert isinstance(pagination, Pagination)
        assert 4 == pagination.size
        assert 6 == pagination.offset

        # Invalid size or offset -> problem detail
        error = Pagination.from_request(dict(size="string").get)
        assert INVALID_INPUT.uri == error.uri
        assert "Invalid page size: string" == str(error.detail)

        error = Pagination.from_request(dict(after="string").get)
        assert INVALID_INPUT.uri == error.uri
        assert "Invalid offset: string" == str(error.detail)

        # Size too large -> cut down to MAX_SIZE
        pagination = Pagination.from_request(dict(size="10000").get)
        assert isinstance(pagination, Pagination)
        assert Pagination.MAX_SIZE == pagination.size
        assert 0 == pagination.offset

    def test_has_next_page_total_size(self, db: DatabaseTransactionFixture):
        """Test the ability of Pagination.total_size to control whether there is a next page."""
        query = db.session.query(Work)
        pagination = Pagination(size=2)

        # When total_size is not set, Pagination assumes there is a
        # next page.
        pagination.modify_database_query(db.session, query)
        assert True == pagination.has_next_page

        # Here, there is one more item on the next page.
        pagination.total_size = 3
        assert 0 == pagination.offset
        assert True == pagination.has_next_page

        # Here, the last item on this page is the last item in the dataset.
        pagination.offset = 1
        assert False == pagination.has_next_page
        assert None == pagination.next_page

        # If we somehow go over the end of the dataset, there is no next page.
        pagination.offset = 400
        assert False == pagination.has_next_page
        assert None == pagination.next_page

        # If both total_size and this_page_size are set, total_size
        # takes precedence.
        pagination.offset = 0
        pagination.total_size = 100
        pagination.this_page_size = 0
        assert True == pagination.has_next_page

        pagination.total_size = 0
        pagination.this_page_size = 10
        assert False == pagination.has_next_page
        assert None == pagination.next_page

    def test_has_next_page_this_page_size(self, db: DatabaseTransactionFixture):
        """Test the ability of Pagination.this_page_size to control whether there is a next page."""
        query = db.session.query(Work)
        pagination = Pagination(size=2)

        # When this_page_size is not set, Pagination assumes there is a
        # next page.
        pagination.modify_database_query(db.session, query)
        assert True == pagination.has_next_page

        # Here, there is nothing on the current page. There is no next page.
        pagination.this_page_size = 0
        assert False == pagination.has_next_page

        # If the page is full, we can be almost certain there is a next page.
        pagination.this_page_size = 400
        assert True == pagination.has_next_page

        # Here, there is one item on the current page. Even though the
        # current page is not full (page size is 2), we assume for
        # safety's sake that there is a next page. The cost of getting
        # this wrong is low, compared to the cost of saying there is no
        # next page when there actually is.
        pagination.this_page_size = 1
        assert True == pagination.has_next_page

    def test_page_loaded(self):
        # Test page_loaded(), which lets the Pagination object see the
        # size of the current page.
        pagination = Pagination()
        assert None == pagination.this_page_size
        assert False == pagination.page_has_loaded
        pagination.page_loaded([1, 2, 3])
        assert 3 == pagination.this_page_size
        assert True == pagination.page_has_loaded

    def test_modify_search_query(self):
        # The default implementation of modify_search_query is to slice
        # a set of search results like a list.
        pagination = Pagination(offset=2, size=3)
        o = [1, 2, 3, 4, 5, 6]
        assert o[2 : 2 + 3] == pagination.modify_search_query(o)


class MockWork:
    """Acts enough like a Work to trick code that doesn't need to make
    database requests.
    """

    def __init__(self, id):
        self.id = id


class MockWorks(WorkList):
    """A WorkList that mocks works_from_database()."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.works = []
        self.works_from_database_calls = []
        self.random_sample_calls = []

    def queue_works(self, works):
        """Set the next return value for works_from_database()."""
        self.works.append(works)

    def works_from_database(self, _db, facets=None, pagination=None, featured=False):
        self.works_from_database_calls.append((facets, pagination, featured))
        try:
            return self.works.pop(0)
        except IndexError:
            return []

    def random_sample(self, query, target_size):
        # The 'query' is actually a list, and we're in a test
        # environment where randomness is not welcome. Just take
        # a sample from the front of the list.
        self.random_sample_calls.append((query, target_size))
        return query[:target_size]


class TestWorkList:
    def test_initialize(self, db: DatabaseTransactionFixture):
        wl = WorkList()
        child = WorkList()
        child.initialize(db.default_library())
        sf, ignore = Genre.lookup(db.session, "Science Fiction")
        romance, ignore = Genre.lookup(db.session, "Romance")

        # Create a WorkList that's associated with a Library, two genres,
        # and a child WorkList.
        wl.initialize(
            db.default_library(),
            children=[child],
            genres=[sf, romance],
            entrypoints=[1, 2, 3],
        )

        # Access the Library.
        assert db.default_library() == wl.get_library(db.session)

        # The Collections associated with the WorkList are those associated
        # with the Library.
        assert set(wl.collection_ids) == {
            x.id for x in db.default_library().collections
        }

        # The Genres associated with the WorkList are the ones passed
        # in on the constructor.
        assert set(wl.genre_ids) == {x.id for x in [sf, romance]}

        # The WorkList's child is the WorkList passed in to the constructor.
        assert [child] == wl.visible_children

        # The Worklist's .entrypoints is whatever was passed in
        # to the constructor.
        assert [1, 2, 3] == wl.entrypoints

    def test_initialize_worklist_without_library(self):
        # It's possible to initialize a WorkList with no Library.
        worklist = WorkList()
        worklist.initialize(None)

        # No restriction is placed on the collection IDs of the
        # Works in this list.
        assert None == worklist.collection_ids

    def test_initialize_with_customlists(self, db: DatabaseTransactionFixture):
        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)

        customlist1, ignore = db.customlist(
            data_source_name=gutenberg.name, num_entries=0
        )
        customlist2, ignore = db.customlist(
            data_source_name=gutenberg.name, num_entries=0
        )
        customlist3, ignore = db.customlist(
            data_source_name=DataSource.OVERDRIVE, num_entries=0
        )

        # Make a WorkList based on specific CustomLists.
        worklist = WorkList()
        worklist.initialize(
            db.default_library(), customlists=[customlist1, customlist3]
        )
        assert [customlist1.id, customlist3.id] == worklist.customlist_ids
        assert None == worklist.list_datasource_id

        # Make a WorkList based on a DataSource, as a shorthand for
        # 'all the CustomLists from that DataSource'.
        worklist = WorkList()
        worklist.initialize(db.default_library(), list_datasource=gutenberg)
        assert [customlist1.id, customlist2.id] == worklist.customlist_ids
        assert gutenberg.id == worklist.list_datasource_id

    def test_initialize_without_library(self, db: DatabaseTransactionFixture):
        wl = WorkList()
        sf, ignore = Genre.lookup(db.session, "Science Fiction")
        romance, ignore = Genre.lookup(db.session, "Romance")

        # Create a WorkList that's associated with two genres.
        wl.initialize(None, genres=[sf, romance])
        wl.collection_ids = [db.default_collection().id]

        # There is no Library.
        assert None == wl.get_library(db.session)

        # The Genres associated with the WorkList are the ones passed
        # in on the constructor.
        assert set(wl.genre_ids) == {x.id for x in [sf, romance]}

    def test_initialize_uses_append_child_hook_method(
        self, db: DatabaseTransactionFixture
    ):
        # When a WorkList is initialized with children, the children
        # are passed individually through the append_child() hook
        # method, not simply set to WorkList.children.
        class Mock(WorkList):
            append_child_calls = []

            def append_child(self, child):
                self.append_child_calls.append(child)
                return super().append_child(child)

        child = WorkList()
        parent = Mock()
        parent.initialize(db.default_library(), children=[child])
        assert [child] == parent.append_child_calls

        # They do end up in WorkList.children, since that's what the
        # default append_child() implementation does.
        assert [child] == parent.children

    def test_top_level_for_library(self, db: DatabaseTransactionFixture):
        """Test the ability to generate a top-level WorkList."""
        # These two top-level lanes should be children of the WorkList.
        lane1 = db.lane(display_name="Top-level Lane 1")
        lane1.priority = 0
        lane2 = db.lane(display_name="Top-level Lane 2")
        lane2.priority = 1

        # This lane is invisible and will be filtered out.
        invisible_lane = db.lane(display_name="Invisible Lane")
        invisible_lane.visible = False

        # This lane has a parent and will be filtered out.
        sublane = db.lane(display_name="Sublane")
        lane1.sublanes.append(sublane)

        # This lane belongs to a different library.
        other_library = db.library(name="Other Library", short_name="Other")
        other_library_lane = db.lane(
            display_name="Other Library Lane", library=other_library
        )

        # The default library gets a TopLevelWorkList with the two top-level lanes as children.
        wl = WorkList.top_level_for_library(db.session, db.default_library())
        assert isinstance(wl, TopLevelWorkList)
        assert [lane1, lane2] == wl.children
        assert Edition.FULFILLABLE_MEDIA == wl.media

        # The other library only has one top-level lane, so we use that lane.
        l = WorkList.top_level_for_library(db.session, other_library)
        assert other_library_lane == l

        # This library has no lanes configured at all.
        no_config_library = db.library(
            name="No configuration Library", short_name="No config"
        )
        wl = WorkList.top_level_for_library(db.session, no_config_library)
        assert isinstance(wl, TopLevelWorkList)
        assert [] == wl.children
        assert Edition.FULFILLABLE_MEDIA == wl.media

    def test_audience_key(self, db: DatabaseTransactionFixture):
        wl = WorkList()
        wl.initialize(library=db.default_library())

        # No audience.
        assert "" == wl.audience_key

        # All audiences.
        wl.audiences = Classifier.AUDIENCES
        assert "" == wl.audience_key

        # Specific audiences.
        wl.audiences = [Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT]
        assert "Children,Young+Adult" == wl.audience_key

    def test_parent(self):
        # A WorkList has no parent.
        assert None == WorkList().parent

    def test_parentage(self):
        # A WorkList has no parentage, since it has no parent.
        assert [] == WorkList().parentage

    def test_inherit_parent_restrictions(self):
        # A WorkList never inherits parent restrictions, because it
        # can't have a parent.
        assert False == WorkList().inherit_parent_restrictions

    def test_hierarchy(self):
        # A WorkList's hierarchy includes only itself, because it
        # can't have a parent.
        wl = WorkList()
        assert [wl] == wl.hierarchy

    def test_visible_children(self, db: DatabaseTransactionFixture):
        """Invisible children don't show up in WorkList.visible_children."""
        wl = WorkList()
        visible = db.lane()
        invisible = db.lane()
        invisible.visible = False
        child_wl = WorkList()
        child_wl.initialize(db.default_library())
        wl.initialize(db.default_library(), children=[visible, invisible, child_wl])
        assert {child_wl, visible} == set(wl.visible_children)

    def test_visible_children_sorted(self, db: DatabaseTransactionFixture):
        """Visible children are sorted by priority and then by display name."""
        wl = WorkList()

        lane_child = db.lane()
        lane_child.display_name = "ZZ"
        lane_child.priority = 0

        wl_child = WorkList()
        wl_child.priority = 1
        wl_child.display_name = "AA"

        wl.initialize(db.default_library(), children=[lane_child, wl_child])

        # lane_child has a higher priority so it shows up first even
        # though its display name starts with a Z.
        assert [lane_child, wl_child] == wl.visible_children

        # If the priorities are the same, wl_child shows up first,
        # because its display name starts with an A.
        wl_child.priority = 0
        assert [wl_child, lane_child] == wl.visible_children

    def test_is_self_or_descendant(self, db: DatabaseTransactionFixture):
        # Test the code that checks whether one WorkList is 'beneath'
        # another.

        class WorkListWithParent(WorkList):
            # A normal WorkList never has a parent; this subclass
            # makes it possible to explicitly set a WorkList's parent
            # and get its parentage.
            #
            # This way we can test WorkList code without bringing in Lane.
            def __init__(self):
                self._parent = None

            @property
            def parent(self):
                return self._parent

            @property
            def parentage(self):
                if not self._parent:
                    return []
                return [self._parent] + list(self._parent.parentage)

        # A WorkList matches itself.
        child = WorkListWithParent()
        child.initialize(db.default_library())
        assert True == child.is_self_or_descendant(child)

        # But not any other WorkList.
        parent = WorkListWithParent()
        parent.initialize(db.default_library())
        assert False == child.is_self_or_descendant(parent)

        grandparent = WorkList()
        grandparent.initialize(db.default_library())
        assert False == child.is_self_or_descendant(grandparent)

        # Unless it's a descendant of that WorkList.
        child._parent = parent
        parent._parent = grandparent
        assert True == child.is_self_or_descendant(parent)
        assert True == child.is_self_or_descendant(grandparent)
        assert True == parent.is_self_or_descendant(grandparent)

        assert False == parent.is_self_or_descendant(child)
        assert False == grandparent.is_self_or_descendant(parent)

    def test_accessible_to(self, db: DatabaseTransactionFixture):
        # Test the circumstances under which a Patron may or may not access a
        # WorkList.

        wl = WorkList()
        wl.initialize(db.default_library())

        # A WorkList is always accessible to unauthenticated users.
        m = wl.accessible_to
        assert True == m(None)

        # A WorkList is never accessible to patrons of a different library.
        other_library = db.library()
        other_library_patron = db.patron(library=other_library)
        assert False == m(other_library_patron)

        # A WorkList is always accessible to patrons with no root lane
        # set.
        patron = db.patron()
        assert True == m(patron)

        # Give the patron a root lane.
        lane = db.lane()
        lane.root_for_patron_type = ["1"]
        patron.external_type = "1"

        # Now that the patron has a root lane, WorkLists will become
        # inaccessible if they might contain content not
        # age-appropriate for that patron (as gauged by their root
        # lane).

        # As initialized, our worklist has no audience restrictions.
        assert True == m(patron)

        # Give it some audience restrictions.
        wl.audiences = [Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_CHILDREN]
        wl.target_age = tuple_to_numericrange((4, 5))

        # Now it depends on the return value of Patron.work_is_age_appropriate.
        # Mock that method.
        patron.work_is_age_appropriate = MagicMock(return_value=False)

        # Since our mock returns false, so does accessible_to
        assert False == m(patron)

        # work_is_age_appropriate was called once, with the
        # WorkList's target age and its first audience restriction.
        # When work_is_age_appropriate returned False, it short-circuited
        # the process and no second call was made.
        patron.work_is_age_appropriate.assert_called_once_with(
            wl.audiences[0], wl.target_age
        )

        # If we tell work_is_age_appropriate to always return true...
        patron.work_is_age_appropriate = MagicMock(return_value=True)

        # ...accessible_to starts returning True.
        assert True == m(patron)

        # The mock method was called once for each audience
        # restriction in our WorkList. Only if _every_ call returns
        # True is the WorkList considered age-appropriate for the
        # patron.
        patron.work_is_age_appropriate.assert_has_calls(
            [
                call(wl.audiences[0], wl.target_age),
                call(wl.audiences[1], wl.target_age),
            ]
        )

    def test_uses_customlists(self, db: DatabaseTransactionFixture):
        """A WorkList is said to use CustomLists if either ._customlist_ids
        or .list_datasource_id is set.
        """
        wl = WorkList()
        wl.initialize(db.default_library())
        assert False == wl.uses_customlists

        wl._customlist_ids = object()
        assert True == wl.uses_customlists

        wl._customlist_ids = None
        wl.list_datasource_id = object()
        assert True == wl.uses_customlists

    def test_max_cache_age(self):
        # By default, the maximum cache age of an OPDS feed based on a
        # WorkList is the default cache age for any type of OPDS feed,
        # no matter what type of feed is being generated.
        wl = WorkList()
        assert OPDSFeed.DEFAULT_MAX_AGE == wl.max_cache_age()

    def test_filter(self, db: DatabaseTransactionFixture):
        # Verify that filter() calls modify_search_filter_hook()
        # and can handle either a new Filter being returned or a Filter
        # modified in place.

        class ModifyInPlace(WorkList):
            # A WorkList that modifies its search filter in place.
            def modify_search_filter_hook(self, filter):
                filter.hook_called = True

        wl = ModifyInPlace()
        wl.initialize(db.default_library())
        facets = SearchFacets()
        filter = wl.filter(db.session, facets)
        assert isinstance(filter, Filter)
        assert True == filter.hook_called  # type: ignore[attr-defined]

        class NewFilter(WorkList):
            # A WorkList that returns a brand new Filter
            def modify_search_filter_hook(self, filter):
                return "A brand new Filter"

        new_filter = NewFilter()
        new_filter.initialize(db.default_library())
        facets = SearchFacets()
        filter = new_filter.filter(db.session, facets)
        assert "A brand new Filter" == filter

    def test_groups(
        self,
        db: DatabaseTransactionFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        w1 = MockWork(1)
        w2 = MockWork(2)
        w3 = MockWork(3)

        class MockWorkList:
            def __init__(self, works):
                self._works = works
                self.visible = True

            def groups(self, *args, **kwargs):
                for i in self._works:
                    yield i, self

        # This WorkList has one featured work.
        child1 = MockWorkList([w1])

        # This WorkList has two featured works.
        child2 = MockWorkList([w2, w1])

        # This WorkList has two children -- the two WorkLists created
        # above.
        wl = WorkList()
        wl.initialize(db.default_library(), children=[child1, child2])

        # Calling groups() on the parent WorkList returns three
        # 2-tuples; one for each work featured by one of its children
        # WorkLists. Note that the same work appears twice, through two
        # different children.
        [wwl1, wwl2, wwl3] = wl.groups(
            db.session, search_engine=external_search_fake_fixture.external_search
        )
        assert (w1, child1) == wwl1
        assert (w2, child2) == wwl2
        assert (w1, child2) == wwl3

    def test_groups_propagates_facets(self, db: DatabaseTransactionFixture):
        # Verify that the Facets object passed into groups() is
        # propagated to the methods called by groups().
        class MockWorkList(WorkList):
            overview_facets_called_with = None

            def works(self, _db, pagination, facets):
                self.works_called_with = (pagination, facets)
                return []

            def overview_facets(self, _db, facets):
                self.overview_facets_called_with = facets
                return "A new faceting object"

            def _groups_for_lanes(
                self,
                _db,
                relevant_children,
                relevant_lanes,
                pagination,
                facets,
                **kwargs,
            ):
                self._groups_for_lanes_called_with = (pagination, facets)
                return []

        mock = MockWorkList()
        mock.initialize(library=db.default_library())
        facets = object()

        # First, try the situation where we're trying to make a grouped feed
        # out of the (imaginary) sublanes of this lane.
        [x for x in mock.groups(db.session, facets=facets)]

        # overview_facets() was not called.
        assert None == mock.overview_facets_called_with

        # The _groups_for_lanes() method was called with the
        # (imaginary) list of sublanes and the original faceting
        # object. No pagination was provided. The _groups_for_lanes()
        # implementation is responsible for giving each sublane a
        # chance to adapt that faceting object to its own needs.
        assert (None, facets) == mock._groups_for_lanes_called_with
        mock._groups_for_lanes_called_with = None

        # Now try the case where we want to use a pagination object to
        # restrict the number of results per lane.
        pagination = object()
        [x for x in mock.groups(db.session, pagination=pagination, facets=facets)]
        # The pagination object is propagated to _groups_for_lanes.
        assert (pagination, facets) == mock._groups_for_lanes_called_with
        mock._groups_for_lanes_called_with = None

        # Now try the situation where we're just trying to get _part_ of
        # a grouped feed -- the part for which this lane is responsible.
        [x for x in mock.groups(db.session, facets=facets, include_sublanes=False)]
        # Now, the original faceting object was passed into
        # overview_facets().
        assert facets == mock.overview_facets_called_with

        # And the return value of overview_facets() was passed into
        # works()
        assert (None, "A new faceting object") == mock.works_called_with

        # _groups_for_lanes was not called.
        assert None == mock._groups_for_lanes_called_with

    def test_works(self, db: DatabaseTransactionFixture):
        # Test the method that uses the search index to fetch a list of
        # results appropriate for a given WorkList.

        class MockSearchClient:
            """Respond to search requests with some fake work IDs."""

            fake_work_ids = [1, 10, 100, 1000]

            def query_works(self, **kwargs):
                self.called_with = kwargs
                return self.fake_work_ids

        class MockWorkList(WorkList):
            """Mock the process of turning work IDs into WorkSearchResult
            objects."""

            fake_work_list = "a list of works"

            def works_for_hits(self, _db, work_ids, facets=None):
                self.called_with = (_db, work_ids)
                return self.fake_work_list

        # Here's a WorkList.
        wl = MockWorkList()
        wl.initialize(db.default_library(), languages=["eng"])
        facets = Facets(
            db.default_library(),
            None,
            None,
            order=Facets.ORDER_TITLE,
            distributor=None,
            collection_name=None,
        )
        mock_pagination = object()
        mock_debug = object()
        search_client = MockSearchClient()

        # Ask the WorkList for a page of works, using the search index
        # to drive the query instead of the database.
        result = wl.works(
            db.session, facets, mock_pagination, search_client, mock_debug
        )

        # MockSearchClient.query_works was used to grab a list of work
        # IDs.
        query_works_kwargs = search_client.called_with

        # Our facets and the requirements of the WorkList were used to
        # make a Filter object, which was passed as the 'filter'
        # keyword argument.
        filter = query_works_kwargs.pop("filter")
        assert Filter.from_worklist(db.session, wl, facets).build() == filter.build()

        # The other arguments to query_works are either constants or
        # our mock objects.
        assert (
            dict(query_string=None, pagination=mock_pagination, debug=mock_debug)
            == query_works_kwargs
        )

        # The fake work IDs returned from query_works() were passed into
        # works_for_hits().
        assert (db.session, search_client.fake_work_ids) == wl.called_with

        # And the fake return value of works_for_hits() was used as
        # the return value of works(), the method we're testing.
        assert wl.fake_work_list == result

    def test_works_for_hits(self, db: DatabaseTransactionFixture):
        # Verify that WorkList.works_for_hits() just calls
        # works_for_resultsets().
        class Mock(WorkList):
            def works_for_resultsets(self, _db, resultsets, facets=None):
                self.called_with = (_db, resultsets)
                return [["some", "results"]]

        wl = Mock()
        results = wl.works_for_hits(db.session, ["hit1", "hit2"])

        # The list of hits was itself wrapped in a list, and passed
        # into works_for_resultsets().
        assert (db.session, [["hit1", "hit2"]]) == wl.called_with

        # The return value -- a list of lists of results, which
        # contained a single item -- was unrolled and used as the
        # return value of works_for_hits().
        assert ["some", "results"] == results

    def test_works_for_resultsets(self, db: DatabaseTransactionFixture):
        # Verify that WorkList.works_for_resultsets turns lists of
        # (mocked) Hit objects into lists of Work or WorkSearchResult
        # objects.

        # Create the WorkList we'll be testing with.
        wl = WorkList()
        wl.initialize(db.default_library())
        m = wl.works_for_resultsets

        # Create two works.
        w1 = db.work(with_license_pool=True)
        w2 = db.work(with_license_pool=True)

        class MockHit:
            def __init__(self, work_id, has_last_update=False):
                if isinstance(work_id, Work):
                    self.work_id = work_id.id
                else:
                    self.work_id = work_id
                self.has_last_update = has_last_update

            def __contains__(self, k):
                # Pretend to have the 'last_update' script field,
                # if necessary.
                return k == "last_update" and self.has_last_update

        hit1 = MockHit(w1)
        hit2 = MockHit(w2)

        # For each list of hits passed in, a corresponding list of
        # Works is returned.
        assert [[w2]] == m(db.session, [[hit2]])
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])
        assert [[w1, w1], [w2, w2], []] == m(
            db.session, [[hit1, hit1], [hit2, hit2], []]
        )

        # Works are returned in the order we ask for.
        for ordering in ([hit1, hit2], [hit2, hit1]):
            [works] = m(db.session, [ordering])
            assert [x.work_id for x in ordering] == [x.id for x in works]

        # If we ask for a work ID that's not in the database,
        # we don't get it.
        assert [[]] == m(db.session, [[MockHit(-100)]])

        # If we pass in Hit objects that have extra information in them,
        # we get WorkSearchResult objects
        hit1_extra = MockHit(w1, True)
        hit2_extra = MockHit(w2, True)

        [results] = m(db.session, [[hit2_extra, hit1_extra]])
        assert all(isinstance(x, WorkSearchResult) for x in results)
        r1, r2 = results

        # These WorkSearchResult objects wrap Work objects together
        # with the corresponding Hit objects.
        assert w2 == r1._work
        assert hit2_extra == r1._hit

        assert w1 == r2._work
        assert hit1_extra == r2._hit

        # Finally, test that undeliverable works are filtered out.
        for lpdm in w2.license_pools[0].delivery_mechanisms:
            db.session.delete(lpdm)
            assert [[]] == m(db.session, [[hit2]])

    def test_search_target(self):
        # A WorkList can be searched - it is its own search target.
        wl = WorkList()
        assert wl == wl.search_target

    def test_search(self, db: DatabaseTransactionFixture):
        # Test the successful execution of WorkList.search()

        class MockWorkList(WorkList):
            def works_for_hits(self, _db, work_ids):
                self.works_for_hits_called_with = (_db, work_ids)
                return "A bunch of Works"

        wl = MockWorkList()
        wl.initialize(db.default_library(), audiences=[Classifier.AUDIENCE_CHILDREN])
        query = "a query"

        class MockSearchClient:
            def query_works(self, query, filter, pagination, debug):
                self.query_works_called_with = (query, filter, pagination, debug)
                return "A bunch of work IDs"

        # Search with the default arguments.
        client = MockSearchClient()
        results = wl.search(db.session, query, client)

        # The results of query_works were passed into
        # MockWorkList.works_for_hits.
        assert (db.session, "A bunch of work IDs") == wl.works_for_hits_called_with

        # The return value of MockWorkList.works_for_hits is
        # used as the return value of query_works().
        assert "A bunch of Works" == results

        # From this point on we are only interested in the arguments
        # passed in to query_works, since MockSearchClient always
        # returns the same result.

        # First, let's see what the default arguments look like.
        qu, filter, pagination, debug = client.query_works_called_with

        # The query was passed through.
        assert query == qu
        assert False == debug

        # A Filter object was created to match only works that belong
        # in the MockWorkList.
        assert [
            Classifier.AUDIENCE_CHILDREN,
            Classifier.AUDIENCE_ALL_AGES,
        ] == filter.audiences

        # A default Pagination object was created.
        assert 0 == pagination.offset
        assert Pagination.DEFAULT_SEARCH_SIZE == pagination.size

        # Now let's try a search with specific Pagination and Facets
        # objects.
        facets = SearchFacets(languages=["chi"])
        pagination = object()
        results = wl.search(db.session, query, client, pagination, facets, debug=True)

        qu, filter, pag, debug = client.query_works_called_with
        assert query == qu
        assert pagination == pag
        assert True == debug

        # The Filter incorporates restrictions imposed by both the
        # MockWorkList and the Facets.
        assert [
            Classifier.AUDIENCE_CHILDREN,
            Classifier.AUDIENCE_ALL_AGES,
        ] == filter.audiences
        assert ["chi"] == filter.languages

    def test_search_failures(self, db: DatabaseTransactionFixture):
        # Test reasons why WorkList.search() might not work.
        wl = WorkList()
        wl.initialize(db.default_library())
        query = "a query"

        # If there is no SearchClient, there are no results.
        assert [] == wl.search(db.session, query, None)

        # If the SearchClient returns nothing, there are no results.
        class NoResults:
            def query_works(self, *args, **kwargs):
                return None

        assert [] == wl.search(db.session, query, NoResults())

        # If there's an Opensearch exception during the query,
        # there are no results.
        class RaisesException:
            def query_works(self, *args, **kwargs):
                raise OpenSearchException("oh no")

        assert [] == wl.search(db.session, query, RaisesException())

    def test_worklist_for_resultset_no_holds_allowed(
        self, db: DatabaseTransactionFixture
    ):
        wl = WorkList()
        wl.initialize(db.default_library())
        m = wl.works_for_resultsets

        # Create two works.
        w1: Work = db.work(with_license_pool=True)
        w2: Work = db.work(with_license_pool=True)

        w1.license_pools[0].licenses_available = 0
        collection1: Collection = w1.license_pools[0].collection
        cs1 = ConfigurationSetting(
            library_id=db.default_library().id,
            external_integration_id=collection1.external_integration_id,
            key=ExternalIntegration.DISPLAY_RESERVES,
            _value="no",
        )
        db.session.add(cs1)
        db.session.commit()

        class MockHit:
            def __init__(self, work_id, has_last_update=False):
                if isinstance(work_id, Work):
                    self.work_id = work_id.id
                else:
                    self.work_id = work_id
                self.has_last_update = has_last_update

            def __contains__(self, k):
                # Pretend to have the 'last_update' script field,
                # if necessary.
                return k == "last_update" and self.has_last_update

        hit1 = MockHit(w1)
        hit2 = MockHit(w2)

        # Basic test
        # For each list of hits passed in, a corresponding list of
        # Works is returned.
        assert [[w2]] == m(db.session, [[hit2]])
        assert [[w2], []] == m(db.session, [[hit2], [hit1]])
        assert [[], [w2, w2], []] == m(db.session, [[hit1, hit1], [hit2, hit2], []])

        # Restricted pool has availability
        w1.license_pools[0].licenses_available = 1
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

        # Revert back, no availablility
        w1.license_pools[0].licenses_available = 0

        # Work1 now has 2 licensepools, one of which has availability
        alternate_collection = db.collection()
        db.default_library().collections.append(alternate_collection)
        alternate_w1_lp: LicensePool = db.licensepool(
            w1.presentation_edition, collection=alternate_collection
        )
        alternate_w1_lp.work_id = w1.id
        db.session.add_all([alternate_collection, alternate_w1_lp])
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

        # Still show availability since alternate collection is not restricted
        alternate_w1_lp.licenses_available = 0
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])

        # Now both collections are restricted and have no availability
        cs2 = ConfigurationSetting(
            library_id=db.default_library().id,
            external_integration_id=alternate_collection.external_integration_id,
            key=ExternalIntegration.DISPLAY_RESERVES,
            _value="no",
        )
        db.session.add(cs2)
        assert [[w2], []] == m(db.session, [[hit2], [hit1]])

        # Both restricted but one has availability
        alternate_w1_lp.licenses_available = 1
        assert [[w2], [w1]] == m(db.session, [[hit2], [hit1]])


class TestDatabaseBackedWorkList:
    def test_works_from_database(self, db: DatabaseTransactionFixture):
        # Verify that the works_from_database() method calls the
        # methods we expect, in the right order.
        class MockQuery:
            # Simulates the behavior of a database Query object
            # without the need to pass around actual database clauses.
            #
            # This is a lot of instrumentation but it means we can
            # test what happened inside works() mainly by looking at a
            # string of method names in the result object.
            def __init__(self, clauses, distinct=False):
                self.clauses = clauses
                self._distinct = distinct

            def filter(self, clause):
                # Create a new MockQuery object with a new clause
                return MockQuery(self.clauses + [clause], self._distinct)

            def distinct(self, fields):
                return MockQuery(self.clauses, fields)

            def __repr__(self):
                return "<MockQuery %d clauses, most recent %s>" % (
                    len(self.clauses),
                    self.clauses[-1],
                )

        class MockWorkList(DatabaseBackedWorkList):
            def __init__(self, _db):
                super().__init__()
                session = _db  # We'll be using this in assertions.
                self.stages = []

            def _stage(self, method_name, _db, qu, qu_is_previous_stage=True):
                # _db must always be session; check it here and then
                # ignore it.
                assert _db == db.session

                if qu_is_previous_stage:
                    # qu must be the MockQuery returned from the
                    # previous call.
                    assert qu == self.stages[-1]
                else:
                    # qu must be a new object, and _not_ the MockQuery
                    # returned from the previous call.
                    assert qu != self.stages[-1]

                # Create a new MockQuery with an additional filter,
                # named after the method that was called.
                new_filter = qu.filter(method_name)
                self.stages.append(new_filter)
                return new_filter

            def base_query(self, _db):
                # This kicks off the process -- most future calls will
                # use _stage().
                assert _db == db.session
                query = MockQuery(["base_query"])
                self.stages.append(query)
                return query

            def only_show_ready_deliverable_works(self, _db, qu):
                return self._stage("only_show_ready_deliverable_works", _db, qu)

            def _restrict_query_for_no_hold_collections(self, _db, qu):
                return self._stage("_restrict_query_for_no_hold_collections", _db, qu)

            def bibliographic_filter_clauses(self, _db, qu):
                # This method is a little different, so we can't use
                # _stage().
                #
                # This implementation doesn't change anything; it will be
                # replaced with an implementation that does.
                assert _db == db.session
                self.bibliographic_filter_clauses_called_with = qu
                return qu, []

            def modify_database_query_hook(self, _db, qu):
                return self._stage("modify_database_query_hook", _db, qu)

            def active_bibliographic_filter_clauses(self, _db, qu):
                # This alternate implementation of
                # bibliographic_filter_clauses returns a brand new
                # MockQuery object and a list of filters.
                self.pre_bibliographic_filter = qu
                new_query = MockQuery(
                    ["new query made inside active_bibliographic_filter_clauses"]
                )
                self.stages.append(new_query)
                return (new_query, [text("clause 1"), text("clause 2")])

        # The simplest case: no facets or pagination,
        # and bibliographic_filter_clauses does nothing.
        wl = MockWorkList(db.session)
        result = wl.works_from_database(db.session, extra_kwarg="ignored")

        # We got a MockQuery.
        assert isinstance(result, MockQuery)

        # During the course of the works() call, we verified that the
        # MockQuery is constructed by chaining method calls.  Now we
        # just need to verify that all the methods were called and in
        # the order we expect.
        assert [
            "base_query",
            "only_show_ready_deliverable_works",
            "_restrict_query_for_no_hold_collections",
            "modify_database_query_hook",
        ] == result.clauses

        # bibliographic_filter_clauses used a different mechanism, but
        # since it stored the MockQuery it was called with, we can see
        # when it was called -- just after
        # only_show_ready_deliverable_works.
        assert [
            "base_query",
            "only_show_ready_deliverable_works",
            "_restrict_query_for_no_hold_collections",
        ] == wl.bibliographic_filter_clauses_called_with.clauses
        wl.bibliographic_filter_clauses_called_with = None

        # Since nobody made the query distinct, it was set distinct on
        # Work.id.
        assert Work.id == result._distinct

        # Now we're going to do a more complicated test, with
        # faceting, pagination, and a bibliographic_filter_clauses that
        # actually does something.
        wl.bibliographic_filter_clauses = wl.active_bibliographic_filter_clauses

        class MockFacets(DatabaseBackedFacets):
            def __init__(self, wl):
                self.wl = wl

            def modify_database_query(self, _db, qu):
                # This is the only place we pass in False for
                # qu_is_previous_stage. This is called right after
                # bibliographic_filter_clauses, which caused a brand
                # new MockQuery object to be created.
                #
                # Normally, _stage() will assert that `qu` is the
                # return value from the previous call, but this time
                # we want to assert the opposite.
                result = self.wl._stage("facets", _db, qu, qu_is_previous_stage=False)

                distinct = result.distinct("some other field")
                self.wl.stages.append(distinct)
                return distinct

        class MockPagination:
            def __init__(self, wl):
                self.wl = wl

            def modify_database_query(self, _db, qu):
                return self.wl._stage("pagination", _db, qu)

        result = wl.works_from_database(
            db.session, facets=MockFacets(wl), pagination=MockPagination(wl)
        )

        # Here are the methods called before bibliographic_filter_clauses.
        assert [
            "base_query",
            "only_show_ready_deliverable_works",
            "_restrict_query_for_no_hold_collections",
        ] == wl.pre_bibliographic_filter.clauses

        # bibliographic_filter_clauses created a brand new object,
        # which ended up as our result after some more methods were
        # called on it.
        assert (
            "new query made inside active_bibliographic_filter_clauses"
            == result.clauses.pop(0)
        )

        # bibliographic_filter_clauses() returned two clauses which were
        # combined with and_().
        bibliographic_filter_clauses = result.clauses.pop(0)
        assert str(and_(text("clause 1"), text("clause 2"))) == str(
            bibliographic_filter_clauses
        )

        # The rest of the calls are easy to trac.
        assert [
            "facets",
            "modify_database_query_hook",
            "pagination",
        ] == result.clauses

        # The query was made distinct on some other field, so the
        # default behavior (making it distinct on Work.id) wasn't
        # triggered.
        assert "some other field" == result._distinct

    def test_works_from_database_end_to_end(self, db: DatabaseTransactionFixture):
        # Verify that works_from_database() correctly locates works
        # that match the criteria specified by the
        # DatabaseBackedWorkList, the faceting object, and the
        # pagination object.
        #
        # This is a simple end-to-end test of functionality that's
        # tested in more detail elsewhere.

        # Create two books.
        oliver_twist = db.work(
            title="Oliver Twist", with_license_pool=True, language="eng"
        )
        barnaby_rudge = db.work(
            title="Barnaby Rudge", with_license_pool=True, language="spa"
        )

        # A standard DatabaseBackedWorkList will find both books.
        wl = DatabaseBackedWorkList()
        wl.initialize(db.default_library())
        assert 2 == wl.works_from_database(db.session).count()

        # A work list with a language restriction will only find books
        # in that language.
        wl.initialize(db.default_library(), languages=["eng"])
        assert [oliver_twist] == [x for x in wl.works_from_database(db.session)]

        # A DatabaseBackedWorkList will only find books licensed
        # through one of its collections.
        collection = db.collection()
        db.default_library().collections = [collection]
        wl.initialize(db.default_library())
        assert 0 == wl.works_from_database(db.session).count()

        # If a DatabaseBackedWorkList has no collections, it has no
        # books.
        db.default_library().collections = []
        wl.initialize(db.default_library())
        assert 0 == wl.works_from_database(db.session).count()

        # A DatabaseBackedWorkList can be set up with a collection
        # rather than a library. TODO: The syntax here could be improved.
        wl = DatabaseBackedWorkList()
        wl.initialize(None)
        wl.collection_ids = [db.default_collection().id]
        assert None == wl.get_library(db.session)
        assert 2 == wl.works_from_database(db.session).count()

        # Facets and pagination can affect which entries and how many
        # are returned.
        facets = DatabaseBackedFacets(
            db.default_library(),
            collection=Facets.COLLECTION_FULL,
            availability=Facets.AVAILABLE_ALL,
            order=Facets.ORDER_TITLE,
            distributor=None,
            collection_name=None,
        )
        pagination = Pagination(offset=1, size=1)
        assert [oliver_twist] == wl.works_from_database(
            db.session, facets, pagination
        ).all()

        facets.order_ascending = False
        assert [barnaby_rudge] == wl.works_from_database(
            db.session, facets, pagination
        ).all()

        # Ensure that availability facets are handled properly
        # We still have two works:
        # - barnaby_rudge is closed access and available
        # - oliver_twist's access and availability is varied below
        ot_lp = oliver_twist.license_pools[0]

        # open access (thus available)
        ot_lp.open_access = True

        facets.availability = Facets.AVAILABLE_ALL
        assert 2 == wl.works_from_database(db.session, facets).count()

        facets.availability = Facets.AVAILABLE_NOW
        assert 2 == wl.works_from_database(db.session, facets).count()

        facets.availability = Facets.AVAILABLE_OPEN_ACCESS
        assert 1 == wl.works_from_database(db.session, facets).count()
        assert [oliver_twist] == wl.works_from_database(db.session, facets).all()

        # closed access & unavailable
        ot_lp.open_access = False
        ot_lp.licenses_owned = 1
        ot_lp.licenses_available = 0

        facets.availability = Facets.AVAILABLE_ALL
        assert 2 == wl.works_from_database(db.session, facets).count()

        facets.availability = Facets.AVAILABLE_NOW
        assert 1 == wl.works_from_database(db.session, facets).count()
        assert [barnaby_rudge] == wl.works_from_database(db.session, facets).all()

        facets.availability = Facets.AVAILABLE_OPEN_ACCESS
        assert 0 == wl.works_from_database(db.session, facets).count()

    def test_base_query(self, db: DatabaseTransactionFixture):
        # Verify that base_query makes the query we expect and then
        # calls some optimization methods (not tested).
        class Mock(DatabaseBackedWorkList):
            @classmethod
            def _modify_loading(cls, qu):
                return [qu, "_modify_loading"]

        result = Mock.base_query(db.session)

        [base_query, m] = result
        expect = (
            db.session.query(Work)
            .join(Work.license_pools)
            .join(Work.presentation_edition)
            .filter(LicensePool.superceded == False)
        )
        assert str(expect) == str(base_query)
        assert "_modify_loading" == m

    def test_bibliographic_filter_clauses(self, db: DatabaseTransactionFixture):
        called = dict()

        class MockWorkList(DatabaseBackedWorkList):
            """Verifies that bibliographic_filter_clauses() calls various hook
            methods.

            The hook methods themselves are tested separately.
            """

            def __init__(self, parent):
                super().__init__()
                self._parent = parent
                self._inherit_parent_restrictions = False

            def audience_filter_clauses(self, _db, qu):
                called["audience_filter_clauses"] = (_db, qu)
                return []

            def customlist_filter_clauses(self, qu):
                called["customlist_filter_clauses"] = qu
                return qu, []

            def age_range_filter_clauses(self):
                called["age_range_filter_clauses"] = True
                return []

            def genre_filter_clause(self, qu):
                called["genre_filter_clause"] = qu
                return qu, None

            @property
            def parent(self):
                return self._parent

            @property
            def inherit_parent_restrictions(self):
                return self._inherit_parent_restrictions

        class MockParent:
            bibliographic_filter_clauses_called_with = None

            def bibliographic_filter_clauses(self, _db, qu):
                self.bibliographic_filter_clauses_called_with = (_db, qu)
                return qu, []

        parent = MockParent()

        # Create a MockWorkList with a parent.
        wl = MockWorkList(parent)
        wl.initialize(db.default_library())
        original_qu = DatabaseBackedWorkList.base_query(db.session)

        # If no languages or genre IDs are specified, and the hook
        # methods do nothing, then bibliographic_filter_clauses() has
        # no effect.
        final_qu, clauses = wl.bibliographic_filter_clauses(db.session, original_qu)
        assert original_qu == final_qu
        assert [] == clauses

        # But at least the apply_audience_filter was called with the correct
        # arguments.
        _db, qu = called["audience_filter_clauses"]
        assert db.session == _db
        assert original_qu == qu

        # age_range_filter_clauses was also called.
        assert True == called["age_range_filter_clauses"]

        # customlist_filter_clauses and genre_filter_clause were not
        # called because the WorkList doesn't do anything relating to
        # custom lists.
        assert "customlist_filter_clauses" not in called
        assert "genre_filter_clause" not in called

        # The parent's bibliographic_filter_clauses() implementation
        # was not called, because wl.inherit_parent_restrictions is
        # set to False.
        assert None == parent.bibliographic_filter_clauses_called_with

        # Set things up so that those other methods will be called.
        empty_list, ignore = db.customlist(num_entries=0)
        sf, ignore = Genre.lookup(db.session, "Science Fiction")
        wl.initialize(db.default_library(), customlists=[empty_list], genres=[sf])
        wl._inherit_parent_restrictions = True

        final_qu, clauses = wl.bibliographic_filter_clauses(db.session, original_qu)

        assert (
            (db.session),
            original_qu,
        ) == parent.bibliographic_filter_clauses_called_with
        assert original_qu == called["genre_filter_clause"]
        assert original_qu == called["customlist_filter_clauses"]

        # But none of those methods changed anything, because their
        # implementations didn't return anything.
        assert [] == clauses

        # Now test the clauses that are created directly by
        # bibliographic_filter_clauses.
        overdrive = DataSource.lookup(db.session, DataSource.OVERDRIVE)
        wl.initialize(
            db.default_library(),
            languages=["eng"],
            media=[Edition.BOOK_MEDIUM],
            fiction=True,
            license_datasource=overdrive,
        )

        final_qu, clauses = wl.bibliographic_filter_clauses(db.session, original_qu)
        assert original_qu == final_qu
        language, medium, fiction, datasource = clauses

        # NOTE: str() doesn't prove that the values are the same, only
        # that the constraints are similar.
        assert str(language) == str(Edition.language.in_(wl.languages))
        assert str(medium) == str(Edition.medium.in_(wl.media))
        assert str(fiction) == str(Work.fiction == True)
        assert str(datasource) == str(LicensePool.data_source_id == overdrive.id)

    def test_bibliographic_filter_clauses_end_to_end(
        self, db: DatabaseTransactionFixture
    ):
        # Verify that bibliographic_filter_clauses generates
        # SQLAlchemy clauses that give the expected results when
        # applied to a real `works` table.
        original_qu = DatabaseBackedWorkList.base_query(db.session)

        # Create a work that may or may not show up in various
        # DatabaseBackedWorkLists.
        sf, ignore = Genre.lookup(db.session, "Science Fiction")
        english_sf = db.work(
            title="English SF",
            language="eng",
            with_license_pool=True,
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
        )
        italian_sf = db.work(
            title="Italian SF",
            language="ita",
            with_license_pool=True,
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
        )
        english_sf.target_age = tuple_to_numericrange((12, 14))
        gutenberg = english_sf.license_pools[0].data_source
        english_sf.presentation_edition.medium = Edition.BOOK_MEDIUM
        english_sf.genres.append(sf)
        italian_sf.genres.append(sf)

        def worklist_has_books(expect_books, worklist=None, **initialize_kwargs):
            """Apply bibliographic filters to a query and verify
            that it finds only the given books.
            """
            if worklist is None:
                worklist = DatabaseBackedWorkList()
                worklist.initialize(db.default_library(), **initialize_kwargs)
            qu, clauses = worklist.bibliographic_filter_clauses(db.session, original_qu)
            qu = qu.filter(and_(*clauses))
            expect_titles = sorted(x.sort_title for x in expect_books)
            actual_titles = sorted(x.sort_title for x in qu)
            assert expect_titles == actual_titles

        # A WorkList will find a book only if all restrictions
        # are met.
        worklist_has_books(
            [english_sf],
            languages=["eng"],
            genres=[sf],
            media=[Edition.BOOK_MEDIUM],
            fiction=True,
            license_datasource=gutenberg,
            audiences=[Classifier.AUDIENCE_YOUNG_ADULT],
            target_age=tuple_to_numericrange((13, 13)),
        )

        # This might be because there _are_ no restrictions.
        worklist_has_books([english_sf, italian_sf], fiction=None)

        # DatabaseBackedWorkLists with a contradictory setting for one
        # of the fields associated with the English SF book will not
        # find it.
        worklist_has_books([italian_sf], languages=["ita"], genres=[sf])
        romance, ignore = Genre.lookup(db.session, "Romance")
        worklist_has_books([], languages=["eng"], genres=[romance])
        worklist_has_books(
            [], languages=["eng"], genres=[sf], media=[Edition.AUDIO_MEDIUM]
        )
        worklist_has_books([], fiction=False)
        worklist_has_books(
            [], license_datasource=DataSource.lookup(db.session, DataSource.OVERDRIVE)
        )

        # If the WorkList has custom list IDs, then works will only show up if
        # they're on one of the matching CustomLists.
        sf_list, ignore = db.customlist(num_entries=0)
        sf_list.add_entry(english_sf)
        sf_list.add_entry(italian_sf)

        worklist_has_books([english_sf, italian_sf], customlists=[sf_list])

        empty_list, ignore = db.customlist(num_entries=0)
        worklist_has_books([], customlists=[empty_list])

        # Test parent restrictions.
        #
        # Ordinary DatabaseBackedWorkLists can't inherit restrictions
        # from their parent (TODO: no reason not to implement this)
        # but Lanes can, so let's use Lanes for the rest of this test.

        # This lane has books from a list of English books.
        english_list, ignore = db.customlist(num_entries=0)
        english_list.add_entry(english_sf)
        english_lane = db.lane()
        english_lane.customlists.append(english_list)

        # This child of that lane has books from the list of SF books.
        sf_lane = db.lane(parent=english_lane, inherit_parent_restrictions=False)
        sf_lane.customlists.append(sf_list)

        # When the child lane does not inherit its parent restrictions,
        # both SF books show up.
        worklist_has_books([english_sf, italian_sf], sf_lane)

        # When the child inherits its parent's restrictions, only the
        # works that are on _both_ lists show up in the lane,
        sf_lane.inherit_parent_restrictions = True
        worklist_has_books([english_sf], sf_lane)

        # Other restrictions are inherited as well. Here, a title must
        # show up on both lists _and_ be a nonfiction book. There are
        # no titles that meet all three criteria.
        sf_lane.fiction = False
        worklist_has_books([], sf_lane)

        sf_lane.fiction = True
        worklist_has_books([english_sf], sf_lane)

        # Parent restrictions based on genre can also be inherited.
        #

        # Here's a lane that finds only short stories.
        short_stories, ignore = Genre.lookup(db.session, "Short Stories")
        short_stories_lane = db.lane(genres=["Short Stories"])

        # Here's a child of that lane, which contains science fiction.
        sf_shorts = db.lane(
            genres=[sf], parent=short_stories_lane, inherit_parent_restrictions=False
        )
        db.session.flush()

        # Without the parent restriction in place, all science fiction
        # shows up in sf_shorts.
        worklist_has_books([english_sf, italian_sf], sf_shorts)

        # With the parent restriction in place, a book must be classified
        # under both science fiction and short stories to show up.
        sf_shorts.inherit_parent_restrictions = True
        worklist_has_books([], sf_shorts)
        english_sf.genres.append(short_stories)
        worklist_has_books([english_sf], sf_shorts)

    def test_age_range_filter_clauses_end_to_end(self, db: DatabaseTransactionFixture):
        # Standalone test of age_range_filter_clauses().
        def worklist_has_books(expect, **wl_args):
            """Make a DatabaseBackedWorkList and find all the works
            that match its age_range_filter_clauses.
            """
            wl = DatabaseBackedWorkList()
            wl.initialize(db.default_library(), **wl_args)
            qu = db.session.query(Work)
            clauses = wl.age_range_filter_clauses()
            qu = qu.filter(and_(*clauses))
            assert set(expect) == set(qu.all())

        adult = db.work(
            title="For adults",
            audience=Classifier.AUDIENCE_ADULT,
            with_license_pool=True,
        )
        assert None == adult.target_age
        fourteen_or_fifteen = db.work(
            title="For teens",
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
            with_license_pool=True,
        )
        fourteen_or_fifteen.target_age = tuple_to_numericrange((14, 15))

        # This DatabaseBackedWorkList contains the YA book because its
        # age range overlaps the age range of the book.
        worklist_has_books([fourteen_or_fifteen], target_age=(12, 14))

        worklist_has_books(
            [adult, fourteen_or_fifteen],
            audiences=[Classifier.AUDIENCE_ADULT],
            target_age=(12, 14),
        )

        # This lane contains no books because it skews too old for the YA
        # book, but books for adults are not allowed.
        older_ya = db.lane()
        older_ya.target_age = (16, 17)
        worklist_has_books([], target_age=(16, 17))

        # Expand it to include books for adults, and the adult book
        # shows up despite having no target age at all.
        worklist_has_books([adult], target_age=(16, 18))

    def test_audience_filter_clauses(self, db: DatabaseTransactionFixture):
        # Verify that audience_filter_clauses restricts a query to
        # reflect a DatabaseBackedWorkList's audience filter.

        # Create a children's book and a book for adults.
        adult = db.work(
            title="Diseases of the Horse",
            with_license_pool=True,
            with_open_access_download=True,
            audience=Classifier.AUDIENCE_ADULT,
        )

        children = db.work(
            title="Wholesome Nursery Rhymes For All Children",
            with_license_pool=True,
            with_open_access_download=True,
            audience=Classifier.AUDIENCE_CHILDREN,
        )

        def for_audiences(*audiences):
            """Invoke audience_filter_clauses using the given
            `audiences`, and return all the matching Work objects.
            """
            wl = DatabaseBackedWorkList()
            wl.audiences = audiences
            qu = wl.base_query(db.session)
            clauses = wl.audience_filter_clauses(db.session, qu)
            if clauses:
                qu = qu.filter(and_(*clauses))
            return qu.all()

        assert [adult] == for_audiences(Classifier.AUDIENCE_ADULT)
        assert [children] == for_audiences(Classifier.AUDIENCE_CHILDREN)

        # If no particular audiences are specified, no books are filtered.
        assert {adult, children} == set(for_audiences())

    def test_customlist_filter_clauses(self, db: DatabaseTransactionFixture):
        # Standalone test of customlist_filter_clauses

        # If a lane has nothing to do with CustomLists,
        # apply_customlist_filter does nothing.
        no_lists = DatabaseBackedWorkList()
        no_lists.initialize(db.default_library())
        qu = no_lists.base_query(db.session)
        new_qu, clauses = no_lists.customlist_filter_clauses(qu)
        assert qu == new_qu
        assert [] == clauses

        # Now set up a Work and a CustomList that contains the work.
        work = db.work(with_license_pool=True)
        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)
        assert gutenberg == work.license_pools[0].data_source
        gutenberg_list, ignore = db.customlist(num_entries=0)
        gutenberg_list.data_source = gutenberg
        gutenberg_list_entry, ignore = gutenberg_list.add_entry(work)

        # This DatabaseBackedWorkList gets every work on a specific list.
        works_on_list = DatabaseBackedWorkList()
        works_on_list.initialize(db.default_library(), customlists=[gutenberg_list])

        # This lane gets every work on every list associated with Project
        # Gutenberg.
        works_on_gutenberg_lists = DatabaseBackedWorkList()
        works_on_gutenberg_lists.initialize(
            db.default_library(), list_datasource=gutenberg
        )

        def _run(qu, clauses):
            # Run a query with certain clauses
            return qu.filter(and_(*clauses)).all()

        def results(wl=works_on_gutenberg_lists, must_be_featured=False):
            qu = wl.base_query(db.session)
            new_qu, clauses = wl.customlist_filter_clauses(qu)

            # The query comes out different than it goes in -- there's a
            # new join against CustomListEntry.
            assert new_qu != qu
            return _run(new_qu, clauses)

        # Both lanes contain the work.
        assert [work] == results(works_on_list)
        assert [work] == results(works_on_gutenberg_lists)

        # If there's another list with the same work on it, the
        # work only shows up once.
        gutenberg_list_2, ignore = db.customlist(num_entries=0)
        gutenberg_list_2_entry, ignore = gutenberg_list_2.add_entry(work)
        works_on_list._customlist_ids.append(gutenberg_list.id)
        assert [work] == results(works_on_list)

        # This WorkList gets every work on a list associated with Overdrive.
        # There are no such lists, so the lane is empty.
        overdrive = DataSource.lookup(db.session, DataSource.OVERDRIVE)
        works_on_overdrive_lists = DatabaseBackedWorkList()
        works_on_overdrive_lists.initialize(
            db.default_library(), list_datasource=overdrive
        )
        assert [] == results(works_on_overdrive_lists)

        # It's possible to restrict a WorkList to works that were seen on
        # a certain list recently.
        now = utc_now()
        two_days_ago = now - datetime.timedelta(days=2)
        gutenberg_list_entry.most_recent_appearance = two_days_ago

        # The lane will only show works that were seen within the last
        # day. There are no such works.
        works_on_gutenberg_lists.list_seen_in_previous_days = 1
        assert [] == results()

        # Now it's been loosened to three days, and the work shows up.
        works_on_gutenberg_lists.list_seen_in_previous_days = 3
        assert [work] == results()

        # Now let's test what happens when we chain calls to this
        # method.
        gutenberg_list_2_wl = DatabaseBackedWorkList()
        gutenberg_list_2_wl.initialize(
            db.default_library(), customlists=[gutenberg_list_2]
        )

        # These two lines won't work, because these are
        # DatabaseBackedWorkLists, not Lanes, but they show the
        # scenario in which this would actually happen. When
        # determining which works belong in the child lane,
        # Lane.customlist_filter_clauses() will be called on the
        # parent lane and then on the child. In this case, only want
        # books that are on _both_ works_on_list and gutenberg_list_2.
        #
        # TODO: There's no reason WorkLists shouldn't be able to have
        # parents and inherit parent restrictions.
        #
        # gutenberg_list_2_wl.parent = works_on_list
        # gutenberg_list_2_wl.inherit_parent_restrictions = True

        qu = works_on_list.base_query(db.session)
        list_1_qu, list_1_clauses = works_on_list.customlist_filter_clauses(qu)

        # The query has been modified -- we've added a join against
        # CustomListEntry.
        assert list_1_qu != qu
        assert [work] == list_1_qu.all()

        # Now call customlist_filter_clauses again so that the query
        # must only match books on _both_ lists. This simulates
        # what happens when the second lane is a child of the first,
        # and inherits its restrictions.
        both_lists_qu, list_2_clauses = gutenberg_list_2_wl.customlist_filter_clauses(
            list_1_qu,
        )
        # The query has been modified again -- we've added a second join
        # against CustomListEntry.
        assert both_lists_qu != list_1_qu
        both_lists_clauses = list_1_clauses + list_2_clauses

        # The combined query matches the work that shows up on
        # both lists.
        assert [work] == _run(both_lists_qu, both_lists_clauses)

        # If we remove `work` from either list, the combined query
        # matches nothing.
        for l in [gutenberg_list, gutenberg_list_2]:
            l.remove_entry(work)
            assert [] == _run(both_lists_qu, both_lists_clauses)
            l.add_entry(work)

    def test_works_from_database_with_superceded_pool(
        self, db: DatabaseTransactionFixture
    ):
        work = db.work(with_license_pool=True)
        work.license_pools[0].superceded = True
        ignore, pool = db.edition(with_license_pool=True)
        work.license_pools.append(pool)
        db.session.commit()

        lane = db.lane()
        [w] = lane.works_from_database(db.session).all()
        # Only one pool is loaded, and it's the non-superceded one.
        assert [pool] == w.license_pools


class TestHierarchyWorkList:
    """Test HierarchyWorkList in terms of its two subclasses, Lane and TopLevelWorkList."""

    def test_accessible_to(self, db: DatabaseTransactionFixture):
        # In addition to the general tests imposed by WorkList, a Lane
        # is only accessible to a patron if it is a descendant of
        # their root lane.
        lane = db.lane()
        patron = db.patron()
        lane.root_for_patron_type = ["1"]
        patron.external_type = "1"

        # Descendant -> it's accessible
        m = lane.accessible_to
        lane.is_self_or_descendant = MagicMock(return_value=True)
        assert True == m(patron)

        # Not a descendant -> it's not accessible
        lane.is_self_or_descendant = MagicMock(return_value=False)
        assert False == m(patron)

        # If the patron has no root lane, is_self_or_descendant
        # isn't consulted -- everything is accessible.
        patron.external_type = "2"
        assert True == m(patron)

        # Similarly if there is no authenticated patron.
        assert True == m(None)

        # TopLevelWorkList works the same way -- it's visible unless the
        # patron has a top-level lane set.
        wl = TopLevelWorkList()
        wl.initialize(db.default_library())

        assert True == wl.accessible_to(None)
        assert True == wl.accessible_to(patron)
        patron.external_type = "1"
        assert False == wl.accessible_to(patron)

        # However, a TopLevelWorkList associated with library A is not
        # visible to a patron from library B.
        library2 = db.library()
        wl.initialize(library2)
        patron.external_type = None
        assert False == wl.accessible_to(patron)


class TestLane:
    def test_get_library(self, db: DatabaseTransactionFixture):
        lane = db.lane()
        assert db.default_library() == lane.get_library(db.session)

    def test_list_datasource(self, db: DatabaseTransactionFixture):
        """Test setting and retrieving the DataSource object and
        the underlying ID.
        """
        lane = db.lane()

        # This lane is based on a specific CustomList.
        customlist1, ignore = db.customlist(num_entries=0)
        customlist2, ignore = db.customlist(num_entries=0)
        lane.customlists.append(customlist1)
        assert None == lane.list_datasource
        assert None == lane.list_datasource_id
        assert [customlist1.id] == lane.customlist_ids

        # Now change it so it's based on all CustomLists from a given
        # DataSource.
        source = customlist1.data_source
        lane.list_datasource = source
        assert source == lane.list_datasource
        assert source.id == lane.list_datasource_id

        # The lane is now based on two CustomLists instead of one.
        assert {customlist1.id, customlist2.id} == set(lane.customlist_ids)

    def test_set_audiences(self, db: DatabaseTransactionFixture):
        """Setting Lane.audiences to a single value will
        auto-convert it into a list containing one value.
        """
        lane = db.lane()
        lane.audiences = Classifier.AUDIENCE_ADULT
        assert [Classifier.AUDIENCE_ADULT] == lane.audiences

    def test_update_size(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        class Mock:
            # Mock the ExternalSearchIndex.count_works() method to
            # return specific values without consulting an actual
            # search index.
            def count_works(self, filter):
                values_by_medium = {
                    None: 102,
                    Edition.AUDIO_MEDIUM: 3,
                    Edition.BOOK_MEDIUM: 99,
                }
                if filter.media:
                    [medium] = filter.media
                else:
                    medium = None
                return values_by_medium[medium]

        search_engine = Mock()

        # Make a lane with some incorrect values that will be fixed by
        # update_size().
        fiction = db.lane(display_name="Fiction", fiction=True)
        fiction.size = 44
        fiction.size_by_entrypoint = {"Nonexistent entrypoint": 33}
        with mock_search_index(search_engine):
            fiction.update_size(db.session)

        # The lane size is also calculated individually for every
        # enabled entry point. EverythingEntryPoint is used for the
        # total size of the lane.
        assert {
            AudiobooksEntryPoint.URI: 3,
            EbooksEntryPoint.URI: 99,
            EverythingEntryPoint.URI: 102,
        } == fiction.size_by_entrypoint
        assert 102 == fiction.size

    def test_visibility(self, db: DatabaseTransactionFixture):
        parent = db.lane()
        visible_child = db.lane(parent=parent)
        invisible_child = db.lane(parent=parent)
        invisible_child.visible = False
        assert [visible_child] == list(parent.visible_children)

        grandchild = db.lane(parent=invisible_child)
        assert True == parent.visible
        assert True == visible_child.visible
        assert False == invisible_child.visible

        # The grandchild lane is set to visible in the database, but
        # it is not visible because its parent is not visible.
        assert True == grandchild._visible
        assert False == grandchild.visible

    def test_parentage(self, db: DatabaseTransactionFixture):
        worklist = WorkList()
        worklist.display_name = "A WorkList"
        lane = db.lane()
        child_lane = db.lane(parent=lane)
        grandchild_lane = db.lane(parent=child_lane)
        unrelated = db.lane()

        # A WorkList has no parentage.
        assert [] == list(worklist.parentage)
        assert "A WorkList" == worklist.full_identifier

        # The WorkList has the Lane as a child, but the Lane doesn't know
        # this.
        assert [] == list(lane.parentage)
        assert [lane] == list(child_lane.parentage)
        assert (
            f"{lane.library.short_name} / {lane.display_name}" == lane.full_identifier
        )

        assert (
            "%s / %s / %s / %s"
            % (
                lane.library.short_name,
                lane.display_name,
                child_lane.display_name,
                grandchild_lane.display_name,
            )
            == grandchild_lane.full_identifier
        )

        assert [lane, child_lane, grandchild_lane] == grandchild_lane.hierarchy

        # TODO: The error should be raised when we try to set the parent
        # to an illegal value, not afterwards.
        lane.parent = child_lane
        with pytest.raises(ValueError) as excinfo:
            list(lane.parentage)
        assert "Lane parentage loop detected" in str(excinfo.value)

    def test_is_self_or_descendant(self, db: DatabaseTransactionFixture):
        # Test the code that checks whether one Lane is 'beneath'
        # a WorkList.

        top_level = TopLevelWorkList()
        top_level.initialize(db.default_library())
        parent = db.lane()
        child = db.lane(parent=parent)

        # Generally this works the same as WorkList.is_self_or_descendant.
        assert True == parent.is_self_or_descendant(parent)
        assert True == child.is_self_or_descendant(child)

        assert True == child.is_self_or_descendant(parent)
        assert False == parent.is_self_or_descendant(child)

        # The big exception: a TopLevelWorkList is a descendant of any
        # Lane so long as they belong to the same library.
        assert True == child.is_self_or_descendant(top_level)
        assert True == parent.is_self_or_descendant(top_level)

        library2 = db.library()
        top_level.initialize(library2)
        assert False == child.is_self_or_descendant(top_level)
        assert False == parent.is_self_or_descendant(top_level)

    def test_depth(self, db: DatabaseTransactionFixture):
        child = db.lane("sublane")
        parent = db.lane("parent")
        parent.sublanes.append(child)
        assert 0 == parent.depth
        assert 1 == child.depth

    def test_url_name(self, db: DatabaseTransactionFixture):
        lane = db.lane("Fantasy / Science Fiction")
        assert lane.id == lane.url_name

    def test_display_name_for_all(self, db: DatabaseTransactionFixture):
        lane = db.lane("Fantasy / Science Fiction")
        assert "All Fantasy / Science Fiction" == lane.display_name_for_all

    def test_entrypoints(self, db: DatabaseTransactionFixture):
        """Currently a Lane can never have entrypoints."""
        assert [] == db.lane().entrypoints

    def test_affected_by_customlist(self, db: DatabaseTransactionFixture):
        # Two lists.
        l1, ignore = db.customlist(data_source_name=DataSource.GUTENBERG, num_entries=0)
        l2, ignore = db.customlist(data_source_name=DataSource.OVERDRIVE, num_entries=0)

        # A lane populated by specific lists.
        lane = db.lane()

        # Not affected by any lists.
        for l in [l1, l2]:
            assert 0 == Lane.affected_by_customlist(l1).count()

        # Add a lane to the list, and it becomes affected.
        lane.customlists.append(l1)
        assert [lane] == lane.affected_by_customlist(l1).all()
        assert 0 == lane.affected_by_customlist(l2).count()
        lane.customlists = []

        # A lane based on all lists with the GUTENBERG db source.
        lane2 = db.lane()
        lane2.list_datasource = l1.data_source

        # It's affected by the GUTENBERG list but not the OVERDRIVE
        # list.
        assert [lane2] == Lane.affected_by_customlist(l1).all()
        assert 0 == Lane.affected_by_customlist(l2).count()

    def test_inherited_value(self, db: DatabaseTransactionFixture):
        # Test WorkList.inherited_value.
        #
        # It's easier to test this in Lane because WorkLists can't have
        # parents.

        # This lane contains fiction.
        fiction_lane = db.lane(fiction=True)

        # This sublane contains nonfiction.
        nonfiction_sublane = db.lane(parent=fiction_lane, fiction=False)
        nonfiction_sublane.inherit_parent_restrictions = False

        # This sublane doesn't specify a value for .fiction.
        default_sublane = db.lane(parent=fiction_lane)
        default_sublane.inherit_parent_restrictions = False

        # When inherit_parent_restrictions is False,
        # inherited_value("fiction") returns whatever value is set for
        # .fiction.
        assert None == default_sublane.inherited_value("fiction")
        assert False == nonfiction_sublane.inherited_value("fiction")

        # When inherit_parent_restrictions is True,
        # inherited_value("fiction") returns False for the sublane
        # that sets no value for .fiction.
        default_sublane.inherit_parent_restrictions = True
        assert True == default_sublane.inherited_value("fiction")

        # The sublane that sets its own value for .fiction is unaffected.
        nonfiction_sublane.inherit_parent_restrictions = True
        assert False == nonfiction_sublane.inherited_value("fiction")

    def test_inherited_values(self, db: DatabaseTransactionFixture):
        # Test WorkList.inherited_values.
        #
        # It's easier to test this in Lane because WorkLists can't have
        # parents.

        # This lane contains best-sellers.
        best_sellers_lane = db.lane()
        best_sellers, ignore = db.customlist(num_entries=0)
        best_sellers_lane.customlists.append(best_sellers)

        # This sublane contains staff picks.
        staff_picks_lane = db.lane(parent=best_sellers_lane)
        staff_picks, ignore = db.customlist(num_entries=0)
        staff_picks_lane.customlists.append(staff_picks)

        # What does it mean that the 'staff picks' lane is *inside*
        # the 'best sellers' lane?

        # If inherit_parent_restrictions is False, it doesn't mean
        # anything in particular. This lane contains books that
        # are on the staff picks list.
        staff_picks_lane.inherit_parent_restrictions = False
        assert [[staff_picks]] == staff_picks_lane.inherited_values("customlists")

        # If inherit_parent_restrictions is True, then the lane
        # has *two* sets of restrictions: a book must be on both
        # the staff picks list *and* the best sellers list.
        staff_picks_lane.inherit_parent_restrictions = True
        x = staff_picks_lane.inherited_values("customlists")
        assert sorted([[staff_picks], [best_sellers]]) == sorted(
            staff_picks_lane.inherited_values("customlists")
        )

    def test_setting_target_age_locks_audiences(self, db: DatabaseTransactionFixture):
        lane = db.lane()
        lane.target_age = (16, 18)
        assert sorted(
            [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_ADULT]
        ) == sorted(lane.audiences)
        lane.target_age = (0, 2)
        assert [Classifier.AUDIENCE_CHILDREN] == lane.audiences
        lane.target_age = 14
        assert [Classifier.AUDIENCE_YOUNG_ADULT] == lane.audiences

        # It's not possible to modify .audiences to a value that's
        # incompatible with .target_age.
        lane.audiences = lane.audiences

        def doomed():
            lane.audiences = [Classifier.AUDIENCE_CHILDREN]

        with pytest.raises(ValueError) as excinfo:
            doomed()
        assert "Cannot modify Lane.audiences when Lane.target_age is set" in str(
            excinfo.value
        )

        # Setting target_age to None leaves preexisting .audiences in place.
        lane.target_age = None
        assert [Classifier.AUDIENCE_YOUNG_ADULT] == lane.audiences

        # But now you can modify .audiences.
        lane.audiences = [Classifier.AUDIENCE_CHILDREN]

    def test_target_age_treats_all_adults_equally(self, db: DatabaseTransactionFixture):
        """We don't distinguish between different age groups for adults."""
        lane = db.lane()
        lane.target_age = (35, 40)
        assert tuple_to_numericrange((18, 18)) == lane.target_age

    def test_uses_customlists(self, db: DatabaseTransactionFixture):
        lane = db.lane()
        assert False == lane.uses_customlists

        customlist, ignore = db.customlist(num_entries=0)
        lane.customlists = [customlist]
        assert True == lane.uses_customlists

        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)
        lane.list_datasource = gutenberg
        db.session.commit()
        assert True == lane.uses_customlists

        # Note that the specific custom list was removed from this
        # Lane when it switched to using all lists from a certain db
        # source.
        assert [] == lane.customlists

        # A Lane may use custom lists by virtue of inheriting
        # restrictions from its parent.
        child = db.lane(parent=lane)
        child.inherit_parent_restrictions = True
        assert True == child.uses_customlists

    def test_genre_ids(self, db: DatabaseTransactionFixture):
        # By default, when you add a genre to a lane, you are saying
        # that Works classified under it and all its subgenres should
        # show up in the lane.
        fantasy = db.lane()
        fantasy.add_genre("Fantasy")

        # At this point the lane picks up Fantasy and all of its
        # subgenres.
        expect = [
            Genre.lookup(db.session, genre)[0].id
            for genre in [
                "Fantasy",
                "Epic Fantasy",
                "Historical Fantasy",
                "Urban Fantasy",
            ]
        ]
        assert set(expect) == fantasy.genre_ids

        # Let's exclude one of the subgenres.
        fantasy.add_genre("Urban Fantasy", inclusive=False)
        urban_fantasy, ignore = Genre.lookup(db.session, "Urban Fantasy")
        # That genre's ID has disappeared from .genre_ids.
        assert urban_fantasy.id not in fantasy.genre_ids

        # Let's add Science Fiction, but not its subgenres.
        fantasy.add_genre("Science Fiction", recursive=False)
        science_fiction, ignore = Genre.lookup(db.session, "Science Fiction")
        space_opera, ignore = Genre.lookup(db.session, "Space Opera")
        assert science_fiction.id in fantasy.genre_ids
        assert space_opera.id not in fantasy.genre_ids

        # Let's add Space Opera, but exclude Science Fiction and its
        # subgenres.
        fantasy.lane_genres = []
        fantasy.add_genre("Space Opera")
        fantasy.add_genre("Science Fiction", inclusive=False, recursive=True)

        # That eliminates everything.
        assert set() == fantasy.genre_ids

        # NOTE: We don't have any doubly nested subgenres, so we can't
        # test the case where a genre is included recursively but one
        # of its subgenres is exclused recursively (in which case the
        # sub-subgenre would be excluded), but it should work.

        # We can exclude a genre even when no genres are explicitly included.
        # The lane will include all genres that aren't excluded.
        no_inclusive_genres = db.lane()
        no_inclusive_genres.add_genre("Science Fiction", inclusive=False)
        assert len(no_inclusive_genres.genre_ids) > 10
        assert science_fiction.id not in no_inclusive_genres.genre_ids

    def test_customlist_ids(self, db: DatabaseTransactionFixture):
        # WorkLists always return None for customlist_ids.
        wl = WorkList()
        wl.initialize(db.default_library())
        assert None == wl.customlist_ids

        # When you add a CustomList to a Lane, you are saying that works
        # from that CustomList can appear in the Lane.
        nyt1, ignore = db.customlist(num_entries=0, data_source_name=DataSource.NYT)
        nyt2, ignore = db.customlist(num_entries=0, data_source_name=DataSource.NYT)

        no_lists = db.lane()
        assert None == no_lists.customlist_ids

        has_list = db.lane()
        has_list.customlists.append(nyt1)
        assert [nyt1.id] == has_list.customlist_ids

        # When you set a Lane's list_datasource, you're saying that
        # works appear in the Lane if they are on _any_ CustomList from
        # that db source.
        has_list_source = db.lane()
        has_list_source.list_datasource = DataSource.lookup(db.session, DataSource.NYT)
        assert {nyt1.id, nyt2.id} == set(has_list_source.customlist_ids)

        # If there are no CustomLists from that db source, an empty
        # list is returned.
        has_no_lists = db.lane()
        has_no_lists.list_datasource = DataSource.lookup(
            db.session, DataSource.OVERDRIVE
        )
        assert [] == has_no_lists.customlist_ids

    def test_search_target(self, db: DatabaseTransactionFixture):
        # A Lane that is the root for a patron type can be
        # searched.
        root_lane = db.lane()
        root_lane.root_for_patron_type = ["A"]
        assert root_lane == root_lane.search_target

        # A Lane that's the descendant of a root Lane for a
        # patron type will search that root Lane.
        child = db.lane(parent=root_lane)
        assert root_lane == child.search_target

        grandchild = db.lane(parent=child)
        assert root_lane == grandchild.search_target

        # Any Lane that does not descend from a root Lane will
        # get a WorkList as its search target, with some
        # restrictions from the Lane.
        lane = db.lane()

        lane.languages = ["eng", "ger"]
        target = lane.search_target
        assert "English/Deutsch" == target.display_name
        assert ["eng", "ger"] == target.languages
        assert None == target.audiences
        assert None == target.media

        # If there are too many languages, they're left out of the
        # display name (so the search description will be "Search").
        lane.languages = ["eng", "ger", "spa", "fre"]
        target = lane.search_target
        assert "" == target.display_name
        assert ["eng", "ger", "spa", "fre"] == target.languages
        assert None == target.audiences
        assert None == target.media

        lane.languages = ["eng"]
        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert None == target.audiences
        assert None == target.media

        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert None == target.audiences
        assert None == target.media

        # Media aren't included in the description, but they
        # are used in search.
        lane.media = [Edition.BOOK_MEDIUM]
        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert None == target.audiences
        assert [Edition.BOOK_MEDIUM] == target.media

        # Audiences are only used in search if one of the
        # audiences is young adult or children.
        lane.audiences = [Classifier.AUDIENCE_ADULTS_ONLY]
        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert None == target.audiences
        assert [Edition.BOOK_MEDIUM] == target.media

        lane.audiences = [Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_YOUNG_ADULT]
        target = lane.search_target
        assert "English Adult and Young Adult" == target.display_name
        assert ["eng"] == target.languages
        assert [
            Classifier.AUDIENCE_ADULT,
            Classifier.AUDIENCE_YOUNG_ADULT,
        ] == target.audiences
        assert [Edition.BOOK_MEDIUM] == target.media

        # If there are too many audiences, they're left
        # out of the display name.
        lane.audiences = [
            Classifier.AUDIENCE_ADULT,
            Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_CHILDREN,
        ]
        target = lane.search_target
        assert "English" == target.display_name
        assert ["eng"] == target.languages
        assert [
            Classifier.AUDIENCE_ADULT,
            Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_CHILDREN,
        ] == target.audiences
        assert [Edition.BOOK_MEDIUM] == target.media

    def test_search(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # Searching a Lane calls search() on its search_target.
        #
        # TODO: This test could be trimmed down quite a bit with
        # mocks.

        work = db.work(with_license_pool=True)

        lane = db.lane()
        search_client = end_to_end_search_fixture.external_search_index
        docs = end_to_end_search_fixture.external_search_index.start_migration()
        assert docs is not None
        docs.add_documents(search_client.create_search_documents_from_works([work]))
        docs.finish()

        pagination = Pagination(offset=0, size=1)

        results = lane.search(
            db.session, work.title, search_client, pagination=pagination
        )
        target_results = lane.search_target.search(
            db.session, work.title, search_client, pagination=pagination
        )
        assert results == target_results

        # The single search result was returned as a Work.
        [result] = results
        assert work == result

        # This still works if the lane is its own search_target.
        lane.root_for_patron_type = ["A"]
        results = lane.search(
            db.session, work.title, search_client, pagination=pagination
        )
        target_results = lane.search_target.search(
            db.session, work.title, search_client, pagination=pagination
        )
        assert results == target_results

    def test_search_propagates_facets(self, db: DatabaseTransactionFixture):
        """Lane.search propagates facets when calling search() on
        its search target.
        """

        class Mock:
            def search(self, *args, **kwargs):
                self.called_with = kwargs["facets"]

        mock = Mock()
        lane = db.lane()

        old_lane_search_target = Lane.search_target
        old_wl_search = WorkList.search
        Lane.search_target = mock  # type: ignore[method-assign, assignment]
        facets = SearchFacets()
        lane.search(db.session, "query", None, facets=facets)
        assert facets == mock.called_with

        # Now try the case where a lane is its own search target.  The
        # Facets object is propagated to the WorkList.search().
        mock.called_with = None
        Lane.search_target = lane
        WorkList.search = mock.search
        lane.search(db.session, "query", None, facets=facets)
        assert facets == mock.called_with

        # Restore methods that were mocked.
        Lane.search_target = old_lane_search_target
        WorkList.search = old_wl_search

    def test_explain(self, db: DatabaseTransactionFixture):
        parent = db.lane(display_name="Parent")
        parent.priority = 1
        child = db.lane(parent=parent, display_name="Child")
        child.priority = 2
        data = parent.explain()
        assert [
            "ID: %s" % parent.id,
            "Library: %s" % db.default_library().short_name,
            "Priority: 1",
            "Display name: Parent",
        ] == data

        data = child.explain()
        assert [
            "ID: %s" % child.id,
            "Library: %s" % db.default_library().short_name,
            "Parent ID: %s (Parent)" % parent.id,
            "Priority: 2",
            "Display name: Child",
        ] == data

    def test_groups_propagates_facets(self, db: DatabaseTransactionFixture):
        # Lane.groups propagates a received Facets object into
        # _groups_for_lanes.
        def mock(self, _db, relevant_lanes, queryable_lanes, facets, *args, **kwargs):
            self.called_with = facets
            return []

        old_value = Lane._groups_for_lanes
        Lane._groups_for_lanes = mock  # type: ignore[method-assign, assignment]
        lane = db.lane()
        facets = FeaturedFacets(0)
        lane.groups(db.session, facets=facets)
        assert facets == lane.called_with
        Lane._groups_for_lanes = old_value

    def test_suppress(self, db: DatabaseTransactionFixture):
        lane1 = db.lane()
        lane2 = db.lane()

        # Updating the flag on one lane does not impact others
        lane1._suppress_before_flush_listeners = True
        assert lane1._suppress_before_flush_listeners is True
        assert lane2._suppress_before_flush_listeners is False


class TestWorkListGroupsEndToEndData:
    best_seller_list: CustomList
    hq_litfic: Work
    hq_ro: Work
    hq_sf: Work
    lq_litfic: Work
    lq_ro: Work
    lq_sf: Work
    mq_ro: Work
    mq_sf: Work
    nonfiction: Work
    children_with_age: Work
    children_without_age: Work
    staff_picks_list: CustomList


class TestWorkListGroupsEndToEnd:
    # A comprehensive end-to-end test of WorkList.groups()
    # using a real Opensearch index.
    #
    # Helper methods are tested in a different class, TestWorkListGroups

    @staticmethod
    def populate_works(
        end_to_end_search_fixture: EndToEndSearchFixture,
    ) -> TestWorkListGroupsEndToEndData:
        fixture = end_to_end_search_fixture
        data, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )

        def _w(**kwargs):
            """Helper method to create a work with license pool."""
            return data.work(with_license_pool=True, **kwargs)

        result = TestWorkListGroupsEndToEndData()

        # Create eight works.
        result.hq_litfic = _w(title="HQ LitFic", fiction=True, genre="Literary Fiction")
        result.hq_litfic.quality = 0.8
        result.lq_litfic = _w(title="LQ LitFic", fiction=True, genre="Literary Fiction")
        result.lq_litfic.quality = 0
        result.hq_sf = _w(title="HQ SF", genre="Science Fiction", fiction=True)

        # Create children works.
        result.children_with_age = _w(
            title="Children work with target age",
            audience=Classifier.AUDIENCE_CHILDREN,
        )
        result.children_with_age.target_age = tuple_to_numericrange((0, 3))

        result.children_without_age = _w(
            title="Children work with out target age",
            audience=Classifier.AUDIENCE_CHILDREN,
        )

        # Add a lot of irrelevant genres to one of the works. This
        # won't affect the results.
        for genre in ["Westerns", "Horror", "Erotica"]:
            genre_obj, is_new = Genre.lookup(session, genre)
            get_one_or_create(session, WorkGenre, work=result.hq_sf, genre=genre_obj)

        result.hq_sf.quality = 0.8
        result.mq_sf = _w(title="MQ SF", genre="Science Fiction", fiction=True)
        result.mq_sf.quality = 0.6
        result.lq_sf = _w(title="LQ SF", genre="Science Fiction", fiction=True)
        result.lq_sf.quality = 0.1
        result.hq_ro = _w(title="HQ Romance", genre="Romance", fiction=True)
        result.hq_ro.quality = 0.79
        result.mq_ro = _w(title="MQ Romance", genre="Romance", fiction=True)
        result.mq_ro.quality = 0.6
        # This work is in a different language -- necessary to run the
        # LQRomanceEntryPoint test below.
        result.lq_ro = _w(
            title="LQ Romance", genre="Romance", fiction=True, language="lan"
        )
        result.lq_ro.quality = 0.1
        result.nonfiction = _w(title="Nonfiction", fiction=False)

        # One of these works (mq_sf) is a best-seller and also a staff
        # pick.
        result.best_seller_list, ignore = data.customlist(num_entries=0)
        result.best_seller_list.add_entry(result.mq_sf)

        result.staff_picks_list, ignore = data.customlist(num_entries=0)
        result.staff_picks_list.add_entry(result.mq_sf)
        return result

    def test_groups(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        library_fixture: LibraryFixture,
    ):
        fixture = end_to_end_search_fixture
        db, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )
        fixture.external_search_index.start_migration().finish()  # type: ignore [union-attr]

        # Tell the fixture to call our populate_works method.
        # In this library, the groups feed includes at most two books
        # for each lane.
        library = db.default_library()
        library_settings = library_fixture.settings(library)
        library_settings.featured_lane_size = 2
        data = self.populate_works(fixture)
        fixture.populate_search_index()

        # Create a 'Fiction' lane with five sublanes.
        fiction = db.lane("Fiction")
        fiction.fiction = True

        # "Best Sellers", which will contain one book.
        best_sellers = db.lane("Best Sellers", parent=fiction)
        best_sellers.customlists.append(data.best_seller_list)

        # "Staff Picks", which will contain the same book.
        staff_picks = db.lane("Staff Picks", parent=fiction)
        staff_picks.customlists.append(data.staff_picks_list)

        # "Science Fiction", which will contain two books (but
        # will not contain the best-seller).
        sf_lane = db.lane("Science Fiction", parent=fiction, genres=["Science Fiction"])

        # "Romance", which will contain two books.
        romance_lane = db.lane("Romance", parent=fiction, genres=["Romance"])

        # "Discredited Nonfiction", which contains a book that would
        # not normally appear in 'Fiction'.
        discredited_nonfiction = db.lane(
            "Discredited Nonfiction", fiction=False, parent=fiction
        )
        discredited_nonfiction.inherit_parent_restrictions = False

        # "Children", which will contain one book, the one with audience children and defined target age.
        children = db.lane("Children")
        children.audiences = Classifier.AUDIENCE_CHILDREN
        children.target_age = (0, 4)

        # Since we have a bunch of lanes and works, plus an
        # Opensearch index, let's take this opportunity to verify that
        # WorkList.works and DatabaseBackedWorkList.works_from_database
        # give the same results.
        facets = DatabaseBackedFacets(
            db.default_library(),
            collection=Facets.COLLECTION_FULL,
            availability=Facets.AVAILABLE_ALL,
            order=Facets.ORDER_TITLE,
            distributor=None,
            collection_name=None,
        )
        for lane in [
            fiction,
            best_sellers,
            staff_picks,
            sf_lane,
            romance_lane,
            discredited_nonfiction,
            children,
        ]:
            t1 = [
                x.id
                for x in lane.works(
                    session,
                    facets,
                    search_engine=end_to_end_search_fixture.external_search_index,
                )
            ]
            t2 = [x.id for x in lane.works_from_database(session, facets)]
            print(f"t1: {t1}")
            print(f"t2: {t2}")
            assert t1 == t2

        def assert_contents(g, expect):
            """Assert that a generator yields the expected
            (Work, lane) 2-tuples.
            """
            results = list(g)
            expect = [(x[0].sort_title, x[1].display_name) for x in expect]
            actual = [(x[0].sort_title, x[1].display_name) for x in results]
            for i, expect_item in enumerate(expect):
                if i >= len(actual):
                    actual_item = None
                else:
                    actual_item = actual[i]
                assert expect_item == actual_item, (
                    "Mismatch in position %d: Expected %r, got %r.\nOverall, expected:\n%r\nGot:\n%r:"
                    % (i, expect_item, actual_item, expect, actual)
                )
            assert len(expect) == len(actual), (
                "Expect matches actual, but actual has extra members.\nOverall, expected:\n%r\nGot:\n%r:"
                % (expect, actual)
            )

        def make_groups(lane, facets=None, **kwargs):
            # Run the `WorkList.groups` method in a way that's
            # instrumented for this unit test.

            # Most of the time, we want a simple deterministic query.
            facets = facets or FeaturedFacets(1, random_seed=Filter.DETERMINISTIC)

            return lane.groups(
                session,
                facets=facets,
                search_engine=fixture.external_search_index,
                debug=True,
                **kwargs,
            )

        assert_contents(
            make_groups(fiction),
            [
                # The lanes based on lists feature every title on the
                # list.  This isn't enough to pad out the lane to
                # FEATURED_LANE_SIZE, but nothing else belongs in the
                # lane.
                (data.mq_sf, best_sellers),
                # In fact, both lanes feature the same title -- this
                # generally won't happen but it can happen when
                # multiple lanes are based on lists that feature the
                # same title.
                (data.mq_sf, staff_picks),
                # The genre-based lanes contain FEATURED_LANE_SIZE
                # (two) titles each. The 'Science Fiction' lane
                # features a low-quality work because the
                # medium-quality work was already used above.
                (data.hq_sf, sf_lane),
                (data.lq_sf, sf_lane),
                (data.hq_ro, romance_lane),
                (data.mq_ro, romance_lane),
                # The 'Discredited Nonfiction' lane contains a single
                # book. There just weren't enough matching books to fill
                # out the lane to FEATURED_LANE_SIZE.
                (data.nonfiction, discredited_nonfiction),
                # The 'Fiction' lane contains a title that fits in the
                # fiction lane but was not classified under any other
                # lane. It also contains a title that was previously
                # featured earlier. The search index knows about a
                # title (lq_litfix) that was not previously featured,
                # but we didn't see it because the Opensearch query
                # didn't happen to fetch it.
                #
                # Each lane gets a separate query, and there were too
                # many high-quality works in 'fiction' for the
                # low-quality one to show up.
                (data.hq_litfic, fiction),
                (data.hq_sf, fiction),
            ],
        )

        # If we ask only about 'Fiction', not including its sublanes,
        # we get only the subset of the books previously returned for
        # 'fiction'.
        assert_contents(
            make_groups(fiction, include_sublanes=False),
            [
                (data.hq_litfic, fiction),
                (data.hq_sf, fiction),
            ],
        )

        # If we exclude 'Fiction' from its own grouped feed, we get
        # all the other books/lane combinations *except for* the books
        # associated directly with 'Fiction'.
        fiction.include_self_in_grouped_feed = False
        assert_contents(
            make_groups(fiction),
            [
                (data.mq_sf, best_sellers),
                (data.mq_sf, staff_picks),
                (data.hq_sf, sf_lane),
                (data.lq_sf, sf_lane),
                (data.hq_ro, romance_lane),
                (data.mq_ro, romance_lane),
                (data.nonfiction, discredited_nonfiction),
            ],
        )
        fiction.include_self_in_grouped_feed = True

        # When a lane has no sublanes, its behavior is the same whether
        # it is called with include_sublanes true or false.
        for include_sublanes in (True, False):
            assert_contents(
                discredited_nonfiction.groups(
                    session, include_sublanes=include_sublanes
                ),
                [(data.nonfiction, discredited_nonfiction)],
            )

        # When a lane's audience is "Children" we need work to have explicit target_age to be included in the lane
        assert_contents(
            make_groups(children),
            [(data.children_with_age, children)],
        )

        # If we make the lanes thirstier for content, we see slightly
        # different behavior.
        library_settings.featured_lane_size = 3
        assert_contents(
            make_groups(fiction),
            [
                # The list-based lanes are the same as before.
                (data.mq_sf, best_sellers),
                (data.mq_sf, staff_picks),
                # After using every single science fiction work that
                # wasn't previously used, we reuse self.mq_sf to pad the
                # "Science Fiction" lane up to three items. It's
                # better to have self.lq_sf show up before self.mq_sf, even
                # though it's lower quality, because self.lq_sf hasn't been
                # used before.
                (data.hq_sf, sf_lane),
                (data.lq_sf, sf_lane),
                (data.mq_sf, sf_lane),
                # The 'Romance' lane now contains all three Romance
                # titles, with the higher-quality titles first.
                (data.hq_ro, romance_lane),
                (data.mq_ro, romance_lane),
                (data.lq_ro, romance_lane),
                # The 'Discredited Nonfiction' lane is the same as
                # before.
                (data.nonfiction, discredited_nonfiction),
                # After using every single fiction work that wasn't
                # previously used, we reuse high-quality works to pad
                # the "Fiction" lane to three items. The
                # lowest-quality Romance title doesn't show up here
                # anymore, because the 'Romance' lane claimed it. If
                # we have to reuse titles, we'll reuse the
                # high-quality ones.
                (data.hq_litfic, fiction),
                (data.hq_sf, fiction),
                (data.hq_ro, fiction),
            ],
        )

        # Let's see how entry points affect the feeds.
        #

        # There are no audiobooks in the system, so passing in a
        # FeaturedFacets scoped to the AudiobooksEntryPoint excludes everything.
        fetured_facets = FeaturedFacets(0, entrypoint=AudiobooksEntryPoint)
        _db = session
        assert [] == list(fiction.groups(session, facets=fetured_facets))

        # Here's an entry point that applies a language filter
        # that only finds one book.
        class LQRomanceEntryPoint(EntryPoint):
            URI = ""

            @classmethod
            def modify_search_filter(cls, filter):
                filter.languages = ["lan"]

        fetured_facets = FeaturedFacets(
            1, entrypoint=LQRomanceEntryPoint, random_seed=Filter.DETERMINISTIC
        )
        assert_contents(
            make_groups(fiction, facets=fetured_facets),
            [
                # The single recognized book shows up in both lanes
                # that can show it.
                (data.lq_ro, romance_lane),
                (data.lq_ro, fiction),
            ],
        )

        # Now, instead of relying on the 'Fiction' lane, make a
        # WorkList containing two different lanes, and call groups() on
        # the WorkList.

        class MockWorkList:
            display_name = "Mock"
            visible = True
            priority = 2

            def groups(slf, _db, include_sublanes, pagination=None, facets=None):
                yield data.lq_litfic, slf

        mock = MockWorkList()

        wl = WorkList()
        wl.initialize(db.default_library(), children=[best_sellers, staff_picks, mock])

        # We get results from the two lanes and from the MockWorkList.
        # Since the MockWorkList wasn't a lane, its results were obtained
        # by calling groups() recursively.
        assert_contents(
            wl.groups(session),
            [
                (data.mq_sf, best_sellers),
                (data.mq_sf, staff_picks),
                (data.lq_litfic, mock),
            ],
        )


class RandomSeedFixture:
    def __init__(self):
        random.seed(42)


@pytest.fixture
def random_seed_fixture() -> RandomSeedFixture:
    """A fixture that initializes the RNG to a predictable value each time."""
    return RandomSeedFixture()


class TestWorkListGroups:
    def test_groups_for_lanes_adapts_facets(
        self,
        random_seed_fixture: RandomSeedFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        db = end_to_end_search_fixture.db

        # Verify that _groups_for_lanes gives each of a WorkList's
        # non-queryable children the opportunity to adapt the incoming
        # FeaturedFacets objects to its own needs.

        class MockParent(WorkList):
            def _featured_works_with_lanes(
                self, _db, lanes, pagination, facets, *args, **kwargs
            ):
                self._featured_works_with_lanes_called_with = (
                    lanes,
                    pagination,
                    facets,
                )
                return super()._featured_works_with_lanes(
                    _db, lanes, pagination, facets, *args, **kwargs
                )

        class MockChild(WorkList):
            def __init__(self, work):
                self.work = work
                self.id = work.title
                super().__init__()

            def overview_facets(self, _db, facets):
                self.overview_facets_called_with = (_db, facets)
                return "Custom facets for %s." % self.id

            def works(self, _db, pagination, facets, *args, **kwargs):
                self.works_called_with = (pagination, facets)
                return [self.work]

        parent = MockParent()
        child1 = MockChild(db.work(title="Lane 1"))
        child2 = MockChild(db.work(title="Lane 2"))
        children = [child1, child2]

        for wl in children:
            wl.initialize(library=db.default_library())
        parent.initialize(library=db.default_library(), children=[child1, child2])

        # We're going to make a grouped feed in which both children
        # are relevant, but neither one is queryable.
        relevant = parent.children
        queryable: list = []
        pagination = Pagination(size=2)
        facets = FeaturedFacets(0)
        groups = list(
            parent._groups_for_lanes(
                db.session, relevant, queryable, pagination, facets
            )
        )

        # Each sublane was asked in turn to provide works for the feed.
        assert [(child1.work, child1), (child2.work, child2)] == groups

        # But we're more interested in what happened to the faceting objects.

        # The original faceting object was passed into
        # _featured_works_with_lanes, but none of the lanes were
        # queryable, so it ended up doing nothing.
        assert ([], pagination, facets) == parent._featured_works_with_lanes_called_with

        # Each non-queryable sublane was given a chance to adapt that
        # faceting object to its own needs.
        for wl in children:
            assert wl.overview_facets_called_with == ((db.session), facets)

        # Each lane's adapted faceting object was then passed into
        # works().
        assert (pagination, "Custom facets for Lane 1.") == child1.works_called_with

        assert (pagination, "Custom facets for Lane 2.") == child2.works_called_with

        # If no pagination object is passed in (the most common case),
        # a new Pagination object is created based on the featured lane
        # size for the library.
        groups = list(
            parent._groups_for_lanes(db.session, relevant, queryable, None, facets)
        )

        (ignore1, pagination, ignore2) = parent._featured_works_with_lanes_called_with
        assert isinstance(pagination, Pagination)

        # For each sublane, we ask for 10% more items than we need to
        # reduce the chance that we'll need to put the same item in
        # multiple lanes.
        assert (
            int(db.default_library().settings.featured_lane_size * 1.10)
            == pagination.size
        )

    def test_featured_works_with_lanes(
        self,
        db: DatabaseTransactionFixture,
        random_seed_fixture: RandomSeedFixture,
    ):
        # _featured_works_with_lanes builds a list of queries and
        # passes the list into search_engine.works_query_multi(). It
        # passes the search results into works_for_resultsets() to
        # create a sequence of (Work, Lane) 2-tuples.
        class MockWorkList(WorkList):
            """Mock the behavior of WorkList that's not being tested here --
            overview_facets() for the child lanes that are being
            searched, and works_for_resultsets() for the parent that's
            doing the searching.
            """

            def __init__(self, *args, **kwargs):
                # Track all the times overview_facets is called (it
                # should be called twice), plus works_for_resultsets
                # (which should only be called once).
                super().__init__(*args, **kwargs)
                self.works_for_resultsets_calls = []
                self.overview_facets_calls = []

            def overview_facets(self, _db, facets):
                # Track that overview_facets was called with a
                # FeaturedFacets object. Then call the superclass
                # implementation -- we need to return a real Facets
                # object so it can be turned into a Filter.
                assert isinstance(facets, FeaturedFacets)
                self.overview_facets_calls.append((_db, facets))
                return super().overview_facets(_db, facets)

            def works_for_resultsets(self, _db, resultsets, facets=None):
                # Take some lists of (mocked) of search results and turn
                # them into lists of (mocked) Works.
                self.works_for_resultsets_calls.append((_db, resultsets))
                one_lane_worth = [["Here is", "one lane", "of works"]]
                return one_lane_worth * len(resultsets)

        class MockSearchEngine:
            """Mock a multi-query call to an Opensearch server."""

            def __init__(self):
                self.called_with = None

            def query_works_multi(self, queries):
                # Pretend to run a multi-query and return three lists of
                # mocked results.
                self.called_with = queries
                return [["some"], ["search"], ["results"]]

        # Now the actual test starts. We've got a parent lane with two
        # children.
        parent = MockWorkList()
        child1 = MockWorkList()
        child2 = MockWorkList()
        parent.initialize(
            library=db.default_library(),
            children=[child1, child2],
            display_name="Parent lane -- call my _featured_works_with_lanes()!",
        )
        child1.initialize(library=db.default_library(), display_name="Child 1")
        child2.initialize(library=db.default_library(), display_name="Child 2")

        # We've got a search engine that's ready to find works in any
        # of these lanes.
        search = MockSearchEngine()

        # Set up facets and pagination, and call the method that's
        # being tested.
        facets = FeaturedFacets(0.1)
        pagination = object()
        results = parent._featured_works_with_lanes(
            db.session, [child1, child2], pagination, facets, search_engine=search
        )
        results = list(results)

        # MockSearchEngine.query_works_multi was called on a list of
        # queries it prepared from child1 and child2.
        q1, q2 = search.called_with

        # These queries are almost the same.
        for query in search.called_with:
            # Neither has a query string.
            assert None == query[0]
            # Both have the same pagination object.
            assert pagination == query[2]

        # But each query has a different Filter.
        f1 = q1[1]
        f2 = q2[1]
        assert f1 != f2

        # How did these Filters come about? Well, for each lane, we
        # called overview_facets() and passed in the same
        # FeaturedFacets object.
        assert ((db.session), facets) == child1.overview_facets_calls.pop()
        assert [] == child1.overview_facets_calls
        child1_facets = child1.overview_facets(db.session, facets)

        assert ((db.session), facets) == child2.overview_facets_calls.pop()
        assert [] == child2.overview_facets_calls
        child2_facets = child1.overview_facets(db.session, facets)

        # We then passed each result into Filter.from_worklist, along
        # with the corresponding lane.
        compare_f1 = Filter.from_worklist(db.session, child1, child1_facets)
        compare_f2 = Filter.from_worklist(db.session, child2, child2_facets)

        # Reproducing that code inside this test, which we just did,
        # gives us Filter objects -- compare_f1 and compare_f2 --
        # identical to the ones passed into query_works_multi -- f1
        # and f2. We know they're the same because they build() to
        # identical dictionaries.
        assert compare_f1.build() == f1.build()
        assert compare_f2.build() == f2.build()

        # So we ended up with q1 and q2, two queries to find the works
        # from child1 and child2. That's what was passed into
        # query_works_multi().

        # We know that query_works_multi() returned: a list
        # of lists of fake "results" that looked like this:
        # [["some"], ["search"], ["results"]]
        #
        # This was passed into parent.works_for_resultsets():
        call = parent.works_for_resultsets_calls.pop()
        assert call == ((db.session), [["some"], ["search"], ["results"]])
        assert [] == parent.works_for_resultsets_calls

        # The return value of works_for_resultsets -- another list of
        # lists -- was then turned into a sequence of ('work', Lane)
        # 2-tuples.
        assert [
            ("Here is", child1),
            ("one lane", child1),
            ("of works", child1),
            ("Here is", child2),
            ("one lane", child2),
            ("of works", child2),
        ] == results
        # And that's how we got a sequence of 2-tuples mapping out a
        # grouped OPDS feed.

    def test__size_for_facets(
        self,
        db: DatabaseTransactionFixture,
        random_seed_fixture: RandomSeedFixture,
    ):
        lane = db.lane()
        m = lane._size_for_facets

        ebooks, audio, everything, nothing = (
            FeaturedFacets(minimum_featured_quality=0.5, entrypoint=x)
            for x in (
                EbooksEntryPoint,
                AudiobooksEntryPoint,
                EverythingEntryPoint,
                None,
            )
        )

        # When Lane.size_by_entrypoint is not set, Lane.size is used.
        # This should only happen immediately after a site is upgraded.
        lane.size = 100
        for facets in (ebooks, audio):
            assert 100 == lane._size_for_facets(facets)

        # Once Lane.size_by_entrypoint is set, it's used when possible.
        lane.size_by_entrypoint = {
            EverythingEntryPoint.URI: 99,
            EbooksEntryPoint.URI: 1,
            AudiobooksEntryPoint.URI: 2,
        }
        assert 99 == m(None)
        assert 99 == m(nothing)
        assert 99 == m(everything)
        assert 1 == m(ebooks)
        assert 2 == m(audio)

        # If size_by_entrypoint contains no estimate for a given
        # EntryPoint URI, the overall lane size is used. This can
        # happen between the time an EntryPoint is enabled and the
        # lane size refresh script is run.
        del lane.size_by_entrypoint[AudiobooksEntryPoint.URI]
        assert 100 == m(audio)
