from unittest.mock import create_autospec

from palace.manager.feed.facets.constants import FacetConstants
from palace.manager.feed.facets.database import DatabaseBackedFacets
from palace.manager.feed.facets.feed import Facets
from palace.manager.feed.worklist.database import DatabaseBackedWorkList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture


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

    def test_default_facets(self, db: DatabaseTransactionFixture):
        # If the configured default sort order is not available,
        # DatabaseBackedFacets chooses the first enabled sort order.
        f1 = Facets
        f2 = DatabaseBackedFacets

        group = FacetConstants.AVAILABILITY_FACET_GROUP_NAME
        assert f1.default_facet(db.default_library(), group) == f2.default_facet(
            db.default_library(), group
        )

        # In this bizarre library, the default sort order is 'time
        # added to collection' -- an order not supported by
        # DatabaseBackedFacets.
        config = create_autospec(Library, instance=True)
        config.enabled_facets.return_value = [
            FacetConstants.ORDER_ADDED_TO_COLLECTION,
            FacetConstants.ORDER_TITLE,
            FacetConstants.ORDER_AUTHOR,
        ]
        config.default_facet.return_value = FacetConstants.ORDER_ADDED_TO_COLLECTION

        # A Facets object uses the 'time added to collection' order by
        # default.
        assert f1.ORDER_ADDED_TO_COLLECTION == f1.default_facet(
            config, f1.ORDER_FACET_GROUP_NAME
        )

        # A DatabaseBacked Facets can't do that. It finds the first
        # enabled sort order that it can support, and uses it instead.
        assert f2.ORDER_TITLE == f2.default_facet(config, f2.ORDER_FACET_GROUP_NAME)

        # If no enabled sort orders are supported, it just sorts
        # by Work ID, so that there is always _some_ sort order.
        config.enabled_facets.return_value = [FacetConstants.ORDER_ADDED_TO_COLLECTION]
        assert f2.ORDER_WORK_ID == f2.default_facet(config, f2.ORDER_FACET_GROUP_NAME)

    def test_order_by(self, db: DatabaseTransactionFixture):
        E = Edition
        W = Work

        def order(facet, ascending=None):
            f = DatabaseBackedFacets(
                db.default_library(),
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
            available=Facets.AVAILABLE_ALL,
            order=Facets.ORDER_TITLE,
        ):
            f = DatabaseBackedFacets(db.default_library(), available, order, None, None)
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
