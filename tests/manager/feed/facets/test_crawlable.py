from unittest.mock import MagicMock

import pytest

from palace.manager.feed.facets.crawlable import CrawlableFacets
from palace.manager.feed.facets.feed import Facets
from palace.manager.sqlalchemy.model.library import Library
from tests.fixtures.database import DatabaseTransactionFixture


class TestCrawlableFacets:
    @pytest.mark.parametrize(
        "c1_distributor, c2_distributor, expected_distributor_count",
        (
            pytest.param("distributor", "distributor", 1, id="same_distributor"),
            pytest.param("distributor", "distributor2", 2, id="different_distributor"),
        ),
    )
    @pytest.mark.parametrize(
        "is_inactive, expected_collection_count",
        (
            pytest.param(True, 2, id="inactive"),
            pytest.param(False, 3, id="active"),
        ),
    )
    def test_default(
        self,
        db: DatabaseTransactionFixture,
        c1_distributor: str,
        c2_distributor: str,
        expected_distributor_count: int,
        is_inactive: bool,
        expected_collection_count: int,
    ):
        library = db.library()
        # The first two collections are always active, ...
        c1 = db.collection(
            library=library,
            settings=db.opds_settings(data_source=c1_distributor),
        )
        # ... but, this one may be inactive.
        c2 = db.collection(
            library=library,
            settings=db.opds_settings(data_source=c2_distributor),
        )
        # The third collection always has the same distributor as the first
        # one, but it may be inactive.
        c3 = db.collection(
            library=library,
            settings=db.opds_settings(data_source=c1_distributor),
            inactive=is_inactive,
        )

        facets = CrawlableFacets.default(library)

        assert facets.availability == CrawlableFacets.AVAILABLE_ALL
        assert facets.order == CrawlableFacets.ORDER_LAST_UPDATE
        assert facets.order_ascending is False

        [
            order,
            availability,
            distributor,
            collectionName,
        ] = facets.enabled_facets

        # The default availability is the only one enabled.
        assert len(availability) == 1
        # Order facet has one enabled facet
        assert len(order) == 1

        # Except for distributor and collectionName, which have the default
        # along with their unique values among each collection in the library.
        assert len(distributor) == 1 + expected_distributor_count
        assert len(collectionName) == 1 + expected_collection_count

    @pytest.mark.parametrize(
        "group_name, expected",
        [
            (
                Facets.ORDER_FACET_GROUP_NAME,
                [Facets.ORDER_LAST_UPDATE],
            ),
            (Facets.AVAILABILITY_FACET_GROUP_NAME, [Facets.AVAILABLE_ALL]),
            (Facets.DISTRIBUTOR_FACETS_GROUP_NAME, [Facets.DISTRIBUTOR_ALL]),
            (Facets.COLLECTION_NAME_FACETS_GROUP_NAME, [Facets.COLLECTION_NAME_ALL]),
        ],
    )
    def test_available_none(self, group_name: str, expected: list[str]) -> None:
        assert CrawlableFacets.available_facets(None, group_name) == expected

    @pytest.mark.parametrize(
        "group_name, expected",
        [
            (
                Facets.ORDER_FACET_GROUP_NAME,
                [Facets.ORDER_LAST_UPDATE],
            ),
            (Facets.AVAILABILITY_FACET_GROUP_NAME, [Facets.AVAILABLE_ALL]),
            (Facets.DISTRIBUTOR_FACETS_GROUP_NAME, [Facets.DISTRIBUTOR_ALL, "foo"]),
            (
                Facets.COLLECTION_NAME_FACETS_GROUP_NAME,
                [Facets.COLLECTION_NAME_ALL, "foo"],
            ),
        ],
    )
    def test_available(self, group_name: str, expected: list[str]):
        mock = MagicMock(spec=Library)
        mock.enabled_facets = MagicMock(return_value=["foo"])

        assert CrawlableFacets.available_facets(mock, group_name) == expected

        if group_name in [
            Facets.DISTRIBUTOR_FACETS_GROUP_NAME,
            Facets.COLLECTION_NAME_FACETS_GROUP_NAME,
        ]:
            assert mock.enabled_facets.call_count == 1
        else:
            assert mock.enabled_facets.call_count == 0
