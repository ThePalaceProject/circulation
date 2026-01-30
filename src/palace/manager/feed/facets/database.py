from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Query, Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from palace.manager.feed.facets.constants import FacetConfig, FacetConstants
from palace.manager.feed.facets.feed import Facets
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.work import Work

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.library import Library


class DatabaseBackedFacets(Facets):
    """A generic faceting object designed for managing queries against the
    database. (Other faceting objects are designed for managing
    Opensearch searches.)
    """

    # Of the sort orders in Facets, these are the only available ones
    # -- they map directly onto a field of one of the tables we're
    # querying.
    ORDER_FACET_TO_DATABASE_FIELD: dict[str, InstrumentedAttribute] = {
        FacetConstants.ORDER_WORK_ID: Work.id,
        FacetConstants.ORDER_TITLE: Edition.sort_title,
        FacetConstants.ORDER_AUTHOR: Edition.sort_author,
        FacetConstants.ORDER_LAST_UPDATE: Work.last_update_time,
        # Reverse variants map to the same fields (direction is handled separately)
        FacetConstants.ORDER_TITLE_DESC: Edition.sort_title,
        FacetConstants.ORDER_AUTHOR_DESC: Edition.sort_author,
    }
    # Note: ORDER_ADDED_TO_COLLECTION variants are not database-backed

    @classmethod
    def available_facets(
        cls, config: Library | FacetConfig | None, facet_group_name: str
    ) -> list[str]:
        """Exclude search orders not available through database queries."""
        standard = config.enabled_facets(facet_group_name) if config else []
        if standard is None:
            standard = []
        if facet_group_name != cls.ORDER_FACET_GROUP_NAME:
            return standard
        return [
            order for order in standard if order in cls.ORDER_FACET_TO_DATABASE_FIELD
        ]

    @classmethod
    def default_facet(
        cls, config: Library | FacetConfig | None, facet_group_name: str
    ) -> str | None:
        """Exclude search orders not available through database queries."""
        standard_default = super().default_facet(config, facet_group_name)
        if facet_group_name != cls.ORDER_FACET_GROUP_NAME:
            return standard_default
        if standard_default in cls.ORDER_FACET_TO_DATABASE_FIELD:
            # This default sort order is supported.
            return standard_default

        # The default sort order is not supported. Just pick the first
        # enabled sort order.
        enabled = config.enabled_facets(facet_group_name) if config else []
        for i in enabled or []:
            if i in cls.ORDER_FACET_TO_DATABASE_FIELD:
                return i

        # None of the enabled sort orders are usable. Order by work ID.
        return cls.ORDER_WORK_ID

    def order_by(
        self,
    ) -> tuple[list[Any], list[InstrumentedAttribute]]:
        """Given these Facets, create a complete ORDER BY clause for queries
        against WorkModelWithGenre.
        """
        default_sort_order: list[InstrumentedAttribute] = [
            Edition.sort_author,
            Edition.sort_title,
            Work.id,
        ]

        primary_order_by = (
            self.ORDER_FACET_TO_DATABASE_FIELD.get(self.order) if self.order else None
        )
        if primary_order_by is not None:
            # Promote the field designated by the sort facet to the top of
            # the order-by list.
            order_by: list[InstrumentedAttribute] = [primary_order_by]

            for i in default_sort_order:
                if i not in order_by:
                    order_by.append(i)
        else:
            # Use the default sort order
            order_by = default_sort_order

        # order_ascending applies only to the first field in the sort order.
        # Everything else is ordered ascending.
        if self.order_ascending:
            order_by_sorted = [x.asc() for x in order_by]
        else:
            order_by_sorted = [order_by[0].desc()] + [x.asc() for x in order_by[1:]]
        return order_by_sorted, order_by

    def modify_database_query(self, _db: Session, qu: Query[Work]) -> Query[Work]:
        """Restrict a query so that it matches only works
        that fit the criteria of this faceting object. Ensure
        query is appropriately ordered and made distinct.
        """

        # Filter by facet criteria
        qu = super().modify_database_query(_db, qu)

        # Set the ORDER BY clause.
        order_by, order_distinct = self.order_by()
        qu = qu.order_by(*order_by)
        qu = qu.distinct(*order_distinct)
        return qu
