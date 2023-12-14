from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session
from sqlalchemy.sql import func, select
from sqlalchemy.sql.expression import and_, false, or_, true

from api.admin.model.dashboard_statistics import (
    CollectionInventory,
    InventoryStatistics,
    LibraryStatistics,
    PatronStatistics,
    StatisticsResponse,
)
from core.model import (
    Admin,
    Collection,
    Edition,
    Hold,
    Library,
    LicensePool,
    Loan,
    Patron,
)

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import (
        BinaryExpression,
        BooleanClauseList,
        ClauseElement,
    )
    from sqlalchemy.sql.expression import ColumnElement
    from sqlalchemy.sql.type_api import TypeEngine


def generate_statistics(admin: Admin, db: Session) -> StatisticsResponse:
    return Statistics(db).stats(admin)


class Statistics:
    METERED_LICENSE_FILTER = and_(
        LicensePool.licenses_owned > 0,
        LicensePool.unlimited_access == false(),
        LicensePool.open_access == false(),
    )
    UNLIMITED_LICENSE_FILTER = and_(
        LicensePool.unlimited_access == true(),
        LicensePool.open_access == false(),
    )
    OPEN_ACCESS_FILTER = LicensePool.open_access == true()
    AT_LEAST_ONE_LOANABLE_FILTER = or_(
        UNLIMITED_LICENSE_FILTER,
        OPEN_ACCESS_FILTER,
        and_(METERED_LICENSE_FILTER, LicensePool.licenses_available > 0),
    )

    def __init__(self, session: Session):
        self._db = session

    def stats(self, admin: Admin) -> StatisticsResponse:
        """Build and return a statistics response for admin user's authorized libraries."""

        # Determine which libraries and collections are authorized for this user.
        authorized_libraries = self._libraries_for_admin(admin)
        authorized_collections_by_library = {
            lib.short_name: set(lib.all_collections) for lib in authorized_libraries
        }
        all_authorized_collections: list[Collection] = [
            c for c in self._db.query(Collection) if admin.can_see_collection(c)
        ]

        collection_inventories = sorted(
            (self._create_collection_inventory(c) for c in all_authorized_collections),
            key=lambda c: c.id,
        )
        (
            collection_inventory_summary,
            collection_inventory_summary_by_medium,
        ) = _summarize_collection_inventories(
            collection_inventories, all_authorized_collections
        )

        inventories_by_library = {
            library_key: _summarize_collection_inventories(
                collection_inventories, collections
            )
            for library_key, collections in authorized_collections_by_library.items()
        }
        patron_stats_by_library = {
            lib.short_name: self._gather_patron_stats(lib)
            for lib in authorized_libraries
        }
        library_statistics = [
            LibraryStatistics(
                key=lib.short_name,
                name=lib.name or "(missing library name)",
                patron_statistics=patron_stats_by_library[lib.short_name],
                inventory_summary=inventories_by_library[lib.short_name][0],
                inventory_by_medium=inventories_by_library[lib.short_name][1],
                collection_ids=sorted(
                    [
                        c.id
                        for c in authorized_collections_by_library[lib.short_name]
                        if c.id is not None
                    ]
                ),
            )
            for lib in authorized_libraries
        ]

        # Accumulate patron summary statistics from authorized libraries.
        patron_summary = sum(
            patron_stats_by_library.values(), PatronStatistics.zeroed()
        )

        return StatisticsResponse(
            collections=collection_inventories,
            libraries=library_statistics,
            inventory_summary=collection_inventory_summary,
            inventory_by_medium=collection_inventory_summary_by_medium,
            patron_summary=patron_summary,
        )

    def _libraries_for_admin(self, admin: Admin) -> list[Library]:
        """Return a list of libraries to which this user has access."""
        return [
            library
            for library in self._db.query(Library)
            if admin.is_librarian(library)
        ]

    def _collection_statistics_by_medium_query(
        self,
        collection_filter: BinaryExpression[TypeEngine[bool]],
        query_filter: BooleanClauseList[ClauseElement],
        /,
        columns: list[ColumnElement[TypeEngine[int]]],
    ) -> dict[str, dict[str, int]]:
        stats_with_medium = (
            self._db.execute(
                select(
                    Edition.medium,
                    *columns,
                )
                .select_from(LicensePool)
                .join(Edition, Edition.id == LicensePool.presentation_edition_id)
                .where(collection_filter)
                .where(query_filter)
                .group_by(Edition.medium)
            )
            .mappings()
            .all()
        )
        return {
            row["medium"]: {k: v for k, v in row.items() if k != "medium"}
            for row in stats_with_medium
        }

    def _run_collection_stats_queries(
        self, collection: Collection
    ) -> _CollectionStatisticsQueryResults:
        collection_filter = LicensePool.collection_id == collection.id
        _query_stats_group: Callable[..., dict[str, dict[str, int]]] = partial(
            self._collection_statistics_by_medium_query, collection_filter
        )
        count = func.count().label("count")
        return _CollectionStatisticsQueryResults(
            metered_title_counts=_query_stats_group(
                self.METERED_LICENSE_FILTER, columns=[count]
            ),
            unlimited_title_counts=_query_stats_group(
                self.UNLIMITED_LICENSE_FILTER, columns=[count]
            ),
            open_access_title_counts=_query_stats_group(
                self.OPEN_ACCESS_FILTER, columns=[count]
            ),
            loanable_title_counts=_query_stats_group(
                self.AT_LEAST_ONE_LOANABLE_FILTER, columns=[count]
            ),
            metered_license_stats=_query_stats_group(
                self.METERED_LICENSE_FILTER,
                columns=[
                    func.sum(LicensePool.licenses_owned).label("owned"),
                    func.sum(LicensePool.licenses_available).label("available"),
                ],
            ),
        )

    def _create_collection_inventory(
        self, collection: Collection
    ) -> CollectionInventory:
        """Return a CollectionInventory for the given collection."""

        statistics = self._run_collection_stats_queries(collection)
        # Ensure that the key is a string, even if the medium is null.
        inventory_by_medium = {
            str(m): inv for m, inv in statistics.inventories_by_medium().items()
        }
        summary_inventory = sum(
            inventory_by_medium.values(), InventoryStatistics.zeroed()
        )
        return CollectionInventory(
            id=collection.id,
            name=collection.name,
            inventory=summary_inventory,
            inventory_by_medium=inventory_by_medium,
        )

    def _gather_patron_stats(self, library: Library) -> PatronStatistics:
        library_patron_query = self._db.query(Patron.id.label("id")).filter(
            Patron.library_id == library.id
        )
        patrons_for_active_loans_query = library_patron_query.join(Loan).filter(
            Loan.end >= datetime.now()
        )
        patrons_for_active_holds_query = library_patron_query.join(Hold)

        patron_count = library_patron_query.count()
        loan_count = patrons_for_active_loans_query.count()
        hold_count = patrons_for_active_holds_query.count()
        patrons_with_active_loans = patrons_for_active_loans_query.distinct(
            "id"
        ).count()
        patrons_with_active_loans_or_holds = patrons_for_active_loans_query.union(
            patrons_for_active_holds_query
        ).count()

        return PatronStatistics(
            total=patron_count,
            with_active_loan=patrons_with_active_loans,
            with_active_loan_or_hold=patrons_with_active_loans_or_holds,
            loans=loan_count,
            holds=hold_count,
        )


def _summarize_collection_inventories(
    collection_inventories: Iterable[CollectionInventory],
    collections: Iterable[Collection],
) -> tuple[InventoryStatistics, dict[str, InventoryStatistics]]:
    """Summarize the inventories associated with the specified collections.

    The collections represented by the specified `collection_inventories`
    must be a superset of the specified `collections`.

    :param collections: `collections` for which to summarize inventory information.
    :param collection_inventories: `CollectionInventory`s for the collections.
    :return: Summary inventory and summary inventory by medium.
    """
    included_collection_inventories = (
        inv for inv in collection_inventories if inv.id in {c.id for c in collections}
    )

    summary_inventory = InventoryStatistics.zeroed()
    summary_inventory_by_medium: dict[str, InventoryStatistics] = {}

    for ci in included_collection_inventories:
        summary_inventory += ci.inventory
        inventory_by_medium = ci.inventory_by_medium or {}
        for medium, inventory in inventory_by_medium.items():
            summary_inventory_by_medium[medium] = (
                summary_inventory_by_medium.get(medium, InventoryStatistics.zeroed())
                + inventory
            )
    return summary_inventory, summary_inventory_by_medium


@dataclasses.dataclass(frozen=True)
class _CollectionStatisticsQueryResults:
    unlimited_title_counts: dict[str, dict[str, int]]
    open_access_title_counts: dict[str, dict[str, int]]
    loanable_title_counts: dict[str, dict[str, int]]
    metered_title_counts: dict[str, dict[str, int]]
    metered_license_stats: dict[str, dict[str, int]]

    def inventories_by_medium(self) -> dict[str, InventoryStatistics]:
        """Return a mapping of all mediums present to their associated inventories."""
        return {
            medium: self.inventory_for_medium(medium)
            for medium in self.mediums_present()
        }

    def mediums_present(self) -> set[str]:
        """Returns a list of the mediums present in these collection statistics."""
        statistics = dataclasses.asdict(self)
        return set().union(*(stat.keys() for stat in statistics.values()))

    def inventory_for_medium(self, medium: str) -> InventoryStatistics:
        """Return statistics for the specified medium."""
        unlimited_titles = self._lookup_property(
            "unlimited_title_counts", medium, "count"
        )
        open_access_titles = self._lookup_property(
            "open_access_title_counts", medium, "count"
        )
        loanable_titles = self._lookup_property(
            "loanable_title_counts", medium, "count"
        )
        metered_titles = self._lookup_property("metered_title_counts", medium, "count")
        metered_owned_licenses = self._lookup_property(
            "metered_license_stats", medium, "owned"
        )
        metered_available_licenses = self._lookup_property(
            "metered_license_stats", medium, "available"
        )

        return InventoryStatistics(
            titles=metered_titles + unlimited_titles + open_access_titles,
            available_titles=loanable_titles,
            open_access_titles=open_access_titles,
            licensed_titles=metered_titles + unlimited_titles,
            unlimited_license_titles=unlimited_titles,
            metered_license_titles=metered_titles,
            metered_licenses_owned=metered_owned_licenses,
            metered_licenses_available=metered_available_licenses,
        )

    def _lookup_property(
        self,
        group: str,
        medium: str,
        column_name: str,
    ) -> int:
        """Return value for a statistic, if present; else, return zero."""
        field: dict[str, dict[str, int]] = getattr(self, group, {})
        return field.get(medium, {}).get(column_name, 0)
