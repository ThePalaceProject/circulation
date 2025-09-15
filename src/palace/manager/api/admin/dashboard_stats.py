from __future__ import annotations

import dataclasses
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from sqlalchemy import not_, union
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select, func, select
from sqlalchemy.sql.expression import and_, distinct, false, or_, true

from palace.manager.api.admin.model.dashboard_statistics import (
    CollectionInventory,
    InventoryStatistics,
    LibraryStatistics,
    PatronStatistics,
    StatisticsResponse,
)
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron


def generate_statistics(admin: Admin, db: Session) -> StatisticsResponse:
    return Statistics(db).stats(admin)


class Statistics:
    METERED_LICENSE_FILTER = and_(  # type: ignore[type-var]
        LicensePool.licenses_owned > 0,
        LicensePool.unlimited_access == false(),
        LicensePool.open_access == false(),
    )
    UNLIMITED_LICENSE_FILTER = and_(  # type: ignore[type-var]
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
        authorized_libraries = admin.authorized_libraries()
        (
            all_authorized_collections,
            authorized_collections_by_library,
            filter_collections,
        ) = self._authorized_collections(admin, authorized_libraries)

        collection_inventories = self._create_collection_inventories(
            all_authorized_collections, filter_collections
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
        patron_stats_by_library = self._gather_patron_stats(authorized_libraries)
        library_statistics = [
            LibraryStatistics(
                key=lib.short_name,
                name=lib.name or "(missing library name)",
                patron_statistics=patron_stats_by_library[lib.short_name],
                inventory_summary=inventories_by_library[lib.short_name][0],
                inventory_by_medium=inventories_by_library[lib.short_name][1],
                collection_ids=[
                    c.id for c in authorized_collections_by_library[lib.short_name]
                ],
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

    def _all_collections(self) -> dict[int | None, _Collection]:
        collection_query = self._db.execute(
            select(Collection.id, IntegrationConfiguration.name)
            .select_from(Collection)
            .join(IntegrationConfiguration)
        ).all()
        return {
            c.id: _Collection(id=c.id, name=c.name)
            for c in collection_query
            if c.id is not None
        }

    def _authorized_collections(
        self,
        admin: Admin,
        authorized_libraries: list[Library],
    ) -> tuple[list[_Collection], dict[str, list[_Collection]], bool]:
        authorized_collections: set[_Collection] = set()
        authorized_collections_by_library = {}

        all_collections = self._all_collections()

        for library in authorized_libraries:
            library_collections = {
                all_collections[c.id] for c in library.associated_collections
            }
            authorized_collections_by_library[library.short_name] = sorted(
                library_collections, key=lambda c: c.id
            )

        if admin.is_system_admin():
            # If the user is a system admin, they have access to all collections, even those
            # not associated with a library.
            authorized_collections.update(all_collections.values())
        else:
            # Otherwise, the user only has access to collections associated with their
            # authorized libraries.
            for collections in authorized_collections_by_library.values():
                authorized_collections.update(collections)

        # This cutoff is arbitrary, but it's a reasonable heuristic for when to filter collections
        # by library as part of the query, vs filtering them in Python. It is slower to filter as
        # part of the query if the user has access to most collections.
        filter_collections = (
            len(authorized_collections) / len(all_collections) < 0.5
            if all_collections
            else False
        )

        return (
            sorted(authorized_collections, key=lambda c: c.id),
            authorized_collections_by_library,
            filter_collections,
        )

    def _collections_statistics_by_medium_query(
        self,
        query_filter: Any,
        /,
        columns: list[Any],
        collections: list[_Collection] | None,
    ) -> dict[int, dict[str, dict[str, int]]]:
        query = (
            select(
                LicensePool.collection_id,
                Edition.medium,
                *columns,
            )
            .select_from(LicensePool)
            .join(Edition, Edition.id == LicensePool.presentation_edition_id)
            .where(query_filter)
            .group_by(Edition.medium, LicensePool.collection_id)
        )
        if collections is not None:
            query = query.where(
                LicensePool.collection_id.in_({c.id for c in collections})
            )

        stats_with_medium = self._db.execute(query).mappings().all()

        statistics: dict[int, dict[str, dict[str, int]]] = defaultdict(dict)
        for row in stats_with_medium:
            collection_id = row["collection_id"]
            medium = row["medium"]
            statistics[collection_id][medium] = {
                k: v for k, v in row.items() if k not in ["medium", "collection_id"]
            }
        return statistics

    def _run_collections_stats_queries(
        self, collections: list[_Collection], filter_collections: bool
    ) -> dict[int | None, _CollectionStatisticsQueryResults]:
        count = func.count().label("count")
        collection_filter = collections if filter_collections else None
        metered_title_counts = self._collections_statistics_by_medium_query(
            self.METERED_LICENSE_FILTER, columns=[count], collections=collection_filter
        )
        unlimited_title_counts = self._collections_statistics_by_medium_query(
            self.UNLIMITED_LICENSE_FILTER,
            columns=[count],
            collections=collection_filter,
        )
        open_access_title_counts = self._collections_statistics_by_medium_query(
            self.OPEN_ACCESS_FILTER,
            columns=[count],
            collections=collection_filter,
        )
        loanable_title_counts = self._collections_statistics_by_medium_query(
            self.AT_LEAST_ONE_LOANABLE_FILTER,
            columns=[count],
            collections=collection_filter,
        )
        metered_license_stats = self._collections_statistics_by_medium_query(
            self.METERED_LICENSE_FILTER,
            columns=[
                func.sum(LicensePool.licenses_owned).label("owned"),
                func.sum(LicensePool.licenses_available).label("available"),
            ],
            collections=collection_filter,
        )

        return {
            c.id: _CollectionStatisticsQueryResults(
                metered_title_counts=(
                    metered_title_counts[c.id] if c.id in metered_title_counts else {}
                ),
                unlimited_title_counts=(
                    unlimited_title_counts[c.id]
                    if c.id in unlimited_title_counts
                    else {}
                ),
                open_access_title_counts=(
                    open_access_title_counts[c.id]
                    if c.id in open_access_title_counts
                    else {}
                ),
                loanable_title_counts=(
                    loanable_title_counts[c.id] if c.id in loanable_title_counts else {}
                ),
                metered_license_stats=(
                    metered_license_stats[c.id] if c.id in metered_license_stats else {}
                ),
            )
            for c in collections
        }

    def _create_collection_inventories(
        self, collections: list[_Collection], filter_collections: bool
    ) -> list[CollectionInventory]:
        statistics = self._run_collections_stats_queries(
            collections, filter_collections
        )
        inventories = []
        for collection in collections:
            inventory_by_medium = {
                str(m): inv
                for m, inv in statistics[collection.id].inventories_by_medium().items()
            }
            summary_inventory = sum(
                inventory_by_medium.values(), InventoryStatistics.zeroed()
            )
            inventories.append(
                CollectionInventory(
                    id=collection.id,
                    name=collection.name,
                    inventory=summary_inventory,
                    inventory_by_medium=inventory_by_medium,
                )
            )
        return inventories

    @staticmethod
    def _loans_or_holds_query(loan_or_hold: type[Loan] | type[Hold]) -> Select:
        query = (
            select(
                Patron.library_id,
            )
            .select_from(loan_or_hold)
            .join(Patron)
        )

        if issubclass(loan_or_hold, Loan):
            query = query.where(loan_or_hold.end >= datetime.now())
        elif issubclass(loan_or_hold, Hold):
            # active holds are holds where the position is greater than zero AND end is not before present.
            # apparently a hold can have an end that is None when the estimate end has not yet been or
            # cannot be calculated.
            # Hold.position = 0 and Hold.end before present is the only state where we can definitively say the
            # hold is not active so we exclude only those holds here.
            query = query.where(
                not_(
                    and_(
                        loan_or_hold.position == 0,
                        loan_or_hold.end != None,
                        loan_or_hold.end < datetime.now(),
                    )
                )
            )

        return query

    @classmethod
    def _loans_or_holds_count_query(
        cls, loan_or_hold: type[Loan] | type[Hold]
    ) -> Select:
        return (
            cls._loans_or_holds_query(loan_or_hold)
            .add_columns(
                func.count().label("count"),
                func.count(distinct(loan_or_hold.patron_id)).label("patron_count"),
            )
            .group_by(Patron.library_id)
        )

    @classmethod
    def _active_loans_or_holds_count_query(cls) -> Select:
        union_query = union(
            cls._loans_or_holds_query(Loan).add_columns(Patron.id.label("patron_id")),
            cls._loans_or_holds_query(Hold).add_columns(Patron.id.label("patron_id")),
        ).subquery()

        return (
            select(
                func.count(distinct(union_query.c["patron_id"])).label("count"),
                union_query.c["library_id"],
            )
            .select_from(union_query)
            .group_by(union_query.c["library_id"])
        )

    def _gather_patron_stats(
        self, libraries: list[Library]
    ) -> dict[str | None, PatronStatistics]:
        patron_count_query = (
            self._db.execute(
                select(Patron.library_id, func.count().label("count"))
                .select_from(Patron)
                .group_by(Patron.library_id)
            )
            .mappings()
            .all()
        )
        patron_count = {row["library_id"]: row["count"] for row in patron_count_query}
        loans_query = (
            self._db.execute(self._loans_or_holds_count_query(Loan)).mappings().all()
        )
        loan_count = {row["library_id"]: row["count"] for row in loans_query}
        active_loans = {row["library_id"]: row["patron_count"] for row in loans_query}
        hold_query = (
            self._db.execute(self._loans_or_holds_count_query(Hold)).mappings().all()
        )
        hold_count = {row["library_id"]: row["count"] for row in hold_query}
        active_loan_or_hold_query = (
            self._db.execute(self._active_loans_or_holds_count_query()).mappings().all()
        )
        active_loan_or_hold = {
            row["library_id"]: row["count"] for row in active_loan_or_hold_query
        }

        return {
            library.short_name: PatronStatistics(
                total=patron_count[library.id] if library.id in patron_count else 0,
                with_active_loan=(
                    active_loans[library.id] if library.id in active_loans else 0
                ),
                with_active_loan_or_hold=(
                    active_loan_or_hold[library.id]
                    if library.id in active_loan_or_hold
                    else 0
                ),
                loans=loan_count[library.id] if library.id in loan_count else 0,
                holds=hold_count[library.id] if library.id in hold_count else 0,
            )
            for library in libraries
        }


def _summarize_collection_inventories(
    collection_inventories: Iterable[CollectionInventory],
    collections: Iterable[_Collection],
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


@dataclasses.dataclass(frozen=True)
class _Collection:
    id: int
    name: str
